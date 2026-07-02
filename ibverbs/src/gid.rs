use std::convert::TryInto;
use std::ffi::CStr;
use std::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use ffi::ibv_gid_type;

#[cfg(doc)]
use crate::QueuePairBuilder;

/// A Global identifier (GID) for an RDMA device port.
///
/// This struct acts as a rust wrapper for [`ffi::ibv_gid`]. We use it instead of
/// `ffi::ibv_gid` directly because the latter is actually an untagged union.
///
/// ```c
/// union ibv_gid {
///     uint8_t   raw[16];
///     struct {
///         __be64 subnet_prefix;
///         __be64 interface_id;
///     } global;
/// };
/// ```
///
/// It appears that `global` exists for convenience, but can be safely ignored.
/// For continuity, the methods `subnet_prefix` and `interface_id` are provided.
/// These methods read the array as big endian, regardless of native cpu
/// endianness.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Default, Copy, Clone, Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct Gid {
    raw: [u8; 16],
}

impl Gid {
    /// Expose the subnet_prefix component of the `Gid` as a u64. This is
    /// equivalent to accessing the `global.subnet_prefix` component of the
    /// `ffi::ibv_gid` union.
    pub fn subnet_prefix(&self) -> u64 {
        u64::from_be_bytes(self.raw[..8].try_into().unwrap())
    }

    /// Expose the interface_id component of the `Gid` as a u64. This is
    /// equivalent to accessing the `global.interface_id` component of the
    /// `ffi::ibv_gid` union.
    pub fn interface_id(&self) -> u64 {
        u64::from_be_bytes(self.raw[8..].try_into().unwrap())
    }

    /// Whether this GID holds an IPv4-mapped address (`::ffff:a.b.c.d`).
    ///
    /// On RoCE, the GIDs of a port mirror the IP addresses of its network interface, so the entry
    /// holding the interface's IPv4 address is IPv4-mapped. That entry is typically the routable
    /// one to pick (by its index, via [`QueuePairBuilder::set_gid_index`]) on plain-Ethernet IPv4
    /// networks.
    pub fn is_ipv4_mapped(&self) -> bool {
        std::net::Ipv6Addr::from(*self).to_ipv4_mapped().is_some()
    }
}

/// A GID is an IPv6 address by construction (RoCE GIDs literally mirror the interface's IP
/// addresses); this conversion makes it printable and comparable as one.
impl From<Gid> for std::net::Ipv6Addr {
    fn from(gid: Gid) -> Self {
        std::net::Ipv6Addr::from(gid.raw)
    }
}

impl From<std::net::Ipv6Addr> for Gid {
    fn from(addr: std::net::Ipv6Addr) -> Self {
        Self { raw: addr.octets() }
    }
}

impl fmt::Display for Gid {
    /// Formats the GID the way `ibv_devinfo -v` and `show_gids` do: as an IPv6 address, for
    /// example `fe80::5054:ff:fe12:3456` or `::ffff:192.0.2.1`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        std::net::Ipv6Addr::from(*self).fmt(f)
    }
}

impl fmt::Debug for Gid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Gid({self})")
    }
}

impl From<ffi::ibv_gid> for Gid {
    fn from(gid: ffi::ibv_gid) -> Self {
        Self {
            raw: unsafe { gid.raw },
        }
    }
}

impl From<Gid> for ffi::ibv_gid {
    fn from(mut gid: Gid) -> Self {
        *gid.as_mut()
    }
}

impl From<Gid> for [u8; 16] {
    fn from(gid: Gid) -> Self {
        gid.raw
    }
}

impl From<[u8; 16]> for Gid {
    fn from(raw: [u8; 16]) -> Self {
        Self { raw }
    }
}

impl AsRef<ffi::ibv_gid> for Gid {
    fn as_ref(&self) -> &ffi::ibv_gid {
        unsafe { &*self.raw.as_ptr().cast::<ffi::ibv_gid>() }
    }
}

impl AsMut<ffi::ibv_gid> for Gid {
    fn as_mut(&mut self) -> &mut ffi::ibv_gid {
        unsafe { &mut *self.raw.as_mut_ptr().cast::<ffi::ibv_gid>() }
    }
}

/// A Global identifier entry for ibv.
///
/// This struct acts as a rust wrapper for `ffi::ibv_gid_entry`. We use it instead of
/// `ffi::ibv_gid_entry` because `ffi::ibv_gid` is wrapped by `Gid`.
#[derive(Debug, Clone)]
pub struct GidEntry {
    /// The GID entry.
    pub gid: Gid,
    /// The GID table index of this entry.
    pub gid_index: u32,
    /// The port number that this GID belongs to.
    pub port_num: u32,
    /// enum ibv_gid_type, can be one of IBV_GID_TYPE_IB, IBV_GID_TYPE_ROCE_V1 or IBV_GID_TYPE_ROCE_V2.
    pub gid_type: ibv_gid_type,
    /// The interface index of the net device associated with this GID.
    ///
    /// It is 0 if there is no net device associated with it.
    pub ndev_ifindex: u32,
}

impl From<ffi::ibv_gid_entry> for GidEntry {
    fn from(gid_entry: ffi::ibv_gid_entry) -> Self {
        Self {
            gid: gid_entry.gid.into(),
            gid_index: gid_entry.gid_index,
            port_num: gid_entry.port_num,
            gid_type: match gid_entry.gid_type {
                0 => ibv_gid_type::IBV_GID_TYPE_IB,
                1 => ibv_gid_type::IBV_GID_TYPE_ROCE_V1,
                2 => ibv_gid_type::IBV_GID_TYPE_ROCE_V2,
                x => panic!("unknown ibv_gid_type: {x}"),
            },
            ndev_ifindex: gid_entry.ndev_ifindex,
        }
    }
}

impl GidEntry {
    /// The name of the network device associated with this GID, if any.
    ///
    /// Resolves [`ndev_ifindex`](Self::ndev_ifindex) with `if_indextoname`. Returns `None` when
    /// there is no associated net device (the index is 0) or the name cannot be resolved.
    pub fn netdev_name(&self) -> Option<String> {
        if self.ndev_ifindex == 0 {
            return None;
        }
        let mut buf = [0 as std::os::raw::c_char; nix::libc::IF_NAMESIZE];
        // SAFETY: `buf` is `IF_NAMESIZE` bytes, the size `if_indextoname` requires.
        let ret = unsafe { nix::libc::if_indextoname(self.ndev_ifindex, buf.as_mut_ptr()) };
        if ret.is_null() {
            return None;
        }
        // SAFETY: on success `if_indextoname` wrote a NUL-terminated name into `buf`.
        let name = unsafe { CStr::from_ptr(buf.as_ptr()) };
        name.to_str().ok().map(String::from)
    }
}

#[cfg(all(test, feature = "serde"))]
mod test_serde {
    use crate::QueuePairEndpoint;
    #[test]
    fn encode_decode() {
        let qpe_default = QueuePairEndpoint {
            num: 72,
            lid: 9,
            gid: Some(Default::default()),
        };

        let mut qpe = qpe_default;
        qpe.gid.as_mut().unwrap().raw =
            unsafe { std::mem::transmute::<[u64; 2], [u8; 16]>([87_u64.to_be(), 192_u64.to_be()]) };
        let encoded = bincode::serialize(&qpe).unwrap();

        let decoded: QueuePairEndpoint = bincode::deserialize(&encoded).unwrap();
        assert_eq!(decoded.gid.unwrap().subnet_prefix(), 87);
        assert_eq!(decoded.gid.unwrap().interface_id(), 192);
        assert_eq!(qpe, decoded);
        assert_ne!(qpe, qpe_default);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn gid_array_conversion() {
        let arr = [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88,
        ];
        let arr2: [u8; 16] = Gid::from(arr).into();
        assert_eq!(arr, arr2);
    }

    #[test]
    fn gid_ipv6_conversion_and_display() {
        let addr: std::net::Ipv6Addr = "fe80::5054:ff:fe12:3456".parse().unwrap();
        let gid = Gid::from(addr);
        assert_eq!(std::net::Ipv6Addr::from(gid), addr);
        assert_eq!(gid.to_string(), "fe80::5054:ff:fe12:3456");
        assert_eq!(format!("{gid:?}"), "Gid(fe80::5054:ff:fe12:3456)");
        assert!(!gid.is_ipv4_mapped());
    }

    #[test]
    fn gid_ipv4_mapped() {
        let gid = Gid::from("::ffff:192.0.2.1".parse::<std::net::Ipv6Addr>().unwrap());
        assert!(gid.is_ipv4_mapped());
        assert_eq!(gid.to_string(), "::ffff:192.0.2.1");
        assert_eq!(gid.subnet_prefix(), 0);
        assert_eq!(gid.interface_id() >> 32, 0xffff);
    }
}
