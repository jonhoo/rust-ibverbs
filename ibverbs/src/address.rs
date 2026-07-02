use std::convert::TryInto;
use std::ffi::CStr;
use std::fmt;
use std::io;
use std::sync::Arc;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::pd::ProtectionDomainInner;

#[cfg(doc)]
use crate::{Context, ProtectionDomain, QueuePairBuilder};

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

/// The type of a GID table entry, deciding how packets sent from it are framed and routed.
/// Carried by [`GidEntry::gid_type`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum GidType {
    /// An InfiniBand GID.
    Ib,
    /// A RoCE v1 GID (Ethernet framing, not routable across IP subnets).
    RoceV1,
    /// A RoCE v2 GID (UDP/IP framing, routable; mirrors an IP address of the interface).
    RoceV2,
    /// A value this crate does not recognize.
    Unknown(u32),
}

impl GidType {
    /// Decode the raw `ibv_gid_entry.gid_type` value.
    fn from_raw(gid_type: u32) -> Self {
        match gid_type {
            0 => GidType::Ib,
            1 => GidType::RoceV1,
            2 => GidType::RoceV2,
            other => GidType::Unknown(other),
        }
    }
}

impl fmt::Display for GidType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GidType::Ib => f.write_str("IB"),
            GidType::RoceV1 => f.write_str("RoCE v1"),
            GidType::RoceV2 => f.write_str("RoCE v2"),
            GidType::Unknown(raw) => write!(f, "unknown ({raw})"),
        }
    }
}

/// A GID table entry. Returned by [`Context::gid_table`].
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
    /// The type of the GID (InfiniBand, RoCE v1, or RoCE v2).
    pub gid_type: GidType,
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
            gid_type: GidType::from_raw(gid_entry.gid_type),
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
            qp_num: 72,
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

    #[test]
    fn gid_type_decodes_raw_values() {
        assert_eq!(GidType::from_raw(0), GidType::Ib);
        assert_eq!(GidType::from_raw(1), GidType::RoceV1);
        assert_eq!(GidType::from_raw(2), GidType::RoceV2);
        // An unrecognized value is preserved rather than panicking.
        assert_eq!(GidType::from_raw(7), GidType::Unknown(7));
        assert_eq!(GidType::RoceV2.to_string(), "RoCE v2");
        assert_eq!(GidType::Unknown(7).to_string(), "unknown (7)");
    }
}

/// Attributes describing how to reach a destination, used to build an [`AddressHandle`].
///
/// Describes a non-global (LID-only) route until a global route is set with
/// [`set_grh`](Self::set_grh), which RoCE and routed InfiniBand require.
#[derive(Clone)]
pub struct AddressHandleAttribute {
    pub(crate) attr: ffi::ibv_ah_attr,
}

impl AddressHandleAttribute {
    /// A new address-handle attribute routing from `port_num`, the local physical port through
    /// which the destination is reached (the port this handle routes from; numbered from 1).
    pub fn new(port_num: u8) -> Self {
        AddressHandleAttribute {
            attr: ffi::ibv_ah_attr {
                port_num,
                ..Default::default()
            },
        }
    }

    /// Set the destination LID (InfiniBand). Not used for RoCE / Ethernet link layers.
    pub fn set_dest_lid(&mut self, lid: u16) -> &mut Self {
        self.attr.dlid = lid;
        self
    }

    /// Set the service level.
    pub fn set_service_level(&mut self, service_level: u8) -> &mut Self {
        self.attr.sl = service_level;
        self
    }

    /// Set the global route (GRH), required for RoCE and routed InfiniBand.
    ///
    /// `dgid` is the destination GID, `sgid_index` indexes the *local* port's GID table to source
    /// from, and `hop_limit` is the IP hop limit (commonly `0xff`).
    pub fn set_grh(
        &mut self,
        dgid: Gid,
        sgid_index: u8,
        hop_limit: u8,
        traffic_class: u8,
    ) -> &mut Self {
        self.attr.is_global = 1;
        self.attr.grh.dgid = dgid.into();
        self.attr.grh.sgid_index = sgid_index;
        self.attr.grh.hop_limit = hop_limit;
        self.attr.grh.traffic_class = traffic_class;
        self
    }
}

/// A handle to a destination, used to address unreliable-datagram (UD) sends.
///
/// Created with [`ProtectionDomain::create_address_handle`] and passed by reference to each UD send;
/// a single UD queue pair can address many destinations with different handles (see
/// [`SendOp::to`](crate::SendOp::to)).
pub struct AddressHandle {
    // Keeps the protection domain (and so its context) alive until the handle is destroyed.
    pub(crate) _pd: Arc<ProtectionDomainInner>,
    pub(crate) ah: *mut ffi::ibv_ah,
}

unsafe impl Send for AddressHandle {}
unsafe impl Sync for AddressHandle {}

impl AddressHandle {
    #[inline]
    pub(crate) fn as_ptr(&self) -> *mut ffi::ibv_ah {
        self.ah
    }

    /// Returns the underlying `ibv_ah` pointer.
    ///
    /// This is an escape hatch for verbs this crate does not yet wrap. The pointer is owned by this
    /// [`AddressHandle`] and stays valid only while it is alive; do not destroy it.
    pub fn as_raw(&self) -> *mut ffi::ibv_ah {
        self.ah
    }
}

impl Drop for AddressHandle {
    fn drop(&mut self) {
        let errno = unsafe { ffi::ibv_destroy_ah(self.ah) };
        if errno != 0 {
            let e = io::Error::from_raw_os_error(errno);
            panic!("{e}");
        }
    }
}
