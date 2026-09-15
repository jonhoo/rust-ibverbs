use std::convert::TryInto;
use std::ffi::CStr;
use std::fmt;
use std::io;
use std::sync::Arc;

use crate::completion::WorkCompletion;
use crate::context::Context;
use crate::error::{Error, Result};
use crate::pd::ProtectionDomainInner;

#[cfg(doc)]
use crate::{ProtectionDomain, QueuePairBuilder};

/// A Global identifier (GID) for an RDMA device port.
///
/// This struct acts as a Rust wrapper for [`ffi::ibv_gid`]. We use it instead of
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
/// The `global` view is a convenience; the raw bytes are authoritative.
/// For continuity, the methods `subnet_prefix` and `interface_id` are provided.
/// These methods read the array as big endian, regardless of native CPU
/// endianness.
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

/// A GID is an IPv6 address by construction (RoCE GIDs mirror the interface's IP
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
    fn from(gid: Gid) -> Self {
        // By value: `Gid` is a byte array (align 1) while the `ibv_gid` union holds `__be64`s
        // (align 8), so a reference cast between them would be misaligned.
        ffi::ibv_gid { raw: gid.raw }
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
/// This struct acts as a Rust wrapper for `ffi::ibv_gid_entry`. We use it instead of
/// `ffi::ibv_gid_entry` because `ffi::ibv_gid` is wrapped by `Gid`.
#[derive(Debug, Clone)]
pub struct GidEntry {
    /// The GID entry.
    pub gid: Gid,
    /// The GID table index of this entry.
    pub gid_index: u32,
    /// The port number that this GID belongs to (numbered from 1, like every port in this crate).
    pub port_num: u8,
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
            // Port numbers are 8-bit on the wire; the C struct widens the field to u32 purely to
            // keep `ibv_gid_entry` free of implicit padding across the kernel boundary.
            port_num: gid_entry.port_num as u8,
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

#[cfg(test)]
mod test_wire {
    use crate::QueuePairEndpoint;

    #[test]
    fn endpoint_bytes_roundtrip() {
        let mut qpe = QueuePairEndpoint {
            qp_num: 72,
            lid: 9,
            gid: Some(Default::default()),
            psn: 0x00ab_cdef,
        };
        qpe.gid.as_mut().unwrap().raw =
            unsafe { std::mem::transmute::<[u64; 2], [u8; 16]>([87_u64.to_be(), 192_u64.to_be()]) };

        let encoded = qpe.to_bytes();
        let decoded = QueuePairEndpoint::from_bytes(&encoded).unwrap();
        assert_eq!(decoded.gid.unwrap().subnet_prefix(), 87);
        assert_eq!(decoded.gid.unwrap().interface_id(), 192);
        assert_eq!(qpe, decoded);
    }

    #[test]
    fn endpoint_bytes_roundtrip_without_gid() {
        let qpe = QueuePairEndpoint {
            qp_num: u32::MAX,
            lid: 0xbeef,
            gid: None,
            psn: u32::MAX - 1,
        };
        let encoded = qpe.to_bytes();
        assert_eq!(encoded[0], 0);
        assert_eq!(&encoded[23..27], &[0xff, 0xff, 0xff, 0xfe]);
        assert_eq!(QueuePairEndpoint::from_bytes(&encoded).unwrap(), qpe);
    }

    #[test]
    fn endpoint_bytes_rejects_unknown_flags() {
        let mut encoded = QueuePairEndpoint {
            qp_num: 1,
            lid: 1,
            gid: None,
            psn: 0,
        }
        .to_bytes();
        encoded[0] = 2;
        assert!(matches!(
            QueuePairEndpoint::from_bytes(&encoded),
            Err(crate::Error::MalformedWireFormat)
        ));
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
    /// A new address-handle attribute routing from `port_num`, the local port through which the
    /// destination is reached (numbered from 1).
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
    /// from, `hop_limit` is the IP hop limit (commonly `0xff`), and `traffic_class` is the GRH
    /// traffic class, with which the originator of the packets specifies the required delivery
    /// priority for handling them by the routers.
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

/// The 40-byte Global Routing Header (GRH) an unreliable-datagram receive places at the front of
/// its buffer when the completion reports one ([`WorkCompletion::has_grh`]): the routing
/// information the sender used, from which [`AddressHandleAttribute::from_wc`] derives the route
/// back. Decode it from the receive buffer with [`from_bytes`](Self::from_bytes).
///
/// On RoCE v2 over IPv4 the buffer holds not an IPv6-style header but, in the second half of
/// those 40 bytes, the packet's IPv4 header (Linux's `rdma_network_hdr` convention, which
/// libibverbs decodes the same way); the accessors translate it, mapping the addresses to
/// IPv4-mapped GIDs (`::ffff:a.b.c.d`).
///
/// [`WorkCompletion::has_grh`]: crate::WorkCompletion::has_grh
#[derive(Clone, Copy)]
pub struct Grh {
    bytes: [u8; Self::LEN],
}

// The header is exactly 40 bytes on the wire, so a receive buffer's prefix converts by copy.
const _: () = assert!(std::mem::size_of::<ffi::ibv_grh>() == Grh::LEN);

/// The form a received "GRH" takes.
enum GrhForm {
    /// An IPv6-style header (InfiniBand, RoCE v1, RoCE v2 over IPv6).
    Ipv6,
    /// RoCE v2 over IPv4: 20 reserved bytes, then the IPv4 header.
    Ipv4,
}

impl Grh {
    /// The header's length in bytes: what a UD receive buffer must reserve at its front.
    pub const LEN: usize = 40;

    /// Where the IPv4 header starts in the IPv4 form.
    const IPV4_OFFSET: usize = 20;

    /// Decode the header from the first 40 bytes of a receive buffer.
    pub fn from_bytes(bytes: &[u8; Self::LEN]) -> Self {
        Grh { bytes: *bytes }
    }

    /// The header as libibverbs' `ibv_grh`, for passing to the C routines that decode it (they
    /// apply the same IPv4 detection).
    pub(crate) fn raw(&self) -> ffi::ibv_grh {
        // The struct holds GIDs, whose union type wants 8-byte alignment that the byte array does
        // not promise, so copy it out unaligned.
        unsafe { std::ptr::read_unaligned(self.bytes.as_ptr().cast::<ffi::ibv_grh>()) }
    }

    /// Tell the two forms apart the way libibverbs does: an IPv6 version nibble means the IPv6
    /// form, unless the bytes also parse as a well-formed IPv4 header with a valid checksum, in
    /// which case the IPv4 form is the one the device wrote.
    fn form(&self) -> GrhForm {
        let ipv4 = &self.bytes[Self::IPV4_OFFSET..];
        if self.bytes[0] >> 4 != 6 {
            return GrhForm::Ipv4;
        }
        if ipv4[0] != 0x45 {
            // Not (version 4, header length 5), the only IPv4 header RoCE v2 carries.
            return GrhForm::Ipv6;
        }
        // The one's-complement sum of a valid IPv4 header, checksum included, is all ones.
        let sum = ipv4
            .chunks_exact(2)
            .map(|word| u32::from(u16::from_be_bytes([word[0], word[1]])))
            .sum::<u32>();
        let folded = (sum & 0xffff) + (sum >> 16);
        let folded = (folded & 0xffff) + (folded >> 16);
        if folded == 0xffff {
            GrhForm::Ipv4
        } else {
            GrhForm::Ipv6
        }
    }

    /// The IPv4-mapped GID (`::ffff:a.b.c.d`) of the IPv4 address at `at` in the header bytes.
    fn ipv4_mapped(&self, at: usize) -> Gid {
        let mut raw = [0u8; 16];
        raw[10] = 0xff;
        raw[11] = 0xff;
        raw[12..16].copy_from_slice(&self.bytes[at..at + 4]);
        Gid::from(raw)
    }

    fn gid_at(&self, at: usize) -> Gid {
        let raw: [u8; 16] = self.bytes[at..at + 16].try_into().expect("16 bytes");
        Gid::from(raw)
    }

    /// The sender's GID (the packet's source address): the destination of a reply.
    pub fn sgid(&self) -> Gid {
        match self.form() {
            GrhForm::Ipv6 => self.gid_at(8),
            GrhForm::Ipv4 => self.ipv4_mapped(Self::IPV4_OFFSET + 12),
        }
    }

    /// The GID the packet was addressed to (an entry of the receiving port's GID table).
    pub fn dgid(&self) -> Gid {
        match self.form() {
            GrhForm::Ipv6 => self.gid_at(24),
            GrhForm::Ipv4 => self.ipv4_mapped(Self::IPV4_OFFSET + 16),
        }
    }

    /// The hop limit (IPv4: the time to live) the packet arrived with.
    pub fn hop_limit(&self) -> u8 {
        match self.form() {
            GrhForm::Ipv6 => self.bytes[7],
            GrhForm::Ipv4 => self.bytes[Self::IPV4_OFFSET + 8],
        }
    }

    /// The traffic class (IPv4: the type-of-service byte) of the packet.
    pub fn traffic_class(&self) -> u8 {
        match self.form() {
            GrhForm::Ipv6 => ((self.version_tclass_flow() >> 20) & 0xff) as u8,
            GrhForm::Ipv4 => self.bytes[Self::IPV4_OFFSET + 1],
        }
    }

    /// The 20-bit flow label of the packet (0 for the IPv4 form, which has none).
    pub fn flow_label(&self) -> u32 {
        match self.form() {
            GrhForm::Ipv6 => self.version_tclass_flow() & 0x000f_ffff,
            GrhForm::Ipv4 => 0,
        }
    }

    /// The length in bytes of what follows the header, as the header declares it.
    pub fn payload_len(&self) -> u16 {
        match self.form() {
            GrhForm::Ipv6 => u16::from_be_bytes([self.bytes[4], self.bytes[5]]),
            GrhForm::Ipv4 => {
                let at = Self::IPV4_OFFSET + 2;
                u16::from_be_bytes([self.bytes[at], self.bytes[at + 1]]).saturating_sub(20)
            }
        }
    }

    /// The first word of the IPv6 form: version, traffic class, and flow label.
    fn version_tclass_flow(&self) -> u32 {
        u32::from_be_bytes([self.bytes[0], self.bytes[1], self.bytes[2], self.bytes[3]])
    }
}

impl fmt::Debug for Grh {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Grh")
            .field("sgid", &self.sgid())
            .field("dgid", &self.dgid())
            .field("hop_limit", &self.hop_limit())
            .field("traffic_class", &self.traffic_class())
            .field("flow_label", &self.flow_label())
            .field("payload_len", &self.payload_len())
            .finish()
    }
}

impl AddressHandleAttribute {
    /// The route back to the sender of a datagram, derived from its receive completion and the
    /// Global Routing Header it carried (`ibv_init_ah_from_wc`): what a UD server needs to answer
    /// a request without the peer telling it its address. Pass the result to
    /// [`ProtectionDomain::create_address_handle`].
    ///
    /// `wc` is the receive's completion in the classic form ([`CompletionQueue::poll_into`]), and
    /// `port_num` the local port the datagram arrived on, which the reply leaves through. `grh` is
    /// the header from the front of the receive buffer ([`Grh::from_bytes`]); it is required when
    /// the completion reports one ([`WcFlags::GRH`], always the case on RoCE) and not read
    /// otherwise (a LID-routed InfiniBand datagram carries none, and its route comes from the
    /// completion alone). The route is global exactly when a header was present, sourced from the
    /// local GID the datagram was addressed to.
    ///
    /// # Errors
    ///
    ///  - [`CreateAddressHandle`](Error::CreateAddressHandle): the completion reports a header
    ///    but none was given, or the header's destination GID is not in the port's GID table.
    ///
    /// [`CompletionQueue::poll_into`]: crate::CompletionQueue::poll_into
    /// [`WcFlags::GRH`]: crate::WcFlags::GRH
    pub fn from_wc(
        context: &Context,
        port_num: u8,
        wc: &ffi::ibv_wc,
        grh: Option<&Grh>,
    ) -> Result<Self> {
        let mut wc = *wc;
        let has_grh = (wc.wc_flags & ffi::ibv_wc_flags::IBV_WC_GRH).0 != 0;
        // `ibv_init_ah_from_wc` reads the header's first word (the flow label) before it checks
        // `IBV_WC_GRH`, so it must always be handed a header: the caller's when the completion
        // reports one, and an all-zero one (no flow label) when it does not.
        let mut grh = match (has_grh, grh) {
            (true, Some(grh)) => grh.raw(),
            (true, None) => {
                return Err(Error::CreateAddressHandle(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "the completion reports a GRH, but none was given to derive the route from",
                )));
            }
            (false, _) => ffi::ibv_grh::default(),
        };
        let mut attr = ffi::ibv_ah_attr::default();
        let ret = unsafe {
            ffi::ibv_init_ah_from_wc(context.as_raw(), port_num, &mut wc, &mut grh, &mut attr)
        };
        if ret != 0 {
            // libibverbs reports the one failure it can hit — the header's destination GID not
            // being in the port's table — as a bare -1, without an errno.
            return Err(Error::CreateAddressHandle(io::Error::new(
                io::ErrorKind::NotFound,
                "the GRH's destination GID is not in the local port's GID table",
            )));
        }
        Ok(AddressHandleAttribute { attr })
    }

    /// As [`from_wc`](Self::from_wc), for a completion read through the extended interface
    /// ([`CompletionQueue::poll`]).
    ///
    /// On RoCE the route comes from the header alone. On InfiniBand it also needs the source LID,
    /// service level, and path bits, which the completion only carries if its queue requested
    /// [`WcFields::SLID`], [`WcFields::SL`], and [`WcFields::DLID_PATH_BITS`]; fields the queue
    /// did not request are taken as zero.
    ///
    /// [`CompletionQueue::poll`]: crate::CompletionQueue::poll
    /// [`WcFields::SLID`]: crate::WcFields::SLID
    /// [`WcFields::SL`]: crate::WcFields::SL
    /// [`WcFields::DLID_PATH_BITS`]: crate::WcFields::DLID_PATH_BITS
    pub fn from_completion(
        context: &Context,
        port_num: u8,
        wc: &WorkCompletion<'_>,
        grh: Option<&Grh>,
    ) -> Result<Self> {
        Self::from_wc(context, port_num, &wc.addressing(), grh)
    }
}

/// A handle to a destination, used to address unreliable-datagram (UD) sends.
///
/// Created with [`ProtectionDomain::create_address_handle`] and passed by reference to each UD send;
/// a single UD queue pair can address many destinations with different handles (see
/// [`SendBatch::to`](crate::SendBatch::to)).
#[must_use = "the address handle is destroyed when dropped"]
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
            panic!("ibv_destroy_ah failed: {e}");
        }
    }
}

#[cfg(test)]
mod test_grh {
    use super::*;

    /// An IPv4 header for 10.1.0.134 -> 10.1.0.134, UDP, TTL 64, with a valid checksum, in the
    /// second half of the 40-byte area, as Soft-RoCE hands a RoCE v2 IPv4 datagram's header up.
    fn ipv4_form() -> Grh {
        let mut bytes = [0u8; Grh::LEN];
        let header: [u8; 20] = [
            0x45, 0x08, 0x00, 0x3c, 0x92, 0x0b, 0x40, 0x00, 0x40, 0x11, 0, 0, 10, 1, 0, 134, 10, 1,
            0, 134,
        ];
        bytes[20..].copy_from_slice(&header);
        // Fill in the checksum so the form detection accepts it.
        let sum: u32 = bytes[20..]
            .chunks_exact(2)
            .map(|w| u32::from(u16::from_be_bytes([w[0], w[1]])))
            .sum();
        let folded = (sum & 0xffff) + (sum >> 16);
        let checksum = !(((folded & 0xffff) + (folded >> 16)) as u16);
        bytes[30..32].copy_from_slice(&checksum.to_be_bytes());
        Grh::from_bytes(&bytes)
    }

    #[test]
    fn ipv4_header_maps_to_ipv4_mapped_gids() {
        let grh = ipv4_form();
        let expected: Gid = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 10, 1, 0, 134].into();
        assert_eq!(grh.sgid(), expected);
        assert_eq!(grh.dgid(), expected);
        assert_eq!(grh.hop_limit(), 64);
        assert_eq!(grh.traffic_class(), 0x08);
        assert_eq!(grh.flow_label(), 0);
        assert_eq!(grh.payload_len(), 0x3c - 20);
    }

    #[test]
    fn ipv6_header_reads_the_grh_fields() {
        let mut bytes = [0u8; Grh::LEN];
        // Version 6, traffic class 0x1c, flow label 0xabcde; payload 100; hop limit 63.
        bytes[..4].copy_from_slice(&(0x6000_0000 | (0x1c << 20) | 0xabcde_u32).to_be_bytes());
        bytes[4..6].copy_from_slice(&100_u16.to_be_bytes());
        bytes[7] = 63;
        bytes[8..24].copy_from_slice(&[0xfe, 0x80, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8]);
        bytes[24..40].copy_from_slice(&[0xfe, 0x80, 0, 0, 0, 0, 0, 0, 8, 7, 6, 5, 4, 3, 2, 1]);
        let grh = Grh::from_bytes(&bytes);
        assert_eq!(grh.sgid().to_string(), "fe80::102:304:506:708");
        assert_eq!(grh.dgid().to_string(), "fe80::807:605:403:201");
        assert_eq!(grh.hop_limit(), 63);
        assert_eq!(grh.traffic_class(), 0x1c);
        assert_eq!(grh.flow_label(), 0xabcde);
        assert_eq!(grh.payload_len(), 100);
    }
}
