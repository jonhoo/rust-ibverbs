use std::convert::TryInto;
use std::io;
use std::ops::{Deref, DerefMut, RangeBounds};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::pd::ProtectionDomainInner;

#[cfg(doc)]
use crate::QueuePair;

flags_newtype! {
    /// Access permissions for a memory region or queue pair (the `IBV_ACCESS_*` bits).
    ///
    /// Combine flags with `|`, or start from the [`PERMISSIVE`](Self::PERMISSIVE) bundle. Local
    /// read access is always enabled and has no flag.
    pub struct AccessFlags(ffi::ibv_access_flags) {
        /// Local write access (required to receive into the region).
        LOCAL_WRITE = IBV_ACCESS_LOCAL_WRITE;
        /// Remote peers may RDMA-write into the region.
        REMOTE_WRITE = IBV_ACCESS_REMOTE_WRITE;
        /// Remote peers may RDMA-read from the region.
        REMOTE_READ = IBV_ACCESS_REMOTE_READ;
        /// Remote peers may target the region with atomic operations.
        REMOTE_ATOMIC = IBV_ACCESS_REMOTE_ATOMIC;
        /// A memory window may be bound to the region.
        MW_BIND = IBV_ACCESS_MW_BIND;
        /// Remote access uses zero-based virtual addresses.
        ZERO_BASED = IBV_ACCESS_ZERO_BASED;
        /// Register the region for on-demand paging (no pinning; pages fault in on access).
        ON_DEMAND = IBV_ACCESS_ON_DEMAND;
        /// Back the region with huge pages.
        HUGETLB = IBV_ACCESS_HUGETLB;
        /// Remote peers may issue a FLUSH to global visibility on the region.
        FLUSH_GLOBAL = IBV_ACCESS_FLUSH_GLOBAL;
        /// Remote peers may issue a FLUSH to persistence on the region.
        FLUSH_PERSISTENT = IBV_ACCESS_FLUSH_PERSISTENT;
        /// Allow the device to relax PCIe write ordering for higher throughput.
        RELAXED_ORDERING = IBV_ACCESS_RELAXED_ORDERING;
    }
}

impl AccessFlags {
    /// Local write plus all remote data access (remote read, remote write, and remote atomics),
    /// with relaxed ordering: a convenient bundle for buffers both sides fully trust. Narrow it
    /// when the region does not need to be remotely writable.
    pub const PERMISSIVE: AccessFlags = AccessFlags(
        AccessFlags::LOCAL_WRITE.0
            | AccessFlags::REMOTE_WRITE.0
            | AccessFlags::REMOTE_READ.0
            | AccessFlags::REMOTE_ATOMIC.0
            | AccessFlags::RELAXED_ORDERING.0,
    );
}

pub(crate) struct MemoryRegionInner {
    pub(crate) _pd: Arc<ProtectionDomainInner>,
    pub(crate) mr: *mut ffi::ibv_mr,
    pub(crate) addr: u64,
}

unsafe impl Sync for MemoryRegionInner {}
unsafe impl Send for MemoryRegionInner {}

impl Drop for MemoryRegionInner {
    fn drop(&mut self) {
        let errno = unsafe { ffi::ibv_dereg_mr(self.mr) };
        if errno != 0 {
            let e = io::Error::from_raw_os_error(errno);
            panic!("ibv_dereg_mr failed: {e}");
        }
    }
}

/// A region of memory registered for use with RDMA.
///
/// Created by [`ProtectionDomain::allocate`](crate::ProtectionDomain::allocate) (the returned
/// region owns its buffer), or by [`register_from_raw`](crate::ProtectionDomain::register_from_raw) /
/// [`register_dmabuf`](crate::ProtectionDomain::register_dmabuf) for memory managed elsewhere.
#[must_use = "the memory region is deregistered when dropped"]
pub struct MemoryRegion<O> {
    pub(crate) inner: MemoryRegionInner,
    pub(crate) owner: O,
}

impl<O> MemoryRegion<O> {
    /// Get the remote key of this memory region: the key peers use to access it directly, usually
    /// communicated as part of [`remote`](Self::remote).
    pub fn rkey(&self) -> u32 {
        unsafe { &*self.inner.mr }.rkey
    }

    /// Get the local key of this memory region (the one [`slice`](Self::slice) stamps on every
    /// scatter/gather entry).
    pub fn lkey(&self) -> u32 {
        unsafe { &*self.inner.mr }.lkey
    }

    /// Returns the underlying `ibv_mr` pointer.
    ///
    /// This is an escape hatch for verbs this crate does not yet wrap. The pointer is owned by this
    /// [`MemoryRegion`] and stays valid only while it is alive; do not deregister it.
    pub fn as_raw(&self) -> *mut ffi::ibv_mr {
        self.inner.mr
    }

    /// The remote handle (address, length, and rkey) covering this whole region, for a peer's
    /// one-sided access.
    pub fn remote(&self) -> RemoteMemorySlice {
        RemoteMemorySlice {
            addr: self.inner.addr,
            len: unsafe { *self.inner.mr }.length,
            rkey: unsafe { *self.inner.mr }.rkey,
        }
    }

    /// Deregister the memory region and return the buffer that backed it.
    pub fn into_inner(self) -> O {
        self.owner
    }

    /// Make a subslice of this memory region, to post as part of a work request.
    ///
    /// The slice carries the region's local key, so it stays postable on its own; but it borrows
    /// nothing, so it is on you not to use it past the region's deregistration (see the safety
    /// contracts on the post methods).
    ///
    /// # Panics
    ///
    /// Panics if `bounds` is empty or falls outside the region.
    pub fn slice(&self, bounds: impl RangeBounds<usize>) -> LocalMemorySlice {
        let (addr, length) =
            calc_addr_len(bounds, self.inner.addr, unsafe { *self.inner.mr }.length);
        let sge = ffi::ibv_sge {
            addr,
            length: length.try_into().unwrap(),
            lkey: unsafe { *self.inner.mr }.lkey,
        };
        LocalMemorySlice { _sge: sge }
    }
}

impl<O: Deref<Target = [u8]>> MemoryRegion<O> {
    /// The registered bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.owner
    }
}

impl<O: DerefMut<Target = [u8]>> MemoryRegion<O> {
    /// The registered bytes, mutably.
    ///
    /// The length is fixed at registration; the buffer can be written but not resized.
    ///
    /// Note that the device also writes to the buffer: while a receive (or an incoming RDMA write)
    /// targeting this region is outstanding, reading or writing the targeted bytes races with the
    /// device. Only touch those bytes after the corresponding work completion has been reaped (see
    /// the safety contract on [`QueuePair::post_recv`]).
    pub fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.owner
    }
}

/// Local memory slice, postable as a scatter/gather entry of a work request.
///
/// Created by [`MemoryRegion::slice`].
#[derive(Debug, Default, Copy, Clone)]
#[repr(transparent)]
pub struct LocalMemorySlice {
    _sge: ffi::ibv_sge,
}

/// An escape hatch for posting memory registered outside this crate (for example through
/// [`ffi::ibv_reg_mr`] with flags the safe API does not cover): a scatter/gather entry converts
/// straight into a postable slice. The post methods' safety contracts (valid registration,
/// no concurrent device access) then cover the converted slice.
impl From<ffi::ibv_sge> for LocalMemorySlice {
    fn from(sge: ffi::ibv_sge) -> Self {
        LocalMemorySlice { _sge: sge }
    }
}

impl From<LocalMemorySlice> for ffi::ibv_sge {
    fn from(slice: LocalMemorySlice) -> Self {
        slice._sge
    }
}

impl LocalMemorySlice {
    /// Get the address of the local memory slice.
    pub fn addr(&self) -> u64 {
        self._sge.addr
    }

    /// Get the length of the local memory slice.
    pub fn len(&self) -> usize {
        self._sge.length as usize
    }

    /// Returns `true` if the slice has length zero.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the local key of the local memory slice.
    pub fn lkey(&self) -> u32 {
        self._sge.lkey
    }

    /// Make a subslice of this slice.
    ///
    /// # Panics
    ///
    /// Panics if `bounds` is empty or falls outside this slice.
    pub fn slice(&self, bounds: impl RangeBounds<usize>) -> Self {
        let (addr, len) = calc_addr_len(bounds, self.addr(), self.len());
        Self {
            _sge: ffi::ibv_sge {
                addr,
                length: len.try_into().unwrap(),
                lkey: self.lkey(),
            },
        }
    }
}

/// Remote memory region, targetable by one-sided operations (RDMA read/write and atomics).
///
/// Created by [`MemoryRegion::remote`], and typically serialized to the peer that initiates the
/// access through [`to_bytes`](Self::to_bytes)/[`from_bytes`](Self::from_bytes).
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct RemoteMemorySlice {
    /// Memory address of the registered region (might have been offset by slicing).
    pub addr: u64,
    /// Length of the registered memory region (might have been narrowed by slicing).
    pub len: usize,
    /// Remote key for accessing this memory region.
    pub rkey: u32,
}

impl RemoteMemorySlice {
    /// The length of the wire encoding produced by [`to_bytes`](Self::to_bytes).
    pub const WIRE_LEN: usize = 20;

    /// Encodes this slice in the crate's stable wire format, for handing to the peer that will
    /// access it remotely.
    ///
    /// The layout, in network byte order: the address (8 bytes), the length (8 bytes), and the
    /// remote key (4 bytes).
    pub fn to_bytes(&self) -> [u8; Self::WIRE_LEN] {
        let mut out = [0u8; Self::WIRE_LEN];
        out[0..8].copy_from_slice(&self.addr.to_be_bytes());
        out[8..16].copy_from_slice(&(self.len as u64).to_be_bytes());
        out[16..20].copy_from_slice(&self.rkey.to_be_bytes());
        out
    }

    /// Decodes a slice from the wire format produced by [`to_bytes`](Self::to_bytes).
    ///
    /// # Errors
    ///
    ///  - [`MalformedWireFormat`](Error::MalformedWireFormat): the encoded length does not fit
    ///    this platform's `usize`.
    pub fn from_bytes(bytes: &[u8; Self::WIRE_LEN]) -> Result<Self> {
        let len = u64::from_be_bytes(bytes[8..16].try_into().expect("slice length is fixed"));
        Ok(RemoteMemorySlice {
            addr: u64::from_be_bytes(bytes[0..8].try_into().expect("slice length is fixed")),
            len: usize::try_from(len).map_err(|_| Error::MalformedWireFormat)?,
            rkey: u32::from_be_bytes(bytes[16..20].try_into().expect("slice length is fixed")),
        })
    }

    /// Make a subslice of this slice.
    ///
    /// # Panics
    ///
    /// Panics if `bounds` is empty or falls outside this slice.
    pub fn slice(&self, bounds: impl RangeBounds<usize>) -> Self {
        let (addr, len) = calc_addr_len(bounds, self.addr, self.len);
        Self {
            addr,
            len,
            rkey: self.rkey,
        }
    }
}

fn calc_addr_len(bounds: impl RangeBounds<usize>, addr: u64, bytes_len: usize) -> (u64, usize) {
    let start = match bounds.start_bound() {
        std::ops::Bound::Included(i) => *i,
        std::ops::Bound::Excluded(i) => *i + 1,
        std::ops::Bound::Unbounded => 0,
    };
    let end = match bounds.end_bound() {
        std::ops::Bound::Included(i) => *i + 1,
        std::ops::Bound::Excluded(i) => *i,
        std::ops::Bound::Unbounded => bytes_len,
    };
    assert!(start < end);
    assert!(start <= bytes_len);
    assert!(end <= bytes_len);
    let addr = addr + start as u64;
    let len = end - start;
    (addr, len)
}

#[cfg(test)]
mod test_layout {
    use super::*;
    #[test]
    fn test_local_memory_slice_sge_memory_layout() {
        assert_eq!(
            std::mem::size_of::<LocalMemorySlice>(),
            std::mem::size_of::<ffi::ibv_sge>()
        );
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn remote_memory_slice_bytes_roundtrip() {
        let remote = RemoteMemorySlice {
            addr: 0xdead_beef_dead_beef,
            len: 4096,
            rkey: 0x1234_5678,
        };
        let encoded = remote.to_bytes();
        assert_eq!(RemoteMemorySlice::from_bytes(&encoded).unwrap(), remote);
    }

    #[test]
    fn local_memory_slice_sge_roundtrip() {
        let sge = ffi::ibv_sge {
            addr: 0xdead_beef,
            length: 64,
            lkey: 42,
        };
        let slice = LocalMemorySlice::from(sge);
        assert_eq!(slice.addr(), 0xdead_beef);
        assert_eq!(slice.len(), 64);
        assert_eq!(slice.lkey(), 42);
        let back = ffi::ibv_sge::from(slice);
        assert_eq!(back.addr, sge.addr);
        assert_eq!(back.length, sge.length);
        assert_eq!(back.lkey, sge.lkey);
    }

    #[test]
    fn access_flags_roundtrip() {
        let flags = AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_READ;
        let raw: ffi::ibv_access_flags = flags.into();
        assert_eq!(
            raw,
            ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                | ffi::ibv_access_flags::IBV_ACCESS_REMOTE_READ
        );
        assert_eq!(AccessFlags::from(raw), flags);
        assert_eq!(
            AccessFlags::from(ffi::ibv_access_flags::IBV_ACCESS_RELAXED_ORDERING),
            AccessFlags::RELAXED_ORDERING
        );
    }

    #[test]
    fn access_flags_bit_ops() {
        let mut flags = AccessFlags::empty();
        assert!(!flags.contains(AccessFlags::LOCAL_WRITE));
        flags |= AccessFlags::LOCAL_WRITE;
        flags |= AccessFlags::REMOTE_WRITE;
        assert!(flags.contains(AccessFlags::LOCAL_WRITE));
        assert!(flags.contains(AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE));
        assert!(!flags.contains(AccessFlags::REMOTE_ATOMIC));
        assert_eq!(flags & AccessFlags::LOCAL_WRITE, AccessFlags::LOCAL_WRITE);
        assert_eq!(flags & AccessFlags::REMOTE_ATOMIC, AccessFlags::empty());
        assert!(
            AccessFlags::PERMISSIVE.contains(AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_ATOMIC)
        );
        assert!(!AccessFlags::PERMISSIVE.contains(AccessFlags::ON_DEMAND));
    }

    #[test]
    fn access_flags_debug_lists_names() {
        assert_eq!(format!("{:?}", AccessFlags::empty()), "AccessFlags(0)");
        assert_eq!(
            format!("{:?}", AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_READ),
            "AccessFlags(LOCAL_WRITE | REMOTE_READ)"
        );
        // Unknown bits are kept visible as a hex remainder.
        assert_eq!(
            format!("{:?}", AccessFlags(AccessFlags::LOCAL_WRITE.0 | 1 << 30)),
            "AccessFlags(LOCAL_WRITE | 0x40000000)"
        );
    }
}
