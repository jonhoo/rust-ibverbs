use std::convert::TryInto;
use std::io;
use std::ops::{Deref, DerefMut, RangeBounds};
use std::sync::Arc;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::pd::ProtectionDomainInner;

#[cfg(doc)]
use crate::QueuePair;

pub(crate) struct MemoryRegionInner {
    pub(crate) _pd: Arc<ProtectionDomainInner>,
    pub(crate) mr: *mut ffi::ibv_mr,
}

unsafe impl Sync for MemoryRegionInner {}
unsafe impl Send for MemoryRegionInner {}

impl Drop for MemoryRegionInner {
    fn drop(&mut self) {
        let errno = unsafe { ffi::ibv_dereg_mr(self.mr) };
        if errno != 0 {
            let e = io::Error::from_raw_os_error(errno);
            panic!("{e}");
        }
    }
}

/// A region of memory registered for use with RDMA.
pub struct MemoryRegion<O> {
    pub(crate) inner: MemoryRegionInner,
    pub(crate) owner: O,
}

impl<O> MemoryRegion<O> {
    /// Get the remote authentication key used to allow direct remote access to this memory region.
    pub fn rkey(&self) -> RemoteKey {
        RemoteKey {
            key: unsafe { &*self.inner.mr }.rkey,
        }
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

    /// Remote region.
    pub fn remote(&self) -> RemoteMemorySlice {
        RemoteMemorySlice {
            addr: unsafe { *self.inner.mr }.addr as u64,
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
        let (addr, length) = calc_addr_len(
            bounds,
            unsafe { *self.inner.mr }.addr as u64,
            unsafe { *self.inner.mr }.length,
        );
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
    /// the safety contract on [`QueuePair::post_receive`]).
    pub fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.owner
    }
}

/// Local memory slice.
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

/// Remote memory region.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct RemoteMemorySlice {
    /// Memory address of the registered region (might have been offset by slicing).
    pub addr: u64,
    /// Length of the registered memory region (might have been offset by slicing).
    pub len: usize,
    /// Remote key for accessing this memory region.
    pub rkey: u32,
}

impl RemoteMemorySlice {
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

/// A key that authorizes direct memory access to a memory region.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct RemoteKey {
    /// The actual key value.
    pub key: u32,
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

#[cfg(all(test, feature = "serde"))]
mod test_serde {
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
}
