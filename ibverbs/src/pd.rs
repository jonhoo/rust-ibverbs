use std::io;
use std::os::raw::c_void;
use std::ptr;
use std::sync::Arc;

use crate::address::{AddressHandle, AddressHandleAttribute};
use crate::completion::CompletionQueue;
use crate::context::ContextInner;
use crate::error::{Error, Result};
use crate::mr::{AccessFlags, LocalMemorySlice, MemoryRegion, MemoryRegionInner};
use crate::qp::{QueuePairBuilder, QueuePairType};

use crate::srq::{SharedReceiveQueue, SharedReceiveQueueInner};

#[cfg(doc)]
use crate::Context;

/// Advice for [`ProtectionDomain::advise_mr`] (the `IBV_ADVISE_MR_ADVICE_*` values).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MrAdvice {
    /// Prefetch the pages for read access (a best-effort hint; the ranges must allow local read).
    Prefetch,
    /// Prefetch the pages for write access (a best-effort hint; the ranges must allow local
    /// write).
    PrefetchWrite,
    /// Prefetch without faulting: pre-load what is already resident, never a page fault.
    PrefetchNoFault,
}

impl From<ffi::ib_uverbs_advise_mr_advice> for MrAdvice {
    fn from(advice: ffi::ib_uverbs_advise_mr_advice) -> Self {
        use ffi::ib_uverbs_advise_mr_advice::*;
        match advice {
            IB_UVERBS_ADVISE_MR_ADVICE_PREFETCH => MrAdvice::Prefetch,
            IB_UVERBS_ADVISE_MR_ADVICE_PREFETCH_WRITE => MrAdvice::PrefetchWrite,
            IB_UVERBS_ADVISE_MR_ADVICE_PREFETCH_NO_FAULT => MrAdvice::PrefetchNoFault,
        }
    }
}

impl From<MrAdvice> for ffi::ib_uverbs_advise_mr_advice {
    fn from(advice: MrAdvice) -> Self {
        use ffi::ib_uverbs_advise_mr_advice::*;
        match advice {
            MrAdvice::Prefetch => IB_UVERBS_ADVISE_MR_ADVICE_PREFETCH,
            MrAdvice::PrefetchWrite => IB_UVERBS_ADVISE_MR_ADVICE_PREFETCH_WRITE,
            MrAdvice::PrefetchNoFault => IB_UVERBS_ADVISE_MR_ADVICE_PREFETCH_NO_FAULT,
        }
    }
}

/// Flags for [`ProtectionDomain::advise_mr`] (the `IBV_ADVISE_MR_FLAG_*` values).
#[derive(Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct MrAdviseFlags(pub(crate) u32);

impl MrAdviseFlags {
    /// The advice may be flushed by other verbs (rather than being sticky).
    pub const FLUSHABLE: MrAdviseFlags =
        MrAdviseFlags(ffi::ib_uverbs_advise_mr_flag::IB_UVERBS_ADVISE_MR_FLAG_FLUSH as u32);

    /// No flags set.
    #[must_use]
    pub const fn empty() -> Self {
        MrAdviseFlags(0)
    }

    /// Whether every flag set in `other` is also set in `self`.
    #[must_use]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

impl std::ops::BitOr for MrAdviseFlags {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        MrAdviseFlags(self.0 | rhs.0)
    }
}

impl std::ops::BitOrAssign for MrAdviseFlags {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

impl std::ops::BitAnd for MrAdviseFlags {
    type Output = Self;
    fn bitand(self, rhs: Self) -> Self {
        MrAdviseFlags(self.0 & rhs.0)
    }
}

impl std::fmt::Debug for MrAdviseFlags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MrAdviseFlags(")?;
        let mut rest = self.0;
        let mut first = true;
        if rest & MrAdviseFlags::FLUSHABLE.0 == MrAdviseFlags::FLUSHABLE.0 {
            f.write_str("FLUSHABLE")?;
            first = false;
            rest &= !MrAdviseFlags::FLUSHABLE.0;
        }
        if rest != 0 {
            if !first {
                f.write_str(" | ")?;
            }
            write!(f, "{rest:#x}")?;
            first = false;
        }
        if first {
            f.write_str("0")?;
        }
        f.write_str(")")
    }
}

impl From<ffi::ib_uverbs_advise_mr_flag> for MrAdviseFlags {
    fn from(flag: ffi::ib_uverbs_advise_mr_flag) -> Self {
        MrAdviseFlags(flag as u32)
    }
}

impl From<MrAdviseFlags> for u32 {
    fn from(flags: MrAdviseFlags) -> Self {
        flags.0
    }
}

pub(crate) struct ProtectionDomainInner {
    pub(crate) ctx: Arc<ContextInner>,
    pub(crate) pd: *mut ffi::ibv_pd,
}

impl Drop for ProtectionDomainInner {
    fn drop(&mut self) {
        let errno = unsafe { ffi::ibv_dealloc_pd(self.pd) };
        if errno != 0 {
            let e = io::Error::from_raw_os_error(errno);
            panic!("{e}");
        }
    }
}

unsafe impl Sync for ProtectionDomainInner {}
unsafe impl Send for ProtectionDomainInner {}

/// A protection domain for a device's context. Created by [`Context::alloc_pd`].
#[must_use]
#[derive(Clone)]
pub struct ProtectionDomain {
    pub(crate) inner: Arc<ProtectionDomainInner>,
}

impl ProtectionDomain {
    /// Returns the underlying `ibv_pd` pointer.
    ///
    /// This is an escape hatch for verbs this crate does not yet wrap. The pointer is owned by this
    /// [`ProtectionDomain`] and stays valid only while it (or a resource derived from it) is alive;
    /// do not deallocate it.
    pub fn as_raw(&self) -> *mut ffi::ibv_pd {
        self.inner.pd
    }

    /// Create an [`AddressHandle`] for the destination described by `attr`.
    ///
    /// Address handles are used to address unreliable-datagram (UD) sends. One handle can be reused
    /// across many sends, and a single UD queue pair can hold handles to many destinations.
    ///
    /// # Errors
    ///
    ///  - [`CreateAddressHandle`](Error::CreateAddressHandle): `ibv_create_ah` failed (`EINVAL`
    ///    for an invalid value in `attr`, `ENOMEM` when out of resources).
    pub fn create_address_handle(&self, attr: &AddressHandleAttribute) -> Result<AddressHandle> {
        let mut ah_attr = attr.attr;
        let ah = unsafe { ffi::ibv_create_ah(self.inner.pd, &mut ah_attr as *mut _) };
        if ah.is_null() {
            Err(Error::CreateAddressHandle(io::Error::last_os_error()))
        } else {
            Ok(AddressHandle {
                _pd: self.inner.clone(),
                ah,
            })
        }
    }

    /// Give advice to the kernel about an address range in memory regions registered under this
    /// protection domain (`ibv_advise_mr`).
    ///
    /// This is mainly useful with on-demand-paging (ODP) memory regions: for example, prefetching
    /// pages with [`MrAdvice::Prefetch`] so that a later access does not take a page fault.
    /// `flags` is a bitmask of [`MrAdviseFlags`] ([`MrAdviseFlags::empty()`] for none), and
    /// `sg_list` describes the ranges to act on; every entry must lie within a memory region
    /// registered under this protection domain.
    ///
    /// # Errors
    ///
    ///  - [`Unsupported`](Error::Unsupported): the device does not support `ibv_advise_mr`
    ///    (`EOPNOTSUPP`).
    ///  - [`AdviseMemoryRegion`](Error::AdviseMemoryRegion): `ibv_advise_mr` failed (`EINVAL` for
    ///    an invalid value in the arguments, `ENOMEM` when out of resources).
    pub fn advise_mr(
        &self,
        advice: MrAdvice,
        flags: MrAdviseFlags,
        sg_list: &[LocalMemorySlice],
    ) -> Result<()> {
        let ret = unsafe {
            ffi::ibv_advise_mr(
                self.inner.pd,
                advice.into(),
                flags.into(),
                sg_list.as_ptr() as *mut ffi::ibv_sge,
                sg_list.len() as u32,
            )
        };
        if ret == 0 {
            Ok(())
        } else {
            Err(Error::errno(ret, Error::AdviseMemoryRegion))
        }
    }

    /// Creates a queue pair builder associated with `port_num` on this protection domain's device.
    ///
    /// `send` and `recv` are the [`CompletionQueue`]s that completions for the send and receive
    /// queues are delivered to, respectively. They may refer to the same queue.
    ///
    /// `qp_type` is the requested transport service type of this QP (for example
    /// [`QueuePairType::ReliableConnection`]).
    ///
    /// `port_num` is the device port this queue pair uses; ports are numbered from 1.
    ///
    /// Note that both this protection domain, *and* both provided completion queues, must outlive
    /// the resulting `QueuePair`.
    ///
    /// # Errors
    ///
    ///  - [`QueryPort`](Error::QueryPort): querying `port_num` failed (`ibv_query_port`).
    ///  - [`PortNotActive`](Error::PortNotActive): `port_num` is not in the `ACTIVE` or `ARMED`
    ///    state, so its GID table and routing are unusable.
    pub fn create_qp(
        &self,
        send: &CompletionQueue,
        recv: &CompletionQueue,
        qp_type: QueuePairType,
        port_num: u8,
    ) -> Result<QueuePairBuilder> {
        let port_attr = self.inner.ctx.query_port(port_num)?;
        Ok(QueuePairBuilder::new(
            self.inner.clone(),
            port_attr,
            port_num,
            send.inner.clone(),
            1,
            recv.inner.clone(),
            1,
            qp_type.into(),
            1,
            1,
        ))
    }

    /// Register `[ptr, ptr + len)` and wrap the resulting MR handle, deduplicating the
    /// `ibv_reg_mr` call and null check shared by the registration entry points.
    ///
    /// # Safety
    ///
    /// `ptr` must be valid for `len` bytes and outlive the returned `MemoryRegionInner`.
    unsafe fn reg_mr(
        &self,
        ptr: *mut c_void,
        len: usize,
        access_flags: AccessFlags,
    ) -> Result<MemoryRegionInner> {
        let mr = ffi::ibv_reg_mr(self.inner.pd, ptr, len, access_flags.0 as i32);
        // ibv_reg_mr() returns a pointer to the registered MR, or NULL if the request fails.
        if mr.is_null() {
            // Promotes EOPNOTSUPP (an access flag the device cannot honor) to Unsupported, like
            // register_dmabuf and the other verbs.
            Err(Error::os(
                io::Error::last_os_error(),
                Error::RegisterMemoryRegion,
            ))
        } else {
            Ok(MemoryRegionInner {
                _pd: self.inner.clone(),
                mr,
            })
        }
    }

    /// Allocates and registers a Memory Region (MR) associated with this `ProtectionDomain`, with
    /// the given access permissions.
    ///
    /// This process allows the RDMA device to read and write data to the allocated memory. Only
    /// registered memory can be sent from and received to by `QueuePair`s. Performing this
    /// registration takes some time, so performing memory registration isn't recommended in the
    /// data path, when fast response is required.
    ///
    /// The buffer is `n` zero-initialized bytes, owned by the returned [`MemoryRegion`] and
    /// deregistered and freed when it drops. To register memory you manage yourself instead, see
    /// [`register_from_raw`](Self::register_from_raw) and
    /// [`register_dmabuf`](Self::register_dmabuf).
    ///
    /// Every successful registration will result with a MR which has unique (within a specific
    /// RDMA device) `lkey` and `rkey` values. These keys must be communicated to the other end's
    /// `QueuePair` for direct memory access.
    ///
    /// The maximum size of the block that can be registered is limited to
    /// `device_attr.max_mr_size`. There isn't any way to know what is the total size of memory
    /// that can be registered for a specific device.
    ///
    /// `access_flags` are the permissions the region is registered with; local read access is
    /// always enabled. [`AccessFlags::PERMISSIVE`] is the everything-enabled set: local write, remote
    /// write, remote read, remote atomics, and relaxed ordering.
    ///
    /// # Panics
    ///
    /// Panics if `n` is 0.
    ///
    /// # Errors
    ///
    ///  - [`Unsupported`](Error::Unsupported): the device cannot honor one of the access flags
    ///    (`EOPNOTSUPP`).
    ///  - [`RegisterMemoryRegion`](Error::RegisterMemoryRegion): `ibv_reg_mr` failed (`EINVAL`
    ///    for an invalid access value, `ENOMEM` when out of resources — either in the operating
    ///    system or in the RDMA device).
    pub fn allocate(&self, n: usize, access_flags: AccessFlags) -> Result<MemoryRegion<Box<[u8]>>> {
        assert!(n > 0);
        let mut data = vec![0u8; n].into_boxed_slice();
        let inner = unsafe { self.reg_mr(data.as_mut_ptr() as *mut c_void, n, access_flags)? };
        Ok(MemoryRegion { inner, owner: data })
    }

    /// Registers externally managed memory as a Memory Region (MR), with the given access
    /// permissions.
    ///
    /// This registers the `len` bytes starting at `ptr`. Unlike [`allocate`](Self::allocate), the
    /// returned [`MemoryRegion`] does *not* own the underlying memory — it is the entry point for
    /// registering memory you manage yourself, such as an `mmap`/hugepage mapping. Use the returned
    /// region's [`slice`](MemoryRegion::slice) / [`remote`](MemoryRegion::remote) handles for RDMA;
    /// access the bytes through your own pointer.
    ///
    /// `access_flags` are the permissions the region is registered with; local read access is
    /// always enabled ([`AccessFlags::PERMISSIVE`] is the everything-enabled set).
    ///
    /// # Safety
    ///
    /// The caller must ensure that, for the entire lifetime of the returned `MemoryRegion`:
    ///
    ///  - `ptr` is valid for reads and writes of `len` bytes,
    ///  - the memory is not moved, freed, or unmapped, and
    ///  - the memory is not accessed in a way that races with the device reading or writing it.
    ///
    /// # Panics
    ///
    /// Panics if `len` is 0.
    ///
    /// # Errors
    ///
    ///  - [`Unsupported`](Error::Unsupported): the device cannot honor one of the access flags
    ///    (`EOPNOTSUPP`).
    ///  - [`RegisterMemoryRegion`](Error::RegisterMemoryRegion): `ibv_reg_mr` failed (`EINVAL`
    ///    for an invalid access value, `ENOMEM` when out of resources — either in the operating
    ///    system or in the RDMA device).
    pub unsafe fn register_from_raw(
        &self,
        ptr: *mut u8,
        len: usize,
        access_flags: AccessFlags,
    ) -> Result<MemoryRegion<()>> {
        assert!(len > 0);
        let inner = self.reg_mr(ptr as *mut c_void, len, access_flags)?;
        Ok(MemoryRegion { inner, owner: () })
    }

    /// Registers an already allocated DMA-BUF as a memory region (MR) associated with this
    /// `ProtectionDomain` (`ibv_reg_dmabuf_mr`, see the [`ibv_reg_mr` man page][man]).
    ///
    /// This is how device memory (for example a GPU buffer exported as a DMA-BUF) is made
    /// available for RDMA without staging through host memory. The buffer stays owned by its
    /// exporter; the returned region only holds the registration, which is dropped on drop.
    ///
    /// # Arguments
    ///
    /// * `fd` - The file descriptor of the DMA-BUF to be registered. This must refer to an already allocated buffer.
    /// * `offset`, `len` - The MR starts at `offset` of the dma-buf and its size is `len`.
    /// * `iova` - The argument iova specifies the virtual base address of the MR when accessed through a lkey or rkey.
    ///   Note: `iova` must have the same page offset as `offset`
    ///
    /// # Errors
    ///
    ///  - [`Unsupported`](Error::Unsupported): the device or kernel does not support DMA-BUF
    ///    registration.
    ///  - [`RegisterMemoryRegion`](Error::RegisterMemoryRegion): `ibv_reg_dmabuf_mr` failed, for
    ///    example due to an invalid descriptor, range, or access flags.
    ///
    /// [man]: https://man7.org/linux/man-pages/man3/ibv_reg_mr.3.html
    pub fn register_dmabuf(
        &self,
        fd: i32,
        offset: u64,
        len: usize,
        iova: u64,
        access_flags: AccessFlags,
    ) -> Result<MemoryRegion<()>> {
        let mr = unsafe {
            ffi::ibv_reg_dmabuf_mr(self.inner.pd, offset, len, iova, fd, access_flags.0 as i32)
        };

        if mr.is_null() {
            // Promotes the EOPNOTSUPP of devices/kernels without DMA-BUF support to Unsupported.
            Err(Error::os(
                io::Error::last_os_error(),
                Error::RegisterMemoryRegion,
            ))
        } else {
            let inner = MemoryRegionInner {
                _pd: self.inner.clone(),
                mr,
            };
            Ok(MemoryRegion { inner, owner: () })
        }
    }

    /// Creates a shared receive queue (SRQ) associated with this protection domain.
    ///
    /// A shared receive queue holds one pool of receive work requests that several queue pairs
    /// consume from (set it with [`QueuePairBuilder::set_srq`]), instead of posting receives to
    /// each queue pair separately.
    ///
    /// `max_wr` is the maximum number of outstanding work requests that can be posted to the SRQ.
    /// `max_sge` is the maximum number of scatter/gather elements per work request.
    /// `srq_limit` arms the SRQ's low-watermark event: when the number of posted receives drops
    /// below it, the device raises an [`SrqLimitReached`](crate::AsyncEventType::SrqLimitReached)
    /// asynchronous event — receive it with [`Context::poll_async_event`] /
    /// [`Context::wait_async_event`] and top the SRQ back up (pass 0 to disable).
    ///
    /// # Errors
    ///
    ///  - [`CreateSharedReceiveQueue`](Error::CreateSharedReceiveQueue): `ibv_create_srq` failed,
    ///    for example because `max_wr` or `max_sge` exceed the device capabilities.
    ///
    /// See also [RDMAmojo's `ibv_create_srq` documentation][1] and the [man page][2].
    ///
    /// [1]: https://www.rdmamojo.com/2013/01/30/ibv_create_srq/
    /// [2]: https://man7.org/linux/man-pages/man3/ibv_create_srq.3.html
    pub fn create_srq(
        &self,
        max_wr: u32,
        max_sge: u32,
        srq_limit: u32,
    ) -> Result<SharedReceiveQueue> {
        let mut srq_init_attr = ffi::ibv_srq_init_attr {
            srq_context: ptr::null_mut(),
            attr: ffi::ibv_srq_attr {
                max_wr,
                max_sge,
                srq_limit,
            },
        };
        let srq = unsafe { ffi::ibv_create_srq(self.inner.pd, &mut srq_init_attr as *mut _) };
        if srq.is_null() {
            Err(Error::CreateSharedReceiveQueue(io::Error::last_os_error()))
        } else {
            Ok(SharedReceiveQueue {
                inner: Arc::new(SharedReceiveQueueInner {
                    _pd: self.inner.clone(),
                    srq,
                }),
            })
        }
    }
}

#[cfg(test)]
mod test_conversions {
    use super::*;

    #[test]
    fn mr_advice_roundtrip() {
        for (wrapper, raw) in [
            (
                MrAdvice::Prefetch,
                ffi::ib_uverbs_advise_mr_advice::IB_UVERBS_ADVISE_MR_ADVICE_PREFETCH,
            ),
            (
                MrAdvice::PrefetchWrite,
                ffi::ib_uverbs_advise_mr_advice::IB_UVERBS_ADVISE_MR_ADVICE_PREFETCH_WRITE,
            ),
            (
                MrAdvice::PrefetchNoFault,
                ffi::ib_uverbs_advise_mr_advice::IB_UVERBS_ADVISE_MR_ADVICE_PREFETCH_NO_FAULT,
            ),
        ] {
            assert_eq!(MrAdvice::from(raw), wrapper);
            assert_eq!(ffi::ib_uverbs_advise_mr_advice::from(wrapper), raw);
        }
    }

    #[test]
    fn mr_advise_flags_bits_and_debug() {
        assert!(!MrAdviseFlags::empty().contains(MrAdviseFlags::FLUSHABLE));
        let mut flags = MrAdviseFlags::empty();
        flags |= MrAdviseFlags::FLUSHABLE;
        assert!(flags.contains(MrAdviseFlags::FLUSHABLE));
        assert_eq!(u32::from(flags), 1);
        assert_eq!(
            MrAdviseFlags::from(ffi::ib_uverbs_advise_mr_flag::IB_UVERBS_ADVISE_MR_FLAG_FLUSH),
            MrAdviseFlags::FLUSHABLE
        );
        assert_eq!(format!("{flags:?}"), "MrAdviseFlags(FLUSHABLE)");
        assert_eq!(format!("{:?}", MrAdviseFlags::empty()), "MrAdviseFlags(0)");
    }
}
