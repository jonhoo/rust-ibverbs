use std::io;
use std::ptr;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::mr::LocalMemorySlice;
use crate::pd::ProtectionDomainInner;

pub(crate) struct SharedReceiveQueueInner {
    pub(crate) _pd: Arc<ProtectionDomainInner>,
    pub(crate) srq: *mut ffi::ibv_srq,
}

unsafe impl Sync for SharedReceiveQueueInner {}
unsafe impl Send for SharedReceiveQueueInner {}

impl Drop for SharedReceiveQueueInner {
    fn drop(&mut self) {
        let errno = unsafe { ffi::ibv_destroy_srq(self.srq) };
        if errno != 0 {
            let e = io::Error::from_raw_os_error(errno);
            panic!("{e}");
        }
    }
}

/// A shared receive queue (SRQ) that allows sharing receive buffers across multiple queue pairs.
#[derive(Clone)]
pub struct SharedReceiveQueue {
    pub(crate) inner: Arc<SharedReceiveQueueInner>,
}

impl SharedReceiveQueue {
    /// Returns the underlying `ibv_srq` pointer.
    ///
    /// This is an escape hatch for verbs this crate does not yet wrap. The pointer is owned by this
    /// [`SharedReceiveQueue`] and stays valid only while it is alive; do not destroy it.
    pub fn as_raw(&self) -> *mut ffi::ibv_srq {
        self.inner.srq
    }

    /// Posts a linked list of Work Requests (WRs) to this Shared Receive Queue (SRQ).
    ///
    /// Generates a HW-specific Receive Request out of it and adds it to the tail of the SRQ
    /// without performing any context switch. The RDMA device will take one of those Work Requests
    /// as soon as an incoming opcode to any Queue Pair (QP) associated with this SRQ consumes a
    /// Receive Request (RR). If there is a failure in one of the WRs because the SRQ is full or
    /// one of the attributes in the WR is bad, it stops immediately and returns the pointer to that
    /// WR.
    ///
    /// `wr_id` is a 64-bit value associated with this WR. When a Work Completion is generated
    /// when this Work Request ends, it will contain this value.
    ///
    /// Internally, the memory at `local[range]` will be received into as a single `ibv_recv_wr`.
    ///
    /// If a WR is being posted to a UD QP associated with an SRQ, the Global Routing Header (GRH)
    /// of the incoming message will be placed in the first 40 bytes of the buffer(s) in the
    /// scatter list. If no GRH is present in the incoming message, then the first bytes will be
    /// undefined. This means that in all cases, the actual data of the incoming message will start
    /// at an offset of 40 bytes into the buffer(s) in the scatter list.
    ///
    /// See also [RDMAmojo's `ibv_post_srq_recv` documentation][1] and the [man page][2].
    ///
    /// # Safety
    ///
    /// The memory region can only be safely reused or dropped after the request is fully executed
    /// and a work completion has been retrieved from the corresponding completion queue (i.e.,
    /// until `CompletionQueue::poll` returns a completion for this receive).
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: Invalid value provided in the Work Request.
    ///  - `ENOMEM`: Receive Queue is full or not enough resources to complete this operation.
    ///  - `EFAULT`: Invalid value provided in `SharedReceiveQueue`.
    ///
    /// [1]: https://www.rdmamojo.com/2013/02/08/ibv_post_srq_recv/
    /// [2]: https://man7.org/linux/man-pages/man3/ibv_post_srq_recv.3.html
    #[inline]
    pub unsafe fn post_receive(&self, local: &[LocalMemorySlice], wr_id: u64) -> Result<()> {
        let mut wr = ffi::ibv_recv_wr {
            wr_id,
            next: ptr::null_mut(),
            sg_list: local.as_ptr() as *mut ffi::ibv_sge,
            num_sge: local.len() as i32,
        };
        let mut bad_wr = ptr::null_mut();

        let ctx = unsafe { *self.inner.srq }.context;
        let ops = &mut unsafe { *ctx }.ops;
        let errno = unsafe {
            ops.post_srq_recv.as_mut().unwrap()(
                self.inner.srq,
                &mut wr as *mut _,
                &mut bad_wr as *mut _,
            )
        };
        if errno != 0 {
            Err(Error::errno(errno, Error::PostReceive))
        } else {
            Ok(())
        }
    }
}
