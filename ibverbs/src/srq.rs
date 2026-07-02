use std::io;
use std::ptr;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::pd::ProtectionDomainInner;
use crate::qp::RecvRequest;

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
/// Created by [`ProtectionDomain::create_srq`](crate::ProtectionDomain::create_srq).
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

    /// Posts a batch of receive Work Requests to this Shared Receive Queue (SRQ) with a single
    /// `ibv_post_srq_recv`.
    ///
    /// Receives have no doorbell form, so the requests are posted as a linked list. `recvs` is the
    /// caller's storage — a stack array or a reusable `Vec` — linked in place rather than copied, so
    /// posting allocates nothing. The RDMA device will take one of the posted work requests as soon
    /// as an incoming message to any Queue Pair (QP) associated with this SRQ consumes a Receive
    /// Request (RR).
    ///
    /// If a work request is consumed by a UD QP associated with this SRQ, the Global Routing Header
    /// (GRH) of the incoming message will be placed in the first 40 bytes of the buffer(s) in the
    /// scatter list. If no GRH is present in the incoming message, then the first bytes will be
    /// undefined. This means that in all cases, the actual data of the incoming message will start
    /// at an offset of 40 bytes into the buffer(s) in the scatter list.
    ///
    /// See also [RDMAmojo's `ibv_post_srq_recv` documentation][1] and the [man page][2].
    ///
    /// # Safety
    ///
    /// Each referenced memory region must stay valid until a work completion has been polled for
    /// the corresponding `wr_id` (i.e., until `CompletionQueue::poll` returns a completion for
    /// that receive).
    ///
    /// # Errors
    ///
    ///  - [`PostReceive`](Error::PostReceive): `ibv_post_srq_recv` failed (`EINVAL` for an invalid
    ///    value in one of the work requests, `ENOMEM` when the SRQ is full or out of resources,
    ///    `EFAULT` for an invalid `SharedReceiveQueue`).
    ///
    /// [1]: https://www.rdmamojo.com/2013/02/08/ibv_post_srq_recv/
    /// [2]: https://man7.org/linux/man-pages/man3/ibv_post_srq_recv.3.html
    pub unsafe fn post_recv<'a>(&self, mut recvs: impl AsMut<[RecvRequest<'a>]>) -> Result<()> {
        let recvs = recvs.as_mut();
        if recvs.is_empty() {
            return Ok(());
        }
        // Link the requests into the list `ibv_post_srq_recv` expects.
        for i in 0..recvs.len() - 1 {
            let next = &mut recvs[i + 1].wr as *mut ffi::ibv_recv_wr;
            recvs[i].wr.next = next;
        }
        recvs.last_mut().unwrap().wr.next = ptr::null_mut();

        let mut bad_wr: *mut ffi::ibv_recv_wr = ptr::null_mut();
        let ctx = unsafe { *self.inner.srq }.context;
        let ops = &mut unsafe { *ctx }.ops;
        let errno = unsafe {
            ops.post_srq_recv.as_mut().unwrap()(
                self.inner.srq,
                &mut recvs[0].wr as *mut _,
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
