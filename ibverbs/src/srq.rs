use std::sync::Arc;

use crate::error::{Error, Result};
use crate::pd::ProtectionDomain;
use crate::qp::{post_linked, RecvRequest};
use crate::raw;

pub(crate) struct SharedReceiveQueueInner {
    pub(crate) _pd: ProtectionDomain,
    pub(crate) srq: *mut ffi::ibv_srq,
}

unsafe impl Sync for SharedReceiveQueueInner {}
unsafe impl Send for SharedReceiveQueueInner {}

impl Drop for SharedReceiveQueueInner {
    fn drop(&mut self) {
        raw::destroyed("ibv_destroy_srq", unsafe { ffi::ibv_destroy_srq(self.srq) });
    }
}

/// The attributes of a [`SharedReceiveQueue`], read with [`SharedReceiveQueue::query`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct SrqAttributes {
    /// The number of receives the queue can hold: the capacity the device granted, at least what
    /// was asked at creation or in the last [`set_max_wr`](SharedReceiveQueue::set_max_wr).
    pub max_wr: u32,
    /// The number of scatter/gather entries a receive may carry.
    pub max_sge: u32,
    /// The low-watermark limit currently armed with [`set_limit`](SharedReceiveQueue::set_limit),
    /// or 0 when none is.
    pub limit: u32,
}

/// A shared receive queue (SRQ) that allows sharing receive buffers across multiple queue pairs.
/// Created by [`ProtectionDomain::create_srq`](crate::ProtectionDomain::create_srq).
#[derive(Clone)]
#[must_use = "the shared receive queue is destroyed when dropped"]
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

    /// The queue's current attributes (`ibv_query_srq`): its capacity, the scatter/gather entries
    /// per receive, and the armed limit.
    ///
    /// # Errors
    ///
    ///  - [`QuerySharedReceiveQueue`](Error::QuerySharedReceiveQueue): `ibv_query_srq` failed.
    pub fn query(&self) -> Result<SrqAttributes> {
        let mut attr = ffi::ibv_srq_attr::default();
        let errno = unsafe { ffi::ibv_query_srq(self.inner.srq, &mut attr) };
        raw::errno(errno, Error::QuerySharedReceiveQueue)?;
        Ok(SrqAttributes {
            max_wr: attr.max_wr,
            max_sge: attr.max_sge,
            limit: attr.srq_limit,
        })
    }

    /// Arm the low-watermark event (`ibv_modify_srq` with `IBV_SRQ_LIMIT`): once fewer than
    /// `limit` receives remain posted, the device raises
    /// [`AsyncEventType::SrqLimitReached`](crate::AsyncEventType::SrqLimitReached) once and
    /// disarms, so re-arm to be told again. A `limit` of 0 disarms. The limit must be below the
    /// queue's capacity ([`SrqAttributes::max_wr`]).
    ///
    /// # Errors
    ///
    ///  - [`ModifySharedReceiveQueue`](Error::ModifySharedReceiveQueue): `ibv_modify_srq` failed
    ///    (`EINVAL` for a limit the queue cannot hold).
    ///  - [`Unsupported`](Error::Unsupported): the provider does not support the limit event.
    pub fn set_limit(&self, limit: u32) -> Result<()> {
        let mut attr = ffi::ibv_srq_attr {
            srq_limit: limit,
            ..Default::default()
        };
        self.modify(&mut attr, ffi::ibv_srq_attr_mask::IBV_SRQ_LIMIT)
    }

    /// Resize the queue to hold `max_wr` receives (`ibv_modify_srq` with `IBV_SRQ_MAX_WR`),
    /// keeping the receives already posted. The device may grant more than asked; read the result
    /// with [`query`](Self::query).
    ///
    /// # Errors
    ///
    ///  - [`ModifySharedReceiveQueue`](Error::ModifySharedReceiveQueue): `ibv_modify_srq` failed
    ///    (`EINVAL` for a size the device cannot provide, or fewer entries than are posted).
    ///  - [`Unsupported`](Error::Unsupported): the provider does not support resizing.
    pub fn set_max_wr(&self, max_wr: u32) -> Result<()> {
        let mut attr = ffi::ibv_srq_attr {
            max_wr,
            ..Default::default()
        };
        self.modify(&mut attr, ffi::ibv_srq_attr_mask::IBV_SRQ_MAX_WR)
    }

    /// `ibv_modify_srq` for the attributes `mask` selects in `attr`.
    fn modify(&self, attr: &mut ffi::ibv_srq_attr, mask: ffi::ibv_srq_attr_mask) -> Result<()> {
        let errno = unsafe { ffi::ibv_modify_srq(self.inner.srq, attr, mask as i32) };
        raw::errno(errno, Error::ModifySharedReceiveQueue)
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
        let srq = self.inner.srq;
        unsafe {
            post_linked(recvs.as_mut(), |wr, bad_wr| {
                ffi::ibv_post_srq_recv(srq, wr, bad_wr)
            })
        }
    }
}
