use std::sync::Arc;

use crate::error::{Error, Result, destroyed, keep};
use crate::pd::ProtectionDomain;
use crate::qp::RecvRequest;

struct SharedReceiveQueueInner {
    _pd: ProtectionDomain,
    srq: *mut ffi::ibv_srq,
    /// The capacities the provider granted at creation.
    max_wr: u32,
    max_sge: u32,
}

unsafe impl Sync for SharedReceiveQueueInner {}
unsafe impl Send for SharedReceiveQueueInner {}

impl Drop for SharedReceiveQueueInner {
    fn drop(&mut self) {
        destroyed(
            "ibv_destroy_srq",
            unsafe { ffi::ibv_destroy_srq(self.srq) },
            || keep(&self._pd),
        );
    }
}

/// The attributes of a [`SharedReceiveQueue`], read with [`SharedReceiveQueue::query`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct SrqAttributes {
    /// The size of the queue as the kernel reports it, at least what was asked at creation or in
    /// the last [`set_max_wr`](SharedReceiveQueue::set_max_wr). This is not always the number of
    /// receives that can be posted: mlx5 keeps about half of its ring for receives parked on a
    /// page fault, and reports the whole ring here. [`SharedReceiveQueue::capacity`] is the number
    /// that can be posted.
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
    _inner: Arc<SharedReceiveQueueInner>,
    /// `_inner.srq`, kept next to the `Arc` for the data path, as `CompletionQueue` does.
    srq: *mut ffi::ibv_srq,
}

// The pointer is a copy of `_inner`'s, valid for as long as `_inner` keeps the queue alive.
unsafe impl Sync for SharedReceiveQueue {}
unsafe impl Send for SharedReceiveQueue {}

impl SharedReceiveQueue {
    /// Wraps `srq`, created on `pd` with the capacities `granted` (for
    /// [`ProtectionDomain::create_srq`]).
    pub(crate) fn new(
        pd: ProtectionDomain,
        srq: *mut ffi::ibv_srq,
        granted: &ffi::ibv_srq_attr,
    ) -> SharedReceiveQueue {
        SharedReceiveQueue {
            _inner: Arc::new(SharedReceiveQueueInner {
                _pd: pd,
                srq,
                max_wr: granted.max_wr,
                max_sge: granted.max_sge,
            }),
            srq,
        }
    }

    /// The number of receives the queue can hold at once: the capacity the provider granted at
    /// creation, at least the `max_wr` asked of
    /// [`ProtectionDomain::create_srq`](crate::ProtectionDomain::create_srq) (providers round it
    /// up; mlx5 to one below a power of two). Posting more fails with `ENOMEM`. The value is the
    /// one from creation: it does not follow a later [`set_max_wr`](Self::set_max_wr).
    pub fn capacity(&self) -> u32 {
        self._inner.max_wr
    }

    /// The number of scatter/gather entries a receive may carry: what the provider granted at
    /// creation, at least the `max_sge` asked.
    pub fn max_sge(&self) -> u32 {
        self._inner.max_sge
    }

    /// Returns the underlying `ibv_srq` pointer.
    ///
    /// This is an escape hatch for verbs this crate does not yet wrap. The pointer is owned by this
    /// [`SharedReceiveQueue`] and stays valid only while it is alive; do not destroy it.
    pub fn as_raw(&self) -> *mut ffi::ibv_srq {
        self.srq
    }

    /// The queue's current attributes (`ibv_query_srq`): its capacity, the scatter/gather entries
    /// per receive, and the armed limit.
    ///
    /// # Errors
    ///
    ///  - [`QuerySharedReceiveQueue`](Error::QuerySharedReceiveQueue): `ibv_query_srq` failed.
    pub fn query(&self) -> Result<SrqAttributes> {
        let mut attr = ffi::ibv_srq_attr::default();
        let errno = unsafe { ffi::ibv_query_srq(self.srq, &mut attr) };
        if errno != 0 {
            return Err(Error::errno(errno, Error::QuerySharedReceiveQueue));
        }
        Ok(SrqAttributes {
            max_wr: attr.max_wr,
            max_sge: attr.max_sge,
            limit: attr.srq_limit,
        })
    }

    /// Arm the low-watermark event (`ibv_modify_srq` with `IBV_SRQ_LIMIT`): once fewer than
    /// `limit` receives remain posted, the device raises
    /// [`AsyncEventType::SrqLimitReached`](crate::AsyncEventType::SrqLimitReached) once and
    /// disarms, so re-arm to be told again. A `limit` of 0 disarms. Keep the limit at or below the
    /// queue's [`capacity`](Self::capacity): the kernel refuses only a limit at or above the size
    /// it reports ([`SrqAttributes::max_wr`]), which on mlx5 is about twice the capacity, and a
    /// limit above the capacity is one the posted receives can never reach.
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
        let errno = unsafe { ffi::ibv_modify_srq(self.srq, attr, mask.0 as i32) };
        if errno != 0 {
            return Err(Error::errno(errno, Error::ModifySharedReceiveQueue));
        }
        Ok(())
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
    /// that receive), or until the shared receive queue is destroyed. After an `Err`, the requests
    /// ahead of the one that failed were posted (the error's `posted` counts them), so the same
    /// holds for theirs.
    ///
    /// A slice of a DMA-BUF region must also meet what "Receiving small messages on mlx5" asks
    /// for in [`register_dmabuf`](crate::ProtectionDomain::register_dmabuf): there the provider
    /// writes a small message to the slice's address with the CPU.
    ///
    /// # Errors
    ///
    ///  - [`PostReceive`](Error::PostReceive): `ibv_post_srq_recv` failed (`EINVAL` for an invalid
    ///    value in one of the work requests, `ENOMEM` when the SRQ is full or out of resources,
    ///    `EFAULT` for an invalid `SharedReceiveQueue`). Its `posted` counts the requests at the
    ///    front of `recvs` that were posted before the one that failed, so the rest can be posted
    ///    again once there is room.
    ///
    /// [1]: https://www.rdmamojo.com/2013/02/08/ibv_post_srq_recv/
    /// [2]: https://man7.org/linux/man-pages/man3/ibv_post_srq_recv.3.html
    pub unsafe fn post_recv<'a>(&self, mut recvs: impl AsMut<[RecvRequest<'a>]>) -> Result<()> {
        RecvRequest::post(recvs.as_mut(), false, |head, bad_wr| unsafe {
            ffi::ibv_post_srq_recv(self.srq, head, bad_wr)
        })
    }
}
