// ---------------------------------------------------------------------------
// AWS Elastic Fabric Adapter (EFA) support, behind the `efa` feature.
//
// EFA's transport is SRD (Scalable Reliable Datagram): reliable like RC but connectionless and
// addressed like UD. Only queue-pair *creation* is EFA-specific (`efadv_create_qp_ex`); the resulting
// queue pair carries the same extended send operations as every other queue pair, so sends go
// through the normal doorbell path ([`QueuePair::start_send`] with a `.to(..)`), receives through
// [`QueuePair::post_recv`], and completions through [`CompletionQueue::poll`].
// ---------------------------------------------------------------------------

use crate::error::{Error, Result, nonnull};
use crate::mr::{LocalMemorySlice, RemoteMemorySlice};
use crate::qp::{
    Datagram, Payload, PreparedQueuePair, QueuePairBuilder, SendOp, Transport, sealed,
};

#[cfg(doc)]
use crate::{AddressHandle, ProtectionDomain, QueuePair};

/// The EFA SRD (Scalable Reliable Datagram) transport: reliable like RC but connectionless and
/// addressed like UD (a provider-specific `IBV_QPT_DRIVER` queue pair).
///
/// Begin building an SRD queue pair with [`ProtectionDomain::create_qp`]`::<Srd>`, configure it
/// like any other queue pair (GID index, queue/SGE limits), create it with
/// [`build`](QueuePairBuilder::build), and bring it to ready with
/// [`activate`](PreparedQueuePair::activate). Send with [`start_send`](QueuePair::start_send) and
/// [`to(..)`](crate::SendBatch::to), receive with [`post_recv`](QueuePair::post_recv).
///
/// Every SRD work request generates a completion: EFA refuses unsignaled ones, so the queue pair
/// is created to signal every send and [`signaled`](SendOp::signaled) is implied. Size
/// the send completion queue for a completion per work request (see
/// [`CompletionQueue::capacity`](crate::CompletionQueue::capacity)).
///
/// EFA also refuses the solicited flag, so an SRD work request has no
/// [`solicited`](SendOp::solicited) modifier:
///
/// ```compile_fail,E0599
/// // EFA refuses the solicited flag: `solicited` exists only on UD.
/// fn f(qp: &mut ibverbs::QueuePair<ibverbs::Srd>, ah: &ibverbs::AddressHandle) {
///     let mut batch = qp.start_send();
///     batch.to(ah, 1, 2).solicited();
/// }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Srd;

impl sealed::Sealed for Srd {
    const TYPE: ffi::ibv_qp_type = ffi::ibv_qp_type::IBV_QPT_DRIVER;
}
impl Transport for Srd {}
impl Datagram for Srd {}

impl QueuePairBuilder<Srd> {
    /// Create the EFA SRD queue pair described by this builder (`efadv_create_qp_ex`).
    ///
    /// The queue pair is created with the builder's send operations
    /// ([`send_ops`](QueuePairBuilder::send_ops): by default SEND and RDMA WRITE, with and without
    /// an immediate, and RDMA READ). EFA grants RDMA READ and WRITE only on a device with those
    /// capabilities (`efadv_query_device`), so on one without them request SEND alone with
    /// [`set_send_ops`](QueuePairBuilder::set_send_ops). Activate the result with
    /// [`activate`](PreparedQueuePair::activate).
    ///
    /// # Errors
    ///
    ///  - [`CreateQueuePair`](Error::CreateQueuePair): a completion queue belongs to another
    ///    device context than the protection domain, or `efadv_create_qp_ex` failed (`EINVAL` for
    ///    an invalid value in the queue pair attributes, `ENOMEM` when out of resources).
    ///  - [`Unsupported`](Error::Unsupported): the provider declined the requested send
    ///    operations (`EOPNOTSUPP`; RDMA READ or WRITE on a device without them), or created the
    ///    queue pair without the extended work-request interface this crate posts through
    ///    (`ibv_qp_to_qp_ex` returned no handle).
    pub fn build(&self) -> Result<PreparedQueuePair<Srd>> {
        self.check_same_context()?;
        let mut attr = ffi::ibv_qp_init_attr_ex {
            // EFA rejects a work request without `IBV_SEND_SIGNALED` unless the queue pair signals
            // every send, and requests a completion for every work request either way, so
            // signaling all costs nothing and spares each request the flag.
            sq_sig_all: 1,
            ..self.init_attr_ex()
        };
        // rdma-core adds fields to this struct over time (64.0 added `wr_flags`); the provider
        // reads only as much as the passed-in length covers.
        let mut efa_attr = ffi::efadv_qp_init_attr {
            driver_qp_type: ffi::EFADV_QP_DRIVER_TYPE_SRD.0,
            ..Default::default()
        };
        let qp = unsafe {
            ffi::efadv_create_qp_ex(
                self.pd.context().as_raw(),
                &mut attr,
                &mut efa_attr,
                std::mem::size_of::<ffi::efadv_qp_init_attr>() as u32,
            )
        };
        let qp = nonnull(qp, Error::CreateQueuePair)
            .map_err(|e| e.for_operation(&"efadv_create_qp_ex"))?;
        self.prepared(qp, &attr)
    }
}

impl SendOp<'_, '_, Srd> {
    /// Post an RDMA WRITE of `local` into `remote`, with the immediate set by [`imm`](Self::imm)
    /// if any.
    ///
    /// The data must come from registered memory: EFA writes inline data only on a queue pair
    /// created with its inline-write flag (`EFADV_QP_FLAGS_INLINE_WRITE`), which doubles the size
    /// of every send work request and needs rdma-core 63, so this crate does not set it.
    ///
    /// # Panics
    ///
    /// Panics if `local` has more entries than the queue pair was built for
    /// ([`QueuePairBuilder::set_max_send_sge`]).
    #[inline]
    pub fn write(self, wr_id: u64, local: &[LocalMemorySlice], remote: RemoteMemorySlice) {
        let imm = self.imm;
        let op = SendOp::<Srd>::write_op(imm, remote);
        self.build(wr_id, Payload::Sges(local), op)
    }

    /// Post an RDMA READ from `remote` into `local`.
    ///
    /// # Panics
    ///
    /// Panics if `local` has more entries than the queue pair was built for
    /// ([`QueuePairBuilder::set_max_send_sge`]).
    #[inline]
    pub fn read(self, wr_id: u64, local: &[LocalMemorySlice], remote: RemoteMemorySlice) {
        let op = SendOp::<Srd>::read_op(remote);
        self.build(wr_id, Payload::Sges(local), op)
    }
}
