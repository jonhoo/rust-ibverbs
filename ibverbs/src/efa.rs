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
    AddressedSendOp, Datagram, Payload, PreparedQueuePair, QueuePair, QueuePairBuilder,
    QueuePairType, Transport, sealed,
};

#[cfg(doc)]
use crate::{AddressHandle, ProtectionDomain};

/// The EFA SRD (Scalable Reliable Datagram) transport: reliable like RC but connectionless and
/// addressed like UD ([`QueuePairType::Driver`]).
///
/// Begin building an SRD queue pair with [`ProtectionDomain::create_qp`]`::<Srd>`, configure it
/// like any other queue pair (GID index, queue/SGE limits), create it with
/// [`build`](QueuePairBuilder::build), and bring it to ready with
/// [`activate`](PreparedQueuePair::activate). Send with [`start_send`](QueuePair::start_send) and
/// [`to(..)`](crate::SendBatch::to), receive with [`post_recv`](QueuePair::post_recv).
///
/// Every SRD work request generates a completion: EFA refuses unsignaled ones, so the queue pair
/// is created to signal every send and [`signaled`](AddressedSendOp::signaled) is implied. Size
/// the send completion queue for a completion per work request (see
/// [`CompletionQueue::capacity`](crate::CompletionQueue::capacity)).
///
/// EFA also refuses the fence and solicited flags, so an SRD work request has no
/// [`fenced`](AddressedSendOp::fenced) or [`solicited`](AddressedSendOp::solicited) modifier:
///
/// ```compile_fail,E0599
/// // EFA refuses the fence flag: `fenced` exists only on UD.
/// fn f(qp: &mut ibverbs::QueuePair<ibverbs::Srd>, ah: &ibverbs::AddressHandle) {
///     let mut batch = qp.start_send();
///     batch.to(ah, 1, 2).fenced();
/// }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Srd;

impl sealed::Sealed for Srd {
    const TYPE: QueuePairType = QueuePairType::Driver;
}
impl Transport for Srd {}
impl Datagram for Srd {}

impl QueuePairBuilder<Srd> {
    /// Create the EFA SRD queue pair described by this builder (`efadv_create_qp_ex`).
    ///
    /// It enables the send and one-sided RDMA doorbell operations; activate the result with
    /// [`activate`](PreparedQueuePair::activate).
    ///
    /// # Errors
    ///
    ///  - [`CreateQueuePair`](Error::CreateQueuePair): a completion queue belongs to another
    ///    device context than the protection domain, or `efadv_create_qp_ex` failed (`EINVAL` for
    ///    an invalid value in the queue pair attributes, `ENOMEM` when out of resources).
    ///  - [`Unsupported`](Error::Unsupported): the provider declined the requested send
    ///    operations (`EOPNOTSUPP`), or created the queue pair without the extended work-request
    ///    interface this crate posts through (`ibv_qp_to_qp_ex` returned no handle).
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
        let qp = nonnull(qp, Error::CreateQueuePair)?;
        let qp_ex = unsafe { ffi::ibv_qp_to_qp_ex(qp) };
        let prepared = PreparedQueuePair {
            lid: self.lid,
            roce: self.roce,
            port_num: self.port_num,
            qp: QueuePair {
                pd: self.pd.clone(),
                _srq: None,
                _send_cq: self.send.clone(),
                _recv_cq: self.recv.clone(),
                qp,
                qp_ex,
                // Creation writes the granted capacities back into `attr.cap`.
                max_inline_data: attr.cap.max_inline_data,
                _transport: std::marker::PhantomData,
            },
            gid_index: self.gid_index,
            traffic_class: self.traffic_class,
            service_level: self.service_level,
            bring_up: self.bring_up.clone(),
        };
        // As in qp.rs: a provider that accepted the send-operations mask without installing the
        // work-request table leaves `ibv_qp_to_qp_ex` returning NULL. Dropping `prepared` destroys
        // the queue pair.
        if qp_ex.is_null() {
            return Err(Error::Unsupported {
                operation: &"ibv_qp_to_qp_ex",
            });
        }
        Ok(prepared)
    }
}

impl PreparedQueuePair<Srd> {
    /// Transition this EFA SRD queue pair to ready with the given Q_Key.
    ///
    /// SRD is connectionless, so this needs no remote endpoint; the transitions are the same as a UD
    /// queue pair's. Address each send with an [`AddressHandle`] (see
    /// [`SendBatch::to`](crate::SendBatch::to)).
    ///
    /// # Errors
    ///
    ///  - [`ModifyQueuePair`](Error::ModifyQueuePair): a state transition failed (`EINVAL` for an
    ///    invalid value in `attr` or `attr_mask`, `ENOMEM` when out of resources).
    pub fn activate(self, qkey: u32) -> Result<QueuePair<Srd>> {
        self.activate_impl(qkey)
    }
}

impl AddressedSendOp<'_, '_, Srd> {
    /// Post an RDMA WRITE of `local` into `remote`, with the immediate set by [`imm`](Self::imm)
    /// if any.
    ///
    /// The data must come from registered memory: EFA writes inline data only on a queue pair
    /// created with its inline-write flag (`EFADV_QP_FLAGS_INLINE_WRITE`), which doubles the size
    /// of every send work request and needs rdma-core 63, so this crate does not set it.
    #[inline]
    pub fn write(self, wr_id: u64, local: &[LocalMemorySlice], remote: RemoteMemorySlice) {
        let imm = self.op.imm;
        self.op.build(wr_id, Payload::Sges(local), move |q| unsafe {
            match imm {
                Some(imm) => {
                    (*q).wr_rdma_write_imm.unwrap()(q, remote.rkey, remote.addr, imm.to_be())
                }
                None => (*q).wr_rdma_write.unwrap()(q, remote.rkey, remote.addr),
            }
        })
    }

    /// Post an RDMA READ from `remote` into `local`.
    #[inline]
    pub fn read(self, wr_id: u64, local: &[LocalMemorySlice], remote: RemoteMemorySlice) {
        self.op.build(wr_id, Payload::Sges(local), move |q| unsafe {
            (*q).wr_rdma_read.unwrap()(q, remote.rkey, remote.addr)
        })
    }
}
