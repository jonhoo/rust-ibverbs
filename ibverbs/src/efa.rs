// ---------------------------------------------------------------------------
// AWS Elastic Fabric Adapter (EFA) support, behind the `efa` feature.
//
// EFA's transport is SRD (Scalable Reliable Datagram): reliable like RC but connectionless and
// addressed like UD. Only queue-pair *creation* is EFA-specific (`efadv_create_qp_ex`); the resulting
// queue pair carries the same extended send operations as every other queue pair, so sends go
// through the normal doorbell path ([`QueuePair::start_send`] with a `.to(..)`), receives through
// [`QueuePair::post_recv`], and completions through [`CompletionQueue::poll`].
// ---------------------------------------------------------------------------

use std::io;
use std::os::raw::c_void;

use crate::completion::CompletionQueue;
use crate::error::{Error, Result};
use crate::mr::{LocalMemorySlice, RemoteMemorySlice};
use crate::pd::ProtectionDomain;
use crate::qp::{
    sealed, AddressedSendOp, Datagram, Payload, PreparedQueuePair, QueuePair, QueuePairBuilder,
    QueuePairType, Transport,
};

#[cfg(doc)]
use crate::AddressHandle;

/// The EFA SRD (Scalable Reliable Datagram) transport: reliable like RC but connectionless and
/// addressed like UD ([`QueuePairType::Driver`], created through
/// [`ProtectionDomain::create_srd_qp`]).
#[derive(Debug, Clone, Copy)]
pub struct Srd;

impl sealed::Sealed for Srd {
    const TYPE: QueuePairType = QueuePairType::Driver;
}
impl Transport for Srd {}
impl Datagram for Srd {}

impl ProtectionDomain {
    /// Begin building an EFA SRD queue pair associated with `port_num` (numbered from 1) on this
    /// protection domain's device.
    ///
    /// Configure it like any other queue pair (GID index, queue/SGE limits), then create it with
    /// [`build`](QueuePairBuilder::build) and bring it to ready with
    /// [`activate`](PreparedQueuePair::activate). Send with
    /// [`start_send`](QueuePair::start_send) and [`to(..)`](crate::SendBatch::to), receive with
    /// [`post_recv`](QueuePair::post_recv).
    pub fn create_srd_qp(
        &self,
        send: &CompletionQueue,
        recv: &CompletionQueue,
        port_num: u8,
    ) -> Result<QueuePairBuilder<Srd>> {
        self.create_qp::<Srd>(send, recv, port_num)
    }
}

impl QueuePairBuilder<Srd> {
    /// Create the EFA SRD queue pair described by this builder (`efadv_create_qp_ex`).
    ///
    /// It enables the send and one-sided RDMA doorbell operations; activate the result with
    /// [`activate`](PreparedQueuePair::activate).
    ///
    /// # Errors
    ///
    ///  - [`CreateQueuePair`](Error::CreateQueuePair): `efadv_create_qp_ex` failed (`EINVAL` for
    ///    an invalid value in the queue pair attributes, `ENOMEM` when out of resources).
    ///  - [`Unsupported`](Error::Unsupported): the provider declined the requested send
    ///    operations (`EOPNOTSUPP`), or created the queue pair without the extended work-request
    ///    interface this crate posts through (`ibv_qp_to_qp_ex` returned no handle).
    pub fn build(&self) -> Result<PreparedQueuePair<Srd>> {
        // SRD supports send and one-sided RDMA, including the immediate variants (the transport's
        // default set), or whatever the builder was told to request instead.
        let send_ops_flags = self.send_ops().0;

        // As in the generic `build_impl` in qp.rs: zero the storage and write only the fields the
        // driver reads, handing the pointer to C without `assume_init` (the `qp_type` enum has no
        // zero variant).
        let mut attr = std::mem::MaybeUninit::<ffi::ibv_qp_init_attr_ex>::zeroed();
        let p = attr.as_mut_ptr();
        unsafe {
            (*p).qp_context = self.ctx as usize as *mut c_void;
            (*p).send_cq = self.send.cq();
            (*p).recv_cq = self.recv.cq();
            (*p).cap = ffi::ibv_qp_cap {
                max_send_wr: self.max_send_wr,
                max_recv_wr: self.max_recv_wr,
                max_send_sge: self.max_send_sge,
                max_recv_sge: self.max_recv_sge,
                max_inline_data: self.max_inline_data,
            };
            (*p).qp_type = ffi::ibv_qp_type::IBV_QPT_DRIVER;
            (*p).comp_mask = ffi::ibv_qp_init_attr_mask::IBV_QP_INIT_ATTR_PD.0
                | ffi::ibv_qp_init_attr_mask::IBV_QP_INIT_ATTR_SEND_OPS_FLAGS.0;
            (*p).pd = self.pd.pd;
            (*p).send_ops_flags = send_ops_flags as u64;
        }

        // Zero the EFA attributes and set only the driver queue-pair type rather than naming every
        // field: rdma-core adds fields to this struct over time (64.0 added `wr_flags`), and the
        // provider reads only as much as the passed-in length covers.
        let mut efa_attr = std::mem::MaybeUninit::<ffi::efadv_qp_init_attr>::zeroed();
        unsafe {
            (*efa_attr.as_mut_ptr()).driver_qp_type = ffi::EFADV_QP_DRIVER_TYPE_SRD as u32;
        }
        let qp = unsafe {
            ffi::efadv_create_qp_ex(
                self.pd.ctx.ctx,
                attr.as_mut_ptr(),
                efa_attr.as_mut_ptr(),
                std::mem::size_of::<ffi::efadv_qp_init_attr>() as u32,
            )
        };
        if qp.is_null() {
            return Err(Error::os(
                io::Error::last_os_error(),
                Error::CreateQueuePair,
            ));
        }
        let qp_ex = unsafe { ffi::ibv_qp_to_qp_ex(qp) };
        let prepared = PreparedQueuePair {
            lid: self.port_attr.lid,
            port_num: self.port_num,
            qp: QueuePair {
                pd: self.pd.clone(),
                _srq: None,
                _send_cq: self.send.clone(),
                _recv_cq: self.recv.clone(),
                qp,
                qp_ex,
                _transport: std::marker::PhantomData,
            },
            gid_index: self.gid_index,
            traffic_class: self.traffic_class,
            access: None,
            timeout: None,
            retry_count: None,
            rnr_retry: None,
            min_rnr_timer: None,
            max_rd_atomic: None,
            max_dest_rd_atomic: None,
            path_mtu: None,
            psn: self.psn,
            service_level: self.service_level,
        };
        // As in qp.rs: a provider that accepted the send-operations mask without installing the
        // work-request table leaves `ibv_qp_to_qp_ex` returning NULL. Dropping `prepared` destroys
        // the queue pair.
        if qp_ex.is_null() {
            return Err(Error::Unsupported {
                operation: "ibv_qp_to_qp_ex",
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
    /// Post an RDMA WRITE of `payload` into `remote`, with the immediate set by
    /// [`imm`](Self::imm) if any.
    #[inline]
    pub fn write<'a>(self, wr_id: u64, payload: impl Into<Payload<'a>>, remote: RemoteMemorySlice) {
        let imm = self.op.imm;
        self.op.build(wr_id, payload.into(), move |q| unsafe {
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
