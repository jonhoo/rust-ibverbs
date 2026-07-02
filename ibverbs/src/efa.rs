// ---------------------------------------------------------------------------
// AWS Elastic Fabric Adapter (EFA) support, behind the `efa` feature.
//
// EFA's transport is SRD (Scalable Reliable Datagram): reliable like RC but connectionless and
// addressed like UD. Only queue-pair *creation* is EFA-specific (`efadv_create_qp_ex`); the resulting
// queue pair is created with the same extended send operations as every other, so sends go through
// the normal doorbell path ([`QueuePair::post_send_ud`] / [`QueuePair::start_send`] with a `.to(..)`),
// receives through [`QueuePair::post_receive`], and completions through [`CompletionQueue::poll`].
// ---------------------------------------------------------------------------

use std::io;
use std::os::raw::c_void;
use std::ptr;

use crate::completion::CompletionQueue;
use crate::error::{Error, Result};
use crate::pd::ProtectionDomain;
use crate::qp::{PreparedQueuePair, QueuePair, QueuePairBuilder};

#[cfg(doc)]
use crate::AddressHandle;

#[cfg(feature = "efa")]
impl ProtectionDomain {
    /// Begin building an EFA SRD queue pair associated with this protection domain.
    ///
    /// Configure it like any other queue pair (GID index, queue/SGE limits), then create it with
    /// [`build_srd`](QueuePairBuilder::build_srd) and bring it to ready with
    /// [`activate_srd`](PreparedQueuePair::activate_srd). Send with
    /// [`post_send_ud`](QueuePair::post_send_ud) (or `start_send().to(..)`), receive with
    /// [`post_receive`](QueuePair::post_receive).
    pub fn create_srd_qp(
        &self,
        send: &CompletionQueue,
        recv: &CompletionQueue,
    ) -> Result<QueuePairBuilder> {
        self.create_qp(send, recv, ffi::ibv_qp_type::IBV_QPT_DRIVER)
    }
}

#[cfg(feature = "efa")]
impl QueuePairBuilder {
    /// Create the EFA SRD queue pair described by this builder (`efadv_create_qp_ex`).
    ///
    /// Like [`build`](Self::build) but for SRD: it enables the send and one-sided RDMA doorbell
    /// operations, then activate it with [`activate_srd`](PreparedQueuePair::activate_srd).
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: invalid value in the queue pair attributes.
    ///  - `ENOMEM`: not enough resources to complete this operation.
    pub fn build_srd(&self) -> Result<PreparedQueuePair> {
        use ffi::ibv_qp_create_send_ops_flags as SendOps;
        // SRD supports send and one-sided RDMA, including the immediate variants.
        let send_ops_flags = SendOps::IBV_QP_EX_WITH_SEND.0
            | SendOps::IBV_QP_EX_WITH_SEND_WITH_IMM.0
            | SendOps::IBV_QP_EX_WITH_RDMA_WRITE.0
            | SendOps::IBV_QP_EX_WITH_RDMA_WRITE_WITH_IMM.0
            | SendOps::IBV_QP_EX_WITH_RDMA_READ.0;

        // As in `build`: zero the storage and write only the fields the driver reads, handing the
        // pointer to C without `assume_init` (the `qp_type` enum has no zero variant).
        let mut attr = std::mem::MaybeUninit::<ffi::ibv_qp_init_attr_ex>::zeroed();
        let p = attr.as_mut_ptr();
        unsafe {
            (*p).qp_context = ptr::null::<c_void>().offset(self.ctx) as *mut _;
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

        let mut efa_attr = ffi::efadv_qp_init_attr {
            comp_mask: 0,
            driver_qp_type: ffi::EFADV_QP_DRIVER_TYPE_SRD as u32,
            flags: 0,
            sl: 0,
            reserved: 0,
        };
        let qp = unsafe {
            ffi::efadv_create_qp_ex(
                self.pd.ctx.ctx,
                attr.as_mut_ptr(),
                &mut efa_attr as *mut _,
                std::mem::size_of::<ffi::efadv_qp_init_attr>() as u32,
            )
        };
        if qp.is_null() {
            return Err(Error::CreateQueuePair(io::Error::last_os_error()));
        }
        let qp_ex = unsafe { ffi::ibv_qp_to_qp_ex(qp) };
        Ok(PreparedQueuePair {
            lid: self.port_attr.lid,
            port_num: self.port_num,
            qp: QueuePair {
                pd: self.pd.clone(),
                _srq: None,
                _send_cq: self.send.clone(),
                _recv_cq: self.recv.clone(),
                qp,
                qp_ex,
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
            rq_psn: None,
            service_level: self.service_level,
        })
    }
}

#[cfg(feature = "efa")]
impl PreparedQueuePair {
    /// Transition an EFA SRD queue pair to ready with the given Q_Key.
    ///
    /// SRD is connectionless, so this needs no remote endpoint; the transitions are the same as a UD
    /// queue pair's. Address each send with an [`AddressHandle`] (see
    /// [`post_send_ud`](QueuePair::post_send_ud)).
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: invalid value provided in `attr` or `attr_mask`.
    ///  - `ENOMEM`: not enough resources to complete this operation.
    pub fn activate_srd(self, qkey: u32) -> Result<QueuePair> {
        self.activate_ud(qkey)
    }
}
