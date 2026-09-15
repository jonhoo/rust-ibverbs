// AWS Elastic Fabric Adapter (EFA) support, behind the `efa` feature.
//
// EFA's transport is SRD (Scalable Reliable Datagram): reliable like RC but connectionless and
// addressed like UD. Only queue-pair *creation* is EFA-specific (`efadv_create_qp_ex`, the `Srd`
// marker's creation hook); the resulting queue pair carries the same extended send operations as
// every other queue pair, so sends go through the normal doorbell path (`QueuePair::start_send`
// with a `.to(..)`), receives through `QueuePair::post_recv`, and completions through
// `CompletionQueue::poll`.

use std::mem::MaybeUninit;

use crate::completion::CompletionQueue;
use crate::error::Result;
use crate::mr::{LocalMemorySlice, RemoteMemorySlice};
use crate::pd::ProtectionDomain;
use crate::qp::{sealed, Datagram, Payload, QueuePairBuilder, QueuePairType, SendOp, Transport};

#[cfg(doc)]
use crate::{AddressHandle, PreparedQueuePair, QueuePair};

/// The EFA SRD (Scalable Reliable Datagram) transport: reliable like RC but connectionless and
/// addressed like UD ([`QueuePairType::Driver`], created through
/// [`ProtectionDomain::create_srd_qp`]).
///
/// An SRD queue pair is built through `efadv_create_qp_ex` with the builder's send operations
/// (by default SEND and one-sided RDMA write and read, with their immediate variants), activated
/// with [`activate`](PreparedQueuePair::activate) like a UD one, and posts SEND, RDMA WRITE, and
/// RDMA READ requests, each addressed with an [`AddressHandle`].
#[derive(Debug, Clone, Copy)]
pub struct Srd;

impl sealed::Sealed for Srd {
    const TYPE: QueuePairType = QueuePairType::Driver;
    const CREATE_VERB: &'static str = "efadv_create_qp_ex";

    unsafe fn create(
        ctx: *mut ffi::ibv_context,
        attr: *mut ffi::ibv_qp_init_attr_ex,
    ) -> *mut ffi::ibv_qp {
        // Zero the EFA attributes and set only the driver queue-pair type rather than naming every
        // field: rdma-core adds fields to this struct over time (64.0 added `wr_flags`), and the
        // provider reads only as much as the passed-in length covers.
        let mut efa_attr = MaybeUninit::<ffi::efadv_qp_init_attr>::zeroed();
        unsafe {
            (*efa_attr.as_mut_ptr()).driver_qp_type = ffi::EFADV_QP_DRIVER_TYPE_SRD as u32;
            ffi::efadv_create_qp_ex(
                ctx,
                attr,
                efa_attr.as_mut_ptr(),
                std::mem::size_of::<ffi::efadv_qp_init_attr>() as u32,
            )
        }
    }
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

impl SendOp<'_, '_, Srd> {
    /// Post an RDMA WRITE of `payload` into `remote`, with the immediate set by
    /// [`imm`](Self::imm) if any.
    ///
    /// Registered memory converts implicitly (`write(id, &[mr.slice(..)], remote)`); inline data
    /// is spelled out (`write(id, Payload::Inline(b"ping"), remote)`). See [`Payload`] for the
    /// lifetime and capacity requirements of each.
    #[inline]
    pub fn write<'a>(self, wr_id: u64, payload: impl Into<Payload<'a>>, remote: RemoteMemorySlice) {
        self.rdma_write(wr_id, payload, remote)
    }

    /// Post an RDMA READ from `remote` into `local`.
    #[inline]
    pub fn read(self, wr_id: u64, local: &[LocalMemorySlice], remote: RemoteMemorySlice) {
        self.rdma_read(wr_id, local, remote)
    }
}
