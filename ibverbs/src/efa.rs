// ---------------------------------------------------------------------------
// AWS Elastic Fabric Adapter (EFA) support, behind the `efa` feature.
//
// EFA's transport is SRD (Scalable Reliable Datagram): reliable like RC but connectionless and
// addressed like UD. Only queue-pair *creation* is EFA-specific (`efadv_create_qp_ex`); the resulting
// queue pair carries the same extended send operations as every other queue pair, so sends go
// through the normal doorbell path ([`QueuePair::start_send`] with a `.to(..)`), receives through
// [`QueuePair::post_recv`], and completions through [`CompletionQueue::poll`].
// ---------------------------------------------------------------------------

use crate::address::AddressHandle;
use crate::context::Context;
use crate::error::{Error, Result, nonnull};
use crate::mr::{LocalMemorySlice, RemoteMemorySlice};
use crate::qp::{
    Datagram, Payload, PreparedQueuePair, QueuePairBuilder, SendOp, SendOps, Transport, sealed,
};

#[cfg(doc)]
use crate::{ProtectionDomain, QueuePair, WorkCompletion};

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

    /// SEND, plus the RDMA operations the device has: EFA refuses to create a queue pair that
    /// requests one it lacks. On anything but an EFA device (where creation fails anyway), SEND.
    fn default_send_ops(ctx: &Context, _atomics: bool) -> SendOps {
        let mut ops = SendOps::SEND | SendOps::SEND_WITH_IMM;
        if let Ok(attr) = ctx.efa_device_attr() {
            if attr.has_rdma_read() {
                ops |= SendOps::RDMA_READ;
            }
            if attr.has_rdma_write() {
                ops |= SendOps::RDMA_WRITE | SendOps::RDMA_WRITE_WITH_IMM;
            }
        }
        ops
    }
}
impl Transport for Srd {}
impl Datagram for Srd {}

impl QueuePairBuilder<Srd> {
    /// Create the EFA SRD queue pair described by this builder (`efadv_create_qp_ex`).
    ///
    /// The queue pair is created with the builder's send operations
    /// ([`send_ops`](QueuePairBuilder::send_ops)): by default SEND, with and without an
    /// immediate, plus RDMA READ and RDMA WRITE (with and without an immediate) where the device
    /// has them ([`Context::efa_device_attr`]). EFA installs only the operations requested, and
    /// refuses a request for one the device lacks. Activate the result with
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

/// The EFA-specific limits and capabilities of a device, as returned by
/// [`Context::efa_device_attr`] (`efadv_query_device`).
///
/// EFA sizes its queues differently from what the generic device attributes suggest (send and
/// receive queues have separate limits, and so do RDMA transfers), and which operations an SRD
/// queue pair can be created with depends on the device generation.
#[derive(Clone, Copy)]
pub struct EfaDeviceAttr(ffi::efadv_device_attr);

impl EfaDeviceAttr {
    /// The most work requests a send queue holds.
    pub fn max_sq_wr(&self) -> u32 {
        self.0.max_sq_wr
    }

    /// The most scatter/gather entries one receive queue holds across all of its work requests:
    /// a queue pair built for `n` entries per receive takes up to `max_rq_wr / n` receives.
    pub fn max_rq_wr(&self) -> u32 {
        self.0.max_rq_wr
    }

    /// The most scatter/gather entries of a SEND.
    pub fn max_sq_sge(&self) -> u16 {
        self.0.max_sq_sge
    }

    /// The most scatter/gather entries of a receive.
    pub fn max_rq_sge(&self) -> u16 {
        self.0.max_rq_sge
    }

    /// The largest inline payload ([`QueuePairBuilder::set_max_inline_data`]) of a queue pair with
    /// work requests of the standard size. Devices that take more report it in
    /// `inline_buf_size_ex` ([`as_raw`](Self::as_raw), rdma-core 63); EFA then doubles the size
    /// of every send work request, which halves the deepest send queue it can create.
    pub fn inline_buf_size(&self) -> u16 {
        self.0.inline_buf_size
    }

    /// The largest RDMA READ or RDMA WRITE, in bytes; 0 on a device with neither.
    pub fn max_rdma_size(&self) -> u32 {
        self.0.max_rdma_size
    }

    /// Whether SRD queue pairs can issue RDMA READs, and memory can be registered with
    /// [`AccessFlags::REMOTE_READ`](crate::AccessFlags::REMOTE_READ).
    pub fn has_rdma_read(&self) -> bool {
        self.has(ffi::EFADV_DEVICE_ATTR_CAPS_RDMA_READ.0)
    }

    /// Whether SRD queue pairs can issue RDMA WRITEs, and memory can be registered with
    /// [`AccessFlags::REMOTE_WRITE`](crate::AccessFlags::REMOTE_WRITE).
    pub fn has_rdma_write(&self) -> bool {
        self.has(ffi::EFADV_DEVICE_ATTR_CAPS_RDMA_WRITE.0)
    }

    /// Whether an SRD queue pair's RNR retry count can be set
    /// ([`QueuePairAttribute::set_rnr_retry`](crate::QueuePairAttribute::set_rnr_retry) on the
    /// transition to `RTS`).
    pub fn has_rnr_retry(&self) -> bool {
        self.has(ffi::EFADV_DEVICE_ATTR_CAPS_RNR_RETRY.0)
    }

    fn has(&self, cap: u32) -> bool {
        self.0.device_caps & cap != 0
    }

    /// The underlying `efadv_device_attr`, whose `device_caps` holds the
    /// `EFADV_DEVICE_ATTR_CAPS_*` bits. Escape hatch for what this crate does not wrap.
    pub fn as_raw(&self) -> &ffi::efadv_device_attr {
        &self.0
    }
}

impl std::fmt::Debug for EfaDeviceAttr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EfaDeviceAttr")
            .field("max_sq_wr", &self.max_sq_wr())
            .field("max_rq_wr", &self.max_rq_wr())
            .field("max_sq_sge", &self.max_sq_sge())
            .field("max_rq_sge", &self.max_rq_sge())
            .field("inline_buf_size", &self.inline_buf_size())
            .field("max_rdma_size", &self.max_rdma_size())
            .field("device_caps", &format_args!("{:#x}", self.0.device_caps))
            .finish()
    }
}

impl Context {
    /// Query the EFA-specific limits and capabilities of this context's device
    /// (`efadv_query_device`). The provider answers from what it read when the device was opened,
    /// so this makes no system call.
    ///
    /// # Errors
    ///
    ///  - [`Unsupported`](Error::Unsupported): the device is not an EFA device (`EOPNOTSUPP`).
    ///  - [`QueryDevice`](Error::QueryDevice): `efadv_query_device` failed.
    pub fn efa_device_attr(&self) -> Result<EfaDeviceAttr> {
        // rdma-core adds fields to this struct over time; the provider fills only as much as the
        // passed-in length covers, and an older one leaves the fields it does not know zeroed.
        let mut attr = ffi::efadv_device_attr::default();
        let errno = unsafe {
            ffi::efadv_query_device(
                self.as_raw(),
                &mut attr,
                std::mem::size_of::<ffi::efadv_device_attr>() as u32,
            )
        };
        if errno != 0 {
            return Err(
                Error::errno(errno, Error::QueryDevice).for_operation(&"efadv_query_device")
            );
        }
        Ok(EfaDeviceAttr(attr))
    }
}

impl AddressHandle {
    /// The number the EFA device knows this address handle by (`efadv_query_ah`), or `None` on a
    /// device that is not EFA.
    ///
    /// EFA has no LIDs. A receive completion names the sender by the number of the address handle
    /// the receiving side holds for the sender's GID ([`WorkCompletion::slid`]), and by `0xffff`
    /// when it holds none: keep a map from this number to the peer to tell who sent a datagram.
    pub fn efa_ah_number(&self) -> Option<u16> {
        let mut attr = ffi::efadv_ah_attr::default();
        let errno = unsafe {
            ffi::efadv_query_ah(
                self.as_raw(),
                &mut attr,
                std::mem::size_of::<ffi::efadv_ah_attr>() as u32,
            )
        };
        (errno == 0).then_some(attr.ahn)
    }
}

impl SendOp<'_, '_, Srd> {
    /// Post an RDMA WRITE of `local` into `remote`, with the immediate set by [`imm`](Self::imm)
    /// if any.
    ///
    /// The data comes from one piece of registered memory: an EFA RDMA work request holds a
    /// single local scatter/gather entry, and EFA writes inline data only on a queue pair created
    /// with its inline-write flag (`EFADV_QP_FLAGS_INLINE_WRITE`), which doubles the size of every
    /// send work request and needs rdma-core 63, so this crate does not set it.
    ///
    /// # Panics
    ///
    /// Panics if the queue pair was built without room for a scatter/gather entry
    /// ([`QueuePairBuilder::set_max_send_sge`]).
    #[inline]
    pub fn write(self, wr_id: u64, local: LocalMemorySlice, remote: RemoteMemorySlice) {
        let imm = self.imm;
        let op = Self::write_op(imm, remote);
        self.build(wr_id, Payload::Sges(std::slice::from_ref(&local)), op)
    }

    /// Post an RDMA READ from `remote` into `local`, one piece of registered memory (an EFA RDMA
    /// work request holds a single local scatter/gather entry).
    ///
    /// # Panics
    ///
    /// Panics if the queue pair was built without room for a scatter/gather entry
    /// ([`QueuePairBuilder::set_max_send_sge`]).
    #[inline]
    pub fn read(self, wr_id: u64, local: LocalMemorySlice, remote: RemoteMemorySlice) {
        let op = Self::read_op(remote);
        self.build(wr_id, Payload::Sges(std::slice::from_ref(&local)), op)
    }
}
