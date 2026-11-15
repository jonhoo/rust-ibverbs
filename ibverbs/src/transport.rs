//! The transports a queue pair can have, as marker types: [`Rc`], [`Uc`], [`Ud`], and (behind the
//! `efa` feature) `Srd`.
//!
//! The queue-pair family is generic over one of them (`QueuePair<transport::Rc>`), and the traits
//! here say which operations a transport has. Import the module rather than its items
//! (`use ibverbs::transport;`), so that [`Rc`] does not shadow [`std::rc::Rc`].

use crate::qp::{PreparedQueuePair, QueuePairBuilder, SendOps};

#[cfg(feature = "efa")]
pub use crate::efa::Srd;

#[cfg(doc)]
use crate::{AddressHandle, QueuePair, SendBatch, SendOp};

/// Seals the transport traits: only the markers defined by this crate implement them.
pub(crate) mod sealed {
    use super::{PreparedQueuePair, QueuePairBuilder, Transport};
    use crate::error::Result;
    use crate::qp::default_send_ops;

    /// Implemented by every transport marker.
    pub trait Sealed {
        /// The queue-pair type (`IBV_QPT_*`) the marker stands for.
        const TYPE: ffi::ibv_qp_type;

        /// The send operations a queue pair of this transport on `ctx` is created with unless
        /// the builder says otherwise: SEND on every transport, RDMA WRITE on the connected
        /// ones, and RDMA READ and the atomics on RC, the atomics only if the device executes
        /// them (`atomics`). SRD asks the device (see `efa.rs`).
        fn default_send_ops(_ctx: &crate::Context, atomics: bool) -> super::SendOps {
            default_send_ops(Self::TYPE, atomics)
        }

        /// Creates the queue pair `builder` describes, with `ibv_create_qp_ex` unless the
        /// transport's queue pairs come from a provider's own verb (SRD's, in `efa.rs`).
        fn create(builder: &QueuePairBuilder<Self>) -> Result<PreparedQueuePair<Self>>
        where
            Self: Transport + Sized,
        {
            builder.create_qp_ex()
        }
    }

    /// Implemented by the transports `ibv_create_qp` creates (every one but SRD), which have
    /// [`build_legacy`](QueuePairBuilder::build_legacy).
    pub trait Legacy: Sealed {}
}

/// A queue-pair transport marker: types the queue-pair family ([`QueuePairBuilder`],
/// [`PreparedQueuePair`], [`QueuePair`], [`SendOp`]) so that transport-specific operations only
/// exist on the transports that support them. Sealed; implemented by [`Rc`], [`Uc`], [`Ud`], and
/// `Srd` (behind the `efa` feature).
///
/// # Compile-time enforcement
///
/// Using a transport-specific operation on the wrong transport is a compile error, not a silent
/// no-op or a runtime provider error:
///
/// ```compile_fail,E0599
/// // An unreliable datagram has no ACK timeout: `set_ack_timeout` requires `T: Reliable`.
/// fn f(builder: &mut ibverbs::QueuePairBuilder<ibverbs::transport::Ud>) {
///     builder.set_ack_timeout(ibverbs::AckTimeout::INFINITE);
/// }
/// ```
///
/// ```compile_fail,E0599
/// // A connected transport addresses no individual sends: `to` requires `T: Datagram`.
/// fn f(qp: &mut ibverbs::QueuePair, ah: &ibverbs::AddressHandle) {
///     let mut batch = qp.start_send();
///     batch.to(ah, 1, 2);
/// }
/// ```
///
/// ```compile_fail,E0599
/// // A datagram work request is always addressed: its batch starts at `to`, and `op` requires
/// // `T: Connected`.
/// fn f(qp: &mut ibverbs::QueuePair<ibverbs::transport::Ud>) {
///     let mut batch = qp.start_send();
///     batch.op();
/// }
/// ```
///
/// ```compile_fail,E0599
/// // Only RC has reads and atomics for a fence to wait for: `fenced` requires `T: Reliable`.
/// fn f(qp: &mut ibverbs::QueuePair<ibverbs::transport::Uc>) {
///     let mut batch = qp.start_send();
///     let _ = batch.op().fenced();
/// }
/// ```
///
/// ```compile_fail,E0599
/// // A datagram queue pair has no remote endpoint to handshake with: use `activate`.
/// fn f(pqp: ibverbs::PreparedQueuePair<ibverbs::transport::Ud>, remote: ibverbs::QueuePairEndpoint) {
///     let _ = pqp.handshake(remote);
/// }
/// ```
pub trait Transport: sealed::Sealed {}

/// Implemented by the connected transports RC and UC, which are brought up against a single
/// remote endpoint with [`PreparedQueuePair::handshake`].
pub trait Connected: Transport {}

/// Implemented by the reliable connected transport RC, the only connected transport with
/// acknowledgements and retries and RDMA reads, and the only transport with atomics (EFA's SRD,
/// behind the `efa` feature, is reliable and has RDMA reads too, but is a datagram transport).
pub trait Reliable: Connected {}

/// Implemented by the datagram transports UD and SRD, which address each send individually with
/// an [`AddressHandle`] ([`SendBatch::to`]).
pub trait Datagram: Transport {}

/// The reliable-connection (RC) transport: connected to exactly one peer, in-order and reliable;
/// supports sends, RDMA read/write, and atomics.
#[derive(Debug, Clone, Copy)]
pub struct Rc;

/// The unreliable-connection (UC) transport: connected to exactly one peer, in order but without
/// delivery guarantees; supports sends and RDMA writes.
#[derive(Debug, Clone, Copy)]
pub struct Uc;

/// The unreliable-datagram (UD) transport: connectionless, each message fits in one MTU, and each
/// send is addressed individually with an [`AddressHandle`].
///
/// On EFA, mark every UD send [`signaled`](SendOp::signaled) and none
/// [`solicited`](SendOp::solicited): EFA refuses unsignaled and solicited work requests,
/// and the batch then fails at [`submit`](SendBatch::submit).
#[derive(Debug, Clone, Copy)]
pub struct Ud;

impl sealed::Sealed for Rc {
    const TYPE: ffi::ibv_qp_type = ffi::ibv_qp_type::IBV_QPT_RC;
}
impl Transport for Rc {}
impl Connected for Rc {}
impl Reliable for Rc {}
impl sealed::Legacy for Rc {}

impl sealed::Sealed for Uc {
    const TYPE: ffi::ibv_qp_type = ffi::ibv_qp_type::IBV_QPT_UC;
}
impl Transport for Uc {}
impl Connected for Uc {}
impl sealed::Legacy for Uc {}

impl sealed::Sealed for Ud {
    const TYPE: ffi::ibv_qp_type = ffi::ibv_qp_type::IBV_QPT_UD;
}
impl Transport for Ud {}
impl Datagram for Ud {}
impl sealed::Legacy for Ud {}
