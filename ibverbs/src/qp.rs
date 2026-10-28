use std::io;
use std::os::raw::c_void;
use std::ptr;
use std::time::Duration;

use crate::address::Gid;
use crate::address::{AddressHandle, AddressHandleAttribute};
use crate::completion::CompletionQueue;
use crate::context::Mtu;
use crate::error::{Error, Result, destroyed, keep, nonnull};
use crate::mr::{AccessFlags, LocalMemorySlice, RemoteMemorySlice};
use crate::pd::ProtectionDomain;
use crate::srq::SharedReceiveQueue;

/// Seals the transport traits: only the markers defined by this crate implement them.
pub(crate) mod sealed {
    /// Implemented by every transport marker.
    pub trait Sealed {
        /// The queue-pair type (`IBV_QPT_*`) the marker stands for.
        const TYPE: ffi::ibv_qp_type;
    }
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
/// // An unreliable datagram has no ACK timeout: `set_timeout` requires `T: Reliable`.
/// fn f(builder: &mut ibverbs::QueuePairBuilder<ibverbs::Ud>) {
///     builder.set_timeout(ibverbs::AckTimeout::INFINITE);
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
/// fn f(qp: &mut ibverbs::QueuePair<ibverbs::Ud>) {
///     let mut batch = qp.start_send();
///     batch.op();
/// }
/// ```
///
/// ```compile_fail,E0599
/// // Only RC has reads and atomics for a fence to wait for: `fenced` requires `T: Reliable`.
/// fn f(qp: &mut ibverbs::QueuePair<ibverbs::Uc>) {
///     let mut batch = qp.start_send();
///     let _ = batch.op().fenced();
/// }
/// ```
///
/// ```compile_fail,E0599
/// // A datagram queue pair has no remote endpoint to handshake with: use `activate`.
/// fn f(pqp: ibverbs::PreparedQueuePair<ibverbs::Ud>, remote: ibverbs::QueuePairEndpoint) {
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
///
/// Note that this marker shadows [`std::rc::Rc`] when glob-imported (`use ibverbs::*`).
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

impl sealed::Sealed for Uc {
    const TYPE: ffi::ibv_qp_type = ffi::ibv_qp_type::IBV_QPT_UC;
}
impl Transport for Uc {}
impl Connected for Uc {}

impl sealed::Sealed for Ud {
    const TYPE: ffi::ibv_qp_type = ffi::ibv_qp_type::IBV_QPT_UD;
}
impl Transport for Ud {}
impl Datagram for Ud {}

/// The state of a queue pair's state machine.
///
/// Set through [`QueuePairAttribute::set_state`] + [`QueuePair::modify`] and read back by
/// [`QueuePair::query`]. [`PreparedQueuePair::handshake`] and friends drive the
/// `Reset -> Init -> ReadyToReceive -> ReadyToSend` bring-up for you.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum QueuePairState {
    /// The newly created queue pair: posting work requests is an error.
    Reset,
    /// Initialized: receives can be posted, but nothing is processed yet.
    Init,
    /// Ready to receive ("RTR"): incoming messages are processed.
    ReadyToReceive,
    /// Ready to send ("RTS"): the fully operational state.
    ReadyToSend,
    /// The send queue is draining ("SQD"): posted sends finish, new ones wait.
    SendQueueDrain,
    /// The send queue errored ("SQE"): receives still work, sends are flushed (UD and UC only;
    /// RC moves straight to [`Error`](Self::Error)).
    SendQueueError,
    /// The error state: outstanding and new work requests are flushed with
    /// [`WcStatus::WorkRequestFlushed`](crate::WcStatus::WorkRequestFlushed).
    Error,
    /// The state cannot be determined.
    Unknown,
}

impl From<ffi::ibv_qp_state> for QueuePairState {
    fn from(state: ffi::ibv_qp_state) -> Self {
        use ffi::ibv_qp_state as S;
        match state {
            S::IBV_QPS_RESET => QueuePairState::Reset,
            S::IBV_QPS_INIT => QueuePairState::Init,
            S::IBV_QPS_RTR => QueuePairState::ReadyToReceive,
            S::IBV_QPS_RTS => QueuePairState::ReadyToSend,
            S::IBV_QPS_SQD => QueuePairState::SendQueueDrain,
            S::IBV_QPS_SQE => QueuePairState::SendQueueError,
            S::IBV_QPS_ERR => QueuePairState::Error,
            _ => QueuePairState::Unknown,
        }
    }
}

impl From<QueuePairState> for ffi::ibv_qp_state {
    fn from(state: QueuePairState) -> Self {
        use ffi::ibv_qp_state as S;
        match state {
            QueuePairState::Reset => S::IBV_QPS_RESET,
            QueuePairState::Init => S::IBV_QPS_INIT,
            QueuePairState::ReadyToReceive => S::IBV_QPS_RTR,
            QueuePairState::ReadyToSend => S::IBV_QPS_RTS,
            QueuePairState::SendQueueDrain => S::IBV_QPS_SQD,
            QueuePairState::SendQueueError => S::IBV_QPS_SQE,
            QueuePairState::Error => S::IBV_QPS_ERR,
            QueuePairState::Unknown => S::IBV_QPS_UNKNOWN,
        }
    }
}

impl std::fmt::Display for QueuePairState {
    /// Formats the state as it is named in the InfiniBand specification (and the C headers), for
    /// example `RTS` for [`ReadyToSend`](Self::ReadyToSend).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            QueuePairState::Reset => "RESET",
            QueuePairState::Init => "INIT",
            QueuePairState::ReadyToReceive => "RTR",
            QueuePairState::ReadyToSend => "RTS",
            QueuePairState::SendQueueDrain => "SQD",
            QueuePairState::SendQueueError => "SQE",
            QueuePairState::Error => "ERR",
            QueuePairState::Unknown => "UNKNOWN",
        };
        f.write_str(name)
    }
}

flags_newtype! {
    /// The set of queue-pair attributes present in a [`QueuePairAttribute`] (the `IBV_QP_*` mask
    /// bits): what to apply in a [`QueuePair::modify`] or read in a [`QueuePair::query`].
    ///
    /// Each `set_*` method on [`QueuePairAttribute`] records its bit automatically; the mask is
    /// spelled out explicitly only for [`QueuePair::query`] and in the
    /// [`InvalidQueuePairAttributeMask`](crate::Error::InvalidQueuePairAttributeMask) error.
    pub struct QueuePairAttributeMask(ffi::ibv_qp_attr_mask) {
        /// The queue-pair state ([`QueuePairAttribute::set_state`]).
        STATE = IBV_QP_STATE;
        /// The assumed current state ([`QueuePairAttribute::set_current_state`]).
        CUR_STATE = IBV_QP_CUR_STATE;
        /// Asynchronous notification when a send-queue drain completes.
        EN_SQD_ASYNC_NOTIFY = IBV_QP_EN_SQD_ASYNC_NOTIFY;
        /// The remote-access flags ([`QueuePairAttribute::set_access_flags`]).
        ACCESS_FLAGS = IBV_QP_ACCESS_FLAGS;
        /// The partition-key index ([`QueuePairAttribute::set_pkey_index`]).
        PKEY_INDEX = IBV_QP_PKEY_INDEX;
        /// The physical port ([`QueuePairAttribute::set_port`]).
        PORT = IBV_QP_PORT;
        /// The Q_Key ([`QueuePairAttribute::set_qkey`]).
        QKEY = IBV_QP_QKEY;
        /// The primary path's address vector ([`QueuePairAttribute::set_address_vector`]).
        AV = IBV_QP_AV;
        /// The path MTU ([`QueuePairAttribute::set_path_mtu`]).
        PATH_MTU = IBV_QP_PATH_MTU;
        /// The ACK timeout ([`QueuePairAttribute::set_timeout`]).
        TIMEOUT = IBV_QP_TIMEOUT;
        /// The retry count ([`QueuePairAttribute::set_retry_count`]).
        RETRY_CNT = IBV_QP_RETRY_CNT;
        /// The RNR retry count ([`QueuePairAttribute::set_rnr_retry`]).
        RNR_RETRY = IBV_QP_RNR_RETRY;
        /// The receive-queue packet sequence number ([`QueuePairAttribute::set_rq_psn`]).
        RQ_PSN = IBV_QP_RQ_PSN;
        /// The number of outstanding RDMA reads/atomics as the initiator
        /// ([`QueuePairAttribute::set_max_rd_atomic`]).
        MAX_QP_RD_ATOMIC = IBV_QP_MAX_QP_RD_ATOMIC;
        /// The alternate path.
        ALT_PATH = IBV_QP_ALT_PATH;
        /// The minimum RNR-NAK timer ([`QueuePairAttribute::set_min_rnr_timer`]).
        MIN_RNR_TIMER = IBV_QP_MIN_RNR_TIMER;
        /// The send-queue packet sequence number ([`QueuePairAttribute::set_sq_psn`]).
        SQ_PSN = IBV_QP_SQ_PSN;
        /// The number of outstanding RDMA reads/atomics as the destination
        /// ([`QueuePairAttribute::set_max_dest_rd_atomic`]).
        MAX_DEST_RD_ATOMIC = IBV_QP_MAX_DEST_RD_ATOMIC;
        /// The path migration state.
        PATH_MIG_STATE = IBV_QP_PATH_MIG_STATE;
        /// The queue-pair capacities.
        CAP = IBV_QP_CAP;
        /// The destination queue-pair number ([`QueuePairAttribute::set_dest_qp_num`]).
        DEST_QPN = IBV_QP_DEST_QPN;
        /// The rate limit, in kbps (raw-packet queue pairs).
        RATE_LIMIT = IBV_QP_RATE_LIMIT;
    }
}

/// The ACK timeout of a reliable-connection queue pair: how long the sender waits for an ACK/NACK
/// from the remote queue pair before retransmitting. Set with [`QueuePairBuilder::set_timeout`].
///
/// The wire encoding is a 5-bit exponent: the device supports exactly the timeouts
/// `4.096 µs × 2^n` for `n` in `1..=31` — from 8.192 µs up to about 2.4 hours — plus
/// [`INFINITE`](Self::INFINITE) (never time out). Construct a timeout from a [`Duration`] with
/// [`at_least`](Self::at_least), which rounds up to the next representable step, or from a raw
/// encoding with [`from_exponent`](Self::from_exponent).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckTimeout(u8);

impl AckTimeout {
    /// Wait forever for the ACK/NACK (the encoding `0`).
    ///
    /// Useful for debugging: if a packet is lost and no ACK or NACK arrives, no retry ever
    /// occurs and the queue pair just stops sending data.
    pub const INFINITE: AckTimeout = AckTimeout(0);

    /// The smallest exponent of a finite timeout (`4.096 µs × 2^1`).
    const MIN_EXPONENT: u8 = 1;
    /// The largest representable exponent (`4.096 µs × 2^31`).
    const MAX_EXPONENT: u8 = 31;

    /// The effective timeout of exponent `n`: `4.096 µs × 2^n`.
    fn step(n: u8) -> Duration {
        // 4.096 µs = 4096 ns; shifted by at most 31, this stays far below u64::MAX nanoseconds.
        Duration::from_nanos(4096u64 << n)
    }

    /// The smallest representable timeout that is at least `d`.
    ///
    /// Rounds `d` up to the next step `4.096 µs × 2^n` (`n` in `1..=31`): a duration below the
    /// smallest step becomes the smallest step (8.192 µs), and one beyond the largest step clamps
    /// to it (`n = 31`, about 2.4 hours). This never returns [`INFINITE`](Self::INFINITE).
    pub fn at_least(d: Duration) -> AckTimeout {
        for n in Self::MIN_EXPONENT..=Self::MAX_EXPONENT {
            if Self::step(n) >= d {
                return AckTimeout(n);
            }
        }
        AckTimeout(Self::MAX_EXPONENT)
    }

    /// The timeout with the raw 5-bit wire encoding `n` (the exponent in `4.096 µs × 2^n`), for
    /// code ported from C. The encoding `0` is [`INFINITE`](Self::INFINITE).
    ///
    /// # Panics
    ///
    /// Panics if `n > 31` (the encoding is 5 bits).
    pub fn from_exponent(n: u8) -> AckTimeout {
        assert!(
            n <= Self::MAX_EXPONENT,
            "an ACK timeout encoding is 5 bits (0..=31), got {n}"
        );
        AckTimeout(n)
    }

    /// The effective timeout, or `None` for [`INFINITE`](Self::INFINITE).
    pub fn duration(&self) -> Option<Duration> {
        (self.0 != 0).then(|| Self::step(self.0))
    }

    /// The raw 5-bit wire encoding (the exponent in `4.096 µs × 2^n`; `0` is infinite).
    pub fn exponent(&self) -> u8 {
        self.0
    }
}

impl std::fmt::Display for AckTimeout {
    /// Formats the effective timeout (for example `65.536µs`), or `infinite`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.duration() {
            None => f.write_str("infinite"),
            Some(d) => write!(f, "{d:?}"),
        }
    }
}

/// The minimum RNR NAK delay of a reliable-connection queue pair: the wait the queue pair demands
/// of its peer, in each receiver-not-ready NAK it sends, before the peer retries a send that found
/// no receive posted. Set with [`QueuePairBuilder::set_min_rnr_timer`].
///
/// The wire encoding is 5 bits naming one of 32 discrete delays from 0.01 ms to 655.36 ms, and it
/// is *not* monotonic: the encoding `0` names the largest delay (655.36 ms), while `1..=31` run in
/// increasing order from 0.01 ms to 491.52 ms. Construct a delay from a [`Duration`] with
/// [`at_least`](Self::at_least), which rounds up to the next representable delay, or from a raw
/// encoding with [`from_encoding`](Self::from_encoding).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RnrTimer(u8);

impl RnrTimer {
    /// The discrete delays the wire encoding can express, in microseconds, sorted ascending, each
    /// with its encoding. The encoding is non-monotonic: `0` names the *largest* delay.
    #[rustfmt::skip]
    const DELAYS: [(u64, u8); 32] = [
        (10, 1), (20, 2), (30, 3), (40, 4),
        (60, 5), (80, 6), (120, 7), (160, 8),
        (240, 9), (320, 10), (480, 11), (640, 12),
        (960, 13), (1_280, 14), (1_920, 15), (2_560, 16),
        (3_840, 17), (5_120, 18), (7_680, 19), (10_240, 20),
        (15_360, 21), (20_480, 22), (30_720, 23), (40_960, 24),
        (61_440, 25), (81_920, 26), (122_880, 27), (163_840, 28),
        (245_760, 29), (327_680, 30), (491_520, 31), (655_360, 0),
    ];

    /// The smallest representable delay that is at least `d`.
    ///
    /// Rounds `d` up to the next of the discrete delays: a duration below the smallest becomes
    /// the smallest (0.01 ms), and one beyond the largest clamps to it (655.36 ms).
    pub fn at_least(d: Duration) -> RnrTimer {
        for (micros, encoding) in Self::DELAYS {
            if Duration::from_micros(micros) >= d {
                return RnrTimer(encoding);
            }
        }
        // Beyond the largest delay: clamp to it (655.36 ms, the encoding 0).
        RnrTimer(0)
    }

    /// The delay with the raw 5-bit wire encoding `v`, for code ported from C. Note that the
    /// encoding `0` names the *largest* delay (655.36 ms), not the smallest.
    ///
    /// # Panics
    ///
    /// Panics if `v > 31` (the encoding is 5 bits).
    pub fn from_encoding(v: u8) -> RnrTimer {
        assert!(
            v <= 31,
            "an RNR NAK timer encoding is 5 bits (0..=31), got {v}"
        );
        RnrTimer(v)
    }

    /// The effective delay.
    pub fn duration(&self) -> Duration {
        let (micros, _) = Self::DELAYS
            .iter()
            .find(|&&(_, encoding)| encoding == self.0)
            .expect("every 5-bit encoding names a delay");
        Duration::from_micros(*micros)
    }

    /// The raw 5-bit wire encoding.
    pub fn encoding(&self) -> u8 {
        self.0
    }
}

impl std::fmt::Display for RnrTimer {
    /// Formats the effective delay (for example `2.56ms`).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.duration())
    }
}

flags_newtype! {
    /// The send operations a queue pair is created with (`ibv_qp_create_send_ops_flags`): the
    /// doorbell opcodes its send queue will accept. Set with [`QueuePairBuilder::set_send_ops`];
    /// the builder's default depends on the transport (see [`QueuePairBuilder::send_ops`]).
    pub struct SendOps(ffi::ibv_qp_create_send_ops_flags) {
        /// RDMA WRITE.
        RDMA_WRITE = IBV_QP_EX_WITH_RDMA_WRITE;
        /// RDMA WRITE with an immediate.
        RDMA_WRITE_WITH_IMM = IBV_QP_EX_WITH_RDMA_WRITE_WITH_IMM;
        /// SEND.
        SEND = IBV_QP_EX_WITH_SEND;
        /// SEND with an immediate.
        SEND_WITH_IMM = IBV_QP_EX_WITH_SEND_WITH_IMM;
        /// RDMA READ.
        RDMA_READ = IBV_QP_EX_WITH_RDMA_READ;
        /// Atomic compare-and-swap.
        ATOMIC_CMP_AND_SWP = IBV_QP_EX_WITH_ATOMIC_CMP_AND_SWP;
        /// Atomic fetch-and-add.
        ATOMIC_FETCH_AND_ADD = IBV_QP_EX_WITH_ATOMIC_FETCH_AND_ADD;
        /// Local invalidation of a memory key.
        LOCAL_INV = IBV_QP_EX_WITH_LOCAL_INV;
        /// Memory-window bind.
        BIND_MW = IBV_QP_EX_WITH_BIND_MW;
        /// SEND with invalidate.
        SEND_WITH_INV = IBV_QP_EX_WITH_SEND_WITH_INV;
        /// TCP segmentation offload (raw-packet queue pairs).
        TSO = IBV_QP_EX_WITH_TSO;
        /// Flush.
        FLUSH = IBV_QP_EX_WITH_FLUSH;
        /// Atomic write.
        ATOMIC_WRITE = IBV_QP_EX_WITH_ATOMIC_WRITE;
    }
}

/// The send operations a queue pair of `qp_type` is created with unless the builder says
/// otherwise: SEND on every transport, RDMA WRITE on the connected transports and SRD, RDMA READ
/// on RC and SRD, and the atomics on RC only.
fn default_send_ops(qp_type: ffi::ibv_qp_type) -> SendOps {
    use ffi::ibv_qp_type as T;
    let mut ops = SendOps::SEND | SendOps::SEND_WITH_IMM;
    if matches!(qp_type, T::IBV_QPT_RC | T::IBV_QPT_UC | T::IBV_QPT_DRIVER) {
        ops |= SendOps::RDMA_WRITE | SendOps::RDMA_WRITE_WITH_IMM;
    }
    if matches!(qp_type, T::IBV_QPT_RC | T::IBV_QPT_DRIVER) {
        ops |= SendOps::RDMA_READ;
    }
    if qp_type == T::IBV_QPT_RC {
        ops |= SendOps::ATOMIC_CMP_AND_SWP | SendOps::ATOMIC_FETCH_AND_ADD;
    }
    ops
}

/// An unconfigured `QueuePair`. Created by [`ProtectionDomain::create_qp`].
///
/// A `QueuePairBuilder` is used to configure a `QueuePair` before it is allocated and initialized.
/// The setters that only make sense on some transports only exist for those transports (see
/// [`Transport`]). See also [RDMAmojo] for many more details.
///
/// [RDMAmojo]: http://www.rdmamojo.com/2013/01/12/ibv_modify_qp/
#[must_use = "a queue-pair builder creates nothing until `build` is called"]
pub struct QueuePairBuilder<T: Transport> {
    ctx: isize,
    pub(crate) pd: ProtectionDomain,
    /// port local identifier
    lid: u16,
    /// whether the port's link layer is Ethernet (RoCE), which routes every packet by GID
    roce: bool,
    /// the device port this queue pair is associated with (numbered from 1)
    port_num: u8,

    send: CompletionQueue,
    max_send_wr: u32,
    recv: CompletionQueue,
    max_recv_wr: u32,

    gid_index: Option<u32>,
    max_send_sge: u32,
    max_recv_sge: u32,
    max_inline_data: u32,
    /// the send operations to request at creation, or the transport's default set
    send_ops: Option<SendOps>,

    // carried along to handshake phase
    /// traffic class set in Global Routing Headers, only used if `gid_index` is set.
    traffic_class: u8,
    /// service level (0-15). Higher value means higher priority.
    service_level: u8,
    /// the transport's bring-up attributes, with the setters' values written into them
    bring_up: BringUp,
    /// shared receive queue
    srq: Option<SharedReceiveQueue>,
    _transport: std::marker::PhantomData<T>,
}

impl<T: Transport> QueuePairBuilder<T> {
    /// Prepare a new `QueuePair` builder on port `port_num`, whose attributes are `port_attr`, with
    /// the defaults the setters document.
    ///
    /// `rd_atomic_limits` is the device's `(initiator, responder)` RDMA read and atomic limit, the
    /// default for a reliable connection; it is `None` for the other transports.
    pub(crate) fn new(
        pd: ProtectionDomain,
        port_attr: &ffi::ibv_port_attr,
        port_num: u8,
        send: CompletionQueue,
        recv: CompletionQueue,
        rd_atomic_limits: Option<(u8, u8)>,
    ) -> QueuePairBuilder<T> {
        let reliable = T::TYPE == ffi::ibv_qp_type::IBV_QPT_RC;
        let connected = reliable || T::TYPE == ffi::ibv_qp_type::IBV_QPT_UC;
        let mut init = QueuePairAttribute::new();
        init.set_state(QueuePairState::Init)
            .set_pkey_index(0)
            .set_port(port_num);
        let mut rtr = QueuePairAttribute::new();
        rtr.set_state(QueuePairState::ReadyToReceive);
        let mut rts = QueuePairAttribute::new();
        rts.set_state(QueuePairState::ReadyToSend).set_sq_psn(0);
        if connected {
            init.set_access_flags(AccessFlags::LOCAL_WRITE);
            rtr.set_path_mtu(port_attr.active_mtu.into());
        }
        if reliable {
            rtr.set_min_rnr_timer(RnrTimer::from_encoding(16));
            rts.set_timeout(AckTimeout::from_exponent(4))
                .set_retry_count(6)
                .set_rnr_retry(6);
        }
        if let Some((initiator, responder)) = rd_atomic_limits {
            rtr.set_max_dest_rd_atomic(responder);
            rts.set_max_rd_atomic(initiator);
        }
        QueuePairBuilder {
            ctx: 0,
            pd,
            lid: port_attr.lid,
            roce: port_attr.link_layer == ffi::IBV_LINK_LAYER_ETHERNET.0 as u8,
            port_num,

            gid_index: None,
            traffic_class: 0,
            send,
            max_send_wr: 1,
            recv,
            max_recv_wr: 1,

            max_send_sge: 1,
            max_recv_sge: 1,
            max_inline_data: 0,
            send_ops: None,

            service_level: 0,
            bring_up: BringUp { init, rtr, rts },
            srq: None,
            _transport: std::marker::PhantomData,
        }
    }

    /// Set the service level of the new `QueuePair`.
    ///
    /// The service level (0–15) selects the virtual lane on InfiniBand, through the subnet
    /// manager's SL-to-VL mapping; on RoCE, its low 3 bits become the packets' 802.1p priority.
    ///
    /// Defaults to 0.
    ///
    /// # Panics
    ///
    /// Panics if `service_level` is above 15: the field is 4 bits wide, and providers would
    /// silently drop the extra bits.
    pub fn set_service_level(&mut self, service_level: u8) -> &mut Self {
        assert!(service_level <= 15, "the service level is a 4-bit value");
        self.service_level = service_level;
        self
    }

    /// Sets the GID table index that should be used for the new `QueuePair`.
    /// The entry corresponds to the index in [`Context::gid_table`](crate::Context::gid_table).
    /// [`PreparedQueuePair::endpoint`] embeds the GID at this index in the local endpoint, and
    /// [`handshake`](PreparedQueuePair::handshake) routes with it when the remote endpoint
    /// carries a `gid`. Only indexes up to 255 can route (an address vector holds the source GID
    /// index in 8 bits), so the handshake refuses a larger one.
    ///
    /// Defaults to unset.
    pub fn set_gid_index(&mut self, gid_index: u32) -> &mut Self {
        self.gid_index = Some(gid_index);
        self
    }

    /// Sets the traffic class of the Global Routing Headers (GRH).
    ///
    /// This value is only used if a `gid_index` was specified. Using this value, the originator
    /// of the packets specifies the required delivery priority for handling them by the routers.
    ///
    /// Defaults to 0.
    pub fn set_traffic_class(&mut self, traffic_class: u8) -> &mut Self {
        self.traffic_class = traffic_class;
        self
    }

    /// Set the Shared Receive Queue (SRQ) associated with this QP.
    pub fn set_srq(&mut self, srq: &SharedReceiveQueue) -> &mut Self {
        self.srq = Some(srq.clone());
        self
    }

    /// Set the opaque context value for the new `QueuePair`.
    ///
    /// Defaults to 0.
    pub fn set_context(&mut self, ctx: isize) -> &mut Self {
        self.ctx = ctx;
        self
    }

    /// Set the packet sequence number (PSN) the send queue starts at.
    ///
    /// The peer's receive queue must expect the same number. [`endpoint`] carries it, and
    /// [`handshake`] sets the local receive-queue PSN from the remote endpoint, so the two sides
    /// agree without further configuration (the datagram transports' `activate` also starts the
    /// send queue here). Applications that reuse queue pairs typically pick a random starting PSN
    /// per connection so stale packets from an earlier connection are not mistaken for new ones.
    ///
    /// Defaults to 0.
    ///
    /// [`endpoint`]: PreparedQueuePair::endpoint
    /// [`handshake`]: PreparedQueuePair::handshake
    pub fn set_sq_psn(&mut self, psn: u32) -> &mut Self {
        self.bring_up.rts.set_sq_psn(psn);
        self
    }

    /// Set the maximum number of send requests in the work queue.
    ///
    /// Defaults to 1.
    ///
    /// # Panics
    ///
    /// Panics if `max_send_wr` is 0. mlx5 then creates the send queue without room for a single
    /// request, and the first one posted to it writes through a null pointer.
    pub fn set_max_send_wr(&mut self, max_send_wr: u32) -> &mut Self {
        assert!(max_send_wr > 0, "the send queue needs room for one request");
        self.max_send_wr = max_send_wr;
        self
    }

    /// The maximum number of scatter/gather elements in any Work Request
    /// that can be posted to the Send Queue in that Queue Pair.
    ///
    /// Value can be `[0..dev_cap.max_sge]`. Some devices support fewer
    /// scatter/gather elements for specific transport types than the
    /// maximum reported value. Posting a work request with more entries than requested here
    /// panics, even where the device granted more; with 0, the queue pair sends only inline or
    /// empty payloads.
    ///
    /// Defaults to 1.
    pub fn set_max_send_sge(&mut self, max_send_sge: u32) -> &mut Self {
        self.max_send_sge = max_send_sge;
        self
    }

    /// Set the maximum number of receive requests in the work queue.
    ///
    /// Defaults to 1.
    pub fn set_max_recv_wr(&mut self, max_recv_wr: u32) -> &mut Self {
        self.max_recv_wr = max_recv_wr;
        self
    }

    /// The maximum number of scatter/gather elements in any Work Request
    /// that can be posted to the Receive Queue in that Queue Pair.
    ///
    /// Value can be `[0..dev_cap.max_sge]`. Some devices support fewer
    /// scatter/gather elements for specific transport types than the
    /// maximum reported value. This value is ignored if the
    /// Queue Pair is associated with an SRQ.
    ///
    /// Defaults to 1.
    pub fn set_max_recv_sge(&mut self, max_recv_sge: u32) -> &mut Self {
        self.max_recv_sge = max_recv_sge;
        self
    }

    /// Set the maximum size, in bytes, of inline data that may be posted on the send queue.
    ///
    /// Inline sends (see [`Payload::Inline`]) copy their payload directly into the work request
    /// rather than referencing a registered memory region, which lowers latency for small messages.
    /// A send queue must reserve this capacity up front. The device may grant more than requested
    /// (reported by [`QueuePair::query`], see [`QueuePairInitAttribute::max_inline_data`]), but
    /// posting more inline bytes than requested here panics.
    ///
    /// Defaults to 0 (inline sends disabled).
    pub fn set_max_inline_data(&mut self, max_inline_data: u32) -> &mut Self {
        self.max_inline_data = max_inline_data;
        self
    }

    /// Set the send operations the queue pair is created with, replacing the transport's default
    /// set (see [`send_ops`](Self::send_ops)).
    ///
    /// The device must support every operation requested, or [`build`](Self::build) fails with
    /// [`Unsupported`](Error::Unsupported): a device without extended atomics, for example, can
    /// still create a send-only RC queue pair this way. Conversely, do not call an opcode method
    /// whose operation was not requested: what happens depends on the provider (rxe and EFA
    /// install only the requested operations, so the method panics; mlx5 posts it anyway).
    /// Operations beyond the ones this crate posts (`LOCAL_INV`, `BIND_MW`, `SEND_WITH_INV`, `TSO`,
    /// `FLUSH`, `ATOMIC_WRITE`) are reachable through [`as_raw_ex`](QueuePair::as_raw_ex).
    pub fn set_send_ops(&mut self, ops: SendOps) -> &mut Self {
        self.send_ops = Some(ops);
        self
    }

    /// The send operations the queue pair will be created with: the set given to
    /// [`set_send_ops`](Self::set_send_ops), or else the transport's default — SEND (with and
    /// without an immediate) everywhere, plus RDMA WRITE on RC, UC, and SRD, RDMA READ on RC and
    /// SRD, and the atomics on RC. Start from it to add an operation:
    /// `let ops = builder.send_ops() | SendOps::SEND_WITH_INV;`.
    pub fn send_ops(&self) -> SendOps {
        self.send_ops.unwrap_or_else(|| default_send_ops(T::TYPE))
    }

    /// Refuses completion queues or a shared receive queue from another device context than the
    /// protection domain's. libibverbs hands the kernel only their handle numbers, which each
    /// context numbers independently and the kernel looks up in the protection domain's context:
    /// a foreign object either fails to resolve or silently names a different object there, while
    /// the provider keeps operating on the foreign one (misrouted completions, and type confusion
    /// across providers).
    pub(crate) fn check_same_context(&self) -> Result<()> {
        let ctx = self.pd.context().as_raw();
        let foreign = unsafe {
            (*self.send.as_raw()).context != ctx
                || (*self.recv.as_raw()).context != ctx
                || self
                    .srq
                    .as_ref()
                    .is_some_and(|s| (*s.as_raw()).context != ctx)
        };
        if foreign {
            return Err(Error::CreateQueuePair(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the completion queues and the shared receive queue must come from the protection \
                 domain's device context",
            )));
        }
        Ok(())
    }

    /// The creation attributes both `ibv_create_qp_ex` and `efadv_create_qp_ex` take: the
    /// builder's context value, completion queues, capacities, and send operations, in its
    /// protection domain. The shared receive queue is left to the caller.
    pub(crate) fn init_attr_ex(&self) -> ffi::ibv_qp_init_attr_ex {
        ffi::ibv_qp_init_attr_ex {
            qp_context: self.ctx as usize as *mut c_void,
            send_cq: self.send.as_raw(),
            recv_cq: self.recv.as_raw(),
            cap: ffi::ibv_qp_cap {
                max_send_wr: self.max_send_wr,
                max_recv_wr: self.max_recv_wr,
                max_send_sge: self.max_send_sge,
                max_recv_sge: self.max_recv_sge,
                max_inline_data: self.max_inline_data,
            },
            qp_type: T::TYPE,
            comp_mask: ffi::ibv_qp_init_attr_mask::IBV_QP_INIT_ATTR_PD.0
                | ffi::ibv_qp_init_attr_mask::IBV_QP_INIT_ATTR_SEND_OPS_FLAGS.0,
            pd: self.pd.as_raw(),
            // The extended send operations driven through the doorbell post API: the transport's
            // default set, or whatever the builder was told to request instead.
            send_ops_flags: self.send_ops().0 as u64,
            ..Default::default()
        }
    }

    /// The `ibv_create_qp_ex` creation shared by the `build` of every transport it serves (SRD
    /// goes through `efadv_create_qp_ex` instead; see `efa.rs`).
    fn build_impl(&self) -> Result<PreparedQueuePair<T>> {
        self.check_same_context()?;
        let mut attr = ffi::ibv_qp_init_attr_ex {
            srq: self.srq.as_ref().map_or(ptr::null_mut(), |s| s.as_raw()),
            ..self.init_attr_ex()
        };
        let qp = nonnull(
            unsafe { ffi::ibv_create_qp_ex(self.pd.context().as_raw(), &mut attr) },
            Error::CreateQueuePair,
        )?;
        self.prepared(qp, &attr)
    }

    /// Wraps `qp`, just created from `attr`, as the prepared queue pair this builder describes:
    /// the end of every transport's `build`. Creation wrote the granted capacities back into
    /// `attr`, and the queue pair holds the shared receive queue `attr` attached it to, if any.
    pub(crate) fn prepared(
        &self,
        qp: *mut ffi::ibv_qp,
        attr: &ffi::ibv_qp_init_attr_ex,
    ) -> Result<PreparedQueuePair<T>> {
        let qp_ex = unsafe { ffi::ibv_qp_to_qp_ex(qp) };
        let prepared = PreparedQueuePair {
            lid: self.lid,
            roce: self.roce,
            port_num: self.port_num,
            qp: QueuePair {
                pd: self.pd.clone(),
                _srq: if attr.srq.is_null() {
                    None
                } else {
                    self.srq.clone()
                },
                _send_cq: self.send.clone(),
                _recv_cq: self.recv.clone(),
                qp,
                qp_ex,
                // At most what was asked for: mlx5 writes back a capacity that went negative when
                // the work request has no room left for an inline segment (`max_send_sge` 0).
                max_inline_data: self.max_inline_data.min(attr.cap.max_inline_data),
                max_send_sge: self.max_send_sge,
                _transport: std::marker::PhantomData,
            },
            gid_index: self.gid_index,
            traffic_class: self.traffic_class,
            service_level: self.service_level,
            bring_up: self.bring_up.clone(),
        };
        // `ibv_qp_to_qp_ex` hands out the extended view only when the provider installed the
        // work-request table; one that accepted the send-operations mask without doing so has
        // made a queue pair this crate could never post to. Dropping `prepared` destroys it.
        if qp_ex.is_null() {
            return Err(Error::Unsupported {
                operation: &"ibv_qp_to_qp_ex",
            });
        }
        Ok(prepared)
    }
}

impl<T: Connected> QueuePairBuilder<T> {
    /// Set the access flags for the new `QueuePair`.
    ///
    /// Defaults to [`AccessFlags::LOCAL_WRITE`].
    pub fn set_access(&mut self, access: AccessFlags) -> &mut Self {
        self.bring_up.init.set_access_flags(access);
        self
    }

    /// Set the path MTU.
    ///
    /// Defaults to the port's active MTU.
    pub fn set_path_mtu(&mut self, path_mtu: Mtu) -> &mut Self {
        self.bring_up.rtr.set_path_mtu(path_mtu);
        self
    }

    /// Create a new [`PreparedQueuePair`] from this builder template (`ibv_create_qp_ex`).
    ///
    /// The returned `PreparedQueuePair` is associated with the builder's `ProtectionDomain`.
    ///
    /// This method will fail if an unreliable-connection (UC) queue pair is associated with an
    /// SRQ (devices support SRQs on RC and UD queue pairs).
    ///
    /// # Errors
    ///
    ///  - [`CreateQueuePair`](Error::CreateQueuePair): a completion queue or the shared receive
    ///    queue belongs to another device context than the protection domain, or
    ///    `ibv_create_qp_ex` failed (`EINVAL` for an invalid `ProtectionDomain` or
    ///    `CompletionQueue`, or an invalid value in `max_send_wr`, `max_recv_wr`, or
    ///    `max_inline_data`; `ENOMEM` when out of resources; `ENOSYS` when the device does not
    ///    support this Transport Service Type; `EPERM` without enough permissions to create a QP
    ///    with this Transport Service Type).
    ///  - [`Unsupported`](Error::Unsupported): the provider declined the requested send
    ///    operations (`EOPNOTSUPP`), or created the queue pair without the extended work-request
    ///    interface this crate posts through (`ibv_qp_to_qp_ex` returned no handle).
    pub fn build(&self) -> Result<PreparedQueuePair<T>> {
        self.build_impl()
    }
}

impl QueuePairBuilder<Ud> {
    /// Create a new [`PreparedQueuePair`] from this builder template (`ibv_create_qp_ex`).
    ///
    /// The returned `PreparedQueuePair` is associated with the builder's `ProtectionDomain`.
    ///
    /// # Errors
    ///
    ///  - [`CreateQueuePair`](Error::CreateQueuePair): a completion queue or the shared receive
    ///    queue belongs to another device context than the protection domain, or
    ///    `ibv_create_qp_ex` failed (`EINVAL` for an invalid `ProtectionDomain` or
    ///    `CompletionQueue`, or an invalid value in `max_send_wr`, `max_recv_wr`, or
    ///    `max_inline_data`; `ENOMEM` when out of resources; `ENOSYS` when the device does not
    ///    support this Transport Service Type; `EPERM` without enough permissions to create a QP
    ///    with this Transport Service Type).
    ///  - [`Unsupported`](Error::Unsupported): the provider declined the requested send
    ///    operations (`EOPNOTSUPP`), or created the queue pair without the extended work-request
    ///    interface this crate posts through (`ibv_qp_to_qp_ex` returned no handle).
    pub fn build(&self) -> Result<PreparedQueuePair<Ud>> {
        self.build_impl()
    }
}

impl<T: Reliable> QueuePairBuilder<T> {
    /// Sets the minimum RNR NAK timer for the new `QueuePair`: the wait it demands of its peer,
    /// in each receiver-not-ready NAK, before the peer retries a send that arrived while no
    /// receive was posted. It does not affect RNR NAKs sent for other reasons.
    ///
    /// The device supports 32 discrete delays between 0.01 ms and 655.36 ms;
    /// [`RnrTimer::at_least`] rounds a [`Duration`] up to the next one.
    ///
    /// Defaults to a 2.56 ms delay.
    pub fn set_min_rnr_timer(&mut self, timer: RnrTimer) -> &mut Self {
        self.bring_up.rtr.set_min_rnr_timer(timer);
        self
    }

    /// Sets the minimum time the new `QueuePair` waits for an ACK/NACK from the remote QP before
    /// retransmitting the packet.
    ///
    /// The device supports exactly the timeouts `4.096 µs × 2^n` for `n` in `1..=31` (8.192 µs up
    /// to about 2.4 hours); [`AckTimeout::at_least`] rounds a [`Duration`] up to the next one.
    /// [`AckTimeout::INFINITE`] waits forever (useful for debugging): if a packet is lost and no
    /// ACK or NACK arrives, no retry ever occurs and the QP just stops sending data.
    ///
    /// Defaults to 65.536 µs.
    pub fn set_timeout(&mut self, timeout: AckTimeout) -> &mut Self {
        self.bring_up.rts.set_timeout(timeout);
        self
    }

    /// Sets the total number of times that the new `QueuePair` will try to resend the packets
    /// before reporting an error because the remote side doesn't answer in the primary path.
    ///
    /// This 3-bit value defaults to 6.
    ///
    /// # Panics
    ///
    /// Panics if a count higher than 7 is given.
    pub fn set_retry_count(&mut self, count: u8) -> &mut Self {
        self.bring_up.rts.set_retry_count(count);
        self
    }

    /// Sets the total number of times that the new `QueuePair` will try to resend the packets when
    /// an RNR NACK was sent by the remote QP before reporting an error.
    ///
    /// This 3-bit value defaults to 6. The value 7 is special: it retries indefinitely when the
    /// remote side answers with an RNR NAK.
    ///
    /// # Panics
    ///
    /// Panics if a limit higher than 7 is given.
    pub fn set_rnr_retry(&mut self, n: u8) -> &mut Self {
        self.bring_up.rts.set_rnr_retry(n);
        self
    }

    /// Set how many RDMA reads and atomic operations this queue pair may have outstanding as the
    /// initiator. [`handshake`](PreparedQueuePair::handshake) lowers it to the responder resources
    /// the peer advertises in its endpoint
    /// ([`max_dest_rd_atomic`](QueuePairEndpoint::max_dest_rd_atomic)), since exceeding them is a
    /// protocol error.
    ///
    /// Defaults to the device's limit (`max_qp_init_rd_atom` in
    /// [`Context::query_device`](crate::Context::query_device)), as librdmacm does: with a single
    /// outstanding operation, every read and atomic waits for the previous one's response.
    pub fn set_max_rd_atomic(&mut self, max_rd_atomic: u8) -> &mut Self {
        self.bring_up.rts.set_max_rd_atomic(max_rd_atomic);
        self
    }

    /// Set how many incoming RDMA reads and atomic operations this queue pair can service at once
    /// as the responder. [`PreparedQueuePair::endpoint`] advertises it, and the peer's handshake
    /// caps its own [`max_rd_atomic`](Self::set_max_rd_atomic) to it.
    ///
    /// Defaults to the device's limit (`max_qp_rd_atom` in
    /// [`Context::query_device`](crate::Context::query_device)).
    pub fn set_max_dest_rd_atomic(&mut self, max_dest_rd_atomic: u8) -> &mut Self {
        self.bring_up.rtr.set_max_dest_rd_atomic(max_dest_rd_atomic);
        self
    }
}

/// An allocated but uninitialized `QueuePair`. Created by [`QueuePairBuilder::build`].
///
/// Specifically, this `QueuePair` has been allocated (with `ibv_create_qp_ex`, or
/// `efadv_create_qp_ex` for SRD), but has not yet been initialized with calls to `ibv_modify_qp`.
///
/// To complete the construction of the `QueuePair`, you will need to obtain the
/// [`QueuePairEndpoint`] of the remote end (by using [`endpoint`](Self::endpoint)), and then call
/// [`handshake`](Self::handshake) on both sides with the other side's endpoint:
///
/// ```text
/// // on host 1
/// let pqp: PreparedQueuePair = ...;
/// let host1end = pqp.endpoint()?;
/// host2.send(host1end);
/// let host2end = host2.recv();
/// let qp = pqp.handshake(host2end)?;
///
/// // on host 2
/// let pqp: PreparedQueuePair = ...;
/// let host2end = pqp.endpoint()?;
/// host1.send(host2end);
/// let host1end = host1.recv();
/// let qp = pqp.handshake(host1end)?;
/// ```
///
/// For a runnable version of this exchange (self-connected, so it fits one process), see
/// `examples/loopback.rs`; `examples/rdmacm_connect.rs` shows the same bring-up driven by the
/// connection manager instead. Datagram transports (UD and SRD) are connectionless and are
/// brought up with `activate` instead of `handshake`.
#[must_use = "a prepared queue pair is destroyed when dropped; connect it with `handshake` or `activate`"]
pub struct PreparedQueuePair<T: Transport> {
    qp: QueuePair<T>,
    /// port local identifier
    lid: u16,
    /// whether the port's link layer is Ethernet (RoCE), which routes every packet by GID
    roce: bool,
    /// the device port this queue pair is associated with (numbered from 1)
    port_num: u8,
    // carried from builder
    gid_index: Option<u32>,
    /// traffic class set in Global Routing Headers, only used if `gid_index` is set.
    traffic_class: u8,
    /// service level (0-15). Higher value means higher priority.
    service_level: u8,
    bring_up: BringUp,
}

/// The `INIT`, `RTR`, and `RTS` attributes of a queue pair's bring-up as the builder configured
/// them: seeded with the transport's defaults, each setter writes into the transition that applies
/// its value. The `*_attributes` methods add only what depends on the peer or the call: a
/// connected transport's route, remote queue pair number, and receive-queue PSN at `RTR` (and the
/// cap of its RDMA read and atomic depth at `RTS`), or a datagram transport's Q_Key at `INIT`.
#[derive(Clone)]
struct BringUp {
    init: QueuePairAttribute,
    rtr: QueuePairAttribute,
    rts: QueuePairAttribute,
}

/// An identifier for the network endpoint of a `QueuePair`. Returned by
/// [`PreparedQueuePair::endpoint`], to exchange with the peer through
/// [`to_bytes`](Self::to_bytes)/[`from_bytes`](Self::from_bytes) — small enough to ride in an
/// rdmacm connection request's `private_data`.
///
/// Internally, this contains the `QueuePair`'s `qp_num`, its port's `lid` and `gid`, the
/// packet sequence number the queue pair's send queue starts at, and how many RDMA reads and
/// atomics it services at once.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct QueuePairEndpoint {
    /// the `QueuePair`'s `qp_num`
    pub qp_num: u32,
    /// the port's `lid`
    pub lid: u16,
    /// the port's `gid` at the configured index, used for global routing
    pub gid: Option<Gid>,
    /// the packet sequence number this queue pair's send queue starts at, which the peer's receive
    /// queue must expect (see [`QueuePairBuilder::set_sq_psn`])
    pub psn: u32,
    /// how many incoming RDMA reads and atomics this queue pair services at once (its responder
    /// resources, see [`QueuePairBuilder::set_max_dest_rd_atomic`]); the peer issues at most this
    /// many at a time. Zero for a transport without RDMA reads.
    pub max_dest_rd_atomic: u8,
}

impl QueuePairEndpoint {
    /// The length of the wire encoding produced by [`to_bytes`](Self::to_bytes).
    pub const WIRE_LEN: usize = 28;

    /// Encodes this endpoint in the crate's stable wire format, for exchanging with the peer over
    /// any transport (it also fits an rdmacm connection request's 56-byte `private_data`).
    ///
    /// The layout, in network byte order: one flags byte (bit 0: a GID is present; all other bits
    /// zero), the queue pair number (4 bytes), the LID (2 bytes), the raw GID (16 bytes, zeroed
    /// when absent), the starting PSN (4 bytes), and the responder resources (1 byte). Adding fields to the format means a new,
    /// longer encoding — this one stays decodable.
    pub fn to_bytes(&self) -> [u8; Self::WIRE_LEN] {
        let mut out = [0u8; Self::WIRE_LEN];
        out[0] = self.gid.is_some() as u8;
        out[1..5].copy_from_slice(&self.qp_num.to_be_bytes());
        out[5..7].copy_from_slice(&self.lid.to_be_bytes());
        if let Some(gid) = self.gid {
            out[7..23].copy_from_slice(&<[u8; 16]>::from(gid));
        }
        out[23..27].copy_from_slice(&self.psn.to_be_bytes());
        out[27] = self.max_dest_rd_atomic;
        out
    }

    /// Decodes an endpoint from the wire format produced by [`to_bytes`](Self::to_bytes).
    ///
    /// # Errors
    ///
    ///  - [`MalformedWireFormat`](Error::MalformedWireFormat): the flags byte carries bits this
    ///    version does not know.
    pub fn from_bytes(bytes: &[u8; Self::WIRE_LEN]) -> Result<Self> {
        let gid = match bytes[0] {
            0 => None,
            1 => {
                let raw: [u8; 16] = bytes[7..23].try_into().expect("slice length is fixed");
                Some(Gid::from(raw))
            }
            _ => return Err(Error::MalformedWireFormat),
        };
        Ok(QueuePairEndpoint {
            qp_num: u32::from_be_bytes(bytes[1..5].try_into().expect("slice length is fixed")),
            lid: u16::from_be_bytes(bytes[5..7].try_into().expect("slice length is fixed")),
            gid,
            psn: u32::from_be_bytes(bytes[23..27].try_into().expect("slice length is fixed")),
            max_dest_rd_atomic: bytes[27],
        })
    }
}

impl<T: Transport> PreparedQueuePair<T> {
    /// Extracts the still-uninitialized (`RESET`) queue pair without transitioning it, so you can
    /// drive the state machine yourself with [`QueuePair::modify`] instead of using `handshake` /
    /// `activate`.
    ///
    /// This is an escape hatch for fully manual bring-up (custom partition keys, packet sequence
    /// numbers, alternate paths, and so on). The returned queue pair is in `RESET` and cannot send
    /// or receive until you transition it through `INIT`, `RTR`, and `RTS`; most users should prefer
    /// `handshake`/`activate`. The RDMA connection manager (the `rdmacm` module, behind the
    /// feature of the same name) uses this to drive the transitions itself.
    pub fn into_queue_pair(self) -> QueuePair<T> {
        self.qp
    }

    /// Get the network endpoint for this `QueuePair`.
    ///
    /// This endpoint will need to be communicated to the `QueuePair` on the remote end. On RoCE,
    /// set a GID index on the builder ([`QueuePairBuilder::set_gid_index`]) first: the endpoint
    /// carries a GID only then, and the peer cannot route to one without it.
    ///
    /// The GID is queried on every call, but the LID is the port's when the builder was created
    /// ([`ProtectionDomain::create_qp`](crate::ProtectionDomain::create_qp)): on InfiniBand it is
    /// stale once the subnet manager changes the port's LID
    /// ([`AsyncEventType::LidChange`](crate::AsyncEventType::LidChange)).
    ///
    /// # Errors
    ///
    ///  - [`QueryGid`](Error::QueryGid): querying the GID at the configured index failed
    ///    (`ibv_query_gid`), or that entry of the GID table is empty (`ENODATA`).
    pub fn endpoint(&self) -> Result<QueuePairEndpoint> {
        let qp_num = unsafe { (*self.qp.qp).qp_num };
        let gid = if let Some(gid_index) = self.gid_index {
            Some(self.qp.pd.context().query_gid(self.port_num, gid_index)?)
        } else {
            None
        };
        Ok(QueuePairEndpoint {
            qp_num,
            lid: self.lid,
            gid,
            psn: self.bring_up.rts.sq_psn(),
            // Unset, so zero, on the transports without RDMA reads.
            max_dest_rd_atomic: self.bring_up.rtr.max_dest_rd_atomic(),
        })
    }
}

impl<T: Datagram> PreparedQueuePair<T> {
    /// The attributes [`activate`](PreparedQueuePair::activate) applies, in order: `INIT` on the
    /// queue pair's port with `qkey` (a datagram queue pair has no access flags), `RTR` (no path
    /// or destination: datagrams are addressed per send), and `RTS` starting the send queue at
    /// the builder's PSN.
    ///
    /// Exposed so a manual bring-up ([`into_queue_pair`](Self::into_queue_pair) plus
    /// [`QueuePair::modify`]) can start from what `activate` would do and adjust it.
    pub fn activate_attributes(&self, qkey: u32) -> [QueuePairAttribute; 3] {
        let BringUp { mut init, rtr, rts } = self.bring_up.clone();
        init.set_qkey(qkey);
        [init, rtr, rts]
    }

    /// Activate this datagram queue pair (UD, or SRD behind the `efa` feature).
    ///
    /// Unlike a connected transport's handshake, a datagram transport is connectionless: there is
    /// no remote endpoint to exchange, so the queue pair is transitioned `INIT -> RTR -> RTS` with
    /// the given `qkey` ([`activate_attributes`](Self::activate_attributes)). Incoming datagrams
    /// whose Q_Key does not match `qkey` are discarded (a sender whose work request sets the
    /// Q_Key's most significant bit transmits with its own QP's Q_Key instead). A `qkey` with that
    /// bit set is a privileged one: since Linux 6.4, using it needs `CAP_NET_RAW` (or, since 6.7,
    /// `rdma system set privileged-qkey on`).
    ///
    /// Each datagram is addressed individually at send time with an [`AddressHandle`]; see
    /// [`SendBatch::to`].
    ///
    /// # Errors
    ///
    ///  - [`ModifyQueuePair`](Error::ModifyQueuePair): a state transition failed (`EINVAL` for an
    ///    invalid value in `attr` or `attr_mask`, `ENOMEM` when out of resources, `EPERM` for a
    ///    privileged `qkey` without the privilege).
    pub fn activate(self, qkey: u32) -> Result<QueuePair<T>> {
        let attributes = self.activate_attributes(qkey);
        let mut qp = self.qp;
        for attr in &attributes {
            qp.modify(attr)?;
        }
        Ok(qp)
    }
}

impl<T: Connected> PreparedQueuePair<T> {
    /// The attributes [`handshake`](Self::handshake) applies to reach `INIT`: the queue pair's
    /// port, partition-key index 0, and the builder's access flags.
    ///
    /// The three `*_attributes` methods are exposed so a manual bring-up
    /// ([`into_queue_pair`](Self::into_queue_pair) plus [`QueuePair::modify`]) can start from
    /// exactly what the handshake would do and adjust it — a partition key, an alternate path, a
    /// timer — instead of rebuilding the address vector by hand.
    pub fn init_attributes(&self) -> QueuePairAttribute {
        self.bring_up.init.clone()
    }

    /// The attributes [`handshake`](Self::handshake) applies to reach `RTR` for `remote`: the
    /// address vector to it (a global route through the builder's GID index when `remote` carries
    /// a GID, with a hop limit of `0xff` and the builder's traffic class and service level), its
    /// queue pair number and starting PSN, and the builder's path MTU, RNR timer, and
    /// responder-side RDMA limit.
    ///
    /// # Errors
    ///
    ///  - [`GidMismatch`](Error::GidMismatch): `remote` carries a GID, but no `gid_index` was
    ///    set on the builder to route from.
    ///  - [`GidRequired`](Error::GidRequired): the port is RoCE, but `remote` carries no GID.
    ///  - [`ModifyQueuePair`](Error::ModifyQueuePair) with [`io::ErrorKind::InvalidInput`]: the
    ///    path MTU is not a valid MTU encoding, as when the port reported no active MTU to default
    ///    to and none was set with [`QueuePairBuilder::set_path_mtu`]; or `remote` carries a GID
    ///    and the builder's GID index is above 255, more than an address vector can hold.
    pub fn rtr_attributes(&self, remote: &QueuePairEndpoint) -> Result<QueuePairAttribute> {
        let mut path = AddressHandleAttribute::new(self.port_num);
        path.set_dest_lid(remote.lid)
            .set_service_level(self.service_level);
        if let Some(gid) = remote.gid {
            let sgid_index = self.gid_index.ok_or(Error::GidMismatch)?;
            // The address vector carries the index in 8 bits (`ibv_global_route.sgid_index`, and
            // the kernel ABI's), so a larger one would route from whatever entry it truncates to.
            let sgid_index = u8::try_from(sgid_index).map_err(|_| {
                Error::ModifyQueuePair(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "GID index {sgid_index} is above 255, the highest an address vector can \
                         route from"
                    ),
                ))
            })?;
            path.set_grh(gid, sgid_index, 0xff, self.traffic_class);
        } else if self.roce {
            // RoCE routes every packet by GID; the kernel refuses an address vector without one
            // with nothing but `EINVAL`.
            return Err(Error::GidRequired);
        }
        let mut attr = self.bring_up.rtr.clone();
        // A port that reports no active MTU (rxe on a netdev whose MTU is too small for 256-byte
        // packets, or mlx5 RoCE without a netdev) leaves the default path MTU unknown, which mlx5
        // refuses with nothing but `EINVAL` and rxe accepts as an MTU of -1.
        if let Mtu::Unknown(raw) = attr.path_mtu() {
            return Err(Error::ModifyQueuePair(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "the path MTU {raw} is not a valid MTU encoding (the port reports no active \
                     MTU); set one with QueuePairBuilder::set_path_mtu"
                ),
            )));
        }
        attr.set_address_vector(&path)
            .set_dest_qp_num(remote.qp_num)
            // The receive queue starts at the PSN the peer's send queue starts at.
            .set_rq_psn(remote.psn);
        Ok(attr)
    }

    /// The attributes [`handshake`](Self::handshake) applies to reach `RTS` with `remote`: the
    /// send queue's starting PSN (the one [`endpoint`](Self::endpoint) advertises) and the
    /// builder's ACK timeout, retry counts, and initiator-side RDMA limit, capped to the responder
    /// resources `remote` advertises.
    pub fn rts_attributes(&self, remote: &QueuePairEndpoint) -> QueuePairAttribute {
        let mut attr = self.bring_up.rts.clone();
        if attr
            .mask()
            .contains(QueuePairAttributeMask::MAX_QP_RD_ATOMIC)
        {
            // More outstanding reads and atomics than the peer has responder resources for is a
            // protocol error, which the peer answers with an invalid-request NAK.
            attr.set_max_rd_atomic(attr.max_rd_atomic().min(remote.max_dest_rd_atomic));
        }
        attr
    }

    /// Set up the `QueuePair` such that it is ready to exchange packets with a remote `QueuePair`.
    ///
    /// Applies [`init_attributes`](Self::init_attributes),
    /// [`rtr_attributes`](Self::rtr_attributes), and [`rts_attributes`](Self::rts_attributes) in
    /// turn with [`QueuePair::modify`], moving the queue pair through `INIT`, `RTR`, and `RTS`
    /// with the settings configured on the builder. Further discussion of the protocol can be
    /// found on [RDMAmojo]. Use [`into_queue_pair`](Self::into_queue_pair) to drive the state
    /// machine yourself instead, starting from those same attributes if you like.
    ///
    /// If the endpoint contains a GID, the routing is global (`is_global = 1`, hop limit `0xff`).
    /// The packet sequence numbers pair up through the endpoints: the local send queue starts at
    /// this queue pair's PSN ([`QueuePairBuilder::set_sq_psn`], carried to the peer by
    /// [`endpoint`](Self::endpoint)), and the local receive queue starts at the PSN in `remote`.
    /// The partition-key index and the source path bits are 0.
    ///
    /// # Errors
    ///
    ///  - [`GidMismatch`](Error::GidMismatch): the remote endpoint carries a GID, but no
    ///    `gid_index` was set on the builder to route from.
    ///  - [`GidRequired`](Error::GidRequired): the port is RoCE, but the remote endpoint carries
    ///    no GID.
    ///  - [`ModifyQueuePair`](Error::ModifyQueuePair) with [`io::ErrorKind::InvalidInput`]: the
    ///    path MTU is not a valid MTU encoding, or the GID index to route from is above 255 (see
    ///    [`rtr_attributes`](Self::rtr_attributes)).
    ///  - The errors of [`QueuePair::modify`]: [`ModifyQueuePair`](Error::ModifyQueuePair) when
    ///    a transition fails (for example because the remote endpoint is unreachable — on RoCE,
    ///    the error spells out the route that failed), or its typed diagnoses of a rejected
    ///    attribute set.
    ///
    /// [RDMAmojo]: http://www.rdmamojo.com/2014/01/18/connecting-queue-pairs/
    pub fn handshake(self, remote: QueuePairEndpoint) -> Result<QueuePair<T>> {
        let init = self.init_attributes();
        let rtr = self.rtr_attributes(&remote)?;
        let rts = self.rts_attributes(&remote);
        let mut qp = self.qp;
        qp.modify(&init)?;
        if let Err(err) = qp.modify(&rtr) {
            // On RoCE, the provider resolves the route to the remote GID during this transition,
            // and reports a GID that does not answer as a timeout or unreachable network. Spell
            // that out: it is the most common RoCE bring-up failure, and "connection timed out"
            // alone sends people looking at the wrong layer.
            return Err(match err {
                Error::ModifyQueuePair(source)
                    if remote.gid.is_some()
                        && matches!(
                            source.raw_os_error(),
                            Some(nix::libc::ETIMEDOUT | nix::libc::ENETUNREACH)
                        ) =>
                {
                    Error::ModifyQueuePair(io::Error::new(
                        source.kind(),
                        format!(
                            "resolving the route to the remote GID failed ({source}); on RoCE this \
                             usually means the remote GID does not answer on the network of the \
                             local GID at index {}, or a firewall drops RoCE (UDP 4791) traffic",
                            rtr.as_raw().ah_attr.grh.sgid_index,
                        ),
                    ))
                }
                other => other,
            });
        }
        qp.modify(&rts)?;
        Ok(qp)
    }
}

/// A receive work request, binding the lifetime of its scatter/gather buffers.
///
/// Build one with [`RecvRequest::new`] and post a batch of them with [`QueuePair::post_recv`] or
/// [`SharedReceiveQueue::post_recv`]. Unlike the send doorbell, receives are posted from a
/// caller-owned slice, so batching allocates nothing.
#[repr(transparent)]
pub struct RecvRequest<'a> {
    pub(crate) wr: ffi::ibv_recv_wr,
    _local: std::marker::PhantomData<&'a [LocalMemorySlice]>,
}

impl<'a> RecvRequest<'a> {
    /// A receive that scatters an incoming message into `local`, tagged with `wr_id`.
    #[inline]
    pub fn new(wr_id: u64, local: &'a [LocalMemorySlice]) -> Self {
        RecvRequest {
            wr: ffi::ibv_recv_wr {
                wr_id,
                next: ptr::null_mut(),
                // An empty Rust slice's `.as_ptr()` is `NonNull::dangling()` (for example `0x8` for
                // `LocalMemorySlice`), not `NULL`. C providers such as `irdma` (`irdma_set_fragment`
                // in `providers/irdma/uk.c`) check `if (sge)` before checking `num_sge == 0` and
                // dereference `sg_list`, so pass a genuine null pointer when the slice is empty.
                sg_list: if local.is_empty() {
                    ptr::null_mut()
                } else {
                    local.as_ptr() as *mut ffi::ibv_sge
                },
                num_sge: local.len() as i32,
            },
            _local: std::marker::PhantomData,
        }
    }

    /// Links the non-empty `recvs` into the list `ibv_post_recv` and `ibv_post_srq_recv` walk, and
    /// returns its head. Every link comes from one pointer to the slice: indexing the slice again
    /// for each request would invalidate the links stored before it, which the provider then
    /// follows.
    pub(crate) fn link(recvs: &mut [RecvRequest<'_>]) -> *mut ffi::ibv_recv_wr {
        let base = recvs.as_mut_ptr();
        unsafe {
            for i in 1..recvs.len() {
                (*base.add(i - 1)).wr.next = &raw mut (*base.add(i)).wr;
            }
            (*base.add(recvs.len() - 1)).wr.next = ptr::null_mut();
            &raw mut (*base).wr
        }
    }
}

/// A batch of send work requests being built on a [`QueuePair`]'s send queue.
///
/// Created by [`QueuePair::start_send`]. Each work request starts at [`op`](Self::op) (on a
/// datagram queue pair, at [`to`](Self::to), which also addresses it), is optionally modified
/// ([`signaled`](SendOp::signaled), [`solicited`](SendOp::solicited), and on RC
/// [`fenced`](SendOp::fenced)), and ends with the opcode method (`send`, `write`, `read`,
/// ...) that appends it to the open block immediately, through the extended ("doorbell")
/// interface. The opcodes available depend on the queue pair's transport (see [`Transport`]).
/// [`submit`](Self::submit) then rings the doorbell once for the whole batch. Dropping the batch
/// without submitting aborts it.
///
/// # Borrow-checked guarantees
///
/// The batch borrows the queue pair for as long as it is open, so the type system enforces the
/// doorbell interface's rule that a queue pair has at most one block being built at a time. A
/// second [`start_send`](QueuePair::start_send) (or any other use of the queue pair) while a batch
/// is open is rejected at compile time:
///
/// ```compile_fail,E0499
/// # fn two_batches(qp: &mut ibverbs::QueuePair) {
/// let first = qp.start_send();
/// let second = qp.start_send(); // error: `*qp` is already mutably borrowed by `first`
/// let _ = (first, second);
/// # }
/// ```
///
/// Likewise, only one request at a time is configured on a batch: each [`op`](Self::op) (or
/// [`to`](Self::to)) borrows the batch until that work request is posted, so two half-built
/// requests cannot overlap:
///
/// ```compile_fail,E0499
/// # fn two_requests(qp: &mut ibverbs::QueuePair) {
/// let mut batch = qp.start_send();
/// let first = batch.op();
/// let second = batch.op(); // error: `batch` is already mutably borrowed by `first`
/// let _ = (first, second);
/// # }
/// ```
///
/// A datagram request keeps its [`AddressHandle`] borrowed until it is posted, so the handle
/// cannot be destroyed while the provider still has to read it:
///
/// ```compile_fail,E0505
/// # fn dropped_handle(
/// #     qp: &mut ibverbs::QueuePair<ibverbs::Ud>,
/// #     ah: ibverbs::AddressHandle,
/// #     sges: &[ibverbs::LocalMemorySlice],
/// # ) {
/// let mut batch = qp.start_send();
/// let op = batch.to(&ah, 1, 2);
/// drop(ah); // error: cannot move out of `ah` because it is borrowed by `op`
/// op.send(1, sges);
/// # }
/// ```
///
/// # Deadlocks
///
/// The provider holds the send queue's lock from [`start_send`](QueuePair::start_send) until the
/// batch is submitted or dropped. On ionic, an open batch holds the lock of the queue pair's send
/// completion queue too, which polling and arming that completion queue take, and so does posting
/// a receive to a queue pair whose receive completion queue is the same one. So on ionic, while a
/// batch is open, the thread holding it must not poll or arm its send completion queue or post
/// receives through it, and other threads doing so wait until the batch ends. On hns, polling a
/// failed completion takes the send queue's lock, so do not poll this queue pair's completion
/// queues while holding an open batch either (see [`Completions`](crate::Completions)).
#[must_use = "a started batch must be `.submit()`ed (otherwise it is aborted on drop)"]
pub struct SendBatch<'qp, T: Transport> {
    qpx: *mut ffi::ibv_qp_ex,
    max_inline_data: u32,
    max_send_sge: u32,
    // Opened through a shared borrow (`QueuePair::open_batch`); what keeps a second batch out is
    // that every caller holds the send queue exclusively.
    _qp: std::marker::PhantomData<&'qp QueuePair<T>>,
}

impl<'qp, T: Connected> SendBatch<'qp, T> {
    /// Begin one work request on the batch.
    ///
    /// The returned [`SendOp`] carries the request until an opcode method posts it: chain the
    /// modifiers first (`batch.op().signaled().send(id, sges)`), because the provider reads the
    /// work-request flags inside the opcode builder.
    #[inline]
    pub fn op(&mut self) -> SendOp<'_, 'qp, T> {
        SendOp {
            batch: self,
            flags: 0,
            imm: None,
            dest: None,
        }
    }
}

impl<'qp, T: Datagram> SendBatch<'qp, T> {
    /// Begin one work request on the batch, addressed to `ah` / `remote_qpn` / `remote_qkey`.
    ///
    /// Every datagram send needs a destination, so this is the datagram batch's entry point; the
    /// destination is per work request, so one batch may address each request to a different
    /// peer. The returned [`SendOp`] carries the request until an opcode method posts it:
    /// chain the modifiers first (`batch.to(&ah, qpn, qkey).signaled().send(id, sges)`),
    /// because the provider reads the work-request flags and addressing inside the opcode
    /// builder.
    ///
    /// `ah` stays borrowed until the request is posted, since the provider reads the handle inside
    /// the opcode builder. Keep it alive until the request completes too: some providers (rxe,
    /// for one) only resolve the handle when the request executes.
    ///
    /// # Panics
    ///
    /// Panics if `ah` was created on another protection domain than the queue pair's: rxe fails
    /// the send (an address handle must share its queue pair's protection domain), and one from
    /// another device context would be read by the provider as one of its own (mlx5 copies its
    /// address vector out of it).
    #[inline]
    pub fn to<'b>(
        &'b mut self,
        ah: &'b AddressHandle,
        remote_qpn: u32,
        remote_qkey: u32,
    ) -> SendOp<'b, 'qp, T> {
        // One protection domain implies one device context.
        assert!(
            unsafe { (*ah.as_raw()).pd == (*self.qpx).qp_base.pd },
            "the address handle must come from the queue pair's protection domain"
        );
        SendOp {
            batch: self,
            flags: 0,
            imm: None,
            dest: Some((ah.as_raw(), remote_qpn, remote_qkey)),
        }
    }
}

impl<'qp, T: Transport> SendBatch<'qp, T> {
    /// Post the whole batch to the device, ringing the doorbell once.
    ///
    /// # Safety
    ///
    /// Every memory region the batch references must stay valid until the device is done with it:
    /// until a work completion has been polled for its request, or for a later signaled request on
    /// the same send queue (an unsignaled request produces no completion of its own), or until the
    /// queue pair has been reset or destroyed.
    ///
    /// That holds even when this returns `Err`, and for requests of a batch that was dropped:
    ///
    /// - rxe hands the requests to the device before it reports a failure (below `RTS`, for one);
    /// - on mlx5, a request added while the send queue is full rewrites the last request posted
    ///   before it, so never have more requests outstanding than the queue's capacity;
    /// - hns devices in owner-doorbell mode may execute each request as soon as its opcode method
    ///   returns, before `submit` and even if the batch is dropped.
    ///
    /// # Errors
    ///
    ///  - [`PostSend`](Error::PostSend): completing the batch failed (`EINVAL` for an invalid
    ///    value in one of the work requests, `ENOMEM` when out of resources or when the send queue
    ///    was full, which on mlx5 also corrupted a posted request; see Safety).
    pub unsafe fn submit(self) -> Result<()> {
        let qpx = self.qpx;
        // Disarm the abort-on-drop before completing: `Drop` would otherwise `wr_abort` the block we
        // are about to `wr_complete`.
        std::mem::forget(self);
        let ret = unsafe { (*qpx).wr_complete.unwrap()(qpx) };
        if ret != 0 {
            Err(Error::errno(ret, Error::PostSend))
        } else {
            Ok(())
        }
    }
}

impl<T: Transport> Drop for SendBatch<'_, T> {
    fn drop(&mut self) {
        // Reached only when the batch was started (`wr_start`) but not submitted; `submit` forgets
        // the batch to skip this. Abort the open work-request block.
        unsafe { (*self.qpx).wr_abort.unwrap()(self.qpx) };
    }
}

/// One work request being configured and posted on a [`SendBatch`]. Created by
/// [`SendBatch::op`] on the connected transports, and by [`SendBatch::to`], which also addresses
/// it, on the datagram ones.
///
/// Chain the modifiers ([`signaled`](Self::signaled), [`imm`](Self::imm), except on SRD
/// [`solicited`](Self::solicited), and on RC [`fenced`](Self::fenced)) and finish with an opcode
/// method (`send`, `write`, ...), which posts the request immediately. The opcodes exist only on
/// the transports that support them (see [`Transport`]): UD supports SEND, and SRD (behind the
/// `efa` feature) RDMA WRITE (from registered memory) and READ as well. `send` and the connected
/// transports' `write` take any [`Payload`].
#[must_use = "a send operation posts nothing until an opcode method (`send`, `write`, ...) is called"]
pub struct SendOp<'b, 'qp, T: Transport> {
    batch: &'b mut SendBatch<'qp, T>,
    flags: u32,
    pub(crate) imm: Option<u32>,
    dest: Option<(*mut ffi::ibv_ah, u32, u32)>,
}

impl<T: Transport> SendOp<'_, '_, T> {
    /// Mark this work request signaled (`IBV_SEND_SIGNALED`), so it generates a completion.
    ///
    /// On SRD every work request generates a completion, so this is implied there. EFA refuses
    /// unsignaled work requests on UD too, so on EFA mark every UD send signaled.
    #[inline]
    pub fn signaled(mut self) -> Self {
        self.flags |= ffi::ibv_send_flags::IBV_SEND_SIGNALED.0;
        self
    }

    /// Post a SEND of `payload`, with the immediate set by [`imm`](Self::imm) if any.
    ///
    /// Registered memory converts implicitly (`send(id, &[mr.slice(..)])`); inline data is spelled
    /// out (`send(id, Payload::Inline(b"ping"))`). See [`Payload`] for the lifetime and capacity
    /// requirements of each.
    ///
    /// # Panics
    ///
    /// Panics if an inline payload is longer than the queue pair's inline capacity, or a
    /// scatter/gather list has more entries than the queue pair was built for (see [`Payload`]).
    #[inline]
    pub fn send<'a>(self, wr_id: u64, payload: impl Into<Payload<'a>>) {
        let imm = self.imm;
        self.build(wr_id, payload.into(), Self::send_op(imm))
    }

    /// Set the solicited flag; the transports that allow it expose this as `solicited`.
    #[inline]
    fn solicit(mut self) -> Self {
        self.flags |= ffi::ibv_send_flags::IBV_SEND_SOLICITED.0;
        self
    }

    /// Attach a 32-bit immediate (host byte order) to the SEND or RDMA WRITE this request posts.
    ///
    /// The receiver reads it from the work completion ([`WorkCompletion::imm_data`](crate::WorkCompletion::imm_data)). An RDMA
    /// WRITE with an immediate consumes a receive on the remote side, like a SEND does, which is
    /// how the writer signals the write's arrival. Ignored by the operations that cannot carry one
    /// (RDMA READ and the atomics).
    #[inline]
    pub fn imm(mut self, imm: u32) -> Self {
        self.imm = Some(imm);
        self
    }

    /// Post one work request: set id and flags, run the opcode builder, then the optional datagram
    /// address, then the payload.
    ///
    /// Always inlined: once an opcode has more than one call site, LLVM otherwise outlines this,
    /// and every work request then pays a call plus runtime branches on the destination and the
    /// payload kind that the inlined form folds away at compile time.
    #[inline(always)]
    pub(crate) fn build(
        self,
        wr_id: u64,
        payload: Payload<'_>,
        op: impl FnOnce(*mut ffi::ibv_qp_ex),
    ) {
        let qpx = self.batch.qpx;
        // Not every provider checks a payload against the queue pair's capacities before writing
        // it into the send queue, so refuse one that does not fit. rxe before rdma-core 64 checks
        // each buffer of an inline list alone, hns sums inline lengths in a u32, and mlx5's
        // single-entry setter writes its entry even into a work request with no room for one
        // (`max_send_sge` 0).
        let (inline_len, sges) = match payload {
            Payload::Sges(local) => (0, local.len()),
            Payload::Inline(data) => (data.len(), 0),
            Payload::InlineList(bufs) => (
                bufs.iter().fold(0, |n: usize, b| n.saturating_add(b.len())),
                0,
            ),
        };
        if inline_len > self.batch.max_inline_data as usize {
            inline_too_long(inline_len, self.batch.max_inline_data);
        }
        if sges > self.batch.max_send_sge as usize {
            too_many_sges(sges, self.batch.max_send_sge);
        }
        unsafe {
            (*qpx).wr_id = wr_id;
            (*qpx).wr_flags = match payload {
                Payload::Sges(_) => self.flags,
                // `IBV_SEND_INLINE` is the work-request flag that marks the payload as inline, as
                // the legacy `ibv_post_send` ABI carries it. Soft-RoCE copies the work-request
                // flags into the request verbatim and needs it; the other providers key off the
                // `wr_set_inline_data` call itself, so the flag is redundant but harmless there.
                Payload::Inline(_) | Payload::InlineList(_) => {
                    self.flags | ffi::ibv_send_flags::IBV_SEND_INLINE.0
                }
            };
            op(qpx);
            if let Some((ah, qpn, qkey)) = self.dest {
                (*qpx).wr_set_ud_addr.unwrap()(qpx, ah, qpn, qkey);
            }
            match payload {
                // A single entry goes through `wr_set_sge`, which providers implement without the
                // list setter's loop, bounds checks, and copy of the entry through memory (the
                // capacity check above stands in for the list setter's).
                Payload::Sges([one]) => {
                    (*qpx).wr_set_sge.unwrap()(qpx, one.lkey(), one.addr(), one.len() as u32)
                }
                // An empty Rust slice's `.as_ptr()` is `NonNull::dangling()` rather than `NULL`;
                // pass a genuine null pointer when the slice is empty so C providers never observe
                // a dangling address alongside a zero count/length.
                Payload::Sges(local) => (*qpx).wr_set_sge_list.unwrap()(
                    qpx,
                    local.len(),
                    if local.is_empty() {
                        ptr::null()
                    } else {
                        local.as_ptr() as *const ffi::ibv_sge
                    },
                ),
                // The bytes are copied during this call, so the payload need not outlive the work
                // completion.
                Payload::Inline(data) => (*qpx).wr_set_inline_data.unwrap()(
                    qpx,
                    if data.is_empty() {
                        ptr::null_mut()
                    } else {
                        data.as_ptr() as *mut c_void
                    },
                    data.len(),
                ),
                // `std::io::IoSlice` is guaranteed ABI-compatible with `struct iovec` on Unix (the
                // only platform rdma-core targets), and `ibv_data_buf` has the same layout as
                // `iovec` (an address and a length), so the buffer list passes straight through
                // without copying it into a temporary array.
                Payload::InlineList(bufs) => (*qpx).wr_set_inline_data_list.unwrap()(
                    qpx,
                    bufs.len(),
                    if bufs.is_empty() {
                        ptr::null()
                    } else {
                        bufs.as_ptr() as *const ffi::ibv_data_buf
                    },
                ),
            }
        }
    }

    /// The opcode builder for a SEND, with or without the immediate set by [`imm`](Self::imm).
    #[inline]
    fn send_op(imm: Option<u32>) -> impl FnOnce(*mut ffi::ibv_qp_ex) + use<T> {
        move |q| unsafe {
            match imm {
                Some(imm) => (*q).wr_send_imm.unwrap()(q, imm.to_be()),
                None => (*q).wr_send.unwrap()(q),
            }
        }
    }

    /// The opcode builder for an RDMA WRITE into `remote`, with or without the immediate set by
    /// [`imm`](Self::imm).
    #[inline]
    pub(crate) fn write_op(
        imm: Option<u32>,
        remote: RemoteMemorySlice,
    ) -> impl FnOnce(*mut ffi::ibv_qp_ex) + use<T> {
        move |q| unsafe {
            match imm {
                Some(imm) => {
                    (*q).wr_rdma_write_imm.unwrap()(q, remote.rkey, remote.addr, imm.to_be())
                }
                None => (*q).wr_rdma_write.unwrap()(q, remote.rkey, remote.addr),
            }
        }
    }

    /// The opcode builder for an RDMA READ from `remote`.
    #[inline]
    pub(crate) fn read_op(remote: RemoteMemorySlice) -> impl FnOnce(*mut ffi::ibv_qp_ex) + use<T> {
        move |q| unsafe { (*q).wr_rdma_read.unwrap()(q, remote.rkey, remote.addr) }
    }
}

impl<T: Connected> SendOp<'_, '_, T> {
    /// Mark this work request solicited (`IBV_SEND_SOLICITED`): a SEND or an RDMA WRITE with an
    /// immediate raises a solicited event on the remote side, waking a peer blocked on its
    /// completion channel after arming with [`CompletionQueue::req_notify`] for solicited events
    /// only. The flag is valid only on those operations: ionic refuses it on any other.
    #[inline]
    pub fn solicited(self) -> Self {
        self.solicit()
    }

    /// Post an RDMA WRITE of `payload` into `remote`, with the immediate set by [`imm`](Self::imm)
    /// if any.
    ///
    /// Registered memory converts implicitly (`write(id, &[mr.slice(..)], remote)`); inline data
    /// is spelled out (`write(id, Payload::Inline(b"ping"), remote)`). See [`Payload`] for the
    /// lifetime and capacity requirements of each.
    ///
    /// # Panics
    ///
    /// Panics if an inline payload is longer than the queue pair's inline capacity, or a
    /// scatter/gather list has more entries than the queue pair was built for (see [`Payload`]).
    #[inline]
    pub fn write<'a>(self, wr_id: u64, payload: impl Into<Payload<'a>>, remote: RemoteMemorySlice) {
        let imm = self.imm;
        self.build(wr_id, payload.into(), Self::write_op(imm, remote))
    }
}

impl<T: Reliable> SendOp<'_, '_, T> {
    /// Mark this work request fenced (`IBV_SEND_FENCE`): the device blocks it until prior RDMA
    /// reads and atomic operations on this queue pair have completed. Used to order a send or
    /// write after a read whose data it depends on. Only RC has reads and atomics to wait for
    /// (`ibv_post_send(3)`: the flag is valid only on RC queue pairs).
    #[inline]
    pub fn fenced(mut self) -> Self {
        self.flags |= ffi::ibv_send_flags::IBV_SEND_FENCE.0;
        self
    }

    /// Post an RDMA READ from `remote` into `local`.
    ///
    /// # Panics
    ///
    /// Panics if `local` has more entries than the queue pair was built for
    /// ([`QueuePairBuilder::set_max_send_sge`]).
    #[inline]
    pub fn read(self, wr_id: u64, local: &[LocalMemorySlice], remote: RemoteMemorySlice) {
        self.build(wr_id, Payload::Sges(local), Self::read_op(remote))
    }

    /// Post an atomic compare-and-swap on the 8-byte value at `remote`: if it equals `compare`, it
    /// is replaced by `swap`. The value it held before is written to `local`.
    ///
    /// `remote` must be 8-byte aligned: the responder refuses a misaligned target as an invalid
    /// request, which moves the queue pairs on both sides to the error state.
    ///
    /// # Panics
    ///
    /// Panics if `local` is not 8 bytes long, or the queue pair was built without room for a
    /// scatter/gather entry ([`QueuePairBuilder::set_max_send_sge`]).
    #[inline]
    pub fn atomic_cmp_swap(
        self,
        wr_id: u64,
        local: LocalMemorySlice,
        remote: RemoteMemorySlice,
        compare: u64,
        swap: u64,
    ) {
        self.atomic(wr_id, local, move |q| unsafe {
            (*q).wr_atomic_cmp_swp.unwrap()(q, remote.rkey, remote.addr, compare, swap)
        })
    }

    /// Post an atomic fetch-and-add on the 8-byte value at `remote`: `add` is added to it. The
    /// value it held before is written to `local`.
    ///
    /// `remote` must be 8-byte aligned: the responder refuses a misaligned target as an invalid
    /// request, which moves the queue pairs on both sides to the error state.
    ///
    /// # Panics
    ///
    /// Panics if `local` is not 8 bytes long, or the queue pair was built without room for a
    /// scatter/gather entry ([`QueuePairBuilder::set_max_send_sge`]).
    #[inline]
    pub fn atomic_fetch_add(
        self,
        wr_id: u64,
        local: LocalMemorySlice,
        remote: RemoteMemorySlice,
        add: u64,
    ) {
        self.atomic(wr_id, local, move |q| unsafe {
            (*q).wr_atomic_fetch_add.unwrap()(q, remote.rkey, remote.addr, add)
        })
    }

    /// Post the atomic `op`, whose original value lands in `local`. Not every provider checks
    /// that buffer: rxe applies the atomic remotely and only then fails it on a shorter one,
    /// moving the queue pair to the error state, so refuse anything but the 8 bytes written.
    #[inline(always)]
    fn atomic(self, wr_id: u64, local: LocalMemorySlice, op: impl FnOnce(*mut ffi::ibv_qp_ex)) {
        if local.len() != 8 {
            atomic_local_not_8_bytes(local.len());
        }
        self.build(wr_id, Payload::Sges(std::slice::from_ref(&local)), op)
    }
}

// UD has the solicited flag but SRD does not: EFA refuses it (on UD as well).
impl SendOp<'_, '_, Ud> {
    /// Mark this work request solicited (`IBV_SEND_SOLICITED`): a SEND raises a solicited event on
    /// the remote side, waking a peer blocked on its completion channel after arming with
    /// [`CompletionQueue::req_notify`] for solicited events only.
    #[inline]
    pub fn solicited(self) -> Self {
        self.solicit()
    }
}

/// The error for a failed `ibv_post_recv` on a queue pair, kept out of line. A queue pair that
/// receives through a shared receive queue has no receive queue of its own, and the providers
/// refuse posts to it with errnos that point elsewhere (mlx5 reports a full queue, hns
/// `EOPNOTSUPP`, rxe `EINVAL`), so name the mistake instead. ionic returns its errnos negated
/// (`-EAGAIN` when the device still owns the slot, so posting again soon succeeds); the others
/// return them as they are.
#[cold]
#[inline(never)]
fn post_recv_error(errno: i32, srq_attached: bool) -> Error {
    if srq_attached {
        return Error::PostReceive(io::Error::new(
            io::ErrorKind::InvalidInput,
            "this queue pair receives through its shared receive queue; post receives there",
        ));
    }
    Error::errno(errno.abs(), Error::PostReceive)
}

/// The panic for an inline payload longer than the queue pair's inline capacity, kept out of line
/// so the posting path carries only the comparison.
#[cold]
#[inline(never)]
fn inline_too_long(len: usize, max_inline_data: u32) -> ! {
    panic!(
        "{len} bytes of inline data exceed the queue pair's inline capacity of {max_inline_data} \
         bytes"
    )
}

/// The panic for a scatter/gather list longer than the queue pair's send capacity, kept out of line
/// like `inline_too_long`.
#[cold]
#[inline(never)]
fn too_many_sges(len: usize, max_send_sge: u32) -> ! {
    panic!(
        "{len} scatter/gather entries exceed the queue pair's capacity of {max_send_sge} per send \
         work request"
    )
}

/// The panic for an atomic whose local buffer is not 8 bytes long, kept out of line like
/// `inline_too_long`.
#[cold]
#[inline(never)]
fn atomic_local_not_8_bytes(len: usize) -> ! {
    panic!("an atomic writes 8 bytes to its local buffer, which is {len} bytes long")
}

/// The data a SEND or RDMA WRITE carries: registered memory the device reads after the request is
/// posted, or bytes copied into the work request as it is posted.
///
/// [`Sges`](Self::Sges) references memory regions through scatter/gather entries, so the buffers
/// must stay valid until the work completion is reaped (the [`submit`](SendBatch::submit)
/// contract), and the opcode method panics on a list with more entries than the queue pair was
/// built for ([`QueuePairBuilder::set_max_send_sge`]). The inline variants copy the bytes into the
/// work request during the opcode call, which lowers latency for small messages and lets the bytes
/// be reused or dropped right away; they need a queue pair built with enough inline capacity
/// ([`QueuePairBuilder::set_max_inline_data`]), and the opcode method panics on a longer inline
/// payload.
///
/// A scatter/gather list converts implicitly, so `batch.op().send(id, &[mr.slice(..)])` reads
/// naturally; inline payloads are spelled out: `batch.op().send(id, Payload::Inline(b"ping"))`.
#[derive(Clone, Copy)]
pub enum Payload<'a> {
    /// Registered memory, as scatter/gather entries.
    Sges(&'a [LocalMemorySlice]),
    /// Bytes copied into the work request.
    Inline(&'a [u8]),
    /// Several buffers concatenated into one inline payload (`wr_set_inline_data_list`), saving
    /// the copy into a contiguous buffer first; the queue pair needs inline capacity for their
    /// combined length.
    InlineList(&'a [io::IoSlice<'a>]),
}

impl<'a> From<&'a [LocalMemorySlice]> for Payload<'a> {
    fn from(local: &'a [LocalMemorySlice]) -> Self {
        Payload::Sges(local)
    }
}

impl<'a, const N: usize> From<&'a [LocalMemorySlice; N]> for Payload<'a> {
    fn from(local: &'a [LocalMemorySlice; N]) -> Self {
        Payload::Sges(local)
    }
}

/// A set of queue-pair attributes together with a mask of which of them are present.
///
/// Used with [`QueuePair::modify`] to change a queue pair's attributes (the general escape hatch
/// for transitions that [`PreparedQueuePair::handshake`] and friends do not cover), and returned by
/// [`QueuePair::query`]. Each `set_*` method records its field in the mask; [`modify`] reads only
/// the fields the mask marks as present, and after a [`query`] the getters are meaningful only for
/// the fields that were requested.
///
/// [`modify`]: QueuePair::modify
/// [`query`]: QueuePair::query
#[derive(Clone)]
pub struct QueuePairAttribute {
    attr: ffi::ibv_qp_attr,
    mask: ffi::ibv_qp_attr_mask,
}

impl Default for QueuePairAttribute {
    fn default() -> Self {
        Self::new()
    }
}

impl QueuePairAttribute {
    /// Create an empty set of attributes (no fields present).
    pub fn new() -> Self {
        QueuePairAttribute {
            attr: ffi::ibv_qp_attr::default(),
            mask: ffi::ibv_qp_attr_mask(0),
        }
    }

    /// Build attributes from a raw `ibv_qp_attr` and mask.
    ///
    /// This is for attributes produced elsewhere, for example the values the RDMA connection
    /// manager fills in via `rdma_init_qp_attr`.
    pub fn from_raw(attr: ffi::ibv_qp_attr, mask: ffi::ibv_qp_attr_mask) -> Self {
        QueuePairAttribute { attr, mask }
    }

    /// The underlying `ibv_qp_attr`. Escape hatch for fields this crate does not wrap.
    pub fn as_raw(&self) -> &ffi::ibv_qp_attr {
        &self.attr
    }

    /// The mask of which attributes are present.
    pub fn mask(&self) -> QueuePairAttributeMask {
        self.mask.into()
    }

    /// Set the next queue-pair state. Not every transition is valid; see [`QueuePair::modify`].
    pub fn set_state(&mut self, state: QueuePairState) -> &mut Self {
        self.attr.qp_state = state.into();
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_STATE;
        self
    }

    /// Set the assumed current state, used to make a transition conditional on it.
    pub fn set_current_state(&mut self, state: QueuePairState) -> &mut Self {
        self.attr.cur_qp_state = state.into();
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_CUR_STATE;
        self
    }

    /// Set the primary partition-key (P_Key) index.
    pub fn set_pkey_index(&mut self, pkey_index: u16) -> &mut Self {
        self.attr.pkey_index = pkey_index;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_PKEY_INDEX;
        self
    }

    /// Set the primary physical port number (ports are numbered from 1).
    pub fn set_port(&mut self, port_num: u8) -> &mut Self {
        self.attr.port_num = port_num;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_PORT;
        self
    }

    /// Set the remote-access flags (RC/UC only).
    pub fn set_access_flags(&mut self, access_flags: AccessFlags) -> &mut Self {
        self.attr.qp_access_flags = access_flags.0;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
        self
    }

    /// Set the path MTU (RC/UC only).
    pub fn set_path_mtu(&mut self, path_mtu: Mtu) -> &mut Self {
        self.attr.path_mtu = path_mtu.into();
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_PATH_MTU;
        self
    }

    /// Set the destination queue-pair number (24 bits; RC/UC only).
    pub fn set_dest_qp_num(&mut self, dest_qp_num: u32) -> &mut Self {
        self.attr.dest_qp_num = dest_qp_num;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_DEST_QPN;
        self
    }

    /// Set the receive-queue packet sequence number (24 bits).
    pub fn set_rq_psn(&mut self, rq_psn: u32) -> &mut Self {
        self.attr.rq_psn = rq_psn;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_RQ_PSN;
        self
    }

    /// Set the send-queue packet sequence number (24 bits).
    pub fn set_sq_psn(&mut self, sq_psn: u32) -> &mut Self {
        self.attr.sq_psn = sq_psn;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_SQ_PSN;
        self
    }

    /// Set the number of outstanding RDMA reads and atomics this queue pair issues as the initiator
    /// (RC only).
    pub fn set_max_rd_atomic(&mut self, max_rd_atomic: u8) -> &mut Self {
        self.attr.max_rd_atomic = max_rd_atomic;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC;
        self
    }

    /// Set the number of outstanding RDMA reads and atomics this queue pair handles as the
    /// destination (RC only).
    pub fn set_max_dest_rd_atomic(&mut self, max_dest_rd_atomic: u8) -> &mut Self {
        self.attr.max_dest_rd_atomic = max_dest_rd_atomic;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC;
        self
    }

    /// Set the minimum RNR-NAK timer: the delay this queue pair demands of its peer, in each
    /// receiver-not-ready NAK, before the peer retries a send that found no receive posted (RC
    /// only). The same knob as [`QueuePairBuilder::set_min_rnr_timer`].
    pub fn set_min_rnr_timer(&mut self, timer: RnrTimer) -> &mut Self {
        self.attr.min_rnr_timer = timer.encoding();
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER;
        self
    }

    /// Set the ACK timeout: how long to wait for an ACK/NACK before retransmitting (RC only). The
    /// same knob as [`QueuePairBuilder::set_timeout`].
    pub fn set_timeout(&mut self, timeout: AckTimeout) -> &mut Self {
        self.attr.timeout = timeout.exponent();
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_TIMEOUT;
        self
    }

    /// Set the retry count for the primary path (RC only): how many times to resend before
    /// reporting an error because the remote side does not answer. The same knob as
    /// [`QueuePairBuilder::set_retry_count`].
    ///
    /// # Panics
    ///
    /// Panics if `retry_count > 7` (the field is 3 bits).
    pub fn set_retry_count(&mut self, retry_count: u8) -> &mut Self {
        assert!(
            retry_count <= 7,
            "the retry count is 3 bits, got {retry_count}"
        );
        self.attr.retry_cnt = retry_count;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_RETRY_CNT;
        self
    }

    /// Set the RNR retry count (RC only): how many times to resend after a receiver-not-ready
    /// NAK before reporting an error, where `7` retries indefinitely. The same knob as
    /// [`QueuePairBuilder::set_rnr_retry`].
    ///
    /// # Panics
    ///
    /// Panics if `rnr_retry > 7` (the field is 3 bits).
    pub fn set_rnr_retry(&mut self, rnr_retry: u8) -> &mut Self {
        assert!(
            rnr_retry <= 7,
            "the RNR retry count is 3 bits, got {rnr_retry}"
        );
        self.attr.rnr_retry = rnr_retry;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_RNR_RETRY;
        self
    }

    /// Set the Q_Key (UD only).
    pub fn set_qkey(&mut self, qkey: u32) -> &mut Self {
        self.attr.qkey = qkey;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_QKEY;
        self
    }

    /// Set the primary path's address vector, describing how to reach the remote queue pair.
    pub fn set_address_vector(&mut self, ah_attr: &AddressHandleAttribute) -> &mut Self {
        self.attr.ah_attr = ah_attr.attr;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_AV;
        self
    }

    /// The queue-pair state (the value set, or the one read back by [`QueuePair::query`]).
    pub fn state(&self) -> QueuePairState {
        self.attr.qp_state.into()
    }

    /// The partition-key index.
    pub fn pkey_index(&self) -> u16 {
        self.attr.pkey_index
    }

    /// The physical port number.
    pub fn port(&self) -> u8 {
        self.attr.port_num
    }

    /// The remote-access flags.
    pub fn access_flags(&self) -> AccessFlags {
        AccessFlags(self.attr.qp_access_flags)
    }

    /// The path MTU. Meaningful only if it was set or queried.
    pub fn path_mtu(&self) -> Mtu {
        self.attr.path_mtu.into()
    }

    /// The destination queue-pair number.
    pub fn dest_qp_num(&self) -> u32 {
        self.attr.dest_qp_num
    }

    /// The receive-queue packet sequence number.
    pub fn rq_psn(&self) -> u32 {
        self.attr.rq_psn
    }

    /// The send-queue packet sequence number.
    pub fn sq_psn(&self) -> u32 {
        self.attr.sq_psn
    }

    /// The number of outstanding RDMA reads and atomics issued as the initiator.
    pub fn max_rd_atomic(&self) -> u8 {
        self.attr.max_rd_atomic
    }

    /// The number of outstanding RDMA reads and atomics handled as the destination.
    pub fn max_dest_rd_atomic(&self) -> u8 {
        self.attr.max_dest_rd_atomic
    }

    /// The minimum RNR-NAK timer.
    pub fn min_rnr_timer(&self) -> RnrTimer {
        RnrTimer::from_encoding(self.attr.min_rnr_timer & 0x1f)
    }

    /// The ACK timeout.
    pub fn timeout(&self) -> AckTimeout {
        AckTimeout::from_exponent(self.attr.timeout & 0x1f)
    }

    /// The primary-path retry count.
    pub fn retry_count(&self) -> u8 {
        self.attr.retry_cnt
    }

    /// The RNR retry count.
    pub fn rnr_retry(&self) -> u8 {
        self.attr.rnr_retry
    }

    /// The Q_Key.
    pub fn qkey(&self) -> u32 {
        self.attr.qkey
    }
}

/// The configured capacities of a queue pair, as returned by [`QueuePair::query`].
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct QueuePairInitAttribute {
    /// The maximum number of outstanding send work requests.
    pub max_send_wr: u32,
    /// The maximum number of outstanding receive work requests.
    pub max_recv_wr: u32,
    /// The maximum number of scatter-gather entries per send work request.
    pub max_send_sge: u32,
    /// The maximum number of scatter-gather entries per receive work request.
    pub max_recv_sge: u32,
    /// The maximum amount of inline data, in bytes.
    pub max_inline_data: u32,
    init_attr: ffi::ibv_qp_init_attr,
}

impl QueuePairInitAttribute {
    /// The underlying `ibv_qp_init_attr`. Escape hatch for fields this crate does not wrap.
    pub fn as_raw(&self) -> &ffi::ibv_qp_init_attr {
        &self.init_attr
    }
}

/// The required and optional attribute-mask bits for a `cur -> next` transition of a queue pair of
/// the given type, or `None` if the transition is not valid.
///
/// This mirrors the kernel's `qp_state_table` and `ib_modify_qp_is_ok`
/// (drivers/infiniband/core/verbs.c): every queue pair may move to `RESET` from any state, and to
/// `ERR` from any state but `RESET`, with only `IBV_QP_STATE`; each type allows a specific set of
/// forward transitions; and `IBV_QP_STATE` and `IBV_QP_RATE_LIMIT` are accepted on every one. It
/// is used only to turn an `EINVAL` from `ibv_modify_qp` into a more precise [`Error`].
fn qp_transition_masks(
    qp_type: ffi::ibv_qp_type,
    cur: ffi::ibv_qp_state,
    next: ffi::ibv_qp_state,
) -> Option<(u32, u32)> {
    use ffi::ibv_qp_state as S;
    use ffi::ibv_qp_type as T;

    let state = ffi::ibv_qp_attr_mask::IBV_QP_STATE.0;
    let cur_state = ffi::ibv_qp_attr_mask::IBV_QP_CUR_STATE.0;
    let pkey = ffi::ibv_qp_attr_mask::IBV_QP_PKEY_INDEX.0;
    let port = ffi::ibv_qp_attr_mask::IBV_QP_PORT.0;
    let access = ffi::ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS.0;
    let qkey = ffi::ibv_qp_attr_mask::IBV_QP_QKEY.0;
    let av = ffi::ibv_qp_attr_mask::IBV_QP_AV.0;
    let path_mtu = ffi::ibv_qp_attr_mask::IBV_QP_PATH_MTU.0;
    let timeout = ffi::ibv_qp_attr_mask::IBV_QP_TIMEOUT.0;
    let retry = ffi::ibv_qp_attr_mask::IBV_QP_RETRY_CNT.0;
    let rnr_retry = ffi::ibv_qp_attr_mask::IBV_QP_RNR_RETRY.0;
    let rq_psn = ffi::ibv_qp_attr_mask::IBV_QP_RQ_PSN.0;
    let max_rd = ffi::ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC.0;
    let alt_path = ffi::ibv_qp_attr_mask::IBV_QP_ALT_PATH.0;
    let min_rnr = ffi::ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER.0;
    let sq_psn = ffi::ibv_qp_attr_mask::IBV_QP_SQ_PSN.0;
    let max_dest_rd = ffi::ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC.0;
    let mig = ffi::ibv_qp_attr_mask::IBV_QP_PATH_MIG_STATE.0;
    let dest_qpn = ffi::ibv_qp_attr_mask::IBV_QP_DEST_QPN.0;
    let rate = ffi::ibv_qp_attr_mask::IBV_QP_RATE_LIMIT.0;
    let sqd_async = ffi::ibv_qp_attr_mask::IBV_QP_EN_SQD_ASYNC_NOTIFY.0;

    // The kernel accepts IBV_QP_STATE and IBV_QP_RATE_LIMIT on every transition.
    let accepted = state | rate;

    // Any state may move to RESET, and any state but RESET to ERR, with only IBV_QP_STATE (and
    // staying in RESET or ERR needs not even that).
    if next == S::IBV_QPS_RESET || (next == S::IBV_QPS_ERR && cur != S::IBV_QPS_RESET) {
        return Some((if next == cur { 0 } else { state }, accepted));
    }

    let masks = match qp_type {
        T::IBV_QPT_RC => match (cur, next) {
            (S::IBV_QPS_RESET, S::IBV_QPS_INIT) => Some((state | pkey | port | access, 0)),
            (S::IBV_QPS_INIT, S::IBV_QPS_INIT) => Some((0, pkey | port | access)),
            (S::IBV_QPS_INIT, S::IBV_QPS_RTR) => Some((
                state | av | path_mtu | dest_qpn | rq_psn | max_dest_rd | min_rnr,
                pkey | access | alt_path,
            )),
            (S::IBV_QPS_RTR, S::IBV_QPS_RTS) => Some((
                state | sq_psn | timeout | retry | rnr_retry | max_rd,
                cur_state | access | min_rnr | alt_path | mig,
            )),
            (S::IBV_QPS_RTS, S::IBV_QPS_RTS) => {
                Some((0, cur_state | access | min_rnr | alt_path | mig))
            }
            (S::IBV_QPS_RTS, S::IBV_QPS_SQD) => Some((state, sqd_async)),
            (S::IBV_QPS_SQD, S::IBV_QPS_RTS) => {
                Some((state, cur_state | access | min_rnr | alt_path | mig))
            }
            (S::IBV_QPS_SQD, S::IBV_QPS_SQD) => Some((
                0,
                pkey | port
                    | access
                    | av
                    | max_rd
                    | min_rnr
                    | alt_path
                    | timeout
                    | retry
                    | rnr_retry
                    | max_dest_rd
                    | mig,
            )),
            // The kernel's table allows this cell for every type, though RC never enters SQE.
            (S::IBV_QPS_SQE, S::IBV_QPS_RTS) => Some((state, 0)),
            _ => None,
        },
        T::IBV_QPT_UC => match (cur, next) {
            (S::IBV_QPS_RESET, S::IBV_QPS_INIT) => Some((state | pkey | port | access, 0)),
            (S::IBV_QPS_INIT, S::IBV_QPS_INIT) => Some((0, pkey | port | access)),
            (S::IBV_QPS_INIT, S::IBV_QPS_RTR) => Some((
                state | av | path_mtu | dest_qpn | rq_psn,
                pkey | access | alt_path,
            )),
            (S::IBV_QPS_RTR, S::IBV_QPS_RTS) => {
                Some((state | sq_psn, cur_state | access | alt_path | mig))
            }
            (S::IBV_QPS_RTS, S::IBV_QPS_RTS) => Some((0, cur_state | access | alt_path | mig)),
            (S::IBV_QPS_RTS, S::IBV_QPS_SQD) => Some((state, sqd_async)),
            (S::IBV_QPS_SQD, S::IBV_QPS_RTS) => Some((state, cur_state | access | alt_path | mig)),
            (S::IBV_QPS_SQD, S::IBV_QPS_SQD) => Some((0, pkey | access | av | alt_path | mig)),
            (S::IBV_QPS_SQE, S::IBV_QPS_RTS) => Some((state, cur_state | access)),
            _ => None,
        },
        T::IBV_QPT_UD => match (cur, next) {
            (S::IBV_QPS_RESET, S::IBV_QPS_INIT) => Some((state | pkey | port | qkey, 0)),
            (S::IBV_QPS_INIT, S::IBV_QPS_INIT) => Some((0, pkey | port | qkey)),
            (S::IBV_QPS_INIT, S::IBV_QPS_RTR) => Some((state, pkey | qkey)),
            (S::IBV_QPS_RTR, S::IBV_QPS_RTS) => Some((state | sq_psn, cur_state | qkey)),
            (S::IBV_QPS_RTS, S::IBV_QPS_RTS) => Some((0, cur_state | qkey)),
            (S::IBV_QPS_RTS, S::IBV_QPS_SQD) => Some((state, sqd_async)),
            (S::IBV_QPS_SQD, S::IBV_QPS_RTS) => Some((state, cur_state | qkey)),
            (S::IBV_QPS_SQD, S::IBV_QPS_SQD) => Some((0, pkey | qkey)),
            (S::IBV_QPS_SQE, S::IBV_QPS_RTS) => Some((state, cur_state | qkey)),
            _ => None,
        },
        // Raw-packet queue pairs take no other attributes after RESET -> INIT.
        T::IBV_QPT_RAW_PACKET => match (cur, next) {
            (S::IBV_QPS_RESET, S::IBV_QPS_INIT) => Some((state | port, 0)),
            (S::IBV_QPS_INIT, S::IBV_QPS_INIT)
            | (S::IBV_QPS_RTS, S::IBV_QPS_RTS)
            | (S::IBV_QPS_SQD, S::IBV_QPS_SQD) => Some((0, 0)),
            (S::IBV_QPS_INIT, S::IBV_QPS_RTR)
            | (S::IBV_QPS_RTR, S::IBV_QPS_RTS)
            | (S::IBV_QPS_RTS, S::IBV_QPS_SQD)
            | (S::IBV_QPS_SQD, S::IBV_QPS_RTS)
            | (S::IBV_QPS_SQE, S::IBV_QPS_RTS) => Some((state, 0)),
            _ => None,
        },
        _ => None,
    };
    masks.map(|(required, optional)| (required, optional | accepted))
}

/// A fully initialized and ready `QueuePair`. Created by the connected transports'
/// [`handshake`](PreparedQueuePair::handshake) / the datagram transports' `activate` (or
/// [`into_queue_pair`](PreparedQueuePair::into_queue_pair) for manual bring-up).
///
/// The transport marker `T` (see [`Transport`]) selects which operations the queue pair supports
/// at compile time; it defaults to [`Rc`], so a plain `QueuePair` is a reliable-connection queue
/// pair.
///
/// A queue pair is the actual object that sends and receives data in the RDMA architecture
/// (something like a socket). It's not exactly like a socket, however. A socket is an abstraction,
/// which is maintained by the network stack and doesn't have a physical resource behind it. A QP
/// is a resource of an RDMA device and only one process at a time can use a given QP number (much
/// as one socket binds a TCP or UDP port).
#[must_use = "QueuePair is immediately destroyed via drop() unless assigned to a variable"]
pub struct QueuePair<T: Transport = Rc> {
    pd: ProtectionDomain,
    _srq: Option<SharedReceiveQueue>,
    // Keep the completion queues alive while the queue pair references them; `ibv_destroy_cq` fails
    // with EBUSY if a queue pair is still attached.
    _send_cq: CompletionQueue,
    _recv_cq: CompletionQueue,
    qp: *mut ffi::ibv_qp,
    // The extended (doorbell) view of `qp`, used by the send path. `ibv_qp_to_qp_ex` is a cast, so
    // this aliases `qp` and lives exactly as long.
    qp_ex: *mut ffi::ibv_qp_ex,
    // The inline capacity the builder asked for (capped at what the provider granted), which the
    // send path enforces (`SendOp::build`).
    max_inline_data: u32,
    // The scatter/gather entries per send work request the builder asked for, which every
    // provider sizes its work requests to hold and the send path enforces (`SendOp::build`).
    max_send_sge: u32,
    _transport: std::marker::PhantomData<T>,
}

unsafe impl<T: Transport> Send for QueuePair<T> {}
unsafe impl<T: Transport> Sync for QueuePair<T> {}

impl<T: Transport> QueuePair<T> {
    /// Returns the local QP number of this QueuePair.
    pub fn qp_num(&self) -> u32 {
        unsafe { (*self.qp).qp_num }
    }

    /// Returns the underlying `ibv_qp` pointer.
    ///
    /// This is an escape hatch for verbs this crate does not yet wrap. The send path uses the
    /// extended (doorbell) interface; see [`as_raw_ex`](Self::as_raw_ex) for that view. The pointer
    /// is owned by this [`QueuePair`] and stays valid only while it is alive; do not destroy it.
    pub fn as_raw(&self) -> *mut ffi::ibv_qp {
        self.qp
    }

    /// Returns the underlying `ibv_qp_ex` pointer (the extended/doorbell view of this queue pair).
    ///
    /// This is an escape hatch for verbs this crate does not yet wrap. `ibv_qp_to_qp_ex` is a cast,
    /// so this aliases [`as_raw`](Self::as_raw) and lives exactly as long. The pointer stays valid
    /// only while this [`QueuePair`] is alive; do not destroy it.
    pub fn as_raw_ex(&self) -> *mut ffi::ibv_qp_ex {
        self.qp_ex
    }

    /// Modify this queue pair's attributes (`ibv_modify_qp`).
    ///
    /// This is the general escape hatch for transitions and attribute changes that
    /// [`PreparedQueuePair::handshake`] and the datagram transports' `activate` do not cover:
    /// changing access flags at runtime, draining the send queue, moving the queue pair to `ERR`
    /// for teardown, setting a custom partition-key index or packet sequence number, and so on.
    /// Only the attributes whose mask bits are set in `attr` are applied.
    ///
    /// # Errors
    ///
    /// If the device rejects the transition with `EINVAL`, the crate consults its queue-pair state
    /// table and returns [`Error::InvalidQueuePairTransition`] or
    /// [`Error::InvalidQueuePairAttributeMask`] where it can pinpoint the problem, and
    /// [`Error::ModifyQueuePair`] otherwise.
    pub fn modify(&mut self, attr: &QueuePairAttribute) -> Result<()> {
        let mut a = attr.attr;
        let errno = unsafe { ffi::ibv_modify_qp(self.qp, &mut a as *mut _, attr.mask.0 as i32) };
        if errno == 0 {
            return Ok(());
        }
        if errno == nix::libc::EINVAL {
            return Err(self.diagnose_modify(attr));
        }
        Err(Error::errno(errno, Error::ModifyQueuePair))
    }

    /// Query this queue pair's attributes (`ibv_query_qp`).
    ///
    /// `mask` selects which attributes to read; the returned [`QueuePairAttribute`]'s getters are
    /// meaningful only for the requested fields. The second return value describes the queue pair's
    /// configured capacities.
    ///
    /// Takes `&mut self`: libibverbs stores the queried state in the queue pair's cached `state`
    /// without a lock, and rxe, EFA, and hns read that cache on every receive post, so a query must
    /// not overlap any other use of the queue pair.
    ///
    /// # Errors
    ///
    ///  - [`QueryQueuePair`](Error::QueryQueuePair): `ibv_query_qp` failed.
    pub fn query(
        &mut self,
        mask: QueuePairAttributeMask,
    ) -> Result<(QueuePairAttribute, QueuePairInitAttribute)> {
        let mask = ffi::ibv_qp_attr_mask::from(mask);
        let mut attr = ffi::ibv_qp_attr::default();
        let mut init_attr = ffi::ibv_qp_init_attr::default();
        let errno = unsafe { ffi::ibv_query_qp(self.qp, &mut attr, mask.0 as i32, &mut init_attr) };
        if errno != 0 {
            return Err(Error::errno(errno, Error::QueryQueuePair));
        }
        let cap = init_attr.cap;
        Ok((
            QueuePairAttribute { attr, mask },
            QueuePairInitAttribute {
                max_send_wr: cap.max_send_wr,
                max_recv_wr: cap.max_recv_wr,
                max_send_sge: cap.max_send_sge,
                max_recv_sge: cap.max_recv_sge,
                max_inline_data: cap.max_inline_data,
                init_attr,
            },
        ))
    }

    /// Turn a rejected [`modify`](Self::modify) into a precise [`Error`], consulting the queue-pair
    /// state table for the actual queue-pair type and current state.
    fn diagnose_modify(&self, attr: &QueuePairAttribute) -> Error {
        let raw = || Error::ModifyQueuePair(io::Error::from_raw_os_error(nix::libc::EINVAL));
        let mask = attr.mask;
        let has = |bit: ffi::ibv_qp_attr_mask| mask.0 & bit.0 != 0;
        // The kernel judges the transition from the state the caller asserts, if it asserts one
        // (mlx5, rxe, EFA, ionic), and the mask without a next state as staying put.
        let cur = if has(ffi::ibv_qp_attr_mask::IBV_QP_CUR_STATE) {
            attr.attr.cur_qp_state
        } else {
            unsafe { (*self.qp).state }
        };
        let next = if has(ffi::ibv_qp_attr_mask::IBV_QP_STATE) {
            attr.attr.qp_state
        } else {
            cur
        };
        let qp_type = unsafe { (*self.qp).qp_type };
        // Only the types with a transition table can be diagnosed; others (driver/SRD, and XRC,
        // which this crate does not create) fall back to the raw error.
        match qp_type {
            ffi::ibv_qp_type::IBV_QPT_RC
            | ffi::ibv_qp_type::IBV_QPT_UC
            | ffi::ibv_qp_type::IBV_QPT_UD
            | ffi::ibv_qp_type::IBV_QPT_RAW_PACKET => {}
            _ => return raw(),
        }
        match qp_transition_masks(qp_type, cur, next) {
            None => Error::InvalidQueuePairTransition {
                current: cur.into(),
                next: next.into(),
            },
            Some((required, optional)) => {
                let invalid = mask.0 & !(required | optional);
                let needed = required & !mask.0;
                if invalid == 0 && needed == 0 {
                    raw()
                } else {
                    Error::InvalidQueuePairAttributeMask {
                        current: cur.into(),
                        next: next.into(),
                        invalid: QueuePairAttributeMask(invalid),
                        needed: QueuePairAttributeMask(needed),
                    }
                }
            }
        }
    }

    /// Posts a batch of receive Work Requests to this Queue Pair's receive queue with a single
    /// `ibv_post_recv`.
    ///
    /// Receives have no doorbell form, so the requests are posted as a linked list. `recvs` is the
    /// caller's storage — a stack array or a reusable `Vec` — linked in place rather than copied, so
    /// posting allocates nothing. Each request is consumed in order as incoming messages arrive.
    ///
    /// On a UD queue pair the 40-byte GRH of an incoming message is placed at the front of the
    /// scatter buffers, so the payload starts at offset 40. If the queue pair uses a shared receive
    /// queue, post to the [`SharedReceiveQueue`] instead; its own receive queue is unused.
    ///
    /// Takes `&self`: the provider serializes posts to the receive queue, so several threads may
    /// post at once. To post receives while a send batch is open, [`split`](Self::split) the
    /// queue pair.
    ///
    /// # Safety
    ///
    /// Each referenced memory region must stay valid until a work completion has been polled for the
    /// corresponding `wr_id`, or until the queue pair has been reset or destroyed (which discards
    /// its outstanding receives). After an `Err`, the requests ahead of the one that failed were
    /// posted, so the same holds for theirs.
    ///
    /// # Errors
    ///
    ///  - [`PostReceive`](Error::PostReceive): the queue pair receives through a shared receive
    ///    queue ([`io::ErrorKind::InvalidInput`]), or `ibv_post_recv` failed (`EINVAL` for an
    ///    invalid value in one of the work requests, `ENOMEM` when the receive queue is full or out
    ///    of resources).
    pub unsafe fn post_recv<'a>(&self, mut recvs: impl AsMut<[RecvRequest<'a>]>) -> Result<()> {
        let recvs = recvs.as_mut();
        if recvs.is_empty() {
            return Ok(());
        }
        let head = RecvRequest::link(recvs);

        let mut bad_wr: *mut ffi::ibv_recv_wr = ptr::null_mut();
        let errno = unsafe { ffi::ibv_post_recv(self.qp, head, &mut bad_wr) };
        if errno != 0 {
            Err(post_recv_error(errno, self._srq.is_some()))
        } else {
            Ok(())
        }
    }

    /// Begin a batch of send work requests on this queue pair's send queue.
    ///
    /// Each [`op`](SendBatch::op) on the returned [`SendBatch`] appends one request (a datagram
    /// batch starts each request at [`to`](SendBatch::to) instead, which also addresses it):
    /// chain [`signaled`](SendOp::signaled) to request a completion, then post with the opcode
    /// method (`send`, `write`, `read`, the atomics, ...; which of them exist depends on the
    /// transport — see [`Transport`]). [`submit`](SendBatch::submit) then rings the doorbell once
    /// for the whole batch; before that, the requests normally do not reach the device (its
    /// safety section lists the exceptions).
    ///
    /// As with any send, the memory backing each scatter/gather slice must stay valid until the
    /// device is done with the request (see [`submit`](SendBatch::submit)).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ibverbs::{QueuePair, LocalMemorySlice, RemoteMemorySlice};
    /// # unsafe fn f(qp: &mut QueuePair, payload: &[LocalMemorySlice], dest: RemoteMemorySlice, note: &[LocalMemorySlice]) -> ibverbs::Result<()> {
    /// let mut batch = qp.start_send();
    /// batch.op().write(1, payload, dest);
    /// batch.op().signaled().send(2, note);
    /// unsafe { batch.submit() }
    /// # }
    /// ```
    #[inline]
    pub fn start_send(&mut self) -> SendBatch<'_, T> {
        self.open_batch()
    }

    /// Splits the queue pair into its send half and a shared reference for the receive side, so
    /// that one thread can build and submit send batches while others post receives
    /// ([`post_recv`](Self::post_recv) takes `&self`). mlx5, rxe, EFA, and hns lock the two
    /// queues separately, so neither side waits for the other. ionic is the exception: an open
    /// send batch also holds its send completion queue's lock, so when one completion queue serves
    /// both queues, receives posted meanwhile wait for the batch to end, and the thread holding the
    /// batch must not post receives itself (see [`SendBatch`]).
    ///
    /// Both borrow the queue pair, which is usable again once they are dropped.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ibverbs::{LocalMemorySlice, QueuePair, RecvRequest};
    /// # fn f(qp: &mut QueuePair, inbox: &[LocalMemorySlice], outbox: &[LocalMemorySlice]) {
    /// let (mut sends, recvs) = qp.split();
    /// std::thread::scope(|s| {
    ///     s.spawn(move || unsafe { recvs.post_recv([RecvRequest::new(1, inbox)]) });
    ///     s.spawn(move || {
    ///         let mut batch = sends.start_send();
    ///         batch.op().signaled().send(2, outbox);
    ///         unsafe { batch.submit() }
    ///     });
    /// });
    /// # }
    /// ```
    pub fn split(&mut self) -> (SendQueue<'_, T>, &Self) {
        (SendQueue { qp: self }, self)
    }

    /// Opens a work-request block on the send queue (`wr_start`). Every caller holds the send
    /// queue exclusively (a `&mut` to the queue pair or to its [`SendQueue`]), which is what keeps
    /// at most one block open at a time.
    #[inline]
    fn open_batch(&self) -> SendBatch<'_, T> {
        let qpx = self.qp_ex;
        unsafe { (*qpx).wr_start.unwrap()(qpx) };
        SendBatch {
            qpx,
            max_inline_data: self.max_inline_data,
            max_send_sge: self.max_send_sge,
            _qp: std::marker::PhantomData,
        }
    }
}

/// The send half of a [`QueuePair`], from [`QueuePair::split`].
///
/// It starts send batches the way [`QueuePair::start_send`] does, and keeps the same rule that at
/// most one is open at a time:
///
/// ```compile_fail,E0499
/// # fn two_batches(qp: &mut ibverbs::QueuePair) {
/// let (mut sends, _recvs) = qp.split();
/// let first = sends.start_send();
/// let second = sends.start_send(); // error: `sends` is already mutably borrowed by `first`
/// let _ = (first, second);
/// # }
/// ```
pub struct SendQueue<'qp, T: Transport> {
    qp: &'qp QueuePair<T>,
}

impl<T: Transport> SendQueue<'_, T> {
    /// Begin a batch of send work requests, as [`QueuePair::start_send`] does.
    #[inline]
    pub fn start_send(&mut self) -> SendBatch<'_, T> {
        self.qp.open_batch()
    }
}

impl<T: Transport> Drop for QueuePair<T> {
    fn drop(&mut self) {
        // ibv_destroy_qp() fails if the QP is attached to a multicast group (through `as_raw`).
        destroyed(
            "ibv_destroy_qp",
            unsafe { ffi::ibv_destroy_qp(self.qp) },
            || {
                keep(&self.pd);
                keep(&self._srq);
                keep(&self._send_cq);
                keep(&self._recv_cq);
            },
        );
    }
}

#[cfg(test)]
mod test_recv_link {
    use super::*;

    /// Follow the list the way a provider does, from the head through each `next`.
    fn walk(mut wr: *mut ffi::ibv_recv_wr) -> Vec<u64> {
        let mut ids = Vec::new();
        while !wr.is_null() {
            unsafe {
                ids.push((*wr).wr_id);
                wr = (*wr).next;
            }
        }
        ids
    }

    #[test]
    fn link_chains_the_requests_in_order() {
        let sges = [LocalMemorySlice::default()];
        let mut recvs = [
            RecvRequest::new(1, &sges),
            RecvRequest::new(2, &sges),
            RecvRequest::new(3, &sges),
        ];
        assert_eq!(walk(RecvRequest::link(&mut recvs)), [1, 2, 3]);

        let mut one = [RecvRequest::new(4, &sges)];
        assert_eq!(walk(RecvRequest::link(&mut one)), [4]);
    }

    #[repr(C)]
    struct SpyQpEx {
        qp_ex: ffi::ibv_qp_ex,
        last_sge_ptr: *const ffi::ibv_sge,
        last_sge_len: usize,
        last_inline_ptr: *mut c_void,
        last_inline_len: usize,
        last_inline_list_ptr: *const ffi::ibv_data_buf,
        last_inline_list_len: usize,
    }

    impl SpyQpEx {
        fn new() -> Self {
            unsafe extern "C" fn on_send(_qpx: *mut ffi::ibv_qp_ex) {}
            unsafe extern "C" fn on_abort(_qpx: *mut ffi::ibv_qp_ex) {}
            unsafe extern "C" fn on_sge_list(
                qpx: *mut ffi::ibv_qp_ex,
                num_sge: usize,
                sg_list: *const ffi::ibv_sge,
            ) {
                let spy = unsafe { &mut *qpx.cast::<SpyQpEx>() };
                spy.last_sge_len = num_sge;
                spy.last_sge_ptr = sg_list;
            }
            unsafe extern "C" fn on_inline_data(
                qpx: *mut ffi::ibv_qp_ex,
                addr: *mut c_void,
                length: usize,
            ) {
                let spy = unsafe { &mut *qpx.cast::<SpyQpEx>() };
                spy.last_inline_ptr = addr;
                spy.last_inline_len = length;
            }
            unsafe extern "C" fn on_inline_data_list(
                qpx: *mut ffi::ibv_qp_ex,
                num_buf: usize,
                buf_list: *const ffi::ibv_data_buf,
            ) {
                let spy = unsafe { &mut *qpx.cast::<SpyQpEx>() };
                spy.last_inline_list_len = num_buf;
                spy.last_inline_list_ptr = buf_list;
            }

            Self {
                qp_ex: ffi::ibv_qp_ex {
                    wr_send: Some(on_send),
                    wr_abort: Some(on_abort),
                    wr_set_sge_list: Some(on_sge_list),
                    wr_set_inline_data: Some(on_inline_data),
                    wr_set_inline_data_list: Some(on_inline_data_list),
                    ..Default::default()
                },
                last_sge_ptr: ptr::NonNull::dangling().as_ptr(),
                last_sge_len: usize::MAX,
                last_inline_ptr: ptr::NonNull::<u8>::dangling().as_ptr().cast(),
                last_inline_len: usize::MAX,
                last_inline_list_ptr: ptr::NonNull::dangling().as_ptr(),
                last_inline_list_len: usize::MAX,
            }
        }
    }

    // Building a receive work request with an empty SGE slice sets `sg_list` to null rather than
    // `NonNull::dangling()`.
    #[test]
    fn recv_request_with_empty_slice_sets_null_sg_list() {
        let empty: &[LocalMemorySlice] = &[];
        let recv = RecvRequest::new(7, empty);
        assert!(recv.wr.sg_list.is_null());
        assert_eq!(recv.wr.num_sge, 0);
        assert_eq!(recv.wr.wr_id, 7);
    }

    // Building a receive work request with a non-empty SGE slice preserves the slice pointer and
    // element count.
    #[test]
    fn recv_request_with_non_empty_slice_preserves_sge_pointer() {
        let non_empty = [LocalMemorySlice::default()];
        let recv_with_sge = RecvRequest::new(8, &non_empty);
        assert_eq!(
            recv_with_sge.wr.sg_list as *const ffi::ibv_sge,
            non_empty.as_ptr() as *const ffi::ibv_sge
        );
        assert_eq!(recv_with_sge.wr.num_sge, 1);
    }

    // Posting a send operation with an empty SGE slice passes a null pointer and zero count to
    // `wr_set_sge_list` rather than `NonNull::dangling()`.
    #[test]
    fn send_op_with_empty_sge_slice_passes_null_pointer_to_provider() {
        let mut spy = SpyQpEx::new();
        let mut batch = SendBatch::<Rc> {
            qpx: (&raw mut spy).cast::<ffi::ibv_qp_ex>(),
            max_inline_data: 64,
            max_send_sge: 1,
            _qp: std::marker::PhantomData,
        };

        let empty_sges: &[LocalMemorySlice] = &[];
        batch.op().send(1, empty_sges);
        assert_eq!(spy.last_sge_len, 0);
        assert!(spy.last_sge_ptr.is_null());
    }

    // Posting a send operation with an empty inline byte slice passes a null pointer and zero
    // length to `wr_set_inline_data` rather than `NonNull::dangling()`.
    #[test]
    fn send_op_with_empty_inline_slice_passes_null_pointer_to_provider() {
        let mut spy = SpyQpEx::new();
        let mut batch = SendBatch::<Rc> {
            qpx: (&raw mut spy).cast::<ffi::ibv_qp_ex>(),
            max_inline_data: 64,
            max_send_sge: 1,
            _qp: std::marker::PhantomData,
        };

        batch.op().send(2, Payload::Inline(&[]));
        assert_eq!(spy.last_inline_len, 0);
        assert!(spy.last_inline_ptr.is_null());
    }

    // Posting a send operation with an empty `IoSlice` list passes a null pointer and zero count
    // to `wr_set_inline_data_list` rather than `NonNull::dangling()`.
    #[test]
    fn send_op_with_empty_inline_list_passes_null_pointer_to_provider() {
        let mut spy = SpyQpEx::new();
        let mut batch = SendBatch::<Rc> {
            qpx: (&raw mut spy).cast::<ffi::ibv_qp_ex>(),
            max_inline_data: 64,
            max_send_sge: 1,
            _qp: std::marker::PhantomData,
        };

        let empty_iov: &[io::IoSlice<'_>] = &[];
        batch.op().send(3, Payload::InlineList(empty_iov));
        assert_eq!(spy.last_inline_list_len, 0);
        assert!(spy.last_inline_list_ptr.is_null());
    }
}

#[cfg(test)]
mod test_conversions {
    use super::*;

    #[test]
    fn split_halves_cross_threads() {
        fn send<T: Send>() {}
        fn sync<T: Sync>() {}
        send::<SendQueue<'static, Rc>>();
        send::<&'static QueuePair<Rc>>();
        sync::<QueuePair<Rc>>();
    }

    #[test]
    fn queue_pair_state_roundtrip() {
        for (wrapper, raw) in [
            (QueuePairState::Reset, ffi::ibv_qp_state::IBV_QPS_RESET),
            (
                QueuePairState::ReadyToReceive,
                ffi::ibv_qp_state::IBV_QPS_RTR,
            ),
            (QueuePairState::ReadyToSend, ffi::ibv_qp_state::IBV_QPS_RTS),
            (QueuePairState::Unknown, ffi::ibv_qp_state::IBV_QPS_UNKNOWN),
        ] {
            assert_eq!(QueuePairState::from(raw), wrapper);
            assert_eq!(ffi::ibv_qp_state::from(wrapper), raw);
        }
        assert_eq!(QueuePairState::ReadyToSend.to_string(), "RTS");
    }

    #[test]
    fn attribute_setters_record_their_mask_bit() {
        let mut attr = QueuePairAttribute::new();
        attr.set_state(QueuePairState::Init)
            .set_port(1)
            .set_access_flags(AccessFlags::LOCAL_WRITE);
        assert!(attr.mask().contains(
            QueuePairAttributeMask::STATE
                | QueuePairAttributeMask::PORT
                | QueuePairAttributeMask::ACCESS_FLAGS
        ));
        assert_eq!(attr.state(), QueuePairState::Init);
        assert_eq!(attr.access_flags(), AccessFlags::LOCAL_WRITE);
    }
}

#[cfg(test)]
mod test_timers {
    use super::*;

    #[test]
    fn ack_timeout_exact_step_is_itself() {
        // 4.096 µs × 2^4 = 65.536 µs, the default.
        let d = Duration::from_nanos(4096 << 4);
        assert_eq!(AckTimeout::at_least(d), AckTimeout::from_exponent(4));
        assert_eq!(AckTimeout::at_least(d).duration(), Some(d));
    }

    #[test]
    fn ack_timeout_rounds_up_between_steps() {
        // One nanosecond above a step lands on the next one.
        let step4 = Duration::from_nanos(4096 << 4);
        assert_eq!(
            AckTimeout::at_least(step4 + Duration::from_nanos(1)),
            AckTimeout::from_exponent(5)
        );
        // 9 µs is between 8.192 µs (n=1) and 16.384 µs (n=2).
        assert_eq!(
            AckTimeout::at_least(Duration::from_micros(9)),
            AckTimeout::from_exponent(2)
        );
    }

    #[test]
    fn ack_timeout_clamps_both_ends() {
        // Below the smallest step: rounds up to it, never to INFINITE.
        assert_eq!(
            AckTimeout::at_least(Duration::ZERO),
            AckTimeout::from_exponent(1)
        );
        assert_eq!(
            AckTimeout::at_least(Duration::from_nanos(1)),
            AckTimeout::from_exponent(1)
        );
        // Beyond the largest step (4.096 µs × 2^31 ≈ 8796 s): clamps to it.
        assert_eq!(
            AckTimeout::at_least(Duration::from_secs(100_000)),
            AckTimeout::from_exponent(31)
        );
    }

    #[test]
    fn ack_timeout_infinite_roundtrip() {
        assert_eq!(AckTimeout::INFINITE, AckTimeout::from_exponent(0));
        assert_eq!(AckTimeout::INFINITE.exponent(), 0);
        assert_eq!(AckTimeout::INFINITE.duration(), None);
        assert_eq!(AckTimeout::INFINITE.to_string(), "infinite");
    }

    #[test]
    fn ack_timeout_exponent_roundtrip() {
        for n in 0..=31 {
            assert_eq!(AckTimeout::from_exponent(n).exponent(), n);
        }
        // Every finite timeout maps back to itself through its duration.
        for n in 1..=31 {
            let timeout = AckTimeout::from_exponent(n);
            assert_eq!(AckTimeout::at_least(timeout.duration().unwrap()), timeout);
        }
        assert_eq!(AckTimeout::from_exponent(4).to_string(), "65.536µs");
    }

    #[test]
    #[should_panic(expected = "5 bits")]
    fn ack_timeout_exponent_out_of_range_panics() {
        let _ = AckTimeout::from_exponent(32);
    }

    #[test]
    fn rnr_timer_exact_delay_is_itself() {
        // 2.56 ms, the default.
        let d = Duration::from_micros(2_560);
        assert_eq!(RnrTimer::at_least(d), RnrTimer::from_encoding(16));
        assert_eq!(RnrTimer::at_least(d).duration(), d);
    }

    #[test]
    fn rnr_timer_rounds_up_between_delays() {
        // Between 2.56 ms (16) and 3.84 ms (17).
        assert_eq!(
            RnrTimer::at_least(Duration::from_micros(2_561)),
            RnrTimer::from_encoding(17)
        );
        // Between 0.04 ms (4) and 0.06 ms (5).
        assert_eq!(
            RnrTimer::at_least(Duration::from_micros(41)),
            RnrTimer::from_encoding(5)
        );
    }

    #[test]
    fn rnr_timer_clamps_both_ends() {
        assert_eq!(
            RnrTimer::at_least(Duration::ZERO),
            RnrTimer::from_encoding(1)
        );
        assert_eq!(
            RnrTimer::at_least(Duration::from_secs(10)),
            RnrTimer::from_encoding(0)
        );
    }

    #[test]
    fn rnr_timer_encoding_zero_is_the_largest_delay() {
        // The non-monotonic oddity: 0 encodes 655.36 ms, above 491.52 ms at encoding 31.
        assert_eq!(
            RnrTimer::from_encoding(0).duration(),
            Duration::from_micros(655_360)
        );
        assert_eq!(
            RnrTimer::from_encoding(31).duration(),
            Duration::from_micros(491_520)
        );
        // A duration just above encoding 31's delay resolves to the encoding 0.
        assert_eq!(
            RnrTimer::at_least(Duration::from_micros(491_521)),
            RnrTimer::from_encoding(0)
        );
        assert_eq!(RnrTimer::from_encoding(0).to_string(), "655.36ms");
    }

    #[test]
    fn rnr_timer_encoding_roundtrip() {
        for v in 0..=31 {
            let timer = RnrTimer::from_encoding(v);
            assert_eq!(timer.encoding(), v);
            // Every delay maps back to its own encoding through its duration.
            assert_eq!(RnrTimer::at_least(timer.duration()), timer);
        }
        assert_eq!(RnrTimer::from_encoding(16).to_string(), "2.56ms");
    }

    #[test]
    #[should_panic(expected = "5 bits")]
    fn rnr_timer_encoding_out_of_range_panics() {
        let _ = RnrTimer::from_encoding(32);
    }
}

#[cfg(test)]
mod test_qp_transitions {
    use super::*;
    use ffi::ibv_qp_state as S;
    const IBV_QPT_RC: ffi::ibv_qp_type = ffi::ibv_qp_type::IBV_QPT_RC;

    fn bit(m: ffi::ibv_qp_attr_mask) -> u32 {
        m.0
    }

    #[test]
    fn reset_to_init_requires_pkey_port_access() {
        let (required, _optional) =
            qp_transition_masks(IBV_QPT_RC, S::IBV_QPS_RESET, S::IBV_QPS_INIT).unwrap();
        let expected = bit(ffi::ibv_qp_attr_mask::IBV_QP_STATE)
            | bit(ffi::ibv_qp_attr_mask::IBV_QP_PKEY_INDEX)
            | bit(ffi::ibv_qp_attr_mask::IBV_QP_PORT)
            | bit(ffi::ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS);
        assert_eq!(required, expected);
    }

    #[test]
    fn init_to_rts_is_not_a_valid_transition() {
        assert!(qp_transition_masks(IBV_QPT_RC, S::IBV_QPS_INIT, S::IBV_QPS_RTS).is_none());
    }

    #[test]
    fn any_state_to_reset_or_err_needs_only_state() {
        let state = bit(ffi::ibv_qp_attr_mask::IBV_QP_STATE);
        let accepted = state | bit(ffi::ibv_qp_attr_mask::IBV_QP_RATE_LIMIT);
        assert_eq!(
            qp_transition_masks(IBV_QPT_RC, S::IBV_QPS_RTS, S::IBV_QPS_ERR),
            Some((state, accepted))
        );
        assert_eq!(
            qp_transition_masks(IBV_QPT_RC, S::IBV_QPS_INIT, S::IBV_QPS_RESET),
            Some((state, accepted))
        );
        // The kernel lets RESET go only to RESET or INIT.
        assert!(qp_transition_masks(IBV_QPT_RC, S::IBV_QPS_RESET, S::IBV_QPS_ERR).is_none());
    }

    #[test]
    fn rows_match_the_kernel_table() {
        use ffi::ibv_qp_type as T;
        let state = bit(ffi::ibv_qp_attr_mask::IBV_QP_STATE);
        let port = bit(ffi::ibv_qp_attr_mask::IBV_QP_PORT);

        // The state is accepted on a transition that keeps it.
        let (_, optional) =
            qp_transition_masks(IBV_QPT_RC, S::IBV_QPS_INIT, S::IBV_QPS_INIT).unwrap();
        assert_eq!(optional & state, state);
        // UC may leave SQE for RTS.
        assert!(qp_transition_masks(T::IBV_QPT_UC, S::IBV_QPS_SQE, S::IBV_QPS_RTS).is_some());
        // Neither UC nor UD may change its port in SQD, nor a raw-packet queue pair after RESET.
        for (qp_type, cur) in [
            (T::IBV_QPT_UC, S::IBV_QPS_SQD),
            (T::IBV_QPT_UD, S::IBV_QPS_SQD),
            (T::IBV_QPT_RAW_PACKET, S::IBV_QPS_INIT),
            (T::IBV_QPT_RAW_PACKET, S::IBV_QPS_SQD),
        ] {
            let (_, optional) = qp_transition_masks(qp_type, cur, cur).unwrap();
            assert_eq!(optional & port, 0, "{qp_type:?} in {cur:?}");
        }
    }
}

/// The kernel's `qp_state_table` and `ib_modify_qp_is_ok` (drivers/infiniband/core/verbs.c),
/// transcribed cell by cell, against `qp_transition_masks` for every type the crate diagnoses.
#[cfg(test)]
mod test_kernel_table {
    use super::*;
    use ffi::ibv_qp_attr_mask as M;
    use ffi::ibv_qp_state as S;
    use ffi::ibv_qp_type as T;

    const STATE: u32 = M::IBV_QP_STATE.0;
    const CUR: u32 = M::IBV_QP_CUR_STATE.0;
    const NOTIFY: u32 = M::IBV_QP_EN_SQD_ASYNC_NOTIFY.0;
    const ACCESS: u32 = M::IBV_QP_ACCESS_FLAGS.0;
    const PKEY: u32 = M::IBV_QP_PKEY_INDEX.0;
    const PORT: u32 = M::IBV_QP_PORT.0;
    const QKEY: u32 = M::IBV_QP_QKEY.0;
    const AV: u32 = M::IBV_QP_AV.0;
    const MTU: u32 = M::IBV_QP_PATH_MTU.0;
    const TIMEOUT: u32 = M::IBV_QP_TIMEOUT.0;
    const RETRY: u32 = M::IBV_QP_RETRY_CNT.0;
    const RNR_RETRY: u32 = M::IBV_QP_RNR_RETRY.0;
    const RQ_PSN: u32 = M::IBV_QP_RQ_PSN.0;
    const MAX_RD: u32 = M::IBV_QP_MAX_QP_RD_ATOMIC.0;
    const ALT: u32 = M::IBV_QP_ALT_PATH.0;
    const MIN_RNR: u32 = M::IBV_QP_MIN_RNR_TIMER.0;
    const SQ_PSN: u32 = M::IBV_QP_SQ_PSN.0;
    const MAX_DEST: u32 = M::IBV_QP_MAX_DEST_RD_ATOMIC.0;
    const MIG: u32 = M::IBV_QP_PATH_MIG_STATE.0;
    const DEST_QPN: u32 = M::IBV_QP_DEST_QPN.0;
    const RATE: u32 = M::IBV_QP_RATE_LIMIT.0;

    const STATES: [S; 7] = [
        S::IBV_QPS_RESET,
        S::IBV_QPS_INIT,
        S::IBV_QPS_RTR,
        S::IBV_QPS_RTS,
        S::IBV_QPS_SQD,
        S::IBV_QPS_SQE,
        S::IBV_QPS_ERR,
    ];

    /// (valid, req, opt) of the kernel cell for `ty` (UD, UC, RC, RAW_PACKET only).
    fn kernel_cell(cur: S, next: S, ty: T) -> (bool, u32, u32) {
        let pick = |ud: u32, uc: u32, rc: u32, raw: u32| match ty {
            T::IBV_QPT_UD => ud,
            T::IBV_QPT_UC => uc,
            T::IBV_QPT_RC => rc,
            T::IBV_QPT_RAW_PACKET => raw,
            _ => unreachable!(),
        };
        let none = (true, 0, 0);
        match (cur, next) {
            (S::IBV_QPS_RESET, S::IBV_QPS_RESET) => none,
            (S::IBV_QPS_RESET, S::IBV_QPS_INIT) => (
                true,
                pick(
                    PKEY | PORT | QKEY,
                    PKEY | PORT | ACCESS,
                    PKEY | PORT | ACCESS,
                    PORT,
                ),
                0,
            ),
            (S::IBV_QPS_INIT, S::IBV_QPS_RESET) | (S::IBV_QPS_INIT, S::IBV_QPS_ERR) => none,
            (S::IBV_QPS_INIT, S::IBV_QPS_INIT) => (
                true,
                0,
                pick(
                    PKEY | PORT | QKEY,
                    PKEY | PORT | ACCESS,
                    PKEY | PORT | ACCESS,
                    0,
                ),
            ),
            (S::IBV_QPS_INIT, S::IBV_QPS_RTR) => (
                true,
                pick(
                    0,
                    AV | MTU | DEST_QPN | RQ_PSN,
                    AV | MTU | DEST_QPN | RQ_PSN | MAX_DEST | MIN_RNR,
                    0,
                ),
                pick(PKEY | QKEY, ALT | ACCESS | PKEY, ALT | ACCESS | PKEY, 0),
            ),
            (S::IBV_QPS_RTR, S::IBV_QPS_RESET) | (S::IBV_QPS_RTR, S::IBV_QPS_ERR) => none,
            (S::IBV_QPS_RTR, S::IBV_QPS_RTS) => (
                true,
                pick(
                    SQ_PSN,
                    SQ_PSN,
                    TIMEOUT | RETRY | RNR_RETRY | SQ_PSN | MAX_RD,
                    0,
                ),
                pick(
                    CUR | QKEY,
                    CUR | ALT | ACCESS | MIG,
                    CUR | ALT | ACCESS | MIN_RNR | MIG,
                    0,
                ),
            ),
            (S::IBV_QPS_RTS, S::IBV_QPS_RESET) | (S::IBV_QPS_RTS, S::IBV_QPS_ERR) => none,
            (S::IBV_QPS_RTS, S::IBV_QPS_RTS) => (
                true,
                0,
                pick(
                    CUR | QKEY,
                    CUR | ACCESS | ALT | MIG,
                    CUR | ACCESS | ALT | MIG | MIN_RNR,
                    0,
                ),
            ),
            (S::IBV_QPS_RTS, S::IBV_QPS_SQD) => (true, 0, pick(NOTIFY, NOTIFY, NOTIFY, 0)),
            (S::IBV_QPS_SQD, S::IBV_QPS_RESET) | (S::IBV_QPS_SQD, S::IBV_QPS_ERR) => none,
            (S::IBV_QPS_SQD, S::IBV_QPS_RTS) => (
                true,
                0,
                pick(
                    CUR | QKEY,
                    CUR | ALT | ACCESS | MIG,
                    CUR | ALT | ACCESS | MIN_RNR | MIG,
                    0,
                ),
            ),
            (S::IBV_QPS_SQD, S::IBV_QPS_SQD) => (
                true,
                0,
                pick(
                    PKEY | QKEY,
                    AV | ALT | ACCESS | PKEY | MIG,
                    PORT | AV
                        | TIMEOUT
                        | RETRY
                        | RNR_RETRY
                        | MAX_RD
                        | MAX_DEST
                        | ALT
                        | ACCESS
                        | PKEY
                        | MIN_RNR
                        | MIG,
                    0,
                ),
            ),
            (S::IBV_QPS_SQE, S::IBV_QPS_RESET) | (S::IBV_QPS_SQE, S::IBV_QPS_ERR) => none,
            (S::IBV_QPS_SQE, S::IBV_QPS_RTS) => (true, 0, pick(CUR | QKEY, CUR | ACCESS, 0, 0)),
            (S::IBV_QPS_ERR, S::IBV_QPS_RESET) | (S::IBV_QPS_ERR, S::IBV_QPS_ERR) => none,
            _ => (false, 0, 0),
        }
    }

    /// The set of masks the kernel accepts for `cur -> next`, as (bits that must be set, bits that
    /// may be set), folding in ib_modify_qp_is_ok's CUR_STATE rule and the fact that a mask
    /// without STATE means "stay in `cur`" (so a real transition always carries STATE).
    fn kernel(ty: T, cur: S, next: S) -> Option<(u32, u32)> {
        let (valid, req, opt) = kernel_cell(cur, next, ty);
        if !valid {
            return None;
        }
        let mut must = req;
        if next != cur {
            must |= STATE;
        }
        let mut may = req | opt | STATE | RATE;
        if !matches!(
            cur,
            S::IBV_QPS_RTR | S::IBV_QPS_RTS | S::IBV_QPS_SQD | S::IBV_QPS_SQE
        ) {
            may &= !CUR;
        }
        Some((must, may))
    }

    #[test]
    fn every_cell_against_the_kernel() {
        let mut diffs = Vec::new();
        for ty in [
            T::IBV_QPT_RC,
            T::IBV_QPT_UC,
            T::IBV_QPT_UD,
            T::IBV_QPT_RAW_PACKET,
        ] {
            for cur in STATES {
                for next in STATES {
                    let k = kernel(ty, cur, next);
                    let c = qp_transition_masks(ty, cur, next).map(|(r, o)| (r, r | o));
                    if k != c {
                        let show = |m: Option<(u32, u32)>| match m {
                            None => "invalid".to_string(),
                            Some((must, may)) => format!(
                                "must {:?} may-also {:?}",
                                QueuePairAttributeMask(must),
                                QueuePairAttributeMask(may & !must)
                            ),
                        };
                        diffs.push(format!(
                            "{ty:?} {cur:?}->{next:?}: kernel {} / crate {}",
                            show(k),
                            show(c)
                        ));
                    }
                }
            }
        }
        assert!(diffs.is_empty(), "{}", diffs.join("\n"));
    }
}
