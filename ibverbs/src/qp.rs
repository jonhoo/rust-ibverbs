use std::io;
use std::os::raw::c_void;
use std::ptr;
use std::sync::Arc;
use std::time::Duration;

use ffi::ibv_mtu;

use crate::address::Gid;
use crate::address::{AddressHandle, AddressHandleAttribute};
use crate::completion::CompletionQueueInner;
use crate::context::Mtu;
use crate::error::{Error, Result};
use crate::mr::{AccessFlags, LocalMemorySlice, RemoteMemorySlice};
use crate::pd::ProtectionDomainInner;
use crate::srq::SharedReceiveQueue;

#[cfg(doc)]
use crate::{CompletionQueue, ProtectionDomain};

/// The transport service type of a queue pair.
///
/// At creation the type is derived from the transport marker (see [`Transport`] and
/// [`ProtectionDomain::create_qp`]); this enum names the types on the wire and in queries. The
/// types without a marker ([`RawPacket`](Self::RawPacket), the XRC pair, and
/// [`Driver`](Self::Driver) other than EFA's SRD) are not usable through the portable wrapper;
/// when they become so, they will get their own markers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum QueuePairType {
    /// Reliable connection ("RC"): connected to exactly one peer, with in-order, reliable
    /// delivery. Supports sends, RDMA read/write, and atomics.
    ReliableConnection,
    /// Unreliable connection ("UC"): connected to exactly one peer, in order but without
    /// delivery guarantees. Supports sends and RDMA writes.
    UnreliableConnection,
    /// Unreliable datagram ("UD"): connectionless; each send is addressed individually with an
    /// [`AddressHandle`], and each message fits in one MTU.
    UnreliableDatagram,
    /// Raw packet ("raw Ethernet"): sends and receives whole L2 frames, bypassing the transport.
    RawPacket,
    /// The sending side of an extended reliable connection ("XRC send").
    XrcSend,
    /// The receiving side of an extended reliable connection ("XRC recv").
    XrcRecv,
    /// A provider-specific ("driver") queue pair, such as EFA's SRD.
    Driver,
}

impl From<ffi::ibv_qp_type> for QueuePairType {
    fn from(qp_type: ffi::ibv_qp_type) -> Self {
        use ffi::ibv_qp_type::*;
        match qp_type {
            IBV_QPT_RC => QueuePairType::ReliableConnection,
            IBV_QPT_UC => QueuePairType::UnreliableConnection,
            IBV_QPT_UD => QueuePairType::UnreliableDatagram,
            IBV_QPT_RAW_PACKET => QueuePairType::RawPacket,
            IBV_QPT_XRC_SEND => QueuePairType::XrcSend,
            IBV_QPT_XRC_RECV => QueuePairType::XrcRecv,
            IBV_QPT_DRIVER => QueuePairType::Driver,
        }
    }
}

impl From<QueuePairType> for ffi::ibv_qp_type {
    fn from(qp_type: QueuePairType) -> Self {
        use ffi::ibv_qp_type::*;
        match qp_type {
            QueuePairType::ReliableConnection => IBV_QPT_RC,
            QueuePairType::UnreliableConnection => IBV_QPT_UC,
            QueuePairType::UnreliableDatagram => IBV_QPT_UD,
            QueuePairType::RawPacket => IBV_QPT_RAW_PACKET,
            QueuePairType::XrcSend => IBV_QPT_XRC_SEND,
            QueuePairType::XrcRecv => IBV_QPT_XRC_RECV,
            QueuePairType::Driver => IBV_QPT_DRIVER,
        }
    }
}

impl std::fmt::Display for QueuePairType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            QueuePairType::ReliableConnection => "RC",
            QueuePairType::UnreliableConnection => "UC",
            QueuePairType::UnreliableDatagram => "UD",
            QueuePairType::RawPacket => "raw packet",
            QueuePairType::XrcSend => "XRC send",
            QueuePairType::XrcRecv => "XRC recv",
            QueuePairType::Driver => "driver",
        };
        f.write_str(name)
    }
}

/// Seals the transport traits: only the markers defined by this crate implement them.
pub(crate) mod sealed {
    use super::QueuePairType;

    /// Implemented by every transport marker.
    pub trait Sealed {
        /// The [`QueuePairType`] the marker stands for.
        const TYPE: QueuePairType;
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
/// // A datagram queue pair has no remote endpoint to handshake with: use `activate`.
/// fn f(pqp: ibverbs::PreparedQueuePair<ibverbs::Ud>, remote: ibverbs::QueuePairEndpoint) {
///     let _ = pqp.handshake(remote);
/// }
/// ```
pub trait Transport: sealed::Sealed {}

/// Implemented by the connected transports RC and UC, which are brought up against a single
/// remote endpoint with [`PreparedQueuePair::handshake`].
pub trait Connected: Transport {}

/// Implemented by the reliable connected transport RC, the only transport with acknowledgements
/// and retries, RDMA reads, and atomics.
pub trait Reliable: Connected {}

/// Implemented by the datagram transports UD and SRD, which address each send individually with
/// an [`AddressHandle`] ([`SendBatch::to`]).
pub trait Datagram: Transport {}

/// The reliable-connection (RC) transport: connected to exactly one peer, in-order and reliable;
/// supports sends, RDMA read/write, and atomics ([`QueuePairType::ReliableConnection`]).
///
/// Note that this marker shadows [`std::rc::Rc`] when glob-imported (`use ibverbs::*`).
#[derive(Debug, Clone, Copy)]
pub struct Rc;

/// The unreliable-connection (UC) transport: connected to exactly one peer, in order but without
/// delivery guarantees; supports sends and RDMA writes
/// ([`QueuePairType::UnreliableConnection`]).
#[derive(Debug, Clone, Copy)]
pub struct Uc;

/// The unreliable-datagram (UD) transport: connectionless, each message fits in one MTU, and each
/// send is addressed individually with an [`AddressHandle`]
/// ([`QueuePairType::UnreliableDatagram`]).
#[derive(Debug, Clone, Copy)]
pub struct Ud;

impl sealed::Sealed for Rc {
    const TYPE: QueuePairType = QueuePairType::ReliableConnection;
}
impl Transport for Rc {}
impl Connected for Rc {}
impl Reliable for Rc {}

impl sealed::Sealed for Uc {
    const TYPE: QueuePairType = QueuePairType::UnreliableConnection;
}
impl Transport for Uc {}
impl Connected for Uc {}

impl sealed::Sealed for Ud {
    const TYPE: QueuePairType = QueuePairType::UnreliableDatagram;
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
    /// The send queue errored ("SQE"): receives still work, sends are flushed (UD and similar
    /// transports only; RC moves straight to [`Error`](Self::Error)).
    SendQueueError,
    /// The error state: outstanding and new work requests are flushed with
    /// [`WcStatus::WorkRequestFlushed`](crate::WcStatus::WorkRequestFlushed).
    Error,
    /// The state cannot be determined.
    Unknown,
}

impl From<ffi::ibv_qp_state> for QueuePairState {
    fn from(state: ffi::ibv_qp_state) -> Self {
        use ffi::ibv_qp_state::*;
        match state {
            IBV_QPS_RESET => QueuePairState::Reset,
            IBV_QPS_INIT => QueuePairState::Init,
            IBV_QPS_RTR => QueuePairState::ReadyToReceive,
            IBV_QPS_RTS => QueuePairState::ReadyToSend,
            IBV_QPS_SQD => QueuePairState::SendQueueDrain,
            IBV_QPS_SQE => QueuePairState::SendQueueError,
            IBV_QPS_ERR => QueuePairState::Error,
            IBV_QPS_UNKNOWN => QueuePairState::Unknown,
        }
    }
}

impl From<QueuePairState> for ffi::ibv_qp_state {
    fn from(state: QueuePairState) -> Self {
        use ffi::ibv_qp_state::*;
        match state {
            QueuePairState::Reset => IBV_QPS_RESET,
            QueuePairState::Init => IBV_QPS_INIT,
            QueuePairState::ReadyToReceive => IBV_QPS_RTR,
            QueuePairState::ReadyToSend => IBV_QPS_RTS,
            QueuePairState::SendQueueDrain => IBV_QPS_SQD,
            QueuePairState::SendQueueError => IBV_QPS_SQE,
            QueuePairState::Error => IBV_QPS_ERR,
            QueuePairState::Unknown => IBV_QPS_UNKNOWN,
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

/// An unconfigured `QueuePair`. Created by [`ProtectionDomain::create_qp`].
///
/// A `QueuePairBuilder` is used to configure a `QueuePair` before it is allocated and initialized.
/// The setters that only make sense on some transports only exist for those transports (see
/// [`Transport`]). See also [RDMAmojo] for many more details.
///
/// [RDMAmojo]: http://www.rdmamojo.com/2013/01/12/ibv_modify_qp/
pub struct QueuePairBuilder<T: Transport> {
    pub(crate) ctx: isize,
    pub(crate) pd: Arc<ProtectionDomainInner>,
    pub(crate) port_attr: ffi::ibv_port_attr,
    /// the device port this queue pair is associated with (numbered from 1)
    pub(crate) port_num: u8,

    pub(crate) send: Arc<CompletionQueueInner>,
    pub(crate) max_send_wr: u32,
    pub(crate) recv: Arc<CompletionQueueInner>,
    pub(crate) max_recv_wr: u32,

    pub(crate) gid_index: Option<u32>,
    pub(crate) max_send_sge: u32,
    pub(crate) max_recv_sge: u32,
    pub(crate) max_inline_data: u32,

    qp_type: ffi::ibv_qp_type,

    // carried along to handshake phase
    /// traffic class set in Global Routing Headers, only used if `gid_index` is set.
    pub(crate) traffic_class: u8,
    /// only valid for RC and UC
    access: Option<ffi::ibv_access_flags>,
    /// only valid for RC
    timeout: Option<u8>,
    /// only valid for RC
    retry_count: Option<u8>,
    /// only valid for RC
    rnr_retry: Option<u8>,
    /// only valid for RC
    min_rnr_timer: Option<u8>,
    /// only valid for RC
    max_rd_atomic: Option<u8>,
    /// only valid for RC
    max_dest_rd_atomic: Option<u8>,
    /// only valid for RC and UC
    path_mtu: Option<ibv_mtu>,
    /// the packet sequence number the send queue starts at; the peer learns it from the endpoint
    pub(crate) psn: u32,
    /// service level (0-15). Higher value means higher priority.
    pub(crate) service_level: u8,
    /// shared receive queue
    srq: Option<SharedReceiveQueue>,
    _transport: std::marker::PhantomData<T>,
}

impl<T: Transport> QueuePairBuilder<T> {
    /// Prepare a new `QueuePair` builder.
    ///
    /// `max_send_wr` is the maximum number of outstanding Work Requests that can be posted to the
    /// Send Queue in that Queue Pair. Value must be in `[0..dev_cap.max_qp_wr]`. Some devices
    /// support fewer outstanding work requests for specific transport types than the maximum
    /// reported value.
    ///
    /// Similarly, `max_recv_wr` is the maximum number of outstanding Work Requests that can be
    /// posted to the Receive Queue in that Queue Pair. Value must be in `[0..dev_cap.max_qp_wr]`.
    /// Some devices support fewer outstanding work requests for specific transport types than the
    /// maximum reported value. This value is ignored if the Queue Pair is associated with an SRQ.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        pd: Arc<ProtectionDomainInner>,
        port_attr: ffi::ibv_port_attr,
        port_num: u8,
        send: Arc<CompletionQueueInner>,
        max_send_wr: u32,
        recv: Arc<CompletionQueueInner>,
        max_recv_wr: u32,
        qp_type: ffi::ibv_qp_type,
        max_send_sge: u32,
        max_recv_sge: u32,
    ) -> QueuePairBuilder<T> {
        let port_active_mtu = port_attr.active_mtu;
        QueuePairBuilder {
            ctx: 0,
            pd,
            port_attr,
            port_num,

            gid_index: None,
            traffic_class: 0,
            send,
            max_send_wr,
            recv,
            max_recv_wr,

            max_send_sge,
            max_recv_sge,
            max_inline_data: 0,

            qp_type,

            access: (qp_type == ffi::ibv_qp_type::IBV_QPT_RC
                || qp_type == ffi::ibv_qp_type::IBV_QPT_UC)
                .then_some(ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE),
            min_rnr_timer: (qp_type == ffi::ibv_qp_type::IBV_QPT_RC).then_some(16),
            retry_count: (qp_type == ffi::ibv_qp_type::IBV_QPT_RC).then_some(6),
            rnr_retry: (qp_type == ffi::ibv_qp_type::IBV_QPT_RC).then_some(6),
            timeout: (qp_type == ffi::ibv_qp_type::IBV_QPT_RC).then_some(4),
            max_rd_atomic: (qp_type == ffi::ibv_qp_type::IBV_QPT_RC).then_some(1),
            max_dest_rd_atomic: (qp_type == ffi::ibv_qp_type::IBV_QPT_RC).then_some(1),
            path_mtu: (qp_type == ffi::ibv_qp_type::IBV_QPT_RC
                || qp_type == ffi::ibv_qp_type::IBV_QPT_UC)
                .then_some(port_active_mtu),
            psn: 0,
            service_level: 0,
            srq: None,
            _transport: std::marker::PhantomData,
        }
    }

    /// Set the service level of the new `QueuePair`.
    ///
    /// The service level (0–15); higher values mean higher priority.
    ///
    /// Defaults to 0.
    pub fn set_service_level(&mut self, service_level: u8) -> &mut Self {
        self.service_level = service_level;
        self
    }

    /// Sets the GID table index that should be used for the new `QueuePair`.
    /// The entry corresponds to the index in [`Context::gid_table`](crate::Context::gid_table).
    /// [`PreparedQueuePair::endpoint`] embeds the GID at this index in the local endpoint, and
    /// [`handshake`](PreparedQueuePair::handshake) routes with it when the remote endpoint
    /// carries a `gid`.
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
        self.psn = psn;
        self
    }

    /// Set the maximum number of send requests in the work queue.
    ///
    /// Defaults to 1.
    pub fn set_max_send_wr(&mut self, max_send_wr: u32) -> &mut Self {
        self.max_send_wr = max_send_wr;
        self
    }

    /// The maximum number of scatter/gather elements in any Work Request
    /// that can be posted to the Send Queue in that Queue Pair.
    ///
    /// Value can be `[0..dev_cap.max_sge]`. Some devices support fewer
    /// scatter/gather elements for specific transport types than the
    /// maximum reported value.
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
    /// Inline sends (see [`SendOp::send_inline`]) copy their payload directly into the work request
    /// rather than referencing a registered memory region, which lowers latency for small messages.
    /// A send queue must reserve this capacity up front; the actual value granted by the device can
    /// be larger than requested and is reported by [`QueuePair::query`] (see
    /// [`QueuePairInitAttribute::max_inline_data`]). Posting more inline bytes than the queue pair
    /// supports fails at submit time.
    ///
    /// Defaults to 0 (inline sends disabled).
    pub fn set_max_inline_data(&mut self, max_inline_data: u32) -> &mut Self {
        self.max_inline_data = max_inline_data;
        self
    }

    /// The `ibv_create_qp_ex` creation shared by the `build` of every transport it serves (SRD
    /// goes through `efadv_create_qp_ex` instead; see `efa.rs`).
    fn build_impl(&self) -> Result<PreparedQueuePair<T>> {
        use ffi::ibv_qp_create_send_ops_flags as SendOps;
        use ffi::ibv_qp_type::{IBV_QPT_RC, IBV_QPT_UC};

        // Enable the extended send operations we drive through the doorbell post API. SEND is
        // available on every transport; one-sided RDMA needs a connected QP (RC/UC), and RDMA read
        // plus atomics are RC-only.
        let mut send_ops_flags =
            SendOps::IBV_QP_EX_WITH_SEND.0 | SendOps::IBV_QP_EX_WITH_SEND_WITH_IMM.0;
        if matches!(self.qp_type, IBV_QPT_RC | IBV_QPT_UC) {
            send_ops_flags |= SendOps::IBV_QP_EX_WITH_RDMA_WRITE.0
                | SendOps::IBV_QP_EX_WITH_RDMA_WRITE_WITH_IMM.0;
        }
        if self.qp_type == IBV_QPT_RC {
            send_ops_flags |= SendOps::IBV_QP_EX_WITH_RDMA_READ.0
                | SendOps::IBV_QP_EX_WITH_ATOMIC_CMP_AND_SWP.0
                | SendOps::IBV_QP_EX_WITH_ATOMIC_FETCH_AND_ADD.0;
        }

        // `ibv_qp_init_attr_ex` has a `qp_type` field with no zero variant plus fields we never use
        // (XRC, TSO, RX hashing). Zero the storage, write only the fields the driver reads, and hand
        // the pointer to C without `assume_init`, so the untouched enum fields never become a Rust
        // value.
        let mut attr = std::mem::MaybeUninit::<ffi::ibv_qp_init_attr_ex>::zeroed();
        let p = attr.as_mut_ptr();
        unsafe {
            (*p).qp_context = self.ctx as usize as *mut c_void;
            (*p).send_cq = self.send.cq();
            (*p).recv_cq = self.recv.cq();
            (*p).srq = self
                .srq
                .as_ref()
                .map(|s| s.inner.srq)
                .unwrap_or(ptr::null_mut());
            (*p).cap = ffi::ibv_qp_cap {
                max_send_wr: self.max_send_wr,
                max_recv_wr: self.max_recv_wr,
                max_send_sge: self.max_send_sge,
                max_recv_sge: self.max_recv_sge,
                max_inline_data: self.max_inline_data,
            };
            (*p).qp_type = self.qp_type;
            (*p).comp_mask = ffi::ibv_qp_init_attr_mask::IBV_QP_INIT_ATTR_PD.0
                | ffi::ibv_qp_init_attr_mask::IBV_QP_INIT_ATTR_SEND_OPS_FLAGS.0;
            (*p).pd = self.pd.pd;
            (*p).send_ops_flags = send_ops_flags as u64;
        }

        let qp = unsafe { ffi::ibv_create_qp_ex(self.pd.ctx.ctx, attr.as_mut_ptr()) };
        if qp.is_null() {
            Err(Error::os(
                io::Error::last_os_error(),
                Error::CreateQueuePair,
            ))
        } else {
            let qp_ex = unsafe { ffi::ibv_qp_to_qp_ex(qp) };
            Ok(PreparedQueuePair {
                lid: self.port_attr.lid,
                port_num: self.port_num,
                qp: QueuePair {
                    pd: self.pd.clone(),
                    _srq: self.srq.clone(),
                    _send_cq: self.send.clone(),
                    _recv_cq: self.recv.clone(),
                    qp,
                    qp_ex,
                    _transport: std::marker::PhantomData,
                },
                gid_index: self.gid_index,
                traffic_class: self.traffic_class,
                access: self.access,
                timeout: self.timeout,
                retry_count: self.retry_count,
                rnr_retry: self.rnr_retry,
                min_rnr_timer: self.min_rnr_timer,
                max_rd_atomic: self.max_rd_atomic,
                max_dest_rd_atomic: self.max_dest_rd_atomic,
                path_mtu: self.path_mtu,
                psn: self.psn,
                service_level: self.service_level,
            })
        }
    }
}

impl<T: Connected> QueuePairBuilder<T> {
    /// Set the access flags for the new `QueuePair`.
    ///
    /// Defaults to [`AccessFlags::LOCAL_WRITE`].
    pub fn set_access(&mut self, access: AccessFlags) -> &mut Self {
        self.access = Some(access.into());
        self
    }

    /// Set the path MTU.
    ///
    /// Defaults to the port's active MTU.
    pub fn set_path_mtu(&mut self, path_mtu: Mtu) -> &mut Self {
        self.path_mtu = Some(path_mtu.into());
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
    ///  - [`CreateQueuePair`](Error::CreateQueuePair): `ibv_create_qp_ex` failed (`EINVAL` for an
    ///    invalid `ProtectionDomain` or `CompletionQueue`, or an invalid value in `max_send_wr`,
    ///    `max_recv_wr`, or `max_inline_data`; `ENOMEM` when out of resources; `ENOSYS` when the
    ///    device does not support this Transport Service Type; `EPERM` without enough permissions
    ///    to create a QP with this Transport Service Type).
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
    ///  - [`CreateQueuePair`](Error::CreateQueuePair): `ibv_create_qp_ex` failed (`EINVAL` for an
    ///    invalid `ProtectionDomain` or `CompletionQueue`, or an invalid value in `max_send_wr`,
    ///    `max_recv_wr`, or `max_inline_data`; `ENOMEM` when out of resources; `ENOSYS` when the
    ///    device does not support this Transport Service Type; `EPERM` without enough permissions
    ///    to create a QP with this Transport Service Type).
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
        self.min_rnr_timer = Some(timer.encoding());
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
        self.timeout = Some(timeout.exponent());
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
        assert!(count <= 7);
        self.retry_count = Some(count);
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
        assert!(n <= 7);
        self.rnr_retry = Some(n);
        self
    }

    /// Set the number of outstanding RDMA reads & atomic operations on the destination Queue Pair.
    ///
    /// This defaults to 1.
    pub fn set_max_rd_atomic(&mut self, max_rd_atomic: u8) -> &mut Self {
        self.max_rd_atomic = Some(max_rd_atomic);
        self
    }

    /// Set the number of responder resources for handling incoming RDMA reads & atomic operations.
    ///
    /// This defaults to 1.
    pub fn set_max_dest_rd_atomic(&mut self, max_dest_rd_atomic: u8) -> &mut Self {
        self.max_dest_rd_atomic = Some(max_dest_rd_atomic);
        self
    }
}

/// An allocated but uninitialized `QueuePair`. Created by [`QueuePairBuilder::build`].
///
/// Specifically, this `QueuePair` has been allocated with `ibv_create_qp_ex`, but has not yet been
/// initialized with calls to `ibv_modify_qp`.
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
pub struct PreparedQueuePair<T: Transport> {
    pub(crate) qp: QueuePair<T>,
    /// port local identifier
    pub(crate) lid: u16,
    /// the device port this queue pair is associated with (numbered from 1)
    pub(crate) port_num: u8,
    // carried from builder
    pub(crate) gid_index: Option<u32>,
    /// traffic class set in Global Routing Headers, only used if `gid_index` is set.
    pub(crate) traffic_class: u8,
    /// only valid for RC and UC
    pub(crate) access: Option<ffi::ibv_access_flags>,
    /// only valid for RC
    pub(crate) min_rnr_timer: Option<u8>,
    /// only valid for RC
    pub(crate) timeout: Option<u8>,
    /// only valid for RC
    pub(crate) retry_count: Option<u8>,
    /// only valid for RC
    pub(crate) rnr_retry: Option<u8>,
    /// only valid for RC
    pub(crate) max_rd_atomic: Option<u8>,
    /// only valid for RC
    pub(crate) max_dest_rd_atomic: Option<u8>,
    /// only valid for RC and UC
    pub(crate) path_mtu: Option<ibv_mtu>,
    /// the packet sequence number the send queue starts at
    pub(crate) psn: u32,
    /// service level (0-15). Higher value means higher priority.
    pub(crate) service_level: u8,
}

/// An identifier for the network endpoint of a `QueuePair`. Returned by
/// [`PreparedQueuePair::endpoint`], to exchange with the peer through
/// [`to_bytes`](Self::to_bytes)/[`from_bytes`](Self::from_bytes) — small enough to ride in an
/// rdmacm connection request's `private_data`.
///
/// Internally, this contains the `QueuePair`'s `qp_num`, the context's `lid` and `gid`, and the
/// packet sequence number the queue pair's send queue starts at.
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
}

impl QueuePairEndpoint {
    /// The length of the wire encoding produced by [`to_bytes`](Self::to_bytes).
    pub const WIRE_LEN: usize = 27;

    /// Encodes this endpoint in the crate's stable wire format, for exchanging with the peer over
    /// any transport (it also fits an rdmacm connection request's 56-byte `private_data`).
    ///
    /// The layout, in network byte order: one flags byte (bit 0: a GID is present; all other bits
    /// zero), the queue pair number (4 bytes), the LID (2 bytes), the raw GID (16 bytes, zeroed
    /// when absent), and the starting PSN (4 bytes). Adding fields to the format means a new,
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
    /// This endpoint will need to be communicated to the `QueuePair` on the remote end.
    ///
    /// # Errors
    ///
    ///  - [`QueryGid`](Error::QueryGid): querying the GID at the configured index failed
    ///    (`ibv_query_gid`).
    pub fn endpoint(&self) -> Result<QueuePairEndpoint> {
        let qp_num = unsafe { &*self.qp.qp }.qp_num;
        let gid = if let Some(gid_index) = self.gid_index {
            let mut gid = ffi::ibv_gid::default();
            let rc = unsafe {
                ffi::ibv_query_gid(
                    self.qp.pd.ctx.ctx,
                    self.port_num,
                    gid_index as i32,
                    &mut gid,
                )
            };
            if rc < 0 {
                return Err(Error::os(io::Error::last_os_error(), |e| Error::QueryGid {
                    port_num: self.port_num,
                    gid_index,
                    source: e,
                }));
            }
            Some(Gid::from(gid))
        } else {
            None
        };
        Ok(QueuePairEndpoint {
            qp_num,
            lid: self.lid,
            gid,
            psn: self.psn,
        })
    }

    /// The connectionless `INIT` -> `RTR` -> `RTS` bring-up with a Q_Key, shared by the
    /// datagram transports' `activate` (UD here, SRD in `efa.rs`).
    pub(crate) fn activate_impl(self, qkey: u32) -> Result<QueuePair<T>> {
        // INIT: associate with the port and set the Q_Key. UD has no access flags.
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_INIT,
            pkey_index: 0,
            port_num: self.port_num,
            qkey,
            ..Default::default()
        };
        let mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE
            | ffi::ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
            | ffi::ibv_qp_attr_mask::IBV_QP_PORT
            | ffi::ibv_qp_attr_mask::IBV_QP_QKEY;
        let errno = unsafe { ffi::ibv_modify_qp(self.qp.qp, &mut attr as *mut _, mask.0 as i32) };
        if errno != 0 {
            return Err(Error::errno(errno, Error::ModifyQueuePair));
        }

        // RTR: a UD queue pair needs no path or destination information.
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_RTR,
            ..Default::default()
        };
        let mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE;
        let errno = unsafe { ffi::ibv_modify_qp(self.qp.qp, &mut attr as *mut _, mask.0 as i32) };
        if errno != 0 {
            return Err(Error::errno(errno, Error::ModifyQueuePair));
        }

        // RTS: start the send queue at the configured PSN.
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_RTS,
            sq_psn: self.psn,
            ..Default::default()
        };
        let mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE | ffi::ibv_qp_attr_mask::IBV_QP_SQ_PSN;
        let errno = unsafe { ffi::ibv_modify_qp(self.qp.qp, &mut attr as *mut _, mask.0 as i32) };
        if errno != 0 {
            return Err(Error::errno(errno, Error::ModifyQueuePair));
        }

        Ok(self.qp)
    }
}

impl<T: Connected> PreparedQueuePair<T> {
    /// Set up the `QueuePair` such that it is ready to exchange packets with a remote `QueuePair`.
    ///
    /// Internally, this uses `ibv_modify_qp` to mark the `QueuePair` as initialized
    /// (`IBV_QPS_INIT`), ready to receive (`IBV_QPS_RTR`), and ready to send (`IBV_QPS_RTS`),
    /// applying the attributes configured on the builder (access flags, timeouts, path MTU,
    /// service level, and so on) at the appropriate steps. Further discussion of the protocol can
    /// be found on [RDMAmojo]. Use [`into_queue_pair`](Self::into_queue_pair) to drive the state
    /// machine yourself instead.
    ///
    /// If the endpoint contains a Gid, the routing will be global. This means:
    /// ```text
    /// ah_attr.is_global = 1;
    /// ah_attr.grh.hop_limit = 0xff;
    /// ```
    ///
    /// The packet sequence numbers pair up through the endpoints: the local send queue starts at
    /// this queue pair's PSN ([`QueuePairBuilder::set_sq_psn`], carried to the peer by
    /// [`endpoint`](Self::endpoint)), and the local receive queue starts at the PSN in `remote`.
    ///
    /// The queue pair is associated with the port chosen when it was created (see
    /// [`ProtectionDomain::create_qp`]). The handshake also sets the following parameters,
    /// which are currently not configurable:
    ///
    /// ```text
    /// pkey_index = 0;
    /// ah_attr.src_path_bits = 0;
    /// ```
    ///
    /// # Errors
    ///
    ///  - [`GidMismatch`](Error::GidMismatch): the remote endpoint carries a GID, but no
    ///    `gid_index` was set on the builder to route from.
    ///  - [`ModifyQueuePair`](Error::ModifyQueuePair): a state transition failed
    ///    (`ibv_modify_qp`), for example because an attribute is invalid for this queue pair's
    ///    type or the remote endpoint is unreachable.
    ///
    /// [RDMAmojo]: http://www.rdmamojo.com/2014/01/18/connecting-queue-pairs/
    pub fn handshake(self, remote: QueuePairEndpoint) -> Result<QueuePair<T>> {
        // init and associate with port
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_INIT,
            pkey_index: 0,
            port_num: self.port_num,
            ..Default::default()
        };
        let mut mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE
            | ffi::ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
            | ffi::ibv_qp_attr_mask::IBV_QP_PORT;
        if let Some(access) = self.access {
            attr.qp_access_flags = access.0;
            mask |= ffi::ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
        }
        let errno = unsafe { ffi::ibv_modify_qp(self.qp.qp, &mut attr as *mut _, mask.0 as i32) };
        if errno != 0 {
            return Err(Error::errno(errno, Error::ModifyQueuePair));
        }

        // set ready to receive; the destination and path are meaningful because this bring-up
        // only exists on the connected transports (RC and UC)
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_RTR,
            dest_qp_num: remote.qp_num,
            ah_attr: ffi::ibv_ah_attr {
                dlid: remote.lid,
                sl: self.service_level,
                src_path_bits: 0,
                port_num: self.port_num,
                grh: Default::default(),
                ..Default::default()
            },
            ..Default::default()
        };
        if let Some(gid) = remote.gid {
            attr.ah_attr.is_global = 1;
            attr.ah_attr.grh.dgid = gid.into();
            attr.ah_attr.grh.hop_limit = 0xff;
            attr.ah_attr.grh.sgid_index = self.gid_index.ok_or(Error::GidMismatch)? as u8;
            attr.ah_attr.grh.traffic_class = self.traffic_class;
        }
        let mut mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE
            | ffi::ibv_qp_attr_mask::IBV_QP_AV
            | ffi::ibv_qp_attr_mask::IBV_QP_DEST_QPN;
        if let Some(max_dest_rd_atomic) = self.max_dest_rd_atomic {
            attr.max_dest_rd_atomic = max_dest_rd_atomic;
            mask |= ffi::ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC;
        }
        if let Some(min_rnr_timer) = self.min_rnr_timer {
            attr.min_rnr_timer = min_rnr_timer;
            mask |= ffi::ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER;
        }
        if let Some(path_mtu) = self.path_mtu {
            attr.path_mtu = path_mtu;
            mask |= ffi::ibv_qp_attr_mask::IBV_QP_PATH_MTU;
        }
        // The receive queue starts at the PSN the peer's send queue starts at.
        attr.rq_psn = remote.psn;
        mask |= ffi::ibv_qp_attr_mask::IBV_QP_RQ_PSN;
        let errno = unsafe { ffi::ibv_modify_qp(self.qp.qp, &mut attr as *mut _, mask.0 as i32) };
        if errno != 0 {
            // On RoCE, the provider resolves the route to the remote GID during this transition,
            // and reports a GID that does not answer as a timeout or unreachable network. Spell
            // that out: it is the most common RoCE bring-up failure, and "connection timed out"
            // alone sends people looking at the wrong layer.
            if remote.gid.is_some()
                && (errno == nix::libc::ETIMEDOUT || errno == nix::libc::ENETUNREACH)
            {
                let source = io::Error::from_raw_os_error(errno);
                return Err(Error::ModifyQueuePair(io::Error::new(
                    source.kind(),
                    format!(
                        "resolving the route to the remote GID failed ({source}); on RoCE this \
                         usually means the remote GID does not answer on the network of the \
                         local GID at index {}, or a firewall drops RoCE (UDP 4791) traffic",
                        attr.ah_attr.grh.sgid_index,
                    ),
                )));
            }
            return Err(Error::errno(errno, Error::ModifyQueuePair));
        }

        // set ready to send, starting the send queue at the PSN advertised in our endpoint
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_RTS,
            sq_psn: self.psn,
            ..Default::default()
        };
        let mut mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE | ffi::ibv_qp_attr_mask::IBV_QP_SQ_PSN;
        if let Some(timeout) = self.timeout {
            attr.timeout = timeout;
            mask |= ffi::ibv_qp_attr_mask::IBV_QP_TIMEOUT;
        }
        if let Some(retry_count) = self.retry_count {
            attr.retry_cnt = retry_count;
            mask |= ffi::ibv_qp_attr_mask::IBV_QP_RETRY_CNT;
        }
        if let Some(rnr_retry) = self.rnr_retry {
            attr.rnr_retry = rnr_retry;
            mask |= ffi::ibv_qp_attr_mask::IBV_QP_RNR_RETRY;
        }
        if let Some(max_rd_atomic) = self.max_rd_atomic {
            attr.max_rd_atomic = max_rd_atomic;
            mask |= ffi::ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC;
        }
        let errno = unsafe { ffi::ibv_modify_qp(self.qp.qp, &mut attr as *mut _, mask.0 as i32) };
        if errno != 0 {
            return Err(Error::errno(errno, Error::ModifyQueuePair));
        }

        Ok(self.qp)
    }
}

impl PreparedQueuePair<Ud> {
    /// Activate this unreliable datagram (UD) queue pair.
    ///
    /// Unlike a connected transport's handshake, UD is connectionless: there is no remote
    /// endpoint to exchange, so the queue pair is transitioned `INIT -> RTR -> RTS` with the given
    /// `qkey`. Incoming datagrams whose Q_Key does not match `qkey` are discarded (a sender whose
    /// work request sets the Q_Key's most significant bit transmits with its own QP's Q_Key
    /// instead).
    ///
    /// Each datagram is addressed individually at send time with an [`AddressHandle`]; see
    /// [`SendBatch::to`].
    ///
    /// # Errors
    ///
    ///  - [`ModifyQueuePair`](Error::ModifyQueuePair): a state transition failed (`EINVAL` for an
    ///    invalid value in `attr` or `attr_mask`, `ENOMEM` when out of resources).
    pub fn activate(self, qkey: u32) -> Result<QueuePair<Ud>> {
        self.activate_impl(qkey)
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
                sg_list: local.as_ptr() as *mut ffi::ibv_sge,
                num_sge: local.len() as i32,
            },
            _local: std::marker::PhantomData,
        }
    }
}

/// A batch of send work requests being built on a [`QueuePair`]'s send queue.
///
/// Created by [`QueuePair::start_send`]. Each work request starts at [`op`](Self::op) (on a
/// datagram queue pair, at [`to`](Self::to), which also addresses it), is optionally modified
/// ([`signaled`](SendOp::signaled), [`fenced`](SendOp::fenced),
/// [`solicited`](SendOp::solicited)), and ends with the opcode method (`send`, `write`, `read`,
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
#[must_use = "a started batch must be `.submit()`ed (otherwise it is aborted on drop)"]
pub struct SendBatch<'qp, T: Transport> {
    qpx: *mut ffi::ibv_qp_ex,
    _qp: std::marker::PhantomData<&'qp mut QueuePair<T>>,
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
            dest: None,
        }
    }
}

impl<'qp, T: Datagram> SendBatch<'qp, T> {
    /// Begin one work request on the batch, addressed to `ah` / `remote_qpn` / `remote_qkey`.
    ///
    /// Every datagram send needs a destination, so this is the datagram batch's entry point; the
    /// destination is per work request, so one batch may address each request to a different
    /// peer. The returned [`AddressedSendOp`] carries the request until an opcode method posts
    /// it: chain the modifiers first (`batch.to(&ah, qpn, qkey).signaled().send(id, sges)`),
    /// because the provider reads the work-request flags and addressing inside the opcode
    /// builder.
    ///
    /// `ah` stays borrowed until the request is posted, since the provider reads the handle inside
    /// the opcode builder. Keep it alive until the request completes too: some providers (rxe,
    /// for one) only resolve the handle when the request executes.
    #[inline]
    pub fn to<'b>(
        &'b mut self,
        ah: &'b AddressHandle,
        remote_qpn: u32,
        remote_qkey: u32,
    ) -> AddressedSendOp<'b, 'qp, T> {
        AddressedSendOp {
            op: SendOp {
                batch: self,
                flags: 0,
                dest: Some((ah.as_ptr(), remote_qpn, remote_qkey)),
            },
        }
    }
}

impl<'qp, T: Transport> SendBatch<'qp, T> {
    /// Post the whole batch to the device, ringing the doorbell once.
    ///
    /// # Safety
    ///
    /// Every memory region referenced by the batch must stay valid until a work completion has been
    /// polled for the corresponding `wr_id`.
    ///
    /// # Errors
    ///
    ///  - [`PostSend`](Error::PostSend): completing the batch failed (`EINVAL` for an invalid
    ///    value in one of the work requests, `ENOMEM` when the send queue is full or out of
    ///    resources).
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
/// [`SendBatch::op`] (connected transports; a datagram batch starts each request at
/// [`SendBatch::to`], which yields an [`AddressedSendOp`] instead).
///
/// Chain the modifiers ([`signaled`](Self::signaled), [`fenced`](Self::fenced),
/// [`solicited`](Self::solicited)) and finish with an opcode method (`send`, `write`, ...), which
/// posts the request immediately. The opcodes exist only on the transports that support them (see
/// [`Transport`]).
pub struct SendOp<'b, 'qp, T: Transport> {
    batch: &'b mut SendBatch<'qp, T>,
    flags: u32,
    dest: Option<(*mut ffi::ibv_ah, u32, u32)>,
}

impl<T: Transport> SendOp<'_, '_, T> {
    /// Mark this work request signaled (`IBV_SEND_SIGNALED`), so it generates a completion.
    #[inline]
    pub fn signaled(mut self) -> Self {
        self.flags |= ffi::ibv_send_flags::IBV_SEND_SIGNALED.0;
        self
    }

    /// Mark this work request fenced (`IBV_SEND_FENCE`): the device blocks it until prior RDMA
    /// reads and atomic operations on this queue pair have completed. Used to order a send or
    /// write after a read whose data it depends on.
    #[inline]
    pub fn fenced(mut self) -> Self {
        self.flags |= ffi::ibv_send_flags::IBV_SEND_FENCE.0;
        self
    }

    /// Mark this work request solicited (`IBV_SEND_SOLICITED`): a SEND or SEND-with-immediate
    /// raises a solicited event on the remote side, waking a peer blocked on its completion
    /// channel after arming with [`CompletionQueue::req_notify`] for solicited events only.
    #[inline]
    pub fn solicited(mut self) -> Self {
        self.flags |= ffi::ibv_send_flags::IBV_SEND_SOLICITED.0;
        self
    }

    /// Post one work request: set id and flags, run the opcode builder, then the optional datagram
    /// address, then the scatter list.
    #[inline]
    pub(crate) fn build(
        self,
        wr_id: u64,
        local: &[LocalMemorySlice],
        op: impl FnOnce(*mut ffi::ibv_qp_ex),
    ) {
        let qpx = self.batch.qpx;
        unsafe {
            (*qpx).wr_id = wr_id;
            (*qpx).wr_flags = self.flags;
            op(qpx);
            if let Some((ah, qpn, qkey)) = self.dest {
                (*qpx).wr_set_ud_addr.unwrap()(qpx, ah, qpn, qkey);
            }
            (*qpx).wr_set_sge_list.unwrap()(
                qpx,
                local.len(),
                local.as_ptr() as *const ffi::ibv_sge,
            );
        }
    }

    /// Like [`build`](Self::build), but copies `data` inline into the work request instead of
    /// referencing a registered memory region. The bytes are copied during this call, so `data` need
    /// not outlive the work completion.
    #[inline]
    fn build_inline(self, wr_id: u64, data: &[u8], op: impl FnOnce(*mut ffi::ibv_qp_ex)) {
        let qpx = self.batch.qpx;
        unsafe {
            (*qpx).wr_id = wr_id;
            // `IBV_SEND_INLINE` is the work-request flag that marks the payload as inline; it is what
            // the legacy `ibv_post_send` ABI carries, and rdma-core's generic doorbell emulation
            // (used by providers such as Soft-RoCE) sets it when translating `wr_set_inline_data`.
            // Native doorbell providers key off the `wr_set_inline_data` call itself, so the flag is
            // redundant but harmless there.
            (*qpx).wr_flags = self.flags | ffi::ibv_send_flags::IBV_SEND_INLINE.0;
            op(qpx);
            if let Some((ah, qpn, qkey)) = self.dest {
                (*qpx).wr_set_ud_addr.unwrap()(qpx, ah, qpn, qkey);
            }
            (*qpx).wr_set_inline_data.unwrap()(qpx, data.as_ptr() as *mut c_void, data.len());
        }
    }

    /// Like [`build_inline`](Self::build_inline), but gathers several buffers into the inline payload
    /// of a single work request (`wr_set_inline_data_list`). The bytes are copied during this call,
    /// so none of the `bufs` need outlive the work completion.
    #[inline]
    fn build_inline_list(
        self,
        wr_id: u64,
        bufs: &[io::IoSlice<'_>],
        op: impl FnOnce(*mut ffi::ibv_qp_ex),
    ) {
        let qpx = self.batch.qpx;
        unsafe {
            (*qpx).wr_id = wr_id;
            (*qpx).wr_flags = self.flags | ffi::ibv_send_flags::IBV_SEND_INLINE.0;
            op(qpx);
            if let Some((ah, qpn, qkey)) = self.dest {
                (*qpx).wr_set_ud_addr.unwrap()(qpx, ah, qpn, qkey);
            }
            // `std::io::IoSlice` is guaranteed ABI-compatible with `struct iovec` on Unix (the only
            // platform rdma-core targets), and `ibv_data_buf` has the same layout as `iovec` (an
            // address and a length), so the buffer list passes straight through without copying it
            // into a temporary array.
            (*qpx).wr_set_inline_data_list.unwrap()(
                qpx,
                bufs.len(),
                bufs.as_ptr() as *const ffi::ibv_data_buf,
            );
        }
    }
}

impl<T: Connected> SendOp<'_, '_, T> {
    /// Post a SEND.
    #[inline]
    pub fn send(self, wr_id: u64, local: &[LocalMemorySlice]) {
        self.build(wr_id, local, |q| unsafe { (*q).wr_send.unwrap()(q) })
    }

    /// Post a SEND whose payload is carried inline in the work request.
    ///
    /// Inline data is copied into the work request rather than referenced through a memory region,
    /// which lowers latency for small messages and lets `data` be reused or dropped right away. The
    /// queue pair must have been built with enough inline capacity (see
    /// [`QueuePairBuilder::set_max_inline_data`]) or the [`submit`](SendBatch::submit) fails with
    /// `EINVAL`.
    #[inline]
    pub fn send_inline(self, wr_id: u64, data: &[u8]) {
        self.build_inline(wr_id, data, |q| unsafe { (*q).wr_send.unwrap()(q) })
    }

    /// Post a SEND carrying an inline payload and a 32-bit immediate (host byte order).
    ///
    /// See [`send_inline`](Self::send_inline) for the inline-data requirements.
    #[inline]
    pub fn send_imm_inline(self, wr_id: u64, data: &[u8], imm: u32) {
        self.build_inline(wr_id, data, move |q| unsafe {
            (*q).wr_send_imm.unwrap()(q, imm.to_be())
        })
    }

    /// Post an RDMA WRITE into `remote` whose payload is carried inline in the work request.
    ///
    /// See [`send_inline`](Self::send_inline) for the inline-data requirements.
    #[inline]
    pub fn write_inline(self, wr_id: u64, data: &[u8], remote: RemoteMemorySlice) {
        self.build_inline(wr_id, data, move |q| unsafe {
            (*q).wr_rdma_write.unwrap()(q, remote.rkey, remote.addr)
        })
    }

    /// Post an RDMA WRITE into `remote` carrying an inline payload and a 32-bit immediate (host byte
    /// order).
    ///
    /// See [`send_inline`](Self::send_inline) for the inline-data requirements.
    #[inline]
    pub fn write_imm_inline(self, wr_id: u64, data: &[u8], remote: RemoteMemorySlice, imm: u32) {
        self.build_inline(wr_id, data, move |q| unsafe {
            (*q).wr_rdma_write_imm.unwrap()(q, remote.rkey, remote.addr, imm.to_be())
        })
    }

    /// Post a SEND whose inline payload is gathered from several buffers.
    ///
    /// Like [`send_inline`](Self::send_inline), but concatenates `bufs` into one inline payload
    /// (saving a copy into a contiguous buffer first), so the queue pair must have enough inline
    /// capacity for their combined length. See [`send_inline`](Self::send_inline) for the
    /// inline-data requirements.
    #[inline]
    pub fn send_inline_list(self, wr_id: u64, bufs: &[io::IoSlice<'_>]) {
        self.build_inline_list(wr_id, bufs, |q| unsafe { (*q).wr_send.unwrap()(q) })
    }

    /// Post a SEND carrying a gathered inline payload and a 32-bit immediate (host byte order).
    ///
    /// See [`send_inline_list`](Self::send_inline_list) for the gathering behavior.
    #[inline]
    pub fn send_imm_inline_list(self, wr_id: u64, bufs: &[io::IoSlice<'_>], imm: u32) {
        self.build_inline_list(wr_id, bufs, move |q| unsafe {
            (*q).wr_send_imm.unwrap()(q, imm.to_be())
        })
    }

    /// Post an RDMA WRITE into `remote` whose inline payload is gathered from several buffers.
    ///
    /// See [`send_inline_list`](Self::send_inline_list) for the gathering behavior.
    #[inline]
    pub fn write_inline_list(
        self,
        wr_id: u64,
        bufs: &[io::IoSlice<'_>],
        remote: RemoteMemorySlice,
    ) {
        self.build_inline_list(wr_id, bufs, move |q| unsafe {
            (*q).wr_rdma_write.unwrap()(q, remote.rkey, remote.addr)
        })
    }

    /// Post an RDMA WRITE into `remote` carrying a gathered inline payload and a 32-bit immediate
    /// (host byte order).
    ///
    /// See [`send_inline_list`](Self::send_inline_list) for the gathering behavior.
    #[inline]
    pub fn write_imm_inline_list(
        self,
        wr_id: u64,
        bufs: &[io::IoSlice<'_>],
        remote: RemoteMemorySlice,
        imm: u32,
    ) {
        self.build_inline_list(wr_id, bufs, move |q| unsafe {
            (*q).wr_rdma_write_imm.unwrap()(q, remote.rkey, remote.addr, imm.to_be())
        })
    }

    /// Post a SEND carrying a 32-bit immediate (host byte order).
    #[inline]
    pub fn send_imm(self, wr_id: u64, local: &[LocalMemorySlice], imm: u32) {
        self.build(wr_id, local, move |q| unsafe {
            (*q).wr_send_imm.unwrap()(q, imm.to_be())
        })
    }

    /// Post an RDMA WRITE into `remote`.
    #[inline]
    pub fn write(self, wr_id: u64, local: &[LocalMemorySlice], remote: RemoteMemorySlice) {
        self.build(wr_id, local, move |q| unsafe {
            (*q).wr_rdma_write.unwrap()(q, remote.rkey, remote.addr)
        })
    }

    /// Post an RDMA WRITE carrying a 32-bit immediate (host byte order).
    #[inline]
    pub fn write_imm(
        self,
        wr_id: u64,
        local: &[LocalMemorySlice],
        remote: RemoteMemorySlice,
        imm: u32,
    ) {
        self.build(wr_id, local, move |q| unsafe {
            (*q).wr_rdma_write_imm.unwrap()(q, remote.rkey, remote.addr, imm.to_be())
        })
    }
}

impl<T: Reliable> SendOp<'_, '_, T> {
    /// Post an RDMA READ from `remote` into `local`.
    #[inline]
    pub fn read(self, wr_id: u64, local: &[LocalMemorySlice], remote: RemoteMemorySlice) {
        self.build(wr_id, local, move |q| unsafe {
            (*q).wr_rdma_read.unwrap()(q, remote.rkey, remote.addr)
        })
    }

    /// Post an atomic compare-and-swap on the 8-byte value at `remote`.
    #[inline]
    pub fn atomic_cmp_swap(
        self,
        wr_id: u64,
        local: &[LocalMemorySlice],
        remote: RemoteMemorySlice,
        compare: u64,
        swap: u64,
    ) {
        self.build(wr_id, local, move |q| unsafe {
            (*q).wr_atomic_cmp_swp.unwrap()(q, remote.rkey, remote.addr, compare, swap)
        })
    }

    /// Post an atomic fetch-and-add on the 8-byte value at `remote`.
    #[inline]
    pub fn atomic_fetch_add(
        self,
        wr_id: u64,
        local: &[LocalMemorySlice],
        remote: RemoteMemorySlice,
        add: u64,
    ) {
        self.build(wr_id, local, move |q| unsafe {
            (*q).wr_atomic_fetch_add.unwrap()(q, remote.rkey, remote.addr, add)
        })
    }
}

/// One datagram work request being configured and posted on a [`SendBatch`], addressed at
/// creation by [`SendBatch::to`].
///
/// Chain the modifiers ([`signaled`](Self::signaled), [`fenced`](Self::fenced),
/// [`solicited`](Self::solicited)) and finish with an opcode method, which posts the request
/// immediately. UD supports the two-sided sends (including the inline variants); SRD (behind the
/// `efa` feature) additionally supports RDMA write (with immediate) and read.
pub struct AddressedSendOp<'b, 'qp, T: Datagram> {
    pub(crate) op: SendOp<'b, 'qp, T>,
}

impl<T: Datagram> AddressedSendOp<'_, '_, T> {
    /// Mark this work request signaled (`IBV_SEND_SIGNALED`), so it generates a completion.
    #[inline]
    pub fn signaled(self) -> Self {
        AddressedSendOp {
            op: self.op.signaled(),
        }
    }

    /// Mark this work request fenced (`IBV_SEND_FENCE`): the device blocks it until prior RDMA
    /// reads and atomic operations on this queue pair have completed. Used to order a send or
    /// write after a read whose data it depends on.
    #[inline]
    pub fn fenced(self) -> Self {
        AddressedSendOp {
            op: self.op.fenced(),
        }
    }

    /// Mark this work request solicited (`IBV_SEND_SOLICITED`): a SEND or SEND-with-immediate
    /// raises a solicited event on the remote side, waking a peer blocked on its completion
    /// channel after arming with [`CompletionQueue::req_notify`] for solicited events only.
    #[inline]
    pub fn solicited(self) -> Self {
        AddressedSendOp {
            op: self.op.solicited(),
        }
    }

    /// Post a SEND.
    #[inline]
    pub fn send(self, wr_id: u64, local: &[LocalMemorySlice]) {
        self.op
            .build(wr_id, local, |q| unsafe { (*q).wr_send.unwrap()(q) })
    }

    /// Post a SEND carrying a 32-bit immediate (host byte order).
    #[inline]
    pub fn send_imm(self, wr_id: u64, local: &[LocalMemorySlice], imm: u32) {
        self.op.build(wr_id, local, move |q| unsafe {
            (*q).wr_send_imm.unwrap()(q, imm.to_be())
        })
    }

    /// Post a SEND whose payload is carried inline in the work request (see
    /// [`QueuePairBuilder::set_max_inline_data`]).
    #[inline]
    pub fn send_inline(self, wr_id: u64, data: &[u8]) {
        self.op
            .build_inline(wr_id, data, |q| unsafe { (*q).wr_send.unwrap()(q) })
    }

    /// Post a SEND carrying an inline payload and a 32-bit immediate (host byte order).
    #[inline]
    pub fn send_imm_inline(self, wr_id: u64, data: &[u8], imm: u32) {
        self.op.build_inline(wr_id, data, move |q| unsafe {
            (*q).wr_send_imm.unwrap()(q, imm.to_be())
        })
    }

    /// Post a SEND whose inline payload is gathered from several buffers.
    #[inline]
    pub fn send_inline_list(self, wr_id: u64, bufs: &[io::IoSlice<'_>]) {
        self.op
            .build_inline_list(wr_id, bufs, |q| unsafe { (*q).wr_send.unwrap()(q) })
    }

    /// Post a SEND carrying a gathered inline payload and a 32-bit immediate (host byte order).
    #[inline]
    pub fn send_imm_inline_list(self, wr_id: u64, bufs: &[io::IoSlice<'_>], imm: u32) {
        self.op.build_inline_list(wr_id, bufs, move |q| unsafe {
            (*q).wr_send_imm.unwrap()(q, imm.to_be())
        })
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

    /// Set the minimum RNR-NAK timer (see [`RnrTimer`] for the typed form).
    pub fn set_min_rnr_timer(&mut self, min_rnr_timer: u8) -> &mut Self {
        self.attr.min_rnr_timer = min_rnr_timer;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER;
        self
    }

    /// Set the ACK timeout (see [`AckTimeout`] for the typed form).
    pub fn set_timeout(&mut self, timeout: u8) -> &mut Self {
        self.attr.timeout = timeout;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_TIMEOUT;
        self
    }

    /// Set the retry count for the primary path.
    pub fn set_retry_count(&mut self, retry_count: u8) -> &mut Self {
        self.attr.retry_cnt = retry_count;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_RETRY_CNT;
        self
    }

    /// Set the RNR retry count (see [`QueuePairBuilder::set_rnr_retry`] for the encoding).
    pub fn set_rnr_retry(&mut self, rnr_retry: u8) -> &mut Self {
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
    pub fn min_rnr_timer(&self) -> u8 {
        self.attr.min_rnr_timer
    }

    /// The ACK timeout.
    pub fn timeout(&self) -> u8 {
        self.attr.timeout
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
pub struct QueuePairInitAttribute {
    init_attr: ffi::ibv_qp_init_attr,
}

impl QueuePairInitAttribute {
    /// The maximum number of outstanding send work requests.
    pub fn max_send_wr(&self) -> u32 {
        self.init_attr.cap.max_send_wr
    }

    /// The maximum number of outstanding receive work requests.
    pub fn max_recv_wr(&self) -> u32 {
        self.init_attr.cap.max_recv_wr
    }

    /// The maximum number of scatter-gather entries per send work request.
    pub fn max_send_sge(&self) -> u32 {
        self.init_attr.cap.max_send_sge
    }

    /// The maximum number of scatter-gather entries per receive work request.
    pub fn max_recv_sge(&self) -> u32 {
        self.init_attr.cap.max_recv_sge
    }

    /// The maximum amount of inline data, in bytes.
    pub fn max_inline_data(&self) -> u32 {
        self.init_attr.cap.max_inline_data
    }

    /// The underlying `ibv_qp_init_attr`. Escape hatch for fields this crate does not wrap.
    pub fn as_raw(&self) -> &ffi::ibv_qp_init_attr {
        &self.init_attr
    }
}

/// The required and optional attribute-mask bits for a `cur -> next` transition of a queue pair of
/// the given type, or `None` if the transition is not valid.
///
/// This mirrors the kernel's `qp_state_table` (drivers/infiniband/core/verbs.c): every queue pair
/// may move to `RESET` or `ERR` from any state with only `IBV_QP_STATE`, and each type allows a
/// specific set of forward transitions. It is used only to turn an `EINVAL` from `ibv_modify_qp`
/// into a more precise [`Error`].
fn qp_transition_masks(
    qp_type: ffi::ibv_qp_type,
    cur: ffi::ibv_qp_state,
    next: ffi::ibv_qp_state,
) -> Option<(u32, u32)> {
    use ffi::ibv_qp_state::*;
    use ffi::ibv_qp_type::*;

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

    // Any state may move to RESET or ERR with only IBV_QP_STATE.
    if let IBV_QPS_RESET | IBV_QPS_ERR = next {
        return Some((state, 0));
    }

    match qp_type {
        IBV_QPT_RC | IBV_QPT_XRC_SEND | IBV_QPT_XRC_RECV => match (cur, next) {
            (IBV_QPS_RESET, IBV_QPS_INIT) => Some((state | pkey | port | access, 0)),
            (IBV_QPS_INIT, IBV_QPS_INIT) => Some((0, pkey | port | access)),
            (IBV_QPS_INIT, IBV_QPS_RTR) => Some((
                state | av | path_mtu | dest_qpn | rq_psn | max_dest_rd | min_rnr,
                pkey | access | alt_path,
            )),
            (IBV_QPS_RTR, IBV_QPS_RTS) => Some((
                state | sq_psn | timeout | retry | rnr_retry | max_rd,
                cur_state | access | min_rnr | alt_path | mig,
            )),
            (IBV_QPS_RTS, IBV_QPS_RTS) => Some((0, cur_state | access | min_rnr | alt_path | mig)),
            (IBV_QPS_RTS, IBV_QPS_SQD) => Some((state, sqd_async)),
            (IBV_QPS_SQD, IBV_QPS_RTS) => {
                Some((state, cur_state | access | min_rnr | alt_path | mig))
            }
            (IBV_QPS_SQD, IBV_QPS_SQD) => Some((
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
            _ => None,
        },
        IBV_QPT_UC => match (cur, next) {
            (IBV_QPS_RESET, IBV_QPS_INIT) => Some((state | pkey | port | access, 0)),
            (IBV_QPS_INIT, IBV_QPS_INIT) => Some((0, pkey | port | access)),
            (IBV_QPS_INIT, IBV_QPS_RTR) => Some((
                state | av | path_mtu | dest_qpn | rq_psn,
                pkey | access | alt_path,
            )),
            (IBV_QPS_RTR, IBV_QPS_RTS) => {
                Some((state | sq_psn, cur_state | access | alt_path | mig))
            }
            (IBV_QPS_RTS, IBV_QPS_RTS) => Some((0, cur_state | access | alt_path | mig)),
            (IBV_QPS_RTS, IBV_QPS_SQD) => Some((state, sqd_async)),
            (IBV_QPS_SQD, IBV_QPS_RTS) => Some((state, cur_state | access | alt_path | mig)),
            (IBV_QPS_SQD, IBV_QPS_SQD) => Some((0, pkey | port | access | av | alt_path | mig)),
            _ => None,
        },
        IBV_QPT_UD => match (cur, next) {
            (IBV_QPS_RESET, IBV_QPS_INIT) => Some((state | pkey | port | qkey, 0)),
            (IBV_QPS_INIT, IBV_QPS_INIT) => Some((0, pkey | port | qkey)),
            (IBV_QPS_INIT, IBV_QPS_RTR) => Some((state, pkey | qkey)),
            (IBV_QPS_RTR, IBV_QPS_RTS) => Some((state | sq_psn, cur_state | qkey)),
            (IBV_QPS_RTS, IBV_QPS_RTS) => Some((0, cur_state | qkey)),
            (IBV_QPS_RTS, IBV_QPS_SQD) => Some((state, sqd_async)),
            (IBV_QPS_SQD, IBV_QPS_RTS) => Some((state, cur_state | qkey)),
            (IBV_QPS_SQD, IBV_QPS_SQD) => Some((0, pkey | port | qkey)),
            (IBV_QPS_SQE, IBV_QPS_RTS) => Some((state, cur_state | qkey)),
            _ => None,
        },
        IBV_QPT_RAW_PACKET => match (cur, next) {
            (IBV_QPS_RESET, IBV_QPS_INIT) => Some((state | port, 0)),
            (IBV_QPS_INIT, IBV_QPS_INIT) => Some((0, port)),
            (IBV_QPS_INIT, IBV_QPS_RTR) => Some((state, 0)),
            (IBV_QPS_RTR, IBV_QPS_RTS) => Some((state, rate)),
            (IBV_QPS_RTS, IBV_QPS_RTS) => Some((0, rate)),
            (IBV_QPS_RTS, IBV_QPS_SQD) => Some((state, sqd_async)),
            (IBV_QPS_SQD, IBV_QPS_RTS) => Some((state, rate)),
            (IBV_QPS_SQD, IBV_QPS_SQD) => Some((0, port | rate)),
            _ => None,
        },
        _ => None,
    }
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
    pub(crate) pd: Arc<ProtectionDomainInner>,
    pub(crate) _srq: Option<SharedReceiveQueue>,
    // Keep the completion queues alive while the queue pair references them; `ibv_destroy_cq` fails
    // with EBUSY if a queue pair is still attached.
    pub(crate) _send_cq: Arc<CompletionQueueInner>,
    pub(crate) _recv_cq: Arc<CompletionQueueInner>,
    pub(crate) qp: *mut ffi::ibv_qp,
    // The extended (doorbell) view of `qp`, used by the send path. `ibv_qp_to_qp_ex` is a cast, so
    // this aliases `qp` and lives exactly as long.
    pub(crate) qp_ex: *mut ffi::ibv_qp_ex,
    pub(crate) _transport: std::marker::PhantomData<T>,
}

unsafe impl<T: Transport> Send for QueuePair<T> {}
unsafe impl<T: Transport> Sync for QueuePair<T> {}

impl<T: Transport> QueuePair<T> {
    /// Returns the local QP number of this QueuePair.
    pub fn qp_num(&self) -> u32 {
        unsafe { *self.qp }.qp_num
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
            let next = if attr.mask.0 & ffi::ibv_qp_attr_mask::IBV_QP_STATE.0 != 0 {
                attr.attr.qp_state
            } else {
                unsafe { (*self.qp).state }
            };
            return Err(self.diagnose_modify(attr.mask, next));
        }
        Err(Error::errno(errno, Error::ModifyQueuePair))
    }

    /// Query this queue pair's attributes (`ibv_query_qp`).
    ///
    /// `mask` selects which attributes to read; the returned [`QueuePairAttribute`]'s getters are
    /// meaningful only for the requested fields. The second return value describes the queue pair's
    /// configured capacities.
    ///
    /// # Errors
    ///
    ///  - [`QueryQueuePair`](Error::QueryQueuePair): `ibv_query_qp` failed.
    pub fn query(
        &self,
        mask: QueuePairAttributeMask,
    ) -> Result<(QueuePairAttribute, QueuePairInitAttribute)> {
        let mask = ffi::ibv_qp_attr_mask::from(mask);
        let mut attr = ffi::ibv_qp_attr::default();
        // `ibv_qp_init_attr` has no valid all-zero representation (its `qp_type` enum has no 0
        // variant), so zero the storage and only `assume_init` once `ibv_query_qp` has filled it in.
        let mut init_attr = std::mem::MaybeUninit::<ffi::ibv_qp_init_attr>::zeroed();
        let errno = unsafe {
            ffi::ibv_query_qp(
                self.qp,
                &mut attr as *mut _,
                mask.0 as i32,
                init_attr.as_mut_ptr(),
            )
        };
        if errno != 0 {
            return Err(Error::errno(errno, Error::QueryQueuePair));
        }
        let init_attr = unsafe { init_attr.assume_init() };
        Ok((
            QueuePairAttribute { attr, mask },
            QueuePairInitAttribute { init_attr },
        ))
    }

    /// Turn a rejected [`modify`](Self::modify) into a precise [`Error`], consulting the queue-pair
    /// state table for the actual queue-pair type and current state.
    fn diagnose_modify(&self, mask: ffi::ibv_qp_attr_mask, next: ffi::ibv_qp_state) -> Error {
        let raw = || Error::ModifyQueuePair(io::Error::from_raw_os_error(nix::libc::EINVAL));
        let cur = unsafe { (*self.qp).state };
        let qp_type = unsafe { (*self.qp).qp_type };
        // Only the types with a transition table can be diagnosed; others (e.g. driver/SRD) fall
        // back to the raw error.
        match qp_type {
            ffi::ibv_qp_type::IBV_QPT_RC
            | ffi::ibv_qp_type::IBV_QPT_UC
            | ffi::ibv_qp_type::IBV_QPT_UD
            | ffi::ibv_qp_type::IBV_QPT_RAW_PACKET
            | ffi::ibv_qp_type::IBV_QPT_XRC_SEND
            | ffi::ibv_qp_type::IBV_QPT_XRC_RECV => {}
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
    /// # Safety
    ///
    /// Each referenced memory region must stay valid until a work completion has been polled for the
    /// corresponding `wr_id`.
    ///
    /// # Errors
    ///
    ///  - [`PostReceive`](Error::PostReceive): `ibv_post_recv` failed (`EINVAL` for an invalid
    ///    value in one of the work requests, `ENOMEM` when the receive queue is full or out of
    ///    resources).
    pub unsafe fn post_recv<'a>(&mut self, mut recvs: impl AsMut<[RecvRequest<'a>]>) -> Result<()> {
        let recvs = recvs.as_mut();
        if recvs.is_empty() {
            return Ok(());
        }
        // Link the requests into the list `ibv_post_recv` expects.
        for i in 0..recvs.len() - 1 {
            let next = &mut recvs[i + 1].wr as *mut ffi::ibv_recv_wr;
            recvs[i].wr.next = next;
        }
        recvs.last_mut().unwrap().wr.next = ptr::null_mut();

        let mut bad_wr: *mut ffi::ibv_recv_wr = ptr::null_mut();
        let ctx = unsafe { *self.qp }.context;
        let ops = &mut unsafe { *ctx }.ops;
        let errno = unsafe {
            ops.post_recv.as_mut().unwrap()(
                self.qp,
                &mut recvs[0].wr as *mut _,
                &mut bad_wr as *mut _,
            )
        };
        if errno != 0 {
            Err(Error::errno(errno, Error::PostReceive))
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
    /// transport — see [`Transport`]). Nothing reaches the device until
    /// [`submit`](SendBatch::submit), which rings the doorbell once for the whole batch.
    ///
    /// As with any send, the memory backing each scatter/gather slice must stay valid until a work
    /// completion has been polled for the request.
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
        let qpx = self.qp_ex;
        unsafe { (*qpx).wr_start.unwrap()(qpx) };
        SendBatch {
            qpx,
            _qp: std::marker::PhantomData,
        }
    }
}

impl<T: Transport> Drop for QueuePair<T> {
    fn drop(&mut self) {
        // TODO: ibv_destroy_qp() fails if the QP is attached to a multicast group.
        let errno = unsafe { ffi::ibv_destroy_qp(self.qp) };
        if errno != 0 {
            let e = io::Error::from_raw_os_error(errno);
            panic!("ibv_destroy_qp failed: {e}");
        }
    }
}

#[cfg(test)]
mod test_conversions {
    use super::*;

    #[test]
    fn queue_pair_type_roundtrip() {
        for (wrapper, raw) in [
            (
                QueuePairType::ReliableConnection,
                ffi::ibv_qp_type::IBV_QPT_RC,
            ),
            (
                QueuePairType::UnreliableDatagram,
                ffi::ibv_qp_type::IBV_QPT_UD,
            ),
            (QueuePairType::Driver, ffi::ibv_qp_type::IBV_QPT_DRIVER),
        ] {
            assert_eq!(QueuePairType::from(raw), wrapper);
            assert_eq!(ffi::ibv_qp_type::from(wrapper), raw);
        }
        assert_eq!(QueuePairType::ReliableConnection.to_string(), "RC");
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
    fn attribute_mask_bit_ops_and_roundtrip() {
        let mask = QueuePairAttributeMask::STATE | QueuePairAttributeMask::PORT;
        assert!(mask.contains(QueuePairAttributeMask::STATE));
        assert!(!mask.contains(QueuePairAttributeMask::QKEY));
        let raw: ffi::ibv_qp_attr_mask = mask.into();
        assert_eq!(
            raw,
            ffi::ibv_qp_attr_mask::IBV_QP_STATE | ffi::ibv_qp_attr_mask::IBV_QP_PORT
        );
        assert_eq!(QueuePairAttributeMask::from(raw), mask);
    }

    #[test]
    fn attribute_mask_debug_lists_flag_names() {
        let mask = QueuePairAttributeMask::STATE
            | QueuePairAttributeMask::PKEY_INDEX
            | QueuePairAttributeMask::PORT;
        assert_eq!(
            format!("{mask:?}"),
            "QueuePairAttributeMask(STATE | PKEY_INDEX | PORT)"
        );
        assert_eq!(
            format!("{:?}", QueuePairAttributeMask::empty()),
            "QueuePairAttributeMask(0)"
        );
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
    use ffi::ibv_qp_state::*;
    use ffi::ibv_qp_type::IBV_QPT_RC;

    fn bit(m: ffi::ibv_qp_attr_mask) -> u32 {
        m.0
    }

    #[test]
    fn reset_to_init_requires_pkey_port_access() {
        let (required, _optional) =
            qp_transition_masks(IBV_QPT_RC, IBV_QPS_RESET, IBV_QPS_INIT).unwrap();
        let expected = bit(ffi::ibv_qp_attr_mask::IBV_QP_STATE)
            | bit(ffi::ibv_qp_attr_mask::IBV_QP_PKEY_INDEX)
            | bit(ffi::ibv_qp_attr_mask::IBV_QP_PORT)
            | bit(ffi::ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS);
        assert_eq!(required, expected);
    }

    #[test]
    fn init_to_rts_is_not_a_valid_transition() {
        assert!(qp_transition_masks(IBV_QPT_RC, IBV_QPS_INIT, IBV_QPS_RTS).is_none());
    }

    #[test]
    fn any_state_to_reset_or_err_needs_only_state() {
        let state = bit(ffi::ibv_qp_attr_mask::IBV_QP_STATE);
        assert_eq!(
            qp_transition_masks(IBV_QPT_RC, IBV_QPS_RTS, IBV_QPS_ERR),
            Some((state, 0))
        );
        assert_eq!(
            qp_transition_masks(IBV_QPT_RC, IBV_QPS_INIT, IBV_QPS_RESET),
            Some((state, 0))
        );
    }
}
