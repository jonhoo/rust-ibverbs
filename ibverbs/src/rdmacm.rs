//! The RDMA connection manager (`librdmacm`).
//!
//! The connection manager establishes reliably-connected queue pairs (and datagram associations)
//! over an IP address, so applications do not have to exchange
//! [`QueuePairEndpoint`](crate::QueuePairEndpoint)s
//! out of band the way [`PreparedQueuePair::handshake`](crate::PreparedQueuePair::handshake)
//! requires. It picks the device, resolves the route, and negotiates the queue pair parameters.
//!
//! Connection setup is blocking and has two roles, with distinct types so they cannot be mixed up:
//! the active side uses a [`Connector`], the passive side an [`Acceptor`]. Each owns its event
//! channel and drives the whole exchange internally, handing back a connected [`Connection`].
//!
//! Setup is two-phase because the device only exists once the address resolves: a handle exposes
//! the [`Context`] to build the queue pair on, then a second call finishes the connection. This
//! wrapper deliberately does not use `rdma_create_qp`/`rdma_create_ep` (which tie the queue pair's
//! lifetime to the connection and limit control over its attributes); you build a normal queue pair
//! and get it back, fully connected, only once setup completes.
//!
//! # Active side
//!
//! ```no_run
//! use std::time::Duration;
//! use ibverbs::rdmacm::{ConnectionParameter, Connector, PortSpace};
//!
//! # fn main() -> ibverbs::Result<()> {
//! let resolved = Connector::new(PortSpace::Tcp)?
//!     .resolve("192.0.2.1:18515".parse().unwrap(), Duration::from_secs(2))?;
//! let ctx = resolved.context()?;
//! let port = resolved.cm_id().port_num().expect("a resolved id has a port");
//! let pd = ctx.alloc_pd()?;
//! let cq = ctx.create_cq(16).build()?;
//! let qp = pd
//!     .create_qp::<ibverbs::Rc>(&cq, &cq, port)?
//!     .build()?;
//! let mut conn = resolved.connect(qp, ConnectionParameter::default(), None)?;
//! // `conn.queue_pair()` is ready to post on; poll completions on `cq`.
//! # let _ = &mut conn;
//! # Ok(())
//! # }
//! ```
//!
//! # Passive side
//!
//! Symmetric: [`Acceptor::bind`] then [`Acceptor::accept`] yields an [`Incoming`] whose
//! [`accept`](Incoming::accept) returns a [`Connection`]. A full client/server example lives in
//! `examples/rdmacm_connect.rs`.
//!
//! # Low-level control
//!
//! The [`Connector`]/[`Acceptor`] helpers block
//! while they drive the connection-manager state machine, so they cannot be integrated with an event
//! loop or async runtime. For full control, drive the state machine yourself with a
//! [`CmId`]: it exposes every step ([`resolve_addr`], route resolution,
//! [`connect`], [`accept`], …), hands back each [`CmEvent`] as it arrives,
//! and its file descriptor ([`AsRawFd`] / [`AsFd`]) is non-blocking, so you can wait on it with
//! `epoll`, `poll`, a `tokio` `AsyncFd`, or any other reactor and pump
//! events with [`poll_cm_event`]. You build the queue pair on the
//! [`context`](CmId::context) and [`port_num`](CmId::port_num) the id resolves to and transition
//! it with [`init_qp_attr`](CmId::init_qp_attr) plus
//! [`QueuePair::modify`](crate::QueuePair::modify): on the active side `Init` before [`connect`],
//! then — once the [`ConnectResponse`](CmEventType::ConnectResponse) has arrived — `Init` again
//! (the attributes computed before the connection existed carry no remote-access flags),
//! `ReadyToReceive`, `ReadyToSend`, and [`establish`](CmId::establish); on the passive side
//! `Init`, `ReadyToReceive`, and `ReadyToSend` before [`accept`]. The blocking helpers are written
//! on top of this same API.
//!
//! [`resolve_addr`]: CmId::resolve_addr
//! [`connect`]: CmId::connect
//! [`accept`]: CmId::accept
//! [`poll_cm_event`]: CmId::poll_cm_event
//! [`AsRawFd`]: std::os::fd::AsRawFd
//! [`AsFd`]: std::os::fd::AsFd

use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};
use std::os::raw::{c_int, c_void};
use std::ptr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nix::sys::socket::{SockaddrIn, SockaddrIn6, SockaddrLike, SockaddrStorage};

use crate::error::{check, nonnull};
use crate::qp::QueuePairState;
use crate::{
    AckTimeout, Context, Error, PreparedQueuePair, QueuePair, QueuePairAttribute, Rc, Result,
};

/// The port space a connection-manager identifier lives in: which namespace its port numbers are
/// allocated from, and which transport its connections use. Passed to [`Connector::new`],
/// [`Acceptor::bind`], and [`CmId::create`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PortSpace {
    /// IP over InfiniBand.
    Ipoib,
    /// TCP port space: reliable connections (RC). The usual choice.
    Tcp,
    /// UDP port space: unreliable datagrams (UD).
    Udp,
    /// The InfiniBand port space, for any port number.
    Ib,
    /// A value this version of the crate does not know.
    Unknown(u32),
}

impl From<PortSpace> for ffi::rdma_port_space {
    fn from(port_space: PortSpace) -> Self {
        use ffi::rdma_port_space as E;
        match port_space {
            PortSpace::Ipoib => E::RDMA_PS_IPOIB,
            PortSpace::Tcp => E::RDMA_PS_TCP,
            PortSpace::Udp => E::RDMA_PS_UDP,
            PortSpace::Ib => E::RDMA_PS_IB,
            PortSpace::Unknown(raw) => ffi::rdma_port_space(raw),
        }
    }
}

/// The kind of a connection-manager event. Returned by [`CmEvent::event_type`]; see [`CmId`] for
/// the sequence in which the events arrive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CmEventType {
    /// The destination address resolved to an RDMA device.
    AddressResolved,
    /// Resolving the destination address failed.
    AddressError,
    /// The route to the destination resolved.
    RouteResolved,
    /// Resolving the route failed.
    RouteError,
    /// An incoming connection request arrived on a listener (take its id with
    /// [`CmEvent::connection_request`]).
    ConnectRequest,
    /// The remote accepted a connection whose queue pair the connection manager does not manage;
    /// finish with [`CmId::establish`].
    ConnectResponse,
    /// Establishing the connection failed.
    ConnectError,
    /// The remote is unreachable.
    Unreachable,
    /// The remote rejected the connection request.
    Rejected,
    /// The connection is established.
    Established,
    /// The connection was disconnected.
    Disconnected,
    /// The device backing the id was removed. The id is dead from then on: the kernel destroys its
    /// side of it and never reports on it again, so drop it and the resources built on its
    /// device. Until it is dropped, a wait on it only ends at its timeout, and the calls that
    /// reach the kernel fail with `ENXIO`.
    DeviceRemoval,
    /// A multicast join completed.
    MulticastJoin,
    /// A multicast join failed or the group errored.
    MulticastError,
    /// The id's network address changed.
    AddressChange,
    /// The connection left the timewait state; its queue pair may be reused.
    TimewaitExit,
    /// Address information resolved (`rdma_getaddrinfo`-style resolution).
    AddressInfoResolved,
    /// Resolving address information failed.
    AddressInfoError,
    /// A user-generated event.
    User,
    /// An internal event.
    Internal,
    /// A value this version of the crate does not know.
    Unknown(u32),
}

impl From<ffi::rdma_cm_event_type> for CmEventType {
    fn from(event: ffi::rdma_cm_event_type) -> Self {
        use ffi::rdma_cm_event_type as E;
        match event {
            E::RDMA_CM_EVENT_ADDR_RESOLVED => CmEventType::AddressResolved,
            E::RDMA_CM_EVENT_ADDR_ERROR => CmEventType::AddressError,
            E::RDMA_CM_EVENT_ROUTE_RESOLVED => CmEventType::RouteResolved,
            E::RDMA_CM_EVENT_ROUTE_ERROR => CmEventType::RouteError,
            E::RDMA_CM_EVENT_CONNECT_REQUEST => CmEventType::ConnectRequest,
            E::RDMA_CM_EVENT_CONNECT_RESPONSE => CmEventType::ConnectResponse,
            E::RDMA_CM_EVENT_CONNECT_ERROR => CmEventType::ConnectError,
            E::RDMA_CM_EVENT_UNREACHABLE => CmEventType::Unreachable,
            E::RDMA_CM_EVENT_REJECTED => CmEventType::Rejected,
            E::RDMA_CM_EVENT_ESTABLISHED => CmEventType::Established,
            E::RDMA_CM_EVENT_DISCONNECTED => CmEventType::Disconnected,
            E::RDMA_CM_EVENT_DEVICE_REMOVAL => CmEventType::DeviceRemoval,
            E::RDMA_CM_EVENT_MULTICAST_JOIN => CmEventType::MulticastJoin,
            E::RDMA_CM_EVENT_MULTICAST_ERROR => CmEventType::MulticastError,
            E::RDMA_CM_EVENT_ADDR_CHANGE => CmEventType::AddressChange,
            E::RDMA_CM_EVENT_TIMEWAIT_EXIT => CmEventType::TimewaitExit,
            E::RDMA_CM_EVENT_ADDRINFO_RESOLVED => CmEventType::AddressInfoResolved,
            E::RDMA_CM_EVENT_ADDRINFO_ERROR => CmEventType::AddressInfoError,
            E::RDMA_CM_EVENT_USER => CmEventType::User,
            E::RDMA_CM_EVENT_INTERNAL => CmEventType::Internal,
            other => CmEventType::Unknown(other.0),
        }
    }
}

impl std::fmt::Display for PortSpace {
    /// Formats the port space as it is named in the C headers, for example `TCP` for
    /// [`Tcp`](Self::Tcp).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            PortSpace::Ipoib => "IPOIB",
            PortSpace::Tcp => "TCP",
            PortSpace::Udp => "UDP",
            PortSpace::Ib => "IB",
            PortSpace::Unknown(raw) => return write!(f, "unknown ({raw})"),
        };
        f.write_str(name)
    }
}

impl std::fmt::Display for CmEventType {
    /// Formats the event as it is named in the C headers, for example `CONNECT_REQUEST` for
    /// [`ConnectRequest`](Self::ConnectRequest).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            CmEventType::AddressResolved => "ADDR_RESOLVED",
            CmEventType::AddressError => "ADDR_ERROR",
            CmEventType::RouteResolved => "ROUTE_RESOLVED",
            CmEventType::RouteError => "ROUTE_ERROR",
            CmEventType::ConnectRequest => "CONNECT_REQUEST",
            CmEventType::ConnectResponse => "CONNECT_RESPONSE",
            CmEventType::ConnectError => "CONNECT_ERROR",
            CmEventType::Unreachable => "UNREACHABLE",
            CmEventType::Rejected => "REJECTED",
            CmEventType::Established => "ESTABLISHED",
            CmEventType::Disconnected => "DISCONNECTED",
            CmEventType::DeviceRemoval => "DEVICE_REMOVAL",
            CmEventType::MulticastJoin => "MULTICAST_JOIN",
            CmEventType::MulticastError => "MULTICAST_ERROR",
            CmEventType::AddressChange => "ADDR_CHANGE",
            CmEventType::TimewaitExit => "TIMEWAIT_EXIT",
            CmEventType::AddressInfoResolved => "ADDR_INFO_RESOLVED",
            CmEventType::AddressInfoError => "ADDR_INFO_ERROR",
            CmEventType::User => "USER",
            CmEventType::Internal => "INTERNAL",
            CmEventType::Unknown(raw) => return write!(f, "unknown ({raw})"),
        };
        f.write_str(name)
    }
}

/// Decode a `sockaddr` owned by an `rdma_cm_id` into a socket address. Returns `None` for any
/// address family other than IPv4 and IPv6 — in particular `AF_UNSPEC`, an id whose address has
/// not been bound or resolved yet.
///
/// # Safety
///
/// `sa` must point to `sockaddr_storage`-sized readable memory (which the `rdma_cm_id`'s own
/// address storage is).
unsafe fn socket_addr_of(sa: *const ffi::sockaddr) -> Option<SocketAddr> {
    // Each `from_raw` checks the family before reading its own struct.
    let sa = sa.cast::<nix::libc::sockaddr>();
    unsafe { SockaddrIn::from_raw(sa, None) }
        .map(SocketAddr::from)
        .or_else(|| unsafe { SockaddrIn6::from_raw(sa, None) }.map(SocketAddr::from))
}

/// `timeout` in the whole milliseconds librdmacm takes, rounded up like every other wait in the
/// crate: truncated, a sub-millisecond timeout would become 0 and expire at once.
fn timeout_ms(timeout: Duration) -> c_int {
    timeout
        .as_nanos()
        .div_ceil(1_000_000)
        .min(c_int::MAX as u128) as c_int
}

/// The `RDMA_MAX_RESP_RES` / `RDMA_MAX_INIT_DEPTH` sentinels of `rdma_cma.h`: a connection
/// parameter of this value means "as many as the device supports, negotiated with the peer", and
/// the queue pair gets the attributes `rdma_init_qp_attr` computed, capped to the device's
/// limits.
const RDMA_MAX_RESP_RES: u8 = 0xFF;
const RDMA_MAX_INIT_DEPTH: u8 = 0xFF;

/// The most private-data bytes a connection request (`rdma_connect`) can carry in a port space:
/// the InfiniBand CM REQ (92 bytes) or, for the datagram port spaces, SIDR REQ (216 bytes) payload,
/// minus the 36-byte header the connection manager prepends in the IP-based port spaces.
fn max_connect_private_data(ps: ffi::rdma_port_space) -> usize {
    use ffi::rdma_port_space as E;
    match ps {
        E::RDMA_PS_TCP => 56,
        E::RDMA_PS_IB => 92,
        E::RDMA_PS_UDP | E::RDMA_PS_IPOIB => 180,
        _ => 56,
    }
}

/// The most private-data bytes a reply (`rdma_accept`) can carry in a port space: the CM REP
/// (196 bytes) or SIDR REP (136 bytes) payload.
fn max_accept_private_data(ps: ffi::rdma_port_space) -> usize {
    use ffi::rdma_port_space as E;
    match ps {
        E::RDMA_PS_TCP | E::RDMA_PS_IB => 196,
        _ => 136,
    }
}

/// The most private-data bytes a rejection (`rdma_reject`) can carry in a port space: the CM REJ
/// (148 bytes) or, for the datagram port spaces, SIDR REP (136 bytes) payload.
fn max_reject_private_data(ps: ffi::rdma_port_space) -> usize {
    use ffi::rdma_port_space as E;
    match ps {
        E::RDMA_PS_TCP | E::RDMA_PS_IB => 148,
        _ => 136,
    }
}

/// Checks that `len` bytes of private data (`what`, for the error message) fit in `limit`.
fn check_private_data(len: usize, limit: usize, what: &str) -> io::Result<()> {
    if len > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{what} is limited to {limit} bytes, got {len}"),
        ));
    }
    Ok(())
}

/// The blocking helpers only set up reliable connections; the datagram port spaces would make
/// them wait for events that never come.
fn require_connected_port_space(port_space: PortSpace, helper: &str) -> Result<()> {
    match port_space {
        PortSpace::Tcp | PortSpace::Ib => Ok(()),
        _ => Err(Error::ConnectionSetup(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{helper} only sets up reliable connections (PortSpace::Tcp or PortSpace::Ib); \
                 drive a CmId directly for the datagram port spaces"
            ),
        ))),
    }
}

/// Whether a connection-manager event reports a failure that aborts setup.
fn is_failure(event: CmEventType) -> bool {
    matches!(
        event,
        CmEventType::AddressError
            | CmEventType::RouteError
            | CmEventType::ConnectError
            | CmEventType::Unreachable
            | CmEventType::Rejected
            | CmEventType::DeviceRemoval
    )
}

/// An rdma_cm event channel. Owned 1:1 by the [`CmId`] whose events it carries; it is created with
/// the id and destroyed after it.
struct EventChannel {
    chan: *mut ffi::rdma_event_channel,
}

impl EventChannel {
    /// Opens a new event channel, with its file descriptor set non-blocking so reading an event
    /// ([`CmId::poll_cm_event`]) reports an empty channel instead of blocking.
    fn new() -> Result<EventChannel> {
        let chan = nonnull(
            unsafe { ffi::rdma_create_event_channel() },
            Error::ConnectionSetup,
        )?;
        let channel = EventChannel { chan };
        // SAFETY: the channel owns this fd, and the borrow ends within this call.
        let fd = unsafe { BorrowedFd::borrow_raw((*chan).fd) };
        // If either call fails, `channel` drops here and destroys the channel again.
        let flags = nix::fcntl::fcntl(fd, nix::fcntl::F_GETFL)
            .map_err(|e| Error::ConnectionSetup(e.into()))?;
        let arg = nix::fcntl::FcntlArg::F_SETFL(
            nix::fcntl::OFlag::from_bits_retain(flags) | nix::fcntl::OFlag::O_NONBLOCK,
        );
        nix::fcntl::fcntl(fd, arg).map_err(|e| Error::ConnectionSetup(e.into()))?;
        Ok(channel)
    }
}

impl Drop for EventChannel {
    fn drop(&mut self) {
        unsafe { ffi::rdma_destroy_event_channel(self.chan) };
    }
}

/// The shared ownership of an `rdma_cm_id` and its event channel: destroyed once the last
/// [`CmId`] clone drops.
struct CmIdInner {
    channel: EventChannel,
    id: *mut ffi::rdma_cm_id,
}

// Ownership of an `rdma_cm_id` and its channel can be moved (and shared) between threads.
unsafe impl Send for CmIdInner {}
unsafe impl Sync for CmIdInner {}

impl Drop for CmIdInner {
    fn drop(&mut self) {
        // Destroy the id before its channel (the `channel` field drops right after this).
        unsafe { ffi::rdma_destroy_id(self.id) };
    }
}

/// An rdma_cm identifier (a connection or a listener) together with its own event channel.
/// Created by [`CmId::create`] (or [`CmEvent::connection_request`] on the passive side).
///
/// This is the low-level connection-manager handle: it exposes every step of connection setup so
/// you can drive the state machine yourself, from an event loop if you like, instead of using the
/// blocking [`Connector`]/[`Acceptor`] helpers (which are built on top of it). See the
/// [module-level docs](self#low-level-control) for the overall flow. A typical active-side sequence
/// is [`resolve_addr`](Self::resolve_addr), [`resolve_route`](Self::resolve_route),
/// [`connect`](Self::connect), [`establish`](Self::establish), pumping for the matching
/// [`CmEvent`] after each with [`get_cm_event`](Self::get_cm_event) (blocking) or
/// [`poll_cm_event`](Self::poll_cm_event) (non-blocking); the queue pair is built on
/// [`context`](Self::context) and [`port_num`](Self::port_num), and transitioned with
/// [`init_qp_attr`](Self::init_qp_attr).
///
/// Cloning is cheap (reference counted); the id (and then its channel) is destroyed once the last
/// clone drops, which ends whatever it was doing: a listener stops listening, a request not yet
/// accepted is declined, a connection is disconnected. Resources built on its
/// [`context`](Self::context) do not keep it alive.
#[derive(Clone)]
pub struct CmId {
    inner: Arc<CmIdInner>,
}

impl CmId {
    /// Creates a new identifier on its own fresh event channel, for the given port space (use
    /// [`PortSpace::Tcp`] for reliable connections).
    ///
    /// This is the entry point for driving connection setup yourself; see the [type-level
    /// docs](Self) for the sequence of calls.
    pub fn create(port_space: PortSpace) -> Result<CmId> {
        let channel = EventChannel::new()?;
        let mut id: *mut ffi::rdma_cm_id = ptr::null_mut();
        let ret = unsafe {
            ffi::rdma_create_id(
                channel.chan,
                &mut id,
                ptr::null_mut::<c_void>(),
                port_space.into(),
            )
        };
        // On failure `channel` drops on return, destroying the event channel.
        check(ret, Error::ConnectionSetup)?;
        Ok(CmId {
            inner: Arc::new(CmIdInner { channel, id }),
        })
    }

    /// Blocks until the next event on this id's channel is available and returns it, even if other
    /// threads race it for events.
    ///
    /// The event is acknowledged automatically when the returned [`CmEvent`] drops. To wait from
    /// an event loop instead, use [`poll_cm_event`](Self::poll_cm_event).
    pub fn get_cm_event(&self) -> Result<CmEvent> {
        self.get_cm_event_deadline(None)
    }

    /// Returns the next event on this id's channel, or `None` if none is currently pending. This
    /// never blocks.
    ///
    /// Intended for event-loop use: wait for the (non-blocking) file descriptor from
    /// [`AsRawFd`]/[`AsFd`] to become readable with your
    /// reactor of choice, then drain pending events with this method (acknowledged on drop).
    pub fn poll_cm_event(&self) -> Result<Option<CmEvent>> {
        // The read itself, which `get_cm_event` waits around; the channel is non-blocking, so an
        // empty one reports `EAGAIN`.
        let mut event: *mut ffi::rdma_cm_event = ptr::null_mut();
        nix::errno::Errno::clear();
        let ret = unsafe { ffi::rdma_get_cm_event(self.inner.channel.chan, &mut event) };
        if ret != 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::WouldBlock {
                return Ok(None);
            }
            return Err(Error::os(e, Error::ConnectionSetup));
        }
        // The kernel reports a failure as a negative errno, but when librdmacm itself fails to
        // finish an address or route resolution while reading the event (querying the device,
        // address, or path), it sets the status to -1 and leaves the reason in `errno`, which is
        // only readable now. `errno` was cleared above, so a -1 from the kernel (`-EPERM`) stays.
        let mut status = unsafe { (*event).status };
        if status == -1 {
            let errno = nix::errno::Errno::last_raw();
            if errno != 0 {
                status = -errno;
            }
        }
        Ok(Some(CmEvent {
            event,
            status,
            _id: self.clone(),
            taken: false,
        }))
    }

    /// A context on the device the connection manager has bound this id to. Build the queue pair
    /// (and its protection domain and completion queue) on this context, on the port
    /// [`port_num`](Self::port_num) reports. Only available once the address has resolved (after
    /// [`CmEventType::AddressResolved`]).
    ///
    /// The context is this crate's own, independent of the id: one per device, shared by every id
    /// on that device while any of them uses it, so its asynchronous events
    /// ([`Context::poll_async_event`]) are those of all of them. It does not keep this id alive:
    /// dropping the id's last handle declines its request or disconnects its connection right
    /// away, however long the queue pair and its resources live on.
    ///
    /// # Errors
    ///
    ///  - [`ConnectionSetup`](Error::ConnectionSetup): the id has no device yet.
    ///  - [`OpenDevice`](Error::OpenDevice): opening the device failed (`ibv_open_device`).
    pub fn context(&self) -> Result<Context> {
        Context::shared(unsafe { (*self.verbs()?).device })
    }

    /// Computes the queue-pair attributes the connection manager derives for transitioning to
    /// `target_state` (`rdma_init_qp_attr`), to apply with [`QueuePair::modify`].
    ///
    /// When driving setup yourself, move the queue pair through [`Init`](QueuePairState::Init),
    /// [`ReadyToReceive`](QueuePairState::ReadyToReceive), and
    /// [`ReadyToSend`](QueuePairState::ReadyToSend) at the points the blocking helpers do (see the
    /// [module docs](self#low-level-control)) by calling this for each state and passing the
    /// result to [`QueuePair::modify`](crate::QueuePair::modify). On the active side, apply
    /// `Init` a second time once the [`ConnectResponse`](CmEventType::ConnectResponse) has
    /// arrived: before the connection exists the `Init` attributes carry no remote-access flags,
    /// and only the second application grants the peer the RDMA access negotiated in the request.
    ///
    /// The outstanding RDMA read and atomic limits (`max_dest_rd_atomic` at `ReadyToReceive`,
    /// `max_rd_atomic` at `ReadyToSend`) are capped to this device's, as `rdma_accept` does: on
    /// the passive side the connection manager reports what the peer requested, which by default
    /// is the peer device's own maximum and can exceed this device's.
    ///
    /// The recipe above is for reliable connections. In the datagram port spaces this returns the
    /// `Init` attributes for every state, which `ReadyToReceive` refuses (they carry the port), so
    /// apply it only for `Init`, then move the queue pair to `ReadyToReceive` with its state alone
    /// and to `ReadyToSend` with its state and a send PSN
    /// ([`set_sq_psn`](QueuePairAttribute::set_sq_psn)), as librdmacm does.
    pub fn init_qp_attr(&self, target_state: QueuePairState) -> Result<QueuePairAttribute> {
        use ffi::ibv_qp_attr_mask as M;
        // `rdma_init_qp_attr` reads the target state from the attribute and fills in the fields it
        // reports in the mask.
        let mut attr = ffi::ibv_qp_attr {
            qp_state: target_state.into(),
            ..Default::default()
        };
        let mut mask: c_int = 0;
        let ret = unsafe { ffi::rdma_init_qp_attr(self.inner.id, &mut attr, &mut mask) };
        check(ret, Error::ModifyQueuePair)?;
        let mask = M(mask as u32);
        if mask.0 & (M::IBV_QP_MAX_DEST_RD_ATOMIC.0 | M::IBV_QP_MAX_QP_RD_ATOMIC.0) != 0 {
            let (max_initiator_depth, max_responder_resources) =
                crate::context::rd_atomic_limits(self.verbs()?)?;
            attr.max_dest_rd_atomic = attr.max_dest_rd_atomic.min(max_responder_resources);
            attr.max_rd_atomic = attr.max_rd_atomic.min(max_initiator_depth);
        }
        Ok(QueuePairAttribute::from_raw(attr, mask))
    }

    /// Blocks until the next event arrives (up to `deadline`, indefinitely with `None`) and returns
    /// it, or [`TimedOut`](Error::TimedOut) once the deadline passes. The channel's file descriptor
    /// is `poll(2)`ed with the remaining time and the event then read without blocking, so the wait
    /// never runs past the deadline.
    fn get_cm_event_deadline(&self, deadline: Option<Instant>) -> Result<CmEvent> {
        loop {
            if !crate::completion::wait_readable(self.as_fd(), deadline)
                .map_err(Error::ConnectionSetup)?
            {
                return Err(Error::TimedOut);
            }
            // The descriptor was readable, but another thread may have taken the event first, or
            // librdmacm consumed it internally; if so, go back to waiting for the next one with
            // whatever time is left.
            if let Some(event) = self.poll_cm_event()? {
                return Ok(event);
            }
        }
    }

    /// Blocks until an `expected` event arrives (up to `deadline`) and returns it, acknowledging
    /// and skipping any others, and returning an error on a failure event or on an expired
    /// deadline. Drives the blocking setup helpers.
    fn wait_for(&self, expected: CmEventType, deadline: Option<Instant>) -> Result<CmEvent> {
        loop {
            // This blocks on the channel's fd until an event arrives — it does not spin. The loop
            // only goes around to skip a non-matching event, re-blocking on the next read. Each
            // skipped event is acknowledged when it drops at the iteration end.
            let event = self.get_cm_event_deadline(deadline)?;
            let kind = event.event_type();
            if kind == expected {
                return Ok(event);
            }
            if is_failure(kind) {
                return Err(event.into_error());
            }
        }
    }

    /// Binds to a local `addr` (passive side). Bind to an unspecified address such as
    /// `0.0.0.0:port` to accept connections on any device. Follow with [`listen`](Self::listen).
    pub fn bind_addr(&self, addr: SocketAddr) -> Result<()> {
        let addr = SockaddrStorage::from(addr);
        let ret =
            unsafe { ffi::rdma_bind_addr(self.inner.id, addr.as_ptr() as *mut ffi::sockaddr) };
        check(ret, Error::BindAddress)
    }

    /// Starts listening for incoming connection requests (passive side). A
    /// [`CmEventType::ConnectRequest`] is then delivered for each incoming connection; take its
    /// new id with [`CmEvent::connection_request`].
    ///
    /// `backlog` caps how many requests can wait to be read from the channel; requests beyond it
    /// are dropped. 0 does not mean no queue: like any value above the kernel's limit
    /// (`net.rdma_ucm.max_backlog`, 1024 by default), it means that limit.
    pub fn listen(&self, backlog: u32) -> Result<()> {
        let ret = unsafe { ffi::rdma_listen(self.inner.id, backlog.min(i32::MAX as u32) as i32) };
        check(ret, Error::ConnectionSetup)
    }

    /// Resolves the destination address to an RDMA device and local route (active side). On success
    /// a [`CmEventType::AddressResolved`] event is delivered, after which
    /// [`context`](Self::context) is available and [`resolve_route`](Self::resolve_route) is the
    /// next step.
    pub fn resolve_addr(&self, dst: SocketAddr, timeout: Duration) -> Result<()> {
        let dst = SockaddrStorage::from(dst);
        let ret = unsafe {
            ffi::rdma_resolve_addr(
                self.inner.id,
                ptr::null_mut(),
                dst.as_ptr() as *mut ffi::sockaddr,
                timeout_ms(timeout),
            )
        };
        check(ret, Error::ResolveAddress)
    }

    /// Resolves the route to the destination (active side), after the address has resolved. On
    /// success a [`CmEventType::RouteResolved`] event is delivered, after which the queue pair can
    /// be built and [`connect`](Self::connect) called.
    ///
    /// # Errors
    ///
    ///  - [`ResolveRoute`](Error::ResolveRoute): `rdma_resolve_route` failed, or the address has
    ///    not resolved yet (the [`CmEventType::AddressResolved`] event has not been read).
    pub fn resolve_route(&self, timeout: Duration) -> Result<()> {
        self.device_or(Error::ResolveRoute)?;
        let ret = unsafe { ffi::rdma_resolve_route(self.inner.id, timeout_ms(timeout)) };
        check(ret, Error::ResolveRoute)
    }

    /// Initiates a connection to the remote (active side). On success a
    /// [`CmEventType::ConnectResponse`] (external queue pair) or [`CmEventType::Established`]
    /// event is delivered. `param` carries the local queue pair number; set it with
    /// [`ConnectionParameter::set_qp_num`] to the number of the queue pair you built. After the
    /// response, apply the `Init` attributes again (see [`init_qp_attr`](Self::init_qp_attr)),
    /// move the queue pair to `RTR`/`RTS`, and call [`establish`](Self::establish).
    ///
    /// # Errors
    ///
    ///  - [`Connect`](Error::Connect): `rdma_connect` failed, the private data exceeds what a
    ///    request can carry in this id's port space (56 bytes for [`PortSpace::Tcp`], 92 for
    ///    [`PortSpace::Ib`], 180 for the datagram port spaces), or the route has not resolved
    ///    yet.
    pub fn connect(&self, param: &ConnectionParameter) -> Result<()> {
        self.check_connect_private_data(param)?;
        self.device_or(Error::Connect)?;
        // The raw struct's private-data pointer aims into `param`'s inline storage, which the
        // borrow keeps alive across the FFI call.
        let mut raw = param.as_raw();
        let ret = unsafe { ffi::rdma_connect(self.inner.id, &mut raw) };
        check(ret, Error::Connect)
    }

    /// Accepts a connection request (passive side), in response to a
    /// [`CmEventType::ConnectRequest`]. Build and move the queue pair to `RTS` first; `param`
    /// carries its number, set with [`ConnectionParameter::set_qp_num`]. On success a
    /// [`CmEventType::Established`] event is delivered.
    ///
    /// # Errors
    ///
    ///  - [`Accept`](Error::Accept): `rdma_accept` failed, the private data exceeds what a reply
    ///    can carry in this id's port space (196 bytes for [`PortSpace::Tcp`] and
    ///    [`PortSpace::Ib`], 136 for the datagram port spaces), or the id has no device (a
    ///    connection request's id always has one).
    pub fn accept(&self, param: &ConnectionParameter) -> Result<()> {
        check_private_data(
            param.param.private_data_len.into(),
            max_accept_private_data(self.port_space()),
            "connection reply private data in this port space",
        )
        .map_err(Error::Accept)?;
        self.device_or(Error::Accept)?;
        // The raw struct's private-data pointer aims into `param`'s inline storage, which the
        // borrow keeps alive across the FFI call.
        let mut raw = param.as_raw();
        let ret = unsafe { ffi::rdma_accept(self.inner.id, &mut raw) };
        check(ret, Error::Accept)
    }

    /// Declines a connection request (passive side), on the id taken from a
    /// [`CmEventType::ConnectRequest`] with [`CmEvent::connection_request`]. The peer's connect
    /// fails with [`CmEventType::Rejected`] ([`Unreachable`](CmEventType::Unreachable) in the
    /// datagram port spaces), carrying `private_data` (at most 148 bytes, 136 in the datagram port
    /// spaces) for it to read. Drop the id afterwards; it has no further use.
    ///
    /// # Errors
    ///
    ///  - [`ConnectionSetup`](Error::ConnectionSetup): `rdma_reject` failed, or `private_data`
    ///    is longer than a rejection can carry.
    pub fn reject(&self, private_data: &[u8]) -> Result<()> {
        check_private_data(
            private_data.len(),
            max_reject_private_data(self.port_space()),
            "rejection private data in this port space",
        )
        .map_err(Error::ConnectionSetup)?;
        let ret = unsafe {
            ffi::rdma_reject(
                self.inner.id,
                private_data.as_ptr().cast::<c_void>(),
                private_data.len() as u8,
            )
        };
        check(ret, Error::ConnectionSetup)
    }

    /// Completes connection establishment on the active side after the queue pair has reached
    /// `RTS`, in response to a [`CmEventType::ConnectResponse`].
    pub fn establish(&self) -> Result<()> {
        let ret = unsafe { ffi::rdma_establish(self.inner.id) };
        check(ret, Error::Connect)
    }

    /// Disconnects an established connection, delivering [`CmEventType::Disconnected`] to both
    /// sides.
    ///
    /// The queue pair is not touched: it is not attached to the id, so `rdma_disconnect` does not
    /// move it to the error state the way it would a connection-manager-created one. Do that
    /// yourself with [`QueuePair::modify`] so outstanding work requests are flushed (the blocking
    /// [`Connection::disconnect`] does).
    ///
    /// # Errors
    ///
    ///  - [`ConnectionSetup`](Error::ConnectionSetup): `rdma_disconnect` failed. On InfiniBand
    ///    and RoCE that happens only on an id that never took part in a connection, or whose
    ///    device was removed (`ENXIO`): disconnecting one that is already down succeeds.
    pub fn disconnect(&self) -> Result<()> {
        let ret = unsafe { ffi::rdma_disconnect(self.inner.id) };
        check(ret, Error::ConnectionSetup)
    }

    /// Set an `RDMA_OPTION_ID`-level option (`rdma_set_option`) to `value`.
    fn set_id_option<T>(&self, option: c_int, mut value: T) -> Result<()> {
        let ret = unsafe {
            ffi::rdma_set_option(
                self.inner.id,
                ffi::RDMA_OPTION_ID.0 as c_int,
                option,
                (&mut value as *mut T).cast::<c_void>(),
                std::mem::size_of::<T>(),
            )
        };
        if ret != 0 {
            let err = io::Error::last_os_error();
            // The kernel answers an option it does not know with `ENOSYS`.
            if err.raw_os_error() == Some(nix::libc::ENOSYS) {
                return Err(Error::Unsupported {
                    operation: &"rdma_set_option",
                });
            }
            return Err(Error::os(err, Error::ConnectionSetup));
        }
        Ok(())
    }

    /// Set the type of service of this id's traffic: the IP DSCP/ToS byte (RFC 2474) its packets
    /// carry, which the network maps to a priority or a lossless class. Set it before resolving
    /// the address (active side) or binding (passive side).
    ///
    /// On InfiniBand and RoCE a listener's setting has no effect, though: the connections it
    /// accepts take their traffic class and service level from the peer's request, so set it on
    /// the connecting side.
    ///
    /// # Errors
    ///
    ///  - [`ConnectionSetup`](Error::ConnectionSetup): `rdma_set_option` failed.
    ///  - [`Unsupported`](Error::Unsupported): the kernel does not know the option (`ENOSYS`).
    pub fn set_tos(&self, tos: u8) -> Result<()> {
        self.set_id_option(ffi::RDMA_OPTION_ID_TOS.0 as c_int, tos)
    }

    /// Allow the local address to be shared, like `SO_REUSEADDR`: another id with the same
    /// setting may bind the same address and port. Set it before binding.
    ///
    /// # Errors
    ///
    ///  - [`ConnectionSetup`](Error::ConnectionSetup): `rdma_set_option` failed.
    ///  - [`Unsupported`](Error::Unsupported): the kernel does not know the option (`ENOSYS`).
    pub fn set_reuse_addr(&self, reuse: bool) -> Result<()> {
        self.set_id_option(ffi::RDMA_OPTION_ID_REUSEADDR.0 as c_int, c_int::from(reuse))
    }

    /// Restrict an id bound to a wildcard address to requests of its own address family, like
    /// `IPV6_V6ONLY`. With it off, a wildcard listener takes IPv4 and IPv6 requests alike, and an
    /// IPv4 peer's id carries a plain IPv4 address rather than an IPv4-mapped one. It is on by
    /// default for IPv4 binds and follows the `net.ipv6.bindv6only` sysctl for IPv6 binds; an
    /// IPv6 wildcard listener needs it on to share its port with an IPv4 one. An id bound to a
    /// specific address takes only requests for that address either way. Set it before binding.
    ///
    /// # Errors
    ///
    ///  - [`ConnectionSetup`](Error::ConnectionSetup): `rdma_set_option` failed.
    ///  - [`Unsupported`](Error::Unsupported): the kernel does not know the option (`ENOSYS`).
    pub fn set_af_only(&self, only: bool) -> Result<()> {
        self.set_id_option(ffi::RDMA_OPTION_ID_AFONLY.0 as c_int, c_int::from(only))
    }

    /// Set the ACK timeout of the connection's queue pair, overriding the one the connection
    /// manager derives from the route. Set it before connecting or accepting.
    ///
    /// # Errors
    ///
    ///  - [`ConnectionSetup`](Error::ConnectionSetup): `rdma_set_option` failed.
    ///  - [`Unsupported`](Error::Unsupported): the kernel does not know the option (`ENOSYS`).
    pub fn set_ack_timeout(&self, timeout: AckTimeout) -> Result<()> {
        self.set_id_option(
            ffi::RDMA_OPTION_ID_ACK_TIMEOUT.0 as c_int,
            timeout.exponent(),
        )
    }

    /// Tell the connection manager that the queue pair saw the peer's first message arrive
    /// (`rdma_notify` with `IBV_EVENT_COMM_EST`).
    ///
    /// With an external queue pair, the device reports that arrival as an `IBV_EVENT_COMM_EST`
    /// asynchronous event on the queue pair ([`Context::wait_async_event`]), which the connection
    /// manager cannot see on its own; forwarding it here lets the passive side treat the
    /// connection as established when the peer's ready-to-use message was lost.
    ///
    /// The ids on one device share one [`context`](Self::context), so its asynchronous events are
    /// those of every connection's queue pair, and each goes to whichever reader takes it first:
    /// one waiting loop per connection would take the others' events. Read them in one place per
    /// context instead, and hand each event to the connection whose queue pair it names
    /// ([`AsyncEvent::as_raw`](crate::AsyncEvent::as_raw)).
    ///
    /// # Errors
    ///
    ///  - [`ConnectionSetup`](Error::ConnectionSetup): `rdma_notify` failed (for example
    ///    `EISCONN` when the connection is already established).
    pub fn notify_established(&self) -> Result<()> {
        let ret =
            unsafe { ffi::rdma_notify(self.inner.id, ffi::ibv_event_type::IBV_EVENT_COMM_EST) };
        check(ret, Error::ConnectionSetup)
    }

    /// The IP address and port of the remote end of this id, or `None` while the destination has
    /// not been resolved yet (or its address family is neither IPv4 nor IPv6).
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        // SAFETY: the id is valid, and its address storage is sockaddr_storage-sized.
        unsafe { socket_addr_of(ffi::rdma_get_peer_addr(self.inner.id)) }
    }

    /// The port of [`context`](Self::context)'s device that the connection manager bound this id
    /// to (ports are numbered from 1), or `None` while it has no device. Build the queue pair on
    /// this port: [`ProtectionDomain::create_qp`](crate::ProtectionDomain::create_qp) refuses a
    /// port that is not active, and on a multi-port device the connection manager may have picked
    /// any of them.
    pub fn port_num(&self) -> Option<u8> {
        // librdmacm sets the port together with the device: when the address resolves, the id
        // binds to a device's address, or a request arrives.
        let port = unsafe { (*self.inner.id).port_num };
        (port != 0).then_some(port)
    }

    /// The local IP address and port this id is bound to, or `None` while it has not been bound
    /// or resolved yet (or its address family is neither IPv4 nor IPv6).
    pub fn local_addr(&self) -> Option<SocketAddr> {
        // SAFETY: the id is valid, and its address storage is sockaddr_storage-sized.
        unsafe { socket_addr_of(ffi::rdma_get_local_addr(self.inner.id)) }
    }

    /// Returns the underlying `rdma_cm_id` pointer.
    ///
    /// This is an escape hatch for librdmacm calls this crate does not yet wrap (for example
    /// `rdma_set_option`). The pointer is owned by this [`CmId`] and stays
    /// valid only while a clone of it is alive; do not destroy it or use it past the id's lifetime.
    pub fn as_raw(&self) -> *mut ffi::rdma_cm_id {
        self.inner.id
    }

    /// The port space this id was created in.
    fn port_space(&self) -> ffi::rdma_port_space {
        unsafe { (*self.inner.id).ps }
    }

    /// Checks that `param`'s private data fits a connection request in this id's port space.
    fn check_connect_private_data(&self, param: &ConnectionParameter) -> Result<()> {
        check_private_data(
            param.param.private_data_len.into(),
            max_connect_private_data(self.port_space()),
            "connection request private data in this port space",
        )
        .map_err(Error::Connect)
    }

    /// The device context the connection manager bound this id to (its `verbs`). Only available once
    /// the address has resolved.
    fn verbs(&self) -> Result<*mut ffi::ibv_context> {
        self.device_or(Error::ConnectionSetup)
    }

    /// The device context the connection manager bound this id to, or the error `wrap` makes of its
    /// absence. Besides [`verbs`](Self::verbs), this guards the calls librdmacm makes assuming a
    /// device: `rdma_resolve_route`, `rdma_connect`, and `rdma_accept` dereference the id's device
    /// without checking for one, so on an id whose address has not resolved (or that is bound to a
    /// wildcard address) they would crash.
    fn device_or(&self, wrap: fn(io::Error) -> Error) -> Result<*mut ffi::ibv_context> {
        let verbs = unsafe { (*self.inner.id).verbs };
        if verbs.is_null() {
            return Err(wrap(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the connection manager has not bound this id to a device yet (its address has \
                 not resolved, or it is bound to a wildcard address)",
            )));
        }
        Ok(verbs)
    }

    /// Builds the queue pair from `prepared` and moves it from `RESET` to `INIT`. Finish the
    /// transition after the connection is set up with [`ready`](Self::ready).
    ///
    /// The queue pair must be on this id's device: the attributes the connection manager computes
    /// (the port, the route) are that device's, and could misconfigure another device's queue
    /// pair without failing.
    fn init_qp(&self, prepared: PreparedQueuePair<Rc>) -> Result<QueuePair<Rc>> {
        let mut qp = prepared.into_queue_pair();
        let device = unsafe { (*self.verbs()?).device };
        if unsafe { (*(*qp.as_raw()).context).device } != device {
            return Err(Error::ModifyQueuePair(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the queue pair must come from the connection's device (the one `context` returns)",
            )));
        }
        self.transition(&mut qp, QueuePairState::Init)?;
        Ok(qp)
    }

    /// Moves `qp` from `INIT` through `RTR` to `RTS`, completing the connection-manager transition.
    ///
    /// Like librdmacm, `responder_resources` and `initiator_depth` override the outstanding-RDMA
    /// limits the connection manager computed (`max_dest_rd_atomic` at `RTR`, `max_rd_atomic` at
    /// `RTS`; already capped to this device's by [`init_qp_attr`](Self::init_qp_attr)), so the
    /// queue pair is configured with the values this side advertises in its reply.
    fn ready(
        &self,
        qp: &mut QueuePair<Rc>,
        responder_resources: Option<u8>,
        initiator_depth: Option<u8>,
    ) -> Result<()> {
        let mut rtr = self.init_qp_attr(QueuePairState::ReadyToReceive)?;
        if let Some(responder_resources) = responder_resources {
            rtr.set_max_dest_rd_atomic(responder_resources);
        }
        qp.modify(&rtr)?;
        let mut rts = self.init_qp_attr(QueuePairState::ReadyToSend)?;
        if let Some(initiator_depth) = initiator_depth {
            rts.set_max_rd_atomic(initiator_depth);
        }
        qp.modify(&rts)
    }

    /// Transitions `qp` to `state` using the attributes the connection manager computes from the
    /// resolved route and negotiated parameters ([`init_qp_attr`](Self::init_qp_attr)), applied with
    /// [`QueuePair::modify`].
    fn transition(&self, qp: &mut QueuePair<Rc>, state: QueuePairState) -> Result<()> {
        qp.modify(&self.init_qp_attr(state)?)
    }
}

impl AsRawFd for CmId {
    /// The raw file descriptor of this id's event channel, for a reactor to drive connection setup
    /// without blocking. It is non-blocking and becomes readable when a connection-manager event
    /// is pending, to be consumed with [`poll_cm_event`](CmId::poll_cm_event).
    fn as_raw_fd(&self) -> RawFd {
        unsafe { (*self.inner.channel.chan).fd }
    }
}

impl AsFd for CmId {
    fn as_fd(&self) -> BorrowedFd<'_> {
        // SAFETY: the channel fd lives as long as this `CmId` (its inner `channel` field), and the
        // borrow is tied to `&self`.
        unsafe { BorrowedFd::borrow_raw((*self.inner.channel.chan).fd) }
    }
}

/// A connection-manager event, retrieved with [`CmId::get_cm_event`]/[`CmId::poll_cm_event`] and
/// acknowledged automatically when dropped.
///
/// The event keeps the id whose channel delivered it alive: `rdma_destroy_id` blocks until every
/// event delivered for an id has been acknowledged, so the id cannot go away underneath an
/// outstanding event. A [`ConnectRequest`](CmEventType::ConnectRequest) whose new id is never
/// taken with [`connection_request`](Self::connection_request) is rejected on drop, so the peer
/// learns right away rather than after its retries time out.
pub struct CmEvent {
    event: *mut ffi::rdma_cm_event,
    /// The event's status, with librdmacm's own failures turned into a negative errno (see
    /// [`CmId::poll_cm_event`]).
    status: i32,
    /// The id whose channel delivered the event (the listener, for a connection request).
    _id: CmId,
    /// Whether a connection request's new id has been taken over by
    /// [`connection_request`](Self::connection_request).
    taken: bool,
}

// The event (and the id it holds, which is `Send + Sync` itself) can move between threads:
// librdmacm does not care which thread acknowledges an event.
unsafe impl Send for CmEvent {}

/// A failure event the connection manager reported while a blocking helper waited for the next
/// setup step, carried by [`Error::ConnectionManager`].
#[derive(Debug)]
#[non_exhaustive]
pub struct CmFailure {
    /// The failure event.
    pub event: CmEventType,
    /// The event's status: a negative `errno` for address, route, and connection errors, or the
    /// transport's reject reason for [`Rejected`](CmEventType::Rejected). On InfiniBand and RoCE
    /// the reasons met in practice are 28 ("consumer defined") when the peer declined the request
    /// itself, 8 ("invalid service ID") when nothing listens on the destination port, 4
    /// ("timeout") when the peer gave up on its own request (seen while accepting), and 10
    /// ("stale connection") when the peer still holds an earlier connection with this queue pair,
    /// for example in timewait.
    pub status: i32,
    /// The private data the peer attached to its rejection, padded by the transport to its wire
    /// format; empty when there is none.
    pub private_data: Vec<u8>,
}

impl CmEvent {
    /// The failure this event reports, for the blocking helpers to return.
    fn into_error(self) -> Error {
        Error::ConnectionManager(Box::new(CmFailure {
            event: self.event_type(),
            status: self.status(),
            private_data: self.private_data().map_or_else(Vec::new, <[u8]>::to_vec),
        }))
    }

    /// The kind of event. Match on the [`CmEventType`] to decide what to do next; see [`CmId`] for
    /// the expected sequence.
    pub fn event_type(&self) -> CmEventType {
        unsafe { (*self.event).event }.into()
    }

    /// The event's status: `0` on success, otherwise a negative errno (for address, route, and
    /// connection errors) or a transport-specific value: the reject reason for
    /// [`Rejected`](CmEventType::Rejected) (see [`CmFailure::status`] for the common ones), and
    /// for [`Unreachable`](CmEventType::Unreachable) in the datagram port spaces the status of the
    /// peer's service-ID-resolution reply. The blocking helpers carry it in
    /// [`Error::ConnectionManager`].
    ///
    /// librdmacm reports its own failures to finish an address or route resolution as a raw
    /// status of -1 with the reason in `errno`; this returns that reason, negated, instead.
    pub fn status(&self) -> i32 {
        self.status
    }

    /// The private data the peer attached to its connection request or reply
    /// ([`ConnectionParameter::set_private_data`]), or `None` if the event carries none.
    ///
    /// The transport pads (and can truncate) the payload to its wire format, so the length here
    /// is the transport's reported length, not the exact number of bytes the sender set. It is
    /// typically longer than the sender's write, with the tail zero-filled.
    pub fn private_data(&self) -> Option<&[u8]> {
        // The `conn` and `ud` union members lead with the same private_data/private_data_len
        // prefix, so reading `conn` is valid for connected and datagram events alike.
        let conn = unsafe { (*self.event).param.conn };
        if conn.private_data.is_null() || conn.private_data_len == 0 {
            return None;
        }
        // SAFETY: librdmacm hands out `private_data_len` readable bytes, owned by the event,
        // which lives until this `CmEvent` drops (the borrow of `self` ties the slice to it).
        Some(unsafe {
            std::slice::from_raw_parts(conn.private_data as *const u8, conn.private_data_len.into())
        })
    }

    /// Returns the underlying `rdma_cm_event` pointer.
    ///
    /// This is an escape hatch for event fields this crate does not yet expose (for example the
    /// negotiated `conn` parameters). The pointer is owned by this
    /// [`CmEvent`] and stays valid only until it drops (which acknowledges the event); do not
    /// acknowledge it yourself.
    pub fn as_raw(&self) -> *mut ffi::rdma_cm_event {
        self.event
    }

    /// Consumes a [`CmEventType::ConnectRequest`] event, taking the new connection id it carries
    /// and migrating it onto its own fresh event channel, so its later events are isolated rather
    /// than colliding with the listener's. Only valid on that event type. The event is acknowledged
    /// on return.
    ///
    /// The returned id is the passive side of the new connection: build a queue pair on its
    /// [`context`](CmId::context) and [`port_num`](CmId::port_num), move it to `RTS`, and
    /// [`accept`](CmId::accept) — or [`reject`](CmId::reject) it.
    ///
    /// # Errors
    ///
    ///  - [`ConnectionSetup`](Error::ConnectionSetup): this is not a connection request, or its
    ///    id could not be moved onto a new event channel (the request is then rejected).
    pub fn connection_request(mut self) -> Result<CmId> {
        if self.event_type() != CmEventType::ConnectRequest {
            return Err(Error::ConnectionSetup(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("not a connection request: {}", self.event_type()),
            )));
        }
        let channel = EventChannel::new()?;
        let id = unsafe { (*self.event).id };
        // From here on the id is ours to release: `Drop` must no longer reject and destroy it.
        self.taken = true;
        let ret = unsafe { ffi::rdma_migrate_id(id, channel.chan) };
        if ret != 0 {
            let err = io::Error::last_os_error();
            // The request id is ours to destroy once we abandon it; `channel` drops after.
            unsafe { ffi::rdma_destroy_id(id) };
            return Err(Error::os(err, Error::ConnectionSetup));
        }
        Ok(CmId {
            inner: Arc::new(CmIdInner { channel, id }),
        })
    }
}

impl Drop for CmEvent {
    fn drop(&mut self) {
        unsafe {
            if !self.taken
                && (*self.event).event == ffi::rdma_cm_event_type::RDMA_CM_EVENT_CONNECT_REQUEST
            {
                // Nobody took the new connection's id: decline the request and free the id
                // librdmacm allocated for it, which only `rdma_destroy_id` releases (the event is
                // charged to the listener, so destroying the new id does not wait on it).
                let id = (*self.event).id;
                ffi::rdma_reject(id, ptr::null(), 0);
                ffi::rdma_ack_cm_event(self.event);
                ffi::rdma_destroy_id(id);
            } else {
                ffi::rdma_ack_cm_event(self.event);
            }
        }
    }
}

/// The most private-data bytes any connection-manager call accepts (a reply's, see
/// [`max_accept_private_data`]); [`CmId::connect`] and [`CmId::accept`] enforce the tighter
/// per-call limits.
const MAX_PRIVATE_DATA: usize = 196;

/// Parameters for connecting and accepting.
///
/// [`Default`] gives sane reliable-connection defaults, and the setters consume and return the
/// value, so a parameter is built up in one expression:
/// `ConnectionParameter::default().set_qp_num(qpn).set_private_data(b"hello")`. The local queue
/// pair number is set automatically by [`Resolved::connect`] / [`Incoming::accept`].
#[derive(Clone, Copy)]
pub struct ConnectionParameter {
    param: ffi::rdma_conn_param,
    /// The bytes `param.private_data` describes, stored inline (rather than borrowing the
    /// caller's slice) so the value is self-contained. The pointer itself is aimed here by
    /// [`as_raw`](Self::as_raw) at the connect/accept call sites, not stored.
    private_data: [u8; MAX_PRIVATE_DATA],
}

impl Default for ConnectionParameter {
    /// Reliable-connection defaults: as many outstanding RDMA reads and atomics in each direction
    /// as both devices support (librdmacm's `RDMA_MAX_RESP_RES` / `RDMA_MAX_INIT_DEPTH`), the
    /// maximum retry counts, and no private data.
    fn default() -> Self {
        ConnectionParameter {
            param: ffi::rdma_conn_param {
                responder_resources: RDMA_MAX_RESP_RES,
                initiator_depth: RDMA_MAX_INIT_DEPTH,
                retry_count: 7,
                rnr_retry_count: 7,
                ..Default::default()
            },
            private_data: [0; MAX_PRIVATE_DATA],
        }
    }
}

impl ConnectionParameter {
    /// Sets the number of outstanding RDMA read/atomic operations the local side can service as a
    /// responder. Defaults to the device's limit, negotiated down to what the peer issues.
    pub fn set_responder_resources(mut self, responder_resources: u8) -> Self {
        self.param.responder_resources = responder_resources;
        self
    }

    /// Sets the number of outstanding RDMA read/atomic operations the local side can issue as an
    /// initiator. Defaults to the device's limit, negotiated down to what the peer services.
    ///
    /// When accepting, stay at or below what the request offered, the peer's responder resources
    /// (`rdma_accept(3)`); the default does. Nothing checks this: a peer that cannot raise its
    /// responder resources to match fails the connection, or answers the excess reads with an
    /// invalid-request error.
    pub fn set_initiator_depth(mut self, initiator_depth: u8) -> Self {
        self.param.initiator_depth = initiator_depth;
        self
    }

    /// Sets how many times to retry a connection or transport operation before reporting an error
    /// (0-7).
    ///
    /// Only the connecting side's value counts: accepting ignores it (`rdma_accept(3)`), and the
    /// accepting queue pair retries as often as the connection request asked.
    pub fn set_retry_count(mut self, retry_count: u8) -> Self {
        self.param.retry_count = retry_count;
        self
    }

    /// Sets how many times to retry sending after a receiver-not-ready error (0-7).
    pub fn set_rnr_retry_count(mut self, rnr_retry_count: u8) -> Self {
        self.param.rnr_retry_count = rnr_retry_count;
        self
    }

    /// Sets the local queue pair number the peer should target — the number of the queue pair you
    /// built and are connecting or accepting with. The blocking [`Resolved::connect`] /
    /// [`Incoming::accept`] helpers set this for you; set it yourself when driving [`CmId::connect`]
    /// or [`CmId::accept`] directly.
    pub fn set_qp_num(mut self, qp_num: u32) -> Self {
        self.param.qp_num = qp_num;
        self
    }

    /// Sets the application payload carried inside the connection request or reply, for the peer
    /// to read with [`Incoming::peer_private_data`] / [`Connection::peer_private_data`] (or
    /// [`CmEvent::private_data`]) — typically a protocol version, a token, or bootstrap parameters
    /// that save a round trip.
    ///
    /// How much fits depends on the call and port space: a request ([`Resolved::connect`] /
    /// [`CmId::connect`]) carries at most 56 bytes in [`PortSpace::Tcp`] (92 in
    /// [`PortSpace::Ib`], 180 in the datagram port spaces), a reply ([`Incoming::accept`] /
    /// [`CmId::accept`]) up to 196 (136 in the datagram port spaces); those calls fail with an
    /// error when the data does not fit. The bytes are copied into the parameter.
    ///
    /// # Panics
    ///
    /// Panics if `data` is longer than 196 bytes, more than any call accepts.
    pub fn set_private_data(mut self, data: &[u8]) -> Self {
        assert!(
            data.len() <= MAX_PRIVATE_DATA,
            "connection private data is limited to {MAX_PRIVATE_DATA} bytes, got {}",
            data.len()
        );
        self.private_data[..data.len()].copy_from_slice(data);
        self.param.private_data_len = data.len() as u8;
        self
    }

    /// The raw `rdma_conn_param`, with its private-data pointer aimed at this value's inline
    /// storage. The pointer borrows `self`: the caller must keep `self` alive across the FFI
    /// call the result is passed to.
    fn as_raw(&self) -> ffi::rdma_conn_param {
        let mut raw = self.param;
        raw.private_data = if self.param.private_data_len == 0 {
            ptr::null()
        } else {
            self.private_data.as_ptr() as *const c_void
        };
        raw
    }
}

/// Active-side blocking connection setup. Created by [`Connector::new`]; drives address and route
/// resolution, then yields a [`Resolved`] from which you build a queue pair and connect.
#[must_use]
pub struct Connector {
    id: CmId,
}

impl Connector {
    /// Creates a connector with its own event channel.
    ///
    /// # Errors
    ///
    ///  - [`ConnectionSetup`](Error::ConnectionSetup): creating the id failed, or `port_space`
    ///    is a datagram port space ([`Udp`](PortSpace::Udp) / [`Ipoib`](PortSpace::Ipoib)): the
    ///    blocking helpers set up reliable connections only.
    pub fn new(port_space: PortSpace) -> Result<Self> {
        require_connected_port_space(port_space, "Connector")?;
        Ok(Connector {
            id: CmId::create(port_space)?,
        })
    }

    /// The underlying id, for options that must be set before resolving ([`CmId::set_tos`],
    /// [`CmId::set_ack_timeout`]).
    pub fn cm_id(&self) -> &CmId {
        &self.id
    }

    /// Resolves the destination address and route (blocking until both complete), then returns a
    /// handle to build the queue pair on the resolved device. `timeout` bounds each of the two
    /// resolution steps (it is enforced by the kernel, which reports expiry as a failure event).
    pub fn resolve(self, dst: SocketAddr, timeout: Duration) -> Result<Resolved> {
        self.id.resolve_addr(dst, timeout)?;
        // The kernel delivers AddressError/RouteError once `timeout` expires, so these waits
        // need no deadline of their own.
        self.id.wait_for(CmEventType::AddressResolved, None)?;
        self.id.resolve_route(timeout)?;
        self.id.wait_for(CmEventType::RouteResolved, None)?;
        Ok(Resolved { id: self.id })
    }
}

/// A resolved active connection, ready for its queue pair to be built and connected. Returned by
/// [`Connector::resolve`].
#[must_use = "a resolved connection is abandoned when dropped; finish it with `connect`"]
pub struct Resolved {
    id: CmId,
}

impl Resolved {
    /// The device the connection manager resolved to. Build the queue pair (and its protection
    /// domain and completion queue) on this context, on the port [`CmId::port_num`] reports, then
    /// pass it to [`connect`](Self::connect).
    pub fn context(&self) -> Result<Context> {
        self.id.context()
    }

    /// The underlying id: the resolved route's addresses, and the options that must be set before
    /// connecting ([`CmId::set_ack_timeout`]).
    pub fn cm_id(&self) -> &CmId {
        &self.id
    }

    /// Connects to the remote (blocking) using `qp`, returning the established [`Connection`]. The
    /// queue pair number in `param` is set automatically.
    ///
    /// `timeout` bounds how long to wait for the remote's response: on expiry
    /// [`TimedOut`](Error::TimedOut) is returned, and `None` waits indefinitely.
    ///
    /// # Errors
    ///
    ///  - [`ConnectionManager`](Error::ConnectionManager): the peer rejected the request (with
    ///    its reject reason and private data), or was unreachable.
    ///  - [`Connect`](Error::Connect): `rdma_connect` failed, or the private data does not fit a
    ///    request (see [`ConnectionParameter::set_private_data`]), which is checked before `qp`
    ///    is touched.
    ///  - [`ModifyQueuePair`](Error::ModifyQueuePair): a queue-pair transition failed, or `qp`
    ///    is not on the device of [`context`](Self::context).
    ///  - [`TimedOut`](Error::TimedOut): the response did not arrive in time.
    ///
    /// Any error, a refused `param` included, consumes `self` and `qp`: resolve again to retry.
    pub fn connect(
        self,
        qp: PreparedQueuePair<Rc>,
        param: ConnectionParameter,
        timeout: Option<Duration>,
    ) -> Result<Connection> {
        let deadline = crate::completion::deadline_after(timeout);
        self.id.check_connect_private_data(&param)?;
        let mut qp = self.id.init_qp(qp)?;
        let param = param.set_qp_num(qp.qp_num());
        self.id.connect(&param)?;
        let response = self.id.wait_for(CmEventType::ConnectResponse, deadline)?;
        let private_data = response
            .private_data()
            .map_or_else(Vec::new, <[u8]>::to_vec);
        drop(response);
        // The `INIT` attributes applied before connecting carried no remote-access flags (there
        // was no connection to derive them from); now there is one, so apply `INIT` again — as
        // librdmacm does — before moving on. The RDMA limits come from the negotiated reply.
        self.id.transition(&mut qp, QueuePairState::Init)?;
        self.id.ready(&mut qp, None, None)?;
        self.id.establish()?;
        Ok(Connection {
            id: self.id,
            qp,
            private_data,
        })
    }
}

/// Passive-side blocking connection setup. Created by [`Acceptor::bind`]; listens for and accepts
/// incoming connections.
#[must_use]
pub struct Acceptor {
    listener: CmId,
}

impl Acceptor {
    /// Binds to `addr` (use an unspecified address such as `0.0.0.0:port` for any device, and
    /// port 0 for an ephemeral port, read back with [`local_addr`](Self::local_addr)) and starts
    /// listening, queueing up to `backlog` requests not yet taken by [`accept`](Self::accept) (0
    /// means the kernel's limit, see [`CmId::listen`]).
    ///
    /// # Errors
    ///
    ///  - [`ConnectionSetup`](Error::ConnectionSetup): creating the id or listening failed, or
    ///    `port_space` is a datagram port space ([`Udp`](PortSpace::Udp) /
    ///    [`Ipoib`](PortSpace::Ipoib)): the blocking helpers set up reliable connections only.
    ///  - [`BindAddress`](Error::BindAddress): `rdma_bind_addr` failed, for example because no
    ///    RDMA device answers to `addr`.
    pub fn bind(addr: SocketAddr, port_space: PortSpace, backlog: u32) -> Result<Self> {
        Self::bind_with(addr, port_space, backlog, |_| Ok(()))
    }

    /// As [`bind`](Self::bind), running `configure` on the listener's id before it binds: the
    /// place for the options that must precede binding ([`CmId::set_reuse_addr`],
    /// [`CmId::set_af_only`]).
    ///
    /// # Errors
    ///
    /// Those of [`bind`](Self::bind), plus whatever `configure` returns.
    pub fn bind_with(
        addr: SocketAddr,
        port_space: PortSpace,
        backlog: u32,
        configure: impl FnOnce(&CmId) -> Result<()>,
    ) -> Result<Self> {
        require_connected_port_space(port_space, "Acceptor")?;
        let listener = CmId::create(port_space)?;
        configure(&listener)?;
        listener.bind_addr(addr)?;
        listener.listen(backlog)?;
        Ok(Acceptor { listener })
    }

    /// The listening id.
    pub fn cm_id(&self) -> &CmId {
        &self.listener
    }

    /// The local address the acceptor listens on: the address passed to [`bind`](Self::bind),
    /// with the port the connection manager assigned when that was 0. `None` if the address
    /// family is neither IPv4 nor IPv6.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.listener.local_addr()
    }

    /// Blocks until the next connection request arrives and returns it, moved onto its own event
    /// channel so its events never collide with the listener's or with other connections'. Build a
    /// queue pair on its [`context`](Incoming::context), then [`accept`](Incoming::accept) it.
    ///
    /// `timeout` bounds how long to wait for a request: on expiry [`TimedOut`](Error::TimedOut)
    /// is returned, and `None` waits indefinitely. Several threads may wait here at once; each
    /// request goes to one of them, and the others keep waiting.
    ///
    /// # Errors
    ///
    ///  - [`ConnectionManager`](Error::ConnectionManager): the listener reported a failure: its
    ///    device was removed, which leaves the acceptor dead (see
    ///    [`DeviceRemoval`](CmEventType::DeviceRemoval)). Only one waiter gets the failure, and
    ///    every other or later `accept` waits out its timeout, forever with `None`; so drop the
    ///    acceptor, and with several waiters give each a timeout and stop them all once one sees
    ///    the failure.
    ///  - [`TimedOut`](Error::TimedOut): no request arrived in time.
    pub fn accept(&self, timeout: Option<Duration>) -> Result<Incoming> {
        let deadline = crate::completion::deadline_after(timeout);
        let request = self
            .listener
            .wait_for(CmEventType::ConnectRequest, deadline)?;
        let private_data = request.private_data().map_or_else(Vec::new, <[u8]>::to_vec);
        Ok(Incoming {
            id: request.connection_request()?,
            private_data,
        })
    }
}

/// An incoming connection request, ready for its queue pair to be built and accepted (or
/// rejected). Returned by [`Acceptor::accept`]. It carries its own event channel, so it is
/// self-contained and can be handed to another thread. Dropping it declines the request, even
/// while resources built on its [`context`](Self::context) live on.
#[must_use = "dropping an incoming request declines it; `accept` or `reject` it"]
pub struct Incoming {
    id: CmId,
    private_data: Vec<u8>,
}

impl Incoming {
    /// The device the request arrived on. Build the queue pair on this context, on the port
    /// [`CmId::port_num`] reports, then pass it to [`accept`](Self::accept).
    pub fn context(&self) -> Result<Context> {
        self.id.context()
    }

    /// The request's id: the peer's address, and the options that must be set before accepting
    /// ([`CmId::set_ack_timeout`]).
    pub fn cm_id(&self) -> &CmId {
        &self.id
    }

    /// The private data the peer attached to its request
    /// ([`ConnectionParameter::set_private_data`]), as reported by the transport: padded to its
    /// wire format, so typically longer than what the peer wrote, with the tail zero-filled.
    /// Empty when the peer attached none.
    pub fn peer_private_data(&self) -> &[u8] {
        &self.private_data
    }

    /// Accepts the connection (blocking) using `qp`, returning the established [`Connection`]. The
    /// queue pair number in `param` is set automatically, and the queue pair is configured with
    /// the outstanding-RDMA limits `param` advertises to the peer
    /// ([`set_responder_resources`](ConnectionParameter::set_responder_resources) /
    /// [`set_initiator_depth`](ConnectionParameter::set_initiator_depth)).
    ///
    /// `timeout` bounds how long to wait for the connection to establish: on expiry
    /// [`TimedOut`](Error::TimedOut) is returned, and `None` waits indefinitely.
    ///
    /// # Errors
    ///
    ///  - [`Accept`](Error::Accept): `rdma_accept` failed, or the private data does not fit a
    ///    reply (see [`ConnectionParameter::set_private_data`]).
    ///  - [`ConnectionManager`](Error::ConnectionManager): the peer gave up after this side
    ///    replied, before the connection was established.
    ///  - [`ModifyQueuePair`](Error::ModifyQueuePair): a queue-pair transition failed, or `qp`
    ///    is not on the device of [`context`](Self::context).
    ///  - [`TimedOut`](Error::TimedOut): the connection was not established in time.
    ///
    /// A peer that withdrew its request before this side replied (for example by dropping its id
    /// after its connect timed out) shows up as `EINVAL` instead: in
    /// [`ModifyQueuePair`](Error::ModifyQueuePair) when the connection manager refuses to compute
    /// the queue pair's attributes (`rdma_init_qp_attr`), or in [`Accept`](Error::Accept) when the
    /// request went away between those transitions and the reply.
    pub fn accept(
        self,
        qp: PreparedQueuePair<Rc>,
        param: ConnectionParameter,
        timeout: Option<Duration>,
    ) -> Result<Connection> {
        let deadline = crate::completion::deadline_after(timeout);
        let mut qp = self.id.init_qp(qp)?;
        let responder_resources = (param.param.responder_resources != RDMA_MAX_RESP_RES)
            .then_some(param.param.responder_resources);
        let initiator_depth = (param.param.initiator_depth != RDMA_MAX_INIT_DEPTH)
            .then_some(param.param.initiator_depth);
        self.id
            .ready(&mut qp, responder_resources, initiator_depth)?;
        let param = param.set_qp_num(qp.qp_num());
        self.id.accept(&param)?;
        self.id.wait_for(CmEventType::Established, deadline)?;
        Ok(Connection {
            id: self.id,
            qp,
            private_data: self.private_data,
        })
    }

    /// Declines the request instead of accepting it. The peer's connect fails with
    /// [`CmEventType::Rejected`] and can read `private_data` (at most 148 bytes) from the error.
    ///
    /// # Errors
    ///
    ///  - [`ConnectionSetup`](Error::ConnectionSetup): `rdma_reject` failed, or `private_data`
    ///    is longer than a rejection can carry. The request is declined all the same when `self`
    ///    drops, but without the data.
    pub fn reject(self, private_data: &[u8]) -> Result<()> {
        self.id.reject(private_data)
    }
}

/// An established connection: a connected [`QueuePair`] plus the connection-manager
/// identifier that keeps it alive. Returned by [`Resolved::connect`] / [`Incoming::accept`].
///
/// Dropping it disconnects (the peer sees [`CmEventType::Disconnected`]) and destroys the queue
/// pair; [`disconnect`](Self::disconnect) does the same while keeping the queue pair around to
/// reap the flushed completions.
#[must_use = "dropping a connection disconnects it"]
pub struct Connection {
    id: CmId,
    qp: QueuePair<Rc>,
    /// The private data the peer attached to its request (passive side) or reply (active side).
    private_data: Vec<u8>,
}

impl Connection {
    /// The connected queue pair, for posting work requests. Poll completions on the completion queue
    /// you built it with.
    pub fn queue_pair(&mut self) -> &mut QueuePair<Rc> {
        &mut self.qp
    }

    /// The connection's id, to watch what happens to it after establishment — the peer
    /// disconnecting ([`CmEventType::Disconnected`]), the device going away, the timewait exit —
    /// with [`CmId::get_cm_event`] or, non-blocking, [`CmId::poll_cm_event`]. Its
    /// [`peer_addr`](CmId::peer_addr) and [`local_addr`](CmId::local_addr) are the connection's two
    /// ends.
    pub fn cm_id(&self) -> &CmId {
        &self.id
    }

    /// The private data the peer attached to its connection request (on the accepting side) or
    /// its reply (on the connecting side), as reported by the transport: padded to its wire
    /// format, so typically longer than what the peer wrote, with the tail zero-filled. Empty
    /// when the peer attached none.
    pub fn peer_private_data(&self) -> &[u8] {
        &self.private_data
    }

    /// Disconnects the connection and moves the queue pair to the error state, so every
    /// outstanding work request completes with
    /// [`WorkRequestFlushed`](crate::WcStatus::WorkRequestFlushed) on its completion queue. The
    /// peer is notified with a [`CmEventType::Disconnected`] event.
    ///
    /// # Errors
    ///
    ///  - [`ConnectionSetup`](Error::ConnectionSetup): `rdma_disconnect` failed. On InfiniBand
    ///    and RoCE it fails only once the device was removed (`ENXIO`), not when the connection
    ///    is already down.
    ///  - [`ModifyQueuePair`](Error::ModifyQueuePair): moving the queue pair to the error state
    ///    failed.
    pub fn disconnect(&mut self) -> Result<()> {
        self.id.disconnect()?;
        let mut error = QueuePairAttribute::new();
        error.set_state(QueuePairState::Error);
        self.qp.modify(&error)
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Tell the peer now: destroying the id would also send the disconnect, but a clone of it
        // (from `cm_id`) can keep it alive much longer. The result is ignored: a drop cannot
        // report it, and on InfiniBand and RoCE this succeeds even if the connection is down.
        // The queue pair is destroyed right after this, which discards its outstanding work
        // requests without completing them (only `disconnect` flushes them as completions).
        let _ = self.id.disconnect();
    }
}

#[cfg(test)]
mod test_conversions {
    use super::*;

    #[test]
    fn port_space_to_raw() {
        for (wrapper, raw) in [
            (PortSpace::Tcp, ffi::rdma_port_space::RDMA_PS_TCP),
            (PortSpace::Udp, ffi::rdma_port_space::RDMA_PS_UDP),
            (PortSpace::Ipoib, ffi::rdma_port_space::RDMA_PS_IPOIB),
            (PortSpace::Ib, ffi::rdma_port_space::RDMA_PS_IB),
        ] {
            assert_eq!(ffi::rdma_port_space::from(wrapper), raw);
        }
    }

    #[test]
    fn cm_event_type_from_raw() {
        for (wrapper, raw) in [
            (
                CmEventType::AddressResolved,
                ffi::rdma_cm_event_type::RDMA_CM_EVENT_ADDR_RESOLVED,
            ),
            (
                CmEventType::ConnectRequest,
                ffi::rdma_cm_event_type::RDMA_CM_EVENT_CONNECT_REQUEST,
            ),
            (
                CmEventType::Established,
                ffi::rdma_cm_event_type::RDMA_CM_EVENT_ESTABLISHED,
            ),
            (
                CmEventType::TimewaitExit,
                ffi::rdma_cm_event_type::RDMA_CM_EVENT_TIMEWAIT_EXIT,
            ),
        ] {
            assert_eq!(CmEventType::from(raw), wrapper);
        }
    }

    #[test]
    fn timeouts_round_up_to_whole_milliseconds() {
        assert_eq!(timeout_ms(Duration::ZERO), 0);
        assert_eq!(timeout_ms(Duration::from_micros(900)), 1);
        assert_eq!(timeout_ms(Duration::from_millis(5)), 5);
        assert_eq!(timeout_ms(Duration::from_micros(5_001)), 6);
        assert_eq!(timeout_ms(Duration::MAX), c_int::MAX);
    }

    #[test]
    fn failure_events_are_failures() {
        assert!(is_failure(CmEventType::Rejected));
        assert!(is_failure(CmEventType::AddressError));
        assert!(!is_failure(CmEventType::Established));
        assert!(!is_failure(CmEventType::ConnectRequest));
    }
}
