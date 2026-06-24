//! The RDMA connection manager (`librdmacm`).
//!
//! The connection manager establishes reliable connections (and unreliable datagrams) over an
//! IP address, so applications do not have to exchange [`QueuePairEndpoint`](crate::QueuePairEndpoint)s
//! out of band the way [`PreparedQueuePair::handshake`](crate::PreparedQueuePair::handshake)
//! requires. It picks the device, resolves the route, and negotiates the queue pair parameters.
//!
//! Connection setup is blocking and has two roles, with distinct types so they cannot be mixed up:
//! the active side uses a [`Connector`], the passive side an [`Acceptor`]. Each owns its event
//! channel and drives the whole exchange internally, handing back a connected [`Connection`].
//!
//! Setup is two phased because the device only exists once the address resolves: a handle exposes
//! the [`Context`] to build the queue pair on, then a second call finishes the connection. This
//! wrapper deliberately does not use `rdma_create_qp`/`rdma_create_ep` (which tie the queue pair's
//! lifetime to the connection and limit control over its attributes); you build a normal queue pair
//! and get it back, fully connected, only once setup completes.
//!
//! # Active side
//!
//! ```no_run
//! use std::time::Duration;
//! use ibverbs::rdmacm::{ConnectionParameter, Connector, rdma_port_space};
//!
//! # fn main() -> ibverbs::Result<()> {
//! let resolved = Connector::new(rdma_port_space::RDMA_PS_TCP)?
//!     .resolve("192.0.2.1:18515".parse().unwrap(), Duration::from_secs(2))?;
//! let ctx = resolved.context()?;
//! let pd = ctx.alloc_pd()?;
//! let cq = ctx.create_cq(16).build()?;
//! let qp = pd.create_qp(&cq, &cq, ibverbs::ibv_qp_type::IBV_QPT_RC)?.build()?;
//! let mut conn = resolved.connect(qp, ConnectionParameter::default())?;
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

use std::io;
use std::mem::MaybeUninit;
use std::net::SocketAddr;
use std::os::raw::{c_int, c_void};
use std::ptr;
use std::sync::Arc;
use std::time::Duration;

use nix::sys::socket::{SockaddrIn, SockaddrIn6, SockaddrLike};

use crate::{Context, Error, PreparedQueuePair, QueuePair, Result};

pub use ffi::rdma_port_space;

/// Holds a `sockaddr` of the right family so its pointer stays valid for a single C call.
enum OsSocketAddr {
    V4(SockaddrIn),
    V6(SockaddrIn6),
}

impl OsSocketAddr {
    fn new(addr: SocketAddr) -> Self {
        match addr {
            SocketAddr::V4(v4) => OsSocketAddr::V4(v4.into()),
            SocketAddr::V6(v6) => OsSocketAddr::V6(v6.into()),
        }
    }

    fn as_ptr(&self) -> *mut ffi::sockaddr {
        match self {
            OsSocketAddr::V4(s) => s.as_ptr() as *mut ffi::sockaddr,
            OsSocketAddr::V6(s) => s.as_ptr() as *mut ffi::sockaddr,
        }
    }
}

fn timeout_ms(timeout: Duration) -> c_int {
    timeout.as_millis().min(c_int::MAX as u128) as c_int
}

/// Whether a connection-manager event reports a failure that aborts setup.
fn is_failure(event: ffi::rdma_cm_event_type) -> bool {
    matches!(
        event,
        ffi::rdma_cm_event_type::RDMA_CM_EVENT_ADDR_ERROR
            | ffi::rdma_cm_event_type::RDMA_CM_EVENT_ROUTE_ERROR
            | ffi::rdma_cm_event_type::RDMA_CM_EVENT_CONNECT_ERROR
            | ffi::rdma_cm_event_type::RDMA_CM_EVENT_UNREACHABLE
            | ffi::rdma_cm_event_type::RDMA_CM_EVENT_REJECTED
            | ffi::rdma_cm_event_type::RDMA_CM_EVENT_DEVICE_REMOVAL
    )
}

/// An rdma_cm event channel. Owned 1:1 by the [`CmId`] whose events it carries; it is created with
/// the id and destroyed after it.
struct EventChannel {
    chan: *mut ffi::rdma_event_channel,
}

impl EventChannel {
    /// Opens a new event channel.
    fn new() -> Result<EventChannel> {
        let chan = unsafe { ffi::rdma_create_event_channel() };
        if chan.is_null() {
            return Err(Error::ConnectionSetup(io::Error::last_os_error()));
        }
        Ok(EventChannel { chan })
    }
}

impl Drop for EventChannel {
    fn drop(&mut self) {
        unsafe { ffi::rdma_destroy_event_channel(self.chan) };
    }
}

/// An rdma_cm identifier (a connection or a listener) together with its own event channel.
///
/// Shared behind an `Arc` so a [`Context`] borrowed from its `verbs` can keep it — and thus that
/// `ibv_context` — alive for as long as the context, or anything built from it, is in use (see
/// [`Resolved::context`]). The id (and then its channel) is destroyed once the last reference drops.
struct CmId {
    channel: EventChannel,
    id: *mut ffi::rdma_cm_id,
}

// Ownership of an `rdma_cm_id` and its channel can be moved (and shared) between threads.
unsafe impl Send for CmId {}
unsafe impl Sync for CmId {}

impl CmId {
    /// Creates a new identifier on its own fresh event channel.
    fn create(port_space: rdma_port_space) -> Result<CmId> {
        let channel = EventChannel::new()?;
        let mut id: *mut ffi::rdma_cm_id = ptr::null_mut();
        let ret = unsafe {
            ffi::rdma_create_id(channel.chan, &mut id, ptr::null_mut::<c_void>(), port_space)
        };
        if ret != 0 {
            // `channel` drops here, destroying the event channel.
            return Err(Error::ConnectionSetup(io::Error::last_os_error()));
        }
        Ok(CmId { channel, id })
    }

    /// Blocks until the next event on this id's channel is available and returns it.
    fn get_cm_event(&self) -> Result<CmEvent> {
        let mut event: *mut ffi::rdma_cm_event = ptr::null_mut();
        let ret = unsafe { ffi::rdma_get_cm_event(self.channel.chan, &mut event) };
        if ret != 0 {
            return Err(Error::ConnectionSetup(io::Error::last_os_error()));
        }
        Ok(CmEvent { event })
    }

    /// Blocks until an `expected` event arrives, acknowledging and skipping any others, and
    /// returning an error on a failure event. Drives the blocking setup helpers.
    fn wait_for(&self, expected: ffi::rdma_cm_event_type) -> Result<()> {
        loop {
            // `get_cm_event` blocks on the channel's (blocking) fd until an event arrives — this
            // does not spin. The loop only goes around to skip a non-matching event, re-blocking on
            // the next `get_cm_event`. Each event is acknowledged when it drops at the iteration end.
            let kind = self.get_cm_event()?.event_type();
            if kind == expected {
                return Ok(());
            }
            if is_failure(kind) {
                return Err(Error::ConnectionManager(kind));
            }
        }
    }

    /// Binds to a local `addr` (passive side). Bind to an unspecified address such as
    /// `0.0.0.0:port` to accept connections on any device.
    fn bind_addr(&self, addr: SocketAddr) -> Result<()> {
        let addr = OsSocketAddr::new(addr);
        let ret = unsafe { ffi::rdma_bind_addr(self.id, addr.as_ptr()) };
        if ret != 0 {
            return Err(Error::BindAddress(io::Error::last_os_error()));
        }
        Ok(())
    }

    /// Starts listening for incoming connection requests (passive side), queueing up to `backlog`.
    fn listen(&self, backlog: i32) -> Result<()> {
        let ret = unsafe { ffi::rdma_listen(self.id, backlog) };
        if ret != 0 {
            return Err(Error::ConnectionSetup(io::Error::last_os_error()));
        }
        Ok(())
    }

    /// Resolves the destination address to an RDMA device and local route (active side). On success
    /// a `RDMA_CM_EVENT_ADDR_RESOLVED` event is delivered.
    fn resolve_addr(&self, dst: SocketAddr, timeout: Duration) -> Result<()> {
        let dst = OsSocketAddr::new(dst);
        let ret = unsafe {
            ffi::rdma_resolve_addr(self.id, ptr::null_mut(), dst.as_ptr(), timeout_ms(timeout))
        };
        if ret != 0 {
            return Err(Error::ResolveAddress(io::Error::last_os_error()));
        }
        Ok(())
    }

    /// Resolves the route to the destination (active side), after the address has resolved. On
    /// success a `RDMA_CM_EVENT_ROUTE_RESOLVED` event is delivered.
    fn resolve_route(&self, timeout: Duration) -> Result<()> {
        let ret = unsafe { ffi::rdma_resolve_route(self.id, timeout_ms(timeout)) };
        if ret != 0 {
            return Err(Error::ResolveRoute(io::Error::last_os_error()));
        }
        Ok(())
    }

    /// Initiates a connection to the remote (active side). On success a
    /// `RDMA_CM_EVENT_CONNECT_RESPONSE` (external queue pair) or `RDMA_CM_EVENT_ESTABLISHED` event
    /// is delivered. `param` carries the local queue pair number.
    fn connect(&self, param: &ConnectionParameter) -> Result<()> {
        let mut param = param.0;
        let ret = unsafe { ffi::rdma_connect(self.id, &mut param) };
        if ret != 0 {
            return Err(Error::Connect(io::Error::last_os_error()));
        }
        Ok(())
    }

    /// Accepts a connection request (passive side), in response to a
    /// `RDMA_CM_EVENT_CONNECT_REQUEST`. `param` carries the local queue pair number.
    fn accept(&self, param: &ConnectionParameter) -> Result<()> {
        let mut param = param.0;
        let ret = unsafe { ffi::rdma_accept(self.id, &mut param) };
        if ret != 0 {
            return Err(Error::Accept(io::Error::last_os_error()));
        }
        Ok(())
    }

    /// Completes connection establishment on the active side after the queue pair has reached
    /// `RTS`, in response to a `RDMA_CM_EVENT_CONNECT_RESPONSE`.
    fn establish(&self) -> Result<()> {
        let ret = unsafe { ffi::rdma_establish(self.id) };
        if ret != 0 {
            return Err(Error::Connect(io::Error::last_os_error()));
        }
        Ok(())
    }

    /// Disconnects an established connection, delivering `RDMA_CM_EVENT_DISCONNECTED` to both sides.
    fn disconnect(&self) -> Result<()> {
        let ret = unsafe { ffi::rdma_disconnect(self.id) };
        if ret != 0 {
            return Err(Error::ConnectionSetup(io::Error::last_os_error()));
        }
        Ok(())
    }

    /// The device context the connection manager bound this id to (its `verbs`). Only available once
    /// the address has resolved.
    fn verbs(&self) -> Result<*mut ffi::ibv_context> {
        let verbs = unsafe { (*self.id).verbs };
        if verbs.is_null() {
            return Err(Error::ConnectionSetup(io::Error::other(
                "connection manager has not bound a device yet",
            )));
        }
        Ok(verbs)
    }

    /// Builds the queue pair from `prepared` and moves it from `RESET` to `INIT`. Finish the
    /// transition after the connection is set up with [`ready`](Self::ready).
    fn init_qp(&self, prepared: PreparedQueuePair) -> Result<QueuePair> {
        let qp = prepared.into_queue_pair();
        self.transition(&qp, ffi::ibv_qp_state::IBV_QPS_INIT)?;
        Ok(qp)
    }

    /// Moves `qp` from `INIT` through `RTR` to `RTS`, completing the connection-manager transition.
    fn ready(&self, qp: &QueuePair) -> Result<()> {
        self.transition(qp, ffi::ibv_qp_state::IBV_QPS_RTR)?;
        self.transition(qp, ffi::ibv_qp_state::IBV_QPS_RTS)
    }

    /// Transitions `qp` to `state` using the attributes the connection manager computes from the
    /// resolved route and negotiated parameters (`rdma_init_qp_attr` + `ibv_modify_qp`).
    fn transition(&self, qp: &QueuePair, state: ffi::ibv_qp_state) -> Result<()> {
        let mut attr = MaybeUninit::<ffi::ibv_qp_attr>::zeroed();
        // `rdma_init_qp_attr` reads the target state from the attribute and fills in the rest.
        unsafe { (*attr.as_mut_ptr()).qp_state = state };
        let mut mask: c_int = 0;
        let ret = unsafe { ffi::rdma_init_qp_attr(self.id, attr.as_mut_ptr(), &mut mask) };
        if ret != 0 {
            return Err(Error::ModifyQueuePair(io::Error::last_os_error()));
        }
        let errno = unsafe { ffi::ibv_modify_qp(qp.as_raw(), attr.as_mut_ptr(), mask) };
        if errno != 0 {
            return Err(Error::errno(errno, Error::ModifyQueuePair));
        }
        Ok(())
    }
}

impl Drop for CmId {
    fn drop(&mut self) {
        // Destroy the id before its channel (the `channel` field drops right after this).
        unsafe { ffi::rdma_destroy_id(self.id) };
    }
}

/// A connection-manager event, acknowledged automatically when dropped.
struct CmEvent {
    event: *mut ffi::rdma_cm_event,
}

impl CmEvent {
    /// The event type.
    fn event_type(&self) -> ffi::rdma_cm_event_type {
        unsafe { (*self.event).event }
    }

    /// Consumes the event, taking the new id a `RDMA_CM_EVENT_CONNECT_REQUEST` carries and migrating
    /// it onto its own fresh event channel, so its later events are isolated rather than colliding
    /// with the listener's. Only valid on that event type. The event is acknowledged on return.
    fn migrate_request_id(self) -> Result<CmId> {
        let channel = EventChannel::new()?;
        let id = unsafe { (*self.event).id };
        let ret = unsafe { ffi::rdma_migrate_id(id, channel.chan) };
        if ret != 0 {
            // The request id is ours to destroy once we abandon it; `channel` drops after.
            unsafe { ffi::rdma_destroy_id(id) };
            return Err(Error::ConnectionSetup(io::Error::last_os_error()));
        }
        Ok(CmId { channel, id })
    }
}

impl Drop for CmEvent {
    fn drop(&mut self) {
        unsafe { ffi::rdma_ack_cm_event(self.event) };
    }
}

/// Parameters for connecting and accepting.
///
/// [`Default`] gives sane reliable-connection defaults; the local queue pair number is set
/// automatically by [`Resolved::connect`] / [`Incoming::accept`].
#[derive(Clone, Copy)]
pub struct ConnectionParameter(ffi::rdma_conn_param);

impl Default for ConnectionParameter {
    /// Reliable-connection defaults: one outstanding RDMA read/atomic in each direction, and the
    /// maximum retry counts.
    fn default() -> Self {
        let mut param: ffi::rdma_conn_param = unsafe { std::mem::zeroed() };
        param.responder_resources = 1;
        param.initiator_depth = 1;
        param.retry_count = 7;
        param.rnr_retry_count = 7;
        ConnectionParameter(param)
    }
}

impl ConnectionParameter {
    /// Sets the number of outstanding RDMA read/atomic operations the local side can service as a
    /// responder.
    pub fn set_responder_resources(&mut self, responder_resources: u8) -> &mut Self {
        self.0.responder_resources = responder_resources;
        self
    }

    /// Sets the number of outstanding RDMA read/atomic operations the local side can issue as an
    /// initiator.
    pub fn set_initiator_depth(&mut self, initiator_depth: u8) -> &mut Self {
        self.0.initiator_depth = initiator_depth;
        self
    }

    /// Sets how many times to retry a connection or transport operation before reporting an error
    /// (0-7).
    pub fn set_retry_count(&mut self, retry_count: u8) -> &mut Self {
        self.0.retry_count = retry_count;
        self
    }

    /// Sets how many times to retry sending after a receiver-not-ready error (0-7).
    pub fn set_rnr_retry_count(&mut self, rnr_retry_count: u8) -> &mut Self {
        self.0.rnr_retry_count = rnr_retry_count;
        self
    }

    /// Sets the local queue pair number the peer should target.
    fn set_qp_num(&mut self, qp_num: u32) {
        self.0.qp_num = qp_num;
    }
}

/// Active-side blocking connection setup. Drives address and route resolution, then yields a
/// [`Resolved`] from which you build a queue pair and connect.
pub struct Connector {
    id: Arc<CmId>,
}

impl Connector {
    /// Creates a connector with its own event channel.
    pub fn new(port_space: rdma_port_space) -> Result<Self> {
        Ok(Connector {
            id: Arc::new(CmId::create(port_space)?),
        })
    }

    /// Resolves the destination address and route (blocking until both complete), then returns a
    /// handle to build the queue pair on the resolved device.
    pub fn resolve(self, dst: SocketAddr, timeout: Duration) -> Result<Resolved> {
        self.id.resolve_addr(dst, timeout)?;
        self.id
            .wait_for(ffi::rdma_cm_event_type::RDMA_CM_EVENT_ADDR_RESOLVED)?;
        self.id.resolve_route(timeout)?;
        self.id
            .wait_for(ffi::rdma_cm_event_type::RDMA_CM_EVENT_ROUTE_RESOLVED)?;
        Ok(Resolved { id: self.id })
    }
}

/// A resolved active connection, ready for its queue pair to be built and connected.
pub struct Resolved {
    id: Arc<CmId>,
}

impl Resolved {
    /// The device the connection manager resolved to. Build the queue pair (and its protection
    /// domain and completion queue) on this context, then pass it to [`connect`](Self::connect).
    pub fn context(&self) -> Result<Context> {
        Ok(Context::from_borrowed_context(
            self.id.verbs()?,
            self.id.clone(),
        ))
    }

    /// Connects to the remote (blocking) using `qp`, returning the established [`Connection`]. The
    /// queue pair number in `param` is set automatically.
    pub fn connect(
        self,
        qp: PreparedQueuePair,
        mut param: ConnectionParameter,
    ) -> Result<Connection> {
        let qp = self.id.init_qp(qp)?;
        param.set_qp_num(qp.qp_num());
        self.id.connect(&param)?;
        self.id
            .wait_for(ffi::rdma_cm_event_type::RDMA_CM_EVENT_CONNECT_RESPONSE)?;
        self.id.ready(&qp)?;
        self.id.establish()?;
        Ok(Connection { id: self.id, qp })
    }
}

/// Passive-side blocking connection setup. Binds, listens, and accepts incoming connections.
pub struct Acceptor {
    listener: Arc<CmId>,
}

impl Acceptor {
    /// Binds to `addr` (use an unspecified address such as `0.0.0.0:port` for any device) and starts
    /// listening, queueing up to `backlog` pending connections.
    pub fn bind(addr: SocketAddr, port_space: rdma_port_space, backlog: i32) -> Result<Self> {
        let listener = CmId::create(port_space)?;
        listener.bind_addr(addr)?;
        listener.listen(backlog)?;
        Ok(Acceptor {
            listener: Arc::new(listener),
        })
    }

    /// Blocks until the next connection request arrives and returns it, moved onto its own event
    /// channel so its events never collide with the listener's or with other connections'. Build a
    /// queue pair on its [`context`](Incoming::context), then [`accept`](Incoming::accept) it.
    pub fn accept(&self) -> Result<Incoming> {
        loop {
            let event = self.listener.get_cm_event()?;
            if event.event_type() == ffi::rdma_cm_event_type::RDMA_CM_EVENT_CONNECT_REQUEST {
                return Ok(Incoming {
                    id: Arc::new(event.migrate_request_id()?),
                });
            }
            // The listener channel only carries connection requests; anything else is acknowledged
            // and ignored when `event` drops here.
        }
    }
}

/// An incoming connection request, ready for its queue pair to be built and accepted. It carries
/// its own event channel, so it is self-contained and can be handed to another thread.
pub struct Incoming {
    id: Arc<CmId>,
}

impl Incoming {
    /// The device the request arrived on. Build the queue pair on this context, then pass it to
    /// [`accept`](Self::accept).
    pub fn context(&self) -> Result<Context> {
        Ok(Context::from_borrowed_context(
            self.id.verbs()?,
            self.id.clone(),
        ))
    }

    /// Accepts the connection (blocking) using `qp`, returning the established [`Connection`]. The
    /// queue pair number in `param` is set automatically.
    pub fn accept(
        self,
        qp: PreparedQueuePair,
        mut param: ConnectionParameter,
    ) -> Result<Connection> {
        let qp = self.id.init_qp(qp)?;
        self.id.ready(&qp)?;
        param.set_qp_num(qp.qp_num());
        self.id.accept(&param)?;
        self.id
            .wait_for(ffi::rdma_cm_event_type::RDMA_CM_EVENT_ESTABLISHED)?;
        Ok(Connection { id: self.id, qp })
    }
}

/// An established connection: a connected [`QueuePair`](crate::QueuePair) plus the connection-manager
/// identifier that keeps it alive. Dropping it tears the connection down.
pub struct Connection {
    id: Arc<CmId>,
    qp: QueuePair,
}

impl Connection {
    /// The connected queue pair, for posting work requests. Poll completions on the completion queue
    /// you built it with.
    pub fn queue_pair(&mut self) -> &mut QueuePair {
        &mut self.qp
    }

    /// Disconnects the connection. The peer is notified with a `RDMA_CM_EVENT_DISCONNECTED` event.
    pub fn disconnect(&self) -> Result<()> {
        self.id.disconnect()
    }
}
