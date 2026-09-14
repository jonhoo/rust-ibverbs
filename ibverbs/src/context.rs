use std::borrow::Cow;
use std::ffi::CStr;
use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::ops::Deref;
use std::os::fd::BorrowedFd;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::address::{Gid, GidEntry, GidType};
use crate::completion::{CompletionChannel, CompletionQueueBuilder};
use crate::device::Guid;
use crate::error::{Error, Result};
use crate::pd::{ProtectionDomain, ProtectionDomainInner};

#[cfg(doc)]
use crate::{Device, QueuePairBuilder, WorkCompletion};

pub(crate) struct ContextInner {
    pub(crate) ctx: *mut ffi::ibv_context,
    ownership: ContextOwnership,
}

/// Whether a [`Context`] owns its `ibv_context` or borrows one owned elsewhere.
enum ContextOwnership {
    /// We opened the device and close it on drop.
    Owned,
    /// `ctx` is borrowed from another owner (an `rdma_cm_id`'s `verbs`). We keep that owner alive so
    /// `ctx` stays valid for as long as this context and anything derived from it lives, and we do
    /// not close the device ourselves. The `Arc` is held purely so its `Drop` runs last, so the
    /// field is intentionally never read.
    #[cfg(feature = "rdmacm")]
    #[allow(dead_code)]
    Borrowed(Arc<dyn Send + Sync>),
}

impl ContextInner {
    pub(crate) fn query_port(&self, port_num: u8) -> Result<ffi::ibv_port_attr> {
        // TODO: from http://www.rdmamojo.com/2012/07/21/ibv_query_port/
        //
        //   Most of the port attributes, returned by ibv_query_port(), aren't constant and may be
        //   changed, mainly by the SM (in InfiniBand), or by the Hardware. It is highly
        //   recommended avoiding saving the result of this query, or to flush them when a new SM
        //   (re)configures the subnet.
        //
        let mut port_attr = ffi::ibv_port_attr::default();
        // The shim (rdma-core's `___ibv_query_port` inline) also fills the extended fields, such
        // as `active_speed_ex`, which the exported compat `ibv_query_port` symbol leaves zeroed.
        let errno = unsafe { ffi::___ibv_query_port(self.ctx, port_num, &mut port_attr) };
        if errno != 0 {
            return Err(Error::errno(errno, |e| Error::QueryPort {
                port_num,
                source: e,
            }));
        }

        // From http://www.rdmamojo.com/2012/08/02/ibv_query_gid/:
        //
        //   The content of the GID table is valid only when the port_attr.state is either
        //   IBV_PORT_ARMED or IBV_PORT_ACTIVE. For other states of the port, the value of the GID
        //   table is indeterminate.
        //
        match port_attr.state {
            ffi::ibv_port_state::IBV_PORT_ACTIVE | ffi::ibv_port_state::IBV_PORT_ARMED => {}
            _ => {
                return Err(Error::PortNotActive(port_num));
            }
        }
        Ok(port_attr)
    }
}

impl Drop for ContextInner {
    fn drop(&mut self) {
        match &self.ownership {
            ContextOwnership::Owned => {
                let errno = unsafe { ffi::ibv_close_device(self.ctx) };
                if errno != 0 {
                    let e = io::Error::from_raw_os_error(errno);
                    panic!("ibv_close_device failed: {e}");
                }
            }
            // Borrowed: don't close the device; dropping the kept-alive owner is enough.
            #[cfg(feature = "rdmacm")]
            ContextOwnership::Borrowed(_) => {}
        }
    }
}

unsafe impl Sync for ContextInner {}
unsafe impl Send for ContextInner {}

/// An RDMA context bound to a device. Created by [`Device::open`].
///
/// Cloning is cheap (reference counted) and hands the same device context to another thread or
/// owner; the context is closed once the last clone, and everything built from it, is dropped.
#[must_use]
#[derive(Clone)]
pub struct Context {
    inner: Arc<ContextInner>,
}

impl fmt::Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = unsafe { ffi::ibv_get_device_name((*self.inner.ctx).device) };
        let mut f = f.debug_tuple("Context");
        if name.is_null() {
            f.field(&"?");
        } else {
            f.field(&unsafe { CStr::from_ptr(name) });
        }
        f.finish()
    }
}

impl Context {
    /// Opens a context for the given device.
    pub(crate) fn with_device(dev: *mut ffi::ibv_device) -> Result<Context> {
        assert!(!dev.is_null());

        let ctx = unsafe { ffi::ibv_open_device(dev) };
        if ctx.is_null() {
            return Err(Error::OpenDevice(io::Error::last_os_error()));
        }
        let context = Context {
            inner: Arc::new(ContextInner {
                ctx,
                ownership: ContextOwnership::Owned,
            }),
        };
        // If this fails, `context` drops here and closes the half-opened device again.
        context.set_async_fd_nonblocking()?;
        Ok(context)
    }

    /// Wraps a raw `ibv_context` owned by `owner` (the RDMA connection manager's `rdma_cm_id`).
    ///
    /// The returned [`Context`] does not close the device on drop, and keeps `owner` alive for as
    /// long as the context (or any protection domain, completion queue, queue pair, or memory
    /// region built from it) is alive, so `ctx` cannot dangle.
    #[cfg(feature = "rdmacm")]
    pub(crate) fn from_borrowed_context(
        ctx: *mut ffi::ibv_context,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Context> {
        let context = Context {
            inner: Arc::new(ContextInner {
                ctx,
                ownership: ContextOwnership::Borrowed(owner),
            }),
        };
        // The async-event descriptor is shared with every other borrow of this device context;
        // setting it non-blocking is idempotent, so doing it once per borrow is harmless.
        context.set_async_fd_nonblocking()?;
        Ok(context)
    }

    /// Set this context's asynchronous-event file descriptor to non-blocking, so
    /// [`poll_async_event`](Self::poll_async_event) reports an empty event queue instead of
    /// blocking.
    fn set_async_fd_nonblocking(&self) -> Result<()> {
        // SAFETY: the context owns this fd, and the borrow ends within this call.
        let fd = unsafe { std::os::fd::BorrowedFd::borrow_raw((*self.inner.ctx).async_fd) };
        let flags =
            nix::fcntl::fcntl(fd, nix::fcntl::F_GETFL).map_err(|e| Error::OpenDevice(e.into()))?;
        let arg = nix::fcntl::FcntlArg::F_SETFL(
            nix::fcntl::OFlag::from_bits_retain(flags) | nix::fcntl::OFlag::O_NONBLOCK,
        );
        nix::fcntl::fcntl(fd, arg).map_err(|e| Error::OpenDevice(e.into()))?;
        Ok(())
    }

    /// Begin building a completion queue (CQ) with room for at least `min_cq_entries` entries.
    ///
    /// When an outstanding Work Request, within a Send or Receive Queue, is completed, a Work
    /// Completion is added to the CQ of that Work Queue. This Work Completion indicates that
    /// the outstanding Work Request has been completed (and no longer considered outstanding) and
    /// provides details on it (status, direction, opcode, etc.).
    ///
    /// A single CQ can be shared by the send and receive queues of multiple QPs. The Work
    /// Completion holds the information to specify the QP number and the Queue (Send or Receive)
    /// that it came from.
    ///
    /// `min_cq_entries` is the minimum size of the CQ (the actual size can be larger) and is the only
    /// required parameter. The optional ones are configured on the returned
    /// [`CompletionQueueBuilder`]: an opaque context cookie, the completion vector, a completion
    /// channel ([`set_comp_channel`](CompletionQueueBuilder::set_comp_channel)), and extra
    /// work-completion fields such as a hardware timestamp. Call
    /// [`build`](CompletionQueueBuilder::build) to create the queue.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # fn f(ctx: &ibverbs::Context) -> ibverbs::Result<()> {
    /// let cq = ctx.create_cq(16).build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn create_cq(&self, min_cq_entries: u32) -> CompletionQueueBuilder {
        CompletionQueueBuilder {
            ctx: self.inner.clone(),
            min_cq_entries,
            cq_context: 0,
            comp_vector: 0,
            wc_flags: 0,
            comp_channel: None,
        }
    }

    /// Create a completion channel: the file descriptor that delivers completion notifications for
    /// the queues built on it with [`CompletionQueueBuilder::set_comp_channel`].
    ///
    /// By default a completion queue has no channel and is driven by polling alone; a channel is
    /// what lets you block for completions ([`CompletionChannel::wait`]) or hand the descriptor to
    /// an event loop instead. Several queues can share one channel — a single descriptor then
    /// reports notifications for all of them, which is what you want for a server driving many
    /// queue pairs from one `epoll`/reactor. See [`CompletionChannel`] for the notification loop.
    ///
    /// # Errors
    ///
    ///  - [`CreateCompletionChannel`](Error::CreateCompletionChannel): creating the channel or
    ///    setting its descriptor non-blocking failed.
    pub fn create_comp_channel(&self) -> Result<CompletionChannel> {
        CompletionChannel::new(&self.inner)
    }

    /// The file descriptor that delivers the device's asynchronous events, for handing to an
    /// event loop: it becomes readable when an event is pending, to be consumed with
    /// [`poll_async_event`](Self::poll_async_event). The descriptor is non-blocking.
    pub fn async_fd(&self) -> BorrowedFd<'_> {
        // SAFETY: the descriptor lives until the device context is closed, which the borrow of
        // `self` prevents.
        unsafe { BorrowedFd::borrow_raw((*self.inner.ctx).async_fd) }
    }

    /// Consume one pending asynchronous event from the device, or `None` if none is pending.
    ///
    /// Asynchronous events are the device's out-of-band reports: affiliated errors on a
    /// completion queue, queue pair, or shared receive queue, port state changes, and
    /// device-wide failures — see [`AsyncEventType`] for the full list. This never blocks; to
    /// wait for an event, use [`wait_async_event`](Self::wait_async_event), or watch
    /// [`async_fd`](Self::async_fd) from your own reactor and drain pending events here when it
    /// becomes readable.
    ///
    /// The returned event is acknowledged when it drops; see [`AsyncEvent`] for why that should
    /// happen promptly.
    ///
    /// # Errors
    ///
    ///  - [`AsyncEvent`](Error::AsyncEvent): reading the event failed (`ibv_get_async_event`).
    pub fn poll_async_event(&self) -> Result<Option<AsyncEvent<'_>>> {
        // `ibv_async_event` embeds an enum with no zero variant inside a union, so let
        // `ibv_get_async_event` initialize the storage before a Rust value is formed.
        let mut event = std::mem::MaybeUninit::<ffi::ibv_async_event>::uninit();
        let rc = unsafe { ffi::ibv_get_async_event(self.inner.ctx, event.as_mut_ptr()) };
        if rc != 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::WouldBlock {
                return Ok(None);
            }
            return Err(Error::AsyncEvent(e));
        }
        Ok(Some(AsyncEvent {
            // SAFETY: `ibv_get_async_event` succeeded, so it filled in the event.
            event: unsafe { event.assume_init() },
            _ctx: PhantomData,
        }))
    }

    /// Block until an asynchronous event is available (up to `timeout`), then consume it.
    /// Returns `None` only if `timeout` elapses first; with no timeout it waits indefinitely,
    /// even if other threads race it for events.
    ///
    /// This is the blocking form of [`poll_async_event`](Self::poll_async_event): it waits on
    /// [`async_fd`](Self::async_fd) for you rather than requiring an external reactor.
    ///
    /// # Errors
    ///
    ///  - [`AsyncEvent`](Error::AsyncEvent): waiting for or reading the event failed.
    pub fn wait_async_event(&self, timeout: Option<Duration>) -> Result<Option<AsyncEvent<'_>>> {
        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        loop {
            let remaining = deadline.map(|deadline| {
                crate::completion::ceil_to_millis(
                    deadline.saturating_duration_since(Instant::now()),
                )
            });
            let pollfd = nix::poll::PollFd::new(self.async_fd(), nix::poll::PollFlags::POLLIN);
            let ret = nix::poll::poll(
                &mut [pollfd],
                remaining
                    .map(nix::poll::PollTimeout::try_from)
                    .transpose()
                    .map_err(|_| {
                        Error::AsyncEvent(io::Error::other(
                            "failed to convert timeout to PollTimeout",
                        ))
                    })?,
            )
            .map_err(|e| Error::AsyncEvent(e.into()))?;
            match ret {
                0 => return Ok(None),
                1 => {
                    // The descriptor was readable, but another thread may have consumed the
                    // event first; if so, go back to waiting for the next one.
                    if let Some(event) = self.poll_async_event()? {
                        return Ok(Some(event));
                    }
                }
                _ => unreachable!("we passed 1 fd to poll, but it returned {ret}"),
            }
        }
    }

    /// Allocate a protection domain (PD) for the device's context.
    ///
    /// The created PD will be used primarily to create `QueuePair`s and `MemoryRegion`s.
    ///
    /// A protection domain is a means of protection, and helps you create a group of objects that
    /// can work together. If several objects were created using PD1, and others were created using
    /// PD2, working with objects from group1 together with objects from group2 will not work.
    ///
    /// # Errors
    ///
    ///  - [`AllocProtectionDomain`](Error::AllocProtectionDomain): `ibv_alloc_pd` failed.
    pub fn alloc_pd(&self) -> Result<ProtectionDomain> {
        let pd = unsafe { ffi::ibv_alloc_pd(self.inner.ctx) };
        if pd.is_null() {
            Err(Error::AllocProtectionDomain(io::Error::last_os_error()))
        } else {
            Ok(ProtectionDomain {
                inner: Arc::new(ProtectionDomainInner {
                    ctx: self.inner.clone(),
                    pd,
                }),
            })
        }
    }

    /// Returns the valid GID table entries of this RDMA device context.
    ///
    /// The entries span all of the device's ports; each carries the `port_num` and `gid_index` it
    /// belongs to (the latter is what [`QueuePairBuilder::set_gid_index`] expects).
    ///
    /// # Errors
    ///
    ///  - [`QueryDevice`](Error::QueryDevice) / [`QueryPort`](Error::QueryPort): sizing the table
    ///    failed.
    ///  - [`QueryGidTable`](Error::QueryGidTable): `ibv_query_gid_table` failed.
    pub fn gid_table(&self) -> Result<Vec<GidEntry>> {
        // The table spans every port, so size the buffer for all of them: each port contributes
        // up to its own `gid_tbl_len` entries.
        let num_ports = self.query_device()?.phys_port_cnt;
        let mut max_entries = 0usize;
        for port_num in 1..=num_ports {
            max_entries += self.query_port(port_num)?.gid_tbl_len.max(0) as usize;
        }
        let mut gid_table = vec![ffi::ibv_gid_entry::default(); max_entries];
        let num_entries = unsafe {
            ffi::_ibv_query_gid_table(
                self.inner.ctx,
                gid_table.as_mut_ptr(),
                max_entries,
                0,
                size_of::<ffi::ibv_gid_entry>(),
            )
        };
        if num_entries < 0 {
            return Err(Error::errno(-num_entries as i32, Error::QueryGidTable));
        }
        gid_table.truncate(num_entries as usize);
        let gid_table = gid_table.into_iter().map(GidEntry::from).collect();
        Ok(gid_table)
    }

    /// The GID table entry to route from on `port_num` (numbered from 1), or `None` if the port
    /// has no entries.
    ///
    /// Not every entry routes. On RoCE over plain Ethernet, peers answer on the RoCE v2 entry that
    /// holds the interface's IP address, and for an IPv4 network that is the IPv4-mapped one
    /// (`::ffff:a.b.c.d`); this prefers such an entry and otherwise falls back to the port's first
    /// entry (on InfiniBand, the port GID at index 0, where any entry routes). Pass the entry's
    /// [`gid_index`](GidEntry::gid_index) to [`QueuePairBuilder::set_gid_index`], and its
    /// [`gid`](GidEntry::gid) to the peer or to [`AddressHandleAttribute::set_grh`]. Choose
    /// differently from [`gid_table`](Self::gid_table) when the network needs another entry.
    ///
    /// # Errors
    ///
    /// The errors of [`gid_table`](Self::gid_table).
    ///
    /// [`AddressHandleAttribute::set_grh`]: crate::AddressHandleAttribute::set_grh
    pub fn routable_gid(&self, port_num: u8) -> Result<Option<GidEntry>> {
        let table = self.gid_table()?;
        let on_port = |entry: &&GidEntry| entry.port_num == port_num;
        let entry = table
            .iter()
            .filter(on_port)
            .find(|entry| entry.gid_type == GidType::RoceV2 && entry.gid.is_ipv4_mapped())
            .or_else(|| table.iter().find(on_port));
        Ok(entry.cloned())
    }

    /// Query a single entry of a port's GID table (`ibv_query_gid`).
    ///
    /// Ports are numbered from 1. For the full table at once (with the GID type and associated net
    /// device of each entry), use [`gid_table`](Self::gid_table).
    ///
    /// # Errors
    ///
    ///  - [`QueryGid`](Error::QueryGid): `ibv_query_gid` failed (`EINVAL` for an invalid
    ///    `port_num` or `gid_index`).
    pub fn query_gid(&self, port_num: u8, gid_index: u32) -> Result<Gid> {
        let mut gid = ffi::ibv_gid::default();
        let rc =
            unsafe { ffi::ibv_query_gid(self.inner.ctx, port_num, gid_index as i32, &mut gid) };
        if rc != 0 {
            return Err(Error::os(io::Error::last_os_error(), |e| Error::QueryGid {
                port_num,
                gid_index,
                source: e,
            }));
        }
        Ok(gid.into())
    }

    /// The number of completion vectors the device supports: the exclusive upper bound for
    /// [`CompletionQueueBuilder::set_comp_vector`].
    ///
    /// Completion vectors map to the device's interrupt vectors, so spreading busy completion
    /// queues across vectors spreads their notification handling across CPUs.
    pub fn num_comp_vectors(&self) -> u32 {
        let n = unsafe { (*self.inner.ctx).num_comp_vectors };
        u32::try_from(n).unwrap_or(0)
    }

    /// Query the attributes and capabilities of this context's device (`ibv_query_device`).
    ///
    /// The returned [`DeviceAttr`] reports device-wide limits such as the maximum number of queue
    /// pairs, completion queues, and memory regions, the maximum outstanding work requests and
    /// scatter/gather entries per queue, and the atomic capability. It dereferences to the raw
    /// [`ffi::ibv_device_attr`], so every field is accessible. Query these before creating
    /// resources to stay within what the device supports.
    ///
    /// # Errors
    ///
    ///  - [`QueryDevice`](Error::QueryDevice): `ibv_query_device` failed (`EINVAL` for invalid
    ///    arguments).
    pub fn query_device(&self) -> Result<DeviceAttr> {
        let mut device_attr = ffi::ibv_device_attr::default();
        let errno = unsafe { ffi::ibv_query_device(self.inner.ctx, &mut device_attr as *mut _) };
        if errno != 0 {
            return Err(Error::errno(errno, Error::QueryDevice));
        }
        Ok(DeviceAttr(device_attr))
    }

    /// Query the extended attributes and capabilities of this context's device
    /// (`ibv_query_device_ex`).
    ///
    /// The returned [`DeviceAttrEx`] carries everything [`query_device`](Self::query_device) does
    /// (its [`orig`](DeviceAttrEx::orig) holds the base [`ffi::ibv_device_attr`]) plus the extended
    /// capabilities that the base query cannot report: the completion-timestamp mask, the HCA core
    /// clock, the PCI atomic capabilities, the packet-pacing (rate-limit) limits, the raw-packet
    /// capabilities, and the maximum device-memory size. Providers that do not implement the
    /// extended verb fall back to the base attributes, and the extended fields read back as zero.
    ///
    /// # Errors
    ///
    ///  - [`QueryDevice`](Error::QueryDevice): `ibv_query_device_ex` failed (`EINVAL` for invalid
    ///    arguments).
    pub fn query_device_ex(&self) -> Result<DeviceAttrEx> {
        // `ibv_device_attr_ex` embeds unions, so it has no `Default`; all-zero is a valid start.
        let mut device_attr: ffi::ibv_device_attr_ex = unsafe { std::mem::zeroed() };
        let errno = unsafe {
            ffi::ibv_query_device_ex(self.inner.ctx, std::ptr::null(), &mut device_attr as *mut _)
        };
        if errno != 0 {
            return Err(Error::errno(errno, Error::QueryDevice));
        }
        Ok(DeviceAttrEx(device_attr))
    }

    /// Query the attributes of `port_num` on this context's device (`ibv_query_port`).
    ///
    /// Ports are numbered from 1. The returned [`PortAttr`] reports the port's state, its active and
    /// maximum MTU, its LID, its link layer, and its GID- and pkey-table lengths, with typed
    /// accessors for the state, MTU, speed, width, link layer, and physical state; it dereferences
    /// to the raw [`ffi::ibv_port_attr`] for everything else. Unlike the check performed when a
    /// queue pair is created on a port, this returns the attributes regardless of the port state.
    ///
    /// Port attributes are not constant (the subnet manager or the hardware may change them), so
    /// avoid caching the result for long.
    ///
    /// # Errors
    ///
    ///  - [`QueryPort`](Error::QueryPort): `ibv_query_port` failed (`EINVAL` for an invalid
    ///    `port_num`, `ENOMEM` when out of memory).
    pub fn query_port(&self, port_num: u8) -> Result<PortAttr> {
        let mut port_attr = ffi::ibv_port_attr::default();
        // The shim (rdma-core's `___ibv_query_port` inline) also fills the extended fields, such
        // as `active_speed_ex`, which the exported compat `ibv_query_port` symbol leaves zeroed.
        let errno = unsafe { ffi::___ibv_query_port(self.inner.ctx, port_num, &mut port_attr) };
        if errno != 0 {
            return Err(Error::errno(errno, |e| Error::QueryPort {
                port_num,
                source: e,
            }));
        }
        Ok(PortAttr(port_attr))
    }

    /// Returns the underlying `ibv_context` pointer.
    ///
    /// This is an escape hatch for calling libibverbs verbs that this crate does not yet wrap. The
    /// pointer is owned by this [`Context`] and stays valid only while it (or a resource derived
    /// from it) is alive; do not close it or use it past the owner's lifetime.
    pub fn as_raw(&self) -> *mut ffi::ibv_context {
        self.inner.ctx
    }

    /// Read the device's current free-running hardware clock (`ibv_query_rt_values_ex`).
    ///
    /// The returned [`HcaClock`] is a raw tick count, not a time: it is the same time base that
    /// [`WorkCompletion::completion_timestamp`] reports its timestamps in, so sampling it lets you
    /// relate completion timestamps to host time (convert tick deltas via
    /// [`DeviceAttrEx::hca_core_clock_khz`]).
    ///
    /// # Errors
    ///
    ///  - [`Unsupported`](Error::Unsupported): the device does not support querying real-time
    ///    values (`EOPNOTSUPP`).
    ///  - [`QueryRealTimeValues`](Error::QueryRealTimeValues): `ibv_query_rt_values_ex` failed
    ///    (every non-`EOPNOTSUPP` errno).
    pub fn query_rt_values_ex(&self) -> Result<HcaClock> {
        // SAFETY: `ibv_values_ex` is a plain C struct (a mask plus a `timespec`); all-zero is a valid
        // initial value.
        let mut values: ffi::ibv_values_ex = unsafe { std::mem::zeroed() };
        values.comp_mask = ffi::ibv_values_mask::IBV_VALUES_MASK_RAW_CLOCK as u32;
        let errno = unsafe { ffi::ibv_query_rt_values_ex(self.inner.ctx, &mut values as *mut _) };
        if errno != 0 {
            return Err(Error::errno(errno, Error::QueryRealTimeValues));
        }
        // The C ABI reports the raw clock through a `timespec`, but the value is a tick count, not
        // a time (mlx5, for instance, returns the whole counter through `tv_nsec`); fold the two
        // fields back into the single 64-bit counter.
        Ok(HcaClock(
            (values.raw_clock.tv_sec as u64)
                .wrapping_mul(1_000_000_000)
                .wrapping_add(values.raw_clock.tv_nsec as u64),
        ))
    }
}

/// An asynchronous event reported by the device, consumed with [`Context::poll_async_event`] /
/// [`Context::wait_async_event`] and acknowledged automatically when dropped.
///
/// Drop events promptly: libibverbs blocks the destruction of the object an event refers to
/// until the event is acknowledged, so holding an `AsyncEvent` while dropping the completion
/// queue, queue pair, or shared receive queue it refers to deadlocks.
pub struct AsyncEvent<'ctx> {
    event: ffi::ibv_async_event,
    /// The event must be acknowledged before the device context closes, so it borrows the
    /// [`Context`] it was read from.
    _ctx: PhantomData<&'ctx Context>,
}

impl AsyncEvent<'_> {
    /// The kind of event.
    pub fn event_type(&self) -> AsyncEventType {
        self.event.event_type.into()
    }

    /// The port the event refers to, for the port-scoped events
    /// ([`PortActive`](AsyncEventType::PortActive), [`PortError`](AsyncEventType::PortError),
    /// [`LidChange`](AsyncEventType::LidChange), [`PkeyChange`](AsyncEventType::PkeyChange),
    /// [`SmChange`](AsyncEventType::SmChange),
    /// [`ClientReregister`](AsyncEventType::ClientReregister), and
    /// [`GidChange`](AsyncEventType::GidChange)); `None` for every other kind.
    pub fn port_num(&self) -> Option<u8> {
        match self.event_type() {
            AsyncEventType::PortActive
            | AsyncEventType::PortError
            | AsyncEventType::LidChange
            | AsyncEventType::PkeyChange
            | AsyncEventType::SmChange
            | AsyncEventType::ClientReregister
            | AsyncEventType::GidChange => {
                // SAFETY: for the port-scoped events, the element union holds the port number.
                Some(unsafe { self.event.element.port_num } as u8)
            }
            _ => None,
        }
    }

    /// The underlying `ibv_async_event`.
    ///
    /// This is the escape hatch for the `element` union: for CQ-, QP-, and SRQ-scoped events it
    /// holds the raw `ibv_cq`/`ibv_qp`/`ibv_srq` pointer the event refers to. Those pointers
    /// cannot be mapped back to this crate's safe wrappers; compare them against the `as_raw`
    /// handles of the wrappers you own to identify the object.
    pub fn as_raw(&self) -> &ffi::ibv_async_event {
        &self.event
    }
}

impl Drop for AsyncEvent<'_> {
    fn drop(&mut self) {
        // Every event from `ibv_get_async_event` must be acknowledged exactly once; the object
        // it refers to cannot be destroyed until then.
        unsafe { ffi::ibv_ack_async_event(&mut self.event) };
    }
}

impl fmt::Debug for AsyncEvent<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut f = f.debug_struct("AsyncEvent");
        f.field("event_type", &self.event_type());
        if let Some(port_num) = self.port_num() {
            f.field("port_num", &port_num);
        }
        f.finish_non_exhaustive()
    }
}

/// The kind of a device asynchronous event. Returned by [`AsyncEvent::event_type`].
///
/// The events fall into three scopes: affiliated errors and state changes on a completion queue,
/// queue pair, or shared receive queue (the [`AsyncEvent::as_raw`] element identifies which),
/// port-level changes (with [`AsyncEvent::port_num`]), and device-wide ("unaffiliated") failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AsyncEventType {
    /// An error occurred on a completion queue (overrun or protection fault); the queue and the
    /// queue pairs attached to it are unusable.
    CqError,
    /// An error occurred on a queue pair that its completion queues could not report; the queue
    /// pair moved to the error state.
    QpFatal,
    /// The transport detected an invalid request on the queue pair while it was the responder.
    QpRequestError,
    /// The transport detected an access violation on the queue pair while it was the responder.
    QpAccessError,
    /// The first message arrived on a queue pair still in `RTR` (communication is established).
    CommEstablished,
    /// The send queue finished draining after a transition to `SQD`.
    SqDrained,
    /// The connection migrated to its alternate path.
    PathMigrated,
    /// The connection failed to migrate to its alternate path.
    PathMigrationError,
    /// The device is in a fatal state; all of its resources are unusable.
    DeviceFatal,
    /// The port's logical state became active.
    PortActive,
    /// The port's logical state left active.
    PortError,
    /// The subnet manager changed the port's LID.
    LidChange,
    /// The port's partition-key (P_Key) table changed.
    PkeyChange,
    /// A new subnet manager took over the port.
    SmChange,
    /// An error occurred on a shared receive queue.
    SrqError,
    /// The number of receives posted to a shared receive queue dropped below its low watermark
    /// (the `srq_limit` of [`ProtectionDomain::create_srq`]).
    SrqLimitReached,
    /// The last work request reached a queue pair, attached to a shared receive queue, that is in
    /// the error state: no more receives will be consumed from the SRQ by this queue pair.
    QpLastWqeReached,
    /// The subnet manager asked the port's clients to reregister their subscriptions.
    ClientReregister,
    /// The port's GID table changed.
    GidChange,
    /// An error occurred on a work queue.
    WqFatal,
    /// The device's link speed changed.
    DeviceSpeedChange,
}

impl From<ffi::ibv_event_type> for AsyncEventType {
    fn from(event: ffi::ibv_event_type) -> Self {
        use ffi::ibv_event_type::*;
        match event {
            IBV_EVENT_CQ_ERR => AsyncEventType::CqError,
            IBV_EVENT_QP_FATAL => AsyncEventType::QpFatal,
            IBV_EVENT_QP_REQ_ERR => AsyncEventType::QpRequestError,
            IBV_EVENT_QP_ACCESS_ERR => AsyncEventType::QpAccessError,
            IBV_EVENT_COMM_EST => AsyncEventType::CommEstablished,
            IBV_EVENT_SQ_DRAINED => AsyncEventType::SqDrained,
            IBV_EVENT_PATH_MIG => AsyncEventType::PathMigrated,
            IBV_EVENT_PATH_MIG_ERR => AsyncEventType::PathMigrationError,
            IBV_EVENT_DEVICE_FATAL => AsyncEventType::DeviceFatal,
            IBV_EVENT_PORT_ACTIVE => AsyncEventType::PortActive,
            IBV_EVENT_PORT_ERR => AsyncEventType::PortError,
            IBV_EVENT_LID_CHANGE => AsyncEventType::LidChange,
            IBV_EVENT_PKEY_CHANGE => AsyncEventType::PkeyChange,
            IBV_EVENT_SM_CHANGE => AsyncEventType::SmChange,
            IBV_EVENT_SRQ_ERR => AsyncEventType::SrqError,
            IBV_EVENT_SRQ_LIMIT_REACHED => AsyncEventType::SrqLimitReached,
            IBV_EVENT_QP_LAST_WQE_REACHED => AsyncEventType::QpLastWqeReached,
            IBV_EVENT_CLIENT_REREGISTER => AsyncEventType::ClientReregister,
            IBV_EVENT_GID_CHANGE => AsyncEventType::GidChange,
            IBV_EVENT_WQ_FATAL => AsyncEventType::WqFatal,
            IBV_EVENT_DEVICE_SPEED_CHANGE => AsyncEventType::DeviceSpeedChange,
        }
    }
}

impl From<AsyncEventType> for ffi::ibv_event_type {
    fn from(event: AsyncEventType) -> Self {
        use ffi::ibv_event_type::*;
        match event {
            AsyncEventType::CqError => IBV_EVENT_CQ_ERR,
            AsyncEventType::QpFatal => IBV_EVENT_QP_FATAL,
            AsyncEventType::QpRequestError => IBV_EVENT_QP_REQ_ERR,
            AsyncEventType::QpAccessError => IBV_EVENT_QP_ACCESS_ERR,
            AsyncEventType::CommEstablished => IBV_EVENT_COMM_EST,
            AsyncEventType::SqDrained => IBV_EVENT_SQ_DRAINED,
            AsyncEventType::PathMigrated => IBV_EVENT_PATH_MIG,
            AsyncEventType::PathMigrationError => IBV_EVENT_PATH_MIG_ERR,
            AsyncEventType::DeviceFatal => IBV_EVENT_DEVICE_FATAL,
            AsyncEventType::PortActive => IBV_EVENT_PORT_ACTIVE,
            AsyncEventType::PortError => IBV_EVENT_PORT_ERR,
            AsyncEventType::LidChange => IBV_EVENT_LID_CHANGE,
            AsyncEventType::PkeyChange => IBV_EVENT_PKEY_CHANGE,
            AsyncEventType::SmChange => IBV_EVENT_SM_CHANGE,
            AsyncEventType::SrqError => IBV_EVENT_SRQ_ERR,
            AsyncEventType::SrqLimitReached => IBV_EVENT_SRQ_LIMIT_REACHED,
            AsyncEventType::QpLastWqeReached => IBV_EVENT_QP_LAST_WQE_REACHED,
            AsyncEventType::ClientReregister => IBV_EVENT_CLIENT_REREGISTER,
            AsyncEventType::GidChange => IBV_EVENT_GID_CHANGE,
            AsyncEventType::WqFatal => IBV_EVENT_WQ_FATAL,
            AsyncEventType::DeviceSpeedChange => IBV_EVENT_DEVICE_SPEED_CHANGE,
        }
    }
}

impl std::fmt::Display for AsyncEventType {
    /// Formats the event as it is named in the C headers, for example `SRQ_LIMIT_REACHED` for
    /// [`SrqLimitReached`](Self::SrqLimitReached).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            AsyncEventType::CqError => "CQ_ERR",
            AsyncEventType::QpFatal => "QP_FATAL",
            AsyncEventType::QpRequestError => "QP_REQ_ERR",
            AsyncEventType::QpAccessError => "QP_ACCESS_ERR",
            AsyncEventType::CommEstablished => "COMM_EST",
            AsyncEventType::SqDrained => "SQ_DRAINED",
            AsyncEventType::PathMigrated => "PATH_MIG",
            AsyncEventType::PathMigrationError => "PATH_MIG_ERR",
            AsyncEventType::DeviceFatal => "DEVICE_FATAL",
            AsyncEventType::PortActive => "PORT_ACTIVE",
            AsyncEventType::PortError => "PORT_ERR",
            AsyncEventType::LidChange => "LID_CHANGE",
            AsyncEventType::PkeyChange => "PKEY_CHANGE",
            AsyncEventType::SmChange => "SM_CHANGE",
            AsyncEventType::SrqError => "SRQ_ERR",
            AsyncEventType::SrqLimitReached => "SRQ_LIMIT_REACHED",
            AsyncEventType::QpLastWqeReached => "QP_LAST_WQE_REACHED",
            AsyncEventType::ClientReregister => "CLIENT_REREGISTER",
            AsyncEventType::GidChange => "GID_CHANGE",
            AsyncEventType::WqFatal => "WQ_FATAL",
            AsyncEventType::DeviceSpeedChange => "DEVICE_SPEED_CHANGE",
        };
        f.write_str(name)
    }
}

/// A reading of the device's free-running hardware clock (the "HCA core clock"), in raw ticks.
///
/// Returned by [`Context::query_rt_values_ex`] and
/// [`WorkCompletion::completion_timestamp`], so clock samples and completion timestamps can be
/// compared directly. A tick is *not* a unit of time: convert a tick delta (the [`Sub`] impl, or
/// [`ticks`](Self::ticks)) to time using the device's clock frequency,
/// [`DeviceAttrEx::hca_core_clock_khz`].
///
/// [`Sub`]: std::ops::Sub
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct HcaClock(pub(crate) u64);

impl HcaClock {
    /// The raw tick count.
    pub fn ticks(&self) -> u64 {
        self.0
    }
}

impl std::ops::Sub for HcaClock {
    type Output = u64;

    /// The number of ticks from `rhs` to `self`.
    fn sub(self, rhs: HcaClock) -> u64 {
        self.0 - rhs.0
    }
}

/// A path or port MTU (maximum transfer unit), the message fragment size on the wire.
///
/// Returned by [`PortAttr::active_mtu`] / [`PortAttr::max_mtu`], and set on a queue pair with
/// [`QueuePairBuilder::set_path_mtu`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Mtu {
    /// 256 bytes.
    Mtu256,
    /// 512 bytes.
    Mtu512,
    /// 1024 bytes.
    Mtu1024,
    /// 2048 bytes.
    Mtu2048,
    /// 4096 bytes.
    Mtu4096,
}

impl Mtu {
    /// The MTU in bytes.
    pub fn bytes(self) -> usize {
        match self {
            Mtu::Mtu256 => 256,
            Mtu::Mtu512 => 512,
            Mtu::Mtu1024 => 1024,
            Mtu::Mtu2048 => 2048,
            Mtu::Mtu4096 => 4096,
        }
    }
}

impl From<ffi::ibv_mtu> for Mtu {
    fn from(mtu: ffi::ibv_mtu) -> Self {
        match mtu {
            ffi::ibv_mtu::IBV_MTU_256 => Mtu::Mtu256,
            ffi::ibv_mtu::IBV_MTU_512 => Mtu::Mtu512,
            ffi::ibv_mtu::IBV_MTU_1024 => Mtu::Mtu1024,
            ffi::ibv_mtu::IBV_MTU_2048 => Mtu::Mtu2048,
            ffi::ibv_mtu::IBV_MTU_4096 => Mtu::Mtu4096,
        }
    }
}

impl From<Mtu> for ffi::ibv_mtu {
    fn from(mtu: Mtu) -> Self {
        match mtu {
            Mtu::Mtu256 => ffi::ibv_mtu::IBV_MTU_256,
            Mtu::Mtu512 => ffi::ibv_mtu::IBV_MTU_512,
            Mtu::Mtu1024 => ffi::ibv_mtu::IBV_MTU_1024,
            Mtu::Mtu2048 => ffi::ibv_mtu::IBV_MTU_2048,
            Mtu::Mtu4096 => ffi::ibv_mtu::IBV_MTU_4096,
        }
    }
}

impl fmt::Display for Mtu {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.bytes())
    }
}

/// The logical state of a port. Returned by [`PortAttr::state`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PortState {
    /// Reserved value (no state change).
    Nop,
    /// The port is down.
    Down,
    /// The port is initializing: the link is up, but the subnet manager has not configured it yet.
    Init,
    /// The port is armed: it may receive, but not yet transmit, data packets.
    Armed,
    /// The port is active and may send and receive packets.
    Active,
    /// The port is active, but temporarily deferring packet transmission.
    ActiveDefer,
}

impl From<ffi::ibv_port_state> for PortState {
    fn from(state: ffi::ibv_port_state) -> Self {
        match state {
            ffi::ibv_port_state::IBV_PORT_NOP => PortState::Nop,
            ffi::ibv_port_state::IBV_PORT_DOWN => PortState::Down,
            ffi::ibv_port_state::IBV_PORT_INIT => PortState::Init,
            ffi::ibv_port_state::IBV_PORT_ARMED => PortState::Armed,
            ffi::ibv_port_state::IBV_PORT_ACTIVE => PortState::Active,
            ffi::ibv_port_state::IBV_PORT_ACTIVE_DEFER => PortState::ActiveDefer,
        }
    }
}

impl From<PortState> for ffi::ibv_port_state {
    fn from(state: PortState) -> Self {
        match state {
            PortState::Nop => ffi::ibv_port_state::IBV_PORT_NOP,
            PortState::Down => ffi::ibv_port_state::IBV_PORT_DOWN,
            PortState::Init => ffi::ibv_port_state::IBV_PORT_INIT,
            PortState::Armed => ffi::ibv_port_state::IBV_PORT_ARMED,
            PortState::Active => ffi::ibv_port_state::IBV_PORT_ACTIVE,
            PortState::ActiveDefer => ffi::ibv_port_state::IBV_PORT_ACTIVE_DEFER,
        }
    }
}

impl fmt::Display for PortState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            PortState::Nop => "Nop",
            PortState::Down => "Down",
            PortState::Init => "Init",
            PortState::Armed => "Armed",
            PortState::Active => "Active",
            PortState::ActiveDefer => "ActiveDefer",
        };
        f.write_str(name)
    }
}

/// The signaling rate of a port's active link, decoded from `ibv_port_attr::active_speed` (or
/// `active_speed_ex`, where the provider reports the speeds the legacy 8-bit field cannot, such
/// as XDR).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PortSpeed {
    /// Single data rate (2.5 Gb/s signaling per lane).
    Sdr,
    /// Double data rate (5 Gb/s signaling per lane).
    Ddr,
    /// Quad data rate (10 Gb/s signaling per lane).
    Qdr,
    /// FDR10 (10.3125 Gb/s signaling per lane).
    Fdr10,
    /// Fourteen data rate (14.0625 Gb/s signaling per lane).
    Fdr,
    /// Enhanced data rate (25.78125 Gb/s signaling per lane).
    Edr,
    /// High data rate (53.125 Gb/s signaling per lane).
    Hdr,
    /// Next data rate (106.25 Gb/s signaling per lane).
    Ndr,
    /// Extreme data rate (212.5 Gb/s signaling per lane).
    Xdr,
    /// A value this crate does not recognize.
    Unknown(u32),
}

impl PortSpeed {
    fn from_active_speed(speed: u32) -> Self {
        match speed {
            1 => PortSpeed::Sdr,
            2 => PortSpeed::Ddr,
            4 => PortSpeed::Qdr,
            8 => PortSpeed::Fdr10,
            16 => PortSpeed::Fdr,
            32 => PortSpeed::Edr,
            64 => PortSpeed::Hdr,
            128 => PortSpeed::Ndr,
            256 => PortSpeed::Xdr,
            other => PortSpeed::Unknown(other),
        }
    }

    /// The effective data rate of a single (1x) lane in gigabits per second, accounting for the
    /// link's encoding overhead. Returns `None` for an unrecognized speed.
    pub fn lane_gbps(self) -> Option<f64> {
        Some(match self {
            PortSpeed::Sdr => 2.0,
            PortSpeed::Ddr => 4.0,
            PortSpeed::Qdr => 8.0,
            PortSpeed::Fdr10 => 10.0,
            PortSpeed::Fdr => 13.64,
            PortSpeed::Edr => 25.0,
            PortSpeed::Hdr => 50.0,
            PortSpeed::Ndr => 100.0,
            PortSpeed::Xdr => 200.0,
            PortSpeed::Unknown(_) => return None,
        })
    }
}

impl fmt::Display for PortSpeed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            PortSpeed::Sdr => "SDR",
            PortSpeed::Ddr => "DDR",
            PortSpeed::Qdr => "QDR",
            PortSpeed::Fdr10 => "FDR10",
            PortSpeed::Fdr => "FDR",
            PortSpeed::Edr => "EDR",
            PortSpeed::Hdr => "HDR",
            PortSpeed::Ndr => "NDR",
            PortSpeed::Xdr => "XDR",
            PortSpeed::Unknown(raw) => return write!(f, "unknown ({raw})"),
        };
        f.write_str(name)
    }
}

/// The width (number of lanes) of a port's active link, decoded from `ibv_port_attr::active_width`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PortWidth {
    /// A single lane.
    Width1x,
    /// Four lanes.
    Width4x,
    /// Eight lanes.
    Width8x,
    /// Twelve lanes.
    Width12x,
    /// Two lanes.
    Width2x,
    /// A value this crate does not recognize.
    Unknown(u8),
}

impl PortWidth {
    fn from_active_width(width: u8) -> Self {
        match width {
            1 => PortWidth::Width1x,
            2 => PortWidth::Width4x,
            4 => PortWidth::Width8x,
            8 => PortWidth::Width12x,
            16 => PortWidth::Width2x,
            other => PortWidth::Unknown(other),
        }
    }

    /// The number of lanes, or `None` for an unrecognized width.
    pub fn lanes(self) -> Option<u8> {
        Some(match self {
            PortWidth::Width1x => 1,
            PortWidth::Width2x => 2,
            PortWidth::Width4x => 4,
            PortWidth::Width8x => 8,
            PortWidth::Width12x => 12,
            PortWidth::Unknown(_) => return None,
        })
    }
}

impl fmt::Display for PortWidth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PortWidth::Unknown(raw) => write!(f, "unknown ({raw})"),
            width => write!(
                f,
                "{}x",
                width.lanes().expect("non-Unknown width has lanes")
            ),
        }
    }
}

/// The link layer of a port, decoded from `ibv_port_attr::link_layer`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum LinkLayer {
    /// The link layer is unspecified.
    Unspecified,
    /// An InfiniBand link.
    InfiniBand,
    /// An Ethernet link (used by RoCE).
    Ethernet,
    /// A value this crate does not recognize.
    Unknown(u8),
}

impl LinkLayer {
    fn from_raw(link_layer: u8) -> Self {
        // IBV_LINK_LAYER_UNSPECIFIED = 0, IBV_LINK_LAYER_INFINIBAND = 1, IBV_LINK_LAYER_ETHERNET = 2.
        match link_layer {
            0 => LinkLayer::Unspecified,
            1 => LinkLayer::InfiniBand,
            2 => LinkLayer::Ethernet,
            other => LinkLayer::Unknown(other),
        }
    }
}

impl fmt::Display for LinkLayer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            LinkLayer::Unspecified => "unspecified",
            LinkLayer::InfiniBand => "InfiniBand",
            LinkLayer::Ethernet => "Ethernet",
            LinkLayer::Unknown(raw) => return write!(f, "unknown ({raw})"),
        };
        f.write_str(name)
    }
}

/// The physical state of a port, decoded from `ibv_port_attr::phys_state`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PhysicalState {
    /// The port is sleeping.
    Sleep,
    /// The port is polling for a peer.
    Polling,
    /// The port is administratively disabled.
    Disabled,
    /// The port is training its configuration.
    PortConfigurationTraining,
    /// The physical link is up.
    LinkUp,
    /// The link is recovering from an error.
    LinkErrorRecovery,
    /// The port is running a PHY test.
    PhyTest,
    /// A value this crate does not recognize.
    Unknown(u8),
}

impl PhysicalState {
    fn from_raw(phys_state: u8) -> Self {
        match phys_state {
            1 => PhysicalState::Sleep,
            2 => PhysicalState::Polling,
            3 => PhysicalState::Disabled,
            4 => PhysicalState::PortConfigurationTraining,
            5 => PhysicalState::LinkUp,
            6 => PhysicalState::LinkErrorRecovery,
            7 => PhysicalState::PhyTest,
            other => PhysicalState::Unknown(other),
        }
    }
}

impl fmt::Display for PhysicalState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            PhysicalState::Sleep => "Sleep",
            PhysicalState::Polling => "Polling",
            PhysicalState::Disabled => "Disabled",
            PhysicalState::PortConfigurationTraining => "PortConfigurationTraining",
            PhysicalState::LinkUp => "LinkUp",
            PhysicalState::LinkErrorRecovery => "LinkErrorRecovery",
            PhysicalState::PhyTest => "PhyTest",
            PhysicalState::Unknown(raw) => return write!(f, "unknown ({raw})"),
        };
        f.write_str(name)
    }
}

/// Device-wide attributes and capabilities, as returned by [`Context::query_device`].
///
/// Dereferences to the raw [`ffi::ibv_device_attr`], so every field is accessible; the inherent
/// methods add typed accessors for the device identifiers.
#[derive(Clone)]
pub struct DeviceAttr(ffi::ibv_device_attr);

impl DeviceAttr {
    /// The node GUID of the device.
    pub fn node_guid(&self) -> Guid {
        Guid::from_be64(self.0.node_guid)
    }

    /// The system-image GUID, shared by the ports of the same physical device.
    pub fn sys_image_guid(&self) -> Guid {
        Guid::from_be64(self.0.sys_image_guid)
    }

    /// The device's firmware version, decoded from the fixed-size `fw_ver` C string. Borrows when it
    /// is valid UTF-8 (the usual case) and allocates only to replace invalid bytes.
    pub fn fw_ver(&self) -> Cow<'_, str> {
        // SAFETY: `fw_ver` is a NUL-terminated C string embedded in the attributes; the borrow is
        // tied to `&self`, so the array outlives the returned `CStr`.
        unsafe { CStr::from_ptr(self.0.fw_ver.as_ptr()) }.to_string_lossy()
    }

    /// The underlying `ibv_device_attr`. Escape hatch for fields this crate does not wrap.
    pub fn as_raw(&self) -> &ffi::ibv_device_attr {
        &self.0
    }
}

impl fmt::Debug for DeviceAttr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeviceAttr")
            .field("node_guid", &self.node_guid())
            .field("sys_image_guid", &self.sys_image_guid())
            .field("fw_ver", &self.fw_ver())
            .field("vendor_id", &format_args!("{:#06x}", self.0.vendor_id))
            .field("vendor_part_id", &self.0.vendor_part_id)
            .field("phys_port_cnt", &self.0.phys_port_cnt)
            .field("max_qp", &self.0.max_qp)
            .field("max_cq", &self.0.max_cq)
            .field("max_mr", &self.0.max_mr)
            .finish_non_exhaustive()
    }
}

impl Deref for DeviceAttr {
    type Target = ffi::ibv_device_attr;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Extended device-wide attributes and capabilities, as returned by [`Context::query_device_ex`].
///
/// Dereferences to the raw [`ffi::ibv_device_attr_ex`], so every field is accessible; the inherent
/// methods add typed accessors for the most useful extended capabilities, and [`orig`] returns the
/// base attributes that [`Context::query_device`] reports.
///
/// [`orig`]: DeviceAttrEx::orig
#[derive(Clone)]
pub struct DeviceAttrEx(ffi::ibv_device_attr_ex);

impl DeviceAttrEx {
    /// The base device attributes, the same set [`Context::query_device`] returns.
    pub fn orig(&self) -> DeviceAttr {
        DeviceAttr(self.0.orig_attr)
    }

    /// The node GUID of the device.
    pub fn node_guid(&self) -> Guid {
        Guid::from_be64(self.0.orig_attr.node_guid)
    }

    /// The system-image GUID, shared by the ports of the same physical device.
    pub fn sys_image_guid(&self) -> Guid {
        Guid::from_be64(self.0.orig_attr.sys_image_guid)
    }

    /// The mask that bounds the device's completion timestamps: the free-running HCA clock that
    /// [`WorkCompletion::completion_timestamp`] reports wraps at this value. Zero if the device does
    /// not support completion timestamps.
    pub fn completion_timestamp_mask(&self) -> u64 {
        self.0.completion_timestamp_mask
    }

    /// The HCA core-clock frequency in kHz, or zero if the device does not report it. Together with
    /// [`Context::query_rt_values_ex`] this relates raw completion timestamps to host time.
    pub fn hca_core_clock_khz(&self) -> u64 {
        self.0.hca_core_clock
    }

    /// The device's PCI atomic capabilities. Each field (`fetch_add`, `swap`, `compare_swap`) is a
    /// bitmask of the operand sizes, in bytes, the device can operate on atomically across PCIe.
    pub fn pci_atomic_caps(&self) -> ffi::ibv_pci_atomic_caps {
        self.0.pci_atomic_caps
    }

    /// The packet-pacing (rate-limit) capabilities: the supported rate range in kbps and the
    /// queue-pair types that can be rate limited. The minimum and maximum rate are zero if the
    /// device does not support packet pacing.
    pub fn packet_pacing_caps(&self) -> ffi::ibv_packet_pacing_caps {
        self.0.packet_pacing_caps
    }

    /// The raw-packet capability flags (`IBV_RAW_PACKET_CAP_*`) the device supports.
    pub fn raw_packet_caps(&self) -> u32 {
        self.0.raw_packet_caps
    }

    /// The maximum size, in bytes, of a single device-memory allocation, or zero if the device has
    /// no on-device memory.
    pub fn max_device_memory(&self) -> u64 {
        self.0.max_dm_size
    }

    /// The underlying `ibv_device_attr_ex`. Escape hatch for fields this crate does not wrap.
    pub fn as_raw(&self) -> &ffi::ibv_device_attr_ex {
        &self.0
    }
}

impl fmt::Debug for DeviceAttrEx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeviceAttrEx")
            .field("orig", &self.orig())
            .field(
                "completion_timestamp_mask",
                &format_args!("{:#x}", self.completion_timestamp_mask()),
            )
            .field("hca_core_clock_khz", &self.hca_core_clock_khz())
            .field("pci_atomic_caps", &self.pci_atomic_caps())
            .field("packet_pacing_caps", &self.packet_pacing_caps())
            .field(
                "raw_packet_caps",
                &format_args!("{:#x}", self.raw_packet_caps()),
            )
            .field("max_device_memory", &self.max_device_memory())
            .finish_non_exhaustive()
    }
}

impl Deref for DeviceAttrEx {
    type Target = ffi::ibv_device_attr_ex;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Per-port attributes, as returned by [`Context::query_port`].
///
/// Dereferences to the raw [`ffi::ibv_port_attr`], so every field is accessible; the inherent
/// methods add typed accessors for the state, MTU, speed, width, link layer, and physical state.
#[derive(Clone)]
pub struct PortAttr(ffi::ibv_port_attr);

impl PortAttr {
    /// The logical port state.
    pub fn state(&self) -> PortState {
        self.0.state.into()
    }

    /// The maximum MTU supported by this port.
    pub fn max_mtu(&self) -> Mtu {
        self.0.max_mtu.into()
    }

    /// The currently active MTU.
    pub fn active_mtu(&self) -> Mtu {
        self.0.active_mtu.into()
    }

    /// The active link speed.
    ///
    /// Read from the extended `active_speed_ex` field when the provider fills it (necessary for
    /// speeds beyond NDR, which overflow the legacy 8-bit field), falling back to the legacy
    /// `active_speed` otherwise.
    pub fn active_speed(&self) -> PortSpeed {
        match self.0.active_speed_ex {
            0 => PortSpeed::from_active_speed(self.0.active_speed as u32),
            ex => PortSpeed::from_active_speed(ex),
        }
    }

    /// The active link width.
    pub fn active_width(&self) -> PortWidth {
        PortWidth::from_active_width(self.0.active_width)
    }

    /// The link layer of the port.
    pub fn link_layer(&self) -> LinkLayer {
        LinkLayer::from_raw(self.0.link_layer)
    }

    /// The physical state of the port.
    pub fn phys_state(&self) -> PhysicalState {
        PhysicalState::from_raw(self.0.phys_state)
    }

    /// The underlying `ibv_port_attr`. Escape hatch for fields this crate does not wrap.
    pub fn as_raw(&self) -> &ffi::ibv_port_attr {
        &self.0
    }
}

impl fmt::Debug for PortAttr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PortAttr")
            .field("state", &self.state())
            .field("phys_state", &self.phys_state())
            .field("link_layer", &self.link_layer())
            .field("active_mtu", &self.active_mtu())
            .field("max_mtu", &self.max_mtu())
            .field("active_speed", &self.active_speed())
            .field("active_width", &self.active_width())
            .field("lid", &self.0.lid)
            .field("sm_lid", &self.0.sm_lid)
            .field("gid_tbl_len", &self.0.gid_tbl_len)
            .field("pkey_tbl_len", &self.0.pkey_tbl_len)
            .finish_non_exhaustive()
    }
}

impl Deref for PortAttr {
    type Target = ffi::ibv_port_attr;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod test_display {
    use super::*;

    #[test]
    fn port_attributes_display_human_names() {
        assert_eq!(PortSpeed::Edr.to_string(), "EDR");
        assert_eq!(PortSpeed::Unknown(7).to_string(), "unknown (7)");
        assert_eq!(PortWidth::Width4x.to_string(), "4x");
        assert_eq!(PortWidth::Unknown(9).to_string(), "unknown (9)");
        assert_eq!(LinkLayer::Ethernet.to_string(), "Ethernet");
        assert_eq!(PhysicalState::LinkUp.to_string(), "LinkUp");
    }

    #[test]
    fn mtu_roundtrip_and_bytes() {
        for (wrapper, raw, bytes) in [
            (Mtu::Mtu256, ffi::ibv_mtu::IBV_MTU_256, 256),
            (Mtu::Mtu1024, ffi::ibv_mtu::IBV_MTU_1024, 1024),
            (Mtu::Mtu4096, ffi::ibv_mtu::IBV_MTU_4096, 4096),
        ] {
            assert_eq!(Mtu::from(raw), wrapper);
            assert_eq!(ffi::ibv_mtu::from(wrapper), raw);
            assert_eq!(wrapper.bytes(), bytes);
            assert_eq!(wrapper.to_string(), bytes.to_string());
        }
    }

    #[test]
    fn port_state_roundtrip() {
        for (wrapper, raw) in [
            (PortState::Down, ffi::ibv_port_state::IBV_PORT_DOWN),
            (PortState::Active, ffi::ibv_port_state::IBV_PORT_ACTIVE),
            (
                PortState::ActiveDefer,
                ffi::ibv_port_state::IBV_PORT_ACTIVE_DEFER,
            ),
        ] {
            assert_eq!(PortState::from(raw), wrapper);
            assert_eq!(ffi::ibv_port_state::from(wrapper), raw);
        }
        assert_eq!(PortState::Active.to_string(), "Active");
    }

    #[test]
    fn async_event_type_roundtrip() {
        for (wrapper, raw) in [
            (
                AsyncEventType::CqError,
                ffi::ibv_event_type::IBV_EVENT_CQ_ERR,
            ),
            (
                AsyncEventType::CommEstablished,
                ffi::ibv_event_type::IBV_EVENT_COMM_EST,
            ),
            (
                AsyncEventType::SrqLimitReached,
                ffi::ibv_event_type::IBV_EVENT_SRQ_LIMIT_REACHED,
            ),
            (
                AsyncEventType::PortActive,
                ffi::ibv_event_type::IBV_EVENT_PORT_ACTIVE,
            ),
            (
                AsyncEventType::DeviceSpeedChange,
                ffi::ibv_event_type::IBV_EVENT_DEVICE_SPEED_CHANGE,
            ),
        ] {
            assert_eq!(AsyncEventType::from(raw), wrapper);
            assert_eq!(ffi::ibv_event_type::from(wrapper), raw);
        }
    }

    #[test]
    fn device_attr_ex_debug_labels() {
        // A zeroed value is valid (`query_device_ex` starts from one); check the Debug impl renders
        // the wrapper's labels and the nested base attributes.
        let attr = DeviceAttrEx(unsafe { std::mem::zeroed() });
        let s = format!("{attr:?}");
        assert!(s.contains("DeviceAttrEx"), "{s}");
        assert!(s.contains("completion_timestamp_mask"), "{s}");
        assert!(s.contains("DeviceAttr {"), "{s}");
    }
}
