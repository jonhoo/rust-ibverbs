use std::io;
use std::os::fd::{AsFd, BorrowedFd};
use std::os::raw::c_void;
use std::ptr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::context::ContextInner;
use crate::error::{Error, Result};

#[cfg(doc)]
use crate::{ibv_create_cq_wc_flags, Context};

/// A completion channel: the file descriptor that delivers completion-queue notifications.
///
/// By default a [`CompletionQueue`] has no channel and is driven by polling alone. To wait for
/// completions instead of burning a core, create a channel with [`Context::create_comp_channel`]
/// and build the queue on it with [`CompletionQueueBuilder::set_comp_channel`]; then arm the queue
/// with [`CompletionQueue::req_notify`], drain it with [`poll`](CompletionQueue::poll), and block
/// on the channel ([`wait`](Self::wait)) — or watch its descriptor
/// ([`AsFd`]/[`AsRawFd`](std::os::fd::AsRawFd)) from your own reactor and consume notifications
/// with [`get_event`](Self::get_event) — before draining and re-arming again.
///
/// Any number of queues can be built on one channel, collapsing their notifications onto a single
/// file descriptor (what you want for a server driving many queue pairs from one `epoll`/reactor).
/// A notification carries the context value of the queue it belongs to (set with
/// [`CompletionQueueBuilder::set_context`]), which is how [`get_event`](Self::get_event) tells the
/// sharers apart — give each queue a distinct one. The channel is a single stream of events:
/// however many threads consume it, each notification is delivered to exactly one of them, so
/// routing it to the right queue is the consumer's job.
///
/// Cloning is cheap (reference counted); the channel is destroyed once the last clone and every queue
/// built on it are dropped.
#[derive(Clone)]
pub struct CompletionChannel {
    inner: Arc<CompletionChannelInner>,
}

struct CompletionChannelInner {
    // Kept so the device outlives the channel.
    _ctx: Arc<ContextInner>,
    cc: *mut ffi::ibv_comp_channel,
}

unsafe impl Send for CompletionChannelInner {}
unsafe impl Sync for CompletionChannelInner {}

impl Drop for CompletionChannelInner {
    fn drop(&mut self) {
        let errno = unsafe { ffi::ibv_destroy_comp_channel(self.cc) };
        if errno != 0 {
            let e = io::Error::from_raw_os_error(errno);
            panic!("{e}");
        }
    }
}

impl CompletionChannel {
    /// Create a completion channel on `ctx` (for [`Context::create_comp_channel`]), with its file
    /// descriptor set non-blocking so [`get_event`](Self::get_event) reports an empty channel
    /// instead of blocking.
    pub(crate) fn new(ctx: &Arc<ContextInner>) -> Result<CompletionChannel> {
        let cc = unsafe { ffi::ibv_create_comp_channel(ctx.ctx) };
        if cc.is_null() {
            return Err(Error::CreateCompletionChannel(io::Error::last_os_error()));
        }
        let channel = CompletionChannel {
            inner: Arc::new(CompletionChannelInner {
                _ctx: ctx.clone(),
                cc,
            }),
        };
        // If this fails, `channel` drops here and tears the half-created channel back down.
        channel.set_nonblocking()?;
        Ok(channel)
    }

    /// Set this channel's file descriptor to non-blocking.
    fn set_nonblocking(&self) -> Result<()> {
        let fd = unsafe { *self.inner.cc }.fd;
        let flags = nix::fcntl::fcntl(fd, nix::fcntl::F_GETFL)
            .map_err(|e| Error::CreateCompletionChannel(e.into()))?;
        let arg = nix::fcntl::FcntlArg::F_SETFL(
            nix::fcntl::OFlag::from_bits_retain(flags) | nix::fcntl::OFlag::O_NONBLOCK,
        );
        nix::fcntl::fcntl(fd, arg).map_err(|e| Error::CreateCompletionChannel(e.into()))?;
        Ok(())
    }

    /// Consume one pending notification from the channel, returning the context value of the
    /// completion queue it belongs to (the value set with [`CompletionQueueBuilder::set_context`]),
    /// or `None` if none is pending.
    ///
    /// This is how you demultiplex several queues that share one channel: after the channel's file
    /// descriptor becomes readable, drain notifications here and map each returned context back to
    /// the queue, which you then [`poll`](CompletionQueue::poll) and re-arm with
    /// [`req_notify`](CompletionQueue::req_notify). Give each queue a distinct
    /// [`set_context`](CompletionQueueBuilder::set_context) value so they can be told apart.
    /// Acknowledgement is handled for you.
    pub fn get_event(&self) -> Result<Option<isize>> {
        let mut out_cq = ptr::null_mut();
        let mut out_cq_context = ptr::null_mut();
        let rc = unsafe { ffi::ibv_get_cq_event(self.inner.cc, &mut out_cq, &mut out_cq_context) };
        if rc < 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::WouldBlock {
                return Ok(None);
            }
            return Err(Error::PollCompletionQueue(e));
        }
        // Every event from ibv_get_cq_event() must eventually be acknowledged.
        unsafe { ffi::ibv_ack_cq_events(out_cq, 1) };
        Ok(Some(out_cq_context as isize))
    }

    /// Block until a notification is available on the channel (up to `timeout`), then consume it,
    /// returning the context value of the completion queue it belongs to (the value set with
    /// [`CompletionQueueBuilder::set_context`]). Returns `None` only if `timeout` elapses first;
    /// with no timeout it waits indefinitely, even if other threads race it for notifications.
    ///
    /// This is the blocking form of [`get_event`](Self::get_event): it waits on the channel's file
    /// descriptor for you rather than requiring an external reactor. Arm each queue with
    /// [`CompletionQueue::req_notify`] and drain it with [`poll`](CompletionQueue::poll) *before*
    /// blocking here — polling after arming closes the race where a completion lands between an
    /// earlier poll and arming — then call this to learn which queue fired, and poll and re-arm
    /// that queue.
    pub fn wait(&self, timeout: Option<Duration>) -> Result<Option<isize>> {
        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        loop {
            let remaining =
                deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()));
            let pollfd = nix::poll::PollFd::new(self.as_fd(), nix::poll::PollFlags::POLLIN);
            let ret = nix::poll::poll(
                &mut [pollfd],
                remaining
                    .map(nix::poll::PollTimeout::try_from)
                    .transpose()
                    .map_err(|_| {
                        Error::PollCompletionQueue(io::Error::other(
                            "failed to convert timeout to PollTimeout",
                        ))
                    })?,
            )
            .map_err(|e| Error::PollCompletionQueue(e.into()))?;
            match ret {
                0 => return Ok(None),
                1 => {
                    // The descriptor was readable, but another thread may have consumed the
                    // notification first; if so, go back to waiting for the next one.
                    if let Some(context) = self.get_event()? {
                        return Ok(Some(context));
                    }
                }
                _ => unreachable!("we passed 1 fd to poll, but it returned {ret}"),
            }
        }
    }

    /// Returns the underlying `ibv_comp_channel` pointer.
    ///
    /// This is an escape hatch for verbs this crate does not yet wrap. The pointer stays valid only
    /// while a clone of this [`CompletionChannel`] (or a queue built on it) is alive; do not destroy
    /// it.
    pub fn as_raw(&self) -> *mut ffi::ibv_comp_channel {
        self.inner.cc
    }
}

impl std::os::fd::AsRawFd for CompletionChannel {
    /// The raw file descriptor of this completion channel. It is non-blocking and becomes readable
    /// when a notification arrives for any queue built on it after
    /// [`req_notify`](CompletionQueue::req_notify).
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        unsafe { *self.inner.cc }.fd
    }
}

impl AsFd for CompletionChannel {
    fn as_fd(&self) -> BorrowedFd<'_> {
        // SAFETY: the channel fd lives until the last `CompletionChannelInner` drops, and the borrow
        // is tied to `&self`.
        unsafe { BorrowedFd::borrow_raw((*self.inner.cc).fd) }
    }
}

/// Builds a [`CompletionQueue`]. Created by [`Context::create_cq`].
///
/// The queue size is fixed when the builder is created; everything else is optional and defaults to
/// a plain polled queue with the standard work-completion fields and no completion channel. Call
/// [`build`](Self::build) to create the queue.
#[must_use]
pub struct CompletionQueueBuilder {
    pub(crate) ctx: Arc<ContextInner>,
    pub(crate) min_cq_entries: i32,
    pub(crate) cq_context: isize,
    pub(crate) comp_vector: u32,
    /// extra work-completion fields requested on top of the always-present standard set
    pub(crate) wc_flags: u32,
    /// the completion channel to deliver notifications on, if any
    pub(crate) comp_channel: Option<CompletionChannel>,
}

impl CompletionQueueBuilder {
    /// Set an opaque context value associated with the completion queue.
    ///
    /// Defaults to 0.
    pub fn set_context(&mut self, id: isize) -> &mut Self {
        self.cq_context = id;
        self
    }

    /// Set the completion vector (the index of the completion event channel used for notifications)
    /// the queue is bound to. Must be in `[0, context.num_comp_vectors)`.
    ///
    /// Defaults to 0.
    pub fn set_comp_vector(&mut self, comp_vector: u32) -> &mut Self {
        self.comp_vector = comp_vector;
        self
    }

    /// Request additional work-completion fields beyond the standard set.
    ///
    /// The standard fields (byte length, immediate data, QP number, and source QP) are always
    /// requested so the [`WorkCompletion`] accessors work; the flags given here are requested *in
    /// addition*. For example, pass [`ibv_create_cq_wc_flags::IBV_WC_EX_WITH_COMPLETION_TIMESTAMP`]
    /// to make [`WorkCompletion::completion_timestamp`] available.
    ///
    /// Not every provider supports every optional field; [`build`](Self::build) then fails (typically
    /// with `EOPNOTSUPP`).
    ///
    /// Defaults to none.
    pub fn set_wc_flags(&mut self, wc_flags: ffi::ibv_create_cq_wc_flags) -> &mut Self {
        self.wc_flags = wc_flags.0;
        self
    }

    /// Deliver this queue's completion notifications on `channel` (from
    /// [`Context::create_comp_channel`]).
    ///
    /// Without a channel the queue can only be polled; with one, arming the queue with
    /// [`CompletionQueue::req_notify`] makes the next completion raise a notification on the
    /// channel, to block on ([`CompletionChannel::wait`]) or watch from an event loop. Several
    /// queues can be built on one channel; give each a distinct
    /// [`set_context`](Self::set_context) so [`CompletionChannel::get_event`] can tell them apart.
    ///
    /// Defaults to no channel.
    pub fn set_comp_channel(&mut self, channel: &CompletionChannel) -> &mut Self {
        self.comp_channel = Some(channel.clone());
        self
    }

    /// Create the completion queue.
    ///
    /// # Errors
    ///
    ///  - `EOPNOTSUPP`: The device does not support a requested work-completion field.
    ///  - `EINVAL`: Invalid `min_cq_entries` (must be `1 <= cqe <= dev_cap.max_cqe`) or comp vector.
    ///  - `ENOMEM`: Not enough resources to complete this operation.
    pub fn build(&self) -> Result<CompletionQueue> {
        // The queue holds a reference to its channel (if any), so the channel cannot be destroyed
        // out from under it.
        let cc = self.comp_channel.clone();

        // Always request the standard work-completion fields so the lazy readers in `WorkCompletion`
        // can serve them, then add any caller-requested extras (such as the completion timestamp).
        let wc_flags = ffi::ibv_create_cq_wc_flags::IBV_WC_EX_WITH_BYTE_LEN.0
            | ffi::ibv_create_cq_wc_flags::IBV_WC_EX_WITH_IMM.0
            | ffi::ibv_create_cq_wc_flags::IBV_WC_EX_WITH_QP_NUM.0
            | ffi::ibv_create_cq_wc_flags::IBV_WC_EX_WITH_SRC_QP.0
            | self.wc_flags;
        let mut cq_attr = ffi::ibv_cq_init_attr_ex {
            cqe: self.min_cq_entries as u32,
            cq_context: unsafe { ptr::null::<c_void>().offset(self.cq_context) } as *mut _,
            channel: cc
                .as_ref()
                .map_or(ptr::null_mut(), |channel| channel.as_raw()),
            comp_vector: self.comp_vector,
            wc_flags: wc_flags as u64,
            comp_mask: 0,
            flags: 0,
            parent_domain: ptr::null_mut(),
        };
        let cq_ex = unsafe { ffi::ibv_create_cq_ex(self.ctx.ctx, &mut cq_attr as *mut _) };

        if cq_ex.is_null() {
            Err(Error::os(
                io::Error::last_os_error(),
                Error::CreateCompletionQueue,
            ))
        } else {
            Ok(CompletionQueue {
                inner: Arc::new(CompletionQueueInner {
                    _ctx: self.ctx.clone(),
                    cc,
                    cq_ex,
                }),
            })
        }
    }
}

pub(crate) struct CompletionQueueInner {
    _ctx: Arc<ContextInner>,
    cq_ex: *mut ffi::ibv_cq_ex,
    cc: Option<CompletionChannel>,
}

impl CompletionQueueInner {
    /// The underlying `ibv_cq`. An `ibv_cq_ex` shares its layout prefix with `ibv_cq`, so this is
    /// just a pointer cast (exactly what `ibv_cq_ex_to_cq` does in C). Used for the verbs that still
    /// take a plain `ibv_cq`: queue-pair creation, completion-event notification, and teardown.
    #[inline]
    pub(crate) fn cq(&self) -> *mut ffi::ibv_cq {
        self.cq_ex as *mut ffi::ibv_cq
    }
}

impl Drop for CompletionQueueInner {
    fn drop(&mut self) {
        let errno = unsafe { ffi::ibv_destroy_cq(self.cq()) };
        if errno != 0 {
            let e = io::Error::from_raw_os_error(errno);
            panic!("{e}");
        }

        // The queue's reference to its completion channel (if any) is released when the `cc` field
        // drops after this, ordered after `ibv_destroy_cq` as the provider requires. The channel
        // itself is destroyed once its other queues and clones are gone too.
    }
}

unsafe impl Send for CompletionQueueInner {}
unsafe impl Sync for CompletionQueueInner {}

/// A single work completion, borrowed from the completion queue being polled.
///
/// Returned by [`Completions::next`]. Fields are read lazily through the extended completion-queue
/// interface, so you only pay for the ones you access (`wr_id` and `status` are plain reads; the
/// rest dispatch to the provider). The handle borrows the [`Completions`] iterator, so it must be
/// dropped before advancing — the completion data is only valid until then.
pub struct WorkCompletion<'iter> {
    cq: *mut ffi::ibv_cq_ex,
    _iter: std::marker::PhantomData<&'iter Completions<'iter>>,
}

#[allow(clippy::len_without_is_empty)]
impl WorkCompletion<'_> {
    /// The 64-bit id that was associated with the corresponding work request when it was posted.
    #[inline]
    pub fn wr_id(&self) -> u64 {
        unsafe { (*self.cq).wr_id }
    }

    /// Whether this work request completed successfully (`IBV_WC_SUCCESS`).
    #[inline]
    pub fn is_success(&self) -> bool {
        unsafe { (*self.cq).status == ffi::ibv_wc_status::IBV_WC_SUCCESS }
    }

    /// `Ok(())` if the work request completed successfully, otherwise the status and vendor error.
    #[inline]
    pub fn ok(&self) -> std::result::Result<(), (ffi::ibv_wc_status, u32)> {
        match self.error() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// The completion status and vendor error syndrome if the work request did not succeed.
    #[inline]
    pub fn error(&self) -> Option<(ffi::ibv_wc_status, u32)> {
        match unsafe { (*self.cq).status } {
            ffi::ibv_wc_status::IBV_WC_SUCCESS => None,
            status => Some((status, unsafe {
                (*self.cq).read_vendor_err.unwrap()(self.cq)
            })),
        }
    }

    /// The opcode of the completed work request.
    ///
    /// Like `len` and the other detail fields, this is only meaningful when the completion
    /// succeeded ([`ok`](Self::ok)); for a failed or flushed work request only
    /// [`wr_id`](Self::wr_id) and the status are defined.
    #[inline]
    pub fn opcode(&self) -> ffi::ibv_wc_opcode {
        unsafe { (*self.cq).read_opcode.unwrap()(self.cq) }
    }

    /// The number of bytes transferred, for a successful completion.
    #[inline]
    pub fn len(&self) -> usize {
        unsafe { (*self.cq).read_byte_len.unwrap()(self.cq) as usize }
    }

    /// The 32-bit immediate value (host byte order) if one was carried (`IBV_WC_WITH_IMM`).
    #[inline]
    pub fn imm_data(&self) -> Option<u32> {
        let flags = ffi::ibv_wc_flags(unsafe { (*self.cq).read_wc_flags.unwrap()(self.cq) });
        if self.is_success() && (flags & ffi::ibv_wc_flags::IBV_WC_WITH_IMM).0 != 0 {
            Some(u32::from_be(unsafe {
                (*self.cq).read_imm_data.unwrap()(self.cq)
            }))
        } else {
            None
        }
    }

    /// The local QP number of the completed work request.
    #[inline]
    pub fn qp_num(&self) -> u32 {
        unsafe { (*self.cq).read_qp_num.unwrap()(self.cq) }
    }

    /// The source (remote) QP number, relevant for datagram receive completions.
    #[inline]
    pub fn src_qp(&self) -> u32 {
        unsafe { (*self.cq).read_src_qp.unwrap()(self.cq) }
    }

    /// The hardware timestamp captured when this work request completed, in the device's free-running
    /// clock units (the same time base as [`Context::query_rt_values_ex`]).
    ///
    /// Only valid on a completion queue that requested
    /// [`IBV_WC_EX_WITH_COMPLETION_TIMESTAMP`](ibv_create_cq_wc_flags::IBV_WC_EX_WITH_COMPLETION_TIMESTAMP)
    /// (see [`CompletionQueueBuilder::set_wc_flags`]). Calling this on a completion from any other
    /// completion queue panics, because the provider did not install the timestamp reader.
    #[inline]
    pub fn completion_timestamp(&self) -> u64 {
        unsafe {
            (*self.cq)
                .read_completion_ts
                .expect("completion queue was not created with timestamps")(self.cq)
        }
    }

    /// The wallclock hardware timestamp (in nanoseconds) captured when this work request completed.
    ///
    /// Only valid on a completion queue that requested
    /// [`IBV_WC_EX_WITH_COMPLETION_TIMESTAMP_WALLCLOCK`](ibv_create_cq_wc_flags::IBV_WC_EX_WITH_COMPLETION_TIMESTAMP_WALLCLOCK)
    /// (see [`CompletionQueueBuilder::set_wc_flags`]). Panics on a completion from any other
    /// completion queue, because the provider did not install the reader.
    #[inline]
    pub fn completion_wallclock_ns(&self) -> u64 {
        unsafe {
            (*self.cq)
                .read_completion_wallclock_ns
                .expect("completion queue was not created with wallclock timestamps")(
                self.cq
            )
        }
    }

    /// The raw work-completion flags (`IBV_WC_*`), such as whether a GRH is present or immediate
    /// data is carried. Always available.
    #[inline]
    pub fn wc_flags(&self) -> ffi::ibv_wc_flags {
        ffi::ibv_wc_flags(unsafe { (*self.cq).read_wc_flags.unwrap()(self.cq) })
    }

    /// Whether the receive completion carries a 40-byte Global Routing Header (GRH) at the front of
    /// the scatter buffers (set for unreliable-datagram receives with a GRH). Always available.
    #[inline]
    pub fn has_grh(&self) -> bool {
        (self.wc_flags() & ffi::ibv_wc_flags::IBV_WC_GRH).0 != 0
    }

    /// The source LID this message was sent from (relevant for datagram receive completions).
    ///
    /// Only valid on a completion queue that requested
    /// [`IBV_WC_EX_WITH_SLID`](ibv_create_cq_wc_flags::IBV_WC_EX_WITH_SLID) (see
    /// [`CompletionQueueBuilder::set_wc_flags`]). Panics otherwise.
    #[inline]
    pub fn slid(&self) -> u32 {
        unsafe {
            (*self.cq)
                .read_slid
                .expect("completion queue did not request the source LID")(self.cq)
        }
    }

    /// The service level this message was sent with (relevant for datagram receive completions).
    ///
    /// Only valid on a completion queue that requested
    /// [`IBV_WC_EX_WITH_SL`](ibv_create_cq_wc_flags::IBV_WC_EX_WITH_SL) (see
    /// [`CompletionQueueBuilder::set_wc_flags`]). Panics otherwise.
    #[inline]
    pub fn sl(&self) -> u8 {
        unsafe {
            (*self.cq)
                .read_sl
                .expect("completion queue did not request the service level")(self.cq)
        }
    }

    /// The destination LID path bits (relevant for datagram receive completions).
    ///
    /// Only valid on a completion queue that requested
    /// [`IBV_WC_EX_WITH_DLID_PATH_BITS`](ibv_create_cq_wc_flags::IBV_WC_EX_WITH_DLID_PATH_BITS) (see
    /// [`CompletionQueueBuilder::set_wc_flags`]). Panics otherwise.
    #[inline]
    pub fn dlid_path_bits(&self) -> u8 {
        unsafe {
            (*self.cq)
                .read_dlid_path_bits
                .expect("completion queue did not request the DLID path bits")(self.cq)
        }
    }
}

/// An in-progress poll of a [`CompletionQueue`], yielding work completions one at a time.
///
/// Created by [`CompletionQueue::poll`]. This is a *lending* iterator: each [`WorkCompletion`]
/// borrows the `Completions`, so it must be dropped before the next [`next`](Completions::next)
/// call (which is why it cannot implement [`Iterator`]). The completion queue is released
/// (`ibv_end_poll`) when the `Completions` is dropped.
#[must_use]
pub struct Completions<'cq> {
    cq: *mut ffi::ibv_cq_ex,
    first: bool,
    _cq: std::marker::PhantomData<&'cq CompletionQueueInner>,
}

impl Completions<'_> {
    /// Return the next work completion, or `None` once the queue has no more.
    ///
    /// Consume with `while let Some(wc) = completions.next() { ... }`.
    ///
    /// `None` also ends the poll if the provider reports an error mid-poll (a rare provider-level
    /// failure, distinct from a completion *status* error, which is reported per work completion
    /// through [`WorkCompletion::ok`]); resources are still released correctly in that case.
    #[allow(clippy::should_implement_trait)]
    #[inline]
    pub fn next(&mut self) -> Option<WorkCompletion<'_>> {
        if self.first {
            self.first = false;
        } else if unsafe { (*self.cq).next_poll.unwrap()(self.cq) } != 0 {
            // ENOENT (no more) or an error: either way the poll is finished.
            return None;
        }
        Some(WorkCompletion {
            cq: self.cq,
            _iter: std::marker::PhantomData,
        })
    }
}

impl Drop for Completions<'_> {
    fn drop(&mut self) {
        unsafe { (*self.cq).end_poll.unwrap()(self.cq) };
    }
}

/// A completion queue that allows subscribing to the completion of queued sends and receives.
#[must_use]
#[derive(Clone)]
pub struct CompletionQueue {
    pub(crate) inner: Arc<CompletionQueueInner>,
}

impl CompletionQueue {
    /// Begin polling for work completions through the extended interface.
    ///
    /// Returns `None` if the queue is currently empty. The returned [`Completions`] is a lending
    /// iterator whose [`WorkCompletion`]s read their fields lazily, so you only pay for the fields
    /// you read.
    ///
    /// Callers must ensure the CQ does not overrun (exceed its capacity), as this triggers an
    /// `IBV_EVENT_CQ_ERR` async event, rendering the CQ unusable. You can do this by limiting the
    /// number of inflight work requests.
    ///
    /// `poll` does not block or cause a context switch; to block until completions arrive, build
    /// the queue on a [`CompletionChannel`] and wait there instead of spinning on `poll` (see
    /// [`req_notify`](Self::req_notify) for the loop).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ibverbs::CompletionQueue;
    /// # fn drain(cq: &CompletionQueue) -> ibverbs::Result<()> {
    /// if let Some(mut completions) = cq.poll()? {
    ///     while let Some(wc) = completions.next() {
    ///         if let Err((status, _)) = wc.ok() {
    ///             eprintln!("work request {} failed: {status:?}", wc.wr_id());
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn poll(&self) -> Result<Option<Completions<'_>>> {
        let cq = self.inner.cq_ex;
        let mut attr = ffi::ibv_poll_cq_attr { comp_mask: 0 };
        // `start_poll` positions the CQ on the first completion; it returns ENOENT (and must not be
        // paired with `end_poll`) when the queue is empty.
        match unsafe { (*cq).start_poll.unwrap()(cq, &mut attr as *mut _) } {
            0 => Ok(Some(Completions {
                cq,
                first: true,
                _cq: std::marker::PhantomData,
            })),
            e if e == nix::libc::ENOENT => Ok(None),
            e => Err(Error::errno(e, Error::PollCompletionQueue)),
        }
    }

    /// Arm the completion queue so the next work completion generates a notification on its
    /// completion channel (`ibv_req_notify_cq`).
    ///
    /// This is the building block for event-driven and asynchronous completion handling: arm the
    /// queue, drain everything already pending with [`poll`](Self::poll), then wait for a
    /// notification on the [completion channel](Self::comp_channel) — either by blocking with
    /// [`CompletionChannel::wait`], or by watching its [`AsFd`] descriptor with
    /// your own reactor (for example `epoll` or a `tokio` `AsyncFd`) and consuming the notification
    /// with [`CompletionChannel::get_event`]. Then drain again and re-arm. Arming a queue that was
    /// built without a channel ([`CompletionQueueBuilder::set_comp_channel`]) has nothing to
    /// notify.
    ///
    /// If `solicited_only` is set, only completions of work requests that asked for a solicited
    /// event generate a notification.
    pub fn req_notify(&self, solicited_only: bool) -> Result<()> {
        let cq = self.inner.cq();
        let ctx = unsafe { *cq }.context;
        let errno = unsafe { (*ctx).ops.req_notify_cq.unwrap()(cq, solicited_only as i32) };
        if errno != 0 {
            return Err(Error::errno(errno, Error::PollCompletionQueue));
        }
        Ok(())
    }

    /// The completion channel this queue delivers notifications on, if it was built with one
    /// ([`CompletionQueueBuilder::set_comp_channel`]).
    pub fn comp_channel(&self) -> Option<&CompletionChannel> {
        self.inner.cc.as_ref()
    }

    /// Returns the underlying `ibv_cq` pointer.
    ///
    /// This is an escape hatch for verbs this crate does not yet wrap. The completion queue is built
    /// through the extended interface; an `ibv_cq_ex` shares its layout prefix with `ibv_cq`, so this
    /// is the same handle viewed as a plain completion queue (see [`as_raw_ex`](Self::as_raw_ex)).
    /// The pointer stays valid only while this [`CompletionQueue`] is alive; do not destroy it.
    pub fn as_raw(&self) -> *mut ffi::ibv_cq {
        self.inner.cq()
    }

    /// Returns the underlying `ibv_cq_ex` pointer (the extended completion queue this is built on).
    ///
    /// This is an escape hatch for verbs this crate does not yet wrap. The pointer stays valid only
    /// while this [`CompletionQueue`] is alive; do not destroy it.
    pub fn as_raw_ex(&self) -> *mut ffi::ibv_cq_ex {
        self.inner.cq_ex
    }
}
