use std::fmt;
use std::io;
use std::os::fd::{AsFd, BorrowedFd};
use std::os::raw::c_void;
use std::ptr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::context::{ContextInner, HcaClock};
use crate::error::{Error, Result};

#[cfg(doc)]
use crate::Context;

/// Round `remaining` up to whole milliseconds, so a `poll(2)` wait (which has millisecond
/// granularity) lasts at least the requested duration instead of returning fractionally early.
pub(crate) fn ceil_to_millis(remaining: Duration) -> Duration {
    Duration::from_millis(remaining.as_nanos().div_ceil(1_000_000) as u64)
}

/// A completion channel: the file descriptor that delivers completion-queue notifications.
/// Created by [`Context::create_comp_channel`].
///
/// By default a [`CompletionQueue`] has no channel and is driven by polling alone. To wait for
/// completions instead of burning a core, create a channel with [`Context::create_comp_channel`]
/// and build the queue on it with [`CompletionQueueBuilder::set_comp_channel`]; then arm the queue
/// with [`CompletionQueue::req_notify`], drain it with [`poll`](CompletionQueue::poll), and block
/// on the channel ([`wait`](Self::wait)), or watch its descriptor
/// ([`AsFd`]/[`AsRawFd`](std::os::fd::AsRawFd)) from your own reactor and consume notifications
/// with [`get_event`](Self::get_event), before draining and re-arming again.
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
#[must_use]
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
            panic!("ibv_destroy_comp_channel failed: {e}");
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
        // SAFETY: the channel owns this fd, and the borrow ends within this call.
        let fd = unsafe { std::os::fd::BorrowedFd::borrow_raw((*self.inner.cc).fd) };
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
    /// Demultiplexing several queues that share one channel works by draining notifications here
    /// after the channel's file descriptor becomes readable, and mapping each returned context
    /// back to the queue, which you then [`poll`](CompletionQueue::poll) and re-arm with
    /// [`req_notify`](CompletionQueue::req_notify). Give each queue a distinct
    /// [`set_context`](CompletionQueueBuilder::set_context) value so they can be told apart.
    /// Acknowledgement is handled for you.
    ///
    /// # Errors
    ///
    ///  - [`PollCompletionQueue`](Error::PollCompletionQueue): `ibv_get_cq_event` failed.
    pub fn get_event(&self) -> Result<Option<u64>> {
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
        Ok(Some(out_cq_context as usize as u64))
    }

    /// Block until a notification is available on the channel (up to `timeout`), then consume it,
    /// returning the context value of the completion queue it belongs to (the value set with
    /// [`CompletionQueueBuilder::set_context`]). Returns `None` only if `timeout` elapses first;
    /// with no timeout it waits indefinitely, even if other threads race it for notifications.
    ///
    /// The blocking form of [`get_event`](Self::get_event): it waits on the channel's file
    /// descriptor for you rather than requiring an external reactor. Arm each queue with
    /// [`CompletionQueue::req_notify`] and drain it with [`poll`](CompletionQueue::poll) *before*
    /// blocking here; polling after arming closes the race where a completion lands between an
    /// earlier poll and arming. Then call this to learn which queue fired, and poll and re-arm
    /// that queue.
    ///
    /// # Errors
    ///
    ///  - [`PollCompletionQueue`](Error::PollCompletionQueue): waiting on the descriptor (`poll`)
    ///    or consuming the notification failed.
    pub fn wait(&self, timeout: Option<Duration>) -> Result<Option<u64>> {
        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        loop {
            let remaining = deadline
                .map(|deadline| ceil_to_millis(deadline.saturating_duration_since(Instant::now())));
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
    pub(crate) min_cq_entries: u32,
    pub(crate) cq_context: u64,
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
    pub fn set_context(&mut self, id: u64) -> &mut Self {
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
    /// requested so the [`WorkCompletion`] accessors work; the fields given here are requested *in
    /// addition*. For example, pass [`WcFields::COMPLETION_TIMESTAMP`] to make
    /// [`WorkCompletion::completion_timestamp`] available.
    ///
    /// Not every provider supports every optional field; [`build`](Self::build) then fails (typically
    /// with `EOPNOTSUPP`).
    ///
    /// Defaults to none.
    pub fn set_wc_flags(&mut self, wc_flags: WcFields) -> &mut Self {
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
    ///  - [`Unsupported`](Error::Unsupported): the device does not support a requested
    ///    work-completion field (`EOPNOTSUPP`).
    ///  - [`CreateCompletionQueue`](Error::CreateCompletionQueue): `ibv_create_cq_ex` failed
    ///    (`EINVAL` for an invalid `min_cq_entries`, which must be `1 <= cqe <= dev_cap.max_cqe`,
    ///    or an invalid completion vector, `ENOMEM` when out of resources).
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
        // Zero the attributes and write only the fields in use (`comp_mask`, `flags`, and
        // `parent_domain` stay zero) rather than naming every field: rdma-core extends this struct
        // over time, and an exhaustive literal stops compiling against newer headers.
        let mut cq_attr = std::mem::MaybeUninit::<ffi::ibv_cq_init_attr_ex>::zeroed();
        let p = cq_attr.as_mut_ptr();
        unsafe {
            (*p).cqe = self.min_cq_entries;
            // The cookie is a plain integer to the caller; the C ABI carries it as a pointer.
            (*p).cq_context = self.cq_context as usize as *mut c_void;
            (*p).channel = cc
                .as_ref()
                .map_or(ptr::null_mut(), |channel| channel.as_raw());
            (*p).comp_vector = self.comp_vector;
            (*p).wc_flags = wc_flags as u64;
        }
        let cq_ex = unsafe { ffi::ibv_create_cq_ex(self.ctx.ctx, cq_attr.as_mut_ptr()) };

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
            panic!("ibv_destroy_cq failed: {e}");
        }

        // The queue's reference to its completion channel (if any) is released when the `cc` field
        // drops after this, ordered after `ibv_destroy_cq` as the provider requires. The channel
        // itself is destroyed once its other queues and clones are gone too.
    }
}

unsafe impl Send for CompletionQueueInner {}
unsafe impl Sync for CompletionQueueInner {}

flags_newtype! {
    /// Optional work-completion fields to request when building a completion queue (the
    /// `IBV_WC_EX_WITH_*` bits), via [`CompletionQueueBuilder::set_wc_flags`].
    ///
    /// The byte length, immediate data, QP number, and source QP are always requested; these flags
    /// add fields on top, at the cost of a larger completion entry. Fields with an accessor on
    /// [`WorkCompletion`] panic when read if they were not requested.
    pub struct WcFields(ffi::ibv_create_cq_wc_flags) {
        /// The number of bytes transferred ([`WorkCompletion::len`]; always requested).
        BYTE_LEN = IBV_WC_EX_WITH_BYTE_LEN;
        /// The immediate data ([`WorkCompletion::imm_data`]; always requested).
        IMM = IBV_WC_EX_WITH_IMM;
        /// The local QP number ([`WorkCompletion::qp_num`]; always requested).
        QP_NUM = IBV_WC_EX_WITH_QP_NUM;
        /// The source QP number ([`WorkCompletion::src_qp`]; always requested).
        SRC_QP = IBV_WC_EX_WITH_SRC_QP;
        /// The source LID ([`WorkCompletion::slid`]).
        SLID = IBV_WC_EX_WITH_SLID;
        /// The service level ([`WorkCompletion::sl`]).
        SL = IBV_WC_EX_WITH_SL;
        /// The destination LID path bits ([`WorkCompletion::dlid_path_bits`]).
        DLID_PATH_BITS = IBV_WC_EX_WITH_DLID_PATH_BITS;
        /// The hardware completion timestamp ([`WorkCompletion::completion_timestamp`]).
        COMPLETION_TIMESTAMP = IBV_WC_EX_WITH_COMPLETION_TIMESTAMP;
        /// The customer VLAN of the incoming packet.
        CVLAN = IBV_WC_EX_WITH_CVLAN;
        /// The flow tag of the incoming packet.
        FLOW_TAG = IBV_WC_EX_WITH_FLOW_TAG;
        /// The tag-matching information.
        TM_INFO = IBV_WC_EX_WITH_TM_INFO;
        /// The wallclock completion timestamp ([`WorkCompletion::completion_wallclock_ns`]).
        COMPLETION_TIMESTAMP_WALLCLOCK = IBV_WC_EX_WITH_COMPLETION_TIMESTAMP_WALLCLOCK;
    }
}

flags_newtype! {
    /// Properties of a completed work request (the `IBV_WC_*` flag bits), as returned by
    /// [`WorkCompletion::wc_flags`].
    pub struct WcFlags(ffi::ibv_wc_flags) {
        /// The receive completion carries a 40-byte Global Routing Header (GRH) at the front of
        /// the scatter buffers (see [`WorkCompletion::has_grh`]).
        GRH = IBV_WC_GRH;
        /// The completion carries a 32-bit immediate value (see [`WorkCompletion::imm_data`]).
        WITH_IMM = IBV_WC_WITH_IMM;
        /// The IP and TCP/UDP checksums of the incoming packet were verified by the device.
        IP_CSUM_OK = IBV_WC_IP_CSUM_OK;
        /// The completion carries an invalidated rkey (from a send-with-invalidate).
        WITH_INV = IBV_WC_WITH_INV;
        /// The tag-matching synchronization request.
        TM_SYNC_REQ = IBV_WC_TM_SYNC_REQ;
        /// A tag-matching receive was matched.
        TM_MATCH = IBV_WC_TM_MATCH;
        /// The tag-matching receive data is valid.
        TM_DATA_VALID = IBV_WC_TM_DATA_VALID;
    }
}

/// The completion status of a work request, reported by [`WorkCompletion::ok`] (as the
/// [`WcError::status`] of a failed completion).
///
/// Anything other than [`Success`](Self::Success) means the work request failed (and, on a
/// connected queue pair, that the queue pair has moved to the error state); once one work request
/// fails, the ones behind it complete as [`WorkRequestFlushed`](Self::WorkRequestFlushed).
/// `Display` gives the human-readable message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WcStatus {
    /// The work request completed successfully.
    Success,
    /// A posted buffer was too small for the data (local length error).
    LocalLengthError,
    /// An internal queue-pair consistency error was detected locally.
    LocalQpOperationError,
    /// An internal EE-context consistency error was detected locally (RD only).
    LocalEecOperationError,
    /// A posted buffer did not have valid protection (local protection error).
    LocalProtectionError,
    /// The work request was flushed because the queue pair entered the error state before (or
    /// while) processing it.
    WorkRequestFlushed,
    /// A memory-window bind operation failed.
    MemoryWindowBindError,
    /// The responder returned a malformed response.
    BadResponse,
    /// A local access violation while responding to an incoming operation.
    LocalAccessError,
    /// The remote side rejected the request as invalid for its queue pair.
    RemoteInvalidRequest,
    /// The remote side reported an access violation for the targeted region.
    RemoteAccessError,
    /// The remote side could not complete the operation.
    RemoteOperationError,
    /// The transport retry counter was exceeded without a response from the remote side.
    RetryExceeded,
    /// The receiver-not-ready retry counter was exceeded.
    RnrRetryExceeded,
    /// A local RD domain violation (RD only).
    LocalRddViolation,
    /// The remote side rejected an RD read request as invalid (RD only).
    RemoteInvalidRdRequest,
    /// The remote side aborted the operation (RD only).
    RemoteAborted,
    /// An invalid EE context number was detected (RD only).
    InvalidEecn,
    /// An invalid EE context state was detected (RD only).
    InvalidEecState,
    /// A fatal transport error occurred; further use of the device is undefined.
    Fatal,
    /// The response timer expired before a response arrived.
    ResponseTimeout,
    /// An error not covered by the other statuses.
    GeneralError,
    /// A tag-matching error occurred.
    TagMatchingError,
    /// A tag-matching rendezvous transfer did not complete.
    TagMatchingRendezvousIncomplete,
}

impl From<ffi::ibv_wc_status> for WcStatus {
    fn from(status: ffi::ibv_wc_status) -> Self {
        use ffi::ibv_wc_status::*;
        match status {
            IBV_WC_SUCCESS => WcStatus::Success,
            IBV_WC_LOC_LEN_ERR => WcStatus::LocalLengthError,
            IBV_WC_LOC_QP_OP_ERR => WcStatus::LocalQpOperationError,
            IBV_WC_LOC_EEC_OP_ERR => WcStatus::LocalEecOperationError,
            IBV_WC_LOC_PROT_ERR => WcStatus::LocalProtectionError,
            IBV_WC_WR_FLUSH_ERR => WcStatus::WorkRequestFlushed,
            IBV_WC_MW_BIND_ERR => WcStatus::MemoryWindowBindError,
            IBV_WC_BAD_RESP_ERR => WcStatus::BadResponse,
            IBV_WC_LOC_ACCESS_ERR => WcStatus::LocalAccessError,
            IBV_WC_REM_INV_REQ_ERR => WcStatus::RemoteInvalidRequest,
            IBV_WC_REM_ACCESS_ERR => WcStatus::RemoteAccessError,
            IBV_WC_REM_OP_ERR => WcStatus::RemoteOperationError,
            IBV_WC_RETRY_EXC_ERR => WcStatus::RetryExceeded,
            IBV_WC_RNR_RETRY_EXC_ERR => WcStatus::RnrRetryExceeded,
            IBV_WC_LOC_RDD_VIOL_ERR => WcStatus::LocalRddViolation,
            IBV_WC_REM_INV_RD_REQ_ERR => WcStatus::RemoteInvalidRdRequest,
            IBV_WC_REM_ABORT_ERR => WcStatus::RemoteAborted,
            IBV_WC_INV_EECN_ERR => WcStatus::InvalidEecn,
            IBV_WC_INV_EEC_STATE_ERR => WcStatus::InvalidEecState,
            IBV_WC_FATAL_ERR => WcStatus::Fatal,
            IBV_WC_RESP_TIMEOUT_ERR => WcStatus::ResponseTimeout,
            IBV_WC_GENERAL_ERR => WcStatus::GeneralError,
            IBV_WC_TM_ERR => WcStatus::TagMatchingError,
            IBV_WC_TM_RNDV_INCOMPLETE => WcStatus::TagMatchingRendezvousIncomplete,
        }
    }
}

impl From<WcStatus> for ffi::ibv_wc_status {
    fn from(status: WcStatus) -> Self {
        use ffi::ibv_wc_status::*;
        match status {
            WcStatus::Success => IBV_WC_SUCCESS,
            WcStatus::LocalLengthError => IBV_WC_LOC_LEN_ERR,
            WcStatus::LocalQpOperationError => IBV_WC_LOC_QP_OP_ERR,
            WcStatus::LocalEecOperationError => IBV_WC_LOC_EEC_OP_ERR,
            WcStatus::LocalProtectionError => IBV_WC_LOC_PROT_ERR,
            WcStatus::WorkRequestFlushed => IBV_WC_WR_FLUSH_ERR,
            WcStatus::MemoryWindowBindError => IBV_WC_MW_BIND_ERR,
            WcStatus::BadResponse => IBV_WC_BAD_RESP_ERR,
            WcStatus::LocalAccessError => IBV_WC_LOC_ACCESS_ERR,
            WcStatus::RemoteInvalidRequest => IBV_WC_REM_INV_REQ_ERR,
            WcStatus::RemoteAccessError => IBV_WC_REM_ACCESS_ERR,
            WcStatus::RemoteOperationError => IBV_WC_REM_OP_ERR,
            WcStatus::RetryExceeded => IBV_WC_RETRY_EXC_ERR,
            WcStatus::RnrRetryExceeded => IBV_WC_RNR_RETRY_EXC_ERR,
            WcStatus::LocalRddViolation => IBV_WC_LOC_RDD_VIOL_ERR,
            WcStatus::RemoteInvalidRdRequest => IBV_WC_REM_INV_RD_REQ_ERR,
            WcStatus::RemoteAborted => IBV_WC_REM_ABORT_ERR,
            WcStatus::InvalidEecn => IBV_WC_INV_EECN_ERR,
            WcStatus::InvalidEecState => IBV_WC_INV_EEC_STATE_ERR,
            WcStatus::Fatal => IBV_WC_FATAL_ERR,
            WcStatus::ResponseTimeout => IBV_WC_RESP_TIMEOUT_ERR,
            WcStatus::GeneralError => IBV_WC_GENERAL_ERR,
            WcStatus::TagMatchingError => IBV_WC_TM_ERR,
            WcStatus::TagMatchingRendezvousIncomplete => IBV_WC_TM_RNDV_INCOMPLETE,
        }
    }
}

impl fmt::Display for WcStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = match self {
            WcStatus::Success => "success",
            WcStatus::LocalLengthError => "local length error",
            WcStatus::LocalQpOperationError => "local QP operation error",
            WcStatus::LocalEecOperationError => "local EE context operation error",
            WcStatus::LocalProtectionError => "local protection error",
            WcStatus::WorkRequestFlushed => "work request flushed",
            WcStatus::MemoryWindowBindError => "memory window bind error",
            WcStatus::BadResponse => "bad response from remote",
            WcStatus::LocalAccessError => "local access error",
            WcStatus::RemoteInvalidRequest => "remote rejected the request as invalid",
            WcStatus::RemoteAccessError => "remote access error",
            WcStatus::RemoteOperationError => "remote operation error",
            WcStatus::RetryExceeded => "transport retry counter exceeded",
            WcStatus::RnrRetryExceeded => "receiver-not-ready retry counter exceeded",
            WcStatus::LocalRddViolation => "local RDD violation",
            WcStatus::RemoteInvalidRdRequest => "remote rejected the RD request as invalid",
            WcStatus::RemoteAborted => "remote aborted the operation",
            WcStatus::InvalidEecn => "invalid EE context number",
            WcStatus::InvalidEecState => "invalid EE context state",
            WcStatus::Fatal => "fatal transport error",
            WcStatus::ResponseTimeout => "response timeout",
            WcStatus::GeneralError => "general error",
            WcStatus::TagMatchingError => "tag matching error",
            WcStatus::TagMatchingRendezvousIncomplete => "tag matching rendezvous incomplete",
        };
        f.write_str(msg)
    }
}

/// The failure of a work request, reported by [`WorkCompletion::ok`]: the completion status and
/// the provider-specific vendor error syndrome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WcError {
    /// The completion status (anything other than [`WcStatus::Success`]).
    pub status: WcStatus,
    /// The provider-specific vendor error syndrome, for support tickets and provider debugging.
    pub vendor_err: u32,
}

impl fmt::Display for WcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "work request failed: {} (vendor error {})",
            self.status, self.vendor_err
        )
    }
}

impl std::error::Error for WcError {}

/// The kind of operation a work completion reports on. Returned by [`WorkCompletion::opcode`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WcOpcode {
    /// A SEND completed.
    Send,
    /// An RDMA write completed.
    RdmaWrite,
    /// An RDMA read completed.
    RdmaRead,
    /// An atomic compare-and-swap completed.
    CompSwap,
    /// An atomic fetch-and-add completed.
    FetchAdd,
    /// A memory-window bind completed.
    BindMw,
    /// A local invalidate completed.
    LocalInv,
    /// A TCP segmentation offload send completed.
    Tso,
    /// A memory flush completed.
    Flush,
    /// An atomic write completed.
    AtomicWrite,
    /// An incoming message was received.
    Recv,
    /// An incoming RDMA-write-with-immediate consumed a receive.
    RecvRdmaWithImm,
    /// A tag-matching entry was added.
    TmAdd,
    /// A tag-matching entry was deleted.
    TmDel,
    /// A tag-matching list synchronization completed.
    TmSync,
    /// A tag-matching receive completed.
    TmRecv,
    /// An unexpected (untagged) tag-matching receive completed.
    TmNoTag,
    /// Provider-specific operation 1.
    Driver1,
    /// Provider-specific operation 2.
    Driver2,
    /// Provider-specific operation 3.
    Driver3,
}

impl From<ffi::ibv_wc_opcode> for WcOpcode {
    fn from(opcode: ffi::ibv_wc_opcode) -> Self {
        use ffi::ibv_wc_opcode::*;
        match opcode {
            IBV_WC_SEND => WcOpcode::Send,
            IBV_WC_RDMA_WRITE => WcOpcode::RdmaWrite,
            IBV_WC_RDMA_READ => WcOpcode::RdmaRead,
            IBV_WC_COMP_SWAP => WcOpcode::CompSwap,
            IBV_WC_FETCH_ADD => WcOpcode::FetchAdd,
            IBV_WC_BIND_MW => WcOpcode::BindMw,
            IBV_WC_LOCAL_INV => WcOpcode::LocalInv,
            IBV_WC_TSO => WcOpcode::Tso,
            IBV_WC_FLUSH => WcOpcode::Flush,
            IBV_WC_ATOMIC_WRITE => WcOpcode::AtomicWrite,
            IBV_WC_RECV => WcOpcode::Recv,
            IBV_WC_RECV_RDMA_WITH_IMM => WcOpcode::RecvRdmaWithImm,
            IBV_WC_TM_ADD => WcOpcode::TmAdd,
            IBV_WC_TM_DEL => WcOpcode::TmDel,
            IBV_WC_TM_SYNC => WcOpcode::TmSync,
            IBV_WC_TM_RECV => WcOpcode::TmRecv,
            IBV_WC_TM_NO_TAG => WcOpcode::TmNoTag,
            IBV_WC_DRIVER1 => WcOpcode::Driver1,
            IBV_WC_DRIVER2 => WcOpcode::Driver2,
            IBV_WC_DRIVER3 => WcOpcode::Driver3,
        }
    }
}

impl From<WcOpcode> for ffi::ibv_wc_opcode {
    fn from(opcode: WcOpcode) -> Self {
        use ffi::ibv_wc_opcode::*;
        match opcode {
            WcOpcode::Send => IBV_WC_SEND,
            WcOpcode::RdmaWrite => IBV_WC_RDMA_WRITE,
            WcOpcode::RdmaRead => IBV_WC_RDMA_READ,
            WcOpcode::CompSwap => IBV_WC_COMP_SWAP,
            WcOpcode::FetchAdd => IBV_WC_FETCH_ADD,
            WcOpcode::BindMw => IBV_WC_BIND_MW,
            WcOpcode::LocalInv => IBV_WC_LOCAL_INV,
            WcOpcode::Tso => IBV_WC_TSO,
            WcOpcode::Flush => IBV_WC_FLUSH,
            WcOpcode::AtomicWrite => IBV_WC_ATOMIC_WRITE,
            WcOpcode::Recv => IBV_WC_RECV,
            WcOpcode::RecvRdmaWithImm => IBV_WC_RECV_RDMA_WITH_IMM,
            WcOpcode::TmAdd => IBV_WC_TM_ADD,
            WcOpcode::TmDel => IBV_WC_TM_DEL,
            WcOpcode::TmSync => IBV_WC_TM_SYNC,
            WcOpcode::TmRecv => IBV_WC_TM_RECV,
            WcOpcode::TmNoTag => IBV_WC_TM_NO_TAG,
            WcOpcode::Driver1 => IBV_WC_DRIVER1,
            WcOpcode::Driver2 => IBV_WC_DRIVER2,
            WcOpcode::Driver3 => IBV_WC_DRIVER3,
        }
    }
}

impl std::fmt::Display for WcOpcode {
    /// Formats the opcode as it is named in the C headers, for example `RDMA_WRITE` for
    /// [`RdmaWrite`](Self::RdmaWrite).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            WcOpcode::Send => "SEND",
            WcOpcode::RdmaWrite => "RDMA_WRITE",
            WcOpcode::RdmaRead => "RDMA_READ",
            WcOpcode::CompSwap => "COMP_SWAP",
            WcOpcode::FetchAdd => "FETCH_ADD",
            WcOpcode::BindMw => "BIND_MW",
            WcOpcode::LocalInv => "LOCAL_INV",
            WcOpcode::Tso => "TSO",
            WcOpcode::Flush => "FLUSH",
            WcOpcode::AtomicWrite => "ATOMIC_WRITE",
            WcOpcode::Recv => "RECV",
            WcOpcode::RecvRdmaWithImm => "RECV_RDMA_WITH_IMM",
            WcOpcode::TmAdd => "TM_ADD",
            WcOpcode::TmDel => "TM_DEL",
            WcOpcode::TmSync => "TM_SYNC",
            WcOpcode::TmRecv => "TM_RECV",
            WcOpcode::TmNoTag => "TM_NO_TAG",
            WcOpcode::Driver1 => "DRIVER1",
            WcOpcode::Driver2 => "DRIVER2",
            WcOpcode::Driver3 => "DRIVER3",
        };
        f.write_str(name)
    }
}

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

    /// `Ok(())` if the work request completed successfully (`IBV_WC_SUCCESS`), otherwise the
    /// [`WcError`] carrying the status and vendor error syndrome.
    #[inline]
    pub fn ok(&self) -> std::result::Result<(), WcError> {
        match unsafe { (*self.cq).status } {
            ffi::ibv_wc_status::IBV_WC_SUCCESS => Ok(()),
            status => Err(WcError {
                status: status.into(),
                vendor_err: unsafe { (*self.cq).read_vendor_err.unwrap()(self.cq) },
            }),
        }
    }

    /// The opcode of the completed work request.
    ///
    /// Like `len` and the other detail fields, this is only meaningful when the completion
    /// succeeded ([`ok`](Self::ok)); for a failed or flushed work request only
    /// [`wr_id`](Self::wr_id) and the status are defined.
    #[inline]
    pub fn opcode(&self) -> WcOpcode {
        unsafe { (*self.cq).read_opcode.unwrap()(self.cq) }.into()
    }

    /// The number of bytes transferred, for a successful completion.
    #[inline]
    pub fn len(&self) -> usize {
        unsafe { (*self.cq).read_byte_len.unwrap()(self.cq) as usize }
    }

    /// The 32-bit immediate value (host byte order) if one was carried ([`WcFlags::WITH_IMM`]).
    #[inline]
    pub fn imm_data(&self) -> Option<u32> {
        if self.ok().is_ok() && self.wc_flags().contains(WcFlags::WITH_IMM) {
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

    /// The hardware timestamp captured when this work request completed, as a reading of the
    /// device's free-running clock (the same time base as [`Context::query_rt_values_ex`]; see
    /// [`HcaClock`] for converting tick deltas to time).
    ///
    /// Only valid on a completion queue that requested [`WcFields::COMPLETION_TIMESTAMP`] (see
    /// [`CompletionQueueBuilder::set_wc_flags`]). Calling this on a completion from any other
    /// completion queue panics, because the provider did not install the timestamp reader.
    #[inline]
    pub fn completion_timestamp(&self) -> HcaClock {
        HcaClock(unsafe {
            (*self.cq)
                .read_completion_ts
                .expect("completion queue was not created with timestamps")(self.cq)
        })
    }

    /// The wallclock hardware timestamp (in nanoseconds) captured when this work request completed.
    ///
    /// Only valid on a completion queue that requested
    /// [`WcFields::COMPLETION_TIMESTAMP_WALLCLOCK`] (see
    /// [`CompletionQueueBuilder::set_wc_flags`]). Panics on a completion from any other
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

    /// The work-completion flags (`IBV_WC_*`), such as whether a GRH is present or immediate
    /// data is carried. Always available.
    #[inline]
    pub fn wc_flags(&self) -> WcFlags {
        WcFlags(unsafe { (*self.cq).read_wc_flags.unwrap()(self.cq) })
    }

    /// Whether the receive completion carries a 40-byte Global Routing Header (GRH) at the front of
    /// the scatter buffers (set for unreliable-datagram receives with a GRH). Always available.
    #[inline]
    pub fn has_grh(&self) -> bool {
        self.wc_flags().contains(WcFlags::GRH)
    }

    /// The source LID this message was sent from (relevant for datagram receive completions).
    ///
    /// Only valid on a completion queue that requested [`WcFields::SLID`] (see
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
    /// Only valid on a completion queue that requested [`WcFields::SL`] (see
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
    /// Only valid on a completion queue that requested [`WcFields::DLID_PATH_BITS`] (see
    /// [`CompletionQueueBuilder::set_wc_flags`]). Panics otherwise.
    #[inline]
    pub fn dlid_path_bits(&self) -> u8 {
        unsafe {
            (*self.cq)
                .read_dlid_path_bits
                .expect("completion queue did not request the DLID path bits")(self.cq)
        }
    }

    /// The remote key a SEND-with-invalidate invalidated, if this completion reports one
    /// ([`WcFlags::WITH_INV`]). It shares its field with the immediate data, so a completion
    /// carries at most one of the two.
    #[inline]
    pub fn invalidated_rkey(&self) -> Option<u32> {
        if self.ok().is_ok() && self.wc_flags().contains(WcFlags::WITH_INV) {
            Some(unsafe { (*self.cq).read_imm_data.unwrap()(self.cq) })
        } else {
            None
        }
    }

    /// The customer VLAN tag (802.1Q) of the incoming packet.
    ///
    /// Only valid on a completion queue that requested [`WcFields::CVLAN`] (see
    /// [`CompletionQueueBuilder::set_wc_flags`]). Panics otherwise.
    #[inline]
    pub fn cvlan(&self) -> u16 {
        unsafe {
            (*self.cq)
                .read_cvlan
                .expect("completion queue did not request the customer VLAN")(self.cq)
        }
    }

    /// The flow tag the device's steering rules attached to the incoming packet.
    ///
    /// Only valid on a completion queue that requested [`WcFields::FLOW_TAG`] (see
    /// [`CompletionQueueBuilder::set_wc_flags`]). Panics otherwise.
    #[inline]
    pub fn flow_tag(&self) -> u32 {
        unsafe {
            (*self.cq)
                .read_flow_tag
                .expect("completion queue did not request the flow tag")(self.cq)
        }
    }

    /// The tag-matching information of a tag-matching receive: the tag and the opaque user data
    /// from the tag-matching header.
    ///
    /// Only valid on a completion queue that requested [`WcFields::TM_INFO`] (see
    /// [`CompletionQueueBuilder::set_wc_flags`]). Panics otherwise.
    #[inline]
    pub fn tag_matching(&self) -> TagMatchingInfo {
        let mut info = ffi::ibv_wc_tm_info::default();
        unsafe {
            (*self.cq)
                .read_tm_info
                .expect("completion queue did not request the tag-matching information")(
                self.cq, &mut info,
            )
        };
        TagMatchingInfo {
            tag: info.tag,
            private: info.priv_,
        }
    }

    /// The raw extended completion queue, positioned on this entry: the escape hatch for the
    /// `ibv_wc_read_*` readers this crate does not wrap. The position is only valid for this
    /// entry, so do not keep the pointer past the [`WorkCompletion`].
    pub fn as_raw(&self) -> *mut ffi::ibv_cq_ex {
        self.cq
    }

    /// The addressing fields of this completion as a classic `ibv_wc`, for deriving the route
    /// back to a datagram's sender: the flags, the source and local queue pair numbers, and — when
    /// the queue requested them — the source LID, service level, and path bits (zero otherwise).
    pub(crate) fn addressing(&self) -> ffi::ibv_wc {
        let mut wc = ffi::ibv_wc::default();
        wc.wc_flags = self.wc_flags().into();
        wc.src_qp = self.src_qp();
        wc.qp_num = self.qp_num();
        unsafe {
            if let Some(read_slid) = (*self.cq).read_slid {
                wc.slid = read_slid(self.cq) as u16;
            }
            if let Some(read_sl) = (*self.cq).read_sl {
                wc.sl = read_sl(self.cq);
            }
            if let Some(read_dlid_path_bits) = (*self.cq).read_dlid_path_bits {
                wc.dlid_path_bits = read_dlid_path_bits(self.cq);
            }
        }
        wc
    }
}

/// The tag-matching information of a receive on a tag-matching queue, read with
/// [`WorkCompletion::tag_matching`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TagMatchingInfo {
    /// The tag from the tag-matching header.
    pub tag: u64,
    /// The opaque user data from the tag-matching header.
    pub private: u32,
}

/// One poll of a [`CompletionQueue`]: the work completions that were ready when it started,
/// yielded one at a time.
///
/// Created by [`CompletionQueue::poll`]. This is a *lending* iterator: each [`WorkCompletion`]
/// borrows the `Completions`, so it must be dropped before the next [`next`](Completions::next)
/// call (which is why it cannot implement [`Iterator`]); [`for_each`](Self::for_each) runs a
/// closure over the rest instead. The provider holds the queue's poll lock from the poll's start
/// until the `Completions` is dropped (`ibv_end_poll`), so keep it short-lived; a poll of an
/// empty queue holds nothing.
#[must_use]
pub struct Completions<'cq> {
    cq: *mut ffi::ibv_cq_ex,
    /// Whether `start_poll` handed out an entry, and so `end_poll` is owed on drop; `false` for
    /// a poll of an empty queue.
    open: bool,
    /// Whether the next entry is the one `start_poll` positioned on (not yet yielded).
    first: bool,
    /// Whether the entries are exhausted (or the provider reported an error), so `next_poll`
    /// must not be called again.
    done: bool,
    _cq: std::marker::PhantomData<&'cq CompletionQueueInner>,
}

impl Completions<'_> {
    /// Return the next work completion, or `None` once the poll has no more.
    ///
    /// Consume with `while let Some(wc) = completions.next() { ... }`.
    ///
    /// `None` also ends the poll if the provider reports an error mid-poll (a rare provider-level
    /// failure, distinct from a completion *status* error, which is reported per work completion
    /// through [`WorkCompletion::ok`]); resources are still released correctly in that case, and
    /// later calls keep returning `None`.
    #[allow(clippy::should_implement_trait)]
    #[inline]
    pub fn next(&mut self) -> Option<WorkCompletion<'_>> {
        if self.done {
            return None;
        }
        if self.first {
            self.first = false;
        } else if unsafe { (*self.cq).next_poll.unwrap()(self.cq) } != 0 {
            // ENOENT (no more) or an error: either way the poll is finished.
            self.done = true;
            return None;
        }
        Some(WorkCompletion {
            cq: self.cq,
            _iter: std::marker::PhantomData,
        })
    }

    /// Run `f` on each remaining work completion, then release the queue.
    ///
    /// The closure form of the `while let` loop over [`next`](Self::next):
    /// `cq.poll()?.for_each(|wc| ..)`.
    #[inline]
    pub fn for_each(mut self, mut f: impl FnMut(WorkCompletion<'_>)) {
        while let Some(wc) = self.next() {
            f(wc);
        }
    }
}

impl Drop for Completions<'_> {
    fn drop(&mut self) {
        if self.open {
            unsafe { (*self.cq).end_poll.unwrap()(self.cq) };
        }
    }
}

/// A completion queue that allows subscribing to the completion of queued sends and receives.
/// Created by [`CompletionQueueBuilder::build`].
#[must_use]
#[derive(Clone)]
pub struct CompletionQueue {
    pub(crate) inner: Arc<CompletionQueueInner>,
}

impl CompletionQueue {
    /// Poll for the work completions that are ready, through the extended interface.
    ///
    /// The returned [`Completions`] is a lending iterator whose [`WorkCompletion`]s read their
    /// fields lazily, so you only pay for the fields you read; it is empty when the queue is.
    ///
    /// Callers must ensure the CQ does not overrun (exceed its [`capacity`](Self::capacity)), as
    /// this triggers an `IBV_EVENT_CQ_ERR` async event, rendering the CQ unusable. You can do
    /// this by limiting the number of inflight work requests.
    ///
    /// `poll` does not block or cause a context switch; to block until completions arrive, build
    /// the queue on a [`CompletionChannel`] and wait there instead of spinning on `poll` (see
    /// [`req_notify`](Self::req_notify) for the loop).
    ///
    /// # Errors
    ///
    ///  - [`PollCompletionQueue`](Error::PollCompletionQueue): starting the poll failed
    ///    (`ibv_start_poll`).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ibverbs::CompletionQueue;
    /// # fn drain(cq: &CompletionQueue) -> ibverbs::Result<()> {
    /// let mut completions = cq.poll()?;
    /// while let Some(wc) = completions.next() {
    ///     if let Err(e) = wc.ok() {
    ///         eprintln!("work request {}: {e}", wc.wr_id());
    ///     }
    /// }
    /// // Or, as a closure over the batch:
    /// cq.poll()?.for_each(|wc| println!("work request {} completed", wc.wr_id()));
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn poll(&self) -> Result<Completions<'_>> {
        let cq = self.inner.cq_ex;
        let mut attr = ffi::ibv_poll_cq_attr::default();
        // `start_poll` positions the CQ on the first completion; it returns ENOENT (and must not be
        // paired with `end_poll`) when the queue is empty.
        match unsafe { (*cq).start_poll.unwrap()(cq, &mut attr as *mut _) } {
            0 => Ok(Completions {
                cq,
                open: true,
                first: true,
                done: false,
                _cq: std::marker::PhantomData,
            }),
            e if e == nix::libc::ENOENT => Ok(Completions {
                cq,
                open: false,
                first: false,
                done: true,
                _cq: std::marker::PhantomData,
            }),
            e => Err(Error::errno(e, Error::PollCompletionQueue)),
        }
    }

    /// Poll for work completions through the standard `ibv_poll_cq` interface, filling `wc` with as
    /// many completions as are ready (up to its length) and returning the filled prefix.
    ///
    /// This is the batch counterpart to [`poll`](Self::poll). Where `poll` reads each field lazily
    /// through the extended interface — an indirect provider call per field — this copies whole
    /// completions into caller-owned [`ibv_wc`](ffi::ibv_wc) entries in one call, then lets you read
    /// their fields as plain struct accesses. For high-throughput draining where you consume many
    /// completions and touch several fields of each, avoiding the per-field indirection makes this
    /// the faster path; when you poll a completion at a time and read only a field or two,
    /// [`poll`](Self::poll) is cheaper. It is also a compatibility fallback for the fields the
    /// standard `ibv_wc` carries, independent of which extended fields the queue was built with.
    ///
    /// The returned slice is empty when no completions are ready. Like [`poll`](Self::poll), this
    /// neither blocks nor causes a context switch; to wait for completions, build the queue on a
    /// [`CompletionChannel`] and drive it through [`req_notify`](Self::req_notify) rather than
    /// spinning here.
    ///
    /// Callers must ensure the CQ does not overrun (exceed its capacity), as this triggers an
    /// `IBV_EVENT_CQ_ERR` async event, rendering the CQ unusable. You can do this by limiting the
    /// number of inflight work requests.
    ///
    /// # Errors
    ///
    ///  - [`PollCompletionQueue`](Error::PollCompletionQueue): `ibv_poll_cq` reported an error.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ibverbs::{ffi, CompletionQueue};
    /// # fn drain(cq: &CompletionQueue) -> ibverbs::Result<()> {
    /// let mut wc = [ffi::ibv_wc::default(); 16];
    /// for completion in cq.poll_into(&mut wc)? {
    ///     if let Some((status, vendor_err)) = completion.error() {
    ///         eprintln!("work request {}: {status:?} (vendor {vendor_err})", completion.wr_id());
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn poll_into<'w>(&self, wc: &'w mut [ffi::ibv_wc]) -> Result<&'w mut [ffi::ibv_wc]> {
        let cq = self.inner.cq();
        // `ibv_poll_cq` is a `static inline` in verbs.h that dispatches through the context op
        // table; an `ibv_cq_ex` shares its prefix with `ibv_cq`, so the standard poll works on the
        // extended queue this crate builds (exactly the `ibv_cq_ex_to_cq` path).
        let ctx = unsafe { (*cq).context };
        let num_entries = wc.len().min(i32::MAX as usize) as i32;
        let n = unsafe { (*ctx).ops.poll_cq.unwrap()(cq, num_entries, wc.as_mut_ptr()) };
        if n < 0 {
            // `ibv_poll_cq` signals failure with a negative return and does not define `errno`;
            // surface whatever the provider left there as the cause.
            return Err(Error::PollCompletionQueue(io::Error::last_os_error()));
        }
        Ok(&mut wc[..n as usize])
    }

    /// Arm the completion queue so the next work completion generates a notification on its
    /// completion channel (`ibv_req_notify_cq`).
    ///
    /// Event-driven and asynchronous completion handling builds on this: arm the
    /// queue, drain everything already pending with [`poll`](Self::poll), then wait for a
    /// notification on the [completion channel](Self::comp_channel) — either by blocking with
    /// [`CompletionChannel::wait`], or by watching its [`AsFd`] descriptor with
    /// your own reactor (for example `epoll` or a `tokio` `AsyncFd`) and consuming the notification
    /// with [`CompletionChannel::get_event`]. Then drain again and re-arm. On a queue built
    /// without a channel ([`CompletionQueueBuilder::set_comp_channel`]), arming has nothing to
    /// notify.
    ///
    /// If `solicited_only` is set, only completions of work requests that asked for a solicited
    /// event generate a notification.
    ///
    /// # Errors
    ///
    ///  - [`PollCompletionQueue`](Error::PollCompletionQueue): `ibv_req_notify_cq` failed.
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

    /// The number of completions the queue can hold: the capacity the device granted, which is at
    /// least the `min_cq_entries` asked of [`Context::create_cq`]. Keep fewer signaled work
    /// requests in flight across the queue pairs sharing this queue, or it overruns
    /// (`IBV_EVENT_CQ_ERR`) and stops working.
    pub fn capacity(&self) -> u32 {
        unsafe { (*self.inner.cq_ex).cqe }.max(0) as u32
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

#[cfg(test)]
mod test_conversions {
    use super::*;

    #[test]
    fn wc_status_roundtrip() {
        for (wrapper, raw) in [
            (WcStatus::Success, ffi::ibv_wc_status::IBV_WC_SUCCESS),
            (
                WcStatus::WorkRequestFlushed,
                ffi::ibv_wc_status::IBV_WC_WR_FLUSH_ERR,
            ),
            (
                WcStatus::RetryExceeded,
                ffi::ibv_wc_status::IBV_WC_RETRY_EXC_ERR,
            ),
            (
                WcStatus::RemoteAccessError,
                ffi::ibv_wc_status::IBV_WC_REM_ACCESS_ERR,
            ),
        ] {
            assert_eq!(WcStatus::from(raw), wrapper);
            assert_eq!(ffi::ibv_wc_status::from(wrapper), raw);
        }
    }

    #[test]
    fn wc_status_display_is_human_readable() {
        assert_eq!(WcStatus::Success.to_string(), "success");
        assert_eq!(
            WcStatus::RnrRetryExceeded.to_string(),
            "receiver-not-ready retry counter exceeded"
        );
        assert_eq!(
            WcStatus::WorkRequestFlushed.to_string(),
            "work request flushed"
        );
    }

    #[test]
    fn wc_opcode_roundtrip() {
        for (wrapper, raw) in [
            (WcOpcode::Send, ffi::ibv_wc_opcode::IBV_WC_SEND),
            (WcOpcode::RdmaRead, ffi::ibv_wc_opcode::IBV_WC_RDMA_READ),
            (WcOpcode::Recv, ffi::ibv_wc_opcode::IBV_WC_RECV),
            (
                WcOpcode::RecvRdmaWithImm,
                ffi::ibv_wc_opcode::IBV_WC_RECV_RDMA_WITH_IMM,
            ),
        ] {
            assert_eq!(WcOpcode::from(raw), wrapper);
            assert_eq!(ffi::ibv_wc_opcode::from(wrapper), raw);
        }
    }

    #[test]
    fn wc_fields_bit_ops_and_roundtrip() {
        let fields = WcFields::SLID | WcFields::SL;
        assert!(fields.contains(WcFields::SLID));
        assert!(!fields.contains(WcFields::COMPLETION_TIMESTAMP));
        let raw: ffi::ibv_create_cq_wc_flags = fields.into();
        assert_eq!(
            raw,
            ffi::ibv_create_cq_wc_flags::IBV_WC_EX_WITH_SLID
                | ffi::ibv_create_cq_wc_flags::IBV_WC_EX_WITH_SL
        );
        assert_eq!(WcFields::from(raw), fields);
        assert_eq!(format!("{fields:?}"), "WcFields(SLID | SL)");
    }

    #[test]
    fn wc_flags_bit_ops_and_roundtrip() {
        let flags = WcFlags::GRH | WcFlags::WITH_IMM;
        assert!(flags.contains(WcFlags::GRH));
        assert!(!flags.contains(WcFlags::IP_CSUM_OK));
        let raw: ffi::ibv_wc_flags = flags.into();
        assert_eq!(
            raw,
            ffi::ibv_wc_flags::IBV_WC_GRH | ffi::ibv_wc_flags::IBV_WC_WITH_IMM
        );
        assert_eq!(WcFlags::from(raw), flags);
        assert_eq!(format!("{flags:?}"), "WcFlags(GRH | WITH_IMM)");
        assert_eq!(format!("{:?}", WcFlags::empty()), "WcFlags(0)");
    }
}
