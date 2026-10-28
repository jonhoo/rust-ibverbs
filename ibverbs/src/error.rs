use std::io;

use crate::qp::{QueuePairAttributeMask, QueuePairState};

#[cfg(feature = "rdmacm")]
use crate::rdmacm::CmFailure;

#[cfg(doc)]
use crate::QueuePair;

/// A specialized [`Result`](std::result::Result) for ibverbs operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that an ibverbs operation can return.
///
/// Most variants wrap the underlying operating-system error (an `errno` from a libibverbs or
/// librdmacm call); the specific variant identifies which operation failed and carries any relevant
/// context. The others — [`Unsupported`](Error::Unsupported),
/// [`PortNotActive`](Error::PortNotActive), [`GidMismatch`](Error::GidMismatch),
/// [`GidRequired`](Error::GidRequired), [`DeviceIndexUnavailable`](Error::DeviceIndexUnavailable),
/// [`MalformedWireFormat`](Error::MalformedWireFormat), the two `InvalidQueuePair*` diagnoses,
/// and the connection manager's `TimedOut` and `ConnectionManager` — capture conditions that
/// callers commonly branch on.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The device, provider, or kernel does not support the requested operation or
    /// work-completion field; `operation` names the verb that was declined, and `errno` who
    /// declined it.
    #[error("{operation} is not supported by the device, provider, or kernel")]
    Unsupported {
        /// The verb (or the capability it asked for) the device, provider, or kernel declined. A
        /// reference to the name rather than the name itself, so that `Error` stays two words.
        operation: &'static &'static str,
        /// The errno it was declined with: `EOPNOTSUPP` when the device or provider lacks the
        /// operation, `EPROTONOSUPPORT` when the kernel is older than the request, `ENOSYS` for a
        /// connection-manager option the kernel does not know, and `ENODEV` for an mlx5 device
        /// without a Data Direct device.
        errno: i32,
    },

    /// The device port is not in the `ACTIVE` or `ARMED` state, so its GID table and routing are
    /// unusable.
    #[error("port {0} is not ACTIVE or ARMED")]
    PortNotActive(u8),

    /// A global identifier (GID) was set for the remote endpoint but not the local one.
    #[error("a GID was set for the remote endpoint but not the local one")]
    GidMismatch,

    /// The local port is RoCE (Ethernet), which routes every packet by GID, but the remote endpoint
    /// carries none: set a GID index on the builders of both ends
    /// ([`QueuePairBuilder::set_gid_index`](crate::QueuePairBuilder::set_gid_index)).
    #[error(
        "RoCE routes by GID, but the remote endpoint carries none (set a GID index on both ends)"
    )]
    GidRequired,

    /// The device does not expose a stable kernel index.
    #[error("the device index is not known")]
    DeviceIndexUnavailable,

    /// Listing the available RDMA devices failed (`ibv_get_device_list`).
    #[error("failed to list RDMA devices")]
    GetDeviceList(#[source] io::Error),

    /// Opening the device failed (`ibv_open_device`).
    #[error("failed to open the RDMA device")]
    OpenDevice(#[source] io::Error),

    /// Reading the device GUID failed (`ibv_get_device_guid`).
    #[error("failed to read the device GUID")]
    DeviceGuid(#[source] io::Error),

    /// Querying device attributes failed (`ibv_query_device`).
    #[error("failed to query device attributes")]
    QueryDevice(#[source] io::Error),

    /// Querying port attributes failed (`ibv_query_port`).
    #[error("failed to query attributes of port {port_num}")]
    QueryPort {
        /// The port that was queried.
        port_num: u8,
        /// The underlying error.
        #[source]
        source: io::Error,
    },

    /// Querying a GID failed (`ibv_query_gid`).
    #[error("failed to query GID index {gid_index} on port {port_num}")]
    QueryGid {
        /// The port that was queried.
        port_num: u8,
        /// The GID table index that was queried.
        gid_index: u32,
        /// The underlying error.
        #[source]
        source: io::Error,
    },

    /// Querying the GID table failed (`ibv_query_gid_table`).
    #[error("failed to query the GID table")]
    QueryGidTable(#[source] io::Error),

    /// Querying the device's real-time values failed (`ibv_query_rt_values_ex`).
    #[error("failed to query the device real-time values")]
    QueryRealTimeValues(#[source] io::Error),

    /// Querying the mlx5 Data Direct sysfs path failed (`mlx5dv_get_data_direct_sysfs_path`).
    #[cfg(feature = "mlx5")]
    #[error("failed to query the mlx5 Data Direct sysfs path")]
    QueryMlx5DataDirectSysfsPath(#[source] io::Error),

    /// Allocating a protection domain failed (`ibv_alloc_pd`).
    #[error("failed to allocate a protection domain")]
    AllocProtectionDomain(#[source] io::Error),

    /// Registering a memory region failed (`ibv_reg_mr` / `ibv_reg_dmabuf_mr`).
    #[error("failed to register a memory region")]
    RegisterMemoryRegion(#[source] io::Error),

    /// Giving advice about a memory region failed (`ibv_advise_mr`).
    #[error("failed to advise on a memory region")]
    AdviseMemoryRegion(#[source] io::Error),

    /// Creating a completion channel failed (`ibv_create_comp_channel`).
    #[error("failed to create a completion channel")]
    CreateCompletionChannel(#[source] io::Error),

    /// Creating a completion queue failed (`ibv_create_cq_ex`).
    #[error("failed to create a completion queue")]
    CreateCompletionQueue(#[source] io::Error),

    /// Creating a queue pair failed (`ibv_create_qp_ex` / `efadv_create_qp_ex`).
    #[error("failed to create a queue pair")]
    CreateQueuePair(#[source] io::Error),

    /// Transitioning a queue pair to a new state failed (`ibv_modify_qp`).
    #[error("failed to transition the queue pair state")]
    ModifyQueuePair(#[source] io::Error),

    /// A queue-pair state transition was rejected because it is not a legal transition for this
    /// queue-pair type.
    ///
    /// Surfaced by [`QueuePair::modify`] when the device rejects the transition and the crate's
    /// state-table check confirms that `current -> next` is not allowed.
    #[error("invalid queue pair state transition from {current} to {next}")]
    InvalidQueuePairTransition {
        /// The queue pair's current state.
        current: QueuePairState,
        /// The requested next state.
        next: QueuePairState,
    },

    /// A queue-pair state transition was rejected because its attribute mask was wrong.
    ///
    /// Surfaced by [`QueuePair::modify`]: `invalid` are bits that were set but are not allowed for
    /// the transition, and `needed` are bits that the transition requires but that were not set.
    #[error(
        "invalid attribute mask for queue pair transition from {current} to {next}: \
         disallowed bits {invalid:?}, missing required bits {needed:?}"
    )]
    InvalidQueuePairAttributeMask {
        /// The queue pair's current state.
        current: QueuePairState,
        /// The requested next state.
        next: QueuePairState,
        /// Attribute bits that were set but are not allowed for this transition.
        invalid: QueuePairAttributeMask,
        /// Attribute bits that the transition requires but that were not set.
        needed: QueuePairAttributeMask,
    },

    /// Querying queue pair attributes failed (`ibv_query_qp`).
    #[error("failed to query queue pair attributes")]
    QueryQueuePair(#[source] io::Error),

    /// Creating an address handle failed (`ibv_create_ah`).
    #[error("failed to create an address handle")]
    CreateAddressHandle(#[source] io::Error),

    /// Creating a shared receive queue failed (`ibv_create_srq`).
    #[error("failed to create a shared receive queue")]
    CreateSharedReceiveQueue(#[source] io::Error),

    /// Modifying a shared receive queue failed (`ibv_modify_srq`).
    #[error("failed to modify the shared receive queue")]
    ModifySharedReceiveQueue(#[source] io::Error),

    /// Querying a shared receive queue failed (`ibv_query_srq`).
    #[error("failed to query the shared receive queue")]
    QuerySharedReceiveQueue(#[source] io::Error),

    /// Posting a send work request failed.
    #[error("failed to post a send work request")]
    PostSend(#[source] io::Error),

    /// Posting a receive work request failed.
    #[error("failed to post a receive work request")]
    PostReceive(#[source] io::Error),

    /// Polling a completion queue failed.
    #[error("failed to poll the completion queue")]
    PollCompletionQueue(#[source] io::Error),

    /// Waiting for or reading a device asynchronous event failed (`poll` /
    /// `ibv_get_async_event`).
    #[error("failed to read a device asynchronous event")]
    AsyncEvent(#[source] io::Error),

    /// A blocking connection-manager helper reached its timeout before the awaited event arrived.
    #[cfg(feature = "rdmacm")]
    #[error("the connection-manager operation timed out")]
    TimedOut,

    /// Decoding a wire-format value ([`QueuePairEndpoint::from_bytes`](crate::QueuePairEndpoint::from_bytes)
    /// or [`RemoteMemorySlice::from_bytes`](crate::RemoteMemorySlice::from_bytes)) failed: the
    /// bytes carry a flag or value this version does not understand. A length in a
    /// [`RemoteMemorySlice`](crate::RemoteMemorySlice) encoding that does not fit the platform's
    /// `usize` also produces this error, and so does a queue pair number or PSN wider than its 24
    /// bits in a [`QueuePairEndpoint`](crate::QueuePairEndpoint) encoding.
    #[error("malformed wire-format encoding")]
    MalformedWireFormat,

    /// The connection manager reported a failure event while a blocking helper was waiting for
    /// the next setup step. Boxed, so that `Error` stays two words.
    #[cfg(feature = "rdmacm")]
    #[error(transparent)]
    ConnectionManager(Box<CmFailure>),

    /// Binding a connection-manager identifier to a local address failed (`rdma_bind_addr`).
    #[cfg(feature = "rdmacm")]
    #[error("failed to bind to a local address")]
    BindAddress(#[source] io::Error),

    /// Resolving the destination address failed (`rdma_resolve_addr`).
    #[cfg(feature = "rdmacm")]
    #[error("failed to resolve the destination address")]
    ResolveAddress(#[source] io::Error),

    /// Resolving the route to the destination failed (`rdma_resolve_route`).
    #[cfg(feature = "rdmacm")]
    #[error("failed to resolve the route to the destination")]
    ResolveRoute(#[source] io::Error),

    /// Establishing a connection failed (`rdma_connect` / `rdma_establish`).
    #[cfg(feature = "rdmacm")]
    #[error("failed to establish the connection")]
    Connect(#[source] io::Error),

    /// Accepting an incoming connection failed (`rdma_accept`).
    #[cfg(feature = "rdmacm")]
    #[error("failed to accept the connection")]
    Accept(#[source] io::Error),

    /// Another connection-manager setup step failed (creating the id, listening, getting an event,
    /// disconnecting, ...).
    #[cfg(feature = "rdmacm")]
    #[error("failed to set up the connection manager")]
    ConnectionSetup(#[source] io::Error),
}

impl Error {
    /// The operating-system error (`errno`) underlying this error, for the variants that wrap one:
    /// the uniform way to branch on `ENOMEM`, `EINVAL`, and friends without matching every
    /// variant. `None` for the variants this crate diagnoses itself, and for
    /// [`Unsupported`](Error::Unsupported), which carries its errno as a field instead. The
    /// crate's checks of its own inputs (a queue from
    /// another context, oversized private data, and the like) are reported in the operation's
    /// variant as an [`io::ErrorKind::InvalidInput`] error with no errno, so for them this is
    /// `Some` but [`raw_os_error`](io::Error::raw_os_error) is `None`. For the connection
    /// manager's `ConnectionManager`, it is the errno a negative status stands for (an address,
    /// route, or connection error), and `None` for a reject reason.
    pub fn os_error(&self) -> Option<&io::Error> {
        std::error::Error::source(self)?.downcast_ref::<io::Error>()
    }

    /// The verb (or step) this error came from, as [`Unsupported`](Self::Unsupported) names it.
    pub(crate) fn operation(&self) -> &'static &'static str {
        match self {
            Error::Unsupported { operation, .. } => operation,
            Error::PortNotActive(_) => &"port activation",
            Error::GidMismatch | Error::GidRequired => &"GID routing",
            Error::DeviceIndexUnavailable => &"the device index",
            Error::GetDeviceList(_) => &"ibv_get_device_list",
            Error::OpenDevice(_) => &"ibv_open_device",
            Error::DeviceGuid(_) => &"ibv_get_device_guid",
            Error::QueryDevice(_) => &"ibv_query_device",
            Error::QueryPort { .. } => &"ibv_query_port",
            Error::QueryGid { .. } => &"ibv_query_gid",
            Error::QueryGidTable(_) => &"ibv_query_gid_table",
            Error::QueryRealTimeValues(_) => &"ibv_query_rt_values_ex",
            #[cfg(feature = "mlx5")]
            Error::QueryMlx5DataDirectSysfsPath(_) => &"mlx5dv_get_data_direct_sysfs_path",
            Error::AllocProtectionDomain(_) => &"ibv_alloc_pd",
            Error::RegisterMemoryRegion(_) => &"ibv_reg_mr",
            Error::AdviseMemoryRegion(_) => &"ibv_advise_mr",
            Error::CreateCompletionChannel(_) => &"ibv_create_comp_channel",
            Error::CreateCompletionQueue(_) => &"ibv_create_cq_ex",
            Error::CreateQueuePair(_) => &"ibv_create_qp_ex",
            Error::ModifyQueuePair(_)
            | Error::InvalidQueuePairTransition { .. }
            | Error::InvalidQueuePairAttributeMask { .. } => &"ibv_modify_qp",
            Error::QueryQueuePair(_) => &"ibv_query_qp",
            Error::CreateAddressHandle(_) => &"ibv_create_ah",
            Error::CreateSharedReceiveQueue(_) => &"ibv_create_srq",
            Error::ModifySharedReceiveQueue(_) => &"ibv_modify_srq",
            Error::QuerySharedReceiveQueue(_) => &"ibv_query_srq",
            Error::PostSend(_) => &"ibv_wr_complete",
            Error::PostReceive(_) => &"ibv_post_recv",
            Error::PollCompletionQueue(_) => &"completion-queue polling",
            Error::AsyncEvent(_) => &"ibv_get_async_event",
            Error::MalformedWireFormat => &"wire-format decoding",
            #[cfg(feature = "rdmacm")]
            Error::TimedOut => &"the connection-manager wait",
            #[cfg(feature = "rdmacm")]
            Error::ConnectionManager(_) => &"the connection manager",
            #[cfg(feature = "rdmacm")]
            Error::BindAddress(_) => &"rdma_bind_addr",
            #[cfg(feature = "rdmacm")]
            Error::ResolveAddress(_) => &"rdma_resolve_addr",
            #[cfg(feature = "rdmacm")]
            Error::ResolveRoute(_) => &"rdma_resolve_route",
            #[cfg(feature = "rdmacm")]
            Error::Connect(_) => &"rdma_connect",
            #[cfg(feature = "rdmacm")]
            Error::Accept(_) => &"rdma_accept",
            #[cfg(feature = "rdmacm")]
            Error::ConnectionSetup(_) => &"connection-manager setup",
        }
    }

    /// Whether `err` says the operation is unsupported: `EOPNOTSUPP` from the provider or kernel,
    /// or `EPROTONOSUPPORT`, which the kernel's ioctl dispatch returns for a method or attribute
    /// it does not know (a kernel older than the request) and reserves for exactly that: a
    /// driver's own `EPROTONOSUPPORT` reaches userspace as `EINVAL` (`uverbs_ioctl.c`).
    pub(crate) fn is_unsupported(err: &io::Error) -> bool {
        matches!(
            err.raw_os_error(),
            Some(nix::libc::EOPNOTSUPP | nix::libc::EPROTONOSUPPORT)
        )
    }

    /// Build an [`Error`] from an OS error, promoting an unsupported one (see
    /// [`is_unsupported`](Error::is_unsupported)) to [`Error::Unsupported`]
    /// (naming the operation `wrap` identifies) and otherwise tagging it with `wrap` (the variant
    /// identifying the operation that failed).
    pub(crate) fn os(err: io::Error, wrap: impl FnOnce(io::Error) -> Error) -> Error {
        let unsupported = err.raw_os_error().filter(|_| Error::is_unsupported(&err));
        let wrapped = wrap(err);
        match unsupported {
            Some(errno) => Error::Unsupported {
                operation: wrapped.operation(),
                errno,
            },
            None => wrapped,
        }
    }

    /// This error, naming `operation` as the declined verb if it is
    /// [`Unsupported`](Error::Unsupported): for the verbs that report their failures through a
    /// variant whose own [`operation`](Error::operation) names another verb.
    pub(crate) fn for_operation(self, operation: &'static &'static str) -> Error {
        match self {
            Error::Unsupported { errno, .. } => Error::Unsupported { operation, errno },
            other => other,
        }
    }

    /// As [`os`](Error::os), but from a raw `errno`.
    ///
    /// Cold and out of line: the data-path calls (`submit`, `post_recv`, `poll`) report failures
    /// through this, and inlining the construction would bloat every one of their call sites.
    #[cold]
    #[inline(never)]
    pub(crate) fn errno(errno: i32, wrap: impl FnOnce(io::Error) -> Error) -> Error {
        Error::os(io::Error::from_raw_os_error(errno), wrap)
    }
}

/// `ptr`, or the error `wrap` makes of `errno` if it is null: for the verbs that return null and
/// set `errno` on failure.
pub(crate) fn nonnull<T>(ptr: *mut T, wrap: impl FnOnce(io::Error) -> Error) -> Result<*mut T> {
    if ptr.is_null() {
        return Err(Error::os(io::Error::last_os_error(), wrap));
    }
    Ok(ptr)
}

/// `Ok` if `ret` is 0, else the error `wrap` makes of `errno`: for the verbs that return -1 and set
/// `errno` on failure. (The verbs that return the `errno` itself go through [`Error::errno`].)
pub(crate) fn check(ret: i32, wrap: impl FnOnce(io::Error) -> Error) -> Result<()> {
    if ret != 0 {
        return Err(Error::os(io::Error::last_os_error(), wrap));
    }
    Ok(())
}

/// Panics with the error a destroy verb (`ibv_destroy_qp`, `ibv_dereg_mr`, ...) returned, if any:
/// the `Drop` impls cannot report it, and must not carry on as if the object were gone.
///
/// The object still exists then, and so must the objects it was created on: `keep_parents` leaks
/// the references it holds to them before the panic unwinds through its fields. Released, the last
/// of them would destroy its object under this one, fail the same way, and panic again during the
/// unwind, which aborts the process.
#[track_caller]
pub(crate) fn destroyed(verb: &str, errno: i32, keep_parents: impl FnOnce()) {
    if errno != 0 {
        keep_parents();
        panic!("{verb} failed: {}", io::Error::from_raw_os_error(errno));
    }
}

/// Leaks one reference to `parent`, for [`destroyed`]'s `keep_parents`.
pub(crate) fn keep<T: Clone>(parent: &T) {
    std::mem::forget(parent.clone());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eopnotsupp_is_promoted_naming_the_operation() {
        let err = Error::os(
            io::Error::from_raw_os_error(nix::libc::EOPNOTSUPP),
            Error::CreateQueuePair,
        );
        assert!(
            matches!(
                err,
                Error::Unsupported {
                    operation: &"ibv_create_qp_ex",
                    errno: nix::libc::EOPNOTSUPP,
                }
            ),
            "{err:?}"
        );
        assert!(err.os_error().is_none());
        assert_eq!(
            err.to_string(),
            "ibv_create_qp_ex is not supported by the device, provider, or kernel"
        );
    }

    #[test]
    fn an_older_kernel_is_unsupported_too() {
        let err = Error::errno(nix::libc::EPROTONOSUPPORT, Error::RegisterMemoryRegion);
        assert!(
            matches!(
                err,
                Error::Unsupported {
                    operation: &"ibv_reg_mr",
                    errno: nix::libc::EPROTONOSUPPORT,
                }
            ),
            "{err:?}"
        );
    }

    #[test]
    fn a_shared_variant_names_the_verb_that_was_declined() {
        let err = Error::errno(nix::libc::EOPNOTSUPP, Error::RegisterMemoryRegion)
            .for_operation(&"ibv_reg_dmabuf_mr");
        assert!(
            matches!(
                err,
                Error::Unsupported {
                    operation: &"ibv_reg_dmabuf_mr",
                    errno: nix::libc::EOPNOTSUPP,
                }
            ),
            "{err:?}"
        );
        let err = Error::errno(nix::libc::ENOMEM, Error::RegisterMemoryRegion)
            .for_operation(&"ibv_reg_dmabuf_mr");
        assert!(matches!(err, Error::RegisterMemoryRegion(_)), "{err:?}");
    }

    #[test]
    fn other_errnos_keep_their_variant_and_expose_the_os_error() {
        let err = Error::errno(nix::libc::ENOMEM, Error::CreateQueuePair);
        assert!(matches!(err, Error::CreateQueuePair(_)), "{err:?}");
        assert_eq!(
            err.os_error().and_then(io::Error::raw_os_error),
            Some(nix::libc::ENOMEM)
        );
        assert!(Error::GidMismatch.os_error().is_none());
    }

    /// A failed destroy panics without releasing its parents: had the child held the last
    /// reference, the parent's destroy would run during the unwind, fail too (the child still
    /// exists), and its second panic would abort the process instead of unwinding to here.
    #[test]
    #[cfg_attr(miri, ignore = "leaks the parent on purpose, as a failed destroy does")]
    fn a_failed_destroy_keeps_its_parents() {
        use std::panic::catch_unwind;
        use std::sync::Arc;

        struct Parent;
        impl Drop for Parent {
            fn drop(&mut self) {
                destroyed("ibv_dealloc_pd", nix::libc::EBUSY, || ());
            }
        }
        struct Child {
            parent: Arc<Parent>,
        }
        impl Drop for Child {
            fn drop(&mut self) {
                destroyed("ibv_dereg_mr", nix::libc::EBUSY, || keep(&self.parent));
            }
        }

        let child = Child {
            parent: Arc::new(Parent),
        };
        let parent = Arc::downgrade(&child.parent);
        assert!(catch_unwind(move || drop(child)).is_err());
        assert!(parent.upgrade().is_some(), "the parent was released");
    }

    #[test]
    fn error_fits_in_two_words() {
        // Returned in registers rather than through memory, on every data-path call.
        let two_words = 2 * std::mem::size_of::<usize>();
        assert!(std::mem::size_of::<Error>() <= two_words);
        assert!(std::mem::size_of::<Result<()>>() <= two_words);
    }
}
