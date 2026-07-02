use std::io;

#[cfg(doc)]
use crate::QueuePair;

/// A specialized [`Result`](std::result::Result) for ibverbs operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that an ibverbs operation can return.
///
/// Most variants wrap the underlying operating-system error (an `errno` from a libibverbs or
/// librdmacm call); the specific variant identifies which operation failed and carries any relevant
/// context. A few variants ([`Unsupported`](Error::Unsupported), ...) capture conditions that
/// callers commonly branch on.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The device or provider does not support the requested operation or work-completion field
    /// (`EOPNOTSUPP`).
    #[error("operation not supported by the device")]
    Unsupported,

    /// The device port is not in the `ACTIVE` or `ARMED` state, so its GID table and routing are
    /// unusable.
    #[error("port {0} is not ACTIVE or ARMED")]
    PortNotActive(u8),

    /// A global routing identifier (GID) was set for the remote endpoint but not the local one.
    #[error("a GID was set for the remote endpoint but not the local one")]
    GidMismatch,

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
    #[error("invalid queue pair state transition from {current:?} to {next:?}")]
    InvalidQueuePairTransition {
        /// The queue pair's current state.
        current: ffi::ibv_qp_state,
        /// The requested next state.
        next: ffi::ibv_qp_state,
    },

    /// A queue-pair state transition was rejected because its attribute mask was wrong.
    ///
    /// Surfaced by [`QueuePair::modify`]: `invalid` are bits that were set but are not allowed for
    /// the transition, and `needed` are bits that the transition requires but that were not set.
    #[error(
        "invalid attribute mask for queue pair transition from {current:?} to {next:?}: \
         disallowed bits {invalid:?}, missing required bits {needed:?}"
    )]
    InvalidQueuePairAttributeMask {
        /// The queue pair's current state.
        current: ffi::ibv_qp_state,
        /// The requested next state.
        next: ffi::ibv_qp_state,
        /// Attribute bits that were set but are not allowed for this transition.
        invalid: ffi::ibv_qp_attr_mask,
        /// Attribute bits that the transition requires but that were not set.
        needed: ffi::ibv_qp_attr_mask,
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

    /// Posting a send work request failed.
    #[error("failed to post a send work request")]
    PostSend(#[source] io::Error),

    /// Posting a receive work request failed.
    #[error("failed to post a receive work request")]
    PostReceive(#[source] io::Error),

    /// Polling a completion queue failed.
    #[error("failed to poll the completion queue")]
    PollCompletionQueue(#[source] io::Error),

    /// The connection manager reported a failure event.
    #[cfg(feature = "rdmacm")]
    #[error("the connection manager reported {0:?}")]
    ConnectionManager(ffi::rdma_cm_event_type),

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
    #[error("connection manager setup failed")]
    ConnectionSetup(#[source] io::Error),
}

impl Error {
    /// Build an [`Error`] from an OS error, promoting `EOPNOTSUPP` to [`Error::Unsupported`] and
    /// otherwise tagging it with `wrap` (the variant identifying the operation that failed).
    pub(crate) fn os(err: io::Error, wrap: impl FnOnce(io::Error) -> Error) -> Error {
        if err.raw_os_error() == Some(nix::libc::EOPNOTSUPP) {
            Error::Unsupported
        } else {
            wrap(err)
        }
    }

    /// As [`os`](Error::os), but from a raw `errno`.
    pub(crate) fn errno(errno: i32, wrap: impl FnOnce(io::Error) -> Error) -> Error {
        Error::os(io::Error::from_raw_os_error(errno), wrap)
    }
}
