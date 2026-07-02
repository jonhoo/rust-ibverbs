//! Rust API wrapping the `ibverbs` RDMA library.
//!
//! `libibverbs` is a library that allows userspace processes to use RDMA "verbs" to perform
//! high-throughput, low-latency network operations for both Infiniband (according to the
//! Infiniband specifications) and iWarp (iWARP verbs specifications). It handles the control path
//! of creating, modifying, querying and destroying resources such as Protection Domains,
//! Completion Queues, Queue-Pairs, Shared Receive Queues, Address Handles, and Memory Regions. It
//! also handles sending and receiving data posted to QPs and SRQs, and getting completions from
//! CQs using polling and completions events.
//!
//! A good place to start is to look at the programs in [`examples/`](examples/), and the upstream
//! [C examples]. You can test RDMA programs on modern Linux kernels even without specialized RDMA
//! hardware by using [SoftRoCE][soft].
//!
//! # For the detail-oriented
//!
//! The control path is implemented through system calls to the `uverbs` kernel module, which
//! further calls the low-level HW driver. The data path is implemented through calls made to
//! low-level HW library which, in most cases, interacts directly with the HW provides kernel and
//! network stack bypass (saving context/mode switches) along with zero copy and an asynchronous
//! I/O model.
//!
//! iWARP ethernet NICs support RDMA over hardware-offloaded TCP/IP, while InfiniBand is a general
//! high-throughput, low-latency networking technology. InfiniBand host channel adapters (HCAs) and
//! iWARP NICs commonly support direct hardware access from userspace (kernel bypass), and
//! `libibverbs` supports this when available.
//!
//! For more information on RDMA verbs, see the [InfiniBand Architecture Specification][infini]
//! vol. 1, especially chapter 11, and the RDMA Consortium's [RDMA Protocol Verbs
//! Specification][RFC5040]. See also the upstream [`libibverbs/verbs.h`] file for the original C
//! definitions, as well as the manpages for the `ibv_*` methods.
//!
//! # Library dependency
//!
//! `libibverbs` is usually available as a free-standing [library package]. It [used to be][1]
//! self-contained, but has recently been adopted into [`rdma-core`]. `cargo` will automatically
//! build the necessary library files and place them in `vendor/rdma-core/build/lib`. If a
//! system-wide installation is not available, those library files can be used instead by copying
//! them to `/usr/lib`, or by adding that path to the dynamic linking search path.
//!
//! # Thread safety
//!
//! All interfaces are `Sync` and `Send` since the underlying ibverbs API [is thread safe][safe].
//!
//! # Documentation
//!
//! Much of the documentation of this crate borrows heavily from the excellent posts over at
//! [RDMAmojo]. If you are going to be working a lot with ibverbs, chances are you will want to
//! head over there. In particular, [this overview post][1] may be a good place to start.
//!
//! [`rdma-core`]: https://github.com/linux-rdma/rdma-core
//! [`libibverbs/verbs.h`]: https://github.com/linux-rdma/rdma-core/blob/master/libibverbs/verbs.h
//! [library package]: https://launchpad.net/ubuntu/+source/libibverbs
//! [C examples]: https://github.com/linux-rdma/rdma-core/tree/master/libibverbs/examples
//! [1]: https://git.kernel.org/pub/scm/libs/infiniband/libibverbs.git/about/
//! [infini]: http://www.infinibandta.org/content/pages.php?pg=technology_public_specification
//! [RFC5040]: https://tools.ietf.org/html/rfc5040
//! [safe]: http://www.rdmamojo.com/2013/07/26/libibverbs-thread-safe-level/
//! [soft]: https://github.com/SoftRoCE/rxe-dev/wiki/rxe-dev:-Home
//! [RDMAmojo]: http://www.rdmamojo.com/
//! [1]: http://www.rdmamojo.com/2012/05/18/libibverbs/

#![deny(missing_docs)]
#![warn(rust_2018_idioms)]
// avoid warnings about RDMAmojo, iWARP, InfiniBand, etc. not being in backticks
#![allow(clippy::doc_markdown)]

use std::borrow::Cow;
use std::convert::TryInto;
use std::ffi::CStr;
use std::fmt;
use std::io;
use std::ops::{Deref, DerefMut, RangeBounds};
use std::os::fd::{AsFd, BorrowedFd};
use std::os::raw::c_void;
use std::ptr;
use std::sync::Arc;
use std::time::{Duration, Instant};

const PORT_NUM: u8 = 1;

/// The RDMA connection manager (`librdmacm`): set up reliable connections without exchanging queue
/// pair endpoints out of band.
#[cfg(feature = "rdmacm")]
pub mod rdmacm;

/// Direct access to low-level libverbs FFI.
pub use ffi::ibv_gid_type;
pub use ffi::ibv_mtu;
pub use ffi::ibv_port_state;
pub use ffi::ibv_qp_attr_mask;
pub use ffi::ibv_qp_state;
pub use ffi::ibv_qp_type;
pub use ffi::ibv_send_wr;
pub use ffi::ibv_transport_type;
pub use ffi::ibv_wc;
pub use ffi::ibv_wc_flags;
pub use ffi::ibv_wc_opcode;
pub use ffi::ibv_wc_status;

/// Optional work-completion fields to request via [`CompletionQueueBuilder::set_wc_flags`].
pub use ffi::ibv_create_cq_wc_flags;

/// The raw device-wide attributes wrapped by [`DeviceAttr`] (returned by [`Context::query_device`]).
pub use ffi::ibv_device_attr;
/// The raw per-port attributes wrapped by [`PortAttr`] (returned by [`Context::query_port`]).
pub use ffi::ibv_port_attr;

/// Advice for [`ProtectionDomain::advise_mr`] (the `IBV_ADVISE_MR_ADVICE_*` values).
pub use ffi::ib_uverbs_advise_mr_advice as ibv_advise_mr_advice;
/// Flags for [`ProtectionDomain::advise_mr`] (the `IBV_ADVISE_MR_FLAG_*` values).
pub use ffi::ib_uverbs_advise_mr_flag as ibv_advise_mr_flags;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Access flags for use with `QueuePair` and `MemoryRegion`.
pub use ffi::ibv_access_flags;

/// Default access flags.
pub const DEFAULT_ACCESS_FLAGS: ffi::ibv_access_flags = ffi::ibv_access_flags(
    ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0
        | ffi::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE.0
        | ffi::ibv_access_flags::IBV_ACCESS_REMOTE_READ.0
        | ffi::ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC.0
        | ffi::ibv_access_flags::IBV_ACCESS_RELAXED_ORDERING.0,
);

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
    fn os(err: io::Error, wrap: impl FnOnce(io::Error) -> Error) -> Error {
        if err.raw_os_error() == Some(nix::libc::EOPNOTSUPP) {
            Error::Unsupported
        } else {
            wrap(err)
        }
    }

    /// As [`os`](Error::os), but from a raw `errno`.
    fn errno(errno: i32, wrap: impl FnOnce(io::Error) -> Error) -> Error {
        Error::os(io::Error::from_raw_os_error(errno), wrap)
    }
}

/// Get list of available RDMA devices.
///
/// # Errors
///
///  - `EPERM`: Permission denied.
///  - `ENOMEM`: Insufficient memory to complete the operation.
///  - `ENOSYS`: No kernel support for RDMA.
pub fn devices() -> Result<DeviceList> {
    let mut n = 0i32;
    let devices = unsafe { ffi::ibv_get_device_list(&mut n as *mut _) };

    if devices.is_null() {
        return Err(Error::os(io::Error::last_os_error(), Error::GetDeviceList));
    }

    let devices = unsafe {
        use std::slice;
        slice::from_raw_parts_mut(devices, n as usize)
    };
    Ok(DeviceList(devices))
}

/// List of available RDMA devices.
#[must_use]
pub struct DeviceList(&'static mut [*mut ffi::ibv_device]);

unsafe impl Sync for DeviceList {}
unsafe impl Send for DeviceList {}

impl Drop for DeviceList {
    fn drop(&mut self) {
        unsafe { ffi::ibv_free_device_list(self.0.as_mut_ptr()) };
    }
}

impl DeviceList {
    /// Returns an iterator over all found devices.
    pub fn iter(&self) -> DeviceListIter<'_> {
        DeviceListIter { list: self, i: 0 }
    }

    /// Returns the number of devices.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if there are any devices.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the device at the given `index`, or `None` if out of bounds.
    pub fn get(&self, index: usize) -> Option<Device<'_>> {
        self.0.get(index).map(|d| d.into())
    }
}

impl<'a> IntoIterator for &'a DeviceList {
    type Item = <DeviceListIter<'a> as Iterator>::Item;
    type IntoIter = DeviceListIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        DeviceListIter { list: self, i: 0 }
    }
}

/// Iterator over a `DeviceList`.
pub struct DeviceListIter<'iter> {
    list: &'iter DeviceList,
    i: usize,
}

impl<'iter> Iterator for DeviceListIter<'iter> {
    type Item = Device<'iter>;
    fn next(&mut self) -> Option<Self::Item> {
        let e = self.list.0.get(self.i);
        if e.is_some() {
            self.i += 1;
        }
        e.map(|e| e.into())
    }
}

/// An RDMA device.
pub struct Device<'devlist>(&'devlist *mut ffi::ibv_device);
unsafe impl Sync for Device<'_> {}
unsafe impl Send for Device<'_> {}

impl<'d> From<&'d *mut ffi::ibv_device> for Device<'d> {
    fn from(d: &'d *mut ffi::ibv_device) -> Self {
        Device(d)
    }
}

impl fmt::Debug for Device<'_> {
    /// Shows the device name and GUID. The GUID is read best-effort and shown as `None` if it
    /// cannot be queried.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Device")
            .field("name", &self.name())
            .field("guid", &self.guid().ok())
            .finish()
    }
}

/// A Global unique identifier for ibv.
///
/// This struct acts as a rust wrapper for GUID value represented as `__be64` in
/// libibverbs. We introduce this struct, because u64 is stored in host
/// endianness, whereas ibverbs stores GUID in network order (big endian).
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Default, Copy, Clone, Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct Guid {
    raw: [u8; 8],
}

impl Guid {
    /// Upper 24 bits of the GUID are OUI (Organizationally Unique Identifier,
    /// http://standards-oui.ieee.org/oui/oui.txt). The function returns OUI as
    /// a 24-bit number inside a u32.
    pub fn oui(&self) -> u32 {
        let padded = [0, self.raw[0], self.raw[1], self.raw[2]];
        u32::from_be_bytes(padded)
    }

    /// Returns `true` if this GUID is all zeroes, which is considered reserved.
    pub fn is_reserved(&self) -> bool {
        self.raw == [0; 8]
    }
}

impl fmt::Display for Guid {
    /// Formats the GUID as four colon-separated 16-bit groups, the conventional `ibv_devinfo`
    /// rendering, for example `0002:c903:00a0:7c8e`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let r = &self.raw;
        write!(
            f,
            "{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}",
            r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]
        )
    }
}

impl fmt::Debug for Guid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Guid({self})")
    }
}

impl From<u64> for Guid {
    fn from(guid: u64) -> Self {
        Self {
            raw: guid.to_be_bytes(),
        }
    }
}

impl From<Guid> for u64 {
    fn from(guid: Guid) -> Self {
        u64::from_be_bytes(guid.raw)
    }
}

impl AsRef<ffi::__be64> for Guid {
    fn as_ref(&self) -> &ffi::__be64 {
        unsafe { &*self.raw.as_ptr().cast::<ffi::__be64>() }
    }
}

impl<'devlist> Device<'devlist> {
    /// Opens an RMDA device and creates a context for further use.
    ///
    /// This context will later be used to query its resources or for creating resources.
    ///
    /// Unlike what the verb name suggests, it doesn't actually open the device. This device was
    /// opened by the kernel low-level driver and may be used by other user/kernel level code. This
    /// verb only opens a context to allow user level applications to use it.
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: port 1 is invalid (from `ibv_query_port_attr`).
    ///  - `ENOMEM`: Out of memory (from `ibv_query_port_attr`).
    ///  - `EMFILE`: Too many files are opened by this process (from `ibv_query_gid`).
    ///  - Other: port 1 is not in `ACTIVE` or `ARMED` state.
    pub fn open(&self) -> Result<Context> {
        Context::with_device(*self.0)
    }

    /// Returns a string of the name, which is associated with this RDMA device.
    ///
    /// This name is unique within a specific machine (the same name cannot be assigned to more
    /// than one device). However, this name isn't unique across an InfiniBand fabric (this name
    /// can be found in different machines).
    ///
    /// When there are more than one RDMA devices in a computer, changing the device location in
    /// the computer (i.e. in the PCI bus) may result a change in the names associated with the
    /// devices. In order to distinguish between the device, it is recommended using the device
    /// GUID, returned by `Device::guid`.
    ///
    /// The name is composed from:
    ///
    ///  - a *prefix* which describes the RDMA device vendor and model
    ///    - `cxgb3` - Chelsio Communications, T3 RDMA family
    ///    - `cxgb4` - Chelsio Communications, T4 RDMA family
    ///    - `ehca` - IBM, eHCA family
    ///    - `ipathverbs` - QLogic
    ///    - `mlx4` - Mellanox Technologies, ConnectX family
    ///    - `mthca` - Mellanox Technologies, InfiniHost family
    ///    - `nes` - Intel, Intel-NE family
    ///  - an *index* that helps to differentiate between several devices from the same vendor and
    ///    family in the same computer
    pub fn name(&self) -> Option<&'devlist CStr> {
        let name_ptr = unsafe { ffi::ibv_get_device_name(*self.0) };
        if name_ptr.is_null() {
            None
        } else {
            Some(unsafe { CStr::from_ptr(name_ptr) })
        }
    }

    /// Returns the Global Unique IDentifier (GUID) of this RDMA device.
    ///
    /// This GUID, that was assigned to this device by its vendor during the manufacturing, is
    /// unique and can be used as an identifier to an RDMA device.
    ///
    /// From the prefix of the RDMA device GUID, one can know who is the vendor of that device
    /// using the [IEEE OUI](http://standards.ieee.org/develop/regauth/oui/oui.txt).
    ///
    /// # Errors
    ///
    ///  - `EMFILE`: Too many files are opened by this process.
    pub fn guid(&self) -> Result<Guid> {
        let guid_int = unsafe { ffi::ibv_get_device_guid(*self.0) };
        let guid: Guid = guid_int.into();
        if guid.is_reserved() {
            Err(Error::os(io::Error::last_os_error(), Error::DeviceGuid))
        } else {
            Ok(guid)
        }
    }

    /// Returns stable IB device index as it is assigned by the kernel
    /// # Errors
    ///
    ///  - `ENOTSUP`: Stable index is not supported
    pub fn index(&self) -> Result<i32> {
        let idx = unsafe { ffi::ibv_get_device_index(*self.0) };
        if idx == -1 {
            Err(Error::DeviceIndexUnavailable)
        } else {
            Ok(idx)
        }
    }

    /// Returns the transport type of this device (for example InfiniBand or iWARP).
    ///
    /// RoCE devices report [`IBV_TRANSPORT_IB`](ffi::ibv_transport_type::IBV_TRANSPORT_IB), since
    /// RoCE is InfiniBand transport over Ethernet.
    pub fn transport_type(&self) -> ffi::ibv_transport_type {
        unsafe { (**self.0).transport_type }
    }

    /// Returns the underlying `ibv_device` pointer.
    ///
    /// This is an escape hatch for calling libibverbs functions this crate does not yet wrap. The
    /// pointer is owned by the [`DeviceList`] this device came from and stays valid only while
    /// that list is alive.
    pub fn as_raw(&self) -> *mut ffi::ibv_device {
        *self.0
    }
}

struct ContextInner {
    ctx: *mut ffi::ibv_context,
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
    fn query_port(&self, port_num: u8) -> Result<ffi::ibv_port_attr> {
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
                let ok = unsafe { ffi::ibv_close_device(self.ctx) };
                assert_eq!(ok, 0);
            }
            // Borrowed: don't close the device; dropping the kept-alive owner is enough.
            #[cfg(feature = "rdmacm")]
            ContextOwnership::Borrowed(_) => {}
        }
    }
}

unsafe impl Sync for ContextInner {}
unsafe impl Send for ContextInner {}

/// An RDMA context bound to a device.
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
    /// Opens a context for the given device, and queries its port and gid.
    fn with_device(dev: *mut ffi::ibv_device) -> Result<Context> {
        assert!(!dev.is_null());

        let ctx = unsafe { ffi::ibv_open_device(dev) };
        if ctx.is_null() {
            return Err(Error::OpenDevice(io::Error::last_os_error()));
        }
        let inner = Arc::new(ContextInner {
            ctx,
            ownership: ContextOwnership::Owned,
        });

        let ctx = Context { inner };
        // checks that the (default) port is active/armed.
        ctx.inner.query_port(PORT_NUM)?;
        Ok(ctx)
    }

    /// Wraps a raw `ibv_context` owned by `owner` (the RDMA connection manager's `rdma_cm_id`).
    ///
    /// The returned [`Context`] does not close the device on drop, and keeps `owner` alive for as
    /// long as the context — or any protection domain, completion queue, queue pair, or memory
    /// region built from it — is alive, so `ctx` cannot dangle.
    #[cfg(feature = "rdmacm")]
    pub(crate) fn from_borrowed_context(
        ctx: *mut ffi::ibv_context,
        owner: Arc<dyn Send + Sync>,
    ) -> Context {
        Context {
            inner: Arc::new(ContextInner {
                ctx,
                ownership: ContextOwnership::Borrowed(owner),
            }),
        }
    }

    /// Begin building a completion queue (CQ) with room for at least `min_cq_entries` entries.
    ///
    /// When an outstanding Work Request, within a Send or Receive Queue, is completed, a Work
    /// Completion is being added to the CQ of that Work Queue. This Work Completion indicates that
    /// the outstanding Work Request has been completed (and no longer considered outstanding) and
    /// provides details on it (status, direction, opcode, etc.).
    ///
    /// A single CQ can be shared for sending, receiving, and sharing across multiple QPs. The Work
    /// Completion holds the information to specify the QP number and the Queue (Send or Receive)
    /// that it came from.
    ///
    /// `min_cq_entries` is the minimum size of the CQ (the actual size can be larger) and is the only
    /// required parameter. The optional ones — an opaque context cookie, the completion vector, and
    /// extra work-completion fields such as a hardware timestamp — are configured on the returned
    /// [`CompletionQueueBuilder`]; call [`build`](CompletionQueueBuilder::build) to create the queue.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # fn f(ctx: &ibverbs::Context) -> ibverbs::Result<()> {
    /// let cq = ctx.create_cq(16).build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn create_cq(&self, min_cq_entries: i32) -> CompletionQueueBuilder {
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
    pub fn create_comp_channel(&self) -> Result<CompletionChannel> {
        CompletionChannel::new(&self.inner)
    }

    /// Allocate a protection domain (PDs) for the device's context.
    ///
    /// The created PD will be used primarily to create `QueuePair`s and `MemoryRegion`s.
    ///
    /// A protection domain is a means of protection, and helps you create a group of object that
    /// can work together. If several objects were created using PD1, and others were created using
    /// PD2, working with objects from group1 together with objects from group2 will not work.
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

    /// Query a single entry of a port's GID table (`ibv_query_gid`).
    ///
    /// Ports are numbered from 1. For the full table at once (with the GID type and associated net
    /// device of each entry), use [`gid_table`](Self::gid_table).
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: Invalid `port_num` or `gid_index`.
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
    /// [`ibv_device_attr`], so every field is accessible. Query these before creating resources to
    /// stay within what the device supports.
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: Invalid arguments.
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
    /// (its [`orig`](DeviceAttrEx::orig) holds the base [`ibv_device_attr`]) plus the extended
    /// capabilities that the base query cannot report: the completion-timestamp mask, the HCA core
    /// clock, the PCI atomic capabilities, the packet-pacing (rate-limit) limits, the raw-packet
    /// capabilities, and the maximum device-memory size. Providers that do not implement the
    /// extended verb fall back to the base attributes, and the extended fields read back as zero.
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: Invalid arguments.
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
    /// accessors for the speed, width, link layer, and physical state; it dereferences to the raw
    /// [`ibv_port_attr`] for everything else. Unlike the check performed when a context is opened,
    /// this returns the attributes regardless of the port state.
    ///
    /// Port attributes are not constant (the subnet manager or the hardware may change them), so
    /// avoid caching the result for long.
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: Invalid `port_num`.
    ///  - `ENOMEM`: Out of memory.
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
    /// The returned [`Duration`] is the device's raw clock value, the same time base that
    /// [`WorkCompletion::completion_timestamp`] reports its (HCA-clock) timestamps in. Sampling it
    /// lets you relate completion timestamps to host time.
    ///
    /// # Errors
    ///
    ///  - `EOPNOTSUPP`: The device does not support querying real-time values.
    pub fn query_rt_values_ex(&self) -> Result<Duration> {
        // SAFETY: `ibv_values_ex` is a plain C struct (a mask plus a `timespec`); all-zero is a valid
        // initial value.
        let mut values: ffi::ibv_values_ex = unsafe { std::mem::zeroed() };
        values.comp_mask = ffi::ibv_values_mask::IBV_VALUES_MASK_RAW_CLOCK as u32;
        let errno = unsafe { ffi::ibv_query_rt_values_ex(self.inner.ctx, &mut values as *mut _) };
        if errno != 0 {
            return Err(Error::errno(errno, Error::QueryRealTimeValues));
        }
        Ok(Duration::new(
            values.raw_clock.tv_sec as u64,
            values.raw_clock.tv_nsec as u32,
        ))
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
/// Dereferences to the raw [`ibv_device_attr`], so every field is accessible; the inherent methods
/// add typed accessors for the device identifiers.
#[derive(Clone)]
pub struct DeviceAttr(ffi::ibv_device_attr);

impl DeviceAttr {
    /// The node GUID of the device.
    pub fn node_guid(&self) -> Guid {
        self.0.node_guid.into()
    }

    /// The system-image GUID, shared by the ports of the same physical device.
    pub fn sys_image_guid(&self) -> Guid {
        self.0.sys_image_guid.into()
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
/// Dereferences to the raw [`ibv_device_attr_ex`], so every field is accessible; the inherent
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
        self.0.orig_attr.node_guid.into()
    }

    /// The system-image GUID, shared by the ports of the same physical device.
    pub fn sys_image_guid(&self) -> Guid {
        self.0.orig_attr.sys_image_guid.into()
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
/// Dereferences to the raw [`ibv_port_attr`], so every field is accessible; the inherent methods
/// add typed accessors for the state, MTU, speed, width, link layer, and physical state.
#[derive(Clone)]
pub struct PortAttr(ffi::ibv_port_attr);

impl PortAttr {
    /// The logical port state.
    pub fn state(&self) -> ffi::ibv_port_state {
        self.0.state
    }

    /// The maximum MTU supported by this port.
    pub fn max_mtu(&self) -> ffi::ibv_mtu {
        self.0.max_mtu
    }

    /// The currently active MTU.
    pub fn active_mtu(&self) -> ffi::ibv_mtu {
        self.0.active_mtu
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
    fn new(ctx: &Arc<ContextInner>) -> Result<CompletionChannel> {
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
    ctx: Arc<ContextInner>,
    min_cq_entries: i32,
    cq_context: isize,
    comp_vector: u32,
    /// extra work-completion fields requested on top of the always-present standard set
    wc_flags: u32,
    /// the completion channel to deliver notifications on, if any
    comp_channel: Option<CompletionChannel>,
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

struct CompletionQueueInner {
    _ctx: Arc<ContextInner>,
    cq_ex: *mut ffi::ibv_cq_ex,
    cc: Option<CompletionChannel>,
}

impl CompletionQueueInner {
    /// The underlying `ibv_cq`. An `ibv_cq_ex` shares its layout prefix with `ibv_cq`, so this is
    /// just a pointer cast (exactly what `ibv_cq_ex_to_cq` does in C). Used for the verbs that still
    /// take a plain `ibv_cq`: queue-pair creation, completion-event notification, and teardown.
    #[inline]
    fn cq(&self) -> *mut ffi::ibv_cq {
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
    #[inline]
    pub fn opcode(&self) -> ffi::ibv_wc_opcode {
        unsafe { (*self.cq).read_opcode.unwrap()(self.cq) }
    }

    /// The number of bytes transferred.
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
    inner: Arc<CompletionQueueInner>,
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

/// An unconfigured `QueuePair`.
///
/// A `QueuePairBuilder` is used to configure a `QueuePair` before it is allocated and initialized.
/// To construct one, use `ProtectionDomain::create_qp`. See also [RDMAmojo] for many more details.
///
/// [RDMAmojo]: http://www.rdmamojo.com/2013/01/12/ibv_modify_qp/
pub struct QueuePairBuilder {
    ctx: isize,
    pd: Arc<ProtectionDomainInner>,
    port_attr: ffi::ibv_port_attr,
    /// the device port this queue pair is associated with (numbered from 1)
    port_num: u8,

    send: Arc<CompletionQueueInner>,
    max_send_wr: u32,
    recv: Arc<CompletionQueueInner>,
    max_recv_wr: u32,

    gid_index: Option<u32>,
    max_send_sge: u32,
    max_recv_sge: u32,
    max_inline_data: u32,

    qp_type: ffi::ibv_qp_type,

    // carried along to handshake phase
    /// traffic class set in Global Routing Headers, only used if `gid_index` is set.
    traffic_class: u8,
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
    /// only valid for RC and UC
    rq_psn: Option<u32>,
    /// service level (0-15). Higher value means higher priority.
    service_level: u8,
    /// shared receive queue
    srq: Option<SharedReceiveQueue>,
}

impl QueuePairBuilder {
    /// Prepare a new `QueuePair` builder.
    ///
    /// `max_send_wr` is the maximum number of outstanding Work Requests that can be posted to the
    /// Send Queue in that Queue Pair. Value must be in `[0..dev_cap.max_qp_wr]`. There may be RDMA
    /// devices that for specific transport types may support less outstanding Work Requests than
    /// the maximum reported value.
    ///
    /// Similarly, `max_recv_wr` is the maximum number of outstanding Work Requests that can be
    /// posted to the Receive Queue in that Queue Pair. Value must be in `[0..dev_cap.max_qp_wr]`.
    /// There may be RDMA devices that for specific transport types may support less outstanding
    /// Work Requests than the maximum reported value. This value is ignored if the Queue Pair is
    /// associated with an SRQ
    #[allow(clippy::too_many_arguments)]
    fn new(
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
    ) -> QueuePairBuilder {
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
            rq_psn: (qp_type == ffi::ibv_qp_type::IBV_QPT_RC
                || qp_type == ffi::ibv_qp_type::IBV_QPT_UC)
                .then_some(0),
            service_level: 0,
            srq: None,
        }
    }

    /// Set the access flags for the new `QueuePair`.
    ///
    /// Valid only for RC and UC QPs.
    ///
    /// Defaults to `IBV_ACCESS_LOCAL_WRITE`.
    pub fn set_access(&mut self, access: ffi::ibv_access_flags) -> &mut Self {
        if self.qp_type == ffi::ibv_qp_type::IBV_QPT_RC
            || self.qp_type == ffi::ibv_qp_type::IBV_QPT_UC
        {
            self.access = Some(access);
        }
        self
    }

    /// Set the access flags of the new `QueuePair` such that it allows remote reads and writes.
    ///
    /// Valid only for RC and UC QPs.
    pub fn allow_remote_rw(&mut self) -> &mut Self {
        if self.qp_type == ffi::ibv_qp_type::IBV_QPT_RC
            || self.qp_type == ffi::ibv_qp_type::IBV_QPT_UC
        {
            self.access = Some(
                self.access.expect("always set to Some in new")
                    | ffi::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
                    | ffi::ibv_access_flags::IBV_ACCESS_REMOTE_READ,
            );
        }
        self
    }

    /// Set the service level of the new `QueuePair`.
    /// service level (0-15). Higher value means higher priority.
    /// Defaults to 0.
    pub fn set_service_level(&mut self, service_level: u8) -> &mut Self {
        self.service_level = service_level;
        self
    }

    /// Sets the GID table index that should be used for the new `QueuePair`.
    /// The entry corresponds to the index in `Context::gid_table()`. This is only used if the
    /// `QueuePairEndpoint` that is passed to `QueuePair::handshake()` has a `gid`.
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

    /// Sets the minimum RNR NAK Timer Field Value for the new `QueuePair`.
    ///
    /// Defaults to 16 (2.56 ms delay).
    /// Valid only for RC QPs.
    ///
    /// When an incoming message to this QP should consume a Work Request from the Receive Queue,
    /// but no Work Request is outstanding on that Queue, the QP will send an RNR NAK packet to
    /// the initiator. It does not affect RNR NAKs sent for other reasons. The value must be one of
    /// the following values:
    ///
    ///  - 0 - 655.36 ms delay
    ///  - 1 - 0.01 ms delay
    ///  - 2 - 0.02 ms delay
    ///  - 3 - 0.03 ms delay
    ///  - 4 - 0.04 ms delay
    ///  - 5 - 0.06 ms delay
    ///  - 6 - 0.08 ms delay
    ///  - 7 - 0.12 ms delay
    ///  - 8 - 0.16 ms delay
    ///  - 9 - 0.24 ms delay
    ///  - 10 - 0.32 ms delay
    ///  - 11 - 0.48 ms delay
    ///  - 12 - 0.64 ms delay
    ///  - 13 - 0.96 ms delay
    ///  - 14 - 1.28 ms delay
    ///  - 15 - 1.92 ms delay
    ///  - 16 - 2.56 ms delay
    ///  - 17 - 3.84 ms delay
    ///  - 18 - 5.12 ms delay
    ///  - 19 - 7.68 ms delay
    ///  - 20 - 10.24 ms delay
    ///  - 21 - 15.36 ms delay
    ///  - 22 - 20.48 ms delay
    ///  - 23 - 30.72 ms delay
    ///  - 24 - 40.96 ms delay
    ///  - 25 - 61.44 ms delay
    ///  - 26 - 81.92 ms delay
    ///  - 27 - 122.88 ms delay
    ///  - 28 - 163.84 ms delay
    ///  - 29 - 245.76 ms delay
    ///  - 30 - 327.68 ms delay
    ///  - 31 - 491.52 ms delay
    pub fn set_min_rnr_timer(&mut self, timer: u8) -> &mut Self {
        if self.qp_type == ffi::ibv_qp_type::IBV_QPT_RC {
            self.min_rnr_timer = Some(timer);
        }
        self
    }

    /// Sets the minimum timeout that the new `QueuePair` waits for ACK/NACK from remote QP before
    /// retransmitting the packet.
    ///
    /// Defaults to 4 (65.536µs).
    /// Valid only for RC QPs.
    ///
    /// The value zero is special value that waits an infinite time for the ACK/NACK (useful
    /// for debugging). This means that if any packet in a message is being lost and no ACK or NACK
    /// is being sent, no retry will ever occur and the QP will just stop sending data.
    ///
    /// For any other value of timeout, the time calculation is `4.096*2^timeout`µs, giving:
    ///
    ///  - 0 - infinite
    ///  - 1 - 8.192 µs
    ///  - 2 - 16.384 µs
    ///  - 3 - 32.768 µs
    ///  - 4 - 65.536 µs
    ///  - 5 - 131.072 µs
    ///  - 6 - 262.144 µs
    ///  - 7 - 524.288 µs
    ///  - 8 - 1.048 ms
    ///  - 9 - 2.097 ms
    ///  - 10 - 4.194 ms
    ///  - 11 - 8.388 ms
    ///  - 12 - 16.777 ms
    ///  - 13 - 33.554 ms
    ///  - 14 - 67.108 ms
    ///  - 15 - 134.217 ms
    ///  - 16 - 268.435 ms
    ///  - 17 - 536.870 ms
    ///  - 18 - 1.07 s
    ///  - 19 - 2.14 s
    ///  - 20 - 4.29 s
    ///  - 21 - 8.58 s
    ///  - 22 - 17.1 s
    ///  - 23 - 34.3 s
    ///  - 24 - 68.7 s
    ///  - 25 - 137 s
    ///  - 26 - 275 s
    ///  - 27 - 550 s
    ///  - 28 - 1100 s
    ///  - 29 - 2200 s
    ///  - 30 - 4400 s
    ///  - 31 - 8800 s
    pub fn set_timeout(&mut self, timeout: u8) -> &mut Self {
        if self.qp_type == ffi::ibv_qp_type::IBV_QPT_RC {
            self.timeout = Some(timeout);
        }
        self
    }

    /// Sets the total number of times that the new `QueuePair` will try to resend the packets
    /// before reporting an error because the remote side doesn't answer in the primary path.
    ///
    /// This 3 bit value defaults to 6.
    /// Valid only for RC QPs.
    ///
    /// # Panics
    ///
    /// Panics if a count higher than 7 is given.
    pub fn set_retry_count(&mut self, count: u8) -> &mut Self {
        if self.qp_type == ffi::ibv_qp_type::IBV_QPT_RC {
            assert!(count <= 7);
            self.retry_count = Some(count);
        }
        self
    }

    /// Sets the total number of times that the new `QueuePair` will try to resend the packets when
    /// an RNR NACK was sent by the remote QP before reporting an error.
    ///
    /// This 3 bit value defaults to 6. The value 7 is special and specify to retry sending the
    /// message indefinitely when a RNR Nack is being sent by remote side.
    /// Valid only for RC QPs.
    ///
    /// # Panics
    ///
    /// Panics if a limit higher than 7 is given.
    pub fn set_rnr_retry(&mut self, n: u8) -> &mut Self {
        if self.qp_type == ffi::ibv_qp_type::IBV_QPT_RC {
            assert!(n <= 7);
            self.rnr_retry = Some(n);
        }
        self
    }

    /// Set the Shared Receive Queue (SRQ) associated with this QP.
    pub fn set_srq(&mut self, srq: &SharedReceiveQueue) -> &mut Self {
        self.srq = Some(srq.clone());
        self
    }

    /// Set the number of outstanding RDMA reads & atomic operations on the destination Queue Pair.
    ///
    /// This defaults to 1.
    /// Valid only for RC QPs.
    pub fn set_max_rd_atomic(&mut self, max_rd_atomic: u8) -> &mut Self {
        if self.qp_type == ffi::ibv_qp_type::IBV_QPT_RC {
            self.max_rd_atomic = Some(max_rd_atomic);
        }
        self
    }

    /// Set the number of responder resources for handling incoming RDMA reads & atomic operations.
    ///
    /// This defaults to 1.
    /// Valid only for RC QPs.
    pub fn set_max_dest_rd_atomic(&mut self, max_dest_rd_atomic: u8) -> &mut Self {
        if self.qp_type == ffi::ibv_qp_type::IBV_QPT_RC {
            self.max_dest_rd_atomic = Some(max_dest_rd_atomic);
        }
        self
    }

    /// Set the path MTU.
    ///
    /// Defaults to the port's active_mtu.
    /// Valid only for RC and UC QPs.
    /// The possible values are:
    ///  - 1: 256
    ///  - 2: 512
    ///  - 3: 1024
    ///  - 4: 2048
    ///  - 5: 4096
    pub fn set_path_mtu(&mut self, path_mtu: ibv_mtu) -> &mut Self {
        if self.qp_type == ffi::ibv_qp_type::IBV_QPT_RC
            || self.qp_type == ffi::ibv_qp_type::IBV_QPT_UC
        {
            self.path_mtu = Some(path_mtu);
        }
        self
    }

    /// Set the PSN for the receive queue.
    ///
    /// Defaults to 0.
    /// Valid only for RC and UC QPs.
    pub fn set_rq_psn(&mut self, rq_psn: u32) -> &mut Self {
        if self.qp_type == ffi::ibv_qp_type::IBV_QPT_RC
            || self.qp_type == ffi::ibv_qp_type::IBV_QPT_UC
        {
            self.rq_psn = Some(rq_psn);
        }
        self
    }

    /// Set the opaque context value for the new `QueuePair`.
    ///
    /// Defaults to 0.
    pub fn set_context(&mut self, ctx: isize) -> &mut Self {
        self.ctx = ctx;
        self
    }

    /// Set the maximum number of send requests in the work queue
    ///
    /// Defaults to 1.
    pub fn set_max_send_wr(&mut self, max_send_wr: u32) -> &mut Self {
        self.max_send_wr = max_send_wr;
        self
    }

    /// The maximum number of scatter/gather elements in any Work Request
    /// that can be posted to the Send Queue in that Queue Pair.
    ///
    /// Value can be [0..dev_cap.max_sge]. There may be RDMA devices that
    /// for specific transport types may support less scatter/gather elements
    /// than the maximum reported value.
    ///
    /// Defaults to 1.
    pub fn set_max_send_sge(&mut self, max_send_sge: u32) -> &mut Self {
        self.max_send_sge = max_send_sge;
        self
    }

    /// Set the maximum number of receive requests in the work queue
    ///
    /// Defaults to 1.
    pub fn set_max_recv_wr(&mut self, max_recv_wr: u32) -> &mut Self {
        self.max_recv_wr = max_recv_wr;
        self
    }

    /// The maximum number of scatter/gather elements in any Work Request
    /// that can be posted to the Receive Queue in that Queue Pair.
    ///
    /// Value can be [0..dev_cap.max_sge]. There may be RDMA devices that
    /// for specific transport types may support less scatter/gather elements
    /// than the maximum reported value. This value is ignored if the
    /// Queue Pair is associated with an SRQ.
    ///
    /// Defaults to 1.
    pub fn set_max_recv_sge(&mut self, max_recv_sge: u32) -> &mut Self {
        self.max_recv_sge = max_recv_sge;
        self
    }

    /// Set the maximum size, in bytes, of inline data that may be posted on the send queue.
    ///
    /// Inline sends (see [`PostOp::send_inline`]) copy their payload directly into the work request
    /// rather than referencing a registered memory region, which lowers latency for small messages.
    /// A send queue must reserve this capacity up front; the actual value granted by the device can
    /// be larger than requested and is reported in the `max_inline_data` field returned by
    /// `ibv_query_qp`. Posting more inline bytes than the queue pair supports fails at submit time.
    ///
    /// Defaults to 0 (inline sends disabled).
    pub fn set_max_inline_data(&mut self, max_inline_data: u32) -> &mut Self {
        self.max_inline_data = max_inline_data;
        self
    }

    /// Create a new `QueuePair` from this builder template.
    ///
    /// The returned `QueuePair` is associated with the builder's `ProtectionDomain`.
    ///
    /// This method will fail if asked to create QP of a type other than `IBV_QPT_RC` or
    /// `IBV_QPT_UD` associated with an SRQ.
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: Invalid `ProtectionDomain`, sending or receiving `Context`, or invalid value
    ///    provided in `max_send_wr`, `max_recv_wr`, or in `max_inline_data`.
    ///  - `ENOMEM`: Not enough resources to complete this operation.
    ///  - `ENOSYS`: QP with this Transport Service Type isn't supported by this RDMA device.
    ///  - `EPERM`: Not enough permissions to create a QP with this Transport Service Type.
    pub fn build(&self) -> Result<PreparedQueuePair> {
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
            (*p).qp_context = ptr::null::<c_void>().offset(self.ctx) as *mut _;
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
            Err(Error::CreateQueuePair(io::Error::last_os_error()))
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
                rq_psn: self.rq_psn,
                service_level: self.service_level,
            })
        }
    }
}

/// An allocated but uninitialized `QueuePair`.
///
/// Specifically, this `QueuePair` has been allocated with `ibv_create_qp`, but has not yet been
/// initialized with calls to `ibv_modify_qp`.
///
/// To complete the construction of the `QueuePair`, you will need to obtain the
/// `QueuePairEndpoint` of the remote end (by using `PreparedQueuePair::endpoint`), and then call
/// `PreparedQueuePair::handshake` on both sides with the other side's `QueuePairEndpoint`:
///
/// ```rust,ignore
/// // on host 1
/// let pqp: PreparedQueuePair = ...;
/// let host1end = pqp.endpoint();
/// host2.send(host1end);
/// let host2end = host2.recv();
/// let qp = pqp.handshake(host2end);
///
/// // on host 2
/// let pqp: PreparedQueuePair = ...;
/// let host2end = pqp.endpoint();
/// host1.send(host2end);
/// let host1end = host1.recv();
/// let qp = pqp.handshake(host1end);
/// ```
pub struct PreparedQueuePair {
    qp: QueuePair,
    /// port local identifier
    lid: u16,
    /// the device port this queue pair is associated with (numbered from 1)
    port_num: u8,
    // carried from builder
    gid_index: Option<u32>,
    /// traffic class set in Global Routing Headers, only used if `gid_index` is set.
    traffic_class: u8,
    /// only valid for RC and UC
    access: Option<ffi::ibv_access_flags>,
    /// only valid for RC
    min_rnr_timer: Option<u8>,
    /// only valid for RC
    timeout: Option<u8>,
    /// only valid for RC
    retry_count: Option<u8>,
    /// only valid for RC
    rnr_retry: Option<u8>,
    /// only valid for RC
    max_rd_atomic: Option<u8>,
    /// only valid for RC
    max_dest_rd_atomic: Option<u8>,
    /// only valid for RC and UC
    path_mtu: Option<ibv_mtu>,
    /// only valid for RC and UC
    rq_psn: Option<u32>,
    /// service level (0-15). Higher value means higher priority.
    service_level: u8,
}

/// A Global identifier for ibv.
///
/// This struct acts as a rust wrapper for `ffi::ibv_gid`. We use it instead of
/// `ffi::ibv_giv` because `ffi::ibv_gid` is actually an untagged union.
///
/// ```c
/// union ibv_gid {
///     uint8_t   raw[16];
///     struct {
///         __be64 subnet_prefix;
///         __be64 interface_id;
///     } global;
/// };
/// ```
///
/// It appears that `global` exists for convenience, but can be safely ignored.
/// For continuity, the methods `subnet_prefix` and `interface_id` are provided.
/// These methods read the array as big endian, regardless of native cpu
/// endianness.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Default, Copy, Clone, Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct Gid {
    raw: [u8; 16],
}

impl Gid {
    /// Expose the subnet_prefix component of the `Gid` as a u64. This is
    /// equivalent to accessing the `global.subnet_prefix` component of the
    /// `ffi::ibv_gid` union.
    pub fn subnet_prefix(&self) -> u64 {
        u64::from_be_bytes(self.raw[..8].try_into().unwrap())
    }

    /// Expose the interface_id component of the `Gid` as a u64. This is
    /// equivalent to accessing the `global.interface_id` component of the
    /// `ffi::ibv_gid` union.
    pub fn interface_id(&self) -> u64 {
        u64::from_be_bytes(self.raw[8..].try_into().unwrap())
    }

    /// Whether this GID holds an IPv4-mapped address (`::ffff:a.b.c.d`).
    ///
    /// On RoCE, the GIDs of a port mirror the IP addresses of its network interface, so the entry
    /// holding the interface's IPv4 address is IPv4-mapped. That entry is typically the routable
    /// one to pick (by its index, via [`QueuePairBuilder::set_gid_index`]) on plain-Ethernet IPv4
    /// networks.
    pub fn is_ipv4_mapped(&self) -> bool {
        std::net::Ipv6Addr::from(*self).to_ipv4_mapped().is_some()
    }
}

/// A GID is an IPv6 address by construction (RoCE GIDs literally mirror the interface's IP
/// addresses); this conversion makes it printable and comparable as one.
impl From<Gid> for std::net::Ipv6Addr {
    fn from(gid: Gid) -> Self {
        std::net::Ipv6Addr::from(gid.raw)
    }
}

impl From<std::net::Ipv6Addr> for Gid {
    fn from(addr: std::net::Ipv6Addr) -> Self {
        Self { raw: addr.octets() }
    }
}

impl fmt::Display for Gid {
    /// Formats the GID the way `ibv_devinfo -v` and `show_gids` do: as an IPv6 address, for
    /// example `fe80::5054:ff:fe12:3456` or `::ffff:192.0.2.1`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        std::net::Ipv6Addr::from(*self).fmt(f)
    }
}

impl fmt::Debug for Gid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Gid({self})")
    }
}

impl From<ffi::ibv_gid> for Gid {
    fn from(gid: ffi::ibv_gid) -> Self {
        Self {
            raw: unsafe { gid.raw },
        }
    }
}

impl From<Gid> for ffi::ibv_gid {
    fn from(mut gid: Gid) -> Self {
        *gid.as_mut()
    }
}

impl From<Gid> for [u8; 16] {
    fn from(gid: Gid) -> Self {
        gid.raw
    }
}

impl From<[u8; 16]> for Gid {
    fn from(raw: [u8; 16]) -> Self {
        Self { raw }
    }
}

impl AsRef<ffi::ibv_gid> for Gid {
    fn as_ref(&self) -> &ffi::ibv_gid {
        unsafe { &*self.raw.as_ptr().cast::<ffi::ibv_gid>() }
    }
}

impl AsMut<ffi::ibv_gid> for Gid {
    fn as_mut(&mut self) -> &mut ffi::ibv_gid {
        unsafe { &mut *self.raw.as_mut_ptr().cast::<ffi::ibv_gid>() }
    }
}

/// A Global identifier entry for ibv.
///
/// This struct acts as a rust wrapper for `ffi::ibv_gid_entry`. We use it instead of
/// `ffi::ibv_gid_entry` because `ffi::ibv_gid` is wrapped by `Gid`.
#[derive(Debug, Clone)]
pub struct GidEntry {
    /// The GID entry.
    pub gid: Gid,
    /// The GID table index of this entry.
    pub gid_index: u32,
    /// The port number that this GID belongs to.
    pub port_num: u32,
    /// enum ibv_gid_type, can be one of IBV_GID_TYPE_IB, IBV_GID_TYPE_ROCE_V1 or IBV_GID_TYPE_ROCE_V2.
    pub gid_type: ibv_gid_type,
    /// The interface index of the net device associated with this GID.
    ///
    /// It is 0 if there is no net device associated with it.
    pub ndev_ifindex: u32,
}

impl From<ffi::ibv_gid_entry> for GidEntry {
    fn from(gid_entry: ffi::ibv_gid_entry) -> Self {
        Self {
            gid: gid_entry.gid.into(),
            gid_index: gid_entry.gid_index,
            port_num: gid_entry.port_num,
            gid_type: match gid_entry.gid_type {
                0 => ibv_gid_type::IBV_GID_TYPE_IB,
                1 => ibv_gid_type::IBV_GID_TYPE_ROCE_V1,
                2 => ibv_gid_type::IBV_GID_TYPE_ROCE_V2,
                x => panic!("unknown ibv_gid_type: {x}"),
            },
            ndev_ifindex: gid_entry.ndev_ifindex,
        }
    }
}

impl GidEntry {
    /// The name of the network device associated with this GID, if any.
    ///
    /// Resolves [`ndev_ifindex`](Self::ndev_ifindex) with `if_indextoname`. Returns `None` when
    /// there is no associated net device (the index is 0) or the name cannot be resolved.
    pub fn netdev_name(&self) -> Option<String> {
        if self.ndev_ifindex == 0 {
            return None;
        }
        let mut buf = [0 as std::os::raw::c_char; nix::libc::IF_NAMESIZE];
        // SAFETY: `buf` is `IF_NAMESIZE` bytes, the size `if_indextoname` requires.
        let ret = unsafe { nix::libc::if_indextoname(self.ndev_ifindex, buf.as_mut_ptr()) };
        if ret.is_null() {
            return None;
        }
        // SAFETY: on success `if_indextoname` wrote a NUL-terminated name into `buf`.
        let name = unsafe { CStr::from_ptr(buf.as_ptr()) };
        name.to_str().ok().map(String::from)
    }
}

/// An identifier for the network endpoint of a `QueuePair`.
///
/// Internally, this contains the `QueuePair`'s `qp_num`, as well as the context's `lid` and `gid`.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct QueuePairEndpoint {
    /// the `QueuePair`'s `qp_num`
    pub num: u32,
    /// the context's `lid`
    pub lid: u16,
    /// the context's `gid`, used for global routing
    pub gid: Option<Gid>,
}

impl PreparedQueuePair {
    /// Extracts the still-uninitialized (`RESET`) queue pair without transitioning it, so you can
    /// drive the state machine yourself with [`QueuePair::modify`] instead of using
    /// [`handshake`](Self::handshake) / [`activate_ud`](Self::activate_ud).
    ///
    /// This is an escape hatch for fully manual bring-up (custom partition keys, packet sequence
    /// numbers, alternate paths, and so on). The returned queue pair is in `RESET` and cannot send
    /// or receive until you transition it through `INIT`, `RTR`, and `RTS`; most users should prefer
    /// `handshake`/`activate_ud`. The RDMA connection manager (the [`rdmacm`](crate::rdmacm) module)
    /// uses this to drive the transitions itself.
    pub fn into_queue_pair(self) -> QueuePair {
        self.qp
    }

    /// Get the network endpoint for this `QueuePair`.
    ///
    /// This endpoint will need to be communicated to the `QueuePair` on the remote end.
    pub fn endpoint(&self) -> Result<QueuePairEndpoint> {
        let num = unsafe { &*self.qp.qp }.qp_num;
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
            num,
            lid: self.lid,
            gid,
        })
    }

    /// Set up the `QueuePair` such that it is ready to exchange packets with a remote `QueuePair`.
    ///
    /// Internally, this uses `ibv_modify_qp` to mark the `QueuePair` as initialized
    /// (`IBV_QPS_INIT`), ready to receive (`IBV_QPS_RTR`), and ready to send (`IBV_QPS_RTS`).
    /// Further discussion of the protocol can be found on [RDMAmojo].
    ///
    /// If the endpoint contains a Gid, the routing will be global. This means:
    /// ```text,ignore
    /// ah_attr.is_global = 1;
    /// ah_attr.grh.hop_limit = 0xff;
    /// ```
    ///
    /// The queue pair is associated with the port chosen when it was created (see
    /// [`ProtectionDomain::create_qp_on_port`]). The handshake also sets the following parameters,
    /// which are currently not configurable:
    ///
    /// # Examples
    ///
    /// ```text,ignore
    /// pkey_index = 0;
    /// sq_psn = 0;
    ///
    /// ah_attr.sl = 0;
    /// ah_attr.src_path_bits = 0;
    /// ```
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: Invalid value provided in `attr` or in `attr_mask`.
    ///  - `ENOMEM`: Not enough resources to complete this operation.
    ///
    /// [RDMAmojo]: http://www.rdmamojo.com/2014/01/18/connecting-queue-pairs/
    pub fn handshake(self, remote: QueuePairEndpoint) -> Result<QueuePair> {
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

        // set ready to receive
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_RTR,
            // TODO: this is only valid for RC and UC
            dest_qp_num: remote.num,
            // TODO: this is only valid for RC and UC
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
        if let Some(rq_psn) = self.rq_psn {
            attr.rq_psn = rq_psn;
            mask |= ffi::ibv_qp_attr_mask::IBV_QP_RQ_PSN;
        }
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

        // set ready to send
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_RTS,
            sq_psn: 0,
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

    /// Activate this queue pair as an unreliable datagram (UD) queue pair.
    ///
    /// Unlike [`handshake`](Self::handshake), UD is connectionless: there is no remote endpoint to
    /// exchange, so the queue pair is transitioned `INIT -> RTR -> RTS` with the given `qkey`.
    /// Incoming datagrams whose Q_Key does not match `qkey` are discarded (unless the sender's Q_Key
    /// has its most significant bit set, meaning "use the QP's Q_Key").
    ///
    /// Each datagram is addressed individually at send time with an [`AddressHandle`]; see
    /// [`QueuePair::post_send_ud`].
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: Invalid value provided in `attr` or `attr_mask`.
    ///  - `ENOMEM`: Not enough resources to complete this operation.
    pub fn activate_ud(self, qkey: u32) -> Result<QueuePair> {
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

        // RTS.
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_RTS,
            sq_psn: 0,
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

/// Attributes describing how to reach a destination, used to build an [`AddressHandle`].
///
/// Defaults to a non-global (LID-only) route on the crate's port. For RoCE and routed InfiniBand,
/// set a global route with [`set_grh`](Self::set_grh).
#[derive(Clone)]
pub struct AddressHandleAttribute {
    attr: ffi::ibv_ah_attr,
}

impl Default for AddressHandleAttribute {
    fn default() -> Self {
        AddressHandleAttribute {
            attr: ffi::ibv_ah_attr {
                port_num: PORT_NUM,
                ..Default::default()
            },
        }
    }
}

impl AddressHandleAttribute {
    /// A new address-handle attribute on the default port.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the destination LID (InfiniBand). Not used for RoCE / Ethernet link layers.
    pub fn set_dest_lid(&mut self, lid: u16) -> &mut Self {
        self.attr.dlid = lid;
        self
    }

    /// Set the service level.
    pub fn set_service_level(&mut self, service_level: u8) -> &mut Self {
        self.attr.sl = service_level;
        self
    }

    /// Set the local physical port through which the destination is reached.
    pub fn set_port(&mut self, port_num: u8) -> &mut Self {
        self.attr.port_num = port_num;
        self
    }

    /// Set the global route (GRH), required for RoCE and routed InfiniBand.
    ///
    /// `dgid` is the destination GID, `sgid_index` indexes the *local* port's GID table to source
    /// from, and `hop_limit` is the IP hop limit (commonly `0xff`).
    pub fn set_grh(
        &mut self,
        dgid: Gid,
        sgid_index: u8,
        hop_limit: u8,
        traffic_class: u8,
    ) -> &mut Self {
        self.attr.is_global = 1;
        self.attr.grh.dgid = dgid.into();
        self.attr.grh.sgid_index = sgid_index;
        self.attr.grh.hop_limit = hop_limit;
        self.attr.grh.traffic_class = traffic_class;
        self
    }
}

/// A handle to a destination, used to address unreliable-datagram (UD) sends.
///
/// Created with [`ProtectionDomain::create_address_handle`] and passed by reference to each UD send;
/// a single UD queue pair can address many destinations with different handles (see
/// [`QueuePair::post_send_ud`]).
pub struct AddressHandle {
    // Keeps the protection domain (and so its context) alive until the handle is destroyed.
    _pd: Arc<ProtectionDomainInner>,
    ah: *mut ffi::ibv_ah,
}

unsafe impl Send for AddressHandle {}
unsafe impl Sync for AddressHandle {}

impl AddressHandle {
    #[inline]
    fn as_ptr(&self) -> *mut ffi::ibv_ah {
        self.ah
    }

    /// Returns the underlying `ibv_ah` pointer.
    ///
    /// This is an escape hatch for verbs this crate does not yet wrap. The pointer is owned by this
    /// [`AddressHandle`] and stays valid only while it is alive; do not destroy it.
    pub fn as_raw(&self) -> *mut ffi::ibv_ah {
        self.ah
    }
}

impl Drop for AddressHandle {
    fn drop(&mut self) {
        let errno = unsafe { ffi::ibv_destroy_ah(self.ah) };
        if errno != 0 {
            let e = io::Error::from_raw_os_error(errno);
            panic!("{e}");
        }
    }
}

struct MemoryRegionInner {
    _pd: Arc<ProtectionDomainInner>,
    mr: *mut ffi::ibv_mr,
}

unsafe impl Sync for MemoryRegionInner {}
unsafe impl Send for MemoryRegionInner {}

impl Drop for MemoryRegionInner {
    fn drop(&mut self) {
        let errno = unsafe { ffi::ibv_dereg_mr(self.mr) };
        if errno != 0 {
            let e = io::Error::from_raw_os_error(errno);
            panic!("{e}");
        }
    }
}

/// A region of memory registered for use with RDMA.
pub struct MemoryRegion<O> {
    inner: MemoryRegionInner,
    owner: O,
}

impl<O> MemoryRegion<O> {
    /// Get the remote authentication key used to allow direct remote access to this memory region.
    pub fn rkey(&self) -> RemoteKey {
        RemoteKey {
            key: unsafe { &*self.inner.mr }.rkey,
        }
    }

    /// Get the local key of this memory region (the one [`slice`](Self::slice) stamps on every
    /// scatter/gather entry).
    pub fn lkey(&self) -> u32 {
        unsafe { &*self.inner.mr }.lkey
    }

    /// Returns the underlying `ibv_mr` pointer.
    ///
    /// This is an escape hatch for verbs this crate does not yet wrap. The pointer is owned by this
    /// [`MemoryRegion`] and stays valid only while it is alive; do not deregister it.
    pub fn as_raw(&self) -> *mut ffi::ibv_mr {
        self.inner.mr
    }

    /// Remote region.
    pub fn remote(&self) -> RemoteMemorySlice {
        RemoteMemorySlice {
            addr: unsafe { *self.inner.mr }.addr as u64,
            len: unsafe { *self.inner.mr }.length,
            rkey: unsafe { *self.inner.mr }.rkey,
        }
    }

    /// Deregister the memory region and return the buffer that backed it.
    pub fn into_inner(self) -> O {
        self.owner
    }

    /// Make a subslice of this memory region.
    pub fn slice(&self, bounds: impl RangeBounds<usize>) -> LocalMemorySlice {
        let (addr, length) = calc_addr_len(
            bounds,
            unsafe { *self.inner.mr }.addr as u64,
            unsafe { *self.inner.mr }.length,
        );
        let sge = ffi::ibv_sge {
            addr,
            length: length.try_into().unwrap(),
            lkey: unsafe { *self.inner.mr }.lkey,
        };
        LocalMemorySlice { _sge: sge }
    }
}

impl<O: Deref<Target = [u8]>> MemoryRegion<O> {
    /// The registered bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.owner
    }
}

impl<O: DerefMut<Target = [u8]>> MemoryRegion<O> {
    /// The registered bytes, mutably.
    ///
    /// The length is fixed at registration; the buffer can be written but not resized.
    pub fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.owner
    }
}

/// Local memory slice.
#[derive(Debug, Default, Copy, Clone)]
#[repr(transparent)]
pub struct LocalMemorySlice {
    _sge: ffi::ibv_sge,
}

/// An escape hatch for posting memory registered outside this crate (for example through
/// [`ffi::ibv_reg_mr`] with flags the safe API does not cover): a scatter/gather entry converts
/// straight into a postable slice. The post methods' safety contracts (valid registration,
/// no concurrent device access) then cover the converted slice.
impl From<ffi::ibv_sge> for LocalMemorySlice {
    fn from(sge: ffi::ibv_sge) -> Self {
        LocalMemorySlice { _sge: sge }
    }
}

impl From<LocalMemorySlice> for ffi::ibv_sge {
    fn from(slice: LocalMemorySlice) -> Self {
        slice._sge
    }
}

impl LocalMemorySlice {
    /// Get the address of the local memory slice.
    pub fn addr(&self) -> u64 {
        self._sge.addr
    }

    /// Get the length of the local memory slice.
    pub fn len(&self) -> usize {
        self._sge.length as usize
    }

    /// Get is_empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the local key of the local memory slice.
    pub fn lkey(&self) -> u32 {
        self._sge.lkey
    }

    /// Make a subslice of this slice.
    pub fn slice(&self, bounds: impl RangeBounds<usize>) -> Self {
        let (addr, len) = calc_addr_len(bounds, self.addr(), self.len());
        Self {
            _sge: ffi::ibv_sge {
                addr,
                length: len.try_into().unwrap(),
                lkey: self.lkey(),
            },
        }
    }
}

/// Remote memory region.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct RemoteMemorySlice {
    /// Memory address of the registered region (might have been offset by slicing).
    pub addr: u64,
    /// Length of the registered memory region (might have been offset by slicing).
    pub len: usize,
    /// Remote key for accessing this memory region.
    pub rkey: u32,
}

impl RemoteMemorySlice {
    /// Make a subslice of this slice.
    pub fn slice(&self, bounds: impl RangeBounds<usize>) -> Self {
        let (addr, len) = calc_addr_len(bounds, self.addr, self.len);
        Self {
            addr,
            len,
            rkey: self.rkey,
        }
    }
}

/// A key that authorizes direct memory access to a memory region.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct RemoteKey {
    /// The actual key value.
    pub key: u32,
}

struct ProtectionDomainInner {
    ctx: Arc<ContextInner>,
    pd: *mut ffi::ibv_pd,
}

impl Drop for ProtectionDomainInner {
    fn drop(&mut self) {
        let errno = unsafe { ffi::ibv_dealloc_pd(self.pd) };
        if errno != 0 {
            let e = io::Error::from_raw_os_error(errno);
            panic!("{e}");
        }
    }
}

unsafe impl Sync for ProtectionDomainInner {}
unsafe impl Send for ProtectionDomainInner {}

/// A protection domain for a device's context.
#[must_use]
#[derive(Clone)]
pub struct ProtectionDomain {
    inner: Arc<ProtectionDomainInner>,
}

impl ProtectionDomain {
    /// Returns the underlying `ibv_pd` pointer.
    ///
    /// This is an escape hatch for verbs this crate does not yet wrap. The pointer is owned by this
    /// [`ProtectionDomain`] and stays valid only while it (or a resource derived from it) is alive;
    /// do not deallocate it.
    pub fn as_raw(&self) -> *mut ffi::ibv_pd {
        self.inner.pd
    }

    /// Create an [`AddressHandle`] for the destination described by `attr`.
    ///
    /// Address handles are used to address unreliable-datagram (UD) sends. One handle can be reused
    /// across many sends, and a single UD queue pair can hold handles to many destinations.
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: Invalid value provided in `attr`.
    ///  - `ENOMEM`: Not enough resources to complete this operation.
    pub fn create_address_handle(&self, attr: &AddressHandleAttribute) -> Result<AddressHandle> {
        let mut ah_attr = attr.attr;
        let ah = unsafe { ffi::ibv_create_ah(self.inner.pd, &mut ah_attr as *mut _) };
        if ah.is_null() {
            Err(Error::CreateAddressHandle(io::Error::last_os_error()))
        } else {
            Ok(AddressHandle {
                _pd: self.inner.clone(),
                ah,
            })
        }
    }

    /// Give advice to the kernel about an address range in memory regions registered under this
    /// protection domain (`ibv_advise_mr`).
    ///
    /// This is mainly useful with on-demand-paging (ODP) memory regions: for example, prefetching
    /// pages with `IB_UVERBS_ADVISE_MR_ADVICE_PREFETCH` so that a later access does not take a page
    /// fault. `flags` is a bitmask of [`ibv_advise_mr_flags`] (`0` for none), and `sg_list`
    /// describes the ranges to act on; every entry must lie within a memory region registered under
    /// this protection domain.
    ///
    /// # Errors
    ///
    ///  - `EOPNOTSUPP`: The device does not support `ibv_advise_mr`.
    ///  - `EINVAL`: Invalid value provided in the arguments.
    ///  - `ENOMEM`: Not enough resources to complete this operation.
    pub fn advise_mr(
        &self,
        advice: ibv_advise_mr_advice,
        flags: u32,
        sg_list: &[LocalMemorySlice],
    ) -> Result<()> {
        let ret = unsafe {
            ffi::ibv_advise_mr(
                self.inner.pd,
                advice,
                flags,
                sg_list.as_ptr() as *mut ffi::ibv_sge,
                sg_list.len() as u32,
            )
        };
        if ret == 0 {
            Ok(())
        } else {
            Err(Error::errno(ret, Error::AdviseMemoryRegion))
        }
    }

    /// Creates a queue pair builder associated with this protection domain.
    ///
    /// `send` and `recv` are the device `Context` to associate with the send and receive queues
    /// respectively. `send` and `recv` may refer to the same `Context`.
    ///
    /// `qp_type` indicates the requested Transport Service Type of this QP:
    ///
    ///  - `IBV_QPT_RC`: Reliable Connection
    ///  - `IBV_QPT_UC`: Unreliable Connection
    ///  - `IBV_QPT_UD`: Unreliable Datagram
    ///
    /// Note that both this protection domain, *and* both provided completion queues, must outlive
    /// the resulting `QueuePair`.
    ///
    /// The queue pair is associated with the device's first port (port 1); use
    /// [`create_qp_on_port`](Self::create_qp_on_port) to choose another port.
    pub fn create_qp(
        &self,
        send: &CompletionQueue,
        recv: &CompletionQueue,
        qp_type: ffi::ibv_qp_type,
    ) -> Result<QueuePairBuilder> {
        self.create_qp_on_port(send, recv, qp_type, PORT_NUM)
    }

    /// Creates a queue pair builder associated with `port_num` on this protection domain's device.
    ///
    /// Like [`create_qp`](Self::create_qp), but the queue pair (and the endpoint reported by
    /// [`PreparedQueuePair::endpoint`]) is associated with the given port instead of port 1. Ports
    /// are numbered from 1.
    pub fn create_qp_on_port(
        &self,
        send: &CompletionQueue,
        recv: &CompletionQueue,
        qp_type: ffi::ibv_qp_type,
        port_num: u8,
    ) -> Result<QueuePairBuilder> {
        let port_attr = self.inner.ctx.query_port(port_num)?;
        Ok(QueuePairBuilder::new(
            self.inner.clone(),
            port_attr,
            port_num,
            send.inner.clone(),
            1,
            recv.inner.clone(),
            1,
            qp_type,
            1,
            1,
        ))
    }

    /// Register `[ptr, ptr + len)` and wrap the resulting MR handle, deduplicating the
    /// `ibv_reg_mr` call and null check shared by the registration entry points.
    ///
    /// # Safety
    ///
    /// `ptr` must be valid for `len` bytes and outlive the returned `MemoryRegionInner`.
    unsafe fn reg_mr(
        &self,
        ptr: *mut c_void,
        len: usize,
        access_flags: ffi::ibv_access_flags,
    ) -> Result<MemoryRegionInner> {
        let mr = ffi::ibv_reg_mr(self.inner.pd, ptr, len, access_flags.0 as i32);
        // ibv_reg_mr() returns a pointer to the registered MR, or NULL if the request fails.
        if mr.is_null() {
            Err(Error::RegisterMemoryRegion(io::Error::last_os_error()))
        } else {
            Ok(MemoryRegionInner {
                _pd: self.inner.clone(),
                mr,
            })
        }
    }

    /// Allocates and registers a Memory Region (MR) associated with this `ProtectionDomain`.
    ///
    /// This process allows the RDMA device to read and write data to the allocated memory. Only
    /// registered memory can be sent from and received to by `QueuePair`s. Performing this
    /// registration takes some time, so performing memory registration isn't recommended in the
    /// data path, when fast response is required.
    ///
    /// Every successful registration will result with a MR which has unique (within a specific
    /// RDMA device) `lkey` and `rkey` values. These keys must be communicated to the other end's
    /// `QueuePair` for direct memory access.
    ///
    /// The maximum size of the block that can be registered is limited to
    /// `device_attr.max_mr_size`. There isn't any way to know what is the total size of memory
    /// that can be registered for a specific device.
    ///
    /// `allocate_with_permissions` accepts a set of permission flags, with local read access
    /// always enabled for the Memory Region (MR).
    ///
    /// # Panics
    ///
    /// Panics if `n` is 0.
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: Invalid access value.
    ///  - `ENOMEM`: Not enough resources (either in operating system or in RDMA device) to
    ///    complete this operation.
    pub fn allocate_with_permissions(
        &self,
        n: usize,
        access_flags: ffi::ibv_access_flags,
    ) -> Result<MemoryRegion<Box<[u8]>>> {
        assert!(n > 0);
        let mut data = vec![0u8; n].into_boxed_slice();
        let inner = unsafe { self.reg_mr(data.as_mut_ptr() as *mut c_void, n, access_flags)? };
        Ok(MemoryRegion { inner, owner: data })
    }

    /// Allocates and registers a Memory Region (MR) associated with this `ProtectionDomain`.
    ///
    /// This process allows the RDMA device to read and write data to the allocated memory. Only
    /// registered memory can be sent from and received to by `QueuePair`s. Performing this
    /// registration takes some time, so performing memory registration isn't recommended in the
    /// data path, when fast response is required.
    ///
    /// Every successful registration will result with a MR which has unique (within a specific
    /// RDMA device) `lkey` and `rkey` values. These keys must be communicated to the other end's
    /// `QueuePair` for direct memory access.
    ///
    /// The maximum size of the block that can be registered is limited to
    /// `device_attr.max_mr_size`. There isn't any way to know what is the total size of memory
    /// that can be registered for a specific device.
    ///
    /// `allocate` currently sets the following permissions for each new `MemoryRegion`:
    ///
    ///  - `IBV_ACCESS_LOCAL_WRITE`: Enables Local Write Access
    ///  - `IBV_ACCESS_REMOTE_WRITE`: Enables Remote Write Access
    ///  - `IBV_ACCESS_REMOTE_READ`: Enables Remote Read Access
    ///  - `IBV_ACCESS_REMOTE_ATOMIC`: Enables Remote Atomic Operation Access (if supported)
    ///
    /// Local read access is always enabled for the MR. For more fine-grained control over
    /// permissions, see `allocate_with_permissions`.
    ///
    /// # Panics
    ///
    /// Panics if `n` is 0.
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: Invalid access value.
    ///  - `ENOMEM`: Not enough resources (either in operating system or in RDMA device) to
    ///    complete this operation.
    pub fn allocate(&self, n: usize) -> Result<MemoryRegion<Box<[u8]>>> {
        let access_flags = DEFAULT_ACCESS_FLAGS;
        self.allocate_with_permissions(n, access_flags)
    }

    /// Registers externally managed memory as a Memory Region (MR), with the given access
    /// permissions.
    ///
    /// This registers the `len` bytes starting at `ptr`. Unlike [`allocate`](Self::allocate), the
    /// returned [`MemoryRegion`] does *not* own the underlying memory — it is the entry point for
    /// registering memory you manage yourself, such as an `mmap`/hugepage mapping. Use the returned
    /// region's [`slice`](MemoryRegion::slice) / [`remote`](MemoryRegion::remote) handles for RDMA;
    /// access the bytes through your own pointer.
    ///
    /// `access_flags` is always required here: registering memory directly is a low-level operation,
    /// so the permissions are spelled out rather than defaulted (see [`DEFAULT_ACCESS_FLAGS`] for the
    /// set [`allocate`](Self::allocate) uses).
    ///
    /// # Safety
    ///
    /// The caller must ensure that, for the entire lifetime of the returned `MemoryRegion`:
    ///
    ///  - `ptr` is valid for reads and writes of `len` bytes,
    ///  - the memory is not moved, freed, or unmapped, and
    ///  - the memory is not accessed in a way that races with the device reading or writing it.
    ///
    /// # Panics
    ///
    /// Panics if `len` is 0.
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: Invalid access value.
    ///  - `ENOMEM`: Not enough resources (either in operating system or in RDMA device) to
    ///    complete this operation.
    pub unsafe fn register_from_raw(
        &self,
        ptr: *mut u8,
        len: usize,
        access_flags: ffi::ibv_access_flags,
    ) -> Result<MemoryRegion<()>> {
        assert!(len > 0);
        let inner = self.reg_mr(ptr as *mut c_void, len, access_flags)?;
        Ok(MemoryRegion { inner, owner: () })
    }

    /// Registers an already allocated DMA-BUF memory region (MR) associated with this `ProtectionDomain`.
    /// https://man7.org/linux/man-pages/man3/ibv_reg_mr.3.html
    ///
    /// # Arguments
    ///
    /// * `fd` - The file descriptor of the DMA-BUF to be registered. This must refer to an already allocated buffer.
    /// * `offset`, `len` - The MR starts at `offset` of the dma-buf and its size is `len`.
    /// * `iova` - The argument iova specifies the virtual base address of the MR when accessed through a lkey or rkey.
    ///   Note: `iova` must have the same page offset as `offset`
    pub fn register_dmabuf(
        &self,
        fd: i32,
        offset: u64,
        len: usize,
        iova: u64,
        access_flags: ffi::ibv_access_flags,
    ) -> Result<MemoryRegion<()>> {
        let mr = unsafe {
            ffi::ibv_reg_dmabuf_mr(self.inner.pd, offset, len, iova, fd, access_flags.0 as i32)
        };

        if mr.is_null() {
            // Promotes the EOPNOTSUPP of devices/kernels without DMA-BUF support to Unsupported.
            Err(Error::os(
                io::Error::last_os_error(),
                Error::RegisterMemoryRegion,
            ))
        } else {
            let inner = MemoryRegionInner {
                _pd: self.inner.clone(),
                mr,
            };
            Ok(MemoryRegion { inner, owner: () })
        }
    }

    /// Creates a shared receive queue (SRQ) associated with this protection domain.
    ///
    /// `max_wr` is the maximum number of outstanding work requests that can be posted to the SRQ.
    /// `max_sge` is the maximum number of scatter/gather elements per work request.
    /// `srq_limit` is the limit value of the SRQ (only valid for asynchronous events).
    ///
    /// See also [RDMAmojo's `ibv_create_srq` documentation][1] and the [man page][2].
    ///
    /// [1]: https://www.rdmamojo.com/2013/01/30/ibv_create_srq/
    /// [2]: https://man7.org/linux/man-pages/man3/ibv_create_srq.3.html
    pub fn create_srq(
        &self,
        max_wr: u32,
        max_sge: u32,
        srq_limit: u32,
    ) -> Result<SharedReceiveQueue> {
        let mut srq_init_attr = ffi::ibv_srq_init_attr {
            srq_context: ptr::null_mut(),
            attr: ffi::ibv_srq_attr {
                max_wr,
                max_sge,
                srq_limit,
            },
        };
        let srq = unsafe { ffi::ibv_create_srq(self.inner.pd, &mut srq_init_attr as *mut _) };
        if srq.is_null() {
            Err(Error::CreateSharedReceiveQueue(io::Error::last_os_error()))
        } else {
            Ok(SharedReceiveQueue {
                inner: Arc::new(SharedReceiveQueueInner {
                    _pd: self.inner.clone(),
                    srq,
                }),
            })
        }
    }
}

struct SharedReceiveQueueInner {
    _pd: Arc<ProtectionDomainInner>,
    srq: *mut ffi::ibv_srq,
}

unsafe impl Sync for SharedReceiveQueueInner {}
unsafe impl Send for SharedReceiveQueueInner {}

impl Drop for SharedReceiveQueueInner {
    fn drop(&mut self) {
        let errno = unsafe { ffi::ibv_destroy_srq(self.srq) };
        if errno != 0 {
            let e = io::Error::from_raw_os_error(errno);
            panic!("{e}");
        }
    }
}

/// A shared receive queue (SRQ) that allows sharing receive buffers across multiple queue pairs.
#[derive(Clone)]
pub struct SharedReceiveQueue {
    inner: Arc<SharedReceiveQueueInner>,
}

impl SharedReceiveQueue {
    /// Returns the underlying `ibv_srq` pointer.
    ///
    /// This is an escape hatch for verbs this crate does not yet wrap. The pointer is owned by this
    /// [`SharedReceiveQueue`] and stays valid only while it is alive; do not destroy it.
    pub fn as_raw(&self) -> *mut ffi::ibv_srq {
        self.inner.srq
    }

    /// Posts a linked list of Work Requests (WRs) to this Shared Receive Queue (SRQ).
    ///
    /// Generates a HW-specific Receive Request out of it and adds it to the tail of the SRQ
    /// without performing any context switch. The RDMA device will take one of those Work Requests
    /// as soon as an incoming opcode to any Queue Pair (QP) associated with this SRQ consumes a
    /// Receive Request (RR). If there is a failure in one of the WRs because the SRQ is full or
    /// one of the attributes in the WR is bad, it stops immediately and returns the pointer to that
    /// WR.
    ///
    /// `wr_id` is a 64-bit value associated with this WR. When a Work Completion is generated
    /// when this Work Request ends, it will contain this value.
    ///
    /// Internally, the memory at `local[range]` will be received into as a single `ibv_recv_wr`.
    ///
    /// If a WR is being posted to a UD QP associated with an SRQ, the Global Routing Header (GRH)
    /// of the incoming message will be placed in the first 40 bytes of the buffer(s) in the
    /// scatter list. If no GRH is present in the incoming message, then the first bytes will be
    /// undefined. This means that in all cases, the actual data of the incoming message will start
    /// at an offset of 40 bytes into the buffer(s) in the scatter list.
    ///
    /// See also [RDMAmojo's `ibv_post_srq_recv` documentation][1] and the [man page][2].
    ///
    /// # Safety
    ///
    /// The memory region can only be safely reused or dropped after the request is fully executed
    /// and a work completion has been retrieved from the corresponding completion queue (i.e.,
    /// until `CompletionQueue::poll` returns a completion for this receive).
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: Invalid value provided in the Work Request.
    ///  - `ENOMEM`: Receive Queue is full or not enough resources to complete this operation.
    ///  - `EFAULT`: Invalid value provided in `SharedReceiveQueue`.
    ///
    /// [1]: https://www.rdmamojo.com/2013/02/08/ibv_post_srq_recv/
    /// [2]: https://man7.org/linux/man-pages/man3/ibv_post_srq_recv.3.html
    #[inline]
    pub unsafe fn post_receive(&self, local: &[LocalMemorySlice], wr_id: u64) -> Result<()> {
        let mut wr = ffi::ibv_recv_wr {
            wr_id,
            next: ptr::null_mut(),
            sg_list: local.as_ptr() as *mut ffi::ibv_sge,
            num_sge: local.len() as i32,
        };
        let mut bad_wr = ptr::null_mut();

        let ctx = unsafe { *self.inner.srq }.context;
        let ops = &mut unsafe { *ctx }.ops;
        let errno = unsafe {
            ops.post_srq_recv.as_mut().unwrap()(
                self.inner.srq,
                &mut wr as *mut _,
                &mut bad_wr as *mut _,
            )
        };
        if errno != 0 {
            Err(Error::errno(errno, Error::PostReceive))
        } else {
            Ok(())
        }
    }
}

/// One operation recorded in a [`PostBatch`] until the batch is flushed to the doorbell.
/// A receive work request, binding the lifetime of its scatter/gather buffers.
///
/// Build one with [`RecvRequest::new`] and post a batch of them with [`QueuePair::post_recv`]. Unlike
/// the send doorbell, receives are posted from a caller-owned slice, so batching allocates nothing.
#[repr(transparent)]
pub struct RecvRequest<'a> {
    wr: ffi::ibv_recv_wr,
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
/// Created by [`QueuePair::start_send`]. Each builder method posts one work request to the open block
/// immediately through the extended ("doorbell") interface; [`submit`](Self::submit) then rings the
/// doorbell once for the whole batch. Dropping the batch without submitting aborts it.
///
/// Configure a request *before* its opcode method: `batch.signaled().to(&ah, qpn, qkey).send(id,
/// sges)`. (The provider reads the work-request flags inside the opcode builder, so signaling and
/// addressing must be set first.)
///
/// # Borrow-checked guarantees
///
/// The batch borrows the queue pair for as long as it is open, so the type system enforces the
/// doorbell interface's rule that a queue pair has at most one block being built at a time. A
/// second [`start_send`](QueuePair::start_send) (or any other use of the queue pair) while a batch
/// is open is rejected at compile time:
///
/// ```compile_fail
/// # fn two_batches(qp: &mut ibverbs::QueuePair) {
/// let first = qp.start_send();
/// let second = qp.start_send(); // error: `*qp` is already mutably borrowed by `first`
/// let _ = (first, second);
/// # }
/// ```
///
/// Likewise, only one request at a time is configured on a batch: each builder method borrows the
/// batch until that work request is posted, so two half-built requests cannot overlap:
///
/// ```compile_fail
/// # fn two_requests(qp: &mut ibverbs::QueuePair) {
/// let mut batch = qp.start_send();
/// let first = batch.signaled();
/// let second = batch.signaled(); // error: `batch` is already mutably borrowed by `first`
/// let _ = (first, second);
/// # }
/// ```
#[must_use = "a started batch must be `.submit()`ed (otherwise it is aborted on drop)"]
pub struct PostBatch<'qp> {
    qpx: *mut ffi::ibv_qp_ex,
    _qp: std::marker::PhantomData<&'qp mut QueuePair>,
}

impl<'qp> PostBatch<'qp> {
    /// Mark the next work request as signaled (`IBV_SEND_SIGNALED`), so it generates a completion.
    #[inline]
    pub fn signaled(&mut self) -> PostOp<'_, 'qp> {
        PostOp {
            batch: self,
            flags: ffi::ibv_send_flags::IBV_SEND_SIGNALED.0,
            dest: None,
        }
    }

    /// Mark the next work request as fenced (`IBV_SEND_FENCE`): the device blocks it until prior
    /// RDMA reads and atomic operations on this queue pair have completed. Used to order a send or
    /// write after a read whose data it depends on.
    #[inline]
    pub fn fenced(&mut self) -> PostOp<'_, 'qp> {
        PostOp {
            batch: self,
            flags: ffi::ibv_send_flags::IBV_SEND_FENCE.0,
            dest: None,
        }
    }

    /// Mark the next work request as solicited (`IBV_SEND_SOLICITED`): a SEND or SEND-with-immediate
    /// raises a solicited event on the remote side, waking a peer blocked on its completion channel
    /// after arming with [`CompletionQueue::req_notify`] for solicited events only.
    #[inline]
    pub fn solicited(&mut self) -> PostOp<'_, 'qp> {
        PostOp {
            batch: self,
            flags: ffi::ibv_send_flags::IBV_SEND_SOLICITED.0,
            dest: None,
        }
    }

    /// Address the next work request to `ah` / `remote_qpn` / `remote_qkey` (required for sends on
    /// UD and SRD queue pairs).
    #[inline]
    pub fn to(&mut self, ah: &AddressHandle, remote_qpn: u32, remote_qkey: u32) -> PostOp<'_, 'qp> {
        PostOp {
            batch: self,
            flags: 0,
            dest: Some((ah.as_ptr(), remote_qpn, remote_qkey)),
        }
    }

    #[inline]
    fn op(&mut self) -> PostOp<'_, 'qp> {
        PostOp {
            batch: self,
            flags: 0,
            dest: None,
        }
    }

    /// Post a SEND.
    #[inline]
    pub fn send(&mut self, wr_id: u64, local: &[LocalMemorySlice]) {
        self.op().send(wr_id, local)
    }

    /// Post a SEND carrying a 32-bit immediate (passed in host byte order).
    #[inline]
    pub fn send_imm(&mut self, wr_id: u64, local: &[LocalMemorySlice], imm: u32) {
        self.op().send_imm(wr_id, local, imm)
    }

    /// Post an RDMA WRITE into the remote region `remote`.
    #[inline]
    pub fn write(&mut self, wr_id: u64, local: &[LocalMemorySlice], remote: RemoteMemorySlice) {
        self.op().write(wr_id, local, remote)
    }

    /// Post an RDMA WRITE carrying a 32-bit immediate (host byte order).
    #[inline]
    pub fn write_imm(
        &mut self,
        wr_id: u64,
        local: &[LocalMemorySlice],
        remote: RemoteMemorySlice,
        imm: u32,
    ) {
        self.op().write_imm(wr_id, local, remote, imm)
    }

    /// Post a SEND whose payload is carried inline. See [`PostOp::send_inline`].
    #[inline]
    pub fn send_inline(&mut self, wr_id: u64, data: &[u8]) {
        self.op().send_inline(wr_id, data)
    }

    /// Post a SEND with an inline payload and a 32-bit immediate. See [`PostOp::send_imm_inline`].
    #[inline]
    pub fn send_imm_inline(&mut self, wr_id: u64, data: &[u8], imm: u32) {
        self.op().send_imm_inline(wr_id, data, imm)
    }

    /// Post an RDMA WRITE with an inline payload. See [`PostOp::write_inline`].
    #[inline]
    pub fn write_inline(&mut self, wr_id: u64, data: &[u8], remote: RemoteMemorySlice) {
        self.op().write_inline(wr_id, data, remote)
    }

    /// Post an RDMA WRITE with an inline payload and a 32-bit immediate. See
    /// [`PostOp::write_imm_inline`].
    #[inline]
    pub fn write_imm_inline(
        &mut self,
        wr_id: u64,
        data: &[u8],
        remote: RemoteMemorySlice,
        imm: u32,
    ) {
        self.op().write_imm_inline(wr_id, data, remote, imm)
    }

    /// Post an RDMA READ from `remote` into `local`.
    #[inline]
    pub fn read(&mut self, wr_id: u64, local: &[LocalMemorySlice], remote: RemoteMemorySlice) {
        self.op().read(wr_id, local, remote)
    }

    /// Post an atomic compare-and-swap on the 8-byte value at `remote`.
    #[inline]
    pub fn atomic_cmp_swap(
        &mut self,
        wr_id: u64,
        local: &[LocalMemorySlice],
        remote: RemoteMemorySlice,
        compare: u64,
        swap: u64,
    ) {
        self.op()
            .atomic_cmp_swap(wr_id, local, remote, compare, swap)
    }

    /// Post an atomic fetch-and-add on the 8-byte value at `remote`.
    #[inline]
    pub fn atomic_fetch_add(
        &mut self,
        wr_id: u64,
        local: &[LocalMemorySlice],
        remote: RemoteMemorySlice,
        add: u64,
    ) {
        self.op().atomic_fetch_add(wr_id, local, remote, add)
    }

    /// Post the whole batch to the device, ringing the doorbell once.
    ///
    /// # Safety
    ///
    /// Every memory region referenced by the batch must stay valid until a work completion has been
    /// polled for the corresponding `wr_id`.
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: invalid value in one of the work requests.
    ///  - `ENOMEM`: the send queue is full or out of resources.
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

impl Drop for PostBatch<'_> {
    fn drop(&mut self) {
        // Reached only when the batch was started (`wr_start`) but not submitted; `submit` forgets
        // the batch to skip this. Abort the open work-request block.
        unsafe { (*self.qpx).wr_abort.unwrap()(self.qpx) };
    }
}

/// Per-request configuration (signaled / datagram address) for the next work request on a
/// [`PostBatch`].
///
/// Returned by [`PostBatch::signaled`] / [`PostBatch::to`]; the chosen opcode method (`send`,
/// `write`, ...) posts the request immediately.
pub struct PostOp<'b, 'qp> {
    batch: &'b mut PostBatch<'qp>,
    flags: u32,
    dest: Option<(*mut ffi::ibv_ah, u32, u32)>,
}

impl PostOp<'_, '_> {
    /// Also mark this work request signaled (`IBV_SEND_SIGNALED`).
    #[inline]
    pub fn signaled(mut self) -> Self {
        self.flags |= ffi::ibv_send_flags::IBV_SEND_SIGNALED.0;
        self
    }

    /// Also mark this work request fenced (`IBV_SEND_FENCE`): the device blocks it until prior RDMA
    /// reads and atomic operations on this queue pair have completed.
    #[inline]
    pub fn fenced(mut self) -> Self {
        self.flags |= ffi::ibv_send_flags::IBV_SEND_FENCE.0;
        self
    }

    /// Also mark this work request solicited (`IBV_SEND_SOLICITED`): a SEND or SEND-with-immediate
    /// raises a solicited event on the remote side.
    #[inline]
    pub fn solicited(mut self) -> Self {
        self.flags |= ffi::ibv_send_flags::IBV_SEND_SOLICITED.0;
        self
    }

    /// Also address this datagram send to `ah` / `remote_qpn` / `remote_qkey`.
    #[inline]
    pub fn to(mut self, ah: &AddressHandle, remote_qpn: u32, remote_qkey: u32) -> Self {
        self.dest = Some((ah.as_ptr(), remote_qpn, remote_qkey));
        self
    }

    /// Post one work request: set id and flags, run the opcode builder, then the optional datagram
    /// address, then the scatter list.
    #[inline]
    fn build(self, wr_id: u64, local: &[LocalMemorySlice], op: impl FnOnce(*mut ffi::ibv_qp_ex)) {
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
    /// [`QueuePairBuilder::set_max_inline_data`]) or the [`submit`](PostBatch::submit) fails with
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
    pub fn mask(&self) -> ffi::ibv_qp_attr_mask {
        self.mask
    }

    /// Set the next queue-pair state. Not every transition is valid; see [`QueuePair::modify`].
    pub fn set_state(&mut self, state: ffi::ibv_qp_state) -> &mut Self {
        self.attr.qp_state = state;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_STATE;
        self
    }

    /// Set the assumed current state, used to make a transition conditional on it.
    pub fn set_current_state(&mut self, state: ffi::ibv_qp_state) -> &mut Self {
        self.attr.cur_qp_state = state;
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
    pub fn set_access_flags(&mut self, access_flags: ffi::ibv_access_flags) -> &mut Self {
        self.attr.qp_access_flags = access_flags.0;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
        self
    }

    /// Set the path MTU (RC/UC only).
    pub fn set_path_mtu(&mut self, path_mtu: ffi::ibv_mtu) -> &mut Self {
        self.attr.path_mtu = path_mtu;
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

    /// Set the minimum RNR-NAK timer.
    pub fn set_min_rnr_timer(&mut self, min_rnr_timer: u8) -> &mut Self {
        self.attr.min_rnr_timer = min_rnr_timer;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER;
        self
    }

    /// Set the ACK timeout (the actual time is `4.096 * 2^timeout` microseconds; 0 means infinite).
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

    /// Set the RNR retry count (7 means retry infinitely).
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
    pub fn state(&self) -> ffi::ibv_qp_state {
        self.attr.qp_state
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
    pub fn access_flags(&self) -> ffi::ibv_access_flags {
        ffi::ibv_access_flags(self.attr.qp_access_flags)
    }

    /// The path MTU. Meaningful only if it was set or queried.
    pub fn path_mtu(&self) -> ffi::ibv_mtu {
        self.attr.path_mtu
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

/// A fully initialized and ready `QueuePair`.
///
/// A queue pair is the actual object that sends and receives data in the RDMA architecture
/// (something like a socket). It's not exactly like a socket, however. A socket is an abstraction,
/// which is maintained by the network stack and doesn't have a physical resource behind it. A QP
/// is a resource of an RDMA device and a QP number can be used by one process at the same time
/// (similar to a socket that is associated with a specific TCP or UDP port number)
#[must_use = "QueuePair is immediately destroyed via drop() unless assigned to a variable"]
pub struct QueuePair {
    pd: Arc<ProtectionDomainInner>,
    _srq: Option<SharedReceiveQueue>,
    // Keep the completion queues alive while the queue pair references them; `ibv_destroy_cq` fails
    // with EBUSY if a queue pair is still attached.
    _send_cq: Arc<CompletionQueueInner>,
    _recv_cq: Arc<CompletionQueueInner>,
    qp: *mut ffi::ibv_qp,
    // The extended (doorbell) view of `qp`, used by the send path. `ibv_qp_to_qp_ex` is a cast, so
    // this aliases `qp` and lives exactly as long.
    qp_ex: *mut ffi::ibv_qp_ex,
}

unsafe impl Send for QueuePair {}
unsafe impl Sync for QueuePair {}

impl QueuePair {
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
    /// [`PreparedQueuePair::handshake`], [`activate_ud`](PreparedQueuePair::activate_ud), and (with
    /// the `efa` feature) `activate_srd` do not cover: changing access flags at runtime, draining
    /// the send queue, moving the queue pair to `ERR` for teardown, setting a custom partition-key
    /// index or packet sequence number, and so on. Only the attributes whose mask bits are set in
    /// `attr` are applied.
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
    pub fn query(
        &self,
        mask: ffi::ibv_qp_attr_mask,
    ) -> Result<(QueuePairAttribute, QueuePairInitAttribute)> {
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
            None => Error::InvalidQueuePairTransition { current: cur, next },
            Some((required, optional)) => {
                let invalid = mask.0 & !(required | optional);
                let needed = required & !mask.0;
                if invalid == 0 && needed == 0 {
                    raw()
                } else {
                    Error::InvalidQueuePairAttributeMask {
                        current: cur,
                        next,
                        invalid: ffi::ibv_qp_attr_mask(invalid),
                        needed: ffi::ibv_qp_attr_mask(needed),
                    }
                }
            }
        }
    }

    /// Posts a single send Work Request (WR) containing a scatter-gather list of local
    /// memory slices to the Send Queue of this Queue Pair.
    ///
    /// `wr_id` is a 64 bits value associated with this WR. If a Work Completion will be generated
    /// when this Work Request ends, it will contain this value.
    ///
    /// Internally, this is a convenience wrapper around [`start_send`](Self::start_send). The local memory
    /// slices will be sent as a single `ibv_send_wr` using `IBV_WR_SEND`. The send has
    /// `IBV_SEND_SIGNALED` set, so a work completion will also be triggered as a result of this send.
    /// # Safety
    ///
    /// See [`start_send`](Self::start_send) for more details on the asynchronous execution, safety, and errors.
    #[inline]
    pub unsafe fn post_send(&mut self, local: &[LocalMemorySlice], wr_id: u64) -> Result<()> {
        let mut batch = self.start_send();
        batch.signaled().send(wr_id, local);
        unsafe { batch.submit() }
    }

    /// Posts a single unreliable-datagram (UD) send, addressed by `ah` / `remote_qpn` /
    /// `remote_qkey`. The request is signaled, so it generates a work completion.
    ///
    /// Only valid on a UD queue pair (see [`PreparedQueuePair::activate_ud`]). A single UD queue
    /// pair can address many destinations by passing a different [`AddressHandle`] per call.
    ///
    /// # Safety
    ///
    /// See [`start_send`](Self::start_send). The address handle and the local buffers must remain valid until a
    /// work completion for this request has been retrieved.
    #[inline]
    pub unsafe fn post_send_ud(
        &mut self,
        local: &[LocalMemorySlice],
        ah: &AddressHandle,
        remote_qpn: u32,
        remote_qkey: u32,
        wr_id: u64,
    ) -> Result<()> {
        let mut batch = self.start_send();
        batch
            .signaled()
            .to(ah, remote_qpn, remote_qkey)
            .send(wr_id, local);
        unsafe { batch.submit() }
    }

    /// Posts a single receive Work Request (WR) containing a scatter-gather list of local
    /// memory slices to the Receive Queue of this Queue Pair.
    ///
    /// Generates a HW-specific Receive Request out of it and add it to the tail of the Queue
    /// Pair's Receive Queue without performing any context switch. The RDMA device will take one
    /// of those Work Requests as soon as an incoming opcode to that QP will consume a Receive
    /// Request (RR). If there is a failure in one of the WRs because the Receive Queue is full or
    /// one of the attributes in the WR is bad, it stops immediately and return the pointer to that
    /// WR.
    ///
    /// `wr_id` is a 64 bits value associated with this WR. When a Work Completion is generated
    /// when this Work Request ends, it will contain this value.
    ///
    /// Internally, the local memory slices will be received into as a single `ibv_recv_wr`.
    ///
    /// See also [RDMAmojo's `ibv_post_recv` documentation][1].
    ///
    /// # Safety
    ///
    /// The memory region can only be safely reused or dropped after the request is fully executed
    /// and a work completion has been retrieved from the corresponding completion queue (i.e.,
    /// until `CompletionQueue::poll` returns a completion for this receive).
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: Invalid value provided in the Work Request.
    ///  - `ENOMEM`: Receive Queue is full or not enough resources to complete this operation.
    ///  - `EFAULT`: Invalid value provided in `QueuePair`.
    ///
    /// [1]: http://www.rdmamojo.com/2013/02/02/ibv_post_recv/
    #[inline]
    pub unsafe fn post_receive(&mut self, local: &[LocalMemorySlice], wr_id: u64) -> Result<()> {
        unsafe { self.post_recv([RecvRequest::new(wr_id, local)]) }
    }

    /// Posts a batch of receive Work Requests to this Queue Pair's receive queue with a single
    /// `ibv_post_recv`.
    ///
    /// Receives have no doorbell form, so the requests are posted as a linked list. `recvs` is the
    /// caller's storage — a stack array or a reusable `Vec` — linked in place rather than copied, so
    /// posting allocates nothing. Each request is consumed by the device when a matching message
    /// arrives.
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
    ///  - `EINVAL`: invalid value in one of the work requests.
    ///  - `ENOMEM`: the receive queue is full or out of resources.
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

    #[inline]
    /// Remote RDMA write.
    ///
    /// Immediate data can be used to signal the completion of the write operation.
    /// The other side uses `post_recv` on a dummy buffer and gets the imm data from the work completion.
    ///
    /// Internally, this is a convenience wrapper around [`start_send`](Self::start_send).
    ///
    /// # Safety
    ///
    /// See [`start_send`](Self::start_send) for more details on the asynchronous execution, safety, and errors.
    pub unsafe fn post_write(
        &mut self,
        local: &[LocalMemorySlice],
        remote: RemoteMemorySlice,
        wr_id: u64,
        imm_data: Option<u32>,
    ) -> Result<()> {
        let mut batch = self.start_send();
        match imm_data {
            Some(imm) => batch.signaled().write_imm(wr_id, local, remote, imm),
            None => batch.signaled().write(wr_id, local, remote),
        };
        unsafe { batch.submit() }
    }

    #[inline]
    /// Remote RDMA read.
    ///
    /// RDMA read does not support immediate data.
    ///
    /// Internally, this is a convenience wrapper around [`start_send`](Self::start_send).
    ///
    /// # Safety
    ///
    /// See [`start_send`](Self::start_send) for more details on the asynchronous execution, safety, and errors.
    pub unsafe fn post_read(
        &mut self,
        local: &[LocalMemorySlice],
        remote: RemoteMemorySlice,
        wr_id: u64,
    ) -> Result<()> {
        let mut batch = self.start_send();
        batch.signaled().read(wr_id, local, remote);
        unsafe { batch.submit() }
    }

    /// Begin a batch of send work requests on this queue pair's send queue.
    ///
    /// Each builder method on the returned [`PostBatch`] (`send`, `write`, `read`, the atomics, ...)
    /// appends one request; chain [`signaled`](PostOp::signaled) to request a completion and
    /// [`to`](PostOp::to) to address a datagram send. Nothing reaches the device until
    /// [`submit`](PostBatch::submit), which rings the doorbell once for the whole batch.
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
    /// batch.write(1, payload, dest);
    /// batch.signaled().send(2, note);
    /// unsafe { batch.submit() }
    /// # }
    /// ```
    #[inline]
    pub fn start_send(&mut self) -> PostBatch<'_> {
        let qpx = self.qp_ex;
        unsafe { (*qpx).wr_start.unwrap()(qpx) };
        PostBatch {
            qpx,
            _qp: std::marker::PhantomData,
        }
    }
}

impl Drop for QueuePair {
    fn drop(&mut self) {
        // TODO: ibv_destroy_qp() fails if the QP is attached to a multicast group.
        let errno = unsafe { ffi::ibv_destroy_qp(self.qp) };
        if errno != 0 {
            let e = io::Error::from_raw_os_error(errno);
            panic!("{e}");
        }
    }
}

fn calc_addr_len(bounds: impl RangeBounds<usize>, addr: u64, bytes_len: usize) -> (u64, usize) {
    let start = match bounds.start_bound() {
        std::ops::Bound::Included(i) => *i,
        std::ops::Bound::Excluded(i) => *i + 1,
        std::ops::Bound::Unbounded => 0,
    };
    let end = match bounds.end_bound() {
        std::ops::Bound::Included(i) => *i + 1,
        std::ops::Bound::Excluded(i) => *i,
        std::ops::Bound::Unbounded => bytes_len,
    };
    assert!(start < end);
    assert!(start <= bytes_len);
    assert!(end <= bytes_len);
    let addr = addr + start as u64;
    let len = end - start;
    (addr, len)
}

#[cfg(all(test, feature = "serde"))]
mod test_serde {
    use super::*;
    #[test]
    fn encode_decode() {
        let qpe_default = QueuePairEndpoint {
            num: 72,
            lid: 9,
            gid: Some(Default::default()),
        };

        let mut qpe = qpe_default;
        qpe.gid.as_mut().unwrap().raw =
            unsafe { std::mem::transmute::<[u64; 2], [u8; 16]>([87_u64.to_be(), 192_u64.to_be()]) };
        let encoded = bincode::serialize(&qpe).unwrap();

        let decoded: QueuePairEndpoint = bincode::deserialize(&encoded).unwrap();
        assert_eq!(decoded.gid.unwrap().subnet_prefix(), 87);
        assert_eq!(decoded.gid.unwrap().interface_id(), 192);
        assert_eq!(qpe, decoded);
        assert_ne!(qpe, qpe_default);
    }

    #[test]
    fn encode_decode_guid() {
        let guid_u64 = 0x12_34_56_78_9a_bc_de_f0_u64;
        let _be: ffi::__be64 = guid_u64.to_be();
        let guid: Guid = guid_u64.into();

        assert!(!guid.is_reserved());
        assert_eq!(guid.raw, [0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0]);
        println!("{:#08x}", guid.oui());
        assert_eq!(guid.oui(), 0x123456);
    }

    #[test]
    fn test_local_memory_slice_sge_memory_layout() {
        assert_eq!(
            std::mem::size_of::<LocalMemorySlice>(),
            std::mem::size_of::<ffi::ibv_sge>()
        );
    }
}

// ---------------------------------------------------------------------------
// AWS Elastic Fabric Adapter (EFA) support, behind the `efa` feature.
//
// EFA's transport is SRD (Scalable Reliable Datagram): reliable like RC but connectionless and
// addressed like UD. Only queue-pair *creation* is EFA-specific (`efadv_create_qp_ex`); the resulting
// queue pair is created with the same extended send operations as every other, so sends go through
// the normal doorbell path ([`QueuePair::post_send_ud`] / [`QueuePair::start_send`] with a `.to(..)`),
// receives through [`QueuePair::post_receive`], and completions through [`CompletionQueue::poll`].
// ---------------------------------------------------------------------------

#[cfg(feature = "efa")]
impl ProtectionDomain {
    /// Begin building an EFA SRD queue pair associated with this protection domain.
    ///
    /// Configure it like any other queue pair (GID index, queue/SGE limits), then create it with
    /// [`build_srd`](QueuePairBuilder::build_srd) and bring it to ready with
    /// [`activate_srd`](PreparedQueuePair::activate_srd). Send with
    /// [`post_send_ud`](QueuePair::post_send_ud) (or `start_send().to(..)`), receive with
    /// [`post_receive`](QueuePair::post_receive).
    pub fn create_srd_qp(
        &self,
        send: &CompletionQueue,
        recv: &CompletionQueue,
    ) -> Result<QueuePairBuilder> {
        self.create_qp(send, recv, ffi::ibv_qp_type::IBV_QPT_DRIVER)
    }
}

#[cfg(feature = "efa")]
impl QueuePairBuilder {
    /// Create the EFA SRD queue pair described by this builder (`efadv_create_qp_ex`).
    ///
    /// Like [`build`](Self::build) but for SRD: it enables the send and one-sided RDMA doorbell
    /// operations, then activate it with [`activate_srd`](PreparedQueuePair::activate_srd).
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: invalid value in the queue pair attributes.
    ///  - `ENOMEM`: not enough resources to complete this operation.
    pub fn build_srd(&self) -> Result<PreparedQueuePair> {
        use ffi::ibv_qp_create_send_ops_flags as SendOps;
        // SRD supports send and one-sided RDMA, including the immediate variants.
        let send_ops_flags = SendOps::IBV_QP_EX_WITH_SEND.0
            | SendOps::IBV_QP_EX_WITH_SEND_WITH_IMM.0
            | SendOps::IBV_QP_EX_WITH_RDMA_WRITE.0
            | SendOps::IBV_QP_EX_WITH_RDMA_WRITE_WITH_IMM.0
            | SendOps::IBV_QP_EX_WITH_RDMA_READ.0;

        // As in `build`: zero the storage and write only the fields the driver reads, handing the
        // pointer to C without `assume_init` (the `qp_type` enum has no zero variant).
        let mut attr = std::mem::MaybeUninit::<ffi::ibv_qp_init_attr_ex>::zeroed();
        let p = attr.as_mut_ptr();
        unsafe {
            (*p).qp_context = ptr::null::<c_void>().offset(self.ctx) as *mut _;
            (*p).send_cq = self.send.cq();
            (*p).recv_cq = self.recv.cq();
            (*p).cap = ffi::ibv_qp_cap {
                max_send_wr: self.max_send_wr,
                max_recv_wr: self.max_recv_wr,
                max_send_sge: self.max_send_sge,
                max_recv_sge: self.max_recv_sge,
                max_inline_data: self.max_inline_data,
            };
            (*p).qp_type = ffi::ibv_qp_type::IBV_QPT_DRIVER;
            (*p).comp_mask = ffi::ibv_qp_init_attr_mask::IBV_QP_INIT_ATTR_PD.0
                | ffi::ibv_qp_init_attr_mask::IBV_QP_INIT_ATTR_SEND_OPS_FLAGS.0;
            (*p).pd = self.pd.pd;
            (*p).send_ops_flags = send_ops_flags as u64;
        }

        let mut efa_attr = ffi::efadv_qp_init_attr {
            comp_mask: 0,
            driver_qp_type: ffi::EFADV_QP_DRIVER_TYPE_SRD as u32,
            flags: 0,
            sl: 0,
            reserved: 0,
        };
        let qp = unsafe {
            ffi::efadv_create_qp_ex(
                self.pd.ctx.ctx,
                attr.as_mut_ptr(),
                &mut efa_attr as *mut _,
                std::mem::size_of::<ffi::efadv_qp_init_attr>() as u32,
            )
        };
        if qp.is_null() {
            return Err(Error::CreateQueuePair(io::Error::last_os_error()));
        }
        let qp_ex = unsafe { ffi::ibv_qp_to_qp_ex(qp) };
        Ok(PreparedQueuePair {
            lid: self.port_attr.lid,
            port_num: self.port_num,
            qp: QueuePair {
                pd: self.pd.clone(),
                _srq: None,
                _send_cq: self.send.clone(),
                _recv_cq: self.recv.clone(),
                qp,
                qp_ex,
            },
            gid_index: self.gid_index,
            traffic_class: self.traffic_class,
            access: None,
            timeout: None,
            retry_count: None,
            rnr_retry: None,
            min_rnr_timer: None,
            max_rd_atomic: None,
            max_dest_rd_atomic: None,
            path_mtu: None,
            rq_psn: None,
            service_level: self.service_level,
        })
    }
}

#[cfg(feature = "efa")]
impl PreparedQueuePair {
    /// Transition an EFA SRD queue pair to ready with the given Q_Key.
    ///
    /// SRD is connectionless, so this needs no remote endpoint; the transitions are the same as a UD
    /// queue pair's. Address each send with an [`AddressHandle`] (see
    /// [`post_send_ud`](QueuePair::post_send_ud)).
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: invalid value provided in `attr` or `attr_mask`.
    ///  - `ENOMEM`: not enough resources to complete this operation.
    pub fn activate_srd(self, qkey: u32) -> Result<QueuePair> {
        self.activate_ud(qkey)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn gid_array_conversion() {
        let arr = [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88,
        ];
        let arr2: [u8; 16] = Gid::from(arr).into();
        assert_eq!(arr, arr2);
    }

    #[test]
    fn gid_ipv6_conversion_and_display() {
        let addr: std::net::Ipv6Addr = "fe80::5054:ff:fe12:3456".parse().unwrap();
        let gid = Gid::from(addr);
        assert_eq!(std::net::Ipv6Addr::from(gid), addr);
        assert_eq!(gid.to_string(), "fe80::5054:ff:fe12:3456");
        assert_eq!(format!("{gid:?}"), "Gid(fe80::5054:ff:fe12:3456)");
        assert!(!gid.is_ipv4_mapped());
    }

    #[test]
    fn gid_ipv4_mapped() {
        let gid = Gid::from("::ffff:192.0.2.1".parse::<std::net::Ipv6Addr>().unwrap());
        assert!(gid.is_ipv4_mapped());
        assert_eq!(gid.to_string(), "::ffff:192.0.2.1");
        assert_eq!(gid.subnet_prefix(), 0);
        assert_eq!(gid.interface_id() >> 32, 0xffff);
    }

    #[test]
    fn local_memory_slice_sge_roundtrip() {
        let sge = ffi::ibv_sge {
            addr: 0xdead_beef,
            length: 64,
            lkey: 42,
        };
        let slice = LocalMemorySlice::from(sge);
        assert_eq!(slice.addr(), 0xdead_beef);
        assert_eq!(slice.len(), 64);
        assert_eq!(slice.lkey(), 42);
        let back = ffi::ibv_sge::from(slice);
        assert_eq!(back.addr, sge.addr);
        assert_eq!(back.length, sge.length);
        assert_eq!(back.lkey, sge.lkey);
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

#[cfg(test)]
mod test_display {
    use super::*;

    #[test]
    fn guid_formats_as_four_groups() {
        let guid = Guid::from(0x0002_c903_00a0_7c8e_u64);
        assert_eq!(guid.to_string(), "0002:c903:00a0:7c8e");
        assert_eq!(format!("{guid:?}"), "Guid(0002:c903:00a0:7c8e)");
    }

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
