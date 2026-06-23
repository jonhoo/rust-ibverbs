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

use std::convert::TryInto;
use std::ffi::CStr;
use std::io;
use std::ops::{Deref, DerefMut, RangeBounds};
use std::os::fd::BorrowedFd;
use std::os::raw::c_void;
use std::ptr;
use std::sync::Arc;
use std::time::Duration;

const PORT_NUM: u8 = 1;

/// Direct access to low-level libverbs FFI.
pub use ffi::ibv_gid_type;
pub use ffi::ibv_mtu;
pub use ffi::ibv_qp_type;
pub use ffi::ibv_send_wr;
pub use ffi::ibv_wc;
pub use ffi::ibv_wc_opcode;
pub use ffi::ibv_wc_status;

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

/// Get list of available RDMA devices.
///
/// # Errors
///
///  - `EPERM`: Permission denied.
///  - `ENOMEM`: Insufficient memory to complete the operation.
///  - `ENOSYS`: No kernel support for RDMA.
pub fn devices() -> io::Result<DeviceList> {
    let mut n = 0i32;
    let devices = unsafe { ffi::ibv_get_device_list(&mut n as *mut _) };

    if devices.is_null() {
        return Err(io::Error::last_os_error());
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

/// A Global unique identifier for ibv.
///
/// This struct acts as a rust wrapper for GUID value represented as `__be64` in
/// libibverbs. We introduce this struct, because u64 is stored in host
/// endianness, whereas ibverbs stores GUID in network order (big endian).
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Default, Copy, Clone, Debug, Eq, PartialEq, Hash)]
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
    ///  - `EINVAL`: `PORT_NUM` is invalid (from `ibv_query_port_attr`).
    ///  - `ENOMEM`: Out of memory (from `ibv_query_port_attr`).
    ///  - `EMFILE`: Too many files are opened by this process (from `ibv_query_gid`).
    ///  - Other: the device is not in `ACTIVE` or `ARMED` state.
    pub fn open(&self) -> io::Result<Context> {
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
    pub fn guid(&self) -> io::Result<Guid> {
        let guid_int = unsafe { ffi::ibv_get_device_guid(*self.0) };
        let guid: Guid = guid_int.into();
        if guid.is_reserved() {
            Err(io::Error::last_os_error())
        } else {
            Ok(guid)
        }
    }

    /// Returns stable IB device index as it is assigned by the kernel
    /// # Errors
    ///
    ///  - `ENOTSUP`: Stable index is not supported
    pub fn index(&self) -> io::Result<i32> {
        let idx = unsafe { ffi::ibv_get_device_index(*self.0) };
        if idx == -1 {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "device index not known",
            ))
        } else {
            Ok(idx)
        }
    }
}

struct ContextInner {
    ctx: *mut ffi::ibv_context,
}

impl ContextInner {
    fn query_port(&self) -> io::Result<ffi::ibv_port_attr> {
        // TODO: from http://www.rdmamojo.com/2012/07/21/ibv_query_port/
        //
        //   Most of the port attributes, returned by ibv_query_port(), aren't constant and may be
        //   changed, mainly by the SM (in InfiniBand), or by the Hardware. It is highly
        //   recommended avoiding saving the result of this query, or to flush them when a new SM
        //   (re)configures the subnet.
        //
        let mut port_attr = ffi::ibv_port_attr::default();
        let errno = unsafe {
            ffi::ibv_query_port(
                self.ctx,
                PORT_NUM,
                &mut port_attr as *mut ffi::ibv_port_attr as *mut _,
            )
        };
        if errno != 0 {
            return Err(io::Error::from_raw_os_error(errno));
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
                return Err(io::Error::other("port is not ACTIVE or ARMED"));
            }
        }
        Ok(port_attr)
    }
}

impl Drop for ContextInner {
    fn drop(&mut self) {
        let ok = unsafe { ffi::ibv_close_device(self.ctx) };
        assert_eq!(ok, 0);
    }
}

unsafe impl Sync for ContextInner {}
unsafe impl Send for ContextInner {}

/// An RDMA context bound to a device.
#[must_use]
pub struct Context {
    inner: Arc<ContextInner>,
}

impl Context {
    /// Opens a context for the given device, and queries its port and gid.
    fn with_device(dev: *mut ffi::ibv_device) -> io::Result<Context> {
        assert!(!dev.is_null());

        let ctx = unsafe { ffi::ibv_open_device(dev) };
        if ctx.is_null() {
            return Err(io::Error::other("failed to open device"));
        }
        let inner = Arc::new(ContextInner { ctx });

        let ctx = Context { inner };
        // checks that the port is active/armed.
        ctx.inner.query_port()?;
        Ok(ctx)
    }

    /// Create a completion queue (CQ).
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
    /// `min_cq_entries` defines the minimum size of the CQ. The actual created size can be equal
    /// or higher than this value. `id` is an opaque identifier that is echoed by
    /// `CompletionQueue::poll`.
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: Invalid `min_cq_entries` (must be `1 <= cqe <= dev_cap.max_cqe`).
    ///  - `ENOMEM`: Not enough resources to complete this operation.
    pub fn create_cq(&self, min_cq_entries: i32, id: isize) -> io::Result<CompletionQueue> {
        let cc = unsafe { ffi::ibv_create_comp_channel(self.inner.ctx) };
        if cc.is_null() {
            return Err(io::Error::last_os_error());
        }

        let cc_fd = unsafe { *cc }.fd;
        let flags = nix::fcntl::fcntl(cc_fd, nix::fcntl::F_GETFL)?;
        // the file descriptor needs to be set to non-blocking because `ibv_get_cq_event()`
        // would block otherwise.
        let arg = nix::fcntl::FcntlArg::F_SETFL(
            nix::fcntl::OFlag::from_bits_retain(flags) | nix::fcntl::OFlag::O_NONBLOCK,
        );
        nix::fcntl::fcntl(cc_fd, arg)?;

        // Request the standard work-completion fields so the lazy readers in `WorkCompletion` can
        // serve them. Optional fields (timestamps, CVLAN, ...) are not requested, since not every
        // provider supports them.
        let wc_flags = ffi::ibv_create_cq_wc_flags::IBV_WC_EX_WITH_BYTE_LEN.0
            | ffi::ibv_create_cq_wc_flags::IBV_WC_EX_WITH_IMM.0
            | ffi::ibv_create_cq_wc_flags::IBV_WC_EX_WITH_QP_NUM.0
            | ffi::ibv_create_cq_wc_flags::IBV_WC_EX_WITH_SRC_QP.0;
        let mut cq_attr = ffi::ibv_cq_init_attr_ex {
            cqe: min_cq_entries as u32,
            cq_context: unsafe { ptr::null::<c_void>().offset(id) } as *mut _,
            channel: cc,
            comp_vector: 0,
            wc_flags: wc_flags as u64,
            comp_mask: 0,
            flags: 0,
            parent_domain: ptr::null_mut(),
        };
        let cq_ex = unsafe { ffi::ibv_create_cq_ex(self.inner.ctx, &mut cq_attr as *mut _) };

        if cq_ex.is_null() {
            Err(io::Error::last_os_error())
        } else {
            Ok(CompletionQueue {
                inner: Arc::new(CompletionQueueInner {
                    _ctx: self.inner.clone(),
                    cc,
                    cq_ex,
                }),
            })
        }
    }

    /// Allocate a protection domain (PDs) for the device's context.
    ///
    /// The created PD will be used primarily to create `QueuePair`s and `MemoryRegion`s.
    ///
    /// A protection domain is a means of protection, and helps you create a group of object that
    /// can work together. If several objects were created using PD1, and others were created using
    /// PD2, working with objects from group1 together with objects from group2 will not work.
    pub fn alloc_pd(&self) -> io::Result<ProtectionDomain> {
        let pd = unsafe { ffi::ibv_alloc_pd(self.inner.ctx) };
        if pd.is_null() {
            Err(io::Error::other("obv_alloc_pd returned null"))
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
    pub fn gid_table(&self) -> io::Result<Vec<GidEntry>> {
        let max_entries = self.inner.query_port()?.gid_tbl_len as usize;
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
            return Err(io::Error::other(format!(
                "failed to query gid table, error={}",
                -num_entries
            )));
        }
        gid_table.truncate(num_entries as usize);
        let gid_table = gid_table.into_iter().map(GidEntry::from).collect();
        Ok(gid_table)
    }
}

struct CompletionQueueInner {
    _ctx: Arc<ContextInner>,
    cq_ex: *mut ffi::ibv_cq_ex,
    cc: *mut ffi::ibv_comp_channel,
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

        let errno = unsafe { ffi::ibv_destroy_comp_channel(self.cc) };
        if errno != 0 {
            let e = io::Error::from_raw_os_error(errno);
            panic!("{e}");
        }
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
    pub fn ok(&self) -> Result<(), (ffi::ibv_wc_status, u32)> {
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
    /// `poll` does not block or cause a context switch; use [`wait`](Self::wait) to block.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ibverbs::CompletionQueue;
    /// # fn drain(cq: &CompletionQueue) -> std::io::Result<()> {
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
    pub fn poll(&self) -> io::Result<Option<Completions<'_>>> {
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
            e => Err(io::Error::from_raw_os_error(e)),
        }
    }

    /// Block until at least one work completion is available, then begin polling.
    ///
    /// Unlike [`poll`](Self::poll), this arms the completion channel and blocks (up to `timeout`)
    /// rather than returning `None` on an empty queue. It returns the same lending [`Completions`]
    /// iterator.
    ///
    /// # Errors
    ///  - `TimedOut`: the timeout expired before any completion arrived.
    ///  - System errors from `req_notify_cq`, `poll`, or `ibv_get_cq_event`.
    pub fn wait(&self, timeout: Option<Duration>) -> io::Result<Completions<'_>> {
        loop {
            // Arm the completion channel, then poll: polling after arming closes the race where a
            // completion arrives between an earlier poll and arming.
            let cq = self.inner.cq();
            let ctx = unsafe { *cq }.context;
            let errno = unsafe { (*ctx).ops.req_notify_cq.unwrap()(cq, 0) };
            if errno != 0 {
                return Err(io::Error::from_raw_os_error(errno));
            }
            if let Some(completions) = self.poll()? {
                return Ok(completions);
            }

            let pollfd = nix::poll::PollFd::new(
                // SAFETY: the comp channel fd lives until this `CompletionQueue`'s `Drop`, and we
                // still hold `self`.
                unsafe { BorrowedFd::borrow_raw({ *self.inner.cc }.fd) },
                nix::poll::PollFlags::POLLIN,
            );
            let ret = nix::poll::poll(
                &mut [pollfd],
                timeout
                    .map(nix::poll::PollTimeout::try_from)
                    .transpose()
                    .map_err(|_| io::Error::other("failed to convert timeout to PollTimeout"))?,
            )?;
            match ret {
                0 => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "Timed out during completion queue wait",
                    ));
                }
                1 => {}
                _ => unreachable!("we passed 1 fd to poll, but it returned {ret}"),
            }

            let mut out_cq = std::ptr::null_mut();
            let mut out_cq_context = std::ptr::null_mut();
            // The completion notification must be read with ibv_get_cq_event(); the fd was set
            // non-blocking so this does not block.
            let rc =
                unsafe { ffi::ibv_get_cq_event(self.inner.cc, &mut out_cq, &mut out_cq_context) };
            if rc < 0 {
                let e = io::Error::last_os_error();
                if e.kind() == io::ErrorKind::WouldBlock {
                    continue;
                }
                return Err(e);
            }
            assert_eq!(self.inner.cq(), out_cq);
            assert!(out_cq_context.is_null());

            // Every event from ibv_get_cq_event() must eventually be acknowledged.
            unsafe { ffi::ibv_ack_cq_events(self.inner.cq(), 1) };
        }
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
    pub fn build(&self) -> io::Result<PreparedQueuePair> {
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
            Err(io::Error::last_os_error())
        } else {
            let qp_ex = unsafe { ffi::ibv_qp_to_qp_ex(qp) };
            Ok(PreparedQueuePair {
                lid: self.port_attr.lid,
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
#[derive(Default, Copy, Clone, Debug, Eq, PartialEq, Hash)]
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
    /// Get the network endpoint for this `QueuePair`.
    ///
    /// This endpoint will need to be communicated to the `QueuePair` on the remote end.
    pub fn endpoint(&self) -> io::Result<QueuePairEndpoint> {
        let num = unsafe { &*self.qp.qp }.qp_num;
        let gid = if let Some(gid_index) = self.gid_index {
            let mut gid = ffi::ibv_gid::default();
            let rc = unsafe {
                ffi::ibv_query_gid(self.qp.pd.ctx.ctx, PORT_NUM, gid_index as i32, &mut gid)
            };
            if rc < 0 {
                return Err(io::Error::last_os_error());
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
    /// The handshake also sets the following parameters, which are currently not configurable:
    ///
    /// # Examples
    ///
    /// ```text,ignore
    /// port_num = PORT_NUM;
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
    pub fn handshake(self, remote: QueuePairEndpoint) -> io::Result<QueuePair> {
        // init and associate with port
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_INIT,
            pkey_index: 0,
            port_num: PORT_NUM,
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
            return Err(io::Error::from_raw_os_error(errno));
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
                port_num: PORT_NUM,
                grh: Default::default(),
                ..Default::default()
            },
            ..Default::default()
        };
        if let Some(gid) = remote.gid {
            attr.ah_attr.is_global = 1;
            attr.ah_attr.grh.dgid = gid.into();
            attr.ah_attr.grh.hop_limit = 0xff;
            attr.ah_attr.grh.sgid_index = self
                .gid_index
                .ok_or_else(|| io::Error::other("gid was set for remote but not local"))?
                as u8;
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
            return Err(io::Error::from_raw_os_error(errno));
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
            return Err(io::Error::from_raw_os_error(errno));
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
    pub fn activate_ud(self, qkey: u32) -> io::Result<QueuePair> {
        // INIT: associate with the port and set the Q_Key. UD has no access flags.
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_INIT,
            pkey_index: 0,
            port_num: PORT_NUM,
            qkey,
            ..Default::default()
        };
        let mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE
            | ffi::ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
            | ffi::ibv_qp_attr_mask::IBV_QP_PORT
            | ffi::ibv_qp_attr_mask::IBV_QP_QKEY;
        let errno = unsafe { ffi::ibv_modify_qp(self.qp.qp, &mut attr as *mut _, mask.0 as i32) };
        if errno != 0 {
            return Err(io::Error::from_raw_os_error(errno));
        }

        // RTR: a UD queue pair needs no path or destination information.
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_RTR,
            ..Default::default()
        };
        let mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE;
        let errno = unsafe { ffi::ibv_modify_qp(self.qp.qp, &mut attr as *mut _, mask.0 as i32) };
        if errno != 0 {
            return Err(io::Error::from_raw_os_error(errno));
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
            return Err(io::Error::from_raw_os_error(errno));
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
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize, Debug, Clone))]
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
    /// Create an [`AddressHandle`] for the destination described by `attr`.
    ///
    /// Address handles are used to address unreliable-datagram (UD) sends. One handle can be reused
    /// across many sends, and a single UD queue pair can hold handles to many destinations.
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: Invalid value provided in `attr`.
    ///  - `ENOMEM`: Not enough resources to complete this operation.
    pub fn create_address_handle(
        &self,
        attr: &AddressHandleAttribute,
    ) -> io::Result<AddressHandle> {
        let mut ah_attr = attr.attr;
        let ah = unsafe { ffi::ibv_create_ah(self.inner.pd, &mut ah_attr as *mut _) };
        if ah.is_null() {
            Err(io::Error::last_os_error())
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
    ) -> io::Result<()> {
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
            Err(io::Error::from_raw_os_error(ret))
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
    pub fn create_qp(
        &self,
        send: &CompletionQueue,
        recv: &CompletionQueue,
        qp_type: ffi::ibv_qp_type,
    ) -> io::Result<QueuePairBuilder> {
        let port_attr = self.inner.ctx.query_port()?;
        Ok(QueuePairBuilder::new(
            self.inner.clone(),
            port_attr,
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
    ) -> io::Result<MemoryRegionInner> {
        let mr = ffi::ibv_reg_mr(self.inner.pd, ptr, len, access_flags.0 as i32);
        // ibv_reg_mr() returns a pointer to the registered MR, or NULL if the request fails.
        if mr.is_null() {
            Err(io::Error::last_os_error())
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
    ) -> io::Result<MemoryRegion<Box<[u8]>>> {
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
    pub fn allocate(&self, n: usize) -> io::Result<MemoryRegion<Box<[u8]>>> {
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
    ) -> io::Result<MemoryRegion<()>> {
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
    ) -> io::Result<MemoryRegion<()>> {
        let mr = unsafe {
            ffi::ibv_reg_dmabuf_mr(self.inner.pd, offset, len, iova, fd, access_flags.0 as i32)
        };

        if mr.is_null() {
            Err(io::Error::last_os_error())
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
    ) -> io::Result<SharedReceiveQueue> {
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
            Err(io::Error::last_os_error())
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
    pub unsafe fn post_receive(&self, local: &[LocalMemorySlice], wr_id: u64) -> io::Result<()> {
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
            Err(io::Error::from_raw_os_error(errno))
        } else {
            Ok(())
        }
    }
}

/// One operation recorded in a [`PostBatch`] until the batch is flushed to the doorbell.
/// A batch of send work requests being built on a [`QueuePair`]'s send queue.
///
/// Created by [`QueuePair::start`]. Each builder method posts one work request to the open block
/// immediately through the extended ("doorbell") interface; [`submit`](Self::submit) then rings the
/// doorbell once for the whole batch. Dropping the batch without submitting aborts it.
///
/// Configure a request *before* its opcode method: `batch.signaled().to(&ah, qpn, qkey).send(id,
/// sges)`. (The provider reads the work-request flags inside the opcode builder, so signaling and
/// addressing must be set first.)
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
    pub unsafe fn submit(self) -> io::Result<()> {
        let qpx = self.qpx;
        // Disarm the abort-on-drop before completing: `Drop` would otherwise `wr_abort` the block we
        // are about to `wr_complete`.
        std::mem::forget(self);
        let ret = unsafe { (*qpx).wr_complete.unwrap()(qpx) };
        if ret != 0 {
            Err(io::Error::from_raw_os_error(ret))
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

    /// Post a SEND.
    #[inline]
    pub fn send(self, wr_id: u64, local: &[LocalMemorySlice]) {
        self.build(wr_id, local, |q| unsafe { (*q).wr_send.unwrap()(q) })
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

    /// Posts a single send Work Request (WR) containing a scatter-gather list of local
    /// memory slices to the Send Queue of this Queue Pair.
    ///
    /// `wr_id` is a 64 bits value associated with this WR. If a Work Completion will be generated
    /// when this Work Request ends, it will contain this value.
    ///
    /// Internally, this is a convenience wrapper around [`post`](Self::post). The local memory
    /// slices will be sent as a single `ibv_send_wr` using `IBV_WR_SEND`. The send has
    /// `IBV_SEND_SIGNALED` set, so a work completion will also be triggered as a result of this send.
    /// # Safety
    ///
    /// See [`post`](Self::post) for more details on the asynchronous execution, safety, and errors.
    #[inline]
    pub unsafe fn post_send(&mut self, local: &[LocalMemorySlice], wr_id: u64) -> io::Result<()> {
        let mut batch = self.start();
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
    /// See [`post`](Self::post). The address handle and the local buffers must remain valid until a
    /// work completion for this request has been retrieved.
    #[inline]
    pub unsafe fn post_send_ud(
        &mut self,
        local: &[LocalMemorySlice],
        ah: &AddressHandle,
        remote_qpn: u32,
        remote_qkey: u32,
        wr_id: u64,
    ) -> io::Result<()> {
        let mut batch = self.start();
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
    pub unsafe fn post_receive(
        &mut self,
        local: &[LocalMemorySlice],
        wr_id: u64,
    ) -> io::Result<()> {
        let mut wr = ffi::ibv_recv_wr {
            wr_id,
            next: ptr::null::<ffi::ibv_send_wr>() as *mut _,
            sg_list: local.as_ptr() as *mut ffi::ibv_sge,
            num_sge: local.len() as i32,
        };
        let mut bad_wr: *mut ffi::ibv_recv_wr = ptr::null::<ffi::ibv_recv_wr>() as *mut _;

        // TODO:
        //
        // If the QP qp is associated with a shared receive queue, you must use the function
        // ibv_post_srq_recv(), and not ibv_post_recv(), since the QP's own receive queue will not
        // be used.
        //
        // If a WR is being posted to a UD QP, the Global Routing Header (GRH) of the incoming
        // message will be placed in the first 40 bytes of the buffer(s) in the scatter list. If no
        // GRH is present in the incoming message, then the first  bytes  will  be undefined. This
        // means that in all cases, the actual data of the incoming message will start at an offset
        // of 40 bytes into the buffer(s) in the scatter list.

        let ctx = unsafe { *self.qp }.context;
        let ops = &mut unsafe { *ctx }.ops;
        let errno = unsafe {
            ops.post_recv.as_mut().unwrap()(self.qp, &mut wr as *mut _, &mut bad_wr as *mut _)
        };
        if errno != 0 {
            Err(io::Error::from_raw_os_error(errno))
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
    /// Internally, this is a convenience wrapper around [`post`](Self::post).
    ///
    /// # Safety
    ///
    /// See [`post`](Self::post) for more details on the asynchronous execution, safety, and errors.
    pub unsafe fn post_write(
        &mut self,
        local: &[LocalMemorySlice],
        remote: RemoteMemorySlice,
        wr_id: u64,
        imm_data: Option<u32>,
    ) -> io::Result<()> {
        let mut batch = self.start();
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
    /// Internally, this is a convenience wrapper around [`post`](Self::post).
    ///
    /// # Safety
    ///
    /// See [`post`](Self::post) for more details on the asynchronous execution, safety, and errors.
    pub unsafe fn post_read(
        &mut self,
        local: &[LocalMemorySlice],
        remote: RemoteMemorySlice,
        wr_id: u64,
    ) -> io::Result<()> {
        let mut batch = self.start();
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
    /// The scatter/gather slices passed to each op must outlive the batch (the borrow checker
    /// enforces this), and — as with any send — the underlying memory must stay valid until a work
    /// completion has been polled for the request.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ibverbs::{QueuePair, LocalMemorySlice, RemoteMemorySlice};
    /// # unsafe fn f(qp: &mut QueuePair, payload: &[LocalMemorySlice], dest: RemoteMemorySlice, note: &[LocalMemorySlice]) -> std::io::Result<()> {
    /// let mut batch = qp.start();
    /// batch.write(1, payload, dest);
    /// batch.signaled().send(2, note);
    /// unsafe { batch.submit() }
    /// # }
    /// ```
    #[inline]
    pub fn start(&mut self) -> PostBatch<'_> {
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
}
