//! Rust API wrapping the `ibverbs` RDMA library.
//!
//! `libibverbs` is a library that allows userspace processes to use RDMA "verbs" to perform
//! high-throughput, low-latency network operations.
//!
//! A good place to start is to look at the programs in [`examples/`](examples/), and the upstream
//! [C examples]. You can test RDMA programs on modern Linux kernels even without specialized RDMA
//! hardware by using [SoftRoCE][soft].
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
//! # Hardware support
//!
//! iWARP ethernet NICs support RDMA over hardware-offloaded TCP/IP, while InfiniBand is a general
//! high-throughput, low-latency networking technology. InfiniBand host channel adapters (HCAs) and
//! iWARP NICs commonly support direct hardware access from userspace (kernel bypass), and
//! libibverbs supports this when available.
//!
//! # Thread safety
//!
//! All interfaces are `Sync` and `Send` since the underlying ibverbs API [is thread safe][safe].
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

#![deny(missing_docs)]
#![feature(slice_get_slice)]

use std::marker::PhantomData;
use std::ptr;
use std::mem;
use std::os::raw::{c_void, c_int};
use std::error::Error;
use std::io;

const PORT_NUM: u8 = 1;

#[allow(non_upper_case_globals)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[allow(missing_docs)]
pub mod ffi;

pub use ffi::ibv_wc;
pub use ffi::ibv_qp_type;
pub use ffi::ibv_wc_status;
pub use ffi::ibv_wc_opcode;

pub use ffi::ibv_access_flags;
pub use ffi::IBV_ACCESS_LOCAL_WRITE;
pub use ffi::IBV_ACCESS_MW_BIND;
pub use ffi::IBV_ACCESS_ON_DEMAND;
pub use ffi::IBV_ACCESS_REMOTE_ATOMIC;
pub use ffi::IBV_ACCESS_REMOTE_READ;
pub use ffi::IBV_ACCESS_REMOTE_WRITE;
pub use ffi::IBV_ACCESS_ZERO_BASED;

/// Get list of available RDMA devices.
pub fn devices() -> io::Result<DeviceList> {
    let mut n = 0i32;
    let devices = unsafe { ffi::ibv_get_device_list(mem::transmute(&mut n)) };

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
    pub fn iter(&self) -> DeviceListIter {
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
    pub fn get(&self, index: usize) -> Option<Device> {
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
pub struct Device<'a>(&'a *mut ffi::ibv_device);
unsafe impl<'a> Sync for Device<'a> {}
unsafe impl<'a> Send for Device<'a> {}

impl<'a> From<&'a *mut ffi::ibv_device> for Device<'a> {
    fn from(d: &'a *mut ffi::ibv_device) -> Self {
        Device(d)
    }
}

impl<'a> Device<'a> {
    /// Opens an RMDA device and creates a context for further use.
    pub fn open(&self) -> io::Result<Context> {
        Context::with_device(*self.0)
    }
}

/// An RDMA context bound to a device.
pub struct Context {
    ctx: *mut ffi::ibv_context,
    port_attr: ffi::ibv_port_attr,
    gid: ffi::ibv_gid,
}

unsafe impl Sync for Context {}
unsafe impl Send for Context {}

impl Context {
    fn with_device(dev: *mut ffi::ibv_device) -> io::Result<Context> {
        assert!(!dev.is_null());

        let ctx = unsafe { ffi::ibv_open_device(dev) };
        if ctx.is_null() {
            return Err(io::Error::new(io::ErrorKind::Other, format!("failed to open device")));
        }

        let mut port_attr = ffi::ibv_port_attr::default();
        let errno = unsafe { ffi::ibv_query_port(ctx, PORT_NUM, mem::transmute(&mut port_attr)) };
        if errno != 0 {
            return Err(std::io::Error::from_raw_os_error(errno));
        }

        let mut gid = ffi::ibv_gid::default();
        let ok = unsafe { ffi::ibv_query_gid(ctx, PORT_NUM, 0, mem::transmute(&mut gid)) };
        if ok != 0 {
            return Err(io::Error::new(io::ErrorKind::Other, format!("failed to query gid")));
        }

        Ok(Context {
               ctx,
               port_attr,
               gid,
           })
    }

    /// Create a completion queue (CQ).
    ///
    /// `create_cq` creates a completion queue with at least `min_cq_entries` entries for the RDMA
    /// device context context. `id` is an opaque identifier that is echoed by
    /// `CompletionQueue::poll`.
    ///
    /// Note that the device may choose to allocate more CQ entries than the provided minimum.
    pub fn create_cq(&self, min_cq_entries: i32, id: isize) -> Result<CompletionQueue, ()> {
        let cq = unsafe {
            ffi::ibv_create_cq(self.ctx,
                               min_cq_entries,
                               ptr::null::<c_void>().offset(id) as *mut _,
                               ptr::null::<c_void>() as *mut _,
                               0)
        };

        if cq.is_null() {
            Err(())
        } else {
            Ok(CompletionQueue {
                   _phantom: PhantomData,
                   cq: cq,
               })
        }
    }

    /// Allocate a protection domain (PDs) for the device's context.
    pub fn alloc_pd(&self) -> Result<ProtectionDomain, ()> {
        let pd = unsafe { ffi::ibv_alloc_pd(self.ctx) };
        if pd.is_null() {
            Err(())
        } else {
            Ok(ProtectionDomain { ctx: self, pd })
        }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        let ok = unsafe { ffi::ibv_close_device(self.ctx) };
        assert_eq!(ok, 0);
    }
}

/// A completion queue that allows subscribing to the completion of queued sends and receives.
pub struct CompletionQueue<'a> {
    _phantom: PhantomData<&'a ()>,
    cq: *mut ffi::ibv_cq,
}

unsafe impl<'a> Send for CompletionQueue<'a> {}
unsafe impl<'a> Sync for CompletionQueue<'a> {}

impl<'a> CompletionQueue<'a> {
    /// Poll a CQ for (possibly multiple) work completions.
    ///
    /// Returns the subset of `completions` that successfully completed. If the returned slice has
    /// fewer elements than the provided `completions` slice, the CQ was emptied.
    ///
    /// Not all attributes of the completed `ibv_wc`'s are always valid. If the completion status
    /// is not `IBV_WC_SUCCESS`, only the following attributes are valid: `wr_id`, `status`,
    /// `qp_num`, and `vendor_err`.
    #[inline]
    pub fn poll<'c>(&self,
                    completions: &'c mut [ffi::ibv_wc])
                    -> Result<&'c mut [ffi::ibv_wc], ()> {
        let ctx: *mut ffi::ibv_context = unsafe { &*self.cq }.context;
        let ops = &mut unsafe { &mut *ctx }.ops;
        let n = unsafe {
            ops.poll_cq.as_mut().unwrap()(self.cq,
                                          completions.len() as i32,
                                          completions.as_mut_ptr())
        };

        if n < 0 {
            Err(())
        } else {
            Ok(&mut completions[0..n as usize])
        }
    }
}

impl<'a> Drop for CompletionQueue<'a> {
    fn drop(&mut self) {
        let errno = unsafe { ffi::ibv_destroy_cq(self.cq) };
        if errno != 0 {
            let e = std::io::Error::from_raw_os_error(errno);
            panic!("{}", e.description());
        }
    }
}

/// An unconfigured `QueuePair`.
///
/// A `QueuePairBuilder` is used to configure a `QueuePair` before it is allocated and initialized.
/// To construct one, use `ProtectionDomain::create_qp`.
pub struct QueuePairBuilder<'a> {
    ctx: isize,
    pd: &'a ProtectionDomain<'a>,

    send: &'a CompletionQueue<'a>,
    max_send_wr: u32,
    recv: &'a CompletionQueue<'a>,
    max_recv_wr: u32,

    max_send_sge: u32,
    max_recv_sge: u32,
    max_inline_data: u32,

    qp_type: ffi::ibv_qp_type,

    // carried along to handshake phase
    access: ffi::ibv_access_flags,
    timeout: u8,
    retry_count: u8,
    rnr_retry: u8,
    min_rnr_timer: u8,
}

impl<'qp> QueuePairBuilder<'qp> {
    fn new<'scq, 'rcq, 'pd>(pd: &'pd ProtectionDomain,
                            send: &'scq CompletionQueue,
                            max_send_wr: u32,
                            recv: &'rcq CompletionQueue,
                            max_recv_wr: u32,
                            qp_type: ffi::ibv_qp_type)
                            -> QueuePairBuilder<'qp>
        where 'scq: 'qp,
              'rcq: 'qp,
              'pd: 'qp
    {
        QueuePairBuilder {
            ctx: 0,
            pd: pd,

            send,
            max_send_wr,
            recv,
            max_recv_wr,

            max_send_sge: 1,
            max_recv_sge: 1,
            max_inline_data: 0,

            qp_type,

            access: ffi::IBV_ACCESS_LOCAL_WRITE,
            min_rnr_timer: 16,
            retry_count: 6,
            rnr_retry: 6,
            timeout: 4,
        }
    }

    /// Set the access flags for the new `QueuePair`.
    pub fn set_access(&mut self, access: ffi::ibv_access_flags) -> &mut Self {
        self.access = access;
        self
    }

    /// Set the access flags of the new `QueuePair` such that it allows remote reads and writes.
    pub fn allow_remote_rw(&mut self) -> &mut Self {
        self.access = self.access | ffi::IBV_ACCESS_REMOTE_WRITE | ffi::IBV_ACCESS_REMOTE_READ;
        self
    }

    /// Set the minimum RNR timer value for the new `QueuePair`.
    pub fn set_min_rnr_timer(&mut self, timer: u8) -> &mut Self {
        self.min_rnr_timer = timer;
        self
    }

    /// Set the timeout for the new `QueuePair`.
    pub fn set_timeout(&mut self, timeout: u8) -> &mut Self {
        self.timeout = timeout;
        self
    }

    /// Set the retry count for the new `QueuePair`.
    ///
    /// # Panics
    ///
    /// Panics if a count higher than 7 is given.
    pub fn set_retry_count(&mut self, count: u8) -> &mut Self {
        assert!(count <= 7);
        self.retry_count = count;
        self
    }

    /// Set the RNR retry limit for the new `QueuePair`.
    ///
    /// # Panics
    ///
    /// Panics if a limit higher than 7 is given.
    pub fn set_rnr_retry(&mut self, n: u8) -> &mut Self {
        assert!(n <= 7);
        self.rnr_retry = n;
        self
    }

    /// Set the opaque context value for the new `QueuePair`.
    pub fn set_context(&mut self, ctx: isize) -> &mut Self {
        self.ctx = ctx;
        self
    }

    /// Create a new `QueuePair` from this builder template.
    ///
    /// The returned `QueuePair` is associated with the builder's `ProtectionDomain`.
    ///
    /// This method will fail if asked to create QP of a type other than `IBV_QPT_RC` or
    /// `IBV_QPT_UD` associated with an SRQ.
    pub fn build(&self) -> Result<PreparedQueuePair<'qp>, ()> {
        let mut attr = ffi::ibv_qp_init_attr {
            qp_context: unsafe { ptr::null::<c_void>().offset(self.ctx) } as *mut _,
            send_cq: unsafe { mem::transmute(self.send) },
            recv_cq: unsafe { mem::transmute(self.recv) },
            srq: ptr::null::<ffi::ibv_srq>() as *mut _,
            cap: ffi::ibv_qp_cap {
                max_send_wr: self.max_send_wr,
                max_recv_wr: self.max_recv_wr,
                max_send_sge: self.max_send_sge,
                max_recv_sge: self.max_recv_sge,
                max_inline_data: self.max_inline_data,
            },
            qp_type: self.qp_type,
            sq_sig_all: 0,
        };

        let qp = unsafe { ffi::ibv_create_qp(self.pd.pd, mem::transmute(&mut attr)) };
        if qp.is_null() {
            Err(())
        } else {
            Ok(PreparedQueuePair {
                   ctx: self.pd.ctx,
                   qp: qp,

                   access: self.access,
                   timeout: self.timeout,
                   retry_count: self.retry_count,
                   rnr_retry: self.rnr_retry,
                   min_rnr_timer: self.min_rnr_timer,
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
pub struct PreparedQueuePair<'a> {
    ctx: &'a Context,
    qp: *mut ffi::ibv_qp,

    // carried from builder
    access: ffi::ibv_access_flags,
    min_rnr_timer: u8,
    timeout: u8,
    retry_count: u8,
    rnr_retry: u8,
}

/// An identifier for the network endpoint of a `QueuePair`.
///
/// Internally, this contains the `QueuePair`'s `qp_num`, as well as the context's `lid` and `gid`.
#[derive(Copy, Clone)]
pub struct QueuePairEndpoint {
    num: u32,
    lid: u16,
    gid: ffi::ibv_gid,
}

impl<'a> PreparedQueuePair<'a> {
    /// Get the network endpoint for this `QueuePair`.
    ///
    /// This endpoint will need to be communicated to the `QueuePair` on the remote end.
    pub fn endpoint(&self) -> QueuePairEndpoint {
        let num = unsafe { &*self.qp }.qp_num;

        QueuePairEndpoint {
            num,
            lid: self.ctx.port_attr.lid,
            gid: self.ctx.gid,
        }
    }

    /// Set up the `QueuePair` such that it is ready to exchange packets with a remote `QueuePair`.
    ///
    /// Internally, this uses `ibv_modify_qp` to mark the `QueuePair` as initialized
    /// (`IBV_QPS_INIT`), ready to receive (`IBV_QPS_RTR`), and ready to send (`IBV_QPS_RTS`).
    /// It also sets the following parameters, which are currently not configurable:
    ///
    /// ```text,ignore
    /// port_num = PORT_NUM;
    /// pkey_index = 0;
    /// rq_psn = 0;
    /// sq_psn = 0;
    ///
    /// max_dest_rd_atomic = 1;
    /// max_rd_atomic = 1;
    ///
    /// ah_attr.sl = 0;
    /// ah_attr.is_global = 1;
    /// ah_attr.src_path_bits = 0;
    /// ah_attr.grh.hop_limit = 0xff;
    /// ```
    pub fn handshake(self, remote: QueuePairEndpoint) -> io::Result<QueuePair<'a>> {
        // init and associate with port
        let mut attr = ffi::ibv_qp_attr::default();
        attr.qp_state = ffi::ibv_qp_state::IBV_QPS_INIT;
        attr.qp_access_flags = self.access.0 as c_int;
        attr.pkey_index = 0;
        attr.port_num = PORT_NUM;
        let mask = ffi::IBV_QP_STATE | ffi::IBV_QP_PKEY_INDEX | ffi::IBV_QP_PORT |
                   ffi::IBV_QP_ACCESS_FLAGS;
        let errno =
            unsafe { ffi::ibv_modify_qp(self.qp, mem::transmute(&mut attr), mask.0 as i32) };
        if errno != 0 {
            return Err(std::io::Error::from_raw_os_error(errno));
        }

        // set ready to receive
        let mut attr = ffi::ibv_qp_attr::default();
        attr.qp_state = ffi::ibv_qp_state::IBV_QPS_RTR;
        attr.path_mtu = self.ctx.port_attr.active_mtu;
        attr.dest_qp_num = remote.num;
        attr.rq_psn = 0;
        attr.max_dest_rd_atomic = 1;
        attr.min_rnr_timer = self.min_rnr_timer;
        attr.ah_attr.is_global = 1;
        attr.ah_attr.dlid = remote.lid;
        attr.ah_attr.sl = 0;
        attr.ah_attr.src_path_bits = 0;
        attr.ah_attr.port_num = PORT_NUM;
        attr.ah_attr.grh.dgid = remote.gid;
        attr.ah_attr.grh.hop_limit = 0xff;
        let mask = ffi::IBV_QP_STATE | ffi::IBV_QP_AV | ffi::IBV_QP_PATH_MTU |
                   ffi::IBV_QP_DEST_QPN | ffi::IBV_QP_RQ_PSN |
                   ffi::IBV_QP_MAX_DEST_RD_ATOMIC | ffi::IBV_QP_MIN_RNR_TIMER;
        let errno =
            unsafe { ffi::ibv_modify_qp(self.qp, mem::transmute(&mut attr), mask.0 as i32) };
        if errno != 0 {
            return Err(std::io::Error::from_raw_os_error(errno));
        }

        // set ready to send
        let mut attr = ffi::ibv_qp_attr::default();
        attr.qp_state = ffi::ibv_qp_state::IBV_QPS_RTS;
        attr.timeout = self.timeout;
        attr.retry_cnt = self.retry_count;
        attr.sq_psn = 0;
        attr.rnr_retry = self.rnr_retry;
        attr.max_rd_atomic = 1;
        let mask = ffi::IBV_QP_STATE | ffi::IBV_QP_TIMEOUT | ffi::IBV_QP_RETRY_CNT |
                   ffi::IBV_QP_SQ_PSN | ffi::IBV_QP_RNR_RETRY |
                   ffi::IBV_QP_MAX_QP_RD_ATOMIC;
        let errno =
            unsafe { ffi::ibv_modify_qp(self.qp, mem::transmute(&mut attr), mask.0 as i32) };
        if errno != 0 {
            return Err(std::io::Error::from_raw_os_error(errno));
        }

        Ok(QueuePair {
               _phantom: PhantomData,
               qp: self.qp,
           })
    }
}

/// A memory region that has been registered for use with RDMA.
pub struct MemoryRegion<T> {
    mr: *mut ffi::ibv_mr,
    data: Vec<T>,
}

unsafe impl<T> Send for MemoryRegion<T> {}
unsafe impl<T> Sync for MemoryRegion<T> {}

use std::ops::{Deref, DerefMut};
impl<T> Deref for MemoryRegion<T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        &self.data[..]
    }
}

impl<T> DerefMut for MemoryRegion<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data[..]
    }
}

impl<T> MemoryRegion<T> {
    /// Get the remote authentication key used to allow direct remote access to this memory region.
    pub fn rkey(&self) -> RemoteKey {
        RemoteKey(unsafe { &*self.mr }.rkey)
    }
}

/// A key that authorizes direct memory access to a memory region.
pub struct RemoteKey(u32);

impl<T> Drop for MemoryRegion<T> {
    fn drop(&mut self) {
        let errno = unsafe { ffi::ibv_dereg_mr(self.mr) };
        if errno != 0 {
            let e = std::io::Error::from_raw_os_error(errno);
            panic!("{}", e.description());
        }
    }
}

/// A protection domain for a device's context.
pub struct ProtectionDomain<'a> {
    ctx: &'a Context,
    pd: *mut ffi::ibv_pd,
}

unsafe impl<'a> Sync for ProtectionDomain<'a> {}
unsafe impl<'a> Send for ProtectionDomain<'a> {}

impl<'a> ProtectionDomain<'a> {
    /// Creates a queue pair builder associated with this protection domain.
    pub fn create_qp<'pd, 'scq, 'rcq, 'qp>(&'pd self,
                                           send: &'scq CompletionQueue,
                                           recv: &'rcq CompletionQueue,
                                           qp_type: ffi::ibv_qp_type)
                                           -> QueuePairBuilder<'qp>
        where 'scq: 'qp,
              'rcq: 'qp,
              'pd: 'qp
    {
        QueuePairBuilder::new(self, send, 1, recv, 1, qp_type)
    }

    /// Register a memory region (MR) associated with this protection domain.
    ///
    /// Only registered memory can be sent from and received to by `QueuePair`s.
    ///
    /// `allocate` currently sets the following permissions for each new `MemoryRegion`:
    ///
    ///  - `IBV_ACCESS_LOCAL_WRITE`: Enables Local Write Access
    ///  - `IBV_ACCESS_REMOTE_WRITE`: Enables Remote Write Access
    ///  - `IBV_ACCESS_REMOTE_READ`: Enables Remote Read Access
    ///  - `IBV_ACCESS_REMOTE_ATOMIC`: Enables Remote Atomic Operation Access (if supported)
    ///
    /// Local read access is always enabled for the MR.
    ///
    /// # Panics
    ///
    /// Panics if the size of the memory region zero bytes, which can occur either if `n` is 0, or
    /// if `mem::size_of::<T>()` is 0.
    ///
    pub fn allocate<T: Sized + Copy + Default>(&self, n: usize) -> Result<MemoryRegion<T>, ()> {
        assert!(n > 0);
        assert!(mem::size_of::<T>() > 0);

        let mut data = Vec::with_capacity(n);
        data.resize(n, T::default());

        let access = ffi::IBV_ACCESS_LOCAL_WRITE | ffi::IBV_ACCESS_REMOTE_WRITE |
                     ffi::IBV_ACCESS_REMOTE_READ |
                     ffi::IBV_ACCESS_REMOTE_ATOMIC;
        let mr = unsafe {
            ffi::ibv_reg_mr(self.pd,
                            data.as_mut_ptr() as *mut _,
                            n * mem::size_of::<T>(),
                            access.0 as i32)
        };

        // TODO
        // ibv_reg_mr()  returns  a  pointer to the registered MR, or NULL if the request fails.
        // The local key (L_Key) field lkey is used as the lkey field of struct ibv_sge when
        // posting buffers with ibv_post_* verbs, and the the remote key (R_Key)  field rkey  is
        // used by remote processes to perform Atomic and RDMA operations.  The remote process
        // places this rkey as the rkey field of struct ibv_send_wr passed to the ibv_post_send
        // function.

        if !mr.is_null() {
            Err(())
        } else {
            Ok(MemoryRegion { mr, data })
        }
    }
}

impl<'a> Drop for ProtectionDomain<'a> {
    fn drop(&mut self) {
        let errno = unsafe { ffi::ibv_dealloc_pd(self.pd) };
        if errno != 0 {
            let e = std::io::Error::from_raw_os_error(errno);
            panic!("{}", e.description());
        }
    }
}

/// A fully initialized and ready `QueuePair`.
pub struct QueuePair<'a> {
    _phantom: PhantomData<&'a ()>,
    qp: *mut ffi::ibv_qp,
}

unsafe impl<'a> Send for QueuePair<'a> {}
unsafe impl<'a> Sync for QueuePair<'a> {}

impl<'a> QueuePair<'a> {
    /// Post a list of work requests to a send queue.
    ///
    /// Specifically, the memory at `mr[range]` will be sent as a single `ibv_send_wr` using
    /// `IBV_WR_SEND`. The send has `IBV_SEND_SIGNALED` set, so a work completion will also be
    /// triggered as a result of this send.
    ///
    /// # Safety
    ///
    /// The memory region can only be safely reused or dropped after the request is fully executed
    /// and a work completion has been retrieved from the corresponding completion queue (i.e.,
    /// until `CompletionQueue::poll` returns a completion for this send).
    #[inline]
    pub unsafe fn post_send<T, R>(&mut self,
                                  mr: &mut MemoryRegion<T>,
                                  range: R,
                                  wr_id: u64)
                                  -> io::Result<()>
        where R: std::slice::SliceIndex<[T], Output = [T]>
    {
        let range = &mr[range];
        let mut sge = ffi::ibv_sge {
            addr: range.as_ptr() as u64,
            length: (mem::size_of::<T>() * range.len()) as u32,
            lkey: (&*mr.mr).lkey,
        };
        let mut wr = ffi::ibv_send_wr {
            wr_id: wr_id,
            next: ptr::null::<ffi::ibv_send_wr>() as *mut _,
            sg_list: mem::transmute(&mut sge),
            num_sge: 1,
            opcode: ffi::ibv_wr_opcode::IBV_WR_SEND,
            send_flags: ffi::IBV_SEND_SIGNALED.0 as i32,
            imm_data: 0,
            wr: Default::default(),
            qp_type: Default::default(),
            __bindgen_anon_1: Default::default(),
        };
        let mut bad_wr: *mut ffi::ibv_send_wr = ptr::null::<ffi::ibv_send_wr>() as *mut _;

        // TODO:
        //
        // ibv_post_send()  posts the linked list of work requests (WRs) starting with wr to the
        // send queue of the queue pair qp.  It stops processing WRs from this list at the first
        // failure (that can  be  detected  immediately  while  requests  are  being posted), and
        // returns this failing WR through bad_wr.
        //
        // The user should not alter or destroy AHs associated with WRs until request is fully
        // executed and  a  work  completion  has been retrieved from the corresponding completion
        // queue (CQ) to avoid unexpected behavior.
        //
        // ... However, if the IBV_SEND_INLINE flag was set, the  buffer  can  be reused
        // immediately after the call returns.

        let ctx = (&*self.qp).context;
        let ops = &mut (&mut *ctx).ops;
        let errno = ops.post_send.as_mut().unwrap()(self.qp,
                                                    mem::transmute(&mut wr),
                                                    mem::transmute(&mut bad_wr));
        if errno != 0 {
            Err(std::io::Error::from_raw_os_error(errno))
        } else {
            Ok(())
        }
    }

    /// Post a list of work requests to a receive queue.
    ///
    /// Specifically, the memory at `mr[range]` will be received into as a single `ibv_recv_wr`.
    ///
    /// # Safety
    ///
    /// The memory region can only be safely reused or dropped after the request is fully executed
    /// and a work completion has been retrieved from the corresponding completion queue (i.e.,
    /// until `CompletionQueue::poll` returns a completion for this receive).
    #[inline]
    pub unsafe fn post_receive<T, R>(&mut self,
                                     mr: &mut MemoryRegion<T>,
                                     range: R,
                                     wr_id: u64)
                                     -> io::Result<()>
        where R: std::slice::SliceIndex<[T], Output = [T]>
    {
        let range = &mr[range];
        let mut sge = ffi::ibv_sge {
            addr: range.as_ptr() as u64,
            length: (mem::size_of::<T>() * range.len()) as u32,
            lkey: (&*mr.mr).lkey,
        };
        let mut wr = ffi::ibv_recv_wr {
            wr_id: wr_id,
            next: ptr::null::<ffi::ibv_send_wr>() as *mut _,
            sg_list: mem::transmute(&mut sge),
            num_sge: 1,
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

        let ctx = (&*self.qp).context;
        let ops = &mut (&mut *ctx).ops;
        let errno = ops.post_recv.as_mut().unwrap()(self.qp,
                                                    mem::transmute(&mut wr),
                                                    mem::transmute(&mut bad_wr));
        if errno != 0 {
            Err(std::io::Error::from_raw_os_error(errno))
        } else {
            Ok(())
        }
    }
}

impl<'a> Drop for QueuePair<'a> {
    fn drop(&mut self) {
        // TODO: ibv_destroy_qp() fails if the QP is attached to a multicast group.
        let errno = unsafe { ffi::ibv_destroy_qp(self.qp) };
        if errno != 0 {
            let e = std::io::Error::from_raw_os_error(errno);
            panic!("{}", e.description());
        }
    }
}
