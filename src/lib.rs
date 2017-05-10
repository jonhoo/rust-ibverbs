//#![deny(missing_docs)]
#![feature(slice_get_slice)]

use std::marker::PhantomData;
use std::ptr;
use std::mem;
use std::os::raw::c_void;

const PORT_NUM: u8 = 0;

#[allow(non_upper_case_globals)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
mod ffi;
pub use ffi::ibv_wc;
pub use ffi::ibv_qp_type;
pub use ffi::ibv_wc_status;
pub use ffi::ibv_wc_opcode;
pub use ffi::ibv_wc__bindgen_ty_1;

pub fn devices() -> DeviceList {
    let mut n = 0i32;
    let devices = unsafe { ffi::ibv_get_device_list(mem::transmute(&mut n)) };
    assert!(n >= 0);
    assert!(!devices.is_null());

    let devices = unsafe {
        use std::slice;
        slice::from_raw_parts_mut(devices, n as usize)
    };
    DeviceList(devices)
}

pub struct DeviceList(&'static mut [*mut ffi::ibv_device]);

impl Drop for DeviceList {
    fn drop(&mut self) {
        unsafe { ffi::ibv_free_device_list(self.0.as_mut_ptr()) };
    }
}

impl DeviceList {
    pub fn iter(&self) -> DeviceListIter {
        DeviceListIter { list: self, i: 0 }
    }
}

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

pub struct Device<'a>(&'a *mut ffi::ibv_device);
impl<'a> From<&'a *mut ffi::ibv_device> for Device<'a> {
    fn from(d: &'a *mut ffi::ibv_device) -> Self {
        Device(d)
    }
}

impl<'a> Device<'a> {
    pub fn open(&self) -> Context {
        Context::with_device(*self.0)
    }
}

pub struct Context {
    ctx: *mut ffi::ibv_context,
    port_attr: ffi::ibv_port_attr,
    gid: ffi::ibv_gid,
}

unsafe impl Sync for Context {}
unsafe impl Send for Context {}

impl Context {
    fn with_device(dev: *mut ffi::ibv_device) -> Context {
        assert!(!dev.is_null());
        let ctx = unsafe { ffi::ibv_open_device(dev) };
        assert!(!ctx.is_null());

        let mut port_attr = ffi::ibv_port_attr::default();
        let ok = unsafe { ffi::ibv_query_port(ctx, PORT_NUM, mem::transmute(&mut port_attr)) };
        assert_eq!(ok, 0);

        let mut gid = ffi::ibv_gid::default();
        let ok = unsafe { ffi::ibv_query_gid(ctx, PORT_NUM, 0, mem::transmute(&mut gid)) };
        assert_eq!(ok, 0);

        Context {
            ctx,
            port_attr,
            gid,
        }
    }

    /// Create a completion queue.
    ///
    /// `cqe` is the minimum number of entries required for CQ.
    /// `cqctx` is an opaque identifier returned by `CompletionQueue::poll`.
    pub fn create_cq(&self, cqe: i32, cqctx: isize) -> CompletionQueue {
        let cq = unsafe {
            ffi::ibv_create_cq(self.ctx,
                               cqe,
                               ptr::null::<c_void>().offset(cqctx) as *mut _,
                               ptr::null::<c_void>() as *mut _,
                               0)
        };
        assert!(!cq.is_null());
        CompletionQueue {
            _phantom: PhantomData,
            cq: cq,
        }
    }

    pub fn alloc_pd(&self) -> ProtectionDomain {
        let pd = unsafe { ffi::ibv_alloc_pd(self.ctx) };
        assert!(!pd.is_null());
        ProtectionDomain { ctx: self, pd }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        let ok = unsafe { ffi::ibv_close_device(self.ctx) };
        assert_eq!(ok, 0);
    }
}

pub struct CompletionQueue<'a> {
    _phantom: PhantomData<&'a ()>,
    cq: *mut ffi::ibv_cq,
}

unsafe impl<'a> Send for CompletionQueue<'a> {}
unsafe impl<'a> Sync for CompletionQueue<'a> {}

impl<'a> CompletionQueue<'a> {
    /// Poll a CQ for (possibly multiple) work completions.
    ///
    /// If the return value is non-negative and strictly less
    /// than num_entries, then the CQ was emptied.
    #[inline]
    pub fn poll(&self, completions: &mut [ffi::ibv_wc]) -> usize {
        let ctx: *mut ffi::ibv_context = unsafe { &*self.cq }.context;
        let ops = &mut unsafe { &mut *ctx }.ops;
        let n = unsafe {
            ops.poll_cq.as_mut().unwrap()(self.cq,
                                          completions.len() as i32,
                                          completions.as_mut_ptr())
        };
        assert!(n >= 0);
        n as usize
    }
}

impl<'a> Drop for CompletionQueue<'a> {
    fn drop(&mut self) {
        let ok = unsafe { ffi::ibv_destroy_cq(self.cq) };
        assert_eq!(ok, 0);
    }
}

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
    access: i32,
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

            access: ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE as i32,
            min_rnr_timer: 16,
            retry_count: 6,
            rnr_retry: 6,
            timeout: 4,
        }
    }

    pub fn set_access(mut self, access: i32) -> Self {
        self.access = access;
        self
    }

    pub fn allow_remote_rw(mut self) -> Self {
        self.access |= ffi::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE as i32 |
                       ffi::ibv_access_flags::IBV_ACCESS_REMOTE_READ as i32;
        self
    }

    pub fn min_rnr_timer(mut self, timer: u8) -> Self {
        self.min_rnr_timer = timer;
        self
    }

    pub fn timeout(mut self, timeout: u8) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn retry_count(mut self, count: u8) -> Self {
        assert!(count <= 7);
        self.retry_count = count;
        self
    }

    pub fn rnr_retry(mut self, n: u8) -> Self {
        assert!(n <= 7);
        self.rnr_retry = n;
        self
    }

    pub fn context(mut self, ctx: isize) -> Self {
        self.ctx = ctx;
        self
    }

    pub fn build(self) -> PreparedQueuePair<'qp> {
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
        assert!(!qp.is_null());

        PreparedQueuePair {
            ctx: self.pd.ctx,
            qp: qp,

            access: self.access,
            timeout: self.timeout,
            retry_count: self.retry_count,
            rnr_retry: self.rnr_retry,
            min_rnr_timer: self.min_rnr_timer,
        }
    }
}

pub struct PreparedQueuePair<'a> {
    ctx: &'a Context,
    qp: *mut ffi::ibv_qp,

    // carried from builder
    access: i32,
    min_rnr_timer: u8,
    timeout: u8,
    retry_count: u8,
    rnr_retry: u8,
}

pub struct QueuePairEndpoint {
    num: u32,
    lid: u16,
    gid: ffi::ibv_gid,
}

impl<'a> PreparedQueuePair<'a> {
    pub fn endpoint(&self) -> QueuePairEndpoint {
        let num = unsafe { &*self.qp }.qp_num;

        QueuePairEndpoint {
            num,
            lid: self.ctx.port_attr.lid,
            gid: self.ctx.gid,
        }
    }

    pub fn handshake(self, remote: QueuePairEndpoint) -> QueuePair<'a> {
        use ffi::ibv_qp_attr_mask::*;

        // init and associate with port
        let mut attr = ffi::ibv_qp_attr::default();
        attr.qp_state = ffi::ibv_qp_state::IBV_QPS_INIT;
        attr.qp_access_flags = self.access;
        attr.pkey_index = 0;
        attr.port_num = PORT_NUM;
        let mask = IBV_QP_STATE as i32 | IBV_QP_PKEY_INDEX as i32 | IBV_QP_PORT as i32 |
                   IBV_QP_ACCESS_FLAGS as i32;
        let ok = unsafe { ffi::ibv_modify_qp(self.qp, mem::transmute(&mut attr), mask) };
        assert_eq!(ok, 0);

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
        let mask = IBV_QP_STATE as i32 | IBV_QP_AV as i32 | IBV_QP_PATH_MTU as i32 |
                   IBV_QP_DEST_QPN as i32 | IBV_QP_RQ_PSN as i32 |
                   IBV_QP_MAX_DEST_RD_ATOMIC as i32 |
                   IBV_QP_MIN_RNR_TIMER as i32;
        let ok = unsafe { ffi::ibv_modify_qp(self.qp, mem::transmute(&mut attr), mask) };
        assert_eq!(ok, 0);

        // set ready to send
        let mut attr = ffi::ibv_qp_attr::default();
        attr.qp_state = ffi::ibv_qp_state::IBV_QPS_RTS;
        attr.timeout = self.timeout;
        attr.retry_cnt = self.retry_count;
        attr.rnr_retry = self.rnr_retry;
        attr.max_rd_atomic = 1;
        let mask = IBV_QP_STATE as i32 | IBV_QP_TIMEOUT as i32 | IBV_QP_RETRY_CNT as i32 |
                   IBV_QP_SQ_PSN as i32 | IBV_QP_MAX_QP_RD_ATOMIC as i32;
        let ok = unsafe { ffi::ibv_modify_qp(self.qp, mem::transmute(&mut attr), mask) };
        assert_eq!(ok, 0);

        QueuePair {
            _phantom: PhantomData,
            qp: self.qp,
        }
    }
}

pub struct ProtectionDomain<'a> {
    ctx: &'a Context,
    pd: *mut ffi::ibv_pd,
}

unsafe impl<'a> Sync for ProtectionDomain<'a> {}
unsafe impl<'a> Send for ProtectionDomain<'a> {}

impl<'a> ProtectionDomain<'a> {
    pub fn create_qp<'pd, 'scq, 'rcq, 'qp>(&'pd self,
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
        QueuePairBuilder::new(self, send, max_send_wr, recv, max_recv_wr, qp_type)
    }

    pub fn allocate<T: Sized + Copy + Default>(&self, n: usize) -> MemoryRegion<T> {
        assert!(n > 0);
        assert!(mem::size_of::<T>() > 0);

        let mut data = Vec::with_capacity(n);
        data.resize(n, T::default());

        use ffi::ibv_access_flags::*;
        let access = IBV_ACCESS_LOCAL_WRITE as i32 | IBV_ACCESS_REMOTE_WRITE as i32 |
                     IBV_ACCESS_REMOTE_READ as i32 |
                     IBV_ACCESS_REMOTE_ATOMIC as i32;
        let mr = unsafe {
            ffi::ibv_reg_mr(self.pd,
                            data.as_mut_ptr() as *mut _,
                            n * mem::size_of::<T>(),
                            access)
        };
        assert!(!mr.is_null());

        MemoryRegion { mr, data }
    }
}

impl<'a> Drop for ProtectionDomain<'a> {
    fn drop(&mut self) {
        let ok = unsafe { ffi::ibv_dealloc_pd(self.pd) };
        assert_eq!(ok, 0);
    }
}

pub struct MemoryRegion<T> {
    mr: *mut ffi::ibv_mr,
    data: Vec<T>,
}

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
    pub fn rkey(&self) -> RemoteKey {
        RemoteKey(unsafe { &*self.mr }.rkey)
    }
}

pub struct RemoteKey(u32);

impl<T> Drop for MemoryRegion<T> {
    fn drop(&mut self) {
        let ok = unsafe { ffi::ibv_dereg_mr(self.mr) };
        assert_eq!(ok, 0);
    }
}

pub struct QueuePair<'a> {
    _phantom: PhantomData<&'a ()>,
    qp: *mut ffi::ibv_qp,
}

impl<'a> QueuePair<'a> {
    /// Post a list of work requests to a send queue.
    ///
    /// If `IBV_SEND_INLINE` flag is set, the data buffers can be reused
    /// immediately after the call returns.
    #[inline]
    pub fn post_send<T, R>(&mut self, mr: &mut MemoryRegion<T>, range: R, wr_id: u64)
        where R: std::slice::SliceIndex<[T], Output = [T]>
    {
        let range = &mr[range];
        let mut sge = ffi::ibv_sge {
            addr: range.as_ptr() as u64,
            length: (mem::size_of::<T>() * range.len()) as u32,
            lkey: unsafe { &*mr.mr }.lkey,
        };
        let mut wr = ffi::ibv_send_wr {
            wr_id: wr_id,
            next: ptr::null::<ffi::ibv_send_wr>() as *mut _,
            sg_list: unsafe { mem::transmute(&mut sge) },
            num_sge: 1,
            opcode: ffi::ibv_wr_opcode::IBV_WR_SEND,
            send_flags: 0,
            imm_data: 0,
            wr: Default::default(),
            qp_type: Default::default(),
            __bindgen_anon_1: Default::default(),
        };
        let mut bad_wr: *mut ffi::ibv_send_wr = ptr::null::<ffi::ibv_send_wr>() as *mut _;

        let ctx = unsafe { &*self.qp }.context;
        let ops = &mut unsafe { &mut *ctx }.ops;
        let n = unsafe {
            ops.post_send.as_mut().unwrap()(self.qp,
                                            mem::transmute(&mut wr),
                                            mem::transmute(&mut bad_wr))
        };
        assert!(n >= 0);
    }

    ///
    /// Post a list of work requests to a receive queue.
    ///
    #[inline]
    pub fn post_receive<T, R>(&mut self, mr: &mut MemoryRegion<T>, range: R, wr_id: u64)
        where R: std::slice::SliceIndex<[T], Output = [T]>
    {
        let range = &mr[range];
        let mut sge = ffi::ibv_sge {
            addr: range.as_ptr() as u64,
            length: (mem::size_of::<T>() * range.len()) as u32,
            lkey: unsafe { &*mr.mr }.lkey,
        };
        let mut wr = ffi::ibv_recv_wr {
            wr_id: wr_id,
            next: ptr::null::<ffi::ibv_send_wr>() as *mut _,
            sg_list: unsafe { mem::transmute(&mut sge) },
            num_sge: 1,
        };
        let mut bad_wr: *mut ffi::ibv_recv_wr = ptr::null::<ffi::ibv_recv_wr>() as *mut _;

        let ctx = unsafe { &*self.qp }.context;
        let ops = &mut unsafe { &mut *ctx }.ops;
        let n = unsafe {
            ops.post_recv.as_mut().unwrap()(self.qp,
                                            mem::transmute(&mut wr),
                                            mem::transmute(&mut bad_wr))
        };
        assert!(n >= 0);
    }
}

impl<'a> Drop for QueuePair<'a> {
    fn drop(&mut self) {
        let ok = unsafe { ffi::ibv_destroy_qp(self.qp) };
        assert_eq!(ok, 0);
    }
}
