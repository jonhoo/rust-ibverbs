use std::borrow::Cow;
use std::ffi::CStr;
use std::fmt;
use std::io;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use crate::completion::{CompletionChannel, CompletionQueueBuilder};
use crate::device::Guid;
use crate::error::{Error, Result};
use crate::gid::{Gid, GidEntry};
use crate::pd::{ProtectionDomain, ProtectionDomainInner};
use crate::PORT_NUM;

#[cfg(doc)]
use crate::{ibv_device_attr, ibv_port_attr, QueuePairBuilder, WorkCompletion};

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
    pub(crate) fn with_device(dev: *mut ffi::ibv_device) -> Result<Context> {
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

    /// Allocate a protection domain (PD) for the device's context.
    ///
    /// The created PD will be used primarily to create `QueuePair`s and `MemoryRegion`s.
    ///
    /// A protection domain is a means of protection, and helps you create a group of objects that
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
