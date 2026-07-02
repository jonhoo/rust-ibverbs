use std::io;
use std::sync::Arc;

use crate::gid::Gid;
use crate::pd::ProtectionDomainInner;
use crate::PORT_NUM;

#[cfg(doc)]
use crate::{ProtectionDomain, QueuePair};

/// Attributes describing how to reach a destination, used to build an [`AddressHandle`].
///
/// Defaults to a non-global (LID-only) route on the crate's port. For RoCE and routed InfiniBand,
/// set a global route with [`set_grh`](Self::set_grh).
#[derive(Clone)]
pub struct AddressHandleAttribute {
    pub(crate) attr: ffi::ibv_ah_attr,
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
    pub(crate) _pd: Arc<ProtectionDomainInner>,
    pub(crate) ah: *mut ffi::ibv_ah,
}

unsafe impl Send for AddressHandle {}
unsafe impl Sync for AddressHandle {}

impl AddressHandle {
    #[inline]
    pub(crate) fn as_ptr(&self) -> *mut ffi::ibv_ah {
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
