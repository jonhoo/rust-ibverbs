use std::ffi::CStr;
use std::fmt;
use std::io;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::context::Context;
use crate::error::{Error, Result};

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
    /// Upper 24 bits of the GUID are the vendor's [Organizationally Unique Identifier
    /// (OUI)](https://standards-oui.ieee.org/oui/oui.txt). The function returns the OUI as a
    /// 24-bit number inside a u32.
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
    /// Opens an RDMA device and creates a context for further use.
    ///
    /// This context will later be used to query its resources or for creating resources.
    ///
    /// Unlike what the verb name suggests, it doesn't actually open the device. This device was
    /// opened by the kernel low-level driver and may be used by other user/kernel level code. This
    /// verb only opens a context to allow user level applications to use it.
    ///
    /// # Errors
    ///
    ///  - [`OpenDevice`](Error::OpenDevice): `ibv_open_device` failed.
    ///  - [`QueryPort`](Error::QueryPort): querying port 1 failed (`ibv_query_port`).
    ///  - [`PortNotActive`](Error::PortNotActive): port 1 is not in the `ACTIVE` or `ARMED`
    ///    state. Note that this check always targets the device's first port.
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
    ///  - a *prefix* which describes the RDMA device vendor and model, or its driver
    ///    - `mlx4`/`mlx5` - NVIDIA (Mellanox) ConnectX families
    ///    - `bnxt_re` - Broadcom NetXtreme-E family
    ///    - `cxgb4` - Chelsio Communications T4/T5/T6 families
    ///    - `efa` - Amazon Elastic Fabric Adapter
    ///    - `hns` - Hisilicon Hip family
    ///    - `irdma` - Intel Ethernet Connection RDMA
    ///    - `rxe` - software RDMA over Ethernet (SoftRoCE)
    ///    - `siw` - software iWARP
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

    /// Returns the stable IB device index as it is assigned by the kernel.
    ///
    /// # Errors
    ///
    ///  - [`DeviceIndexUnavailable`](Error::DeviceIndexUnavailable): the kernel does not expose a
    ///    stable index for this device.
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

#[cfg(all(test, feature = "serde"))]
mod test_serde {
    use super::*;
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
}
