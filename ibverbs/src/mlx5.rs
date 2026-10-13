use std::ffi::{CStr, OsStr};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;

use crate::mr::MemoryRegionInner;
use crate::{AccessFlags, Context, Error, MemoryRegion, ProtectionDomain, Result};

flags_newtype! {
    /// mlx5-specific DMA-BUF registration flags, separate from [`AccessFlags`].
    pub struct Mlx5DmabufAccessFlags(ffi::mlx5_ib_uapi_reg_dmabuf_flags) {
        /// Request Data Direct access. Registration fails if the device or exporter cannot
        /// support it; it is not retried as ordinary DMA-BUF registration.
        DATA_DIRECT = MLX5_IB_UAPI_REG_DMABUF_ACCESS_DATA_DIRECT;
    }
}

impl ProtectionDomain {
    /// Register an exported DMA-BUF through mlx5 direct verbs.
    ///
    /// Like [`register_dmabuf`](Self::register_dmabuf), `offset` and `len` identify the
    /// region within `fd`, and `iova` is the address used in work requests. `iova` and
    /// `offset` must have the same page offset. The returned region retains this PD,
    /// but does not own the exporter: keep the exported allocation alive until the MR
    /// is dropped, and finish all operations using the MR before dropping it.
    ///
    /// `access_flags` grants ordinary verbs access; `mlx5_access_flags` selects provider
    /// behavior. [`Mlx5DmabufAccessFlags::DATA_DIRECT`] requires a supported device and
    /// a compatible DMA-BUF exporter (for example, a PCIe-mapped GPU export).
    /// No fallback to ordinary registration occurs on failure.
    ///
    /// Requires the `mlx5` feature, rdma-core 54 or newer, and (for the mlx5 access flags)
    /// Linux 6.12 or newer. Unsupported providers and older kernels return
    /// [`Error::Unsupported`]; other registration errors retain their OS error.
    pub fn register_dmabuf_mlx5(
        &self,
        fd: i32,
        offset: u64,
        len: usize,
        iova: u64,
        access_flags: AccessFlags,
        mlx5_access_flags: Mlx5DmabufAccessFlags,
    ) -> Result<MemoryRegion<()>> {
        // SAFETY: the PD is live; the provider validates the DMA-BUF descriptor and ranges.
        let mr = unsafe {
            ffi::mlx5dv_reg_dmabuf_mr(
                self.as_raw(),
                offset,
                len,
                iova,
                fd,
                access_flags.0 as i32,
                mlx5_access_flags.0 as i32,
            )
        };
        if mr.is_null() {
            return Err(registration_error(io::Error::last_os_error()));
        }
        Ok(MemoryRegion {
            inner: MemoryRegionInner {
                _pd: self.inner.clone(),
                mr,
                addr: iova,
            },
            owner: (),
        })
    }
}

impl Context {
    /// Query the associated mlx5 Data Direct device's sysfs path.
    ///
    /// Returns the provider's path unchanged, without the `/sys` mount prefix. Path bytes
    /// need not be UTF-8. The caller decides how to use the path for device selection.
    ///
    /// Requires the `mlx5` feature, rdma-core 54 or newer, and Linux 6.12 or newer. Unsupported
    /// providers and older kernels return [`Error::Unsupported`]. Other errors, including
    /// `ENODEV` (no associated Data Direct
    /// device) and `ENOSPC` (insufficient buffer), retain their OS error. Empty or
    /// unterminated success output returns an [`io::ErrorKind::InvalidData`] source.
    pub fn mlx5_data_direct_sysfs_path(&self) -> Result<PathBuf> {
        let mut buf = [0xff; nix::libc::PATH_MAX as usize];
        // SAFETY: this context is live and buf is writable for the supplied length.
        let rc = unsafe {
            ffi::mlx5dv_get_data_direct_sysfs_path(
                self.as_raw(),
                buf.as_mut_ptr().cast(),
                buf.len(),
            )
        };
        data_direct_path(rc, &buf)
    }
}

fn registration_error(err: io::Error) -> Error {
    if Error::is_unsupported(&err) {
        Error::Unsupported {
            operation: &"mlx5dv_reg_dmabuf_mr",
        }
    } else {
        Error::RegisterMemoryRegion(err)
    }
}

fn data_direct_path(rc: i32, buf: &[u8]) -> Result<PathBuf> {
    if rc != 0 {
        return Err(Error::errno(rc, Error::QueryMlx5DataDirectSysfsPath));
    }
    let path = CStr::from_bytes_until_nul(buf).map_err(|_| {
        Error::QueryMlx5DataDirectSysfsPath(io::Error::new(
            io::ErrorKind::InvalidData,
            "mlx5dv_get_data_direct_sysfs_path returned a path without a NUL terminator",
        ))
    })?;
    if path.is_empty() {
        return Err(Error::QueryMlx5DataDirectSysfsPath(io::Error::new(
            io::ErrorKind::InvalidData,
            "mlx5dv_get_data_direct_sysfs_path returned an empty path",
        )));
    }
    Ok(PathBuf::from(OsStr::from_bytes(path.to_bytes())))
}

#[cfg(test)]
mod tests;
