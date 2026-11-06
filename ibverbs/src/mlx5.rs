use std::ffi::OsStr;
use std::io;
use std::os::fd::{AsRawFd, BorrowedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;

use crate::{AccessFlags, Context, Error, MemoryRegion, ProtectionDomain, Result};

impl ProtectionDomain {
    /// Register an exported DMA-BUF through mlx5 Data Direct (`mlx5dv_reg_dmabuf_mr` with
    /// `MLX5DV_REG_DMABUF_ACCESS_DATA_DIRECT`): the device reaches the exporter's memory through
    /// its associated Data Direct device (see [`Context::mlx5_data_direct_sysfs_path`]).
    ///
    /// Like [`register_dmabuf`](Self::register_dmabuf), `offset` and `len` identify the
    /// region within `fd`, and `iova` is the address used in work requests. `iova` and
    /// `offset` must have the same page offset. The returned region retains this PD,
    /// but does not own the exporter: keep the exported allocation alive until the MR
    /// is dropped, and finish all operations using the MR before dropping it.
    ///
    /// Data Direct requires a supported device and a compatible DMA-BUF exporter (for example,
    /// a PCIe-mapped GPU export), a page-aligned `iova`, and no
    /// [`ON_DEMAND`](AccessFlags::ON_DEMAND) access. No fallback to ordinary registration occurs
    /// on failure.
    ///
    /// Requires the `mlx5` feature, rdma-core 54 or newer, and Linux 6.12 or newer.
    ///
    /// # Errors
    ///
    ///  - [`Unsupported`](Error::Unsupported): the provider is not mlx5, the kernel is older than
    ///    Linux 6.12, or the device has no associated Data Direct device.
    ///  - [`RegisterMemoryRegion`](Error::RegisterMemoryRegion): the registration failed, with its
    ///    OS error; [`io::ErrorKind::InvalidInput`] for an `iova` that is not page aligned or
    ///    `access_flags` that include `ON_DEMAND`.
    pub fn register_dmabuf_data_direct(
        &self,
        fd: BorrowedFd<'_>,
        offset: u64,
        len: usize,
        iova: u64,
        access_flags: AccessFlags,
    ) -> Result<MemoryRegion<()>> {
        // SAFETY: `sysconf` only reads a system constant.
        let page_size = unsafe { nix::libc::sysconf(nix::libc::_SC_PAGESIZE) } as u64;
        check_data_direct_arguments(iova, access_flags, page_size)?;
        let data_direct =
            ffi::mlx5_ib_uapi_reg_dmabuf_flags::MLX5_IB_UAPI_REG_DMABUF_ACCESS_DATA_DIRECT;
        // SAFETY: the PD is live; the provider validates the DMA-BUF descriptor and ranges.
        let mr = unsafe {
            ffi::mlx5dv_reg_dmabuf_mr(
                self.as_raw(),
                offset,
                len,
                iova,
                fd.as_raw_fd(),
                access_flags.0 as i32,
                data_direct.0 as i32,
            )
        };
        if mr.is_null() {
            let err = io::Error::last_os_error();
            // The kernel reports a device without a Data Direct device as `EINVAL`, like a bad
            // argument; its Data Direct query tells the two apart by reporting `ENODEV`.
            if err.raw_os_error() == Some(nix::libc::EINVAL)
                && !has_data_direct_device(self.context())
            {
                return Err(Error::Unsupported {
                    operation: &"mlx5dv_reg_dmabuf_mr",
                    errno: nix::libc::ENODEV,
                });
            }
            return Err(registration_error(err));
        }
        Ok(MemoryRegion::new(self.clone(), mr, iova, ()))
    }
}

impl Context {
    /// Query the associated mlx5 Data Direct device's sysfs path.
    ///
    /// Returns the provider's path unchanged, without the `/sys` mount prefix. Path bytes
    /// need not be UTF-8. The caller decides how to use the path for device selection.
    ///
    /// Requires the `mlx5` feature, rdma-core 54 or newer, and Linux 6.12 or newer.
    ///
    /// # Errors
    ///
    ///  - [`Unsupported`](Error::Unsupported): the provider is not mlx5, or the kernel is older
    ///    than Linux 6.12.
    ///  - [`QueryMlx5DataDirectSysfsPath`](Error::QueryMlx5DataDirectSysfsPath): the query failed,
    ///    with its OS error, such as `ENODEV` (no associated Data Direct device).
    pub fn mlx5_data_direct_sysfs_path(&self) -> Result<PathBuf> {
        let mut buf = [0u8; nix::libc::PATH_MAX as usize];
        // SAFETY: this context is live and buf is writable for the supplied length.
        let rc = unsafe {
            ffi::mlx5dv_get_data_direct_sysfs_path(
                self.as_raw(),
                buf.as_mut_ptr().cast(),
                buf.len(),
            )
        };
        if rc != 0 {
            return Err(Error::errno(rc, Error::QueryMlx5DataDirectSysfsPath));
        }
        // The kernel copies the path and its NUL, or fails with `ENOSPC` if they do not fit.
        let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
        Ok(PathBuf::from(OsStr::from_bytes(&buf[..len])))
    }
}

/// Refuses what the kernel refuses for a Data Direct registration with `EOPNOTSUPP`, which would
/// otherwise read as an unsupported device: the hardware maps Data Direct regions in whole pages,
/// so the IOVA must be page aligned, and it cannot fault pages in on demand (mlx5's
/// `reg_user_mr_dmabuf_by_data_direct`).
fn check_data_direct_arguments(iova: u64, access_flags: AccessFlags, page_size: u64) -> Result<()> {
    let problem = if iova % page_size != 0 {
        "a Data Direct registration needs a page-aligned iova"
    } else if access_flags.contains(AccessFlags::ON_DEMAND) {
        "a Data Direct registration cannot use on-demand paging"
    } else {
        return Ok(());
    };
    Err(Error::RegisterMemoryRegion(io::Error::new(
        io::ErrorKind::InvalidInput,
        problem,
    )))
}

/// Whether the device behind `ctx` has a Data Direct device: false only when the provider's query
/// says it has none (`ENODEV`).
fn has_data_direct_device(ctx: &Context) -> bool {
    !matches!(
        ctx.mlx5_data_direct_sysfs_path(),
        Err(Error::QueryMlx5DataDirectSysfsPath(e)) if e.raw_os_error() == Some(nix::libc::ENODEV)
    )
}

fn registration_error(err: io::Error) -> Error {
    match err.raw_os_error().filter(|_| Error::is_unsupported(&err)) {
        Some(errno) => Error::Unsupported {
            operation: &"mlx5dv_reg_dmabuf_mr",
            errno,
        },
        None => Error::RegisterMemoryRegion(err),
    }
}

#[cfg(test)]
mod tests;
