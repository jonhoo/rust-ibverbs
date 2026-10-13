use std::io;
use std::os::unix::ffi::OsStrExt;

use super::{Mlx5DmabufAccessFlags, data_direct_path, registration_error};
use crate::{AccessFlags, Error, MemoryRegion, ProtectionDomain, Result};

#[test]
fn data_direct_path_preserves_provider_bytes() {
    for bytes in [
        b"/devices/pci0000:00/0000:00:01.0".as_slice(),
        b"/devices/non-utf8-\xff",
        b" relative path/with trailing space \n",
        b"/devices//unchanged/../path/",
    ] {
        let mut buf = bytes.to_vec();
        buf.extend_from_slice(b"\0ignored trailing bytes");
        let path = data_direct_path(0, &buf).unwrap();
        assert_eq!(path.as_os_str().as_bytes(), bytes);
    }
}

#[test]
fn data_direct_path_rejects_empty_and_unterminated_output() {
    for buf in [
        b"".as_slice(),
        b"\0",
        b"\0ignored",
        b"/devices/no-terminator",
    ] {
        let err = data_direct_path(0, buf).unwrap_err();
        assert!(matches!(err, Error::QueryMlx5DataDirectSysfsPath(_)));
        assert_eq!(err.os_error().unwrap().kind(), io::ErrorKind::InvalidData);
    }
}

#[test]
fn data_direct_path_preserves_return_code_as_os_error() {
    for errno in [nix::libc::ENODEV, nix::libc::ENOSPC, nix::libc::EINVAL] {
        let err = data_direct_path(errno, b"/devices/unused\0").unwrap_err();
        assert!(matches!(err, Error::QueryMlx5DataDirectSysfsPath(_)));
        assert_eq!(err.os_error().unwrap().raw_os_error(), Some(errno));
    }
}

#[test]
fn data_direct_path_unsupported_names_the_mlx5_operation() {
    // EPROTONOSUPPORT: a kernel older than the Data Direct query (Linux 6.12).
    for errno in [nix::libc::EOPNOTSUPP, nix::libc::EPROTONOSUPPORT] {
        let err = data_direct_path(errno, b"").unwrap_err();
        assert!(matches!(
            err,
            Error::Unsupported {
                operation: &"mlx5dv_get_data_direct_sysfs_path"
            }
        ));
        assert!(err.os_error().is_none());
        assert_eq!(
            err.to_string(),
            "mlx5dv_get_data_direct_sysfs_path is not supported by the device, provider, or kernel"
        );
    }
}

#[test]
fn registration_preserves_os_errors() {
    for errno in [nix::libc::ENODEV, nix::libc::ENOSPC, nix::libc::EINVAL] {
        let err = registration_error(io::Error::from_raw_os_error(errno));
        assert!(matches!(err, Error::RegisterMemoryRegion(_)));
        assert_eq!(err.os_error().unwrap().raw_os_error(), Some(errno));
    }
}

#[test]
fn registration_unsupported_names_the_mlx5_operation() {
    // EPROTONOSUPPORT: a kernel older than the mlx5 access-flags attribute (Linux 6.12).
    for errno in [nix::libc::EOPNOTSUPP, nix::libc::EPROTONOSUPPORT] {
        let err = registration_error(io::Error::from_raw_os_error(errno));
        assert!(matches!(
            err,
            Error::Unsupported {
                operation: &"mlx5dv_reg_dmabuf_mr"
            }
        ));
        assert!(err.os_error().is_none());
        assert_eq!(
            err.to_string(),
            "mlx5dv_reg_dmabuf_mr is not supported by the device, provider, or kernel"
        );
    }
}

#[test]
fn registration_returns_the_existing_memory_region_type() {
    let _register = |pd: &ProtectionDomain| -> Result<MemoryRegion<()>> {
        pd.register_dmabuf_mlx5(
            -1,
            0,
            4096,
            0,
            AccessFlags::LOCAL_WRITE,
            Mlx5DmabufAccessFlags::DATA_DIRECT,
        )
    };
}
