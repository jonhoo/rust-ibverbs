use std::io;
use std::os::unix::ffi::OsStrExt;

use super::{Mlx5DmabufAccessFlags, data_direct_path, registration_error};
use crate::{AccessFlags, Error, MemoryRegion, ProtectionDomain, Result};

#[test]
fn flags_match_generated_bindings_and_round_trip_unknown_bits() {
    let raw: ffi::mlx5_ib_uapi_reg_dmabuf_flags = Mlx5DmabufAccessFlags::DATA_DIRECT.into();
    assert_eq!(
        raw.0,
        ffi::mlx5_ib_uapi_reg_dmabuf_flags::MLX5_IB_UAPI_REG_DMABUF_ACCESS_DATA_DIRECT.0
    );
    for bits in [0, raw.0, 0x8000_0000, u32::MAX] {
        let flags = Mlx5DmabufAccessFlags::from(ffi::mlx5_ib_uapi_reg_dmabuf_flags(bits));
        let round_trip: ffi::mlx5_ib_uapi_reg_dmabuf_flags = flags.into();
        assert_eq!(round_trip.0, bits);
    }
}

#[test]
fn flags_support_empty_contains_and_bit_operators() {
    let empty = Mlx5DmabufAccessFlags::empty();
    let direct = Mlx5DmabufAccessFlags::DATA_DIRECT;
    let unknown = Mlx5DmabufAccessFlags::from(ffi::mlx5_ib_uapi_reg_dmabuf_flags(0x8000_0000));
    assert_eq!(empty, Mlx5DmabufAccessFlags::default());
    assert!(empty.contains(empty));
    assert!(!empty.contains(direct));
    assert!(direct.contains(empty));
    assert!(direct.contains(direct));
    assert_eq!(direct | empty, direct);
    assert_eq!(direct & empty, empty);
    let combined = direct | unknown;
    assert!(combined.contains(direct));
    assert!(combined.contains(unknown));
    assert!(!direct.contains(combined));
    assert_eq!(combined & direct, direct);
    assert_eq!(combined & unknown, unknown);
    let mut assigned = empty;
    assigned |= direct;
    assigned |= unknown;
    assert_eq!(assigned, combined);
}

#[test]
fn flags_debug_names_known_and_unknown_bits() {
    assert_eq!(
        format!("{:?}", Mlx5DmabufAccessFlags::empty()),
        "Mlx5DmabufAccessFlags(0)"
    );
    assert_eq!(
        format!("{:?}", Mlx5DmabufAccessFlags::DATA_DIRECT),
        "Mlx5DmabufAccessFlags(DATA_DIRECT)"
    );
    let unknown = Mlx5DmabufAccessFlags::from(ffi::mlx5_ib_uapi_reg_dmabuf_flags(0x8000_0000));
    assert_eq!(format!("{unknown:?}"), "Mlx5DmabufAccessFlags(0x80000000)");
    assert_eq!(
        format!("{:?}", Mlx5DmabufAccessFlags::DATA_DIRECT | unknown),
        "Mlx5DmabufAccessFlags(DATA_DIRECT | 0x80000000)"
    );
}

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
    let err = data_direct_path(nix::libc::EOPNOTSUPP, b"").unwrap_err();
    assert!(matches!(
        err,
        Error::Unsupported {
            operation: &"mlx5dv_get_data_direct_sysfs_path"
        }
    ));
    assert!(err.os_error().is_none());
    assert_eq!(
        err.to_string(),
        "mlx5dv_get_data_direct_sysfs_path is not supported by the device or provider"
    );
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
    let err = registration_error(io::Error::from_raw_os_error(nix::libc::EOPNOTSUPP));
    assert!(matches!(
        err,
        Error::Unsupported {
            operation: &"mlx5dv_reg_dmabuf_mr"
        }
    ));
    assert!(err.os_error().is_none());
    assert_eq!(
        err.to_string(),
        "mlx5dv_reg_dmabuf_mr is not supported by the device or provider"
    );
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
