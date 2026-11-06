use std::io;

use super::{check_data_direct_arguments, registration_error};
use crate::{AccessFlags, Error};

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
                operation: &"mlx5dv_reg_dmabuf_mr",
                errno: e,
            } if e == errno
        ));
        assert!(err.os_error().is_none());
        assert_eq!(
            err.to_string(),
            "mlx5dv_reg_dmabuf_mr is not supported by the device, provider, or kernel"
        );
    }
}

#[test]
fn data_direct_arguments_are_checked_up_front() {
    let rejected = |iova, flags| {
        matches!(
            check_data_direct_arguments(iova, flags, 4096),
            Err(Error::RegisterMemoryRegion(e)) if e.kind() == io::ErrorKind::InvalidInput
        )
    };
    assert!(check_data_direct_arguments(0x20_0000, AccessFlags::LOCAL_WRITE, 4096).is_ok());
    assert!(
        rejected(0x20_0010, AccessFlags::LOCAL_WRITE),
        "unaligned iova"
    );
    assert!(
        rejected(0x20_0000, AccessFlags::LOCAL_WRITE | AccessFlags::ON_DEMAND),
        "on-demand paging"
    );
}
