#![cfg(feature = "mlx5")]

mod common;

use common::open_test_device;
use ibverbs::{AccessFlags, Error};

#[test]
#[ignore = "requires IBVERBS_TEST_DEVICE naming a Soft-RoCE (rxe) device"]
fn rxe_rejects_mlx5_operations() {
    let name = std::env::var("IBVERBS_TEST_DEVICE")
        .expect("set IBVERBS_TEST_DEVICE to a Soft-RoCE device name, such as rxe0");
    assert!(
        name.starts_with("rxe"),
        "IBVERBS_TEST_DEVICE={name:?} must name a Soft-RoCE device starting with rxe"
    );
    let ctx = open_test_device();
    let err = ctx
        .mlx5_data_direct_sysfs_path()
        .expect_err("rxe must reject mlx5dv_get_data_direct_sysfs_path");
    assert!(
        matches!(
            err,
            Error::Unsupported {
                operation: &"mlx5dv_get_data_direct_sysfs_path"
            }
        ),
        "mlx5dv_get_data_direct_sysfs_path on {name} returned {err:?}"
    );

    let pd = ctx
        .alloc_pd()
        .unwrap_or_else(|err| panic!("failed to allocate a protection domain on {name}: {err:?}"));
    let Err(err) = pd.register_dmabuf_data_direct(-1, 0, 4096, 0, AccessFlags::LOCAL_WRITE) else {
        panic!("rxe must reject mlx5dv_reg_dmabuf_mr");
    };
    assert!(
        matches!(
            err,
            Error::Unsupported {
                operation: &"mlx5dv_reg_dmabuf_mr"
            }
        ),
        "mlx5dv_reg_dmabuf_mr on {name} returned {err:?}"
    );
}
