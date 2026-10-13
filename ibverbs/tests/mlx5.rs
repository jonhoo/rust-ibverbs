#![cfg(feature = "mlx5")]

use ibverbs::{AccessFlags, Error, Mlx5DmabufAccessFlags};

#[test]
#[ignore = "requires IBVERBS_TEST_DEVICE naming a Soft-RoCE (rxe) device"]
fn rxe_rejects_mlx5_operations() {
    let name = std::env::var("IBVERBS_TEST_DEVICE")
        .expect("set IBVERBS_TEST_DEVICE to a Soft-RoCE device name, such as rxe0");
    assert!(
        name.starts_with("rxe"),
        "IBVERBS_TEST_DEVICE={name:?} must name a Soft-RoCE device starting with rxe"
    );
    let devices = ibverbs::devices().expect("failed to list RDMA devices");
    let device = devices
        .iter()
        .find(|device| {
            device
                .name()
                .is_some_and(|candidate| candidate.to_bytes() == name.as_bytes())
        })
        .unwrap_or_else(|| panic!("IBVERBS_TEST_DEVICE={name} is not an available RDMA device"));
    let ctx = device
        .open()
        .unwrap_or_else(|err| panic!("failed to open IBVERBS_TEST_DEVICE={name}: {err:?}"));
    let err = ctx
        .mlx5_data_direct_sysfs_path()
        .expect_err("rxe must reject mlx5dv_get_data_direct_sysfs_path");
    assert!(
        matches!(
            err,
            Error::Unsupported {
                operation: "mlx5dv_get_data_direct_sysfs_path"
            }
        ),
        "mlx5dv_get_data_direct_sysfs_path on {name} returned {err:?}"
    );

    let pd = ctx
        .alloc_pd()
        .unwrap_or_else(|err| panic!("failed to allocate a protection domain on {name}: {err:?}"));
    for flags in [
        Mlx5DmabufAccessFlags::empty(),
        Mlx5DmabufAccessFlags::DATA_DIRECT,
    ] {
        let Err(err) = pd.register_dmabuf_mlx5(-1, 0, 4096, 0, AccessFlags::LOCAL_WRITE, flags)
        else {
            panic!("rxe must reject mlx5dv_reg_dmabuf_mr");
        };
        assert!(
            matches!(
                err,
                Error::Unsupported {
                    operation: "mlx5dv_reg_dmabuf_mr"
                }
            ),
            "mlx5dv_reg_dmabuf_mr on {name} with {flags:?} returned {err:?}"
        );
    }
}
