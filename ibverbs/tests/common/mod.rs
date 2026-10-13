//! Helpers shared by the device tests. Each test crate uses only some of them.
#![allow(dead_code)]

use ibverbs::{Context, Device, DeviceList};

/// The device named by `IBVERBS_TEST_DEVICE`, or the first available one. Panics if absent; the
/// device tests only run when explicitly requested, so a missing device is a real setup error.
pub fn test_device(devices: &DeviceList) -> Device<'_> {
    match std::env::var("IBVERBS_TEST_DEVICE") {
        Ok(name) if !name.is_empty() => devices
            .iter()
            .find(|d| d.name().is_some_and(|n| n.to_bytes() == name.as_bytes()))
            .unwrap_or_else(|| {
                panic!("IBVERBS_TEST_DEVICE={name} is not among the available RDMA devices")
            }),
        _ => devices
            .iter()
            .next()
            .expect("no RDMA device available (attach one or set IBVERBS_TEST_DEVICE)"),
    }
}

/// Open the [`test_device`].
pub fn open_test_device() -> Context {
    let devices = ibverbs::devices().expect("failed to list RDMA devices");
    test_device(&devices)
        .open()
        .expect("failed to open the RDMA device")
}
