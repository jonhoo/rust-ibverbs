use ibverbs::{MemoryRegion, PreparedQueuePair};

#[test]
fn can_list_rdma_devices() {
    ibverbs::devices().expect("Could not fetch list of rdma devices");
}

#[test]
fn list_of_rdma_devices_contains_test_device() {
    let device_list = ibverbs::devices().expect("Could not fetch list of rdma devices");
    helper::test_device(&device_list).expect("Could not find RDMA test device");
}

#[test]
fn can_open_test_device() {
    let device_list = ibverbs::devices().expect("Could not fetch list of rdma devices");
    let test_device = helper::test_device(&device_list).expect("Could not find RDMA test device");
    test_device.open().expect("Could not open test device");
}

#[test]
fn test_device_can_open_then_close_then_open() {
    let device_list = ibverbs::devices().expect("Could not fetch list of rdma devices");
    {
        let test_device = helper::test_device(&device_list).expect("Could not find RDMA test device");
        test_device.open().expect("Could not open test device");
    }
    {
        let test_device = helper::test_device(&device_list).expect("Could not find RDMA test device");
        test_device.open().expect("Could not open test device");
    }
}

// Test fails in CI
#[test]
fn can_send_rdma_loopback_traffic_on_test_device() {
    const MINIMUM_COMPLETION_QUEUE_SIZE: i32 = 128;
    const COMPLETION_QUEUE_ID: isize = 1;

    let device_list = ibverbs::devices().expect("Could not fetch list of rdma devices");
    let test_device = helper::test_device(&device_list).expect("Could not find RDMA test device");
    let context = test_device.open().expect("Could not open test device");
    let completion_queue = context
        .create_cq(MINIMUM_COMPLETION_QUEUE_SIZE, COMPLETION_QUEUE_ID)
        .expect("Could not create completion queue");
    {
        let protection_domain = context
            .alloc_pd()
            .expect("Could not allocate protection domain");
        {
            let mut memory_region: MemoryRegion<u64> = protection_domain
                .allocate::<u64>(2)
                .expect("Could not allocate memory region");

            let prepared_queue_pair: PreparedQueuePair = protection_domain
                .create_qp(
                    &completion_queue,
                    &completion_queue,
                    ibverbs::ibv_qp_type::IBV_QPT_RC,
                )
                .allow_remote_rw()
                .set_retry_count(7)
                .build()
                .expect("Could not create prepared queue pair");

            let endpoint = prepared_queue_pair.endpoint();
            {
                let mut queue_pair = prepared_queue_pair
                    .handshake(endpoint)
                    .expect("Could not create queue pair");

                let message = 0x42;
                memory_region[0] = message;

                const RECEIVE_REQUEST_ID: u64 = 1;
                const SEND_REQUEST_ID: u64 = 2;

                unsafe {
                    queue_pair
                        .post_receive(&mut memory_region, ..1, RECEIVE_REQUEST_ID)
                        .expect("failed to post receive request on queue pair");
                    queue_pair
                        .post_send(&mut memory_region, 1.., SEND_REQUEST_ID)
                        .expect("failed to post send request on queue pair");
                }

                let mut sent = false;
                let mut received = false;

                let mut completions = [ibverbs::ibv_wc::default(); MINIMUM_COMPLETION_QUEUE_SIZE as usize];

                while !(sent && received) {
                    let completed = completion_queue
                        .poll(&mut completions[..])
                        .expect("failed to poll completion queue");
                    assert!(completed.len() <= 2);
                    for completion in completed {
                        match completion.wr_id() {
                            SEND_REQUEST_ID => {
                                assert!(!sent);
                                println!("sent!");
                                sent = true;
                            }
                            RECEIVE_REQUEST_ID => {
                                assert!(!received);
                                received = true;
                                println!("received!");
                                assert_eq!(memory_region.get(0).unwrap().clone(), memory_region[1]);
                            }
                            _ => unreachable!()
                        }
                    }
                }
            }
        }
    }
}

mod helper {
    use ibverbs::{Device, DeviceList};

    pub fn test_device(device_list: &DeviceList) -> Option<Device> {
        device_list.iter().find_map(|rdma_device| {
            if let Some(device_name_cstr) = rdma_device.name() {
                match device_name_cstr.to_str() {
                    Ok(device_name) => {
                        if device_name == "rust_ibverbs" {
                            Some(rdma_device)
                        } else {
                            None
                        }
                    }
                    Err(_) => None
                }
            } else {
                None
            }
        })
    }
}
