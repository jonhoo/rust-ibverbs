//! EFA Write Example
//! 
//! Key Notes:
//! - "handshakes" in SRD is not exactly a handshake. We really just need to create the AHs on 
//!   both sides and then we can use the AHs to send and receive data. This is because SRD
//!   is a connectionless protocol. It does not gurantee that both sides are ready.
//! - We use the QP_EX API to create the QP. This is primarily needed so that we can create an SRD QP.
//! - All MRs must be registered before the QP is created and handshaked...
//! - All post_receives on receiver side must be posted before the writer sends.
//!   Failure to do so will result in a `IBV_WC_RNR_RETRY_EXC_ERR` error in WCE
//! - Writes are non ordered. You must not depend on the "last" write with imm data to be received as a
//!   signal of last write completion.

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Starting EFA one-sided write example with separate sender/receiver tasks...");

    // Channels to communicate the endpoints between sender and receiver.
    let (sender_tx, sender_rx) = std::sync::mpsc::channel();
    let (receiver_tx, receiver_rx) = std::sync::mpsc::channel();
    // Channels to communicate the remote MRs between sender and receiver.
    let (sender_remote_mr_tx, sender_remote_mr_rx) = std::sync::mpsc::channel();
    let (receiver_remote_mr_tx, receiver_remote_mr_rx) = std::sync::mpsc::channel();
    // Chan to communicate that the receiver is ready to receive data.
    let (receiver_is_ready_tx, receiver_is_ready_rx) = std::sync::mpsc::channel();

    // Spawn sender and receiver tasks
    let sender_handle = std::thread::spawn(move || {
        sender_task(sender_tx, receiver_rx, sender_remote_mr_tx, receiver_remote_mr_rx, receiver_is_ready_rx)
    });

    let receiver_handle = std::thread::spawn(move || {
        receiver_task(receiver_tx, sender_rx, receiver_remote_mr_tx, sender_remote_mr_rx, receiver_is_ready_tx)
    });

    // Wait for both tasks to complete
    let sender_result = sender_handle.join().map_err(|_| "Sender thread panicked")?;
    let receiver_result = receiver_handle.join().map_err(|_| "Receiver thread panicked")?;

    sender_result?;
    receiver_result?;

    println!("Both sender and receiver tasks completed successfully!");
    Ok(())
}

fn sender_task(
    endpoint_tx: std::sync::mpsc::Sender<ibverbs::QueuePairEndpoint>,
    peer_rx: std::sync::mpsc::Receiver<ibverbs::QueuePairEndpoint>,
    remote_mr_tx: std::sync::mpsc::Sender<ibverbs::RemoteMemorySlice>,
    remote_mr_rx: std::sync::mpsc::Receiver<ibverbs::RemoteMemorySlice>,
    receiver_is_ready_rx: std::sync::mpsc::Receiver<bool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Open RDMA device - assume EFA compatibility
    let ctx = match ibverbs::devices()
        .unwrap()
        .iter()
        .nth(1) {
        Some(dev) => dev.open().unwrap(),
        None => {
            eprintln!("No RDMA devices found!");
            return Ok(());
        }
    };

    // Create completion queue and protection domain
    let cq = ctx.create_cq(16, 0).unwrap();
    let pd = ctx.alloc_pd().unwrap();

    // Create EFA QP
    let qp_builder_result = pd
        .create_qp(&cq, &cq, ibverbs::ibv_qp_type::IBV_QPT_DRIVER)
        .and_then(|mut builder| builder.set_gid_index(0).enable_efa(true).build());

    let qp_builder = match qp_builder_result {
        Ok(builder) => builder,
        Err(e) => {
            println!("EFA QP creation failed: {}. This is expected if EFA is not available.", e);
            return Ok(());
        }
    };

    let my_endpoint = qp_builder.endpoint()?;

    // Allocate and register memory regions
    let efa_access_flags = ibverbs::ibv_access_flags(
        ibverbs::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0 |
        ibverbs::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE.0 |
        ibverbs::ibv_access_flags::IBV_ACCESS_REMOTE_READ.0
    );

    let local_mr = pd.allocate_with_permissions(4096, efa_access_flags).unwrap();

    // Exchange endpoints and memory regions
    endpoint_tx.send(my_endpoint).map_err(|_| "Failed to send endpoint")?;
    let peer_endpoint = peer_rx.recv().map_err(|_| "Failed to receive peer endpoint")?;
    let remote_mr = remote_mr_rx.recv().unwrap();
    remote_mr_tx.send(local_mr.remote()).unwrap();

    // Move QP to RTR states and create remote AH
    let mut qp = qp_builder.handshake(peer_endpoint).unwrap();

    // Wait for receiver to be ready
    let receiver_is_ready = receiver_is_ready_rx.recv().unwrap();
    if !receiver_is_ready {
        return Err("Receiver is not ready".into());
    }

    // Post EFA write with immediate data
    qp.post_write(&[local_mr.slice(..4096)], remote_mr, 0, Some(0x67)).unwrap();

    // Wait for completion
    let mut completions = [ibverbs::ibv_wc::default(); 16];
    loop {
        let completed = cq.poll(&mut completions[..]).unwrap();
        if completed.is_empty() {
            continue;
        }
        for wr in completed {
            if wr.wr_id() == 0 {
                if let Some((wc_code, vendor_err)) = wr.error() {
                    println!("EFA write failed: wc_code={:?}, vendor_err={:?}", wc_code, vendor_err);
                    return Err("EFA write failed".into());
                }
                return Ok(());
            }
        }
    }
}

fn receiver_task(
    endpoint_tx: std::sync::mpsc::Sender<ibverbs::QueuePairEndpoint>,
    peer_rx: std::sync::mpsc::Receiver<ibverbs::QueuePairEndpoint>,
    remote_mr_tx: std::sync::mpsc::Sender<ibverbs::RemoteMemorySlice>,
    remote_mr_rx: std::sync::mpsc::Receiver<ibverbs::RemoteMemorySlice>,
    receiver_is_ready_tx: std::sync::mpsc::Sender<bool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Open RDMA device - assume EFA compatibility
    let ctx = match ibverbs::devices()
        .unwrap()
        .iter()
        .nth(2) {
        Some(dev) => dev.open().unwrap(),
        None => {
            eprintln!("No RDMA devices found!");
            return Ok(());
        }
    };

    // Create completion queue and protection domain
    let cq = ctx.create_cq(16, 0).unwrap();
    let pd = ctx.alloc_pd().unwrap();

    // Create EFA QP
    let qp_builder_result = pd
        .create_qp(&cq, &cq, ibverbs::ibv_qp_type::IBV_QPT_DRIVER)
        .and_then(|mut builder| builder.set_gid_index(0).enable_efa(true).build());

    let qp_builder = match qp_builder_result {
        Ok(builder) => builder,
        Err(e) => {
            println!("EFA QP creation failed: {}. This is expected if EFA is not available.", e);
            return Ok(());
        }
    };

    let my_endpoint = qp_builder.endpoint()?;

    // Allocate and register memory regions
    let efa_access_flags = ibverbs::ibv_access_flags(
        ibverbs::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0 |
        ibverbs::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE.0 |
        ibverbs::ibv_access_flags::IBV_ACCESS_REMOTE_READ.0
    );

    let local_mr = pd.allocate_with_permissions(4096, efa_access_flags).unwrap();

    // Exchange endpoints and memory regions
    endpoint_tx.send(my_endpoint).map_err(|_| "Failed to send endpoint")?;
    let peer_endpoint = peer_rx.recv().map_err(|_| "Failed to receive peer endpoint")?;
    remote_mr_tx.send(local_mr.remote()).unwrap();
    let _remote_mr = remote_mr_rx.recv().unwrap();

    // Move QP to RTR states and create remote AH
    let mut qp = qp_builder.handshake(peer_endpoint).unwrap();

    // Post receive for the write operation
    let dummy_buffer = pd.allocate_with_permissions(4096, efa_access_flags).unwrap();
    unsafe { qp.post_receive(&[dummy_buffer.slice(..)], 1) }.unwrap();

    // Signal ready to sender
    receiver_is_ready_tx.send(true).unwrap();

    // Wait for completion
    let mut completions = [ibverbs::ibv_wc::default(); 16];
    loop {
        let completed = cq.poll(&mut completions[..]).unwrap();
        if completed.is_empty() {
            std::thread::sleep(std::time::Duration::from_millis(1));
            continue;
        }
        for wr in completed {
            if wr.wr_id() == 1 {
                let imm_data = wr.imm_data();
                let imm_data_host_order = u32::from_be(imm_data.unwrap());
                println!("Received immediate data: {} (0x{:08x})", imm_data_host_order, imm_data_host_order);
                return Ok(());
            }
        }
    }
}
