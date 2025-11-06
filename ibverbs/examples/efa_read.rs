//! EFA Read Example

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Starting EFA one-sided read example with separate reader/writer tasks...");

    // Channels to communicate the endpoints between reader and writer.
    let (reader_tx, reader_rx) = std::sync::mpsc::channel();
    let (writer_tx, writer_rx) = std::sync::mpsc::channel();
    // Channels to communicate the remote MRs between reader and writer.
    let (reader_remote_mr_tx, reader_remote_mr_rx) = std::sync::mpsc::channel();
    let (writer_remote_mr_tx, writer_remote_mr_rx) = std::sync::mpsc::channel();
    // Chan to communicate that the writer is ready with data.
    let (writer_is_ready_tx, writer_is_ready_rx) = std::sync::mpsc::channel();
    // Chan to communicate that the reader has finished reading.
    let (reader_done_tx, reader_done_rx) = std::sync::mpsc::channel();

    // Spawn reader and writer tasks
    let reader_handle = std::thread::spawn(move || {
        reader_task(reader_tx, writer_rx, reader_remote_mr_tx, writer_remote_mr_rx, writer_is_ready_rx, reader_done_tx)
    });

    let writer_handle = std::thread::spawn(move || {
        writer_task(writer_tx, reader_rx, writer_remote_mr_tx, reader_remote_mr_rx, writer_is_ready_tx, reader_done_rx)
    });

    // Wait for both tasks to complete
    let reader_result = reader_handle.join().map_err(|_| "Reader thread panicked")?;
    let writer_result = writer_handle.join().map_err(|_| "Writer thread panicked")?;

    reader_result?;
    writer_result?;

    println!("Both reader and writer tasks completed successfully!");
    Ok(())
}

fn reader_task(
    endpoint_tx: std::sync::mpsc::Sender<ibverbs::QueuePairEndpoint>,
    peer_rx: std::sync::mpsc::Receiver<ibverbs::QueuePairEndpoint>,
    remote_mr_tx: std::sync::mpsc::Sender<ibverbs::RemoteMemorySlice>,
    remote_mr_rx: std::sync::mpsc::Receiver<ibverbs::RemoteMemorySlice>,
    writer_is_ready_rx: std::sync::mpsc::Receiver<bool>,
    reader_done_tx: std::sync::mpsc::Sender<bool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Open RDMA device - assume EFA compatibility
    let ctx = match ibverbs::devices()
        .unwrap()
        .iter()
        .nth(1) {
        Some(dev) => dev.open_with_efa().unwrap(),
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
        .and_then(|mut builder| builder.set_gid_index(0).build());

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

    // Wait for writer to be ready with data
    let writer_is_ready = writer_is_ready_rx.recv().unwrap();
    if !writer_is_ready {
        return Err("Writer is not ready".into());
    }

    // Post EFA read from remote memory into local memory
    qp.post_read_efa(&[local_mr.slice(..4096)], remote_mr, peer_endpoint, 0, None).unwrap();

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
                    println!("EFA read failed: wc_code={:?}, vendor_err={:?}", wc_code, vendor_err);
                    return Err("EFA read failed".into());
                }
                // Check the data that was read
                let data = local_mr.inner();
                let read_number = u64::from_le_bytes(data[..8].try_into().unwrap());
                println!("Read number: {}", read_number);

                // Notify writer that reading is complete
                reader_done_tx.send(true).unwrap();
                println!("Reader notified writer of completion");

                return Ok(());
            }
        }
    }
}

fn writer_task(
    endpoint_tx: std::sync::mpsc::Sender<ibverbs::QueuePairEndpoint>,
    peer_rx: std::sync::mpsc::Receiver<ibverbs::QueuePairEndpoint>,
    remote_mr_tx: std::sync::mpsc::Sender<ibverbs::RemoteMemorySlice>,
    remote_mr_rx: std::sync::mpsc::Receiver<ibverbs::RemoteMemorySlice>,
    writer_is_ready_tx: std::sync::mpsc::Sender<bool>,
    reader_done_rx: std::sync::mpsc::Receiver<bool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Open RDMA device - assume EFA compatibility
    let ctx = match ibverbs::devices()
        .unwrap()
        .iter()
        .nth(2) {
        Some(dev) => dev.open_with_efa().unwrap(),
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
        .and_then(|mut builder| builder.set_gid_index(0).build());

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

    let mut local_mr = pd.allocate_with_permissions(4096, efa_access_flags).unwrap();

    // Write a simple number to the local memory region
    let test_number: u64 = 42;
    local_mr.inner_mut()[..std::mem::size_of::<u64>()].copy_from_slice(&test_number.to_le_bytes());
    println!("Writer prepared test number: {}", test_number);

    // Exchange endpoints and memory regions
    endpoint_tx.send(my_endpoint).map_err(|_| "Failed to send endpoint")?;
    let peer_endpoint = peer_rx.recv().map_err(|_| "Failed to receive peer endpoint")?;
    remote_mr_tx.send(local_mr.remote()).unwrap();
    let _remote_mr = remote_mr_rx.recv().unwrap();

    // Move QP to RTR states and create remote AH
    let _qp = qp_builder.handshake(peer_endpoint).unwrap();

    // Signal ready to reader
    writer_is_ready_tx.send(true).unwrap();

    // Writer waits for reader to complete reading
    println!("Writer is ready and waiting for reader to complete...");
    let reader_done = reader_done_rx.recv().unwrap();
    if reader_done {
        println!("Writer received completion notification from reader");
    }

    Ok(())
}
