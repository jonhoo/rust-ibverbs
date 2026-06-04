use ibverbs::{ibv_send_wr, LocalMemorySlice, RemoteMemorySlice};

const RECEIVE_NOTIFICATION_WR_ID: u64 = 100;
const NOTIFY_BUF_SIZE: usize = std::mem::size_of::<u32>();

// Queue capacities (sized to 16, which is larger than the 6 send work requests and 2 completion queue entries needed)
const CQ_CAPACITY: usize = 16;
const MAX_SEND_WR: u32 = 16;

fn main() {
    // 1. Find and open the first RDMA device
    let ctx = ibverbs::devices()
        .unwrap()
        .iter()
        .next()
        .expect("no rdma device available")
        .open()
        .unwrap();

    // 2. Create Completion Queue (CQ) and Protection Domain (PD)
    let cq = ctx.create_cq(CQ_CAPACITY as i32, 0).unwrap();
    let pd = ctx.alloc_pd().unwrap();

    // 3. Create Queue Pair (QP) and connect it to itself in loopback mode
    let prepared_qp = pd
        .create_qp(&cq, &cq, ibverbs::ibv_qp_type::IBV_QPT_RC)
        .unwrap()
        .set_gid_index(1)
        .set_max_send_wr(MAX_SEND_WR)
        .allow_remote_rw()
        .build()
        .unwrap();

    let endpoint = prepared_qp.endpoint().unwrap();
    let mut qp = prepared_qp.handshake(endpoint).unwrap();

    // 4. Allocate memory regions
    // We will write pieces of a string into a destination buffer using RDMA Writes,
    // then send a final notification containing the count of write operations.
    let text = b"Hello from chained RDMA writes!";
    let mut src_mr = pd.allocate(text.len()).unwrap();
    src_mr.inner_mut().copy_from_slice(text);

    let dest_mr = pd.allocate(text.len()).unwrap();

    let mut notify_mr = pd.allocate(NOTIFY_BUF_SIZE).unwrap();
    let recv_mr = pd.allocate(NOTIFY_BUF_SIZE).unwrap();

    // 5. Post receive request for the final send notification (4-byte payload)
    unsafe { qp.post_receive(&[recv_mr.slice(..)], RECEIVE_NOTIFICATION_WR_ID) }.unwrap();

    // 6. Split the string into slices by space delimiter and prepare the notification payload
    let locals: Vec<[LocalMemorySlice; 1]> = text
        .split_inclusive(|&b| b == b' ')
        .map(|sub| {
            let offset = sub.as_ptr() as usize - text.as_ptr() as usize;
            [src_mr.slice(offset..offset + sub.len())]
        })
        .collect();

    let remotes: Vec<RemoteMemorySlice> = text
        .split_inclusive(|&b| b == b' ')
        .map(|sub| {
            let offset = sub.as_ptr() as usize - text.as_ptr() as usize;
            dest_mr.remote().slice(offset..offset + sub.len())
        })
        .collect();

    let num_writes = locals.len();
    let send_chain_completion_wr_id = (num_writes + 1) as u64;
    let total_send_wrs = num_writes + 1;

    // Write the count of write operations to the notification buffer as payload
    notify_mr.inner_mut()[..NOTIFY_BUF_SIZE].copy_from_slice(&(num_writes as u32).to_ne_bytes());
    let notify_slice = [notify_mr.slice(..)];

    // 7. Build and post the chain of work requests
    // Allocate the WR buffer dynamically based on the total number of send requests
    let mut wrs = vec![ibv_send_wr::default(); total_send_wrs];

    let mut chain = qp.send_chain(&mut wrs);
    // Chain the RDMA Write operations for each word segment
    for i in 0..num_writes {
        chain = chain.write(&locals[i], remotes[i].clone(), (i + 1) as u64, None);
    }
    // Append the final Send operation to signal completion of the chain and carry the write count
    chain = chain.send(&notify_slice, send_chain_completion_wr_id);

    // Post the completed chain to the Queue Pair
    unsafe { chain.post() }.unwrap();

    // 8. Poll completion queue until both the send chain and the receive completion are done.
    // Note that because the writes and the send are chained, only the last operation (the Send)
    // in the chain generates a completion event.
    let mut chain_completed = false;
    let mut receive_completed = false;
    let mut completions = [ibverbs::ibv_wc::default(); CQ_CAPACITY];

    while !chain_completed || !receive_completed {
        let completed = cq.poll(&mut completions[..]).unwrap();
        for wc in completed {
            println!(
                "Polled WC: wr_id={}, status={:?}, opcode={:?}",
                wc.wr_id(),
                wc.error(),
                wc.opcode()
            );
            if !wc.is_valid() {
                panic!(
                    "Work completion failed: {:?}, wr_id: {}, opcode: {:?}",
                    wc.error(),
                    wc.wr_id(),
                    wc.opcode()
                );
            }
            if wc.wr_id() == send_chain_completion_wr_id {
                assert!(!chain_completed);
                chain_completed = true;
                println!("Send chain completed successfully.");
            } else if wc.wr_id() == RECEIVE_NOTIFICATION_WR_ID {
                assert!(!receive_completed);
                receive_completed = true;
                let received_count =
                    u32::from_ne_bytes(recv_mr.inner()[..NOTIFY_BUF_SIZE].try_into().unwrap());
                println!(
                    "Receive notification completed successfully. Count of writes: {}",
                    received_count
                );
                assert_eq!(received_count, num_writes as u32);
            } else {
                panic!("Unexpected work completion ID: {}", wc.wr_id());
            }
        }
    }

    // 9. Print the written data on the receiver side
    let written_str = std::str::from_utf8(dest_mr.inner()).unwrap();
    println!("Written data in destination buffer: {:?}", written_str);
    assert_eq!(written_str, "Hello from chained RDMA writes!");
}
