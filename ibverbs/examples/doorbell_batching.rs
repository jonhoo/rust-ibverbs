//! An example showcasing how to batch multiple work requests into a single doorbell using
//! `QueuePair::start_send`, with only the final operation generating a completion. It RDMA-writes
//! the pieces of a string into a destination buffer and finishes with a send that notifies the
//! (self-looped) receiver.
//!
//! This runs against the first RDMA device; on a machine without one, create a SoftRoCE device
//! with `rdma link add rxe0 type rxe netdev <netdev>`.

use ibverbs::{AccessFlags, LocalMemorySlice, RecvRequest, RemoteMemorySlice};

const RECEIVE_NOTIFICATION_WR_ID: u64 = 100;
const NOTIFY_BUF_SIZE: usize = std::mem::size_of::<u32>();

// Queue capacities (sized to 16, which is larger than the 6 send work requests and 2 completion queue entries needed)
const CQ_CAPACITY: u32 = 16;
const MAX_SEND_WR: u32 = 16;

fn main() {
    // Find and open the first RDMA device
    let ctx = ibverbs::devices()
        .unwrap()
        .iter()
        .next()
        .expect("no rdma device available")
        .open()
        .unwrap();

    // Create Completion Queue (CQ) and Protection Domain (PD)
    let cq = ctx.create_cq(CQ_CAPACITY).build().unwrap();
    let pd = ctx.alloc_pd().unwrap();

    // Create Queue Pair (QP) and connect it to itself in loopback mode, routing from the port's
    // routable GID.
    let gid_index = ctx
        .routable_gid(1)
        .unwrap()
        .expect("no GID available")
        .gid_index;
    let prepared_qp = pd
        .create_qp::<ibverbs::Rc>(&cq, &cq, 1)
        .unwrap()
        .set_gid_index(gid_index)
        .set_max_send_wr(MAX_SEND_WR)
        .set_access(AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE | AccessFlags::REMOTE_READ)
        .build()
        .unwrap();

    let endpoint = prepared_qp.endpoint().unwrap();
    let mut qp = prepared_qp.handshake(endpoint).unwrap();

    // Allocate memory regions
    // We will write pieces of a string into a destination buffer using RDMA Writes,
    // then send a final notification containing the count of write operations.
    let text = b"Hello from chained RDMA writes!";
    let mut src_mr = pd.allocate(text.len(), AccessFlags::PERMISSIVE).unwrap();
    src_mr.bytes_mut().copy_from_slice(text);

    let dest_mr = pd.allocate(text.len(), AccessFlags::PERMISSIVE).unwrap();

    let mut notify_mr = pd
        .allocate(NOTIFY_BUF_SIZE, AccessFlags::PERMISSIVE)
        .unwrap();
    let recv_mr = pd
        .allocate(NOTIFY_BUF_SIZE, AccessFlags::PERMISSIVE)
        .unwrap();

    // Post receive request for the final send notification (4-byte payload)
    unsafe {
        qp.post_recv([RecvRequest::new(
            RECEIVE_NOTIFICATION_WR_ID,
            &[recv_mr.slice(..)],
        )])
    }
    .unwrap();

    // Split the string into slices by space delimiter and prepare the notification payload
    let (locals, remotes): (Vec<[LocalMemorySlice; 1]>, Vec<RemoteMemorySlice>) = text
        .split_inclusive(|&b| b == b' ')
        .map(|sub| {
            let offset = sub.as_ptr() as usize - text.as_ptr() as usize;
            let range = offset..offset + sub.len();
            ([src_mr.slice(range.clone())], dest_mr.remote().slice(range))
        })
        .unzip();

    let num_writes = locals.len();
    let send_chain_completion_wr_id = (num_writes + 1) as u64;

    // Write the count of write operations to the notification buffer as payload
    notify_mr.bytes_mut()[..NOTIFY_BUF_SIZE].copy_from_slice(&(num_writes as u32).to_ne_bytes());
    let notify_slice = [notify_mr.slice(..)];

    // Build and post the chain of work requests as a single doorbell batch.
    let mut batch = qp.start_send();
    // Chain the RDMA Write operations for each word segment.
    for i in 0..num_writes {
        batch.op().write((i + 1) as u64, &locals[i], remotes[i]);
    }
    // Append the final Send to signal completion of the chain and carry the write count.
    batch
        .op()
        .signaled()
        .send(send_chain_completion_wr_id, &notify_slice);
    // Post (ring the doorbell once) for the whole chain.
    unsafe { batch.submit() }.unwrap();

    // Poll completion queue until both the send chain and the receive completion are done.
    // Note that only the final send is posted `.signaled()`, so it alone reports a completion;
    // the unsignaled writes complete silently.
    let mut chain_completed = false;
    let mut receive_completed = false;

    while !chain_completed || !receive_completed {
        let mut completions = cq.poll().unwrap();
        while let Some(wc) = completions.next() {
            println!(
                "Polled WC: wr_id={}, status={:?}, opcode={:?}",
                wc.wr_id(),
                wc.ok(),
                wc.opcode()
            );
            if let Err(e) = wc.ok() {
                panic!(
                    "Work completion failed: {e}, wr_id: {}, opcode: {:?}",
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
                    u32::from_ne_bytes(recv_mr.bytes()[..NOTIFY_BUF_SIZE].try_into().unwrap());
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

    // Print the written data on the receiver side
    let written_str = std::str::from_utf8(dest_mr.bytes()).unwrap();
    println!("Written data in destination buffer: {:?}", written_str);
    assert_eq!(written_str, "Hello from chained RDMA writes!");
}
