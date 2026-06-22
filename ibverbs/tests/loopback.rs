//! Integration tests for the RDMA data path.
//!
//! Each test self-loops a single queue pair (connected to its own endpoint), so it needs only one
//! host and no peer, which makes the data path testable with Soft-RoCE (rxe).
//!
//! The tests require a real RDMA device, so they are `#[ignore]`d: plain `cargo test` skips them
//! (but still compile-checks them); `cargo test -- --ignored` runs them. They use the first
//! available device, or `IBVERBS_TEST_DEVICE` (e.g. `rxe0`) if set.

use std::time::{Duration, Instant};

use ibverbs::{
    ibv_qp_type, AddressHandleAttribute, CompletionQueue, Context, ProtectionDomain, QueuePair,
    WorkRequest,
};

/// A queue pair connected to itself, with the resources it uses.
struct Loopback {
    qp: QueuePair,
    cq: CompletionQueue,
    pd: ProtectionDomain,
}

/// Open the device named by `IBVERBS_TEST_DEVICE`, or the first available one. Panics if absent;
/// these tests only run when explicitly requested, so a missing device is a real setup error.
fn open_test_device() -> Context {
    let devices = ibverbs::devices().expect("failed to list RDMA devices");
    let device = match std::env::var("IBVERBS_TEST_DEVICE") {
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
    };
    device.open().expect("failed to open the RDMA device")
}

/// Build a self-connected queue pair of the given type, with generous queue/SGE limits and remote
/// access so the tests can post batches, multi-SGE lists, and one-sided operations.
fn loopback_of(qp_type: ibv_qp_type) -> Loopback {
    let ctx = open_test_device();
    let cq = ctx
        .create_cq(64, 0)
        .expect("failed to create completion queue");
    let pd = ctx
        .alloc_pd()
        .expect("failed to allocate protection domain");

    let mut builder = pd
        .create_qp(&cq, &cq, qp_type)
        .expect("failed to create queue pair");
    builder
        .set_gid_index(1)
        .set_max_send_wr(16)
        .set_max_recv_wr(16)
        .set_max_send_sge(4)
        .set_max_recv_sge(4);
    // One-sided ops loop back to this same QP, so it must grant remote access (RC/UC only).
    builder.allow_remote_rw();

    let prepared = builder.build().expect("failed to build queue pair");
    let endpoint = prepared.endpoint().expect("failed to read local endpoint");
    let qp = prepared
        .handshake(endpoint)
        .expect("failed to transition queue pair to RTS");

    Loopback { qp, cq, pd }
}

/// A reliable-connected self-loopback queue pair (the common case).
fn loopback() -> Loopback {
    loopback_of(ibv_qp_type::IBV_QPT_RC)
}

/// Poll until `n` completions arrive (asserting each succeeded) and return them. Panics after a few
/// seconds if they don't (loopback completes in microseconds).
fn drain(cq: &CompletionQueue, n: usize) -> Vec<ibverbs::ibv_wc> {
    let mut observed = Vec::with_capacity(n);
    let mut completions = [ibverbs::ibv_wc::default(); 16];
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        let done = cq.poll(&mut completions).expect("failed to poll CQ");
        for wc in done.iter() {
            if let Some((status, vendor_err)) = wc.error() {
                panic!(
                    "work request {} failed: status {status:?}, vendor_err {vendor_err}",
                    wc.wr_id()
                );
            }
            observed.push(*wc);
        }
        if observed.len() >= n {
            return observed;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for completions: got {} of {n}",
            observed.len()
        );
    }
}

/// Reading the GID table of the device exercises the control path.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn gid_table() {
    let ctx = open_test_device();
    let gids = ctx.gid_table().expect("failed to read GID table");
    assert!(!gids.is_empty(), "expected at least one GID entry");
}

/// Two-sided SEND / RECV: a posted receive catches a send to the same queue pair.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn send_recv() {
    let mut lb = loopback();

    let mut recv = lb.pd.allocate(64).expect("failed to register recv MR");
    let mut send = lb.pd.allocate(64).expect("failed to register send MR");
    send.inner_mut()[..5].copy_from_slice(b"hello");

    unsafe { lb.qp.post_receive(&[recv.slice(..5)], 1) }.expect("post_receive failed");
    unsafe { lb.qp.post_send(&[send.slice(..5)], 2) }.expect("post_send failed");

    let comps = drain(&lb.cq, 2);
    assert!(
        comps.iter().any(|wc| wc.wr_id() == 1),
        "missing recv completion"
    );
    assert!(
        comps.iter().any(|wc| wc.wr_id() == 2),
        "missing send completion"
    );
    assert_eq!(&recv.inner_mut()[..5], b"hello");
}

/// A larger transfer that spans multiple MTU-sized packets reports the right received byte length.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn send_recv_large() {
    let mut lb = loopback();

    const LEN: usize = 4096;
    let mut recv = lb.pd.allocate(LEN).expect("failed to register recv MR");
    let mut send = lb.pd.allocate(LEN).expect("failed to register send MR");
    for (i, b) in send.inner_mut().iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }

    unsafe { lb.qp.post_receive(&[recv.slice(..)], 1) }.expect("post_receive failed");
    unsafe { lb.qp.post_send(&[send.slice(..)], 2) }.expect("post_send failed");

    let comps = drain(&lb.cq, 2);
    let recv_wc = comps
        .iter()
        .find(|wc| wc.wr_id() == 1)
        .expect("missing recv completion");
    assert_eq!(recv_wc.len(), LEN, "received byte length mismatch");
    assert_eq!(recv.inner_mut().as_slice(), send.inner_mut().as_slice());
}

/// Scatter-gather: a send gathers two non-contiguous source slices, and the receive scatters the
/// resulting contiguous message into two non-contiguous destination slices.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn scatter_gather() {
    let mut lb = loopback();

    let mut send = lb.pd.allocate(64).expect("failed to register send MR");
    let mut recv = lb.pd.allocate(64).expect("failed to register recv MR");
    send.inner_mut()[0..4].copy_from_slice(b"AAAA");
    send.inner_mut()[16..20].copy_from_slice(b"BBBB");

    unsafe {
        lb.qp
            .post_receive(&[recv.slice(0..4), recv.slice(32..36)], 1)
    }
    .expect("post_receive failed");
    unsafe { lb.qp.post_send(&[send.slice(0..4), send.slice(16..20)], 2) }
        .expect("post_send failed");

    let comps = drain(&lb.cq, 2);
    let recv_wc = comps
        .iter()
        .find(|wc| wc.wr_id() == 1)
        .expect("missing recv completion");
    assert_eq!(recv_wc.len(), 8, "scattered byte length mismatch");
    // The 8-byte gathered message "AAAABBBB" is scattered into the two receive slices in order.
    assert_eq!(&recv.inner_mut()[0..4], b"AAAA");
    assert_eq!(&recv.inner_mut()[32..36], b"BBBB");
}

/// One-sided RDMA WRITE: the initiator writes directly into a remote memory region.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn rdma_write() {
    let mut lb = loopback();

    let mut src = lb.pd.allocate(64).expect("failed to register src MR");
    let mut dst = lb.pd.allocate(64).expect("failed to register dst MR");
    src.inner_mut()[..6].copy_from_slice(b"verbs!");

    let remote = dst.remote().slice(..6);
    unsafe { lb.qp.post_write(&[src.slice(..6)], remote, 1, None) }.expect("post_write failed");

    let comps = drain(&lb.cq, 1);
    assert_eq!(comps[0].wr_id(), 1);
    assert_eq!(&dst.inner_mut()[..6], b"verbs!");
}

/// RDMA WRITE with immediate: the write lands in remote memory and also consumes a receive work
/// request, whose completion carries the immediate value.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn rdma_write_with_imm() {
    let mut lb = loopback();

    let mut src = lb.pd.allocate(64).expect("failed to register src MR");
    let mut dst = lb.pd.allocate(64).expect("failed to register dst MR");
    let dummy = lb.pd.allocate(64).expect("failed to register dummy MR");
    src.inner_mut()[..4].copy_from_slice(&[1, 2, 3, 4]);

    // A write-with-immediate consumes a receive work request on the target queue pair.
    unsafe { lb.qp.post_receive(&[dummy.slice(..1)], 10) }.expect("post_receive failed");

    let imm = 0xdead_beef_u32;
    let remote = dst.remote().slice(..4);
    unsafe { lb.qp.post_write(&[src.slice(..4)], remote, 11, Some(imm)) }
        .expect("post_write failed");

    let comps = drain(&lb.cq, 2);
    assert_eq!(&dst.inner_mut()[..4], &[1, 2, 3, 4]);
    let recv = comps
        .iter()
        .find(|wc| wc.wr_id() == 10)
        .expect("missing recv completion for write-with-imm");
    assert_eq!(recv.imm_data(), Some(imm), "immediate value not delivered");
}

/// One-sided RDMA READ: the initiator reads directly from a remote memory region.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn rdma_read() {
    let mut lb = loopback();

    let mut remote_mr = lb.pd.allocate(64).expect("failed to register remote MR");
    let mut local = lb.pd.allocate(64).expect("failed to register local MR");
    remote_mr.inner_mut()[..8].copy_from_slice(&[9, 8, 7, 6, 5, 4, 3, 2]);

    let remote = remote_mr.remote().slice(..8);
    unsafe { lb.qp.post_read(&[local.slice(..8)], remote, 1) }.expect("post_read failed");

    let comps = drain(&lb.cq, 1);
    assert_eq!(comps[0].wr_id(), 1);
    assert_eq!(&local.inner_mut()[..8], &[9, 8, 7, 6, 5, 4, 3, 2]);
}

/// Batched posting: a single `post` call chains an (unsignaled) RDMA write followed by a signaled
/// send. Only the send is signaled, so the send queue yields exactly one completion.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn batched_post() {
    let mut lb = loopback();

    let mut payload = lb.pd.allocate(64).expect("failed to register payload MR");
    let mut dst = lb.pd.allocate(64).expect("failed to register dst MR");
    let mut note = lb.pd.allocate(64).expect("failed to register note MR");
    let mut recv = lb.pd.allocate(64).expect("failed to register recv MR");
    payload.inner_mut()[..3].copy_from_slice(&[42, 43, 44]);
    note.inner_mut()[..2].copy_from_slice(&[1, 2]);

    unsafe { lb.qp.post_receive(&[recv.slice(..2)], 100) }.expect("post_receive failed");

    let payload_sge = [payload.slice(..3)];
    let note_sge = [note.slice(..2)];
    let remote = dst.remote().slice(..3);
    unsafe {
        lb.qp.post([
            WorkRequest::write(&payload_sge, remote, 101, None),
            WorkRequest::send(&note_sge, 102, None).signaled(),
        ])
    }
    .expect("batched post failed");

    // The receive completion for the send plus the single signaled send completion.
    let comps = drain(&lb.cq, 2);
    assert!(
        comps.iter().any(|wc| wc.wr_id() == 100),
        "missing recv completion"
    );
    assert!(
        comps.iter().any(|wc| wc.wr_id() == 102),
        "missing send completion"
    );
    assert_eq!(&dst.inner_mut()[..3], &[42, 43, 44]);
    assert_eq!(&recv.inner_mut()[..2], &[1, 2]);
}

/// Many outstanding work requests complete: post a batch of receives and sends, then confirm every
/// one produces a completion.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn multiple_outstanding() {
    let mut lb = loopback();

    const N: u64 = 8;
    // Keep the memory regions alive until their work requests complete.
    let mut recv_mrs = Vec::new();
    for i in 0..N {
        let mr = lb.pd.allocate(8).expect("failed to register recv MR");
        unsafe { lb.qp.post_receive(&[mr.slice(..8)], 1000 + i) }.expect("post_receive failed");
        recv_mrs.push(mr);
    }
    let mut send_mrs = Vec::new();
    for i in 0..N {
        let mut mr = lb.pd.allocate(8).expect("failed to register send MR");
        mr.inner_mut()[0] = i as u8;
        unsafe { lb.qp.post_send(&[mr.slice(..8)], i) }.expect("post_send failed");
        send_mrs.push(mr);
    }

    let comps = drain(&lb.cq, (2 * N) as usize);
    for i in 0..N {
        assert!(
            comps.iter().any(|wc| wc.wr_id() == i),
            "missing send completion {i}"
        );
        assert!(
            comps.iter().any(|wc| wc.wr_id() == 1000 + i),
            "missing recv completion {i}"
        );
    }
}

/// The blocking completion-channel path (`wait`) returns completions just like polling does.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn wait_for_completion() {
    let mut lb = loopback();

    let mut recv = lb.pd.allocate(16).expect("failed to register recv MR");
    let mut send = lb.pd.allocate(16).expect("failed to register send MR");
    send.inner_mut()[..4].copy_from_slice(b"wait");

    unsafe { lb.qp.post_receive(&[recv.slice(..4)], 1) }.expect("post_receive failed");
    unsafe { lb.qp.post_send(&[send.slice(..4)], 2) }.expect("post_send failed");

    // Block on the completion channel instead of busy-polling.
    let mut completions = [ibverbs::ibv_wc::default(); 8];
    let mut ids = Vec::new();
    while ids.len() < 2 {
        let done = lb
            .cq
            .wait(&mut completions, Some(Duration::from_secs(5)))
            .expect("wait failed");
        for wc in done.iter() {
            assert!(wc.error().is_none(), "work request {} failed", wc.wr_id());
            ids.push(wc.wr_id());
        }
    }
    assert!(
        ids.contains(&1) && ids.contains(&2),
        "missing completions: {ids:?}"
    );
    assert_eq!(&recv.inner_mut()[..4], b"wait");
}

/// An unreliable-connected (UC) queue pair carries SEND/RECV traffic to itself.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn unreliable_connection() {
    let mut lb = loopback_of(ibv_qp_type::IBV_QPT_UC);

    let mut recv = lb.pd.allocate(64).expect("failed to register recv MR");
    let mut send = lb.pd.allocate(64).expect("failed to register send MR");
    send.inner_mut()[..3].copy_from_slice(b"ucq");

    unsafe { lb.qp.post_receive(&[recv.slice(..3)], 1) }.expect("post_receive failed");
    unsafe { lb.qp.post_send(&[send.slice(..3)], 2) }.expect("post_send failed");

    let comps = drain(&lb.cq, 2);
    assert!(
        comps.iter().any(|wc| wc.wr_id() == 1),
        "missing recv completion"
    );
    assert!(
        comps.iter().any(|wc| wc.wr_id() == 2),
        "missing send completion"
    );
    assert_eq!(&recv.inner_mut()[..3], b"ucq");
}

/// Shared receive queue: the queue pair draws its receive buffers from an SRQ rather than its own
/// receive queue. This exercises a feature the crate exposes that some alternatives do not.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn shared_receive_queue() {
    let ctx = open_test_device();

    let cq = ctx.create_cq(16, 0).expect("failed to create CQ");
    let pd = ctx.alloc_pd().expect("failed to allocate PD");
    let srq = pd.create_srq(16, 1, 0).expect("failed to create SRQ");

    let prepared = pd
        .create_qp(&cq, &cq, ibv_qp_type::IBV_QPT_RC)
        .expect("failed to create QP")
        .set_gid_index(1)
        .set_srq(&srq)
        .build()
        .expect("failed to build QP");
    let endpoint = prepared.endpoint().expect("failed to read endpoint");
    let mut qp = prepared.handshake(endpoint).expect("failed to connect QP");

    let mut recv = pd.allocate(64).expect("failed to register recv MR");
    let mut send = pd.allocate(64).expect("failed to register send MR");
    send.inner_mut()[..4].copy_from_slice(b"srq!");

    // Receives go to the SRQ, not the queue pair's own receive queue.
    unsafe { srq.post_receive(&[recv.slice(..4)], 1) }.expect("SRQ post_receive failed");
    unsafe { qp.post_send(&[send.slice(..4)], 2) }.expect("post_send failed");

    let comps = drain(&cq, 2);
    assert!(
        comps.iter().any(|wc| wc.wr_id() == 1),
        "missing SRQ recv completion"
    );
    assert!(
        comps.iter().any(|wc| wc.wr_id() == 2),
        "missing send completion"
    );
    assert_eq!(&recv.inner_mut()[..4], b"srq!");
}

/// Unreliable datagram (UD): a connectionless queue pair sends a datagram to itself via an address
/// handle pointing at its own GID. UD prepends a 40-byte GRH to received messages.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn unreliable_datagram() {
    let ctx = open_test_device();
    let cq = ctx.create_cq(16, 0).expect("failed to create CQ");
    let pd = ctx.alloc_pd().expect("failed to allocate PD");

    const GID_INDEX: u32 = 1;
    const QKEY: u32 = 0x1234_5678;

    let prepared = pd
        .create_qp(&cq, &cq, ibv_qp_type::IBV_QPT_UD)
        .expect("failed to create UD QP")
        .set_gid_index(GID_INDEX)
        .build()
        .expect("failed to build UD QP");
    let endpoint = prepared.endpoint().expect("failed to read endpoint");
    let mut qp = prepared
        .activate_ud(QKEY)
        .expect("failed to activate UD QP");

    // Address handle pointing at our own GID, so the datagram loops back to us.
    let my_gid = endpoint.gid.expect("RoCE requires a GID");
    let mut ah_attr = AddressHandleAttribute::new();
    ah_attr.set_grh(my_gid, GID_INDEX as u8, 64, 0);
    let ah = pd
        .create_address_handle(&ah_attr)
        .expect("failed to create address handle");

    let payload = b"datagram";
    // UD receives prepend a 40-byte GRH, so the receive buffer must allow for it.
    let mut recv = pd.allocate(40 + 64).expect("failed to register recv MR");
    let mut send = pd.allocate(64).expect("failed to register send MR");
    send.inner_mut()[..payload.len()].copy_from_slice(payload);

    unsafe { qp.post_receive(&[recv.slice(..40 + payload.len())], 1) }
        .expect("post_receive failed");
    unsafe { qp.post_send_ud(&[send.slice(..payload.len())], &ah, endpoint.num, QKEY, 2) }
        .expect("post_send_ud failed");

    let comps = drain(&cq, 2);
    let recv_wc = comps
        .iter()
        .find(|wc| wc.wr_id() == 1)
        .expect("missing recv completion");
    assert_eq!(
        recv_wc.len(),
        40 + payload.len(),
        "UD receive length should include the 40-byte GRH"
    );
    assert!(
        comps.iter().any(|wc| wc.wr_id() == 2),
        "missing send completion"
    );
    // The payload starts after the 40-byte GRH.
    assert_eq!(&recv.inner_mut()[40..40 + payload.len()], payload);
}
