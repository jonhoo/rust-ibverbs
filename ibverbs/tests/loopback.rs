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
    ibv_access_flags, ibv_advise_mr_advice, ibv_create_cq_wc_flags, ibv_port_state,
    ibv_qp_attr_mask, ibv_qp_state, ibv_qp_type, ibv_transport_type, AddressHandleAttribute,
    CompletionQueue, Context, Error, ProtectionDomain, QueuePair, QueuePairAttribute, RecvRequest,
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
        .create_cq(64)
        .build()
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
    // One-sided ops loop back to this same QP, so it must grant remote access (RC/UC only). RC also
    // serves the atomic loopback, which additionally requires remote-atomic access.
    if qp_type == ibv_qp_type::IBV_QPT_RC {
        builder.set_access(
            ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
                | ibv_access_flags::IBV_ACCESS_REMOTE_READ
                | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC,
        );
    } else {
        builder.allow_remote_rw();
    }

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

/// An owned snapshot of the completion fields the tests inspect (the borrowed `WorkCompletion`
/// cannot escape the poll, so `drain` copies out what it needs).
struct Completed {
    wr_id: u64,
    len: usize,
    imm_data: Option<u32>,
}

impl Completed {
    fn wr_id(&self) -> u64 {
        self.wr_id
    }
    fn len(&self) -> usize {
        self.len
    }
    fn imm_data(&self) -> Option<u32> {
        self.imm_data
    }
}

/// Poll until `n` completions arrive (asserting each succeeded) and return them. Panics after a few
/// seconds if they don't (loopback completes in microseconds).
fn drain(cq: &CompletionQueue, n: usize) -> Vec<Completed> {
    let mut observed = Vec::with_capacity(n);
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        if let Some(mut completions) = cq.poll().expect("failed to poll CQ") {
            while let Some(wc) = completions.next() {
                if let Err((status, vendor_err)) = wc.ok() {
                    panic!(
                        "work request {} failed: status {status:?}, vendor_err {vendor_err}",
                        wc.wr_id()
                    );
                }
                observed.push(Completed {
                    wr_id: wc.wr_id(),
                    len: wc.len(),
                    imm_data: wc.imm_data(),
                });
            }
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

/// Read the first 8 bytes of a buffer as a host-order `u64`.
fn first_u64(bytes: &[u8]) -> u64 {
    u64::from_ne_bytes(bytes[..8].try_into().unwrap())
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
    send.bytes_mut()[..5].copy_from_slice(b"hello");

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
    assert_eq!(&recv.bytes_mut()[..5], b"hello");
}

/// A larger transfer that spans multiple MTU-sized packets reports the right received byte length.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn send_recv_large() {
    let mut lb = loopback();

    const LEN: usize = 4096;
    let recv = lb.pd.allocate(LEN).expect("failed to register recv MR");
    let mut send = lb.pd.allocate(LEN).expect("failed to register send MR");
    for (i, b) in send.bytes_mut().iter_mut().enumerate() {
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
    assert_eq!(recv.bytes(), send.bytes());
}

/// Scatter-gather: a send gathers two non-contiguous source slices, and the receive scatters the
/// resulting contiguous message into two non-contiguous destination slices.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn scatter_gather() {
    let mut lb = loopback();

    let mut send = lb.pd.allocate(64).expect("failed to register send MR");
    let mut recv = lb.pd.allocate(64).expect("failed to register recv MR");
    send.bytes_mut()[0..4].copy_from_slice(b"AAAA");
    send.bytes_mut()[16..20].copy_from_slice(b"BBBB");

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
    assert_eq!(&recv.bytes_mut()[0..4], b"AAAA");
    assert_eq!(&recv.bytes_mut()[32..36], b"BBBB");
}

/// One-sided RDMA WRITE: the initiator writes directly into a remote memory region.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn rdma_write() {
    let mut lb = loopback();

    let mut src = lb.pd.allocate(64).expect("failed to register src MR");
    let mut dst = lb.pd.allocate(64).expect("failed to register dst MR");
    src.bytes_mut()[..6].copy_from_slice(b"verbs!");

    let remote = dst.remote().slice(..6);
    unsafe { lb.qp.post_write(&[src.slice(..6)], remote, 1, None) }.expect("post_write failed");

    let comps = drain(&lb.cq, 1);
    assert_eq!(comps[0].wr_id(), 1);
    assert_eq!(&dst.bytes_mut()[..6], b"verbs!");
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
    src.bytes_mut()[..4].copy_from_slice(&[1, 2, 3, 4]);

    // A write-with-immediate consumes a receive work request on the target queue pair.
    unsafe { lb.qp.post_receive(&[dummy.slice(..1)], 10) }.expect("post_receive failed");

    let imm = 0xdead_beef_u32;
    let remote = dst.remote().slice(..4);
    unsafe { lb.qp.post_write(&[src.slice(..4)], remote, 11, Some(imm)) }
        .expect("post_write failed");

    let comps = drain(&lb.cq, 2);
    assert_eq!(&dst.bytes_mut()[..4], &[1, 2, 3, 4]);
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
    remote_mr.bytes_mut()[..8].copy_from_slice(&[9, 8, 7, 6, 5, 4, 3, 2]);

    let remote = remote_mr.remote().slice(..8);
    unsafe { lb.qp.post_read(&[local.slice(..8)], remote, 1) }.expect("post_read failed");

    let comps = drain(&lb.cq, 1);
    assert_eq!(comps[0].wr_id(), 1);
    assert_eq!(&local.bytes_mut()[..8], &[9, 8, 7, 6, 5, 4, 3, 2]);
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
    payload.bytes_mut()[..3].copy_from_slice(&[42, 43, 44]);
    note.bytes_mut()[..2].copy_from_slice(&[1, 2]);

    unsafe { lb.qp.post_receive(&[recv.slice(..2)], 100) }.expect("post_receive failed");

    let payload_sge = [payload.slice(..3)];
    let note_sge = [note.slice(..2)];
    let remote = dst.remote().slice(..3);
    unsafe {
        let mut batch = lb.qp.start_send();
        batch.write(101, &payload_sge, remote);
        batch.signaled().send(102, &note_sge);
        batch.submit()
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
    assert_eq!(&dst.bytes_mut()[..3], &[42, 43, 44]);
    assert_eq!(&recv.bytes_mut()[..2], &[1, 2]);
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
        mr.bytes_mut()[0] = i as u8;
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
    send.bytes_mut()[..4].copy_from_slice(b"wait");

    unsafe { lb.qp.post_receive(&[recv.slice(..4)], 1) }.expect("post_receive failed");
    unsafe { lb.qp.post_send(&[send.slice(..4)], 2) }.expect("post_send failed");

    // Block on the completion channel instead of busy-polling.
    let mut ids = Vec::new();
    while ids.len() < 2 {
        let mut completions = lb
            .cq
            .wait(Some(Duration::from_secs(5)))
            .expect("wait failed");
        while let Some(wc) = completions.next() {
            assert!(wc.ok().is_ok(), "work request {} failed", wc.wr_id());
            ids.push(wc.wr_id());
        }
    }
    assert!(
        ids.contains(&1) && ids.contains(&2),
        "missing completions: {ids:?}"
    );
    assert_eq!(&recv.bytes_mut()[..4], b"wait");
}

/// An unreliable-connected (UC) queue pair carries SEND/RECV traffic to itself.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn unreliable_connection() {
    let mut lb = loopback_of(ibv_qp_type::IBV_QPT_UC);

    let mut recv = lb.pd.allocate(64).expect("failed to register recv MR");
    let mut send = lb.pd.allocate(64).expect("failed to register send MR");
    send.bytes_mut()[..3].copy_from_slice(b"ucq");

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
    assert_eq!(&recv.bytes_mut()[..3], b"ucq");
}

/// Shared receive queue: the queue pair draws its receive buffers from an SRQ rather than its own
/// receive queue. This exercises a feature the crate exposes that some alternatives do not.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn shared_receive_queue() {
    let ctx = open_test_device();

    let cq = ctx.create_cq(16).build().expect("failed to create CQ");
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
    send.bytes_mut()[..4].copy_from_slice(b"srq!");

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
    assert_eq!(&recv.bytes_mut()[..4], b"srq!");
}

/// Unreliable datagram (UD): a connectionless queue pair sends a datagram to itself via an address
/// handle pointing at its own GID. UD prepends a 40-byte GRH to received messages.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn unreliable_datagram() {
    let ctx = open_test_device();
    let cq = ctx.create_cq(16).build().expect("failed to create CQ");
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
    send.bytes_mut()[..payload.len()].copy_from_slice(payload);

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
    assert_eq!(&recv.bytes_mut()[40..40 + payload.len()], payload);
}

/// Atomic compare-and-swap and fetch-and-add against a remote 8-byte value on an RC queue pair.
///
/// RDMA atomics operate on 8-byte, 8-byte-aligned operands whose in-memory byte order is
/// implementation defined, so the value assertions accept either endianness.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn atomic_operations() {
    let mut lb = loopback();

    let mut target = lb.pd.allocate(8).expect("failed to register target MR");
    let mut local = lb.pd.allocate(8).expect("failed to register local MR");

    // The compare matches the zeroed target, so the swap takes effect and the original value (0) is
    // returned into `local`.
    let swapped = 0x0102_0304_0506_0708_u64;
    {
        let sg = [local.slice(..)];
        let remote = target.remote().slice(..);
        unsafe {
            let mut batch = lb.qp.start_send();
            batch.signaled().atomic_cmp_swap(1, &sg, remote, 0, swapped);
            batch.submit()
        }
        .expect("post atomic_cmp_swap failed");
    }
    assert_eq!(drain(&lb.cq, 1)[0].wr_id(), 1);
    assert_eq!(first_u64(local.bytes_mut()), 0, "CAS returns the old value");
    let stored = first_u64(target.bytes_mut());
    assert!(
        stored == swapped || stored == swapped.swap_bytes(),
        "CAS should have stored the swap value"
    );

    // A non-matching compare leaves the target untouched and returns the current value.
    {
        let sg = [local.slice(..)];
        let remote = target.remote().slice(..);
        unsafe {
            let mut batch = lb.qp.start_send();
            batch.signaled().atomic_cmp_swap(2, &sg, remote, 0, 0);
            batch.submit()
        }
        .expect("post atomic_cmp_swap failed");
    }
    assert_eq!(drain(&lb.cq, 1)[0].wr_id(), 2);
    assert_eq!(
        first_u64(target.bytes_mut()),
        stored,
        "CAS must not modify the target on a compare mismatch"
    );
    assert_eq!(
        first_u64(local.bytes_mut()),
        stored,
        "CAS returns the current value on a mismatch"
    );

    // Fetch-and-add on a fresh zeroed counter returns the old value and adds in place.
    let mut counter = lb.pd.allocate(8).expect("failed to register counter MR");
    {
        let sg = [local.slice(..)];
        let remote = counter.remote().slice(..);
        unsafe {
            let mut batch = lb.qp.start_send();
            batch.signaled().atomic_fetch_add(3, &sg, remote, 5);
            batch.submit()
        }
        .expect("post atomic_fetch_add failed");
    }
    assert_eq!(drain(&lb.cq, 1)[0].wr_id(), 3);
    assert_eq!(
        first_u64(local.bytes_mut()),
        0,
        "fetch-add returns the old value"
    );
    let sum = first_u64(counter.bytes_mut());
    assert!(
        sum == 5 || sum == 5u64.swap_bytes(),
        "fetch-add should add 5"
    );
}

/// `ibv_advise_mr` against a registered memory region. Prefetch advice is meant for on-demand-paging
/// MRs, which Soft-RoCE does not support, so the device may report the verb as unsupported; the
/// point of the test is that the dispatch path works and returns a result rather than crashing.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn advise_mr() {
    let lb = loopback();
    let mr = lb.pd.allocate(4096).expect("failed to register MR");
    let sg = [mr.slice(..)];

    match lb.pd.advise_mr(
        ibv_advise_mr_advice::IB_UVERBS_ADVISE_MR_ADVICE_PREFETCH,
        0,
        &sg,
    ) {
        // Either the device prefetched, or it does not implement advise_mr / on-demand paging.
        Ok(()) => {}
        Err(ibverbs::Error::Unsupported) => {}
        Err(e) => panic!("advise_mr returned an unexpected error: {e}"),
    }
}

/// Registering externally managed memory through the unsafe raw entry point, then using it for a
/// loopback transfer. The caller owns the buffers and keeps them alive past the regions.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn register_from_raw() {
    let mut lb = loopback();

    let mut send_buf = vec![0u8; 16];
    let mut recv_buf = vec![0u8; 16];
    send_buf[..6].copy_from_slice(b"extern");

    // SAFETY: both buffers outlive their regions (they are declared first, so dropped last) and are
    // never moved or resized while registered.
    let access = ibverbs::DEFAULT_ACCESS_FLAGS;
    let send_mr = unsafe {
        lb.pd
            .register_from_raw(send_buf.as_mut_ptr(), send_buf.len(), access)
    }
    .expect("register_from_raw send failed");
    let recv_mr = unsafe {
        lb.pd
            .register_from_raw(recv_buf.as_mut_ptr(), recv_buf.len(), access)
    }
    .expect("register_from_raw recv failed");

    unsafe { lb.qp.post_receive(&[recv_mr.slice(..6)], 1) }.expect("post_receive failed");
    unsafe { lb.qp.post_send(&[send_mr.slice(..6)], 2) }.expect("post_send failed");

    let comps = drain(&lb.cq, 2);
    assert!(comps.iter().any(|wc| wc.wr_id() == 1), "missing recv");
    assert!(comps.iter().any(|wc| wc.wr_id() == 2), "missing send");
    assert_eq!(&recv_buf[..6], b"extern");
}

/// Batched receive: two receives posted in one `post_recv` (caller-owned array) catch two sends, in
/// order, on the same RC queue pair.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn batched_recv() {
    let mut lb = loopback();

    let recv_a = lb.pd.allocate(64).expect("failed to register recv MR a");
    let recv_b = lb.pd.allocate(64).expect("failed to register recv MR b");
    let mut send_a = lb.pd.allocate(64).expect("failed to register send MR a");
    let mut send_b = lb.pd.allocate(64).expect("failed to register send MR b");
    send_a.bytes_mut()[..3].copy_from_slice(b"one");
    send_b.bytes_mut()[..3].copy_from_slice(b"two");

    // Post both receives in a single `ibv_post_recv` from a stack array (no allocation).
    let sg_a = [recv_a.slice(..3)];
    let sg_b = [recv_b.slice(..3)];
    unsafe {
        lb.qp
            .post_recv([RecvRequest::new(1, &sg_a), RecvRequest::new(2, &sg_b)])
    }
    .expect("post_recv failed");

    // RC is in order, so the first send fills the first receive and so on.
    unsafe { lb.qp.post_send(&[send_a.slice(..3)], 11) }.expect("post_send a failed");
    unsafe { lb.qp.post_send(&[send_b.slice(..3)], 12) }.expect("post_send b failed");

    let comps = drain(&lb.cq, 4);
    for id in [1, 2, 11, 12] {
        assert!(
            comps.iter().any(|c| c.wr_id() == id),
            "missing completion {id}"
        );
    }
    assert_eq!(&recv_a.bytes()[..3], b"one");
    assert_eq!(&recv_b.bytes()[..3], b"two");
}

/// Sends carrying the fence and solicited send flags complete successfully.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn send_flags() {
    let mut lb = loopback();

    let recv = lb.pd.allocate(64).expect("failed to register recv MR");
    let mut send = lb.pd.allocate(64).expect("failed to register send MR");
    send.bytes_mut()[..4].copy_from_slice(b"flag");

    let sg = [recv.slice(..4)];
    unsafe {
        lb.qp
            .post_recv([RecvRequest::new(1, &sg), RecvRequest::new(2, &sg)])
    }
    .expect("post_recv failed");

    let mut batch = lb.qp.start_send();
    // Fenced: ordered after any prior reads/atomics on this queue pair.
    batch.signaled().fenced().send(10, &[send.slice(..4)]);
    // Solicited: raises a solicited event on the receiver.
    batch.signaled().solicited().send(11, &[send.slice(..4)]);
    unsafe { batch.submit() }.expect("submit failed");

    let comps = drain(&lb.cq, 4);
    for id in [1, 2, 10, 11] {
        assert!(
            comps.iter().any(|c| c.wr_id() == id),
            "missing completion {id}"
        );
    }
    assert_eq!(&recv.bytes()[..4], b"flag");
}

/// `query_device` and `query_port` return sane device-wide and per-port attributes.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn query_device_and_port() {
    let ctx = open_test_device();

    let dev = ctx.query_device().expect("query_device failed");
    assert!(dev.max_qp > 0, "device should support queue pairs");
    assert!(
        dev.max_cqe > 0,
        "device should support completion queue entries"
    );
    assert!(
        dev.phys_port_cnt >= 1,
        "device should have at least one port"
    );

    // The device GUID is set via the typed accessor, and `Deref` still exposes the raw fields.
    assert!(
        !dev.node_guid().is_reserved(),
        "device should report a GUID"
    );

    let port = ctx.query_port(1).expect("query_port failed");
    assert!(
        port.gid_tbl_len > 0,
        "a RoCE port should expose a GID table"
    );
    // `open_test_device` only succeeds on an active port, so the typed state reflects that, and the
    // remaining typed accessors decode without panicking.
    assert!(matches!(
        port.state(),
        ibv_port_state::IBV_PORT_ACTIVE | ibv_port_state::IBV_PORT_ARMED
    ));
    let _ = port.active_mtu();
    let _ = port.active_speed();
    let _ = port.active_width();
    let _ = port.link_layer();
    let _ = port.phys_state();
}

/// The raw-handle escape hatches return the live, non-null FFI pointers for each resource.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn raw_handles() {
    let ctx = open_test_device();
    assert!(!ctx.as_raw().is_null());

    let pd = ctx.alloc_pd().expect("failed to allocate PD");
    assert!(!pd.as_raw().is_null());

    let cq = ctx.create_cq(16).build().expect("failed to create CQ");
    assert!(!cq.as_raw().is_null());
    assert!(!cq.as_raw_ex().is_null());
    // The plain and extended views are the same underlying completion queue.
    assert_eq!(cq.as_raw() as *const (), cq.as_raw_ex() as *const ());

    let mr = pd.allocate(64).expect("failed to register MR");
    assert!(!mr.as_raw().is_null());

    let srq = pd.create_srq(16, 1, 0).expect("failed to create SRQ");
    assert!(!srq.as_raw().is_null());

    // A UD queue pair plus an address handle to our own GID exercise the QP and AH accessors.
    let prepared = pd
        .create_qp(&cq, &cq, ibv_qp_type::IBV_QPT_UD)
        .expect("failed to create UD QP")
        .set_gid_index(1)
        .build()
        .expect("failed to build UD QP");
    let endpoint = prepared.endpoint().expect("failed to read endpoint");
    let qp = prepared
        .activate_ud(0x1234_5678)
        .expect("failed to activate UD QP");
    assert!(!qp.as_raw().is_null());
    assert!(!qp.as_raw_ex().is_null());

    let mut ah_attr = AddressHandleAttribute::new();
    ah_attr.set_grh(endpoint.gid.expect("RoCE requires a GID"), 1, 64, 0);
    let ah = pd
        .create_address_handle(&ah_attr)
        .expect("failed to create address handle");
    assert!(!ah.as_raw().is_null());
}

/// Inline SEND and inline RDMA WRITE deliver their payloads without a registered source region.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn inline_send() {
    let ctx = open_test_device();
    let cq = ctx.create_cq(16).build().expect("failed to create CQ");
    let pd = ctx.alloc_pd().expect("failed to allocate PD");

    let mut builder = pd
        .create_qp(&cq, &cq, ibv_qp_type::IBV_QPT_RC)
        .expect("failed to create RC QP");
    builder.set_gid_index(1).set_max_inline_data(64).set_access(
        ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_READ,
    );
    let prepared = builder.build().expect("failed to build QP");
    let endpoint = prepared.endpoint().expect("failed to read endpoint");
    let mut qp = prepared.handshake(endpoint).expect("failed to reach RTS");

    // Inline SEND: the payload lives only in this stack array, never in a registered MR.
    let recv = pd.allocate(64).expect("failed to register recv MR");
    unsafe { qp.post_receive(&[recv.slice(..5)], 1) }.expect("post_receive failed");
    let mut batch = qp.start_send();
    batch.signaled().send_inline(2, b"inrun");
    unsafe { batch.submit() }.expect("inline send submit failed");
    let comps = drain(&cq, 2);
    assert!(comps.iter().any(|c| c.wr_id() == 1), "missing recv");
    assert!(comps.iter().any(|c| c.wr_id() == 2), "missing send");
    assert_eq!(&recv.bytes()[..5], b"inrun");

    // Inline RDMA WRITE into a remote region.
    let dst = pd.allocate(64).expect("failed to register dst MR");
    let remote = dst.remote().slice(..4);
    let mut batch = qp.start_send();
    batch.signaled().write_inline(3, b"wxyz", remote);
    unsafe { batch.submit() }.expect("inline write submit failed");
    let comps = drain(&cq, 1);
    assert_eq!(comps[0].wr_id(), 3);
    assert_eq!(&dst.bytes()[..4], b"wxyz");
}

/// A queue pair created on an explicitly chosen port (port 1) connects and transfers data. The test
/// device has a single port, so this exercises the port-threading path rather than a second port.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn queue_pair_on_explicit_port() {
    let ctx = open_test_device();
    let cq = ctx.create_cq(16).build().expect("failed to create CQ");
    let pd = ctx.alloc_pd().expect("failed to allocate PD");

    let mut builder = pd
        .create_qp_on_port(&cq, &cq, ibv_qp_type::IBV_QPT_RC, 1)
        .expect("failed to create QP on port 1");
    builder.set_gid_index(1).set_access(
        ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_READ,
    );
    let prepared = builder.build().expect("failed to build QP");
    let endpoint = prepared.endpoint().expect("failed to read endpoint");
    let mut qp = prepared.handshake(endpoint).expect("failed to reach RTS");

    let recv = pd.allocate(64).expect("failed to register recv MR");
    let mut send = pd.allocate(64).expect("failed to register send MR");
    send.bytes_mut()[..4].copy_from_slice(b"port");

    unsafe { qp.post_receive(&[recv.slice(..4)], 1) }.expect("post_receive failed");
    unsafe { qp.post_send(&[send.slice(..4)], 2) }.expect("post_send failed");

    let comps = drain(&cq, 2);
    assert!(comps.iter().any(|c| c.wr_id() == 1), "missing recv");
    assert!(comps.iter().any(|c| c.wr_id() == 2), "missing send");
    assert_eq!(&recv.bytes()[..4], b"port");
}

/// The device clock and per-completion hardware timestamps, where the device supports them.
///
/// Both are optional features (Soft-RoCE, for instance, supports neither), so the test skips the
/// parts the device does not implement rather than failing.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn completion_timestamps() {
    let ctx = open_test_device();

    match ctx.query_rt_values_ex() {
        Ok(_clock) => {}
        Err(ibverbs::Error::Unsupported) => {
            eprintln!("device does not support query_rt_values_ex; skipping that check");
        }
        Err(e) => panic!("query_rt_values_ex failed: {e}"),
    }

    let cq = match ctx
        .create_cq(16)
        .set_wc_flags(ibv_create_cq_wc_flags::IBV_WC_EX_WITH_COMPLETION_TIMESTAMP)
        .build()
    {
        Ok(cq) => cq,
        Err(ibverbs::Error::Unsupported) => {
            eprintln!("device does not support completion timestamps; skipping");
            return;
        }
        Err(e) => panic!("create_cq with timestamps failed: {e}"),
    };
    let pd = ctx.alloc_pd().expect("failed to allocate PD");
    let mut builder = pd
        .create_qp(&cq, &cq, ibv_qp_type::IBV_QPT_RC)
        .expect("failed to create QP");
    builder.set_gid_index(1).set_access(
        ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_READ,
    );
    let prepared = builder.build().expect("failed to build QP");
    let endpoint = prepared.endpoint().expect("failed to read endpoint");
    let mut qp = prepared.handshake(endpoint).expect("failed to reach RTS");

    let recv = pd.allocate(64).expect("failed to register recv MR");
    let mut send = pd.allocate(64).expect("failed to register send MR");
    send.bytes_mut()[..4].copy_from_slice(b"time");
    unsafe { qp.post_receive(&[recv.slice(..4)], 1) }.expect("post_receive failed");
    unsafe { qp.post_send(&[send.slice(..4)], 2) }.expect("post_send failed");

    // Each completion carries a hardware timestamp; reading it must succeed (not panic).
    let mut seen = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    while seen < 2 {
        if let Some(mut comps) = cq.poll().expect("failed to poll CQ") {
            while let Some(wc) = comps.next() {
                wc.ok().expect("work request failed");
                let _ts = wc.completion_timestamp();
                seen += 1;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for completions"
        );
    }
}

/// The extended work-completion accessors: the always-available GRH flag, plus the optional
/// SL / source-LID / DLID-path-bits fields requested through the completion-queue builder. The
/// addressing fields are optional, so the test skips them on a device that cannot request them.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn extended_wc_fields() {
    let ctx = open_test_device();

    let cq = match ctx
        .create_cq(16)
        .set_wc_flags(
            ibv_create_cq_wc_flags::IBV_WC_EX_WITH_SLID
                | ibv_create_cq_wc_flags::IBV_WC_EX_WITH_SL
                | ibv_create_cq_wc_flags::IBV_WC_EX_WITH_DLID_PATH_BITS,
        )
        .build()
    {
        Ok(cq) => cq,
        Err(ibverbs::Error::Unsupported) => {
            eprintln!("device does not support these completion fields; skipping");
            return;
        }
        Err(e) => panic!("create_cq failed: {e}"),
    };

    let pd = ctx.alloc_pd().expect("failed to allocate PD");
    let mut builder = pd
        .create_qp(&cq, &cq, ibv_qp_type::IBV_QPT_RC)
        .expect("failed to create QP");
    builder.set_gid_index(1).set_access(
        ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_READ,
    );
    let prepared = builder.build().expect("failed to build QP");
    let endpoint = prepared.endpoint().expect("failed to read endpoint");
    let mut qp = prepared.handshake(endpoint).expect("failed to reach RTS");

    let recv = pd.allocate(64).expect("failed to register recv MR");
    let mut send = pd.allocate(64).expect("failed to register send MR");
    send.bytes_mut()[..4].copy_from_slice(b"wcfl");
    unsafe { qp.post_receive(&[recv.slice(..4)], 1) }.expect("post_receive failed");
    unsafe { qp.post_send(&[send.slice(..4)], 2) }.expect("post_send failed");

    // Exercise the accessors on each completion. The addressing values are device-defined (and zero
    // on RoCE), so just ensure the reads succeed; an RC completion never carries a GRH.
    let mut seen = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    while seen < 2 {
        if let Some(mut comps) = cq.poll().expect("failed to poll CQ") {
            while let Some(wc) = comps.next() {
                wc.ok().expect("work request failed");
                let _ = wc.wc_flags();
                let _ = wc.has_grh();
                let _ = wc.slid();
                let _ = wc.sl();
                let _ = wc.dlid_path_bits();
                seen += 1;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for completions"
        );
    }
}

/// The completion channel exposes a descriptor and arm/consume hooks for event-driven polling.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn event_driven_completion() {
    use std::os::fd::AsRawFd;

    let mut lb = loopback();
    // The completion channel is backed by a real (non-blocking) descriptor an event loop can wait on.
    assert!(
        lb.cq.as_raw_fd() >= 0,
        "completion channel should expose an fd"
    );

    let mut recv = lb.pd.allocate(64).expect("failed to register recv MR");
    let mut send = lb.pd.allocate(64).expect("failed to register send MR");
    send.bytes_mut()[..5].copy_from_slice(b"hello");

    // Arm the channel before posting so the completions raise a notification on the descriptor.
    lb.cq
        .req_notify(false)
        .expect("failed to arm the completion channel");
    unsafe { lb.qp.post_receive(&[recv.slice(..5)], 1) }.expect("post_receive failed");
    unsafe { lb.qp.post_send(&[send.slice(..5)], 2) }.expect("post_send failed");

    // A real event loop would await readability of the descriptor; here we consume the notification
    // as soon as it arrives. `get_event` reads the (non-blocking) channel and acknowledges for us.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut notified = false;
    while Instant::now() < deadline {
        if lb.cq.get_event().expect("get_event failed") {
            notified = true;
            break;
        }
        std::thread::yield_now();
    }
    assert!(
        notified,
        "expected a completion notification on the channel"
    );

    // The completions themselves are drained through the normal poll path.
    let comps = drain(&lb.cq, 2);
    assert!(
        comps.iter().any(|wc| wc.wr_id() == 1),
        "missing recv completion"
    );
    assert!(
        comps.iter().any(|wc| wc.wr_id() == 2),
        "missing send completion"
    );
    assert_eq!(&recv.bytes_mut()[..5], b"hello");
}

/// Device transport type, single-GID query, and GID net-device names are all reported.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn gid_and_device_introspection() {
    // Select the same device `open_test_device` would, so we can read its transport type first.
    let devices = ibverbs::devices().expect("failed to list RDMA devices");
    let device = match std::env::var("IBVERBS_TEST_DEVICE") {
        Ok(name) if !name.is_empty() => devices
            .iter()
            .find(|d| d.name().is_some_and(|n| n.to_bytes() == name.as_bytes()))
            .expect("IBVERBS_TEST_DEVICE is not among the available devices"),
        _ => devices.iter().next().expect("no RDMA device available"),
    };
    // RoCE (including Soft-RoCE) presents the InfiniBand transport.
    assert_eq!(
        device.transport_type(),
        ibv_transport_type::IBV_TRANSPORT_IB
    );

    let ctx = device.open().expect("failed to open the RDMA device");

    // A single GID query returns the same GID as the matching full-table entry.
    let table = ctx.gid_table().expect("failed to read GID table");
    let entry = table.first().expect("expected at least one GID entry");
    let gid = ctx
        .query_gid(entry.port_num as u8, entry.gid_index)
        .expect("query_gid failed");
    assert_eq!(gid, entry.gid);

    // Every GID bound to a net device resolves to that device's name.
    for entry in &table {
        if entry.ndev_ifindex != 0 {
            assert!(
                entry.netdev_name().is_some(),
                "expected a net-device name for ifindex {}",
                entry.ndev_ifindex
            );
        }
    }
}

#[test]
#[ignore]
fn modify_and_query_queue_pair() {
    // `loopback()` builds an RC queue pair and drives it to RTS via `handshake`. The general
    // `query`/`modify` API then lets us inspect and change it afterwards.
    let mut lb = loopback();

    // Query the attributes the handshake negotiated. Because the queue pair is connected to its own
    // endpoint, its destination QP number is its own.
    let mask = ibv_qp_attr_mask::IBV_QP_STATE
        | ibv_qp_attr_mask::IBV_QP_CUR_STATE
        | ibv_qp_attr_mask::IBV_QP_DEST_QPN
        | ibv_qp_attr_mask::IBV_QP_SQ_PSN;
    let (attr, init) = lb.qp.query(mask).expect("failed to query the queue pair");
    assert_eq!(attr.state(), ibv_qp_state::IBV_QPS_RTS);
    assert_eq!(attr.dest_qp_num(), lb.qp.qp_num());
    assert!(init.max_send_wr() >= 16);

    // An illegal transition (RTS -> INIT) is reported precisely.
    let mut to_init = QueuePairAttribute::new();
    to_init.set_state(ibv_qp_state::IBV_QPS_INIT);
    match lb.qp.modify(&to_init) {
        Err(Error::InvalidQueuePairTransition { current, next }) => {
            assert_eq!(current, ibv_qp_state::IBV_QPS_RTS);
            assert_eq!(next, ibv_qp_state::IBV_QPS_INIT);
        }
        other => panic!("expected InvalidQueuePairTransition, got {other:?}"),
    }

    // A legal transition (any state -> ERR) succeeds, and the change is visible to a later query.
    let mut to_err = QueuePairAttribute::new();
    to_err.set_state(ibv_qp_state::IBV_QPS_ERR);
    lb.qp
        .modify(&to_err)
        .expect("failed to move the queue pair to ERR");
    let (attr, _) = lb
        .qp
        .query(ibv_qp_attr_mask::IBV_QP_STATE)
        .expect("failed to re-query the queue pair");
    assert_eq!(attr.state(), ibv_qp_state::IBV_QPS_ERR);
}

/// `into_queue_pair` plus `modify` lets you drive a queue pair through its states by hand, the raw
/// path that `activate_ud` wraps.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn manual_bringup_via_modify() {
    let ctx = open_test_device();
    let cq = ctx.create_cq(16).build().expect("failed to create CQ");
    let pd = ctx.alloc_pd().expect("failed to allocate PD");

    // Build a UD queue pair but do not activate it; take the still-RESET queue pair to drive by hand.
    let prepared = pd
        .create_qp(&cq, &cq, ibv_qp_type::IBV_QPT_UD)
        .expect("failed to create QP")
        .build()
        .expect("failed to build QP");
    let mut qp = prepared.into_queue_pair();

    const QKEY: u32 = 0x1111_1111;

    // RESET -> INIT: associate the port, partition key, and Q_Key.
    let mut init = QueuePairAttribute::new();
    init.set_state(ibv_qp_state::IBV_QPS_INIT)
        .set_pkey_index(0)
        .set_port(1)
        .set_qkey(QKEY);
    qp.modify(&init).expect("RESET -> INIT failed");

    // INIT -> RTR.
    let mut rtr = QueuePairAttribute::new();
    rtr.set_state(ibv_qp_state::IBV_QPS_RTR);
    qp.modify(&rtr).expect("INIT -> RTR failed");

    // RTR -> RTS.
    let mut rts = QueuePairAttribute::new();
    rts.set_state(ibv_qp_state::IBV_QPS_RTS).set_sq_psn(0);
    qp.modify(&rts).expect("RTR -> RTS failed");

    // The hand-driven queue pair reached RTS with the Q_Key we set.
    let (attr, _) = qp
        .query(ibv_qp_attr_mask::IBV_QP_STATE | ibv_qp_attr_mask::IBV_QP_QKEY)
        .expect("query failed");
    assert_eq!(attr.state(), ibv_qp_state::IBV_QPS_RTS);
    assert_eq!(attr.qkey(), QKEY);
}

/// Gathered inline SEND and inline RDMA WRITE assemble their payloads from several buffers, with no
/// registered source region.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn inline_send_list() {
    use std::io::IoSlice;

    let ctx = open_test_device();
    let cq = ctx.create_cq(16).build().expect("failed to create CQ");
    let pd = ctx.alloc_pd().expect("failed to allocate PD");

    let mut builder = pd
        .create_qp(&cq, &cq, ibv_qp_type::IBV_QPT_RC)
        .expect("failed to create RC QP");
    builder.set_gid_index(1).set_max_inline_data(64).set_access(
        ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_READ,
    );
    let prepared = builder.build().expect("failed to build QP");
    let endpoint = prepared.endpoint().expect("failed to read endpoint");
    let mut qp = prepared.handshake(endpoint).expect("failed to reach RTS");

    // Gathered inline SEND: the payload is assembled from three separate stack buffers.
    let recv = pd.allocate(64).expect("failed to register recv MR");
    unsafe { qp.post_receive(&[recv.slice(..9)], 1) }.expect("post_receive failed");
    let bufs = [
        IoSlice::new(b"ab"),
        IoSlice::new(b"cde"),
        IoSlice::new(b"fghi"),
    ];
    let mut batch = qp.start_send();
    batch.signaled().send_inline_list(2, &bufs);
    unsafe { batch.submit() }.expect("inline send-list submit failed");
    let comps = drain(&cq, 2);
    let recv_len = comps
        .iter()
        .find(|c| c.wr_id() == 1)
        .expect("missing recv")
        .len();
    assert!(comps.iter().any(|c| c.wr_id() == 2), "missing send");

    // Soft-RoCE's `wr_set_inline_data_list` copies the payload but forgets to accumulate the total
    // length (providers/rxe/rxe.c omits `tot_length += length`), so it transmits a zero-length
    // message. The gather call and the completions still succeed, which is what this exercises on
    // rxe; only check the delivered bytes on a provider that reports the real length.
    if recv_len == 0 {
        return;
    }
    assert_eq!(
        recv_len, 9,
        "gathered inline send delivered a partial payload"
    );
    assert_eq!(&recv.bytes()[..9], b"abcdefghi");

    // Gathered inline RDMA WRITE into a remote region.
    let dst = pd.allocate(64).expect("failed to register dst MR");
    let remote = dst.remote().slice(..6);
    let parts = [IoSlice::new(b"uvw"), IoSlice::new(b"xyz")];
    let mut batch = qp.start_send();
    batch.signaled().write_inline_list(3, &parts, remote);
    unsafe { batch.submit() }.expect("inline write-list submit failed");
    let comps = drain(&cq, 1);
    assert_eq!(comps[0].wr_id(), 3);
    assert_eq!(&dst.bytes()[..6], b"uvwxyz");
}

/// The extended device query returns at least the base attributes, and its typed accessors decode
/// without failing even on a provider (such as Soft-RoCE) that lacks the extended verb.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn query_device_extended() {
    let ctx = open_test_device();

    let basic = ctx.query_device().expect("query_device failed");
    let ex = ctx.query_device_ex().expect("query_device_ex failed");

    // The base attributes carried by `orig()` match the plain query.
    assert_eq!(ex.orig().node_guid(), basic.node_guid());
    assert_eq!(ex.node_guid(), basic.node_guid());
    assert_eq!(ex.orig().max_qp, basic.max_qp);

    // The extended accessors decode without panicking; they read back zero on a provider that does
    // not implement the extended verb (the C inline's legacy fallback fills only the base fields).
    let _ = ex.completion_timestamp_mask();
    let _ = ex.hca_core_clock_khz();
    let _ = ex.pci_atomic_caps();
    let _ = ex.packet_pacing_caps();
    let _ = ex.raw_packet_caps();
    let _ = ex.max_device_memory();
}

/// The `Debug` impls on `Device`, `DeviceAttr`, and `PortAttr` render the device's real attributes,
/// and `fw_ver` decodes the firmware string without the caller touching raw pointers.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn debug_formatting() {
    let devices = ibverbs::devices().expect("failed to list RDMA devices");
    let device = match std::env::var("IBVERBS_TEST_DEVICE") {
        Ok(name) if !name.is_empty() => devices
            .iter()
            .find(|d| d.name().is_some_and(|n| n.to_bytes() == name.as_bytes()))
            .expect("IBVERBS_TEST_DEVICE is not among the available devices"),
        _ => devices.iter().next().expect("no RDMA device available"),
    };
    assert!(format!("{device:?}").contains("Device"), "{device:?}");

    let ctx = device.open().expect("failed to open the RDMA device");
    let attr = ctx.query_device().expect("query_device failed");
    let attr_dbg = format!("{attr:?}");
    assert!(
        attr_dbg.contains("DeviceAttr") && attr_dbg.contains("fw_ver"),
        "{attr_dbg}"
    );
    let _ = attr.fw_ver();

    let port = ctx.query_port(1).expect("query_port failed");
    assert!(format!("{port:?}").contains("PortAttr"), "{port:?}");
}
