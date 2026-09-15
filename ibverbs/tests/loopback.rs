//! Integration tests for the RDMA data path.
//!
//! Most tests self-loop a single queue pair (connected to its own endpoint), so they need only one
//! host and no peer, which makes the data path testable with Soft-RoCE (rxe).
//!
//! The tests require a real RDMA device, so they are `#[ignore]`d: plain `cargo test` skips them
//! (but still compile-checks them); `cargo test -- --ignored` runs them. They use the first
//! available device, or `IBVERBS_TEST_DEVICE` (e.g. `rxe0`) if set.

use std::time::{Duration, Instant};

use ibverbs::{
    AccessFlags, AddressHandleAttribute, CompletionQueue, Connected, Context, Error, Grh, MrAdvice,
    MrAdviseFlags, Payload, PortState, ProtectionDomain, QueuePair, QueuePairAttribute,
    QueuePairAttributeMask, QueuePairState, Rc, RecvRequest, RnrTimer, SendOps, TransportType, Uc,
    Ud, WcFields,
};

/// A queue pair connected to itself, with the resources it uses.
struct Loopback<T: Connected> {
    qp: QueuePair<T>,
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

/// The GID index the tests route from on port 1: the device's routable entry (its IPv4 RoCE v2
/// one on Soft-RoCE).
fn gid_index(ctx: &Context) -> u32 {
    ctx.routable_gid(1)
        .expect("failed to read the GID table")
        .expect("no GID on port 1")
        .gid_index
}

/// Build a self-connected queue pair of the given connected transport, with generous queue/SGE
/// limits and the given remote-access grants so the tests can post batches, multi-SGE lists, and
/// one-sided operations (which loop back to this same QP).
fn loopback_of<T: Connected>(access: AccessFlags) -> Loopback<T> {
    let ctx = open_test_device();
    let cq = ctx
        .create_cq(64)
        .build()
        .expect("failed to create completion queue");
    let pd = ctx
        .alloc_pd()
        .expect("failed to allocate protection domain");

    let mut builder = pd
        .create_qp::<T>(&cq, &cq, 1)
        .expect("failed to create queue pair");
    builder
        .set_gid_index(gid_index(&ctx))
        .set_max_send_wr(16)
        .set_max_recv_wr(16)
        .set_max_send_sge(4)
        .set_max_recv_sge(4)
        .set_access(access);

    let prepared = builder.build().expect("failed to build queue pair");
    let endpoint = prepared.endpoint().expect("failed to read local endpoint");
    let qp = prepared
        .handshake(endpoint)
        .expect("failed to transition queue pair to RTS");

    Loopback { qp, cq, pd }
}

/// A reliable-connected self-loopback queue pair (the common case). Besides remote read/write, it
/// grants remote-atomic access, which the atomic loopback additionally requires (RC only).
fn loopback() -> Loopback<Rc> {
    loopback_of::<Rc>(
        AccessFlags::LOCAL_WRITE
            | AccessFlags::REMOTE_WRITE
            | AccessFlags::REMOTE_READ
            | AccessFlags::REMOTE_ATOMIC,
    )
}

/// Build a reliable-connected self-loopback queue pair on a caller-provided protection domain and
/// completion queue (rather than fresh ones), so several queue pairs can share resources — used to
/// test a completion channel shared across queues.
fn loopback_on(pd: &ProtectionDomain, cq: &CompletionQueue, gid_index: u32) -> QueuePair {
    let mut builder = pd
        .create_qp::<Rc>(cq, cq, 1)
        .expect("failed to create queue pair");
    builder
        .set_gid_index(gid_index)
        .set_max_send_wr(16)
        .set_max_recv_wr(16)
        .set_max_send_sge(4)
        .set_max_recv_sge(4)
        .set_access(
            AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE | AccessFlags::REMOTE_READ,
        );
    let prepared = builder.build().expect("failed to build queue pair");
    let endpoint = prepared.endpoint().expect("failed to read local endpoint");
    prepared
        .handshake(endpoint)
        .expect("failed to transition queue pair to RTS")
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
        let mut completions = cq.poll().expect("failed to poll CQ");
        while let Some(wc) = completions.next() {
            if let Err(e) = wc.ok() {
                panic!("work request {} failed: {e}", wc.wr_id());
            }
            observed.push(Completed {
                wr_id: wc.wr_id(),
                len: wc.len(),
                imm_data: wc.imm_data(),
            });
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

/// Like [`drain`], but through the standard `ibv_poll_cq` batch path ([`CompletionQueue::poll_into`])
/// instead of the extended lazy-read path, so the tests cover both interfaces.
fn drain_into(cq: &CompletionQueue, n: usize) -> Vec<Completed> {
    let mut observed = Vec::with_capacity(n);
    let mut wc = vec![ibverbs::ffi::ibv_wc::default(); n];
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        for completion in cq.poll_into(&mut wc).expect("failed to poll CQ") {
            if let Some((status, vendor_err)) = completion.error() {
                panic!(
                    "work request {} failed: {status:?} (vendor {vendor_err})",
                    completion.wr_id()
                );
            }
            observed.push(Completed {
                wr_id: completion.wr_id(),
                len: completion.len(),
                imm_data: completion.imm_data(),
            });
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

    let mut recv = lb
        .pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register recv MR");
    let mut send = lb
        .pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register send MR");
    send.bytes_mut()[..5].copy_from_slice(b"hello");

    unsafe { lb.qp.post_recv([RecvRequest::new(1, &[recv.slice(..5)])]) }
        .expect("post_recv failed");
    let mut batch = lb.qp.start_send();
    batch.op().signaled().send(2, &[send.slice(..5)]);
    unsafe { batch.submit() }.expect("send failed");

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

/// The standard `ibv_poll_cq` batch path drains the same SEND / RECV completions as the extended
/// interface, reporting matching `wr_id`s and byte lengths.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn send_recv_poll_into() {
    let mut lb = loopback();

    let mut recv = lb
        .pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register recv MR");
    let mut send = lb
        .pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register send MR");
    send.bytes_mut()[..5].copy_from_slice(b"hello");

    unsafe { lb.qp.post_recv([RecvRequest::new(1, &[recv.slice(..5)])]) }
        .expect("post_recv failed");
    let mut batch = lb.qp.start_send();
    batch.op().signaled().send(2, &[send.slice(..5)]);
    unsafe { batch.submit() }.expect("send failed");

    let comps = drain_into(&lb.cq, 2);
    assert!(
        comps.iter().any(|wc| wc.wr_id() == 1),
        "missing recv completion"
    );
    assert!(
        comps.iter().any(|wc| wc.wr_id() == 2),
        "missing send completion"
    );
    let recv_wc = comps
        .iter()
        .find(|wc| wc.wr_id() == 1)
        .expect("missing recv completion");
    assert_eq!(recv_wc.len(), 5, "received byte length mismatch");
    assert_eq!(&recv.bytes_mut()[..5], b"hello");
}

/// Polling into an empty completion queue returns an empty slice rather than blocking or erroring.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn poll_into_empty_is_empty() {
    let lb = loopback();
    let mut wc = [ibverbs::ffi::ibv_wc::default(); 8];
    let ready = lb.cq.poll_into(&mut wc).expect("failed to poll CQ");
    assert!(ready.is_empty(), "no completions were posted");
}

/// A larger transfer that spans multiple MTU-sized packets reports the right received byte length.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn send_recv_large() {
    let mut lb = loopback();

    const LEN: usize = 4096;
    let recv = lb
        .pd
        .allocate(LEN, AccessFlags::PERMISSIVE)
        .expect("failed to register recv MR");
    let mut send = lb
        .pd
        .allocate(LEN, AccessFlags::PERMISSIVE)
        .expect("failed to register send MR");
    for (i, b) in send.bytes_mut().iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }

    unsafe { lb.qp.post_recv([RecvRequest::new(1, &[recv.slice(..)])]) }.expect("post_recv failed");
    let mut batch = lb.qp.start_send();
    batch.op().signaled().send(2, &[send.slice(..)]);
    unsafe { batch.submit() }.expect("send failed");

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

    let mut send = lb
        .pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register send MR");
    let mut recv = lb
        .pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register recv MR");
    send.bytes_mut()[0..4].copy_from_slice(b"AAAA");
    send.bytes_mut()[16..20].copy_from_slice(b"BBBB");

    unsafe {
        lb.qp
            .post_recv([RecvRequest::new(1, &[recv.slice(0..4), recv.slice(32..36)])])
    }
    .expect("post_recv failed");
    let mut batch = lb.qp.start_send();
    batch
        .op()
        .signaled()
        .send(2, &[send.slice(0..4), send.slice(16..20)]);
    unsafe { batch.submit() }.expect("send failed");

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

    let mut src = lb
        .pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register src MR");
    let mut dst = lb
        .pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register dst MR");
    src.bytes_mut()[..6].copy_from_slice(b"verbs!");

    let remote = dst.remote().slice(..6);
    let mut batch = lb.qp.start_send();
    batch.op().signaled().write(1, &[src.slice(..6)], remote);
    unsafe { batch.submit() }.expect("write failed");

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

    let mut src = lb
        .pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register src MR");
    let mut dst = lb
        .pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register dst MR");
    let dummy = lb
        .pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register dummy MR");
    src.bytes_mut()[..4].copy_from_slice(&[1, 2, 3, 4]);

    // A write-with-immediate consumes a receive work request on the target queue pair.
    unsafe { lb.qp.post_recv([RecvRequest::new(10, &[dummy.slice(..1)])]) }
        .expect("post_recv failed");

    let imm = 0xdead_beef_u32;
    let remote = dst.remote().slice(..4);
    let mut batch = lb.qp.start_send();
    batch
        .op()
        .signaled()
        .imm(imm)
        .write(11, &[src.slice(..4)], remote);
    unsafe { batch.submit() }.expect("write failed");

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

    let mut remote_mr = lb
        .pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register remote MR");
    let mut local = lb
        .pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register local MR");
    remote_mr.bytes_mut()[..8].copy_from_slice(&[9, 8, 7, 6, 5, 4, 3, 2]);

    let remote = remote_mr.remote().slice(..8);
    let mut batch = lb.qp.start_send();
    batch.op().signaled().read(1, &[local.slice(..8)], remote);
    unsafe { batch.submit() }.expect("read failed");

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

    let mut payload = lb
        .pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register payload MR");
    let mut dst = lb
        .pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register dst MR");
    let mut note = lb
        .pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register note MR");
    let mut recv = lb
        .pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register recv MR");
    payload.bytes_mut()[..3].copy_from_slice(&[42, 43, 44]);
    note.bytes_mut()[..2].copy_from_slice(&[1, 2]);

    unsafe { lb.qp.post_recv([RecvRequest::new(100, &[recv.slice(..2)])]) }
        .expect("post_recv failed");

    let payload_sge = [payload.slice(..3)];
    let note_sge = [note.slice(..2)];
    let remote = dst.remote().slice(..3);
    unsafe {
        let mut batch = lb.qp.start_send();
        batch.op().write(101, &payload_sge, remote);
        batch.op().signaled().send(102, &note_sge);
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
        let mr = lb
            .pd
            .allocate(8, AccessFlags::PERMISSIVE)
            .expect("failed to register recv MR");
        unsafe {
            lb.qp
                .post_recv([RecvRequest::new(1000 + i, &[mr.slice(..8)])])
        }
        .expect("post_recv failed");
        recv_mrs.push(mr);
    }
    let mut send_mrs = Vec::new();
    for i in 0..N {
        let mut mr = lb
            .pd
            .allocate(8, AccessFlags::PERMISSIVE)
            .expect("failed to register send MR");
        mr.bytes_mut()[0] = i as u8;
        let mut batch = lb.qp.start_send();
        batch.op().signaled().send(i, &[mr.slice(..8)]);
        unsafe { batch.submit() }.expect("send failed");
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

/// Blocking on a completion channel (`CompletionChannel::wait`) delivers completions without
/// busy-polling: arm the queue, drain it, and only then block on the channel.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn wait_for_completion() {
    const CQ_CONTEXT: u64 = 7;

    let ctx = open_test_device();
    let channel = ctx
        .create_comp_channel()
        .expect("failed to create completion channel");
    let cq = ctx
        .create_cq(64)
        .set_comp_channel(&channel)
        .set_context(CQ_CONTEXT)
        .build()
        .expect("failed to create completion queue");
    let pd = ctx
        .alloc_pd()
        .expect("failed to allocate protection domain");
    let mut qp = loopback_on(&pd, &cq, gid_index(&ctx));

    let mut recv = pd
        .allocate(16, AccessFlags::PERMISSIVE)
        .expect("failed to register recv MR");
    let mut send = pd
        .allocate(16, AccessFlags::PERMISSIVE)
        .expect("failed to register send MR");
    send.bytes_mut()[..4].copy_from_slice(b"wait");

    unsafe { qp.post_recv([RecvRequest::new(1, &[recv.slice(..4)])]) }.expect("post_recv failed");
    let mut batch = qp.start_send();
    batch.op().signaled().send(2, &[send.slice(..4)]);
    unsafe { batch.submit() }.expect("send failed");

    let mut ids = Vec::new();
    while ids.len() < 2 {
        // Arm first, then drain: polling after arming closes the race where a completion lands
        // between the drain and the arm (its notification then just wakes the wait immediately).
        cq.req_notify(false).expect("failed to arm");
        let drained = ids.len();
        let mut completions = cq.poll().expect("poll failed");
        while let Some(wc) = completions.next() {
            assert!(wc.ok().is_ok(), "work request {} failed", wc.wr_id());
            ids.push(wc.wr_id());
        }
        // Release the poll before blocking: a live `Completions` holds the queue's poll lock.
        drop(completions);
        if ids.len() > drained {
            continue;
        }
        match channel
            .wait(Some(Duration::from_secs(5)))
            .expect("wait failed")
        {
            Some(context) => assert_eq!(context, CQ_CONTEXT),
            None => panic!("timed out waiting for completions: {ids:?}"),
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
    let mut lb = loopback_of::<Uc>(
        AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE | AccessFlags::REMOTE_READ,
    );

    let mut recv = lb
        .pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register recv MR");
    let mut send = lb
        .pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register send MR");
    send.bytes_mut()[..3].copy_from_slice(b"ucq");

    unsafe { lb.qp.post_recv([RecvRequest::new(1, &[recv.slice(..3)])]) }
        .expect("post_recv failed");
    let mut batch = lb.qp.start_send();
    batch.op().signaled().send(2, &[send.slice(..3)]);
    unsafe { batch.submit() }.expect("send failed");

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
/// receive queue.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn shared_receive_queue() {
    let ctx = open_test_device();

    let cq = ctx.create_cq(16).build().expect("failed to create CQ");
    let pd = ctx.alloc_pd().expect("failed to allocate PD");
    let srq = pd.create_srq(16, 1, 0).expect("failed to create SRQ");

    let prepared = pd
        .create_qp::<Rc>(&cq, &cq, 1)
        .expect("failed to create QP")
        .set_gid_index(gid_index(&ctx))
        .set_srq(&srq)
        .build()
        .expect("failed to build QP");
    let endpoint = prepared.endpoint().expect("failed to read endpoint");
    let mut qp = prepared.handshake(endpoint).expect("failed to connect QP");

    let mut recv = pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register recv MR");
    let mut send = pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register send MR");
    send.bytes_mut()[..4].copy_from_slice(b"srq!");

    // Receives go to the SRQ, not the queue pair's own receive queue.
    unsafe { srq.post_recv([RecvRequest::new(1, &[recv.slice(..4)])]) }
        .expect("SRQ post_recv failed");
    let mut batch = qp.start_send();
    batch.op().signaled().send(2, &[send.slice(..4)]);
    unsafe { batch.submit() }.expect("send failed");

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

/// Asynchronous device events on a quiet context: the descriptor is exposed, the non-blocking
/// poll reports nothing pending, and the bounded wait times out instead of hanging.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn async_events_quiet_context() {
    use std::os::fd::AsRawFd;

    let ctx = open_test_device();
    assert!(
        ctx.async_fd().as_raw_fd() >= 0,
        "the context exposes its asynchronous-event descriptor"
    );
    assert!(
        ctx.poll_async_event()
            .expect("failed to poll for an async event")
            .is_none(),
        "a quiet context has no pending async event"
    );
    let before = Instant::now();
    assert!(
        ctx.wait_async_event(Some(Duration::from_millis(50)))
            .expect("failed to wait for an async event")
            .is_none(),
        "waiting on a quiet context times out with None"
    );
    // poll(2) has millisecond granularity, so allow it to undershoot the timeout slightly.
    assert!(before.elapsed() >= Duration::from_millis(45));
}

/// The SRQ low-watermark event arrives as an asynchronous event: an SRQ armed with a limit raises
/// `SrqLimitReached` once sends consume its posted receives down below the limit.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn srq_limit_reached_async_event() {
    let ctx = open_test_device();

    let cq = ctx.create_cq(16).build().expect("failed to create CQ");
    let pd = ctx.alloc_pd().expect("failed to allocate PD");
    // Arm the low watermark after creation: dropping below 2 posted receives raises the event.
    let srq = pd.create_srq(16, 1, 0).expect("failed to create SRQ");
    srq.set_limit(2).expect("failed to arm the SRQ limit");
    let attrs = srq.query().expect("failed to query the SRQ");
    assert!(attrs.max_wr() >= 16, "{attrs:?}");
    assert!(attrs.max_sge() >= 1, "{attrs:?}");
    assert_eq!(attrs.limit(), 2, "{attrs:?}");
    // Resizing is provider-dependent; where it works, the queue reports the larger capacity.
    match srq.set_max_wr(32) {
        Ok(()) => {
            let grown = srq.query().expect("failed to query the SRQ");
            assert!(grown.max_wr() >= 32, "{grown:?}");
        }
        Err(e) => eprintln!("SRQ resize not available here: {e}"),
    }

    let prepared = pd
        .create_qp::<Rc>(&cq, &cq, 1)
        .expect("failed to create QP")
        .set_gid_index(gid_index(&ctx))
        .set_srq(&srq)
        .set_max_send_wr(8)
        .build()
        .expect("failed to build QP");
    let endpoint = prepared.endpoint().expect("failed to read endpoint");
    let mut qp = prepared.handshake(endpoint).expect("failed to connect QP");

    let recv = pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register recv MR");
    let mut send = pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register send MR");
    send.bytes_mut()[..4].copy_from_slice(b"srq!");

    // Post three receives, then consume all of them: the count crosses below the limit of 2.
    unsafe {
        srq.post_recv([
            RecvRequest::new(1, &[recv.slice(..4)]),
            RecvRequest::new(2, &[recv.slice(..4)]),
            RecvRequest::new(3, &[recv.slice(..4)]),
        ])
    }
    .expect("SRQ post_recv failed");
    let mut batch = qp.start_send();
    batch.op().signaled().send(4, &[send.slice(..4)]);
    batch.op().signaled().send(5, &[send.slice(..4)]);
    batch.op().signaled().send(6, &[send.slice(..4)]);
    unsafe { batch.submit() }.expect("send failed");
    drain(&cq, 6);

    // The event may share the queue with unrelated ones (port changes and the like); wait until
    // the SRQ limit event shows up, dropping (acknowledging) everything else.
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match ctx
            .wait_async_event(Some(remaining))
            .expect("failed to wait for an async event")
        {
            Some(event) if event.event_type() == ibverbs::AsyncEventType::SrqLimitReached => {
                assert_eq!(event.port_num(), None, "an SRQ event is not port-scoped");
                break;
            }
            Some(_other) => continue,
            None => panic!("no SrqLimitReached event within the deadline"),
        }
    }
}

/// Unreliable datagram (UD): a connectionless queue pair sends a datagram to itself via an address
/// handle pointing at its own GID. UD prepends a 40-byte GRH to received messages.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn unreliable_datagram() {
    let ctx = open_test_device();
    let cq = ctx.create_cq(16).build().expect("failed to create CQ");
    let pd = ctx.alloc_pd().expect("failed to allocate PD");

    let gid_index = gid_index(&ctx);
    const QKEY: u32 = 0x1234_5678;

    let prepared = pd
        .create_qp::<Ud>(&cq, &cq, 1)
        .expect("failed to create UD QP")
        .set_gid_index(gid_index)
        .build()
        .expect("failed to build UD QP");
    let endpoint = prepared.endpoint().expect("failed to read endpoint");
    let mut qp = prepared.activate(QKEY).expect("failed to activate UD QP");

    // Address handle pointing at our own GID, so the datagram loops back to us.
    let my_gid = endpoint.gid.expect("RoCE requires a GID");
    let mut ah_attr = AddressHandleAttribute::new(1);
    ah_attr.set_grh(my_gid, gid_index as u8, 64, 0);
    let ah = pd
        .create_address_handle(&ah_attr)
        .expect("failed to create address handle");

    let payload = b"datagram";
    // UD receives prepend a 40-byte GRH, so the receive buffer must allow for it.
    let mut recv = pd
        .allocate(40 + 64, AccessFlags::PERMISSIVE)
        .expect("failed to register recv MR");
    let mut send = pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register send MR");
    send.bytes_mut()[..payload.len()].copy_from_slice(payload);

    unsafe { qp.post_recv([RecvRequest::new(1, &[recv.slice(..40 + payload.len())])]) }
        .expect("post_recv failed");
    let mut batch = qp.start_send();
    batch
        .to(&ah, endpoint.qp_num, QKEY)
        .signaled()
        .send(2, &[send.slice(..payload.len())]);
    unsafe { batch.submit() }.expect("UD send failed");

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

    let mut target = lb
        .pd
        .allocate(8, AccessFlags::PERMISSIVE)
        .expect("failed to register target MR");
    let mut local = lb
        .pd
        .allocate(8, AccessFlags::PERMISSIVE)
        .expect("failed to register local MR");

    // The compare matches the zeroed target, so the swap takes effect and the original value (0) is
    // returned into `local`.
    let swapped = 0x0102_0304_0506_0708_u64;
    {
        let sg = [local.slice(..)];
        let remote = target.remote().slice(..);
        unsafe {
            let mut batch = lb.qp.start_send();
            batch
                .op()
                .signaled()
                .atomic_cmp_swap(1, &sg, remote, 0, swapped);
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
            batch.op().signaled().atomic_cmp_swap(2, &sg, remote, 0, 0);
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
    let mut counter = lb
        .pd
        .allocate(8, AccessFlags::PERMISSIVE)
        .expect("failed to register counter MR");
    {
        let sg = [local.slice(..)];
        let remote = counter.remote().slice(..);
        unsafe {
            let mut batch = lb.qp.start_send();
            batch.op().signaled().atomic_fetch_add(3, &sg, remote, 5);
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
    let mr = lb
        .pd
        .allocate(4096, AccessFlags::PERMISSIVE)
        .expect("failed to register MR");
    let sg = [mr.slice(..)];

    match lb
        .pd
        .advise_mr(MrAdvice::Prefetch, MrAdviseFlags::empty(), &sg)
    {
        // Either the device prefetched, or it does not implement advise_mr / on-demand paging.
        Ok(()) => {}
        Err(ibverbs::Error::Unsupported { .. }) => {}
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
    let access = AccessFlags::PERMISSIVE;
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

    unsafe {
        lb.qp
            .post_recv([RecvRequest::new(1, &[recv_mr.slice(..6)])])
    }
    .expect("post_recv failed");
    let mut batch = lb.qp.start_send();
    batch.op().signaled().send(2, &[send_mr.slice(..6)]);
    unsafe { batch.submit() }.expect("send failed");

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

    let recv_a = lb
        .pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register recv MR a");
    let recv_b = lb
        .pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register recv MR b");
    let mut send_a = lb
        .pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register send MR a");
    let mut send_b = lb
        .pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register send MR b");
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
    let mut batch = lb.qp.start_send();
    batch.op().signaled().send(11, &[send_a.slice(..3)]);
    unsafe { batch.submit() }.expect("send a failed");
    let mut batch = lb.qp.start_send();
    batch.op().signaled().send(12, &[send_b.slice(..3)]);
    unsafe { batch.submit() }.expect("send b failed");

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

    let recv = lb
        .pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register recv MR");
    let mut send = lb
        .pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register send MR");
    send.bytes_mut()[..4].copy_from_slice(b"flag");

    let sg = [recv.slice(..4)];
    unsafe {
        lb.qp
            .post_recv([RecvRequest::new(1, &sg), RecvRequest::new(2, &sg)])
    }
    .expect("post_recv failed");

    let mut batch = lb.qp.start_send();
    // Fenced: ordered after any prior reads/atomics on this queue pair.
    batch.op().signaled().fenced().send(10, &[send.slice(..4)]);
    // Solicited: raises a solicited event on the receiver.
    batch
        .op()
        .signaled()
        .solicited()
        .send(11, &[send.slice(..4)]);
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
    assert!(matches!(port.state(), PortState::Active | PortState::Armed));
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
    let devices = ibverbs::devices().expect("failed to list RDMA devices");
    let device = devices.iter().next().expect("no RDMA device available");
    assert!(!device.as_raw().is_null());

    let ctx = open_test_device();
    assert!(!ctx.as_raw().is_null());

    // The context handle is cheaply cloneable and debug-printable.
    let ctx2 = ctx.clone();
    assert_eq!(ctx.as_raw(), ctx2.as_raw());
    assert!(format!("{ctx:?}").starts_with("Context("), "{ctx:?}");

    assert!(ctx.num_comp_vectors() > 0, "device has completion vectors");

    let pd = ctx.alloc_pd().expect("failed to allocate PD");
    assert!(!pd.as_raw().is_null());

    let cq = ctx.create_cq(16).build().expect("failed to create CQ");
    assert!(!cq.as_raw().is_null());
    assert!(!cq.as_raw_ex().is_null());
    // The plain and extended views are the same underlying completion queue.
    assert_eq!(cq.as_raw() as *const (), cq.as_raw_ex() as *const ());

    let mr = pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register MR");
    assert!(!mr.as_raw().is_null());
    assert_eq!(mr.lkey(), mr.slice(..).lkey());

    let srq = pd.create_srq(16, 1, 0).expect("failed to create SRQ");
    assert!(!srq.as_raw().is_null());

    // A UD queue pair plus an address handle to our own GID exercise the QP and AH accessors.
    let prepared = pd
        .create_qp::<Ud>(&cq, &cq, 1)
        .expect("failed to create UD QP")
        .set_gid_index(gid_index(&ctx))
        .build()
        .expect("failed to build UD QP");
    let endpoint = prepared.endpoint().expect("failed to read endpoint");
    let qp = prepared
        .activate(0x1234_5678)
        .expect("failed to activate UD QP");
    assert!(!qp.as_raw().is_null());
    assert!(!qp.as_raw_ex().is_null());

    let mut ah_attr = AddressHandleAttribute::new(1);
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
        .create_qp::<Rc>(&cq, &cq, 1)
        .expect("failed to create RC QP");
    builder
        .set_gid_index(gid_index(&ctx))
        .set_max_inline_data(64)
        .set_access(
            AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE | AccessFlags::REMOTE_READ,
        );
    let prepared = builder.build().expect("failed to build QP");
    let endpoint = prepared.endpoint().expect("failed to read endpoint");
    let mut qp = prepared.handshake(endpoint).expect("failed to reach RTS");

    // Inline SEND: the payload lives only in this stack array, never in a registered MR.
    let recv = pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register recv MR");
    unsafe { qp.post_recv([RecvRequest::new(1, &[recv.slice(..5)])]) }.expect("post_recv failed");
    let mut batch = qp.start_send();
    batch.op().signaled().send(2, Payload::Inline(b"inrun"));
    unsafe { batch.submit() }.expect("inline send submit failed");
    let comps = drain(&cq, 2);
    assert!(comps.iter().any(|c| c.wr_id() == 1), "missing recv");
    assert!(comps.iter().any(|c| c.wr_id() == 2), "missing send");
    assert_eq!(&recv.bytes()[..5], b"inrun");

    // Inline RDMA WRITE into a remote region.
    let dst = pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register dst MR");
    let remote = dst.remote().slice(..4);
    let mut batch = qp.start_send();
    batch
        .op()
        .signaled()
        .write(3, Payload::Inline(b"wxyz"), remote);
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
        .create_qp::<Rc>(&cq, &cq, 1)
        .expect("failed to create QP on port 1");
    builder.set_gid_index(gid_index(&ctx)).set_access(
        AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE | AccessFlags::REMOTE_READ,
    );
    let prepared = builder.build().expect("failed to build QP");
    let endpoint = prepared.endpoint().expect("failed to read endpoint");
    let mut qp = prepared.handshake(endpoint).expect("failed to reach RTS");

    let recv = pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register recv MR");
    let mut send = pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register send MR");
    send.bytes_mut()[..4].copy_from_slice(b"port");

    unsafe { qp.post_recv([RecvRequest::new(1, &[recv.slice(..4)])]) }.expect("post_recv failed");
    let mut batch = qp.start_send();
    batch.op().signaled().send(2, &[send.slice(..4)]);
    unsafe { batch.submit() }.expect("send failed");

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
        Ok(clock) => {
            let _ticks: u64 = clock.ticks();
        }
        Err(ibverbs::Error::Unsupported { .. }) => {
            eprintln!("device does not support query_rt_values_ex; skipping that check");
        }
        Err(e) => panic!("query_rt_values_ex failed: {e}"),
    }

    let cq = match ctx
        .create_cq(16)
        .set_wc_flags(WcFields::COMPLETION_TIMESTAMP)
        .build()
    {
        Ok(cq) => cq,
        Err(ibverbs::Error::Unsupported { .. }) => {
            eprintln!("device does not support completion timestamps; skipping");
            return;
        }
        Err(e) => panic!("create_cq with timestamps failed: {e}"),
    };
    let pd = ctx.alloc_pd().expect("failed to allocate PD");
    let mut builder = pd
        .create_qp::<Rc>(&cq, &cq, 1)
        .expect("failed to create QP");
    builder.set_gid_index(gid_index(&ctx)).set_access(
        AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE | AccessFlags::REMOTE_READ,
    );
    let prepared = builder.build().expect("failed to build QP");
    let endpoint = prepared.endpoint().expect("failed to read endpoint");
    let mut qp = prepared.handshake(endpoint).expect("failed to reach RTS");

    let recv = pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register recv MR");
    let mut send = pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register send MR");
    send.bytes_mut()[..4].copy_from_slice(b"time");
    unsafe { qp.post_recv([RecvRequest::new(1, &[recv.slice(..4)])]) }.expect("post_recv failed");
    let mut batch = qp.start_send();
    batch.op().signaled().send(2, &[send.slice(..4)]);
    unsafe { batch.submit() }.expect("send failed");

    // Each completion carries a hardware timestamp; reading it must succeed (not panic).
    let mut stamps = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while stamps.len() < 2 {
        let mut comps = cq.poll().expect("failed to poll CQ");
        while let Some(wc) = comps.next() {
            wc.ok().expect("work request failed");
            stamps.push(wc.completion_timestamp());
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for completions"
        );
    }
    // Timestamps are raw HCA-clock readings; their difference is a tick count.
    let earliest = *stamps.iter().min().expect("have completions");
    let latest = *stamps.iter().max().expect("have completions");
    let _delta: u64 = latest - earliest;
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
        .set_wc_flags(WcFields::SLID | WcFields::SL | WcFields::DLID_PATH_BITS)
        .build()
    {
        Ok(cq) => cq,
        Err(ibverbs::Error::Unsupported { .. }) => {
            eprintln!("device does not support these completion fields; skipping");
            return;
        }
        Err(e) => panic!("create_cq failed: {e}"),
    };

    let pd = ctx.alloc_pd().expect("failed to allocate PD");
    let mut builder = pd
        .create_qp::<Rc>(&cq, &cq, 1)
        .expect("failed to create QP");
    builder.set_gid_index(gid_index(&ctx)).set_access(
        AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE | AccessFlags::REMOTE_READ,
    );
    let prepared = builder.build().expect("failed to build QP");
    let endpoint = prepared.endpoint().expect("failed to read endpoint");
    let mut qp = prepared.handshake(endpoint).expect("failed to reach RTS");

    let recv = pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register recv MR");
    let mut send = pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register send MR");
    send.bytes_mut()[..4].copy_from_slice(b"wcfl");
    unsafe { qp.post_recv([RecvRequest::new(1, &[recv.slice(..4)])]) }.expect("post_recv failed");
    let mut batch = qp.start_send();
    batch.op().signaled().send(2, &[send.slice(..4)]);
    unsafe { batch.submit() }.expect("send failed");

    // Exercise the accessors on each completion. The addressing values are device-defined (and zero
    // on RoCE), so just ensure the reads succeed; an RC completion never carries a GRH.
    let mut seen = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    while seen < 2 {
        let mut comps = cq.poll().expect("failed to poll CQ");
        while let Some(wc) = comps.next() {
            wc.ok().expect("work request failed");
            let _ = wc.wc_flags();
            let _ = wc.has_grh();
            let _ = wc.slid();
            let _ = wc.sl();
            let _ = wc.dlid_path_bits();
            assert_eq!(
                wc.invalidated_rkey(),
                None,
                "a plain send invalidates nothing"
            );
            let raw = wc.as_raw();
            assert!(!raw.is_null());
            assert_eq!(unsafe { (*raw).wr_id }, wc.wr_id());
            seen += 1;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for completions"
        );
    }
    assert!(
        cq.capacity() >= 16,
        "the device grants at least what was asked"
    );

    // The packet-classification fields exist only on hardware that classifies; read them where
    // the device offers them, and accept a refusal elsewhere (Soft-RoCE has none).
    match ctx
        .create_cq(16)
        .set_wc_flags(WcFields::CVLAN | WcFields::FLOW_TAG)
        .build()
    {
        Ok(cq) => {
            let mut qp = loopback_on(&pd, &cq, gid_index(&ctx));
            unsafe { qp.post_recv([RecvRequest::new(1, &[recv.slice(..4)])]) }
                .expect("post_recv failed");
            let mut batch = qp.start_send();
            batch.op().signaled().send(2, &[send.slice(..4)]);
            unsafe { batch.submit() }.expect("send failed");
            let mut seen = 0;
            let deadline = Instant::now() + Duration::from_secs(5);
            while seen < 2 {
                let mut comps = cq.poll().expect("failed to poll CQ");
                while let Some(wc) = comps.next() {
                    wc.ok().expect("work request failed");
                    let _ = wc.cvlan();
                    let _ = wc.flow_tag();
                    seen += 1;
                }
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for completions"
                );
            }
        }
        Err(ibverbs::Error::Unsupported { .. }) => {
            eprintln!("device does not classify packets (no VLAN/flow-tag fields); skipping");
        }
        Err(e) => panic!("create_cq failed: {e}"),
    }
}

/// The completion channel exposes a descriptor and arm/consume hooks for event-driven polling.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn event_driven_completion() {
    use std::os::fd::AsRawFd;

    let ctx = open_test_device();
    let channel = ctx
        .create_comp_channel()
        .expect("failed to create completion channel");
    // The completion channel is backed by a real (non-blocking) descriptor an event loop can wait on.
    assert!(
        channel.as_raw_fd() >= 0,
        "completion channel should expose an fd"
    );

    let cq = ctx
        .create_cq(64)
        .set_comp_channel(&channel)
        .build()
        .expect("failed to create completion queue");
    assert_eq!(
        cq.comp_channel().expect("built with a channel").as_raw_fd(),
        channel.as_raw_fd()
    );
    let pd = ctx
        .alloc_pd()
        .expect("failed to allocate protection domain");
    let mut qp = loopback_on(&pd, &cq, gid_index(&ctx));

    let mut recv = pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register recv MR");
    let mut send = pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register send MR");
    send.bytes_mut()[..5].copy_from_slice(b"hello");

    // Arm the queue before posting so the completions raise a notification on the descriptor.
    cq.req_notify(false)
        .expect("failed to arm the completion queue");
    unsafe { qp.post_recv([RecvRequest::new(1, &[recv.slice(..5)])]) }.expect("post_recv failed");
    let mut batch = qp.start_send();
    batch.op().signaled().send(2, &[send.slice(..5)]);
    unsafe { batch.submit() }.expect("send failed");

    // A real event loop would await readability of the descriptor; here we consume the notification
    // as soon as it arrives. `get_event` reads the (non-blocking) channel and acknowledges for us.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut notified = false;
    while Instant::now() < deadline {
        if channel.get_event().expect("get_event failed").is_some() {
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
    let comps = drain(&cq, 2);
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
    assert_eq!(device.transport_type(), TransportType::Ib);

    let ctx = device.open().expect("failed to open the RDMA device");

    // A single GID query returns the same GID as the matching full-table entry.
    let table = ctx.gid_table().expect("failed to read GID table");
    let entry = table.first().expect("expected at least one GID entry");
    let gid = ctx
        .query_gid(entry.port_num, entry.gid_index)
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

/// `query` and `modify` inspect and change a queue pair's attributes after `handshake`.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn modify_and_query_queue_pair() {
    // `loopback()` builds an RC queue pair and drives it to RTS via `handshake`. The general
    // `query`/`modify` API then lets us inspect and change it afterwards.
    let mut lb = loopback();

    // Query the attributes the handshake negotiated. Because the queue pair is connected to its own
    // endpoint, its destination QP number is its own.
    let mask = QueuePairAttributeMask::STATE
        | QueuePairAttributeMask::CUR_STATE
        | QueuePairAttributeMask::DEST_QPN
        | QueuePairAttributeMask::SQ_PSN;
    let (attr, init) = lb.qp.query(mask).expect("failed to query the queue pair");
    assert_eq!(attr.state(), QueuePairState::ReadyToSend);
    assert_eq!(attr.dest_qp_num(), lb.qp.qp_num());
    assert!(init.max_send_wr() >= 16);

    // An illegal transition (RTS -> INIT) is reported precisely.
    let mut to_init = QueuePairAttribute::new();
    to_init.set_state(QueuePairState::Init);
    match lb.qp.modify(&to_init) {
        Err(Error::InvalidQueuePairTransition { current, next }) => {
            assert_eq!(current, QueuePairState::ReadyToSend);
            assert_eq!(next, QueuePairState::Init);
        }
        other => panic!("expected InvalidQueuePairTransition, got {other:?}"),
    }

    // A legal transition (any state -> ERR) succeeds, and the change is visible to a later query.
    let mut to_err = QueuePairAttribute::new();
    to_err.set_state(QueuePairState::Error);
    lb.qp
        .modify(&to_err)
        .expect("failed to move the queue pair to ERR");
    let (attr, _) = lb
        .qp
        .query(QueuePairAttributeMask::STATE)
        .expect("failed to re-query the queue pair");
    assert_eq!(attr.state(), QueuePairState::Error);
}

/// `into_queue_pair` plus `modify` lets you drive a queue pair through its states by hand, the
/// raw path that the datagram transports' `activate` wraps.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn manual_bringup_via_modify() {
    let ctx = open_test_device();
    let cq = ctx.create_cq(16).build().expect("failed to create CQ");
    let pd = ctx.alloc_pd().expect("failed to allocate PD");

    // Build a UD queue pair but do not activate it; take the still-RESET queue pair to drive by hand.
    let prepared = pd
        .create_qp::<Ud>(&cq, &cq, 1)
        .expect("failed to create QP")
        .build()
        .expect("failed to build QP");
    let mut qp = prepared.into_queue_pair();

    const QKEY: u32 = 0x1111_1111;

    // RESET -> INIT: associate the port, partition key, and Q_Key.
    let mut init = QueuePairAttribute::new();
    init.set_state(QueuePairState::Init)
        .set_pkey_index(0)
        .set_port(1)
        .set_qkey(QKEY);
    qp.modify(&init).expect("RESET -> INIT failed");

    // INIT -> RTR.
    let mut rtr = QueuePairAttribute::new();
    rtr.set_state(QueuePairState::ReadyToReceive);
    qp.modify(&rtr).expect("INIT -> RTR failed");

    // RTR -> RTS.
    let mut rts = QueuePairAttribute::new();
    rts.set_state(QueuePairState::ReadyToSend).set_sq_psn(0);
    qp.modify(&rts).expect("RTR -> RTS failed");

    // The hand-driven queue pair reached RTS with the Q_Key we set.
    let (attr, _) = qp
        .query(QueuePairAttributeMask::STATE | QueuePairAttributeMask::QKEY)
        .expect("query failed");
    assert_eq!(attr.state(), QueuePairState::ReadyToSend);
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
        .create_qp::<Rc>(&cq, &cq, 1)
        .expect("failed to create RC QP");
    builder
        .set_gid_index(gid_index(&ctx))
        .set_max_inline_data(64)
        .set_access(
            AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE | AccessFlags::REMOTE_READ,
        );
    let prepared = builder.build().expect("failed to build QP");
    let endpoint = prepared.endpoint().expect("failed to read endpoint");
    let mut qp = prepared.handshake(endpoint).expect("failed to reach RTS");

    // Gathered inline SEND: the payload is assembled from three separate stack buffers.
    let recv = pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register recv MR");
    unsafe { qp.post_recv([RecvRequest::new(1, &[recv.slice(..9)])]) }.expect("post_recv failed");
    let bufs = [
        IoSlice::new(b"ab"),
        IoSlice::new(b"cde"),
        IoSlice::new(b"fghi"),
    ];
    let mut batch = qp.start_send();
    batch.op().signaled().send(2, Payload::InlineList(&bufs));
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
    // message. The gather call and the completions still succeed; only check the delivered bytes
    // on a provider that reports the real length.
    if recv_len == 0 {
        return;
    }
    assert_eq!(
        recv_len, 9,
        "gathered inline send delivered a partial payload"
    );
    assert_eq!(&recv.bytes()[..9], b"abcdefghi");

    // Gathered inline RDMA WRITE into a remote region.
    let dst = pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("failed to register dst MR");
    let remote = dst.remote().slice(..6);
    let parts = [IoSlice::new(b"uvw"), IoSlice::new(b"xyz")];
    let mut batch = qp.start_send();
    batch
        .op()
        .signaled()
        .write(3, Payload::InlineList(&parts), remote);
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

    // The Debug impl renders the wrapper and its nested base attributes.
    let dbg = format!("{ex:?}");
    assert!(
        dbg.contains("DeviceAttrEx") && dbg.contains("DeviceAttr {"),
        "{dbg}"
    );
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

/// Two completion queues share one completion channel, so a single file descriptor carries
/// notifications for both. Drive them through the channel and demultiplex by the context value each
/// queue was built with.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn shared_completion_channel() {
    use std::collections::HashSet;
    use std::os::fd::AsRawFd;

    let ctx = open_test_device();
    let channel = ctx
        .create_comp_channel()
        .expect("failed to create completion channel");

    // Distinct context values let `CompletionChannel::get_event` say which queue a notification is
    // for.
    const CTX_A: u64 = 1;
    const CTX_B: u64 = 2;
    let cq_a = ctx
        .create_cq(16)
        .set_comp_channel(&channel)
        .set_context(CTX_A)
        .build()
        .expect("failed to build first completion queue");
    let cq_b = ctx
        .create_cq(16)
        .set_comp_channel(&channel)
        .set_context(CTX_B)
        .build()
        .expect("failed to build second completion queue");

    // The point of sharing: both queues and the channel expose the very same descriptor.
    assert_eq!(
        cq_a.comp_channel().expect("has a channel").as_raw_fd(),
        channel.as_raw_fd()
    );
    assert_eq!(
        cq_b.comp_channel().expect("has a channel").as_raw_fd(),
        channel.as_raw_fd()
    );

    let pd = ctx.alloc_pd().expect("failed to allocate pd");
    let mut qp_a = loopback_on(&pd, &cq_a, gid_index(&ctx));
    let mut qp_b = loopback_on(&pd, &cq_b, gid_index(&ctx));

    let mut recv_a = pd.allocate(16, AccessFlags::PERMISSIVE).expect("recv a");
    let mut recv_b = pd.allocate(16, AccessFlags::PERMISSIVE).expect("recv b");
    let mut send_a = pd.allocate(16, AccessFlags::PERMISSIVE).expect("send a");
    let mut send_b = pd.allocate(16, AccessFlags::PERMISSIVE).expect("send b");
    send_a.bytes_mut()[..4].copy_from_slice(b"aaaa");
    send_b.bytes_mut()[..4].copy_from_slice(b"bbbb");

    unsafe { qp_a.post_recv([RecvRequest::new(10, &[recv_a.slice(..4)])]) }.expect("post_recv a");
    unsafe { qp_b.post_recv([RecvRequest::new(20, &[recv_b.slice(..4)])]) }.expect("post_recv b");

    // Arm both queues before posting, so the completions raise notifications on the shared channel.
    cq_a.req_notify(false).expect("arm a");
    cq_b.req_notify(false).expect("arm b");

    let mut batch = qp_a.start_send();
    batch.op().signaled().send(11, &[send_a.slice(..4)]);
    unsafe { batch.submit() }.expect("send a");
    let mut batch = qp_b.start_send();
    batch.op().signaled().send(21, &[send_b.slice(..4)]);
    unsafe { batch.submit() }.expect("send b");

    // Drive completions off the one channel, demultiplexing by context.
    let mut a = HashSet::new();
    let mut b = HashSet::new();
    let mut contexts_seen = HashSet::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while a.len() < 2 || b.len() < 2 {
        assert!(Instant::now() < deadline, "timed out: a={a:?} b={b:?}");
        match channel
            .wait(Some(Duration::from_millis(100)))
            .expect("channel wait")
        {
            None => continue,
            Some(context) => {
                contexts_seen.insert(context);
                let (cq, ids) = match context {
                    CTX_A => (&cq_a, &mut a),
                    CTX_B => (&cq_b, &mut b),
                    other => panic!("unexpected completion-queue context {other}"),
                };
                // Re-arm before draining so a completion racing in is not missed.
                cq.req_notify(false).expect("re-arm");
                let mut completions = cq.poll().expect("poll");
                while let Some(wc) = completions.next() {
                    assert!(wc.ok().is_ok(), "work request {} failed", wc.wr_id());
                    ids.insert(wc.wr_id());
                }
            }
        }
    }

    assert!(
        contexts_seen.contains(&CTX_A) && contexts_seen.contains(&CTX_B),
        "both queues should have notified on the shared channel: {contexts_seen:?}"
    );
    assert_eq!(a, HashSet::from([10, 11]), "first queue's completions");
    assert_eq!(b, HashSet::from([20, 21]), "second queue's completions");
    assert_eq!(&recv_a.bytes_mut()[..4], b"aaaa");
    assert_eq!(&recv_b.bytes_mut()[..4], b"bbbb");

    // A queue built without a channel has none to reach.
    let bare = ctx
        .create_cq(16)
        .build()
        .expect("failed to build channel-less completion queue");
    assert!(
        bare.comp_channel().is_none(),
        "a queue without set_comp_channel has no channel"
    );
}

/// Handshaking toward a remote GID that nobody answers fails with the RoCE routing diagnostic
/// (the provider resolves the route during the RTR transition, and a bare "connection timed out"
/// points debugging at the wrong layer).
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn roce_route_failure_diagnostic() {
    let ctx = open_test_device();
    let cq = ctx.create_cq(16).build().expect("failed to create CQ");
    let pd = ctx.alloc_pd().expect("failed to allocate PD");
    let prepared = pd
        .create_qp::<Rc>(&cq, &cq, 1)
        .expect("failed to create QP")
        .set_gid_index(gid_index(&ctx))
        .build()
        .expect("failed to build QP");

    // A link-local address derived from nothing on this network: neighbor discovery cannot
    // resolve it, so the RTR transition fails.
    let mut endpoint = prepared.endpoint().expect("failed to read endpoint");
    endpoint.gid = Some(ibverbs::Gid::from([
        0xfe, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xde, 0xad, 0xbe, 0xef,
    ]));

    match prepared.handshake(endpoint) {
        Err(Error::ModifyQueuePair(e)) if e.raw_os_error().is_none() => {
            // The wrapped diagnostic: points at RoCE routing and names the local GID index.
            assert!(e.to_string().contains("RoCE"), "{e}");
            assert!(e.to_string().contains("index 1"), "{e}");
        }
        Err(Error::ModifyQueuePair(e)) => {
            // Some providers report a different errno for an unresolvable neighbor; the
            // diagnostic only wraps timeouts and unreachable-network errors.
            eprintln!("provider reported {e} instead of a route timeout");
        }
        Err(other) => panic!("unexpected error kind: {other:?}"),
        Ok(_) => panic!("handshake to an unanswerable GID unexpectedly succeeded"),
    }
}

/// `routable_gid` names an entry of the port's GID table, and reports a port without entries.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn routable_gid() {
    let ctx = open_test_device();
    let entry = ctx
        .routable_gid(1)
        .expect("failed to read the GID table")
        .expect("port 1 has a GID");
    assert_eq!(entry.port_num, 1);
    let table = ctx.gid_table().expect("failed to read the GID table");
    assert!(
        table
            .iter()
            .any(|e| e.port_num == 1 && e.gid_index == entry.gid_index && e.gid == entry.gid),
        "the routable entry comes from the table: {entry:?}"
    );
    assert!(ctx
        .routable_gid(250)
        .expect("failed to read the GID table")
        .is_none());
}

/// The attributes `handshake` applies are exposed, so a manual bring-up can start from them,
/// adjust one, and still reach a working queue pair.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn manual_bringup_from_handshake_attributes() {
    let ctx = open_test_device();
    let cq = ctx.create_cq(16).build().expect("failed to create CQ");
    let pd = ctx.alloc_pd().expect("failed to allocate PD");
    let mut builder = pd
        .create_qp::<Rc>(&cq, &cq, 1)
        .expect("failed to create QP");
    builder
        .set_gid_index(gid_index(&ctx))
        .set_access(AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE);
    let prepared = builder.build().expect("failed to build QP");
    let endpoint = prepared.endpoint().expect("failed to read endpoint");

    let init = prepared.init_attributes();
    assert_eq!(init.state(), QueuePairState::Init);
    assert!(init.mask().contains(QueuePairAttributeMask::ACCESS_FLAGS));
    let mut rtr = prepared.rtr_attributes(&endpoint).expect("rtr attributes");
    assert_eq!(rtr.dest_qp_num(), endpoint.qp_num);
    assert_eq!(rtr.rq_psn(), endpoint.psn);
    // Adjust one attribute on the way: a shorter RNR timer than the builder's default.
    rtr.set_min_rnr_timer(RnrTimer::at_least(Duration::from_micros(10)));
    let mut rts = prepared.rts_attributes();
    assert_eq!(rts.sq_psn(), endpoint.psn);
    rts.set_retry_count(3);

    let mut qp = prepared.into_queue_pair();
    for attr in [&init, &rtr, &rts] {
        qp.modify(attr).expect("manual transition");
    }
    let (attr, _) = qp
        .query(
            QueuePairAttributeMask::STATE
                | QueuePairAttributeMask::RETRY_CNT
                | QueuePairAttributeMask::MIN_RNR_TIMER,
        )
        .expect("query");
    assert_eq!(attr.state(), QueuePairState::ReadyToSend);
    assert_eq!(attr.retry_count(), 3);
    assert_eq!(attr.min_rnr_timer().duration(), Duration::from_micros(10));

    // And the result moves data.
    let recv = pd
        .allocate(16, AccessFlags::PERMISSIVE)
        .expect("failed to register recv MR");
    let mut send = pd
        .allocate(16, AccessFlags::PERMISSIVE)
        .expect("failed to register send MR");
    send.bytes_mut()[..4].copy_from_slice(b"attr");
    unsafe { qp.post_recv([RecvRequest::new(1, &[recv.slice(..4)])]) }.expect("post_recv");
    let mut batch = qp.start_send();
    batch.op().signaled().send(2, &[send.slice(..4)]);
    unsafe { batch.submit() }.expect("send");
    drain(&cq, 2);
    assert_eq!(&recv.bytes()[..4], b"attr");
}

/// A queue pair created with only SEND requested (no RDMA or atomics) still moves two-sided
/// traffic, and the builder reports the effective set of operations.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn send_ops_subset() {
    let ctx = open_test_device();
    let cq = ctx.create_cq(16).build().expect("failed to create CQ");
    let pd = ctx.alloc_pd().expect("failed to allocate PD");
    let mut builder = pd
        .create_qp::<Rc>(&cq, &cq, 1)
        .expect("failed to create QP");
    let default = builder.send_ops();
    assert!(
        default.contains(SendOps::SEND | SendOps::RDMA_READ | SendOps::ATOMIC_FETCH_AND_ADD),
        "an RC queue pair requests the full set by default: {default:?}"
    );
    builder
        .set_gid_index(gid_index(&ctx))
        .set_send_ops(SendOps::SEND);
    assert_eq!(builder.send_ops(), SendOps::SEND);
    let prepared = builder.build().expect("failed to build a send-only QP");
    let endpoint = prepared.endpoint().expect("failed to read endpoint");
    let mut qp = prepared.handshake(endpoint).expect("failed to reach RTS");

    let recv = pd
        .allocate(16, AccessFlags::PERMISSIVE)
        .expect("failed to register recv MR");
    let mut send = pd
        .allocate(16, AccessFlags::PERMISSIVE)
        .expect("failed to register send MR");
    send.bytes_mut()[..4].copy_from_slice(b"only");
    unsafe { qp.post_recv([RecvRequest::new(1, &[recv.slice(..4)])]) }.expect("post_recv failed");
    let mut batch = qp.start_send();
    batch.op().signaled().send(2, &[send.slice(..4)]);
    unsafe { batch.submit() }.expect("send failed");
    drain(&cq, 2);
    assert_eq!(&recv.bytes()[..4], b"only");
}

/// A UD queue pair answers a datagram through the route derived from the receive completion and
/// its GRH, in both completion forms: the classic `ibv_wc` from `poll_into`, and the lazily read
/// `WorkCompletion` from `poll`.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn reply_through_address_handle_from_wc() {
    let ctx = open_test_device();
    let cq = ctx.create_cq(16).build().expect("failed to create CQ");
    let pd = ctx.alloc_pd().expect("failed to allocate PD");
    let gid_index = gid_index(&ctx);
    const QKEY: u32 = 0x1234_5678;

    let prepared = pd
        .create_qp::<Ud>(&cq, &cq, 1)
        .expect("failed to create UD QP")
        .set_gid_index(gid_index)
        .build()
        .expect("failed to build UD QP");
    let endpoint = prepared.endpoint().expect("failed to read endpoint");
    let mut qp = prepared.activate(QKEY).expect("failed to activate UD QP");

    // The "request" goes to our own GID through an explicitly built address handle.
    let my_gid = endpoint.gid.expect("RoCE requires a GID");
    let mut request_attr = AddressHandleAttribute::new(1);
    request_attr.set_grh(my_gid, gid_index as u8, 64, 0);
    let request_ah = pd
        .create_address_handle(&request_attr)
        .expect("failed to create the request's address handle");

    let recv = pd
        .allocate(2 * (Grh::LEN + 16), AccessFlags::PERMISSIVE)
        .expect("failed to register recv MR");
    let mut send = pd
        .allocate(32, AccessFlags::PERMISSIVE)
        .expect("failed to register send MR");
    send.bytes_mut()[..4].copy_from_slice(b"ask?");
    send.bytes_mut()[16..20].copy_from_slice(b"yes!");
    let (first, second) = (Grh::LEN + 16, 2 * (Grh::LEN + 16));

    // Round 1: request in, its completion read the classic way, reply out through the derived
    // route.
    unsafe { qp.post_recv([RecvRequest::new(1, &[recv.slice(..first)])]) }.expect("post_recv");
    let mut batch = qp.start_send();
    batch
        .to(&request_ah, endpoint.qp_num, QKEY)
        .signaled()
        .send(2, &[send.slice(..4)]);
    unsafe { batch.submit() }.expect("UD send failed");
    let mut wcs = [ibverbs::ffi::ibv_wc::default(); 4];
    let mut request_wc = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while request_wc.is_none() {
        for wc in cq.poll_into(&mut wcs).expect("poll_into failed") {
            assert!(wc.is_valid(), "work request {} failed", wc.wr_id());
            if wc.wr_id() == 1 {
                request_wc = Some(*wc);
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for the request"
        );
    }
    let request_wc = request_wc.unwrap();
    assert_eq!(&recv.bytes()[Grh::LEN..Grh::LEN + 4], b"ask?");
    let grh = Grh::from_bytes(recv.bytes()[..Grh::LEN].try_into().unwrap());
    assert_eq!(grh.dgid(), my_gid, "the datagram was addressed to our GID");
    assert_eq!(grh.sgid(), my_gid, "and sent from it too (loopback)");
    let reply_attr = AddressHandleAttribute::from_wc(&ctx, 1, &request_wc, Some(&grh))
        .expect("failed to derive the reply route");
    let reply_ah = pd
        .create_address_handle(&reply_attr)
        .expect("failed to create the reply's address handle");

    // Round 2: the reply arrives through that route; derive the next route from the lazily read
    // completion this time.
    unsafe { qp.post_recv([RecvRequest::new(3, &[recv.slice(first..second)])]) }
        .expect("post_recv");
    let mut batch = qp.start_send();
    batch
        .to(&reply_ah, request_wc.src_qp, QKEY)
        .signaled()
        .send(4, &[send.slice(16..20)]);
    unsafe { batch.submit() }.expect("UD reply failed");
    let mut next_attr = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while next_attr.is_none() {
        let mut completions = cq.poll().expect("poll failed");
        while let Some(wc) = completions.next() {
            wc.ok().expect("work request failed");
            if wc.wr_id() == 3 {
                assert!(wc.has_grh(), "a UD receive on RoCE carries a GRH");
                let grh =
                    Grh::from_bytes(recv.bytes()[first..first + Grh::LEN].try_into().unwrap());
                next_attr = Some(
                    AddressHandleAttribute::from_completion(&ctx, 1, &wc, Some(&grh))
                        .expect("failed to derive a route from the lazy completion"),
                );
            }
        }
        assert!(Instant::now() < deadline, "timed out waiting for the reply");
    }
    assert_eq!(
        &recv.bytes()[first + Grh::LEN..first + Grh::LEN + 4],
        b"yes!"
    );
    let _next_ah = pd
        .create_address_handle(&next_attr.unwrap())
        .expect("the derived route makes a valid address handle");
}

/// A completion without a GRH (a LID-routed InfiniBand datagram) still yields a route: libibverbs
/// reads the header before checking whether the completion reports one, so the crate must hand it
/// a header either way rather than a null pointer.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test -- --ignored`"]
fn address_handle_from_completion_without_grh() {
    let ctx = open_test_device();
    // A default completion reports no flags, so no GRH.
    let mut wc = ibverbs::ffi::ibv_wc::default();
    wc.slid = 7;
    wc.sl = 3;
    AddressHandleAttribute::from_wc(&ctx, 1, &wc, None)
        .expect("a completion without a GRH derives a local route");
    let unread = Grh::from_bytes(&[0; Grh::LEN]);
    AddressHandleAttribute::from_wc(&ctx, 1, &wc, Some(&unread))
        .expect("a header given alongside a GRH-less completion is left unread");
}
