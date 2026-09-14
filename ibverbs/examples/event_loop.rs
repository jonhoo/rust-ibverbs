//! Event-driven completion handling: instead of burning a core on `poll`, queues are armed and a
//! completion channel is blocked on. Two queue pairs (each self-connected, as in the loopback
//! example) share one channel, so a single file descriptor reports notifications for both, and
//! the context cookie set at build time says which queue fired — the shape a server driving many
//! connections from one `epoll`/reactor uses. Device-level async events are checked at the end.
//!
//! This runs against the first RDMA device; on a machine without one, create a SoftRoCE device
//! with `rdma link add rxe0 type rxe netdev <netdev>`.

use std::time::Duration;

use ibverbs::{AccessFlags, CompletionQueue, ProtectionDomain, QueuePair, Rc, RecvRequest};

/// Build a reliable-connected queue pair on `cq` and connect it to itself (see the loopback
/// example for the GID selection).
fn self_connected(pd: &ProtectionDomain, cq: &CompletionQueue, gid_index: u32) -> QueuePair {
    let prepared = pd
        .create_qp::<Rc>(cq, cq, 1)
        .unwrap()
        .set_gid_index(gid_index)
        .build()
        .unwrap();
    let endpoint = prepared.endpoint().unwrap();
    prepared.handshake(endpoint).unwrap()
}

fn main() {
    let ctx = ibverbs::devices()
        .unwrap()
        .iter()
        .next()
        .expect("no rdma device available")
        .open()
        .unwrap();
    let pd = ctx.alloc_pd().unwrap();

    let gid_index = ctx
        .routable_gid(1)
        .unwrap()
        .expect("no GID available")
        .gid_index;

    // One channel, two queues built on it. The context cookies let the notifications be told
    // apart; an event loop would instead register `channel.as_fd()` with its reactor.
    const PING: u64 = 1;
    const PONG: u64 = 2;
    let channel = ctx.create_comp_channel().unwrap();
    let cq_ping = ctx
        .create_cq(16)
        .set_comp_channel(&channel)
        .set_context(PING)
        .build()
        .unwrap();
    let cq_pong = ctx
        .create_cq(16)
        .set_comp_channel(&channel)
        .set_context(PONG)
        .build()
        .unwrap();

    let mut qp_ping = self_connected(&pd, &cq_ping, gid_index);
    let mut qp_pong = self_connected(&pd, &cq_pong, gid_index);

    let mut recv_ping = pd.allocate(16, AccessFlags::PERMISSIVE).unwrap();
    let mut recv_pong = pd.allocate(16, AccessFlags::PERMISSIVE).unwrap();
    let mut send_ping = pd.allocate(16, AccessFlags::PERMISSIVE).unwrap();
    let mut send_pong = pd.allocate(16, AccessFlags::PERMISSIVE).unwrap();
    send_ping.bytes_mut()[..4].copy_from_slice(b"ping");
    send_pong.bytes_mut()[..4].copy_from_slice(b"pong");

    unsafe { qp_ping.post_recv([RecvRequest::new(10, &[recv_ping.slice(..4)])]) }.unwrap();
    unsafe { qp_pong.post_recv([RecvRequest::new(20, &[recv_pong.slice(..4)])]) }.unwrap();

    // Arm both queues *before* posting the sends, so the completions raise notifications on the
    // shared channel rather than landing unobserved.
    cq_ping.req_notify(false).unwrap();
    cq_pong.req_notify(false).unwrap();

    let mut batch = qp_ping.start_send();
    batch.op().signaled().send(11, &[send_ping.slice(..4)]);
    unsafe { batch.submit() }.unwrap();
    let mut batch = qp_pong.start_send();
    batch.op().signaled().send(21, &[send_pong.slice(..4)]);
    unsafe { batch.submit() }.unwrap();

    // The event loop: block on the channel, learn which queue fired, re-arm it, then drain it.
    // Re-arming before draining closes the race where a completion lands in between.
    let mut outstanding = 4;
    while outstanding > 0 {
        let fired = channel
            .wait(Some(Duration::from_secs(5)))
            .unwrap()
            .expect("timed out waiting for a completion notification");
        let cq = match fired {
            PING => &cq_ping,
            PONG => &cq_pong,
            _ => unreachable!(),
        };
        cq.req_notify(false).unwrap();
        let mut completions = cq.poll().unwrap();
        while let Some(wc) = completions.next() {
            wc.ok().expect("work request failed");
            println!(
                "queue {} completed work request {}",
                if fired == PING { "ping" } else { "pong" },
                wc.wr_id()
            );
            outstanding -= 1;
        }
    }
    assert_eq!(&recv_ping.bytes_mut()[..4], b"ping");
    assert_eq!(&recv_pong.bytes_mut()[..4], b"pong");

    // Device-level asynchronous events (port changes, queue errors, SRQ limits) arrive on their
    // own descriptor, `ctx.async_fd()` — a reactor would watch it alongside the channel. Nothing
    // dramatic happened here, so the queue should be empty.
    assert!(ctx.poll_async_event().unwrap().is_none());
    println!("done");
}
