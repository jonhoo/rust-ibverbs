//! Integration test for the RDMA connection manager (`rdmacm` feature).
//!
//! It connects a queue pair to itself across two threads in one process, using the connection
//! manager to set up the connection over the device's own IP address (Soft-RoCE works for this).
//! Like the other data-path tests it needs a real RDMA device, so it is `#[ignore]`d; run it with
//! `cargo test --features rdmacm -- --ignored`. It uses the first device, or `IBVERBS_TEST_DEVICE`.
#![cfg(feature = "rdmacm")]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{mpsc, Arc, Barrier};
use std::time::{Duration, Instant};

use ibverbs::ibv_qp_type::IBV_QPT_RC;
use ibverbs::rdmacm::{rdma_port_space, Acceptor, ConnectionParameter, Connector};
use ibverbs::{CompletionQueue, Context};

/// Open the device named by `IBVERBS_TEST_DEVICE`, or the first available one.
fn open_test_device() -> Context {
    let devices = ibverbs::devices().expect("failed to list RDMA devices");
    let device = match std::env::var("IBVERBS_TEST_DEVICE") {
        Ok(name) if !name.is_empty() => devices
            .iter()
            .find(|d| d.name().is_some_and(|n| n.to_bytes() == name.as_bytes()))
            .unwrap_or_else(|| panic!("IBVERBS_TEST_DEVICE={name} is not available")),
        _ => devices
            .iter()
            .next()
            .expect("no RDMA device available (attach one or set IBVERBS_TEST_DEVICE)"),
    };
    device.open().expect("failed to open the RDMA device")
}

/// The device's RoCEv2 IPv4 address, read from its GID table. The connection manager needs to route
/// over an IP address, and a RoCEv2 GID is the IPv4 address mapped into IPv6 (`::ffff:a.b.c.d`).
fn device_ipv4(ctx: &Context) -> Ipv4Addr {
    let gids = ctx.gid_table().expect("failed to read GID table");
    for entry in gids {
        let raw: [u8; 16] = entry.gid.into();
        if raw[..10] == [0; 10] && raw[10] == 0xff && raw[11] == 0xff {
            return Ipv4Addr::new(raw[12], raw[13], raw[14], raw[15]);
        }
    }
    panic!("no RoCEv2 IPv4 GID found; the connection manager test needs an IP-addressed device");
}

/// Poll `cq` until a completion with `wr_id` arrives (or time out), checking it succeeded.
fn wait_for(cq: &CompletionQueue, wr_id: u64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(mut completions) = cq.poll().expect("failed to poll CQ") {
            while let Some(wc) = completions.next() {
                wc.ok().unwrap_or_else(|(status, _)| {
                    panic!("work request {wr_id} failed: {status:?}")
                });
                if wc.wr_id() == wr_id {
                    return;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for completion {wr_id}"
        );
    }
}

const MESSAGE: &[u8] = b"hello over rdmacm";

#[test]
#[ignore = "requires an RDMA device; run with `cargo test --features rdmacm -- --ignored`"]
fn connect_and_send() {
    let addr = SocketAddr::new(IpAddr::V4(device_ipv4(&open_test_device())), 18599);
    let (ready_tx, ready_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    // Passive side: bind, accept, receive the message.
    let server = std::thread::spawn(move || {
        let acceptor = Acceptor::bind(addr, rdma_port_space::RDMA_PS_TCP, 1).expect("bind");
        ready_tx.send(()).expect("signal ready");

        let incoming = acceptor.accept().expect("accept");
        let ctx = incoming.context().expect("server device context");
        let pd = ctx.alloc_pd().expect("server pd");
        let cq = ctx.create_cq(16, 0).expect("server cq");
        let qp = pd
            .create_qp(&cq, &cq, IBV_QPT_RC)
            .expect("server qp builder")
            .build()
            .expect("server qp");
        let mut recv = pd.allocate(64).expect("server recv mr");

        let mut conn = incoming
            .accept(qp, ConnectionParameter::default())
            .expect("accept");
        // The queue pair is RTS now; rnr_retry keeps the peer's send retrying until this is posted.
        unsafe {
            conn.queue_pair()
                .post_receive(&[recv.slice(..MESSAGE.len())], 1)
        }
        .expect("post_receive");

        wait_for(&cq, 1);
        assert_eq!(&recv.bytes_mut()[..MESSAGE.len()], MESSAGE);
        done_tx.send(()).expect("signal done");
    });

    // Active side: connect and send.
    ready_rx.recv().expect("server ready");
    let resolved = Connector::new(rdma_port_space::RDMA_PS_TCP)
        .expect("connector")
        .resolve(addr, Duration::from_secs(5))
        .expect("resolve");
    let ctx = resolved.context().expect("client device context");
    let pd = ctx.alloc_pd().expect("client pd");
    let cq = ctx.create_cq(16, 0).expect("client cq");
    let qp = pd
        .create_qp(&cq, &cq, IBV_QPT_RC)
        .expect("client qp builder")
        .build()
        .expect("client qp");
    let mut send = pd.allocate(64).expect("client send mr");
    send.bytes_mut()[..MESSAGE.len()].copy_from_slice(MESSAGE);

    let mut conn = resolved
        .connect(qp, ConnectionParameter::default())
        .expect("connect");
    unsafe {
        conn.queue_pair()
            .post_send(&[send.slice(..MESSAGE.len())], 2)
    }
    .expect("post_send");
    wait_for(&cq, 2);

    done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("server received the message");
    conn.disconnect().ok();
    server.join().expect("server thread");
}

#[test]
#[ignore = "requires an RDMA device; run with `cargo test --features rdmacm -- --ignored`"]
fn two_connections() {
    // Two clients connect to one acceptor. The second connection request must survive while the
    // first connection is being set up: with a per-connection event channel each connection's events
    // are isolated, so the listener channel never loses a request. (A shared channel would discard
    // the second request while waiting for the first's establishment.)
    let addr = SocketAddr::new(IpAddr::V4(device_ipv4(&open_test_device())), 18601);
    let (ready_tx, ready_rx) = mpsc::channel();
    // Holds every side connected until all three threads have finished their transfer, so no
    // connection is torn down early.
    let barrier = Arc::new(Barrier::new(3));

    let server_barrier = barrier.clone();
    let server = std::thread::spawn(move || {
        let acceptor = Acceptor::bind(addr, rdma_port_space::RDMA_PS_TCP, 2).expect("bind");
        ready_tx.send(()).expect("signal ready");

        let mut received = Vec::new();
        let mut conns = Vec::new();
        for _ in 0..2 {
            let incoming = acceptor.accept().expect("accept");
            let ctx = incoming.context().expect("server device context");
            let pd = ctx.alloc_pd().expect("server pd");
            let cq = ctx.create_cq(16, 0).expect("server cq");
            let qp = pd
                .create_qp(&cq, &cq, IBV_QPT_RC)
                .expect("server qp builder")
                .build()
                .expect("server qp");
            let mut recv = pd.allocate(64).expect("server recv mr");
            let mut conn = incoming
                .accept(qp, ConnectionParameter::default())
                .expect("accept");
            unsafe {
                conn.queue_pair()
                    .post_receive(&[recv.slice(..MESSAGE.len())], 1)
            }
            .expect("post_receive");
            wait_for(&cq, 1);
            received.push(recv.bytes_mut()[..MESSAGE.len()].to_vec());
            // Keep the connection (and its cq/pd) alive until all transfers are done.
            conns.push((conn, cq, pd));
        }
        server_barrier.wait();
        received
    });

    ready_rx.recv().expect("server ready");
    let clients: Vec<_> = [b'A', b'B']
        .into_iter()
        .map(|tag| {
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let resolved = Connector::new(rdma_port_space::RDMA_PS_TCP)
                    .expect("connector")
                    .resolve(addr, Duration::from_secs(5))
                    .expect("resolve");
                let ctx = resolved.context().expect("client device context");
                let pd = ctx.alloc_pd().expect("client pd");
                let cq = ctx.create_cq(16, 0).expect("client cq");
                let qp = pd
                    .create_qp(&cq, &cq, IBV_QPT_RC)
                    .expect("client qp builder")
                    .build()
                    .expect("client qp");
                let mut send = pd.allocate(64).expect("client send mr");
                send.bytes_mut()[..MESSAGE.len()].fill(tag);
                let mut conn = resolved
                    .connect(qp, ConnectionParameter::default())
                    .expect("connect");
                unsafe {
                    conn.queue_pair()
                        .post_send(&[send.slice(..MESSAGE.len())], 2)
                }
                .expect("post_send");
                wait_for(&cq, 2);
                barrier.wait();
            })
        })
        .collect();

    for client in clients {
        client.join().expect("client thread");
    }
    let mut received = server.join().expect("server thread");
    received.sort();
    assert_eq!(
        received,
        vec![vec![b'A'; MESSAGE.len()], vec![b'B'; MESSAGE.len()]],
        "both connections should deliver their distinct message"
    );
}
