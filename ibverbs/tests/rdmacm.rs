//! Integration test for the RDMA connection manager (`rdmacm` feature).
//!
//! It connects a queue pair to itself across two threads in one process, using the connection
//! manager to set up the connection over the device's own IP address (Soft-RoCE works for this).
//! Like the other data-path tests it needs a real RDMA device, so it is `#[ignore]`d; run it with
//! `cargo test --features rdmacm -- --ignored`. It uses the first device, or `IBVERBS_TEST_DEVICE`.
#![cfg(feature = "rdmacm")]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::os::fd::AsRawFd;
use std::sync::{mpsc, Arc, Barrier};
use std::time::{Duration, Instant};

use ibverbs::rdmacm::{
    Acceptor, CmEvent, CmEventType, CmId, ConnectionParameter, Connector, PortSpace,
};
use ibverbs::{AccessFlags, CompletionQueue, Context, QueuePairState, Rc, RecvRequest};

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
                wc.ok()
                    .unwrap_or_else(|e| panic!("work request {wr_id} failed: {e}"));
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

/// A generous bound for the blocking helpers: far above what loopback setup needs, so it only
/// trips if something hangs outright.
const SETUP_TIMEOUT: Option<Duration> = Some(Duration::from_secs(10));

#[test]
#[ignore = "requires an RDMA device; run with `cargo test --features rdmacm -- --ignored`"]
fn connect_and_send() {
    let addr = SocketAddr::new(IpAddr::V4(device_ipv4(&open_test_device())), 18599);
    let (ready_tx, ready_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    // Passive side: bind, accept, receive the message.
    let server = std::thread::spawn(move || {
        let acceptor = Acceptor::bind(addr, PortSpace::Tcp, 1).expect("bind");
        ready_tx.send(()).expect("signal ready");

        let incoming = acceptor.accept(SETUP_TIMEOUT).expect("accept");
        let ctx = incoming.context().expect("server device context");
        let pd = ctx.alloc_pd().expect("server pd");
        let cq = ctx.create_cq(16).build().expect("server cq");
        let qp = pd
            .create_qp::<Rc>(&cq, &cq, 1)
            .expect("server qp builder")
            .build()
            .expect("server qp");
        let mut recv = pd
            .allocate(64, AccessFlags::PERMISSIVE)
            .expect("server recv mr");

        let mut conn = incoming
            .accept(qp, ConnectionParameter::default(), SETUP_TIMEOUT)
            .expect("accept");
        // The accepted connection keeps the listener's address, and its peer is the client on
        // the same device (the port is the client's ephemeral one).
        assert_eq!(conn.local_addr(), Some(addr));
        let peer = conn.peer_addr().expect("server peer address");
        assert_eq!(peer.ip(), addr.ip());
        // The queue pair is RTS now; rnr_retry keeps the peer's send retrying until this is posted.
        unsafe {
            conn.queue_pair()
                .post_recv([RecvRequest::new(1, &[recv.slice(..MESSAGE.len())])])
        }
        .expect("post_recv");

        wait_for(&cq, 1);
        assert_eq!(&recv.bytes_mut()[..MESSAGE.len()], MESSAGE);
        done_tx.send(()).expect("signal done");
    });

    // Active side: connect and send.
    ready_rx.recv().expect("server ready");
    let resolved = Connector::new(PortSpace::Tcp)
        .expect("connector")
        .resolve(addr, Duration::from_secs(5))
        .expect("resolve");
    let ctx = resolved.context().expect("client device context");
    let pd = ctx.alloc_pd().expect("client pd");
    let cq = ctx.create_cq(16).build().expect("client cq");
    let qp = pd
        .create_qp::<Rc>(&cq, &cq, 1)
        .expect("client qp builder")
        .build()
        .expect("client qp");
    let mut send = pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("client send mr");
    send.bytes_mut()[..MESSAGE.len()].copy_from_slice(MESSAGE);

    let mut conn = resolved
        .connect(qp, ConnectionParameter::default(), SETUP_TIMEOUT)
        .expect("connect");
    // The client's peer is the server's listen address; its own address is on the same device.
    assert_eq!(conn.peer_addr(), Some(addr));
    let local = conn.local_addr().expect("client local address");
    assert_eq!(local.ip(), addr.ip());
    let mut batch = conn.queue_pair().start_send();
    batch
        .op()
        .signaled()
        .send(2, &[send.slice(..MESSAGE.len())]);
    unsafe { batch.submit() }.expect("send");
    wait_for(&cq, 2);

    done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("server received the message");
    conn.disconnect().ok();
    server.join().expect("server thread");
}

#[test]
#[ignore = "requires an RDMA device; run with `cargo test --features rdmacm -- --ignored`"]
fn accept_times_out() {
    // An acceptor with a timeout and no client reports TimedOut instead of blocking forever.
    let addr = SocketAddr::new(IpAddr::V4(device_ipv4(&open_test_device())), 18605);
    let acceptor = Acceptor::bind(addr, PortSpace::Tcp, 1).expect("bind");
    let before = Instant::now();
    match acceptor.accept(Some(Duration::from_millis(50))) {
        Err(ibverbs::Error::TimedOut) => {}
        Err(e) => panic!("expected TimedOut, got error {e:?}"),
        Ok(_) => panic!("expected TimedOut, got a connection"),
    }
    assert!(before.elapsed() >= Duration::from_millis(50));
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
        let acceptor = Acceptor::bind(addr, PortSpace::Tcp, 2).expect("bind");
        ready_tx.send(()).expect("signal ready");

        let mut received = Vec::new();
        let mut conns = Vec::new();
        for _ in 0..2 {
            let incoming = acceptor.accept(SETUP_TIMEOUT).expect("accept");
            let ctx = incoming.context().expect("server device context");
            let pd = ctx.alloc_pd().expect("server pd");
            let cq = ctx.create_cq(16).build().expect("server cq");
            let qp = pd
                .create_qp::<Rc>(&cq, &cq, 1)
                .expect("server qp builder")
                .build()
                .expect("server qp");
            let mut recv = pd
                .allocate(64, AccessFlags::PERMISSIVE)
                .expect("server recv mr");
            let mut conn = incoming
                .accept(qp, ConnectionParameter::default(), SETUP_TIMEOUT)
                .expect("accept");
            unsafe {
                conn.queue_pair()
                    .post_recv([RecvRequest::new(1, &[recv.slice(..MESSAGE.len())])])
            }
            .expect("post_recv");
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
                let resolved = Connector::new(PortSpace::Tcp)
                    .expect("connector")
                    .resolve(addr, Duration::from_secs(5))
                    .expect("resolve");
                let ctx = resolved.context().expect("client device context");
                let pd = ctx.alloc_pd().expect("client pd");
                let cq = ctx.create_cq(16).build().expect("client cq");
                let qp = pd
                    .create_qp::<Rc>(&cq, &cq, 1)
                    .expect("client qp builder")
                    .build()
                    .expect("client qp");
                let mut send = pd
                    .allocate(64, AccessFlags::PERMISSIVE)
                    .expect("client send mr");
                send.bytes_mut()[..MESSAGE.len()].fill(tag);
                let mut conn = resolved
                    .connect(qp, ConnectionParameter::default(), SETUP_TIMEOUT)
                    .expect("connect");
                let mut batch = conn.queue_pair().start_send();
                batch
                    .op()
                    .signaled()
                    .send(2, &[send.slice(..MESSAGE.len())]);
                unsafe { batch.submit() }.expect("send");
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

/// Pump events on `id` in non-blocking mode until the wanted one arrives (returning it, still
/// unacknowledged), ignoring (acknowledging) others. Mirrors how a reactor would drive the
/// connection manager: poll, and only sleep when the channel is empty. Exercises
/// [`CmId::poll_cm_event`] and [`CmId::set_nonblocking`].
fn pump_until(id: &CmId, want: CmEventType) -> CmEvent {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match id.poll_cm_event().expect("poll cm event") {
            Some(event) => {
                if event.event_type() == want {
                    assert_eq!(
                        event.status(),
                        0,
                        "event {want:?} reported a failure status"
                    );
                    return event;
                }
                // A non-matching event is acknowledged when `event` drops here.
            }
            None => {
                assert!(Instant::now() < deadline, "timed out waiting for {want:?}");
                std::thread::sleep(Duration::from_millis(5));
            }
        }
    }
}

/// The private data each side attaches to its connection request or reply. The transport pads the
/// payload on the wire, so the receiver asserts on the prefix it knows, within the reported length.
const CLIENT_PDATA: &[u8] = b"client says hi";
const SERVER_PDATA: &[u8] = b"server says welcome";

#[test]
#[ignore = "requires an RDMA device; run with `cargo test --features rdmacm -- --ignored`"]
fn low_level_connect_and_send() {
    // Drive the connection-manager state machine directly with `CmId`, instead of the blocking
    // `Connector`/`Acceptor` helpers: the passive side blocks on `get_cm_event`, while the active
    // side runs its channel non-blocking and pumps events off the file descriptor the way an event
    // loop would. Proves the low-level escape hatch can set up a working connection, and that
    // private data crosses it in both directions.
    let addr = SocketAddr::new(IpAddr::V4(device_ipv4(&open_test_device())), 18603);
    let (ready_tx, ready_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    // Passive side: bind, listen, take the request id, build and ready the queue pair, accept.
    let server = std::thread::spawn(move || {
        let listener = CmId::create(PortSpace::Tcp).expect("listener");
        listener.bind_addr(addr).expect("bind");
        listener.listen(1).expect("listen");
        ready_tx.send(()).expect("signal ready");

        let request = loop {
            let event = listener.get_cm_event().expect("listener event");
            if event.event_type() == CmEventType::ConnectRequest {
                // The client's private data rides in the connection-request event. The transport
                // pads it, so check the prefix within the reported length.
                let pdata = event
                    .private_data()
                    .expect("connect request carries private data");
                assert!(
                    pdata.len() >= CLIENT_PDATA.len(),
                    "reported private data is shorter than what the client wrote"
                );
                assert_eq!(&pdata[..CLIENT_PDATA.len()], CLIENT_PDATA);
                break event.connection_request().expect("connection request");
            }
        };
        let ctx = request.context().expect("server device context");
        let pd = ctx.alloc_pd().expect("server pd");
        let cq = ctx.create_cq(16).build().expect("server cq");
        let mut qp = pd
            .create_qp::<Rc>(&cq, &cq, 1)
            .expect("server qp builder")
            .build()
            .expect("server prepared qp")
            .into_queue_pair();
        for state in [
            QueuePairState::Init,
            QueuePairState::ReadyToReceive,
            QueuePairState::ReadyToSend,
        ] {
            let attr = request.init_qp_attr(state).expect("server init_qp_attr");
            qp.modify(&attr).expect("server modify");
        }
        let mut recv = pd
            .allocate(64, AccessFlags::PERMISSIVE)
            .expect("server recv mr");
        unsafe { qp.post_recv([RecvRequest::new(1, &[recv.slice(..MESSAGE.len())])]) }
            .expect("post_recv");

        let param = ConnectionParameter::default()
            .set_qp_num(qp.qp_num())
            .set_private_data(SERVER_PDATA);
        request.accept(&param).expect("accept");
        loop {
            let event = request.get_cm_event().expect("server event");
            if event.event_type() == CmEventType::Established {
                break;
            }
        }

        wait_for(&cq, 1);
        assert_eq!(&recv.bytes_mut()[..MESSAGE.len()], MESSAGE);
        done_tx.send(()).expect("signal done");
    });

    // Active side: drive resolution and connection non-blocking, off the channel's file descriptor.
    ready_rx.recv().expect("server ready");
    let id = CmId::create(PortSpace::Tcp).expect("client id");
    assert!(
        id.as_raw_fd() >= 0,
        "the event channel exposes a file descriptor"
    );
    id.set_nonblocking(true).expect("set_nonblocking");

    id.resolve_addr(addr, Duration::from_secs(5))
        .expect("resolve_addr");
    pump_until(&id, CmEventType::AddressResolved);
    id.resolve_route(Duration::from_secs(5))
        .expect("resolve_route");
    pump_until(&id, CmEventType::RouteResolved);

    // Once the destination has resolved, both endpoint addresses are readable off the id.
    assert_eq!(id.peer_addr(), Some(addr));
    let local = id.local_addr().expect("local address after resolution");
    assert_eq!(local.ip(), addr.ip());

    let ctx = id.context().expect("client device context");
    let pd = ctx.alloc_pd().expect("client pd");
    let cq = ctx.create_cq(16).build().expect("client cq");
    let mut qp = pd
        .create_qp::<Rc>(&cq, &cq, 1)
        .expect("client qp builder")
        .build()
        .expect("client prepared qp")
        .into_queue_pair();
    let init = id
        .init_qp_attr(QueuePairState::Init)
        .expect("client init attr");
    qp.modify(&init).expect("client init");

    let param = ConnectionParameter::default()
        .set_qp_num(qp.qp_num())
        .set_private_data(CLIENT_PDATA);
    id.connect(&param).expect("connect");
    // The server's private data rides back in the connect-response event; check the prefix
    // within the reported (transport-padded) length, then acknowledge the event by dropping it.
    let response = pump_until(&id, CmEventType::ConnectResponse);
    let pdata = response
        .private_data()
        .expect("connect response carries private data");
    assert!(
        pdata.len() >= SERVER_PDATA.len(),
        "reported private data is shorter than what the server wrote"
    );
    assert_eq!(&pdata[..SERVER_PDATA.len()], SERVER_PDATA);
    drop(response);
    for state in [QueuePairState::ReadyToReceive, QueuePairState::ReadyToSend] {
        let attr = id.init_qp_attr(state).expect("client init_qp_attr");
        qp.modify(&attr).expect("client modify");
    }
    id.establish().expect("establish");

    let mut send = pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("client send mr");
    send.bytes_mut()[..MESSAGE.len()].copy_from_slice(MESSAGE);
    let mut batch = qp.start_send();
    batch
        .op()
        .signaled()
        .send(2, &[send.slice(..MESSAGE.len())]);
    unsafe { batch.submit() }.expect("send");
    wait_for(&cq, 2);

    done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("server received the message");
    id.disconnect().ok();
    server.join().expect("server thread");
}
