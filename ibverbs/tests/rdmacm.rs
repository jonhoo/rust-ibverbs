//! Integration test for the RDMA connection manager (`rdmacm` feature).
//!
//! Each test connects client and server queue pairs across two threads in one process, using the
//! connection manager to set up the connection over the device's own IP address (Soft-RoCE works
//! for this).
//! Like the other data-path tests it needs a real RDMA device, so it is `#[ignore]`d; run it with
//! `cargo test --features rdmacm -- --ignored`. It uses the first device, or `IBVERBS_TEST_DEVICE`.
#![cfg(feature = "rdmacm")]

use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::os::fd::AsRawFd;
use std::sync::{mpsc, Arc, Barrier};
use std::time::{Duration, Instant};

use ibverbs::rdmacm::{
    Acceptor, CmEvent, CmEventType, CmId, ConnectionParameter, Connector, PortSpace,
};
use ibverbs::{
    AccessFlags, AckTimeout, CompletionQueue, Context, Error, GidType, QueuePairAttributeMask,
    QueuePairEndpoint, QueuePairState, Rc, RecvRequest, RemoteMemorySlice, WcStatus,
};

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

/// The device's RoCEv2 IPv4 address, read from its GID table. The connection manager needs to
/// route over an IP address; prefer the RoCEv2 entry holding the interface's IPv4 address (an
/// IPv4-mapped GID).
fn device_ipv4(ctx: &Context) -> Ipv4Addr {
    let gids = ctx.gid_table().expect("failed to read GID table");
    for entry in gids {
        if entry.gid_type == GidType::RoceV2 && entry.gid.is_ipv4_mapped() {
            if let Some(ipv4) = std::net::Ipv6Addr::from(entry.gid).to_ipv4_mapped() {
                return ipv4;
            }
        }
    }
    panic!("no RoCEv2 IPv4 GID found; the connection manager test needs an IP-addressed device");
}

/// Poll `cq` until a completion with `wr_id` arrives (or time out), checking it succeeded.
fn wait_for(cq: &CompletionQueue, wr_id: u64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut completions = cq.poll().expect("failed to poll CQ");
        while let Some(wc) = completions.next() {
            wc.ok()
                .unwrap_or_else(|e| panic!("work request {} failed: {e}", wc.wr_id()));
            if wc.wr_id() == wr_id {
                return;
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

/// How long to keep retrying a bind that fails with `ENODEV`. Right after a Soft-RoCE device is
/// attached, the kernel connection manager can lag a moment behind the GID table in associating
/// the interface's address with the device, and reports that as "no such device".
const BIND_RETRY: Duration = Duration::from_secs(5);

/// Whether a bind failure is the transient `ENODEV` worth retrying.
fn is_transient_bind_error(err: &Error) -> bool {
    matches!(err, Error::BindAddress(e) if e.raw_os_error() == Some(nix::libc::ENODEV))
}

/// Binds a listening acceptor on an ephemeral port of `ip` (so concurrent test runs never collide
/// on a port), retrying a transient `ENODEV` for a while. Read the actual address back with
/// [`Acceptor::local_addr`].
fn bind_acceptor(ip: IpAddr, backlog: u32) -> Acceptor {
    let addr = SocketAddr::new(ip, 0);
    let deadline = Instant::now() + BIND_RETRY;
    loop {
        match Acceptor::bind(addr, PortSpace::Tcp, backlog) {
            Ok(acceptor) => return acceptor,
            Err(e) if is_transient_bind_error(&e) && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => panic!("bind {addr}: {e:?}"),
        }
    }
}

/// The low-level counterpart of [`bind_acceptor`]: a bound (not yet listening) [`CmId`] on an
/// ephemeral port of `ip`.
fn bind_listener(ip: IpAddr) -> CmId {
    let addr = SocketAddr::new(ip, 0);
    let deadline = Instant::now() + BIND_RETRY;
    loop {
        let id = CmId::create(PortSpace::Tcp).expect("listener");
        match id.bind_addr(addr) {
            Ok(()) => return id,
            Err(e) if is_transient_bind_error(&e) && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => panic!("bind {addr}: {e:?}"),
        }
    }
}

#[test]
#[ignore = "requires an RDMA device; run with `cargo test --features rdmacm -- --ignored`"]
fn connect_and_send() {
    let ip = IpAddr::V4(device_ipv4(&open_test_device()));
    let (ready_tx, ready_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    // Passive side: bind, accept, receive the message.
    let server = std::thread::spawn(move || {
        let acceptor = bind_acceptor(ip, 1);
        let addr = acceptor.local_addr().expect("listen address");
        ready_tx.send(addr).expect("signal ready");

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
    let addr = ready_rx.recv().expect("server ready");
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
    let acceptor = bind_acceptor(IpAddr::V4(device_ipv4(&open_test_device())), 1);
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
    let ip = IpAddr::V4(device_ipv4(&open_test_device()));
    let (ready_tx, ready_rx) = mpsc::channel();
    // Holds every side connected until all three threads have finished their transfer, so no
    // connection is torn down early.
    let barrier = Arc::new(Barrier::new(3));

    let server_barrier = barrier.clone();
    let server = std::thread::spawn(move || {
        let acceptor = bind_acceptor(ip, 2);
        let addr = acceptor.local_addr().expect("listen address");
        ready_tx.send(addr).expect("signal ready");

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

    let addr = ready_rx.recv().expect("server ready");
    let clients: Vec<_> = b"AB"
        .iter()
        .copied()
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

/// The private data the server attaches to its reply; the client's request carries its endpoint
/// encoding instead. Longer than a request may carry (56 bytes), since a reply may carry 196.
const SERVER_PDATA: &[u8] =
    b"server says welcome, and keeps on saying it for well over fifty-six bytes of reply";

#[test]
#[ignore = "requires an RDMA device; run with `cargo test --features rdmacm -- --ignored`"]
fn low_level_connect_and_send() {
    // Drive the connection-manager state machine directly with `CmId`, instead of the blocking
    // `Connector`/`Acceptor` helpers: the passive side blocks on `get_cm_event`, while the active
    // side runs its channel non-blocking and pumps events off the file descriptor the way an event
    // loop would. Proves the low-level escape hatch can set up a working connection, and that
    // private data crosses it in both directions.
    let ip = IpAddr::V4(device_ipv4(&open_test_device()));
    let (ready_tx, ready_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    // Passive side: bind, listen, take the request id, build and ready the queue pair, accept.
    let server = std::thread::spawn(move || {
        let listener = bind_listener(ip);
        listener.listen(1).expect("listen");
        let addr = listener.local_addr().expect("listen address");
        ready_tx.send(addr).expect("signal ready");

        let request = loop {
            let event = listener.get_cm_event().expect("listener event");
            if event.event_type() == CmEventType::ConnectRequest {
                // The client sent its queue-pair endpoint's wire encoding as private data (the
                // in-band bootstrap the fixed format enables). The transport pads the payload, so
                // decode the known-length prefix.
                let pdata = event
                    .private_data()
                    .expect("connect request carries private data");
                assert!(
                    pdata.len() >= QueuePairEndpoint::WIRE_LEN,
                    "reported private data is shorter than an endpoint encoding"
                );
                let bytes: [u8; QueuePairEndpoint::WIRE_LEN] =
                    pdata[..QueuePairEndpoint::WIRE_LEN].try_into().unwrap();
                let client_endpoint =
                    QueuePairEndpoint::from_bytes(&bytes).expect("well-formed endpoint bytes");
                assert!(client_endpoint.qp_num > 0, "{client_endpoint:?}");
                assert!(client_endpoint.gid.is_none(), "{client_endpoint:?}");
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
    let addr = ready_rx.recv().expect("server ready");
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
    let qp = pd
        .create_qp::<Rc>(&cq, &cq, 1)
        .expect("client qp builder")
        .build()
        .expect("client prepared qp");
    let client_endpoint = qp.endpoint().expect("client endpoint");
    let mut qp = qp.into_queue_pair();
    let init = id
        .init_qp_attr(QueuePairState::Init)
        .expect("client init attr");
    qp.modify(&init).expect("client init");

    let param = ConnectionParameter::default()
        .set_qp_num(qp.qp_num())
        .set_private_data(&client_endpoint.to_bytes());
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
    // The `Init` attributes computed before connecting carried no remote-access flags; now that
    // the connection exists, applying `Init` again grants the peer the negotiated RDMA access.
    let init = id
        .init_qp_attr(QueuePairState::Init)
        .expect("client init attr after the response");
    assert!(
        init.access_flags().contains(AccessFlags::REMOTE_WRITE),
        "the connection's INIT attributes grant remote write: {:?}",
        init.access_flags()
    );
    qp.modify(&init).expect("client re-init");
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

/// Bring the passive side of a low-level connection to `RTS` and accept it, returning once the
/// connection is established.
fn accept_low_level(request: &CmId, qp: &mut ibverbs::QueuePair<Rc>) {
    for state in [
        QueuePairState::Init,
        QueuePairState::ReadyToReceive,
        QueuePairState::ReadyToSend,
    ] {
        let attr = request.init_qp_attr(state).expect("server init_qp_attr");
        qp.modify(&attr).expect("server modify");
    }
    let param = ConnectionParameter::default().set_qp_num(qp.qp_num());
    request.accept(&param).expect("accept");
    loop {
        let event = request.get_cm_event().expect("server event");
        if event.event_type() == CmEventType::Established {
            break;
        }
    }
}

#[test]
#[ignore = "requires an RDMA device; run with `cargo test --features rdmacm -- --ignored`"]
fn server_writes_into_client() {
    // A peer-initiated one-sided operation aimed at the active side: the client hands the server a
    // remote slice in its request's private data, the server RDMA-writes the message into it and
    // then sends a one-byte notification, which the client receives before checking the buffer.
    // Catches an active-side queue pair brought up without its remote-access flags.
    let ip = IpAddr::V4(device_ipv4(&open_test_device()));
    let (ready_tx, ready_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    let server = std::thread::spawn(move || {
        let acceptor = bind_acceptor(ip, 1);
        ready_tx
            .send(acceptor.local_addr().expect("listen address"))
            .expect("signal ready");
        let incoming = acceptor.accept(SETUP_TIMEOUT).expect("accept");
        let pdata = incoming.peer_private_data();
        assert!(
            pdata.len() >= RemoteMemorySlice::WIRE_LEN,
            "the request carries a remote slice"
        );
        let bytes: [u8; RemoteMemorySlice::WIRE_LEN] =
            pdata[..RemoteMemorySlice::WIRE_LEN].try_into().unwrap();
        let target = RemoteMemorySlice::from_bytes(&bytes).expect("well-formed remote slice");

        let ctx = incoming.context().expect("server device context");
        let pd = ctx.alloc_pd().expect("server pd");
        let cq = ctx.create_cq(16).build().expect("server cq");
        let qp = pd
            .create_qp::<Rc>(&cq, &cq, 1)
            .expect("server qp builder")
            .set_max_send_wr(4)
            .build()
            .expect("server qp");
        let mut src = pd
            .allocate(64, AccessFlags::PERMISSIVE)
            .expect("server source mr");
        src.bytes_mut()[..MESSAGE.len()].copy_from_slice(MESSAGE);
        let notify = pd
            .allocate(1, AccessFlags::PERMISSIVE)
            .expect("server notify mr");
        let mut conn = incoming
            .accept(qp, ConnectionParameter::default(), SETUP_TIMEOUT)
            .expect("accept");

        let mut batch = conn.queue_pair().start_send();
        batch
            .op()
            .signaled()
            .write(1, &[src.slice(..MESSAGE.len())], target);
        batch.op().signaled().send(2, &[notify.slice(..)]);
        unsafe { batch.submit() }.expect("submit");
        wait_for(&cq, 1);
        wait_for(&cq, 2);
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("client saw the write");
    });

    let addr = ready_rx.recv().expect("server ready");
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
    let target = pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("client target mr");
    let notify = pd
        .allocate(1, AccessFlags::PERMISSIVE)
        .expect("client notify mr");
    let param = ConnectionParameter::default()
        .set_private_data(&target.remote().slice(..MESSAGE.len()).to_bytes());
    let mut conn = resolved.connect(qp, param, SETUP_TIMEOUT).expect("connect");
    unsafe {
        conn.queue_pair()
            .post_recv([RecvRequest::new(1, &[notify.slice(..)])])
    }
    .expect("post_recv");
    wait_for(&cq, 1);
    assert_eq!(&target.bytes()[..MESSAGE.len()], MESSAGE);
    done_tx.send(()).expect("signal done");
    server.join().expect("server thread");
}

#[test]
#[ignore = "requires an RDMA device; run with `cargo test --features rdmacm -- --ignored`"]
fn accept_applies_rd_atomic_parameters() {
    // The outstanding-RDMA limits the passive side advertises in its reply are also what its queue
    // pair is configured with, and the active side ends up with the matching (swapped) pair.
    let ip = IpAddr::V4(device_ipv4(&open_test_device()));
    let (ready_tx, ready_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let rd_atomic =
        QueuePairAttributeMask::MAX_QP_RD_ATOMIC | QueuePairAttributeMask::MAX_DEST_RD_ATOMIC;

    let server = std::thread::spawn(move || {
        let acceptor = bind_acceptor(ip, 1);
        ready_tx
            .send(acceptor.local_addr().expect("listen address"))
            .expect("signal ready");
        let incoming = acceptor.accept(SETUP_TIMEOUT).expect("accept");
        let ctx = incoming.context().expect("server device context");
        let pd = ctx.alloc_pd().expect("server pd");
        let cq = ctx.create_cq(16).build().expect("server cq");
        let qp = pd
            .create_qp::<Rc>(&cq, &cq, 1)
            .expect("server qp builder")
            .build()
            .expect("server qp");
        let param = ConnectionParameter::default()
            .set_responder_resources(4)
            .set_initiator_depth(2);
        let mut conn = incoming.accept(qp, param, SETUP_TIMEOUT).expect("accept");
        let (attr, _) = conn.queue_pair().query(rd_atomic).expect("server query");
        assert_eq!(attr.max_dest_rd_atomic(), 4, "server responder resources");
        assert_eq!(attr.max_rd_atomic(), 2, "server initiator depth");
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("client done");
    });

    let addr = ready_rx.recv().expect("server ready");
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
    let mut conn = resolved
        .connect(qp, ConnectionParameter::default(), SETUP_TIMEOUT)
        .expect("connect");
    let (attr, _) = conn.queue_pair().query(rd_atomic).expect("client query");
    assert_eq!(
        attr.max_rd_atomic(),
        4,
        "client initiator depth = server responder resources"
    );
    assert_eq!(
        attr.max_dest_rd_atomic(),
        2,
        "client responder resources = server initiator depth"
    );
    done_tx.send(()).expect("signal done");
    server.join().expect("server thread");
}

#[test]
#[ignore = "requires an RDMA device; run with `cargo test --features rdmacm -- --ignored`"]
fn rejected_request_reports_reason_and_private_data() {
    let ip = IpAddr::V4(device_ipv4(&open_test_device()));
    let (ready_tx, ready_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    let server = std::thread::spawn(move || {
        let acceptor = bind_acceptor(ip, 1);
        ready_tx
            .send(acceptor.local_addr().expect("listen address"))
            .expect("signal ready");
        let incoming = acceptor.accept(SETUP_TIMEOUT).expect("accept");
        incoming.reject(b"nope").expect("reject");
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("client saw the rejection");
    });

    let addr = ready_rx.recv().expect("server ready");
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
    match resolved.connect(qp, ConnectionParameter::default(), SETUP_TIMEOUT) {
        Err(Error::ConnectionManager {
            event: CmEventType::Rejected,
            status,
            private_data,
        }) => {
            // 28 is the InfiniBand CM's "consumer defined" reject reason: the peer itself said no.
            assert_eq!(status, 28, "reject reason");
            assert!(
                private_data.len() >= 4 && &private_data[..4] == b"nope",
                "the rejection carries the peer's private data: {private_data:?}"
            );
        }
        Err(other) => panic!("expected a rejection, got {other:?}"),
        Ok(_) => panic!("expected a rejection, got a connection"),
    }
    done_tx.send(()).expect("signal done");
    server.join().expect("server thread");
}

#[test]
#[ignore = "requires an RDMA device; run with `cargo test --features rdmacm -- --ignored`"]
fn dropped_connect_request_is_rejected() {
    // A listener that drops a connection-request event without taking its id declines the
    // request, so the peer fails fast instead of retrying until its timeout.
    let ip = IpAddr::V4(device_ipv4(&open_test_device()));
    let (ready_tx, ready_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    let server = std::thread::spawn(move || {
        let listener = bind_listener(ip);
        listener.listen(1).expect("listen");
        ready_tx
            .send(listener.local_addr().expect("listen address"))
            .expect("signal ready");
        let event = listener.get_cm_event().expect("listener event");
        assert_eq!(event.event_type(), CmEventType::ConnectRequest);
        drop(event);
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("client saw the rejection");
    });

    let addr = ready_rx.recv().expect("server ready");
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
    let before = Instant::now();
    match resolved.connect(qp, ConnectionParameter::default(), SETUP_TIMEOUT) {
        Err(Error::ConnectionManager {
            event: CmEventType::Rejected,
            ..
        }) => {}
        Err(other) => panic!("expected a rejection, got {other:?}"),
        Ok(_) => panic!("expected a rejection, got a connection"),
    }
    assert!(
        before.elapsed() < Duration::from_secs(5),
        "the rejection arrived promptly, not after the request's retries"
    );
    done_tx.send(()).expect("signal done");
    server.join().expect("server thread");
}

#[test]
#[ignore = "requires an RDMA device; run with `cargo test --features rdmacm -- --ignored`"]
fn disconnect_flushes_outstanding_receives() {
    let ip = IpAddr::V4(device_ipv4(&open_test_device()));
    let (ready_tx, ready_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    let server = std::thread::spawn(move || {
        let acceptor = bind_acceptor(ip, 1);
        ready_tx
            .send(acceptor.local_addr().expect("listen address"))
            .expect("signal ready");
        let incoming = acceptor.accept(SETUP_TIMEOUT).expect("accept");
        let ctx = incoming.context().expect("server device context");
        let pd = ctx.alloc_pd().expect("server pd");
        let cq = ctx.create_cq(16).build().expect("server cq");
        let qp = pd
            .create_qp::<Rc>(&cq, &cq, 1)
            .expect("server qp builder")
            .build()
            .expect("server qp");
        let _conn = incoming
            .accept(qp, ConnectionParameter::default(), SETUP_TIMEOUT)
            .expect("accept");
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("client done");
    });

    let addr = ready_rx.recv().expect("server ready");
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
    let recv = pd
        .allocate(64, AccessFlags::PERMISSIVE)
        .expect("client recv mr");
    let mut conn = resolved
        .connect(qp, ConnectionParameter::default(), SETUP_TIMEOUT)
        .expect("connect");
    unsafe {
        conn.queue_pair()
            .post_recv([RecvRequest::new(7, &[recv.slice(..)])])
    }
    .expect("post_recv");
    conn.disconnect().expect("disconnect");

    // The receive never got a message; disconnecting flushes it with an error completion.
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = 'flushed: loop {
        let mut completions = cq.poll().expect("poll");
        while let Some(wc) = completions.next() {
            if wc.wr_id() == 7 {
                break 'flushed wc.ok().map_err(|e| e.status);
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for the flushed receive"
        );
    };
    assert_eq!(status, Err(WcStatus::WorkRequestFlushed));
    done_tx.send(()).expect("signal done");
    server.join().expect("server thread");
}

#[test]
#[ignore = "requires an RDMA device; run with `cargo test --features rdmacm -- --ignored`"]
fn dropping_connection_disconnects_peer() {
    // Dropping a `Connection` sends the disconnect right away, even though other resources built
    // on the same device context (which keep the underlying id alive) are still around.
    let ip = IpAddr::V4(device_ipv4(&open_test_device()));
    let (ready_tx, ready_rx) = mpsc::channel();

    let server = std::thread::spawn(move || {
        let listener = bind_listener(ip);
        listener.listen(1).expect("listen");
        ready_tx
            .send(listener.local_addr().expect("listen address"))
            .expect("signal ready");
        let request = loop {
            let event = listener.get_cm_event().expect("listener event");
            if event.event_type() == CmEventType::ConnectRequest {
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
        accept_low_level(&request, &mut qp);
        // Then watch for the peer going away.
        request.set_nonblocking(true).expect("set_nonblocking");
        pump_until(&request, CmEventType::Disconnected);
    });

    let addr = ready_rx.recv().expect("server ready");
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
    let conn = resolved
        .connect(qp, ConnectionParameter::default(), SETUP_TIMEOUT)
        .expect("connect");
    // `ctx`, `pd`, and `cq` (all holding the connection's id) outlive the connection itself.
    drop(conn);
    server.join().expect("server thread saw the disconnect");
    drop((cq, pd, ctx));
}

#[test]
#[ignore = "requires an RDMA device; run with `cargo test --features rdmacm -- --ignored`"]
fn connect_rejects_oversized_private_data() {
    let id = CmId::create(PortSpace::Tcp).expect("id");
    // 57 bytes is one more than a TCP-port-space request can carry; the parameter itself holds
    // up to a reply's worth (196 bytes), which the request check rejects too.
    for len in [57, 196] {
        let param = ConnectionParameter::default().set_private_data(&vec![0x5a; len]);
        match id.connect(&param) {
            Err(Error::Connect(e)) if e.kind() == io::ErrorKind::InvalidInput => {}
            other => panic!("expected an InvalidInput error for {len} bytes, got {other:?}"),
        }
    }
}

#[test]
fn blocking_helpers_need_a_connected_port_space() {
    // Checked before anything touches a device, so this needs none.
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    for port_space in [PortSpace::Udp, PortSpace::Ipoib] {
        assert!(
            matches!(Connector::new(port_space), Err(Error::ConnectionSetup(e)) if e.kind() == io::ErrorKind::InvalidInput),
            "Connector refuses {port_space}"
        );
        assert!(
            matches!(Acceptor::bind(addr, port_space, 1), Err(Error::ConnectionSetup(e)) if e.kind() == io::ErrorKind::InvalidInput),
            "Acceptor refuses {port_space}"
        );
    }
}

#[test]
#[should_panic(expected = "limited to 196 bytes")]
fn private_data_beyond_any_limit_panics() {
    let _ = ConnectionParameter::default().set_private_data(&[0; 197]);
}

/// The id options and the `cm_id` accessors of the blocking helpers: reuse-address lets bound ids
/// share a port, and the type of service and ACK timeout set on a connector before resolving
/// survive a connection round trip.
#[test]
#[ignore = "requires an RDMA device; run with `cargo test --features rdmacm -- --ignored`"]
fn cm_id_options_and_accessors() {
    let ip = IpAddr::V4(device_ipv4(&open_test_device()));

    // Two bound ids share a port when both allow reuse; a third without the option is refused.
    let first = bind_listener(ip);
    let addr = first.local_addr().expect("bound address");
    // `bind_listener` binds before any option can be set, so bind a fresh id with reuse first.
    drop(first);
    let first = CmId::create(PortSpace::Tcp).expect("first id");
    first.set_reuse_addr(true).expect("set_reuse_addr");
    first.bind_addr(addr).expect("first bind");
    let second = CmId::create(PortSpace::Tcp).expect("second id");
    second.set_reuse_addr(true).expect("set_reuse_addr");
    second
        .bind_addr(addr)
        .expect("a second id shares the port with reuse-address set");
    let third = CmId::create(PortSpace::Tcp).expect("third id");
    match third.bind_addr(addr) {
        Err(Error::BindAddress(e)) => {
            assert_eq!(e.raw_os_error(), Some(nix::libc::EADDRINUSE), "{e}");
        }
        other => panic!("expected EADDRINUSE without reuse-address, got {other:?}"),
    }
    drop((first, second, third));

    let (ready_tx, ready_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let acceptor = Acceptor::bind_with(SocketAddr::new(ip, 0), PortSpace::Tcp, 1, |id| {
            id.set_tos(0x08)?;
            id.set_reuse_addr(true)
        })
        .expect("bind_with");
        let addr = acceptor.cm_id().local_addr().expect("listen address");
        assert_eq!(acceptor.local_addr(), Some(addr));
        ready_tx.send(addr).expect("signal ready");
        let incoming = acceptor.accept(SETUP_TIMEOUT).expect("accept");
        assert_eq!(incoming.cm_id().local_addr(), Some(addr));
        assert!(incoming.cm_id().peer_addr().is_some());
        incoming
            .cm_id()
            .set_ack_timeout(AckTimeout::from_exponent(14))
            .expect("set_ack_timeout before accepting");
        let ctx = incoming.context().expect("server device context");
        let pd = ctx.alloc_pd().expect("server pd");
        let cq = ctx.create_cq(16).build().expect("server cq");
        let qp = pd
            .create_qp::<Rc>(&cq, &cq, 1)
            .expect("server qp builder")
            .build()
            .expect("server qp");
        let conn = incoming
            .accept(qp, ConnectionParameter::default(), SETUP_TIMEOUT)
            .expect("accept");
        assert_eq!(conn.cm_id().peer_addr(), conn.peer_addr());
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("client done");
    });

    let addr = ready_rx.recv().expect("server ready");
    let connector = Connector::new(PortSpace::Tcp).expect("connector");
    connector
        .cm_id()
        .set_tos(0x08)
        .expect("set_tos before resolving");
    connector
        .cm_id()
        .set_ack_timeout(AckTimeout::from_exponent(14))
        .expect("set_ack_timeout before resolving");
    let resolved = connector
        .resolve(addr, Duration::from_secs(5))
        .expect("resolve");
    assert_eq!(resolved.cm_id().peer_addr(), Some(addr));
    let ctx = resolved.context().expect("client device context");
    let pd = ctx.alloc_pd().expect("client pd");
    let cq = ctx.create_cq(16).build().expect("client cq");
    let qp = pd
        .create_qp::<Rc>(&cq, &cq, 1)
        .expect("client qp builder")
        .build()
        .expect("client qp");
    let conn = resolved
        .connect(qp, ConnectionParameter::default(), SETUP_TIMEOUT)
        .expect("connect");
    assert_eq!(conn.cm_id().peer_addr(), Some(addr));
    assert_eq!(conn.cm_id().local_addr(), conn.local_addr());
    done_tx.send(()).expect("signal done");
    server.join().expect("server thread");
}
