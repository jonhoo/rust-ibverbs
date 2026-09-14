//! Establish an RDMA connection with the connection manager and send one message over it.
//!
//! Run the passive side first, then the active side, pointing it at the server's address (use the
//! RDMA device's own IP, not a loopback address):
//!
//! ```text
//! cargo run --features rdmacm --example rdmacm_connect -- server 0.0.0.0:18515
//! cargo run --features rdmacm --example rdmacm_connect -- client 192.0.2.1:18515
//! ```

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use ibverbs::rdmacm::{Acceptor, ConnectionParameter, Connector, PortSpace};
use ibverbs::{AccessFlags, CompletionQueue, Rc, RecvRequest};

const MESSAGE: &[u8] = b"hello over rdmacm";

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_default();
    let addr: SocketAddr = std::env::args()
        .nth(2)
        .expect("usage: rdmacm_connect <server|client> <addr>")
        .parse()
        .expect("invalid socket address");

    match mode.as_str() {
        "server" => server(addr),
        "client" => client(addr),
        _ => panic!("usage: rdmacm_connect <server|client> <addr>"),
    }
}

/// Poll until a completion with `wr_id` arrives, checking it succeeded.
fn wait_for(cq: &CompletionQueue, wr_id: u64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut completions = cq.poll().expect("poll cq");
        while let Some(wc) = completions.next() {
            wc.ok().expect("work completion failed");
            if wc.wr_id() == wr_id {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for completion"
        );
    }
}

fn server(addr: SocketAddr) {
    let acceptor = Acceptor::bind(addr, PortSpace::Tcp, 1).unwrap();
    println!("listening on {addr}");

    // Wait for a client indefinitely, but bound the establishment of an arrived request.
    let incoming = acceptor.accept(None).unwrap();
    let ctx = incoming.context().unwrap();
    let pd = ctx.alloc_pd().unwrap();
    let cq = ctx.create_cq(16).build().unwrap();
    let qp = pd.create_qp::<Rc>(&cq, &cq, 1).unwrap().build().unwrap();
    let mut recv = pd.allocate(64, AccessFlags::PERMISSIVE).unwrap();

    let mut conn = incoming
        .accept(
            qp,
            ConnectionParameter::default(),
            Some(Duration::from_secs(10)),
        )
        .unwrap();
    println!("connected to {:?}", conn.peer_addr());
    // The queue pair is RTS now; rnr_retry keeps the peer's send retrying until this is posted.
    unsafe {
        conn.queue_pair()
            .post_recv([RecvRequest::new(1, &[recv.slice(..MESSAGE.len())])])
    }
    .unwrap();

    wait_for(&cq, 1);
    println!(
        "received: {:?}",
        String::from_utf8_lossy(&recv.bytes_mut()[..MESSAGE.len()])
    );
    conn.disconnect().ok();
}

fn client(addr: SocketAddr) {
    let resolved = Connector::new(PortSpace::Tcp)
        .unwrap()
        .resolve(addr, Duration::from_secs(5))
        .unwrap();
    let ctx = resolved.context().unwrap();
    let pd = ctx.alloc_pd().unwrap();
    let cq = ctx.create_cq(16).build().unwrap();
    let qp = pd.create_qp::<Rc>(&cq, &cq, 1).unwrap().build().unwrap();
    let mut send = pd.allocate(64, AccessFlags::PERMISSIVE).unwrap();
    send.bytes_mut()[..MESSAGE.len()].copy_from_slice(MESSAGE);

    let mut conn = resolved
        .connect(
            qp,
            ConnectionParameter::default(),
            Some(Duration::from_secs(10)),
        )
        .unwrap();
    let mut batch = conn.queue_pair().start_send();
    batch
        .op()
        .signaled()
        .send(2, &[send.slice(..MESSAGE.len())]);
    unsafe { batch.submit() }.unwrap();
    wait_for(&cq, 2);
    println!("sent: {:?}", String::from_utf8_lossy(MESSAGE));
    conn.disconnect().ok();
}
