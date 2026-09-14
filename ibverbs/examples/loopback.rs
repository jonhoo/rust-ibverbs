//! The smallest complete data-path program: a reliable-connected queue pair connected to itself,
//! posting a receive and a send, and polling both completions.
//!
//! This runs against the first RDMA device; on a machine without one, create a SoftRoCE device
//! with `rdma link add rxe0 type rxe netdev <netdev>`.

fn main() {
    let ctx = ibverbs::devices()
        .unwrap()
        .iter()
        .next()
        .expect("no rdma device available")
        .open()
        .unwrap();

    let cq = ctx.create_cq(16).build().unwrap();
    let pd = ctx.alloc_pd().unwrap();

    // Routing needs a GID on RoCE, and not every table entry routes; `routable_gid` picks the
    // port's entry that does (its IPv4 RoCE v2 one on a plain-Ethernet setup).
    let gid_index = ctx
        .routable_gid(1)
        .unwrap()
        .expect("no GID available")
        .gid_index;

    let prepared = pd
        .create_qp::<ibverbs::Rc>(&cq, &cq, 1)
        .unwrap()
        .set_gid_index(gid_index)
        .build()
        .unwrap();

    // Both sides of a connection exchange endpoints and handshake; connected to ourselves, we
    // "exchange" with ourselves.
    let endpoint = prepared.endpoint().unwrap();
    let mut qp = prepared.handshake(endpoint).unwrap();

    let mut mr = pd.allocate(16, ibverbs::AccessFlags::PERMISSIVE).unwrap();
    mr.bytes_mut()[9] = 0x42;

    // Receive into the first half of the buffer what we send from the second half. Note that byte
    // 9 of the region is byte 1 of the posted send slice, and lands in byte 1 of the receive half.
    unsafe { qp.post_recv([ibverbs::RecvRequest::new(2, &[mr.slice(..8)])]) }.unwrap();
    let mut batch = qp.start_send();
    batch.op().signaled().send(1, &[mr.slice(8..)]);
    unsafe { batch.submit() }.unwrap();

    let mut sent = false;
    let mut received = false;
    while !sent || !received {
        let mut completions = cq.poll().unwrap();
        while let Some(wc) = completions.next() {
            wc.ok().expect("work request failed");
            match wc.wr_id() {
                1 => {
                    assert!(!sent);
                    sent = true;
                    println!("sent");
                }
                2 => {
                    assert!(!received);
                    received = true;
                    assert_eq!(mr.bytes_mut()[1], 0x42);
                    println!("received");
                }
                _ => unreachable!(),
            }
        }
    }
}
