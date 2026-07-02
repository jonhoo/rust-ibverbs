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

    // Routing needs a GID on RoCE, and not every table entry routes: on the queue pair's port
    // (1, the default; GID indices are per port), prefer the RoCEv2 entry holding the interface's
    // IPv4 address (an IPv4-mapped GID, `::ffff:a.b.c.d`), which is the one plain-Ethernet setups
    // actually answer on, and fall back to the port's first entry otherwise.
    let gids = ctx.gid_table().unwrap();
    let gid_index = gids
        .iter()
        .filter(|e| e.port_num == 1)
        .find(|e| {
            e.gid_type == ibverbs::ibv_gid_type::IBV_GID_TYPE_ROCE_V2
                && e.gid.subnet_prefix() == 0
                && e.gid.interface_id() >> 32 == 0xffff
        })
        .or_else(|| gids.iter().find(|e| e.port_num == 1))
        .expect("no GID available")
        .gid_index;

    let qp_builder = pd
        .create_qp(&cq, &cq, ibverbs::ibv_qp_type::IBV_QPT_RC)
        .unwrap()
        .set_gid_index(gid_index)
        .build()
        .unwrap();

    // Both sides of a connection exchange endpoints and handshake; connected to ourselves, we
    // "exchange" with ourselves.
    let endpoint = qp_builder.endpoint().unwrap();
    let mut qp = qp_builder.handshake(endpoint).unwrap();

    let mut mr = pd.allocate(16).unwrap();
    mr.bytes_mut()[9] = 0x42;

    // Receive into the first half of the buffer what we send from the second half. Note that byte
    // 9 of the region is byte 1 of the posted send slice, and lands in byte 1 of the receive half.
    unsafe { qp.post_receive(&[mr.slice(..8)], 2) }.unwrap();
    unsafe { qp.post_send(&[mr.slice(8..)], 1) }.unwrap();

    let mut sent = false;
    let mut received = false;
    while !sent || !received {
        let Some(mut completions) = cq.poll().unwrap() else {
            continue;
        };
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
