fn main() {
    let ctx = ibverbs::devices()
        .unwrap()
        .iter()
        .next()
        .expect("no rdma device available")
        .open()
        .unwrap();

    let cq = ctx.create_cq(16, 0).unwrap();
    let pd = ctx.alloc_pd().unwrap();

    let qp_builder = pd
        .create_qp(&cq, &cq, ibverbs::ibv_qp_type::IBV_QPT_RC)
        .unwrap()
        .set_gid_index(1)
        .build()
        .unwrap();

    let endpoint = qp_builder.endpoint().unwrap();
    let mut qp = qp_builder.handshake(endpoint).unwrap();

    let mut mr = pd.allocate(16).unwrap();
    mr.inner_mut()[9] = 0x42;

    unsafe { qp.post_receive(&[mr.slice(..8)], 2) }.unwrap();
    unsafe { qp.post_send(&[mr.slice(8..)], 1) }.unwrap();

    let mut sent = false;
    let mut received = false;
    let mut completions = [ibverbs::ibv_wc::default(); 16];
    while !sent || !received {
        let completed = cq.poll(&mut completions[..]).unwrap();
        if completed.is_empty() {
            continue;
        }
        assert!(completed.len() <= 2);
        for wr in completed {
            match wr.wr_id() {
                1 => {
                    assert!(!sent);
                    sent = true;
                    println!("sent");
                }
                2 => {
                    assert!(!received);
                    received = true;
                    assert_eq!(mr.inner_mut()[1], 0x42);
                    println!("received");
                }
                _ => unreachable!(),
            }
        }
    }
}
