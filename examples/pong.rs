extern crate ibverbs;

fn main() {
    let ctx = ibverbs::devices()
        .iter()
        .next()
        .expect("no rdma device available")
        .open();

    let cq = ctx.create_cq(16, 0);
    let pd = ctx.alloc_pd();

    let qp_builder = pd.create_qp(&cq, &cq, ibverbs::ibv_qp_type::IBV_QPT_RC)
        .build();

    let endpoint = qp_builder.endpoint();
    let mut qp = qp_builder.handshake(endpoint);

    let mut mr = pd.allocate::<u8>(16);
    mr[8] = 0x42;
    mr[15] = 0x41;

    unsafe { qp.post_receive(&mut mr, 8.., 2) };
    unsafe { qp.post_send(&mut mr, ..8, 1) };

    let mut sent = false;
    let mut received = false;
    let mut completions = [ibverbs::ibv_wc::default(); 16];
    while !sent || !received {
        let n = cq.poll(&mut completions[..]);
        assert!(n >= 0);
        if n == 0 {
            continue;
        }
        assert!(n <= 2);
        for wr in &completions[0..n as usize] {
            match wr.wr_id() {
                1 => {
                    assert!(!sent);
                    sent = true;
                }
                2 => {
                    assert!(!received);
                    received = true;
                    assert_eq!(mr[0], 0x42);
                    assert_eq!(mr[7], 0x41);
                }
                _ => unreachable!(),
            }
        }
    }
}
