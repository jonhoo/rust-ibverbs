//! Minimal EFA SRD example: two SRD queue pairs on one device, one sends a datagram to the other.
//!
//! The only EFA-specific call is `create_srd_qp`; from there, building, activating, and the data
//! path are the same API every other transport uses.
//!
//! Requires an AWS Elastic Fabric Adapter (EFA) device and the `efa` feature
//! (`cargo run --features efa --example efa_srd`). This example follows the EFA documentation but
//! has not been run on EFA hardware; validate before relying on it.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    const QKEY: u32 = 0x1111_2222;

    let device = ibverbs::devices()?
        .iter()
        .next()
        .ok_or("no RDMA device found")?
        .open()?;
    let cq = device.create_cq(16).build()?;
    let pd = device.alloc_pd()?;
    let gid_index = device.routable_gid(1)?.ok_or("no GID on port 1")?.gid_index;

    // Two SRD queue pairs. SRD is connectionless, so each is just brought to ready with a Q_Key.
    let mut sender = pd
        .create_srd_qp(&cq, &cq, 1)?
        .set_gid_index(gid_index)
        .build()?
        .activate(QKEY)?;

    let receiver_prepared = pd
        .create_srd_qp(&cq, &cq, 1)?
        .set_gid_index(gid_index)
        .build()?;
    let receiver_endpoint = receiver_prepared.endpoint()?;
    let mut receiver = receiver_prepared.activate(QKEY)?;

    // Address the receiver by its GID.
    let receiver_gid = receiver_endpoint.gid.ok_or("EFA requires a GID")?;
    let mut ah_attr = ibverbs::AddressHandleAttribute::new(1);
    ah_attr.set_grh(receiver_gid, gid_index as u8, 64, 0);
    let ah = pd.create_address_handle(&ah_attr)?;

    let mut send_buf = pd.allocate(64, ibverbs::AccessFlags::PERMISSIVE)?;
    let recv_buf = pd.allocate(64, ibverbs::AccessFlags::PERMISSIVE)?;
    send_buf.bytes_mut()[..5].copy_from_slice(b"hello");

    // Post the receive, then the send. Both go through the normal, non-EFA-specific API.
    unsafe { receiver.post_recv([ibverbs::RecvRequest::new(1, &[recv_buf.slice(..)])]) }?;
    let mut batch = sender.start_send();
    batch
        .to(&ah, receiver_endpoint.qp_num, QKEY)
        .signaled()
        .send(2, &[send_buf.slice(..5)]);
    unsafe { batch.submit() }?;

    // Wait for the receive and send completions.
    let mut recv_len = None;
    let mut got_send = false;
    while recv_len.is_none() || !got_send {
        let mut completions = cq.poll()?;
        while let Some(wc) = completions.next() {
            if let Err(e) = wc.ok() {
                return Err(format!("work request {} failed: {e}", wc.wr_id()).into());
            }
            match wc.wr_id() {
                1 => recv_len = Some(wc.len()),
                2 => got_send = true,
                _ => {}
            }
        }
    }

    // A datagram receive is prefixed by the 40-byte GRH, so the payload starts at offset 40.
    let len = recv_len.unwrap();
    println!("received {len} bytes: {:?}", &recv_buf.bytes()[40..len]);
    Ok(())
}
