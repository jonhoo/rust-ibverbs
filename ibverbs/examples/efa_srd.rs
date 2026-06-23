//! Minimal EFA SRD example: two SRD queue pairs on one device, one sends a datagram to the other.
//!
//! The only EFA-specific calls are `create_srd_qp` / `build_srd` / `activate_srd`; everything else
//! (`post_send_ud`, `post_recv`, `poll`) is the same API every other transport uses.
//!
//! Requires an AWS Elastic Fabric Adapter (EFA) device and the `efa` feature
//! (`cargo run --features efa --example efa_srd`). It is illustrative: the data path should be
//! validated on real EFA hardware.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    const GID_INDEX: u32 = 0;
    const QKEY: u32 = 0x1111_2222;

    let device = ibverbs::devices()?
        .iter()
        .next()
        .ok_or("no RDMA device found")?
        .open()?;
    let cq = device.create_cq(16, 0)?;
    let pd = device.alloc_pd()?;

    // Two SRD queue pairs. SRD is connectionless, so each is just brought to ready with a Q_Key.
    let mut sender = pd
        .create_srd_qp(&cq, &cq)?
        .set_gid_index(GID_INDEX)
        .build_srd()?
        .activate_srd(QKEY)?;

    let receiver_prepared = pd
        .create_srd_qp(&cq, &cq)?
        .set_gid_index(GID_INDEX)
        .build_srd()?;
    let receiver_endpoint = receiver_prepared.endpoint()?;
    let mut receiver = receiver_prepared.activate_srd(QKEY)?;

    // Address the receiver by its GID.
    let receiver_gid = receiver_endpoint.gid.ok_or("EFA requires a GID")?;
    let mut ah_attr = ibverbs::AddressHandleAttribute::new();
    ah_attr.set_grh(receiver_gid, GID_INDEX as u8, 64, 0);
    let ah = pd.create_address_handle(&ah_attr)?;

    let mut send_buf = pd.allocate(64)?;
    let recv_buf = pd.allocate(64)?;
    send_buf.bytes_mut()[..5].copy_from_slice(b"hello");

    // Post the receive, then the send. Both go through the normal, non-EFA-specific API.
    unsafe { receiver.post_receive(&[recv_buf.slice(..)], 1) }?;
    unsafe { sender.post_send_ud(&[send_buf.slice(..5)], &ah, receiver_endpoint.num, QKEY, 2) }?;

    // Wait for the receive and send completions.
    let mut recv_len = None;
    let mut got_send = false;
    while recv_len.is_none() || !got_send {
        let Some(mut completions) = cq.poll()? else {
            continue;
        };
        while let Some(wc) = completions.next() {
            if let Err((status, _)) = wc.ok() {
                return Err(format!("work request {} failed: {status:?}", wc.wr_id()).into());
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
