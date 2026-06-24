//! A small `ibv_devinfo`-style tool: list every RDMA device and its ports.
//!
//! Run with `cargo run --example devinfo`. It leans on the `Debug` impls of `Device`, `DeviceAttr`,
//! and `PortAttr`, so it does little more than enumerate the devices and pretty-print them.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let devices = ibverbs::devices()?;
    if devices.iter().next().is_none() {
        println!("no RDMA devices found");
        return Ok(());
    }

    for device in &devices {
        println!("{device:#?}");
        let ctx = match device.open() {
            Ok(ctx) => ctx,
            Err(e) => {
                println!("    could not open device: {e}");
                continue;
            }
        };
        let attr = ctx.query_device()?;
        println!("{attr:#?}");
        for port in 1..=attr.phys_port_cnt {
            println!("port {port}: {:#?}", ctx.query_port(port)?);
        }
    }
    Ok(())
}
