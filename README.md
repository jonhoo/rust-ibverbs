# ibverbs

[![Crates.io](https://img.shields.io/crates/v/ibverbs.svg)](https://crates.io/crates/ibverbs)
[![Documentation](https://docs.rs/ibverbs/badge.svg)](https://docs.rs/ibverbs/)
[![codecov](https://codecov.io/gh/jonhoo/rust-ibverbs/graph/badge.svg?token=3nylKSTA6R)](https://codecov.io/gh/jonhoo/rust-ibverbs)
[![Dependency status](https://deps.rs/repo/github/jonhoo/rust-ibverbs/status.svg)](https://deps.rs/repo/github/jonhoo/rust-ibverbs)

A safe Rust API for RDMA over InfiniBand, RoCE, and iWARP, wrapping `libibverbs`.

RDMA "verbs" let userspace talk to the network adapter directly: no system calls on the data
path, no copies, single-digit-microsecond latencies. The C API leaves you to uphold a long list
of lifetime and aliasing rules by hand. This crate encodes those rules in Rust types, without
taking the low-level control away: every wrapper hands out its raw handle for verbs the safe API
does not cover.

```rust,no_run
fn main() -> ibverbs::Result<()> {
    let ctx = ibverbs::devices()?.iter().next().expect("no device").open()?;

    let cq = ctx.create_cq(16).build()?;
    let pd = ctx.alloc_pd()?;

    // On RoCE, routing needs a GID; pick the index of a routable entry from `ctx.gid_table()?`.
    let prepared = pd
        .create_qp(&cq, &cq, ibverbs::ibv_qp_type::IBV_QPT_RC)?
        .set_gid_index(1)
        .build()?;

    // Exchange endpoints with the peer out of band (they serialize with the `serde`
    // feature), or use the `rdmacm` feature to negotiate connections over IP instead.
    // Here we self-connect for brevity.
    let endpoint = prepared.endpoint()?;
    let mut qp = prepared.handshake(endpoint)?;

    let mut recv = pd.allocate(4096)?;
    let mut send = pd.allocate(4096)?;
    send.bytes_mut()[..5].copy_from_slice(b"hello");
    unsafe { qp.post_receive(&[recv.slice(..)], 1) }?;
    unsafe { qp.post_send(&[send.slice(..5)], 2) }?;

    let mut pending = 2;
    while pending > 0 {
        if let Some(mut completions) = cq.poll()? {
            while let Some(wc) = completions.next() {
                wc.ok().expect("work request failed");
                pending -= 1;
            }
        }
    }
    assert_eq!(&recv.bytes_mut()[..5], b"hello");
    Ok(())
}
```

More complete programs live in [`ibverbs/examples/`](ibverbs/examples/): a loopback transfer, an
`ibv_devinfo`-style device dump, doorbell batching, connection setup through the RDMA connection
manager, and EFA SRD queue pairs.

## What is covered

- Device listing and typed device, port, and GID-table queries (including extended device
  attributes and GID-to-netdev resolution).
- RC, UC, and UD queue pairs, with a builder for their many knobs, one-call bring-up
  (`handshake`, `activate_ud`), and validated manual state transitions (`modify`/`query`) when
  you want to drive `INIT`/`RTR`/`RTS` yourself.
- Memory regions that own their buffer, plus registration of caller-managed memory
  (`register_from_raw` for mmap/hugepages, `register_dmabuf` for device memory such as GPU
  buffers) and `ibv_advise_mr`.
- Two-sided send/receive, one-sided RDMA read and write (with immediate), and atomics
  (compare-and-swap, fetch-and-add), all also available as doorbell batches through the extended
  `ibv_wr_*` API: many work requests, one doorbell, with per-operation `signaled`/`fenced`/inline
  modifiers.
- Inline sends, scatter/gather lists, and shared receive queues.
- Completion queues on the extended (`ibv_cq_ex`) interface: batched lazy-read polling, optional
  hardware completion timestamps, and event-driven waiting through completion channels that plug
  into `epoll`/`tokio` (`AsFd`), including many queues multiplexed onto one descriptor.
- The RDMA connection manager (`rdmacm` feature): connection setup over IP addresses, with
  blocking helpers for the common case and a low-level, non-blocking `CmId` API for event loops.
- AWS Elastic Fabric Adapter SRD queue pairs (`efa` feature).

Everything else is reachable through the escape hatches: every wrapper exposes `as_raw`, the raw
bindings are re-exported as `ibverbs::ffi`, and escape-hatch constructors such as
`QueuePairAttribute::from_raw` and `PreparedQueuePair::into_queue_pair` let you mix safe and raw
freely.

## Safety model

The crate aims for APIs that are misuse-resistant without costing data-path performance:

- Resources are reference-counted internally; a queue pair keeps its completion queues and
  protection domain alive, so handles cannot dangle, and there are no lifetime parameters to
  thread through your types.
- Polling is a lending iterator: a `WorkCompletion` cannot be held across `next()`, so you cannot
  read fields of a completion the hardware cursor has moved past. Field reads are lazy; you only
  pay for what you read.
- Doorbell batches borrow the queue pair mutably until submitted, enforced at compile time (there
  are `compile_fail` tests for this).
- Buffers registered via `allocate` are owned by the `MemoryRegion`, so the memory cannot be
  freed or moved while registered. Posting is `unsafe` with a precisely documented contract (the
  device may still be reading/writing the buffer), rather than pretending a safe signature could
  uphold it.
- Errors are a `thiserror` enum with the failing verb and errno attached; queue-pair state
  transitions diagnose exactly which attribute-mask bits were wrong.

## Building

The compiled crate dynamically links `libibverbs` (part of
[`rdma-core`](https://github.com/linux-rdma/rdma-core); packaged as `libibverbs-dev` on
Debian/Ubuntu, `rdma-core` on Arch, `rdma-core-devel` on Fedora), plus `librdmacm` and `libefa`
with the corresponding features.

At build time, bindings are generated from a vendored `rdma-core` checkout, built automatically
by `ibverbs-sys` (this needs `cmake` and a C toolchain, but no RDMA packages). To use pre-built
rdma-core headers instead, set `RDMA_CORE_INCLUDE_DIR` and `RDMA_CORE_LIB_DIR`.

The minimum supported Rust version is 1.82.

## Testing without RDMA hardware

Any modern Linux kernel can attach a software RDMA device
([SoftRoCE](https://docs.kernel.org/infiniband/rxe.html)) to an ordinary network interface:

```console
$ sudo rdma link add rxe0 type rxe netdev <netdev>
```

The examples and the integration test suite run against it unchanged. CI does exactly this on
every pull request: the data-path tests (send/receive, RDMA read and write, doorbell batching,
shared receive queues, timestamps, event-driven completion, and more) run against a SoftRoCE
device and assert on the transferred bytes. A few tests cover paths the CI runner's rxe module
mishandles (atomics, UC/UD, inline sends) and are skipped there; they pass on real hardware and
current kernels.

## Workspace layout

- [`ibverbs`](ibverbs/) is the safe wrapper, and what you almost certainly want.
- [`ibverbs-sys`](ibverbs-sys/) holds the raw bindgen bindings and the vendored `rdma-core`. You
  do not need to depend on it directly: the `ibverbs` crate re-exports it as `ibverbs::ffi`.

## Documentation

Much of the documentation of this crate borrows heavily from the excellent posts over at
[RDMAmojo](http://www.rdmamojo.com/). If you are going to be working a lot with ibverbs, chances
are you will want to head over there. In particular, [this overview
post](http://www.rdmamojo.com/2012/05/18/libibverbs/) may be a good place to start.

For more information on RDMA verbs in general, see the [InfiniBand Architecture
Specification](http://www.infinibandta.org/content/pages.php?pg=technology_public_specification)
vol. 1, especially chapter 11, the RDMA Consortium's [RDMA Protocol Verbs
Specification](https://tools.ietf.org/html/rfc5040), the upstream
[`libibverbs/verbs.h`](https://github.com/linux-rdma/rdma-core/blob/master/libibverbs/verbs.h)
definitions, the manpages for the `ibv_*` functions, and the upstream [C
examples](https://github.com/linux-rdma/rdma-core/tree/master/libibverbs/examples).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
