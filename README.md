# ibverbs

[![Crates.io](https://img.shields.io/crates/v/ibverbs.svg)](https://crates.io/crates/ibverbs)
[![Documentation](https://docs.rs/ibverbs/badge.svg)](https://docs.rs/ibverbs/)
[![codecov](https://codecov.io/gh/jonhoo/rust-ibverbs/graph/badge.svg?token=3nylKSTA6R)](https://codecov.io/gh/jonhoo/rust-ibverbs)
[![Dependency status](https://deps.rs/repo/github/jonhoo/rust-ibverbs/status.svg)](https://deps.rs/repo/github/jonhoo/rust-ibverbs)

A safe Rust API for RDMA over InfiniBand, RoCE, and iWARP, wrapping `libibverbs`.

RDMA "verbs" let userspace talk to the network adapter directly: no system calls on the data
path, no copies, single-digit-microsecond latencies. The C API leaves you to uphold a long list
of lifetime, aliasing, and transport rules by hand. This crate encodes those rules in Rust
types: queue pairs are typed by their transport, so posting a datagram without an address
handle, or setting an RC-only timeout on a UD queue pair, is a compile error. The low-level
control stays: every wrapper hands out its raw handle for verbs the safe API does not cover.

```rust,no_run
use ibverbs::{AccessFlags, RecvRequest};

fn main() -> ibverbs::Result<()> {
    let ctx = ibverbs::devices()?.iter().next().expect("no device").open()?;

    let cq = ctx.create_cq(16).build()?;
    let pd = ctx.alloc_pd()?;

    // A reliable-connection (RC) queue pair on port 1. On RoCE, routing needs a GID; take the
    // port's routable entry (its IPv4 RoCE v2 one, when there is one).
    let gid = ctx.routable_gid(1)?.expect("no GID on port 1");
    let prepared = pd
        .create_qp::<ibverbs::Rc>(&cq, &cq, 1)?
        .set_gid_index(gid.gid_index)
        .build()?;

    // Exchange endpoints with the peer out of band (`endpoint.to_bytes()` is the wire
    // format), or use the `rdmacm` feature to negotiate connections over IP instead.
    // Here we self-connect for brevity.
    let endpoint = prepared.endpoint()?;
    let mut qp = prepared.handshake(endpoint)?;

    let mut recv = pd.allocate(4096, AccessFlags::PERMISSIVE)?;
    let mut send = pd.allocate(4096, AccessFlags::PERMISSIVE)?;
    send.bytes_mut()[..5].copy_from_slice(b"hello");

    unsafe { qp.post_recv([RecvRequest::new(1, &[recv.slice(..)])]) }?;
    let mut batch = qp.start_send();
    batch.op().signaled().send(2, &[send.slice(..5)]);
    unsafe { batch.submit() }?;

    let mut pending = 2;
    while pending > 0 {
        let mut completions = cq.poll()?;
        while let Some(wc) = completions.next() {
            wc.ok().expect("work request failed");
            pending -= 1;
        }
    }
    assert_eq!(&recv.bytes_mut()[..5], b"hello");
    Ok(())
}
```

Complete programs live in [`ibverbs/examples/`](ibverbs/examples/): a loopback transfer, an
event-driven loop multiplexing queues over one completion channel, an `ibv_devinfo`-style device
dump, doorbell batching, connection setup through the RDMA connection manager, and EFA SRD queue
pairs.

## What is covered

- Reliable and unreliable connections (RC/UC) and unreliable datagrams (UD), typed at compile
  time: builder knobs, activation, and postable operations exist only on the transports they
  apply to, and a datagram send is addressed (`.to(&ah, qpn, qkey)`) by construction.
- Device listing and typed device, port, and GID-table queries, including extended attributes
  and GID-to-netdev resolution.
- One-call connection bring-up (`handshake`, `activate`) with typed timeout/retry values, or
  validated manual state transitions (`modify`/`query`) when you want to drive the state machine
  yourself.
- Memory regions that own their buffer, plus registration of caller-managed memory
  (`register_from_raw` for mmap/hugepages, `register_dmabuf` for device memory such as GPU
  buffers) and `ibv_advise_mr`.
- Two-sided send/receive, one-sided RDMA read and write (with immediate), and atomics, all
  posted as doorbell batches: many work requests, one doorbell, with per-operation
  `signaled`/`fenced`/`solicited` modifiers, inline data, and scatter/gather lists.
- Shared receive queues, with receives posted in batches on queue pairs and SRQs alike.
- Completion handling on the extended interface: lazy-read polling, hardware completion
  timestamps, and event-driven waiting through completion channels that plug into
  `epoll`/`tokio` — including many queues multiplexed onto one file descriptor — plus
  device-level asynchronous events (port changes, queue errors, SRQ limits).
- The RDMA connection manager (`rdmacm` feature): blocking helpers with timeouts and in-band
  `private_data` exchange for the common case, and a low-level, non-blocking `CmId` API for
  event loops.
- AWS Elastic Fabric Adapter SRD queue pairs (`efa` feature).

Everything else stays reachable through `as_raw` on every wrapper and the raw bindings
re-exported as `ibverbs::ffi`.

## Safety model

- Resources are reference-counted internally; a queue pair keeps its completion queues and
  protection domain alive, so handles cannot dangle and there are no lifetime parameters to
  thread through your types.
- Transport rules are enforced by the type system and pinned by `compile_fail` tests; so are the
  posting rules — a doorbell batch borrows the queue pair until submitted, and polled
  completions are lent, so stale reads don't compile.
- Buffers registered via `allocate` are owned by the `MemoryRegion` and cannot be freed or moved
  while registered. Posting is `unsafe` with a precisely documented contract (the device may
  still be reading or writing the buffer), rather than pretending a safe signature could uphold
  it.
- Errors are a `thiserror` enum naming the failing verb; queue-pair state transitions diagnose
  exactly which attribute-mask bits were wrong, and RoCE routing failures say what was wrong
  with the route.

## Cargo features

None are enabled by default.

- `rdmacm`: the RDMA connection manager. Links `librdmacm`.
- `efa`: SRD queue pairs on AWS Elastic Fabric Adapter. Links `libefa`.
- `mlx5`: mlx5 DMA-BUF registration and Data Direct device queries. Links `libmlx5`;
  requires matching rdma-core 54+ libraries at build and run time.

### mlx5 Data Direct

Enable `mlx5` and call `ProtectionDomain::register_dmabuf_mlx5` with ordinary
`AccessFlags` and separate `Mlx5DmabufAccessFlags::DATA_DIRECT`. The exporter must
support Data Direct (for GPU memory, use its PCIe-mapped DMA-BUF export).
Registration failure is returned directly, never retried as ordinary registration.

The result is the same `MemoryRegion<()>` returned by `register_dmabuf`: it retains
its protection domain and deregisters on drop. Its slices and remote descriptors
use the requested IOVA, not a host virtual address. Keep the exported allocation
alive until the MR is dropped and finish all RDMA operations before dropping it.

`Context::mlx5_data_direct_sysfs_path` returns the provider's path bytes unchanged,
without the `/sys` mount prefix. Unsupported providers return `Error::Unsupported`;
`ENODEV` means no associated Data Direct device. Device selection and fallback policy
remain with the caller.

## Building

This crate dynamically links `libibverbs`, which is part of
[`rdma-core`](https://github.com/linux-rdma/rdma-core) (the package is `libibverbs1`, with
`libibverbs-dev` for linking, on Debian and Ubuntu; `rdma-core` on Arch; `rdma-core-devel` on
Fedora), plus `librdmacm`, `libefa`, and `libmlx5` when the corresponding features are enabled.

At build time, `bindgen` generates the bindings from the `rdma-core` headers vendored in the
`ibverbs-sys` crate, so building needs `libclang` but no RDMA development packages. To use
installed `rdma-core` headers instead, set `RDMA_CORE_INCLUDE_DIR` and `RDMA_CORE_LIB_DIR`. You do
not need to depend on `ibverbs-sys` directly: it is re-exported as `ibverbs::ffi`.
`RDMA_CORE_LIB_DIR` also works alone when using vendored headers. The `mlx5` feature
requires libraries exporting both Data Direct functions; newer headers do not upgrade
older system libraries. Set the runtime library search path as well when linking a
custom rdma-core build, and keep its provider libraries and `libibverbs` together.

The minimum supported Rust version is 1.85.

### Provider requirements

The crate drives completion queues and queue pairs exclusively through the extended verbs
(`ibv_create_cq_ex`, `ibv_create_qp_ex`, and the `ibv_wr_*` send API). Providers that implement
them include `mlx5`, `hns`, `efa`, and `rxe`; on providers that do not (for example
`mlx4`-generation hardware), creation fails cleanly (with `Error::Unsupported`) rather than
degrading to the legacy verbs.

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

## Testing without RDMA hardware

Any modern Linux kernel can attach a software RDMA device
([SoftRoCE](https://docs.kernel.org/infiniband/rxe.html)) to an ordinary network interface:

```console
$ sudo rdma link add rxe0 type rxe netdev <netdev>
```

The examples (except the EFA one, which needs EFA hardware) and the integration test suite run
against it unchanged, and CI does exactly this on every pull request: the whole data-path suite
(two-sided, one-sided, atomics, inline, UC/UD, and the connection manager) runs against a
SoftRoCE device and asserts on the transferred bytes.
