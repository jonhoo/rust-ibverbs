//! A safe Rust API for RDMA (`libibverbs`).
//!
//! RDMA "verbs" let userspace perform high-throughput, low-latency network operations directly
//! against the network adapter — zero copies on the data path and no kernel involvement — over
//! InfiniBand, RoCE, and iWARP transports. This crate wraps both the control path (creating,
//! querying, and tearing down resources such as protection domains, completion queues, queue
//! pairs, and memory regions) and the data path (posting work requests and reaping completions) in
//! safe Rust types whose lifetimes and aliasing rules encode the verbs contracts, while keeping
//! escape hatches (`as_raw` on every wrapper, and the re-exported [`ffi`] bindings) for anything
//! it does not yet cover.
//!
//! # Quick start
//!
//! The path to a working connection is always the same: open a device [`Context`], allocate a
//! [`ProtectionDomain`] and a [`CompletionQueue`], build a [`QueuePair`], exchange
//! [`QueuePairEndpoint`]s with the peer, and connect with [`PreparedQueuePair::handshake`]. Then
//! register memory, post work requests, and poll for their completions:
//!
//! ```no_run
//! # fn main() -> ibverbs::Result<()> {
//! let ctx = ibverbs::devices()?
//!     .iter()
//!     .next()
//!     .expect("no rdma device available")
//!     .open()?;
//!
//! let cq = ctx.create_cq(16).build()?;
//! let pd = ctx.alloc_pd()?;
//!
//! // On RoCE, routing needs a GID; pick the index of a suitable entry in `ctx.gid_table()?`.
//! let prepared = pd
//!     .create_qp(&cq, &cq, ibverbs::ibv_qp_type::IBV_QPT_RC)?
//!     .set_gid_index(1)
//!     .build()?;
//!
//! // Exchange endpoints with the peer out of band (they are serializable with the `serde`
//! // feature), or let the `rdmacm` feature's connection manager negotiate the connection over IP.
//! // This example self-connects the queue pair, so the "exchange" is with itself.
//! let endpoint = prepared.endpoint()?;
//! let mut qp = prepared.handshake(endpoint)?;
//!
//! // Register memory with the device and post work requests: a receive, and a send that loops
//! // back into it.
//! let mut recv = pd.allocate(4096)?;
//! let mut send = pd.allocate(4096)?;
//! send.bytes_mut()[..5].copy_from_slice(b"hello");
//! unsafe { qp.post_receive(&[recv.slice(..)], /* wr_id */ 1) }?;
//! unsafe { qp.post_send(&[send.slice(..5)], /* wr_id */ 2) }?;
//!
//! // Poll the completion queue until both work requests have completed.
//! let mut pending = 2;
//! while pending > 0 {
//!     if let Some(mut completions) = cq.poll()? {
//!         while let Some(wc) = completions.next() {
//!             wc.ok().expect("work request failed");
//!             pending -= 1;
//!         }
//!     }
//! }
//! assert_eq!(&recv.bytes_mut()[..5], b"hello");
//! # Ok(())
//! # }
//! ```
//!
//! Runnable programs live in the [`examples/` directory][examples] — a loopback transfer, an
//! `ibv_devinfo`-style device dump, doorbell batching, the `rdmacm` connection manager, and EFA
//! SRD queue pairs. You can run all of them (and this crate's test suite) without RDMA hardware on
//! any modern Linux kernel using [SoftRoCE][soft]: `rdma link add rxe0 type rxe netdev <netdev>`.
//!
//! # Cargo features
//!
//! - `serde` *(default)*: [`QueuePairEndpoint`] and [`RemoteMemorySlice`] implement
//!   `Serialize`/`Deserialize`, for sending to the peer during connection setup.
//! - `rdmacm`: the `rdmacm` module, wrapping the `librdmacm` connection manager: connection setup
//!   over IP addresses, blocking or event-loop driven, instead of an out-of-band endpoint
//!   exchange. Links `librdmacm`.
//! - `efa`: SRD queue pairs on AWS Elastic Fabric Adapter, via
//!   `ProtectionDomain::create_srd_qp`. Links `libefa`.
//!
//! # Library dependency
//!
//! At runtime, this crate dynamically links `libibverbs` (part of [`rdma-core`], packaged as
//! `libibverbs-dev` on Debian/Ubuntu and `libibverbs` or `rdma-core-devel` elsewhere), plus
//! `librdmacm` and `libefa` when the corresponding features are enabled.
//!
//! At build time, the bindings are generated from a vendored [`rdma-core`] checkout, which
//! `ibverbs-sys` builds automatically (this requires `cmake` and a C toolchain, but nothing
//! RDMA-specific to be installed). To generate bindings from pre-built rdma-core headers instead,
//! set `RDMA_CORE_INCLUDE_DIR` and `RDMA_CORE_LIB_DIR`.
//!
//! # Thread safety
//!
//! The underlying ibverbs API [is thread safe][safe], and the wrapper types here are `Send` and
//! `Sync` where that holds. Handles like [`Context`], [`ProtectionDomain`], and
//! [`CompletionQueue`] can be shared freely across threads. Operations whose verbs contracts are
//! per-caller are encoded in the types instead: posting work requests takes `&mut QueuePair`
//! (wrap the queue pair in a lock to post from several threads), a [`PostBatch`] borrows its
//! queue pair until submitted, and the views handed out during a poll ([`Completions`],
//! [`WorkCompletion`]) borrow the queue and cannot outlive or escape it.
//!
//! # Resource cleanup
//!
//! Wrappers return their resource to the device when dropped, in dependency order (internal
//! reference counts keep, for example, a completion queue alive until the last queue pair built on
//! it is gone). If the device rejects a teardown — which can only happen when raw handles obtained
//! through the escape hatches still reference the resource — the drop panics rather than silently
//! leak the resource.
//!
//! # For the detail-oriented
//!
//! The control path is implemented through system calls to the `uverbs` kernel module, which
//! further calls the low-level hardware driver. The data path goes through a low-level hardware
//! library (the provider) which, in most cases, talks to the device directly — bypassing the
//! kernel and its network stack, with zero copies and an asynchronous I/O model.
//!
//! For more information on RDMA verbs, see the [InfiniBand Architecture Specification][infini]
//! vol. 1, especially chapter 11, and the RDMA Consortium's [RDMA Protocol Verbs
//! Specification][RFC5040]. See also the upstream [`libibverbs/verbs.h`] file for the original C
//! definitions, the manpages for the `ibv_*` functions, and the upstream [C examples].
//!
//! # Documentation
//!
//! Much of the documentation of this crate borrows heavily from the excellent posts over at
//! [RDMAmojo]. If you are going to be working a lot with ibverbs, chances are you will want to
//! head over there. In particular, [this overview post][overview] may be a good place to start.
//!
//! [`rdma-core`]: https://github.com/linux-rdma/rdma-core
//! [`libibverbs/verbs.h`]: https://github.com/linux-rdma/rdma-core/blob/master/libibverbs/verbs.h
//! [C examples]: https://github.com/linux-rdma/rdma-core/tree/master/libibverbs/examples
//! [examples]: https://github.com/jonhoo/rust-ibverbs/tree/main/ibverbs/examples
//! [infini]: http://www.infinibandta.org/content/pages.php?pg=technology_public_specification
//! [RFC5040]: https://tools.ietf.org/html/rfc5040
//! [safe]: http://www.rdmamojo.com/2013/07/26/libibverbs-thread-safe-level/
//! [soft]: https://docs.kernel.org/infiniband/rxe.html
//! [RDMAmojo]: http://www.rdmamojo.com/
//! [overview]: http://www.rdmamojo.com/2012/05/18/libibverbs/

#![deny(missing_docs)]
#![warn(rust_2018_idioms)]
// avoid warnings about RDMAmojo, iWARP, InfiniBand, etc. not being in backticks
#![allow(clippy::doc_markdown)]

mod ah;
mod completion;
mod context;
mod device;
mod error;
mod gid;
mod mr;
mod pd;
mod qp;
mod srq;

#[cfg(feature = "efa")]
mod efa;

pub use ah::*;
pub use completion::*;
pub use context::*;
pub use device::*;
pub use error::*;
pub use gid::*;
pub use mr::*;
pub use pd::*;
pub use qp::*;
pub use srq::*;

pub(crate) const PORT_NUM: u8 = 1;

#[cfg(feature = "rdmacm")]
pub mod rdmacm;

/// The raw `libibverbs` bindings (the `ibverbs-sys` crate), re-exported.
///
/// This is the escape hatch of last resort: every wrapper in this crate exposes the raw handle it
/// owns (see the `as_raw` methods), which can be passed to any verb here that the safe API does
/// not cover — without adding a separate dependency on `ibverbs-sys` and keeping its version in
/// sync.
pub use ffi;

pub use ffi::ibv_gid_type;
pub use ffi::ibv_mtu;
pub use ffi::ibv_port_state;
pub use ffi::ibv_qp_attr_mask;
pub use ffi::ibv_qp_state;
pub use ffi::ibv_qp_type;
pub use ffi::ibv_send_wr;
pub use ffi::ibv_transport_type;
pub use ffi::ibv_wc;
pub use ffi::ibv_wc_flags;
pub use ffi::ibv_wc_opcode;
pub use ffi::ibv_wc_status;

/// Optional work-completion fields to request via [`CompletionQueueBuilder::set_wc_flags`].
pub use ffi::ibv_create_cq_wc_flags;

/// The raw device-wide attributes wrapped by [`DeviceAttr`] (returned by [`Context::query_device`]).
pub use ffi::ibv_device_attr;
/// The raw per-port attributes wrapped by [`PortAttr`] (returned by [`Context::query_port`]).
pub use ffi::ibv_port_attr;

/// Advice for [`ProtectionDomain::advise_mr`] (the `IBV_ADVISE_MR_ADVICE_*` values).
pub use ffi::ib_uverbs_advise_mr_advice as ibv_advise_mr_advice;
/// Flags for [`ProtectionDomain::advise_mr`] (the `IBV_ADVISE_MR_FLAG_*` values).
pub use ffi::ib_uverbs_advise_mr_flag as ibv_advise_mr_flags;

/// Access flags for use with `QueuePair` and `MemoryRegion`.
pub use ffi::ibv_access_flags;

/// Default access flags.
pub const DEFAULT_ACCESS_FLAGS: ffi::ibv_access_flags = ffi::ibv_access_flags(
    ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0
        | ffi::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE.0
        | ffi::ibv_access_flags::IBV_ACCESS_REMOTE_READ.0
        | ffi::ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC.0
        | ffi::ibv_access_flags::IBV_ACCESS_RELAXED_ORDERING.0,
);
