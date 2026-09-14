//! A safe Rust API for RDMA (`libibverbs`).
//!
//! RDMA "verbs" let userspace perform high-throughput, low-latency network operations directly
//! against the network adapter (zero copies on the data path and no kernel involvement) over
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
//! // On RoCE, routing needs a GID; take the port's routable entry (its IPv4 RoCE v2 one, when
//! // there is one).
//! let gid = ctx.routable_gid(1)?.expect("no GID on port 1");
//! let prepared = pd
//!     .create_qp::<ibverbs::Rc>(&cq, &cq, 1)?
//!     .set_gid_index(gid.gid_index)
//!     .build()?;
//!
//! // Exchange endpoints with the peer out of band (`QueuePairEndpoint::to_bytes` is the wire
//! // format), or let the `rdmacm` feature's connection manager negotiate the connection over IP.
//! // This example self-connects the queue pair, so the "exchange" is with itself.
//! let endpoint = prepared.endpoint()?;
//! let mut qp = prepared.handshake(endpoint)?;
//!
//! // Register memory with the device and post work requests: a receive, and a send that loops
//! // back into it.
//! let mut recv = pd.allocate(4096, ibverbs::AccessFlags::PERMISSIVE)?;
//! let mut send = pd.allocate(4096, ibverbs::AccessFlags::PERMISSIVE)?;
//! send.bytes_mut()[..5].copy_from_slice(b"hello");
//! unsafe { qp.post_recv([ibverbs::RecvRequest::new(/* wr_id */ 1, &[recv.slice(..)])]) }?;
//! let mut batch = qp.start_send();
//! batch.op().signaled().send(/* wr_id */ 2, &[send.slice(..5)]);
//! unsafe { batch.submit() }?;
//!
//! // Poll the completion queue until both work requests have completed.
//! let mut pending = 2;
//! while pending > 0 {
//!     let mut completions = cq.poll()?;
//!     while let Some(wc) = completions.next() {
//!         wc.ok().expect("work request failed");
//!         pending -= 1;
//!     }
//! }
//! assert_eq!(&recv.bytes_mut()[..5], b"hello");
//! # Ok(())
//! # }
//! ```
//!
//! Runnable programs live in the [`examples/` directory][examples] — a loopback transfer, an
//! event-driven loop over a shared completion channel, an `ibv_devinfo`-style device dump,
//! doorbell batching, the `rdmacm` connection manager, and EFA SRD queue pairs. You can run all
//! of them except the EFA example (and this crate's test suite) without RDMA hardware on any
//! modern Linux kernel using [SoftRoCE][soft]: `rdma link add rxe0 type rxe netdev <netdev>`.
//!
//! You do not have to poll: completion channels ([`Context::create_comp_channel`]) deliver
//! completion notifications on a file descriptor to block on ([`CompletionChannel::wait`]) or hand
//! to an `epoll`/`tokio` reactor, and the device's out-of-band reports — port state changes,
//! queue-pair errors, SRQ low-watermark warnings — arrive the same two ways through the
//! asynchronous-event API ([`Context::wait_async_event`], or [`Context::async_fd`] plus
//! [`Context::poll_async_event`] from a reactor).
//!
//! # Cargo features
//!
//! - `rdmacm`: the `rdmacm` module, wrapping the `librdmacm` connection manager: connection setup
//!   over IP addresses, blocking or event-loop driven, instead of an out-of-band endpoint
//!   exchange. Links `librdmacm`.
//! - `efa`: SRD queue pairs on AWS Elastic Fabric Adapter, via
//!   `ProtectionDomain::create_srd_qp`. Links `libefa`.
//!
//! # Library dependency
//!
//! At runtime, this crate dynamically links `libibverbs`, which is part of [`rdma-core`] (the
//! package is `libibverbs1`, with `libibverbs-dev` for linking, on Debian and Ubuntu; `rdma-core`
//! on Arch; `rdma-core-devel` on Fedora), plus `librdmacm` and `libefa` when the corresponding
//! features are enabled.
//!
//! At build time, the bindings are generated from a vendored [`rdma-core`] checkout, whose headers
//! `ibverbs-sys` generates by running `cmake`'s configure step (nothing is compiled, so this needs
//! `cmake` and a C compiler for its probes, but no RDMA development packages). To generate
//! bindings from pre-built `rdma-core` headers instead, set `RDMA_CORE_INCLUDE_DIR` and
//! `RDMA_CORE_LIB_DIR`.
//!
//! The crate drives completion queues and queue pairs exclusively through rdma-core's extended
//! verbs (`ibv_create_cq_ex`, and `ibv_create_qp_ex` with the `ibv_wr_*` send API), so it needs a
//! provider that implements them — of the in-tree rdma-core providers, `mlx5`, `hns`, `efa`, and
//! `rxe` (SoftRoCE) implement both. A provider that lacks them fails cleanly at completion-queue
//! or queue-pair creation (with [`Error::Unsupported`]) rather than degrading to the
//! legacy verbs.
//!
//! # Typed transports
//!
//! The queue-pair family is typed by its transport (see [`Transport`]): [`Rc`] supports sends,
//! RDMA read/write, and atomics; [`Uc`] sends and RDMA writes; [`Ud`] (and, behind the `efa`
//! feature, `Srd`) datagram sends addressed through an [`AddressHandle`]. Transport-specific
//! operations only exist on the matching types, so using one on the wrong transport is a compile
//! error. The queue-pair types without a marker (raw packet, XRC, and driver-specific types other
//! than EFA's SRD) are not usable through the portable wrapper anyway; if that changes, they will
//! get their own markers.
//!
//! # Thread safety
//!
//! The underlying ibverbs API [is thread safe][safe], and the wrapper types here are `Send` and
//! `Sync` where that holds. Handles like [`Context`], [`ProtectionDomain`], and
//! [`CompletionQueue`] can be shared freely across threads. Operations whose verbs contracts are
//! per-caller are encoded in the types instead: posting work requests takes `&mut QueuePair`
//! (wrap the queue pair in a lock to post from several threads), a [`SendBatch`] borrows its
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

/// Implements the shared surface of the crate's flag newtypes: associated constants for each
/// known flag, `empty`/`contains`, the bit operators, a `Debug` that lists the names of the set
/// flags, and lossless conversions to and from the corresponding raw ffi bitfield type.
macro_rules! flags_newtype {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident($ffi:ty) {
            $( $(#[$cmeta:meta])* $cname:ident = $fconst:ident; )+
        }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Default, PartialEq, Eq, Hash)]
        $vis struct $name(pub(crate) u32);

        impl $name {
            $(
                $(#[$cmeta])*
                pub const $cname: $name = $name(<$ffi>::$fconst.0);
            )+

            /// No flags set.
            #[must_use]
            pub const fn empty() -> Self {
                $name(0)
            }

            /// Whether every flag set in `other` is also set in `self`.
            #[must_use]
            pub const fn contains(self, other: Self) -> bool {
                self.0 & other.0 == other.0
            }
        }

        impl ::std::ops::BitOr for $name {
            type Output = Self;
            fn bitor(self, rhs: Self) -> Self {
                $name(self.0 | rhs.0)
            }
        }

        impl ::std::ops::BitOrAssign for $name {
            fn bitor_assign(&mut self, rhs: Self) {
                self.0 |= rhs.0;
            }
        }

        impl ::std::ops::BitAnd for $name {
            type Output = Self;
            fn bitand(self, rhs: Self) -> Self {
                $name(self.0 & rhs.0)
            }
        }

        impl ::std::fmt::Debug for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(concat!(stringify!($name), "("))?;
                let mut rest = self.0;
                let mut first = true;
                $(
                    if $name::$cname.0 != 0 && rest & $name::$cname.0 == $name::$cname.0 {
                        if !first {
                            f.write_str(" | ")?;
                        }
                        f.write_str(stringify!($cname))?;
                        first = false;
                        rest &= !$name::$cname.0;
                    }
                )+
                if rest != 0 {
                    if !first {
                        f.write_str(" | ")?;
                    }
                    write!(f, "{rest:#x}")?;
                    first = false;
                }
                if first {
                    f.write_str("0")?;
                }
                f.write_str(")")
            }
        }

        impl From<$ffi> for $name {
            fn from(raw: $ffi) -> Self {
                $name(raw.0)
            }
        }

        impl From<$name> for $ffi {
            fn from(flags: $name) -> Self {
                Self(flags.0)
            }
        }
    };
}

mod address;
mod completion;
mod context;
mod device;
mod error;
mod mr;
mod pd;
mod qp;
mod srq;

#[cfg(feature = "efa")]
mod efa;

#[cfg(feature = "efa")]
pub use efa::Srd;

pub use address::*;
pub use completion::*;
pub use context::*;
pub use device::*;
pub use error::*;
pub use mr::*;
pub use pd::*;
pub use qp::*;
pub use srq::*;

#[cfg(feature = "rdmacm")]
pub mod rdmacm;

/// The raw `libibverbs` bindings (the `ibverbs-sys` crate), re-exported.
///
/// This is the escape hatch of last resort: every wrapper in this crate exposes the raw handle it
/// owns (see the `as_raw` methods), which can be passed to any verb here that the safe API does
/// not cover — without adding a separate dependency on `ibverbs-sys` and keeping its version in
/// sync.
pub use ffi;
