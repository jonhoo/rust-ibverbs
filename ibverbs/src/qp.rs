use std::io;
use std::os::raw::c_void;
use std::ptr;
use std::sync::Arc;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use ffi::ibv_mtu;

use crate::ah::{AddressHandle, AddressHandleAttribute};
use crate::completion::CompletionQueueInner;
use crate::error::{Error, Result};
use crate::gid::Gid;
use crate::mr::{LocalMemorySlice, RemoteMemorySlice};
use crate::pd::ProtectionDomainInner;
use crate::srq::SharedReceiveQueue;

#[cfg(doc)]
use crate::{CompletionQueue, ProtectionDomain};

/// An unconfigured `QueuePair`.
///
/// A `QueuePairBuilder` is used to configure a `QueuePair` before it is allocated and initialized.
/// To construct one, use `ProtectionDomain::create_qp`. See also [RDMAmojo] for many more details.
///
/// [RDMAmojo]: http://www.rdmamojo.com/2013/01/12/ibv_modify_qp/
pub struct QueuePairBuilder {
    pub(crate) ctx: isize,
    pub(crate) pd: Arc<ProtectionDomainInner>,
    pub(crate) port_attr: ffi::ibv_port_attr,
    /// the device port this queue pair is associated with (numbered from 1)
    pub(crate) port_num: u8,

    pub(crate) send: Arc<CompletionQueueInner>,
    pub(crate) max_send_wr: u32,
    pub(crate) recv: Arc<CompletionQueueInner>,
    pub(crate) max_recv_wr: u32,

    pub(crate) gid_index: Option<u32>,
    pub(crate) max_send_sge: u32,
    pub(crate) max_recv_sge: u32,
    pub(crate) max_inline_data: u32,

    qp_type: ffi::ibv_qp_type,

    // carried along to handshake phase
    /// traffic class set in Global Routing Headers, only used if `gid_index` is set.
    pub(crate) traffic_class: u8,
    /// only valid for RC and UC
    access: Option<ffi::ibv_access_flags>,
    /// only valid for RC
    timeout: Option<u8>,
    /// only valid for RC
    retry_count: Option<u8>,
    /// only valid for RC
    rnr_retry: Option<u8>,
    /// only valid for RC
    min_rnr_timer: Option<u8>,
    /// only valid for RC
    max_rd_atomic: Option<u8>,
    /// only valid for RC
    max_dest_rd_atomic: Option<u8>,
    /// only valid for RC and UC
    path_mtu: Option<ibv_mtu>,
    /// only valid for RC and UC
    rq_psn: Option<u32>,
    /// service level (0-15). Higher value means higher priority.
    pub(crate) service_level: u8,
    /// shared receive queue
    srq: Option<SharedReceiveQueue>,
}

impl QueuePairBuilder {
    /// Prepare a new `QueuePair` builder.
    ///
    /// `max_send_wr` is the maximum number of outstanding Work Requests that can be posted to the
    /// Send Queue in that Queue Pair. Value must be in `[0..dev_cap.max_qp_wr]`. There may be RDMA
    /// devices that for specific transport types may support less outstanding Work Requests than
    /// the maximum reported value.
    ///
    /// Similarly, `max_recv_wr` is the maximum number of outstanding Work Requests that can be
    /// posted to the Receive Queue in that Queue Pair. Value must be in `[0..dev_cap.max_qp_wr]`.
    /// There may be RDMA devices that for specific transport types may support less outstanding
    /// Work Requests than the maximum reported value. This value is ignored if the Queue Pair is
    /// associated with an SRQ
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        pd: Arc<ProtectionDomainInner>,
        port_attr: ffi::ibv_port_attr,
        port_num: u8,
        send: Arc<CompletionQueueInner>,
        max_send_wr: u32,
        recv: Arc<CompletionQueueInner>,
        max_recv_wr: u32,
        qp_type: ffi::ibv_qp_type,
        max_send_sge: u32,
        max_recv_sge: u32,
    ) -> QueuePairBuilder {
        let port_active_mtu = port_attr.active_mtu;
        QueuePairBuilder {
            ctx: 0,
            pd,
            port_attr,
            port_num,

            gid_index: None,
            traffic_class: 0,
            send,
            max_send_wr,
            recv,
            max_recv_wr,

            max_send_sge,
            max_recv_sge,
            max_inline_data: 0,

            qp_type,

            access: (qp_type == ffi::ibv_qp_type::IBV_QPT_RC
                || qp_type == ffi::ibv_qp_type::IBV_QPT_UC)
                .then_some(ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE),
            min_rnr_timer: (qp_type == ffi::ibv_qp_type::IBV_QPT_RC).then_some(16),
            retry_count: (qp_type == ffi::ibv_qp_type::IBV_QPT_RC).then_some(6),
            rnr_retry: (qp_type == ffi::ibv_qp_type::IBV_QPT_RC).then_some(6),
            timeout: (qp_type == ffi::ibv_qp_type::IBV_QPT_RC).then_some(4),
            max_rd_atomic: (qp_type == ffi::ibv_qp_type::IBV_QPT_RC).then_some(1),
            max_dest_rd_atomic: (qp_type == ffi::ibv_qp_type::IBV_QPT_RC).then_some(1),
            path_mtu: (qp_type == ffi::ibv_qp_type::IBV_QPT_RC
                || qp_type == ffi::ibv_qp_type::IBV_QPT_UC)
                .then_some(port_active_mtu),
            rq_psn: (qp_type == ffi::ibv_qp_type::IBV_QPT_RC
                || qp_type == ffi::ibv_qp_type::IBV_QPT_UC)
                .then_some(0),
            service_level: 0,
            srq: None,
        }
    }

    /// Set the access flags for the new `QueuePair`.
    ///
    /// Valid only for RC and UC QPs.
    ///
    /// Defaults to `IBV_ACCESS_LOCAL_WRITE`.
    pub fn set_access(&mut self, access: ffi::ibv_access_flags) -> &mut Self {
        if self.qp_type == ffi::ibv_qp_type::IBV_QPT_RC
            || self.qp_type == ffi::ibv_qp_type::IBV_QPT_UC
        {
            self.access = Some(access);
        }
        self
    }

    /// Set the access flags of the new `QueuePair` such that it allows remote reads and writes.
    ///
    /// Valid only for RC and UC QPs.
    pub fn allow_remote_rw(&mut self) -> &mut Self {
        if self.qp_type == ffi::ibv_qp_type::IBV_QPT_RC
            || self.qp_type == ffi::ibv_qp_type::IBV_QPT_UC
        {
            self.access = Some(
                self.access.expect("always set to Some in new")
                    | ffi::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
                    | ffi::ibv_access_flags::IBV_ACCESS_REMOTE_READ,
            );
        }
        self
    }

    /// Set the service level of the new `QueuePair`.
    /// service level (0-15). Higher value means higher priority.
    /// Defaults to 0.
    pub fn set_service_level(&mut self, service_level: u8) -> &mut Self {
        self.service_level = service_level;
        self
    }

    /// Sets the GID table index that should be used for the new `QueuePair`.
    /// The entry corresponds to the index in `Context::gid_table()`. This is only used if the
    /// `QueuePairEndpoint` that is passed to `QueuePair::handshake()` has a `gid`.
    ///
    /// Defaults to unset.
    pub fn set_gid_index(&mut self, gid_index: u32) -> &mut Self {
        self.gid_index = Some(gid_index);
        self
    }

    /// Sets the traffic class of the Global Routing Headers (GRH).
    ///
    /// This value is only used if a `gid_index` was specified. Using this value, the originator
    /// of the packets specifies the required delivery priority for handling them by the routers.
    ///
    /// Defaults to 0.
    pub fn set_traffic_class(&mut self, traffic_class: u8) -> &mut Self {
        self.traffic_class = traffic_class;
        self
    }

    /// Sets the minimum RNR NAK Timer Field Value for the new `QueuePair`.
    ///
    /// Defaults to 16 (2.56 ms delay).
    /// Valid only for RC QPs.
    ///
    /// When an incoming message to this QP should consume a Work Request from the Receive Queue,
    /// but no Work Request is outstanding on that Queue, the QP will send an RNR NAK packet to
    /// the initiator. It does not affect RNR NAKs sent for other reasons. The value must be one of
    /// the following values:
    ///
    ///  - 0 - 655.36 ms delay
    ///  - 1 - 0.01 ms delay
    ///  - 2 - 0.02 ms delay
    ///  - 3 - 0.03 ms delay
    ///  - 4 - 0.04 ms delay
    ///  - 5 - 0.06 ms delay
    ///  - 6 - 0.08 ms delay
    ///  - 7 - 0.12 ms delay
    ///  - 8 - 0.16 ms delay
    ///  - 9 - 0.24 ms delay
    ///  - 10 - 0.32 ms delay
    ///  - 11 - 0.48 ms delay
    ///  - 12 - 0.64 ms delay
    ///  - 13 - 0.96 ms delay
    ///  - 14 - 1.28 ms delay
    ///  - 15 - 1.92 ms delay
    ///  - 16 - 2.56 ms delay
    ///  - 17 - 3.84 ms delay
    ///  - 18 - 5.12 ms delay
    ///  - 19 - 7.68 ms delay
    ///  - 20 - 10.24 ms delay
    ///  - 21 - 15.36 ms delay
    ///  - 22 - 20.48 ms delay
    ///  - 23 - 30.72 ms delay
    ///  - 24 - 40.96 ms delay
    ///  - 25 - 61.44 ms delay
    ///  - 26 - 81.92 ms delay
    ///  - 27 - 122.88 ms delay
    ///  - 28 - 163.84 ms delay
    ///  - 29 - 245.76 ms delay
    ///  - 30 - 327.68 ms delay
    ///  - 31 - 491.52 ms delay
    pub fn set_min_rnr_timer(&mut self, timer: u8) -> &mut Self {
        if self.qp_type == ffi::ibv_qp_type::IBV_QPT_RC {
            self.min_rnr_timer = Some(timer);
        }
        self
    }

    /// Sets the minimum timeout that the new `QueuePair` waits for ACK/NACK from remote QP before
    /// retransmitting the packet.
    ///
    /// Defaults to 4 (65.536µs).
    /// Valid only for RC QPs.
    ///
    /// The value zero is special value that waits an infinite time for the ACK/NACK (useful
    /// for debugging). This means that if any packet in a message is being lost and no ACK or NACK
    /// is being sent, no retry will ever occur and the QP will just stop sending data.
    ///
    /// For any other value of timeout, the time calculation is `4.096*2^timeout`µs, giving:
    ///
    ///  - 0 - infinite
    ///  - 1 - 8.192 µs
    ///  - 2 - 16.384 µs
    ///  - 3 - 32.768 µs
    ///  - 4 - 65.536 µs
    ///  - 5 - 131.072 µs
    ///  - 6 - 262.144 µs
    ///  - 7 - 524.288 µs
    ///  - 8 - 1.048 ms
    ///  - 9 - 2.097 ms
    ///  - 10 - 4.194 ms
    ///  - 11 - 8.388 ms
    ///  - 12 - 16.777 ms
    ///  - 13 - 33.554 ms
    ///  - 14 - 67.108 ms
    ///  - 15 - 134.217 ms
    ///  - 16 - 268.435 ms
    ///  - 17 - 536.870 ms
    ///  - 18 - 1.07 s
    ///  - 19 - 2.14 s
    ///  - 20 - 4.29 s
    ///  - 21 - 8.58 s
    ///  - 22 - 17.1 s
    ///  - 23 - 34.3 s
    ///  - 24 - 68.7 s
    ///  - 25 - 137 s
    ///  - 26 - 275 s
    ///  - 27 - 550 s
    ///  - 28 - 1100 s
    ///  - 29 - 2200 s
    ///  - 30 - 4400 s
    ///  - 31 - 8800 s
    pub fn set_timeout(&mut self, timeout: u8) -> &mut Self {
        if self.qp_type == ffi::ibv_qp_type::IBV_QPT_RC {
            self.timeout = Some(timeout);
        }
        self
    }

    /// Sets the total number of times that the new `QueuePair` will try to resend the packets
    /// before reporting an error because the remote side doesn't answer in the primary path.
    ///
    /// This 3 bit value defaults to 6.
    /// Valid only for RC QPs.
    ///
    /// # Panics
    ///
    /// Panics if a count higher than 7 is given.
    pub fn set_retry_count(&mut self, count: u8) -> &mut Self {
        if self.qp_type == ffi::ibv_qp_type::IBV_QPT_RC {
            assert!(count <= 7);
            self.retry_count = Some(count);
        }
        self
    }

    /// Sets the total number of times that the new `QueuePair` will try to resend the packets when
    /// an RNR NACK was sent by the remote QP before reporting an error.
    ///
    /// This 3 bit value defaults to 6. The value 7 is special and specify to retry sending the
    /// message indefinitely when a RNR Nack is being sent by remote side.
    /// Valid only for RC QPs.
    ///
    /// # Panics
    ///
    /// Panics if a limit higher than 7 is given.
    pub fn set_rnr_retry(&mut self, n: u8) -> &mut Self {
        if self.qp_type == ffi::ibv_qp_type::IBV_QPT_RC {
            assert!(n <= 7);
            self.rnr_retry = Some(n);
        }
        self
    }

    /// Set the Shared Receive Queue (SRQ) associated with this QP.
    pub fn set_srq(&mut self, srq: &SharedReceiveQueue) -> &mut Self {
        self.srq = Some(srq.clone());
        self
    }

    /// Set the number of outstanding RDMA reads & atomic operations on the destination Queue Pair.
    ///
    /// This defaults to 1.
    /// Valid only for RC QPs.
    pub fn set_max_rd_atomic(&mut self, max_rd_atomic: u8) -> &mut Self {
        if self.qp_type == ffi::ibv_qp_type::IBV_QPT_RC {
            self.max_rd_atomic = Some(max_rd_atomic);
        }
        self
    }

    /// Set the number of responder resources for handling incoming RDMA reads & atomic operations.
    ///
    /// This defaults to 1.
    /// Valid only for RC QPs.
    pub fn set_max_dest_rd_atomic(&mut self, max_dest_rd_atomic: u8) -> &mut Self {
        if self.qp_type == ffi::ibv_qp_type::IBV_QPT_RC {
            self.max_dest_rd_atomic = Some(max_dest_rd_atomic);
        }
        self
    }

    /// Set the path MTU.
    ///
    /// Defaults to the port's active_mtu.
    /// Valid only for RC and UC QPs.
    /// The possible values are:
    ///  - 1: 256
    ///  - 2: 512
    ///  - 3: 1024
    ///  - 4: 2048
    ///  - 5: 4096
    pub fn set_path_mtu(&mut self, path_mtu: ibv_mtu) -> &mut Self {
        if self.qp_type == ffi::ibv_qp_type::IBV_QPT_RC
            || self.qp_type == ffi::ibv_qp_type::IBV_QPT_UC
        {
            self.path_mtu = Some(path_mtu);
        }
        self
    }

    /// Set the PSN for the receive queue.
    ///
    /// Defaults to 0.
    /// Valid only for RC and UC QPs.
    pub fn set_rq_psn(&mut self, rq_psn: u32) -> &mut Self {
        if self.qp_type == ffi::ibv_qp_type::IBV_QPT_RC
            || self.qp_type == ffi::ibv_qp_type::IBV_QPT_UC
        {
            self.rq_psn = Some(rq_psn);
        }
        self
    }

    /// Set the opaque context value for the new `QueuePair`.
    ///
    /// Defaults to 0.
    pub fn set_context(&mut self, ctx: isize) -> &mut Self {
        self.ctx = ctx;
        self
    }

    /// Set the maximum number of send requests in the work queue
    ///
    /// Defaults to 1.
    pub fn set_max_send_wr(&mut self, max_send_wr: u32) -> &mut Self {
        self.max_send_wr = max_send_wr;
        self
    }

    /// The maximum number of scatter/gather elements in any Work Request
    /// that can be posted to the Send Queue in that Queue Pair.
    ///
    /// Value can be [0..dev_cap.max_sge]. There may be RDMA devices that
    /// for specific transport types may support less scatter/gather elements
    /// than the maximum reported value.
    ///
    /// Defaults to 1.
    pub fn set_max_send_sge(&mut self, max_send_sge: u32) -> &mut Self {
        self.max_send_sge = max_send_sge;
        self
    }

    /// Set the maximum number of receive requests in the work queue
    ///
    /// Defaults to 1.
    pub fn set_max_recv_wr(&mut self, max_recv_wr: u32) -> &mut Self {
        self.max_recv_wr = max_recv_wr;
        self
    }

    /// The maximum number of scatter/gather elements in any Work Request
    /// that can be posted to the Receive Queue in that Queue Pair.
    ///
    /// Value can be [0..dev_cap.max_sge]. There may be RDMA devices that
    /// for specific transport types may support less scatter/gather elements
    /// than the maximum reported value. This value is ignored if the
    /// Queue Pair is associated with an SRQ.
    ///
    /// Defaults to 1.
    pub fn set_max_recv_sge(&mut self, max_recv_sge: u32) -> &mut Self {
        self.max_recv_sge = max_recv_sge;
        self
    }

    /// Set the maximum size, in bytes, of inline data that may be posted on the send queue.
    ///
    /// Inline sends (see [`PostOp::send_inline`]) copy their payload directly into the work request
    /// rather than referencing a registered memory region, which lowers latency for small messages.
    /// A send queue must reserve this capacity up front; the actual value granted by the device can
    /// be larger than requested and is reported in the `max_inline_data` field returned by
    /// `ibv_query_qp`. Posting more inline bytes than the queue pair supports fails at submit time.
    ///
    /// Defaults to 0 (inline sends disabled).
    pub fn set_max_inline_data(&mut self, max_inline_data: u32) -> &mut Self {
        self.max_inline_data = max_inline_data;
        self
    }

    /// Create a new `QueuePair` from this builder template.
    ///
    /// The returned `QueuePair` is associated with the builder's `ProtectionDomain`.
    ///
    /// This method will fail if asked to create QP of a type other than `IBV_QPT_RC` or
    /// `IBV_QPT_UD` associated with an SRQ.
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: Invalid `ProtectionDomain` or `CompletionQueue`, or invalid value provided in
    ///    `max_send_wr`, `max_recv_wr`, or in `max_inline_data`.
    ///  - `ENOMEM`: Not enough resources to complete this operation.
    ///  - `ENOSYS`: QP with this Transport Service Type isn't supported by this RDMA device.
    ///  - `EPERM`: Not enough permissions to create a QP with this Transport Service Type.
    pub fn build(&self) -> Result<PreparedQueuePair> {
        use ffi::ibv_qp_create_send_ops_flags as SendOps;
        use ffi::ibv_qp_type::{IBV_QPT_RC, IBV_QPT_UC};

        // Enable the extended send operations we drive through the doorbell post API. SEND is
        // available on every transport; one-sided RDMA needs a connected QP (RC/UC), and RDMA read
        // plus atomics are RC-only.
        let mut send_ops_flags =
            SendOps::IBV_QP_EX_WITH_SEND.0 | SendOps::IBV_QP_EX_WITH_SEND_WITH_IMM.0;
        if matches!(self.qp_type, IBV_QPT_RC | IBV_QPT_UC) {
            send_ops_flags |= SendOps::IBV_QP_EX_WITH_RDMA_WRITE.0
                | SendOps::IBV_QP_EX_WITH_RDMA_WRITE_WITH_IMM.0;
        }
        if self.qp_type == IBV_QPT_RC {
            send_ops_flags |= SendOps::IBV_QP_EX_WITH_RDMA_READ.0
                | SendOps::IBV_QP_EX_WITH_ATOMIC_CMP_AND_SWP.0
                | SendOps::IBV_QP_EX_WITH_ATOMIC_FETCH_AND_ADD.0;
        }

        // `ibv_qp_init_attr_ex` has a `qp_type` field with no zero variant plus fields we never use
        // (XRC, TSO, RX hashing). Zero the storage, write only the fields the driver reads, and hand
        // the pointer to C without `assume_init`, so the untouched enum fields never become a Rust
        // value.
        let mut attr = std::mem::MaybeUninit::<ffi::ibv_qp_init_attr_ex>::zeroed();
        let p = attr.as_mut_ptr();
        unsafe {
            (*p).qp_context = ptr::null::<c_void>().offset(self.ctx) as *mut _;
            (*p).send_cq = self.send.cq();
            (*p).recv_cq = self.recv.cq();
            (*p).srq = self
                .srq
                .as_ref()
                .map(|s| s.inner.srq)
                .unwrap_or(ptr::null_mut());
            (*p).cap = ffi::ibv_qp_cap {
                max_send_wr: self.max_send_wr,
                max_recv_wr: self.max_recv_wr,
                max_send_sge: self.max_send_sge,
                max_recv_sge: self.max_recv_sge,
                max_inline_data: self.max_inline_data,
            };
            (*p).qp_type = self.qp_type;
            (*p).comp_mask = ffi::ibv_qp_init_attr_mask::IBV_QP_INIT_ATTR_PD.0
                | ffi::ibv_qp_init_attr_mask::IBV_QP_INIT_ATTR_SEND_OPS_FLAGS.0;
            (*p).pd = self.pd.pd;
            (*p).send_ops_flags = send_ops_flags as u64;
        }

        let qp = unsafe { ffi::ibv_create_qp_ex(self.pd.ctx.ctx, attr.as_mut_ptr()) };
        if qp.is_null() {
            Err(Error::CreateQueuePair(io::Error::last_os_error()))
        } else {
            let qp_ex = unsafe { ffi::ibv_qp_to_qp_ex(qp) };
            Ok(PreparedQueuePair {
                lid: self.port_attr.lid,
                port_num: self.port_num,
                qp: QueuePair {
                    pd: self.pd.clone(),
                    _srq: self.srq.clone(),
                    _send_cq: self.send.clone(),
                    _recv_cq: self.recv.clone(),
                    qp,
                    qp_ex,
                },
                gid_index: self.gid_index,
                traffic_class: self.traffic_class,
                access: self.access,
                timeout: self.timeout,
                retry_count: self.retry_count,
                rnr_retry: self.rnr_retry,
                min_rnr_timer: self.min_rnr_timer,
                max_rd_atomic: self.max_rd_atomic,
                max_dest_rd_atomic: self.max_dest_rd_atomic,
                path_mtu: self.path_mtu,
                rq_psn: self.rq_psn,
                service_level: self.service_level,
            })
        }
    }
}

/// An allocated but uninitialized `QueuePair`.
///
/// Specifically, this `QueuePair` has been allocated with `ibv_create_qp`, but has not yet been
/// initialized with calls to `ibv_modify_qp`.
///
/// To complete the construction of the `QueuePair`, you will need to obtain the
/// [`QueuePairEndpoint`] of the remote end (by using [`endpoint`](Self::endpoint)), and then call
/// [`handshake`](Self::handshake) on both sides with the other side's endpoint:
///
/// ```text
/// // on host 1
/// let pqp: PreparedQueuePair = ...;
/// let host1end = pqp.endpoint()?;
/// host2.send(host1end);
/// let host2end = host2.recv();
/// let qp = pqp.handshake(host2end)?;
///
/// // on host 2
/// let pqp: PreparedQueuePair = ...;
/// let host2end = pqp.endpoint()?;
/// host1.send(host2end);
/// let host1end = host1.recv();
/// let qp = pqp.handshake(host1end)?;
/// ```
///
/// For a runnable version of this exchange (self-connected, so it fits one process), see
/// `examples/loopback.rs`; `examples/rdmacm_connect.rs` shows the same bring-up driven by the
/// connection manager instead.
pub struct PreparedQueuePair {
    pub(crate) qp: QueuePair,
    /// port local identifier
    pub(crate) lid: u16,
    /// the device port this queue pair is associated with (numbered from 1)
    pub(crate) port_num: u8,
    // carried from builder
    pub(crate) gid_index: Option<u32>,
    /// traffic class set in Global Routing Headers, only used if `gid_index` is set.
    pub(crate) traffic_class: u8,
    /// only valid for RC and UC
    pub(crate) access: Option<ffi::ibv_access_flags>,
    /// only valid for RC
    pub(crate) min_rnr_timer: Option<u8>,
    /// only valid for RC
    pub(crate) timeout: Option<u8>,
    /// only valid for RC
    pub(crate) retry_count: Option<u8>,
    /// only valid for RC
    pub(crate) rnr_retry: Option<u8>,
    /// only valid for RC
    pub(crate) max_rd_atomic: Option<u8>,
    /// only valid for RC
    pub(crate) max_dest_rd_atomic: Option<u8>,
    /// only valid for RC and UC
    pub(crate) path_mtu: Option<ibv_mtu>,
    /// only valid for RC and UC
    pub(crate) rq_psn: Option<u32>,
    /// service level (0-15). Higher value means higher priority.
    pub(crate) service_level: u8,
}

/// An identifier for the network endpoint of a `QueuePair`.
///
/// Internally, this contains the `QueuePair`'s `qp_num`, as well as the context's `lid` and `gid`.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct QueuePairEndpoint {
    /// the `QueuePair`'s `qp_num`
    pub num: u32,
    /// the context's `lid`
    pub lid: u16,
    /// the context's `gid`, used for global routing
    pub gid: Option<Gid>,
}

impl PreparedQueuePair {
    /// Extracts the still-uninitialized (`RESET`) queue pair without transitioning it, so you can
    /// drive the state machine yourself with [`QueuePair::modify`] instead of using
    /// [`handshake`](Self::handshake) / [`activate_ud`](Self::activate_ud).
    ///
    /// This is an escape hatch for fully manual bring-up (custom partition keys, packet sequence
    /// numbers, alternate paths, and so on). The returned queue pair is in `RESET` and cannot send
    /// or receive until you transition it through `INIT`, `RTR`, and `RTS`; most users should prefer
    /// `handshake`/`activate_ud`. The RDMA connection manager (the `rdmacm` module, behind the
    /// feature of the same name) uses this to drive the transitions itself.
    pub fn into_queue_pair(self) -> QueuePair {
        self.qp
    }

    /// Get the network endpoint for this `QueuePair`.
    ///
    /// This endpoint will need to be communicated to the `QueuePair` on the remote end.
    pub fn endpoint(&self) -> Result<QueuePairEndpoint> {
        let num = unsafe { &*self.qp.qp }.qp_num;
        let gid = if let Some(gid_index) = self.gid_index {
            let mut gid = ffi::ibv_gid::default();
            let rc = unsafe {
                ffi::ibv_query_gid(
                    self.qp.pd.ctx.ctx,
                    self.port_num,
                    gid_index as i32,
                    &mut gid,
                )
            };
            if rc < 0 {
                return Err(Error::os(io::Error::last_os_error(), |e| Error::QueryGid {
                    port_num: self.port_num,
                    gid_index,
                    source: e,
                }));
            }
            Some(Gid::from(gid))
        } else {
            None
        };
        Ok(QueuePairEndpoint {
            num,
            lid: self.lid,
            gid,
        })
    }

    /// Set up the `QueuePair` such that it is ready to exchange packets with a remote `QueuePair`.
    ///
    /// Internally, this uses `ibv_modify_qp` to mark the `QueuePair` as initialized
    /// (`IBV_QPS_INIT`), ready to receive (`IBV_QPS_RTR`), and ready to send (`IBV_QPS_RTS`),
    /// applying the attributes configured on the builder (access flags, timeouts, path MTU,
    /// service level, and so on) at the appropriate steps. Further discussion of the protocol can
    /// be found on [RDMAmojo]. This bring-up is for connected queue pairs (RC and UC); use
    /// [`activate_ud`](Self::activate_ud) for UD queue pairs, or
    /// [`into_queue_pair`](Self::into_queue_pair) to drive the state machine yourself.
    ///
    /// If the endpoint contains a Gid, the routing will be global. This means:
    /// ```text
    /// ah_attr.is_global = 1;
    /// ah_attr.grh.hop_limit = 0xff;
    /// ```
    ///
    /// The queue pair is associated with the port chosen when it was created (see
    /// [`ProtectionDomain::create_qp_on_port`]). The handshake also sets the following parameters,
    /// which are currently not configurable:
    ///
    /// ```text
    /// pkey_index = 0;
    /// sq_psn = 0;
    /// ah_attr.src_path_bits = 0;
    /// ```
    ///
    /// # Errors
    ///
    ///  - [`GidMismatch`](Error::GidMismatch): the remote endpoint carries a GID, but no
    ///    `gid_index` was set on the builder to route from.
    ///  - [`ModifyQueuePair`](Error::ModifyQueuePair): a state transition failed
    ///    (`ibv_modify_qp`), for example because an attribute is invalid for this queue pair's
    ///    type or the remote endpoint is unreachable.
    ///
    /// [RDMAmojo]: http://www.rdmamojo.com/2014/01/18/connecting-queue-pairs/
    pub fn handshake(self, remote: QueuePairEndpoint) -> Result<QueuePair> {
        // init and associate with port
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_INIT,
            pkey_index: 0,
            port_num: self.port_num,
            ..Default::default()
        };
        let mut mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE
            | ffi::ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
            | ffi::ibv_qp_attr_mask::IBV_QP_PORT;
        if let Some(access) = self.access {
            attr.qp_access_flags = access.0;
            mask |= ffi::ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
        }
        let errno = unsafe { ffi::ibv_modify_qp(self.qp.qp, &mut attr as *mut _, mask.0 as i32) };
        if errno != 0 {
            return Err(Error::errno(errno, Error::ModifyQueuePair));
        }

        // set ready to receive
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_RTR,
            // TODO: this is only valid for RC and UC
            dest_qp_num: remote.num,
            // TODO: this is only valid for RC and UC
            ah_attr: ffi::ibv_ah_attr {
                dlid: remote.lid,
                sl: self.service_level,
                src_path_bits: 0,
                port_num: self.port_num,
                grh: Default::default(),
                ..Default::default()
            },
            ..Default::default()
        };
        if let Some(gid) = remote.gid {
            attr.ah_attr.is_global = 1;
            attr.ah_attr.grh.dgid = gid.into();
            attr.ah_attr.grh.hop_limit = 0xff;
            attr.ah_attr.grh.sgid_index = self.gid_index.ok_or(Error::GidMismatch)? as u8;
            attr.ah_attr.grh.traffic_class = self.traffic_class;
        }
        let mut mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE
            | ffi::ibv_qp_attr_mask::IBV_QP_AV
            | ffi::ibv_qp_attr_mask::IBV_QP_DEST_QPN;
        if let Some(max_dest_rd_atomic) = self.max_dest_rd_atomic {
            attr.max_dest_rd_atomic = max_dest_rd_atomic;
            mask |= ffi::ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC;
        }
        if let Some(min_rnr_timer) = self.min_rnr_timer {
            attr.min_rnr_timer = min_rnr_timer;
            mask |= ffi::ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER;
        }
        if let Some(path_mtu) = self.path_mtu {
            attr.path_mtu = path_mtu;
            mask |= ffi::ibv_qp_attr_mask::IBV_QP_PATH_MTU;
        }
        if let Some(rq_psn) = self.rq_psn {
            attr.rq_psn = rq_psn;
            mask |= ffi::ibv_qp_attr_mask::IBV_QP_RQ_PSN;
        }
        let errno = unsafe { ffi::ibv_modify_qp(self.qp.qp, &mut attr as *mut _, mask.0 as i32) };
        if errno != 0 {
            // On RoCE, the provider resolves the route to the remote GID during this transition,
            // and reports a GID that does not answer as a timeout or unreachable network. Spell
            // that out: it is the most common RoCE bring-up failure, and "connection timed out"
            // alone sends people looking at the wrong layer.
            if remote.gid.is_some()
                && (errno == nix::libc::ETIMEDOUT || errno == nix::libc::ENETUNREACH)
            {
                let source = io::Error::from_raw_os_error(errno);
                return Err(Error::ModifyQueuePair(io::Error::new(
                    source.kind(),
                    format!(
                        "resolving the route to the remote GID failed ({source}); on RoCE this \
                         usually means the remote GID does not answer on the network of the \
                         local GID at index {}, or a firewall drops RoCE (UDP 4791) traffic",
                        attr.ah_attr.grh.sgid_index,
                    ),
                )));
            }
            return Err(Error::errno(errno, Error::ModifyQueuePair));
        }

        // set ready to send
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_RTS,
            sq_psn: 0,
            ..Default::default()
        };
        let mut mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE | ffi::ibv_qp_attr_mask::IBV_QP_SQ_PSN;
        if let Some(timeout) = self.timeout {
            attr.timeout = timeout;
            mask |= ffi::ibv_qp_attr_mask::IBV_QP_TIMEOUT;
        }
        if let Some(retry_count) = self.retry_count {
            attr.retry_cnt = retry_count;
            mask |= ffi::ibv_qp_attr_mask::IBV_QP_RETRY_CNT;
        }
        if let Some(rnr_retry) = self.rnr_retry {
            attr.rnr_retry = rnr_retry;
            mask |= ffi::ibv_qp_attr_mask::IBV_QP_RNR_RETRY;
        }
        if let Some(max_rd_atomic) = self.max_rd_atomic {
            attr.max_rd_atomic = max_rd_atomic;
            mask |= ffi::ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC;
        }
        let errno = unsafe { ffi::ibv_modify_qp(self.qp.qp, &mut attr as *mut _, mask.0 as i32) };
        if errno != 0 {
            return Err(Error::errno(errno, Error::ModifyQueuePair));
        }

        Ok(self.qp)
    }

    /// Activate this queue pair as an unreliable datagram (UD) queue pair.
    ///
    /// Unlike [`handshake`](Self::handshake), UD is connectionless: there is no remote endpoint to
    /// exchange, so the queue pair is transitioned `INIT -> RTR -> RTS` with the given `qkey`.
    /// Incoming datagrams whose Q_Key does not match `qkey` are discarded (unless the sender's Q_Key
    /// has its most significant bit set, meaning "use the QP's Q_Key").
    ///
    /// Each datagram is addressed individually at send time with an [`AddressHandle`]; see
    /// [`QueuePair::post_send_ud`].
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: Invalid value provided in `attr` or `attr_mask`.
    ///  - `ENOMEM`: Not enough resources to complete this operation.
    pub fn activate_ud(self, qkey: u32) -> Result<QueuePair> {
        // INIT: associate with the port and set the Q_Key. UD has no access flags.
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_INIT,
            pkey_index: 0,
            port_num: self.port_num,
            qkey,
            ..Default::default()
        };
        let mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE
            | ffi::ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
            | ffi::ibv_qp_attr_mask::IBV_QP_PORT
            | ffi::ibv_qp_attr_mask::IBV_QP_QKEY;
        let errno = unsafe { ffi::ibv_modify_qp(self.qp.qp, &mut attr as *mut _, mask.0 as i32) };
        if errno != 0 {
            return Err(Error::errno(errno, Error::ModifyQueuePair));
        }

        // RTR: a UD queue pair needs no path or destination information.
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_RTR,
            ..Default::default()
        };
        let mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE;
        let errno = unsafe { ffi::ibv_modify_qp(self.qp.qp, &mut attr as *mut _, mask.0 as i32) };
        if errno != 0 {
            return Err(Error::errno(errno, Error::ModifyQueuePair));
        }

        // RTS.
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_RTS,
            sq_psn: 0,
            ..Default::default()
        };
        let mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE | ffi::ibv_qp_attr_mask::IBV_QP_SQ_PSN;
        let errno = unsafe { ffi::ibv_modify_qp(self.qp.qp, &mut attr as *mut _, mask.0 as i32) };
        if errno != 0 {
            return Err(Error::errno(errno, Error::ModifyQueuePair));
        }

        Ok(self.qp)
    }
}

/// One operation recorded in a [`PostBatch`] until the batch is flushed to the doorbell.
/// A receive work request, binding the lifetime of its scatter/gather buffers.
///
/// Build one with [`RecvRequest::new`] and post a batch of them with [`QueuePair::post_recv`]. Unlike
/// the send doorbell, receives are posted from a caller-owned slice, so batching allocates nothing.
#[repr(transparent)]
pub struct RecvRequest<'a> {
    wr: ffi::ibv_recv_wr,
    _local: std::marker::PhantomData<&'a [LocalMemorySlice]>,
}

impl<'a> RecvRequest<'a> {
    /// A receive that scatters an incoming message into `local`, tagged with `wr_id`.
    #[inline]
    pub fn new(wr_id: u64, local: &'a [LocalMemorySlice]) -> Self {
        RecvRequest {
            wr: ffi::ibv_recv_wr {
                wr_id,
                next: ptr::null_mut(),
                sg_list: local.as_ptr() as *mut ffi::ibv_sge,
                num_sge: local.len() as i32,
            },
            _local: std::marker::PhantomData,
        }
    }
}

/// A batch of send work requests being built on a [`QueuePair`]'s send queue.
///
/// Created by [`QueuePair::start_send`]. Each builder method posts one work request to the open block
/// immediately through the extended ("doorbell") interface; [`submit`](Self::submit) then rings the
/// doorbell once for the whole batch. Dropping the batch without submitting aborts it.
///
/// Configure a request *before* its opcode method: `batch.signaled().to(&ah, qpn, qkey).send(id,
/// sges)`. (The provider reads the work-request flags inside the opcode builder, so signaling and
/// addressing must be set first.)
///
/// # Borrow-checked guarantees
///
/// The batch borrows the queue pair for as long as it is open, so the type system enforces the
/// doorbell interface's rule that a queue pair has at most one block being built at a time. A
/// second [`start_send`](QueuePair::start_send) (or any other use of the queue pair) while a batch
/// is open is rejected at compile time:
///
/// ```compile_fail
/// # fn two_batches(qp: &mut ibverbs::QueuePair) {
/// let first = qp.start_send();
/// let second = qp.start_send(); // error: `*qp` is already mutably borrowed by `first`
/// let _ = (first, second);
/// # }
/// ```
///
/// Likewise, only one request at a time is configured on a batch: each builder method borrows the
/// batch until that work request is posted, so two half-built requests cannot overlap:
///
/// ```compile_fail
/// # fn two_requests(qp: &mut ibverbs::QueuePair) {
/// let mut batch = qp.start_send();
/// let first = batch.signaled();
/// let second = batch.signaled(); // error: `batch` is already mutably borrowed by `first`
/// let _ = (first, second);
/// # }
/// ```
#[must_use = "a started batch must be `.submit()`ed (otherwise it is aborted on drop)"]
pub struct PostBatch<'qp> {
    qpx: *mut ffi::ibv_qp_ex,
    _qp: std::marker::PhantomData<&'qp mut QueuePair>,
}

impl<'qp> PostBatch<'qp> {
    /// Mark the next work request as signaled (`IBV_SEND_SIGNALED`), so it generates a completion.
    #[inline]
    pub fn signaled(&mut self) -> PostOp<'_, 'qp> {
        PostOp {
            batch: self,
            flags: ffi::ibv_send_flags::IBV_SEND_SIGNALED.0,
            dest: None,
        }
    }

    /// Mark the next work request as fenced (`IBV_SEND_FENCE`): the device blocks it until prior
    /// RDMA reads and atomic operations on this queue pair have completed. Used to order a send or
    /// write after a read whose data it depends on.
    #[inline]
    pub fn fenced(&mut self) -> PostOp<'_, 'qp> {
        PostOp {
            batch: self,
            flags: ffi::ibv_send_flags::IBV_SEND_FENCE.0,
            dest: None,
        }
    }

    /// Mark the next work request as solicited (`IBV_SEND_SOLICITED`): a SEND or SEND-with-immediate
    /// raises a solicited event on the remote side, waking a peer blocked on its completion channel
    /// after arming with [`CompletionQueue::req_notify`] for solicited events only.
    #[inline]
    pub fn solicited(&mut self) -> PostOp<'_, 'qp> {
        PostOp {
            batch: self,
            flags: ffi::ibv_send_flags::IBV_SEND_SOLICITED.0,
            dest: None,
        }
    }

    /// Address the next work request to `ah` / `remote_qpn` / `remote_qkey` (required for sends on
    /// UD and SRD queue pairs).
    #[inline]
    pub fn to(&mut self, ah: &AddressHandle, remote_qpn: u32, remote_qkey: u32) -> PostOp<'_, 'qp> {
        PostOp {
            batch: self,
            flags: 0,
            dest: Some((ah.as_ptr(), remote_qpn, remote_qkey)),
        }
    }

    #[inline]
    fn op(&mut self) -> PostOp<'_, 'qp> {
        PostOp {
            batch: self,
            flags: 0,
            dest: None,
        }
    }

    /// Post a SEND.
    #[inline]
    pub fn send(&mut self, wr_id: u64, local: &[LocalMemorySlice]) {
        self.op().send(wr_id, local)
    }

    /// Post a SEND carrying a 32-bit immediate (passed in host byte order).
    #[inline]
    pub fn send_imm(&mut self, wr_id: u64, local: &[LocalMemorySlice], imm: u32) {
        self.op().send_imm(wr_id, local, imm)
    }

    /// Post an RDMA WRITE into the remote region `remote`.
    #[inline]
    pub fn write(&mut self, wr_id: u64, local: &[LocalMemorySlice], remote: RemoteMemorySlice) {
        self.op().write(wr_id, local, remote)
    }

    /// Post an RDMA WRITE carrying a 32-bit immediate (host byte order).
    #[inline]
    pub fn write_imm(
        &mut self,
        wr_id: u64,
        local: &[LocalMemorySlice],
        remote: RemoteMemorySlice,
        imm: u32,
    ) {
        self.op().write_imm(wr_id, local, remote, imm)
    }

    /// Post a SEND whose payload is carried inline. See [`PostOp::send_inline`].
    #[inline]
    pub fn send_inline(&mut self, wr_id: u64, data: &[u8]) {
        self.op().send_inline(wr_id, data)
    }

    /// Post a SEND with an inline payload and a 32-bit immediate. See [`PostOp::send_imm_inline`].
    #[inline]
    pub fn send_imm_inline(&mut self, wr_id: u64, data: &[u8], imm: u32) {
        self.op().send_imm_inline(wr_id, data, imm)
    }

    /// Post an RDMA WRITE with an inline payload. See [`PostOp::write_inline`].
    #[inline]
    pub fn write_inline(&mut self, wr_id: u64, data: &[u8], remote: RemoteMemorySlice) {
        self.op().write_inline(wr_id, data, remote)
    }

    /// Post an RDMA WRITE with an inline payload and a 32-bit immediate. See
    /// [`PostOp::write_imm_inline`].
    #[inline]
    pub fn write_imm_inline(
        &mut self,
        wr_id: u64,
        data: &[u8],
        remote: RemoteMemorySlice,
        imm: u32,
    ) {
        self.op().write_imm_inline(wr_id, data, remote, imm)
    }

    /// Post an RDMA READ from `remote` into `local`.
    #[inline]
    pub fn read(&mut self, wr_id: u64, local: &[LocalMemorySlice], remote: RemoteMemorySlice) {
        self.op().read(wr_id, local, remote)
    }

    /// Post an atomic compare-and-swap on the 8-byte value at `remote`.
    #[inline]
    pub fn atomic_cmp_swap(
        &mut self,
        wr_id: u64,
        local: &[LocalMemorySlice],
        remote: RemoteMemorySlice,
        compare: u64,
        swap: u64,
    ) {
        self.op()
            .atomic_cmp_swap(wr_id, local, remote, compare, swap)
    }

    /// Post an atomic fetch-and-add on the 8-byte value at `remote`.
    #[inline]
    pub fn atomic_fetch_add(
        &mut self,
        wr_id: u64,
        local: &[LocalMemorySlice],
        remote: RemoteMemorySlice,
        add: u64,
    ) {
        self.op().atomic_fetch_add(wr_id, local, remote, add)
    }

    /// Post the whole batch to the device, ringing the doorbell once.
    ///
    /// # Safety
    ///
    /// Every memory region referenced by the batch must stay valid until a work completion has been
    /// polled for the corresponding `wr_id`.
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: invalid value in one of the work requests.
    ///  - `ENOMEM`: the send queue is full or out of resources.
    pub unsafe fn submit(self) -> Result<()> {
        let qpx = self.qpx;
        // Disarm the abort-on-drop before completing: `Drop` would otherwise `wr_abort` the block we
        // are about to `wr_complete`.
        std::mem::forget(self);
        let ret = unsafe { (*qpx).wr_complete.unwrap()(qpx) };
        if ret != 0 {
            Err(Error::errno(ret, Error::PostSend))
        } else {
            Ok(())
        }
    }
}

impl Drop for PostBatch<'_> {
    fn drop(&mut self) {
        // Reached only when the batch was started (`wr_start`) but not submitted; `submit` forgets
        // the batch to skip this. Abort the open work-request block.
        unsafe { (*self.qpx).wr_abort.unwrap()(self.qpx) };
    }
}

/// Per-request configuration (signaled / datagram address) for the next work request on a
/// [`PostBatch`].
///
/// Returned by [`PostBatch::signaled`] / [`PostBatch::to`]; the chosen opcode method (`send`,
/// `write`, ...) posts the request immediately.
pub struct PostOp<'b, 'qp> {
    batch: &'b mut PostBatch<'qp>,
    flags: u32,
    dest: Option<(*mut ffi::ibv_ah, u32, u32)>,
}

impl PostOp<'_, '_> {
    /// Also mark this work request signaled (`IBV_SEND_SIGNALED`).
    #[inline]
    pub fn signaled(mut self) -> Self {
        self.flags |= ffi::ibv_send_flags::IBV_SEND_SIGNALED.0;
        self
    }

    /// Also mark this work request fenced (`IBV_SEND_FENCE`): the device blocks it until prior RDMA
    /// reads and atomic operations on this queue pair have completed.
    #[inline]
    pub fn fenced(mut self) -> Self {
        self.flags |= ffi::ibv_send_flags::IBV_SEND_FENCE.0;
        self
    }

    /// Also mark this work request solicited (`IBV_SEND_SOLICITED`): a SEND or SEND-with-immediate
    /// raises a solicited event on the remote side.
    #[inline]
    pub fn solicited(mut self) -> Self {
        self.flags |= ffi::ibv_send_flags::IBV_SEND_SOLICITED.0;
        self
    }

    /// Also address this datagram send to `ah` / `remote_qpn` / `remote_qkey`.
    #[inline]
    pub fn to(mut self, ah: &AddressHandle, remote_qpn: u32, remote_qkey: u32) -> Self {
        self.dest = Some((ah.as_ptr(), remote_qpn, remote_qkey));
        self
    }

    /// Post one work request: set id and flags, run the opcode builder, then the optional datagram
    /// address, then the scatter list.
    #[inline]
    fn build(self, wr_id: u64, local: &[LocalMemorySlice], op: impl FnOnce(*mut ffi::ibv_qp_ex)) {
        let qpx = self.batch.qpx;
        unsafe {
            (*qpx).wr_id = wr_id;
            (*qpx).wr_flags = self.flags;
            op(qpx);
            if let Some((ah, qpn, qkey)) = self.dest {
                (*qpx).wr_set_ud_addr.unwrap()(qpx, ah, qpn, qkey);
            }
            (*qpx).wr_set_sge_list.unwrap()(
                qpx,
                local.len(),
                local.as_ptr() as *const ffi::ibv_sge,
            );
        }
    }

    /// Like [`build`](Self::build), but copies `data` inline into the work request instead of
    /// referencing a registered memory region. The bytes are copied during this call, so `data` need
    /// not outlive the work completion.
    #[inline]
    fn build_inline(self, wr_id: u64, data: &[u8], op: impl FnOnce(*mut ffi::ibv_qp_ex)) {
        let qpx = self.batch.qpx;
        unsafe {
            (*qpx).wr_id = wr_id;
            // `IBV_SEND_INLINE` is the work-request flag that marks the payload as inline; it is what
            // the legacy `ibv_post_send` ABI carries, and rdma-core's generic doorbell emulation
            // (used by providers such as Soft-RoCE) sets it when translating `wr_set_inline_data`.
            // Native doorbell providers key off the `wr_set_inline_data` call itself, so the flag is
            // redundant but harmless there.
            (*qpx).wr_flags = self.flags | ffi::ibv_send_flags::IBV_SEND_INLINE.0;
            op(qpx);
            if let Some((ah, qpn, qkey)) = self.dest {
                (*qpx).wr_set_ud_addr.unwrap()(qpx, ah, qpn, qkey);
            }
            (*qpx).wr_set_inline_data.unwrap()(qpx, data.as_ptr() as *mut c_void, data.len());
        }
    }

    /// Like [`build_inline`](Self::build_inline), but gathers several buffers into the inline payload
    /// of a single work request (`wr_set_inline_data_list`). The bytes are copied during this call,
    /// so none of the `bufs` need outlive the work completion.
    #[inline]
    fn build_inline_list(
        self,
        wr_id: u64,
        bufs: &[io::IoSlice<'_>],
        op: impl FnOnce(*mut ffi::ibv_qp_ex),
    ) {
        let qpx = self.batch.qpx;
        unsafe {
            (*qpx).wr_id = wr_id;
            (*qpx).wr_flags = self.flags | ffi::ibv_send_flags::IBV_SEND_INLINE.0;
            op(qpx);
            if let Some((ah, qpn, qkey)) = self.dest {
                (*qpx).wr_set_ud_addr.unwrap()(qpx, ah, qpn, qkey);
            }
            // `std::io::IoSlice` is guaranteed ABI-compatible with `struct iovec` on Unix (the only
            // platform rdma-core targets), and `ibv_data_buf` has the same layout as `iovec` (an
            // address and a length), so the buffer list passes straight through without copying it
            // into a temporary array.
            (*qpx).wr_set_inline_data_list.unwrap()(
                qpx,
                bufs.len(),
                bufs.as_ptr() as *const ffi::ibv_data_buf,
            );
        }
    }

    /// Post a SEND.
    #[inline]
    pub fn send(self, wr_id: u64, local: &[LocalMemorySlice]) {
        self.build(wr_id, local, |q| unsafe { (*q).wr_send.unwrap()(q) })
    }

    /// Post a SEND whose payload is carried inline in the work request.
    ///
    /// Inline data is copied into the work request rather than referenced through a memory region,
    /// which lowers latency for small messages and lets `data` be reused or dropped right away. The
    /// queue pair must have been built with enough inline capacity (see
    /// [`QueuePairBuilder::set_max_inline_data`]) or the [`submit`](PostBatch::submit) fails with
    /// `EINVAL`.
    #[inline]
    pub fn send_inline(self, wr_id: u64, data: &[u8]) {
        self.build_inline(wr_id, data, |q| unsafe { (*q).wr_send.unwrap()(q) })
    }

    /// Post a SEND carrying an inline payload and a 32-bit immediate (host byte order).
    ///
    /// See [`send_inline`](Self::send_inline) for the inline-data requirements.
    #[inline]
    pub fn send_imm_inline(self, wr_id: u64, data: &[u8], imm: u32) {
        self.build_inline(wr_id, data, move |q| unsafe {
            (*q).wr_send_imm.unwrap()(q, imm.to_be())
        })
    }

    /// Post an RDMA WRITE into `remote` whose payload is carried inline in the work request.
    ///
    /// See [`send_inline`](Self::send_inline) for the inline-data requirements.
    #[inline]
    pub fn write_inline(self, wr_id: u64, data: &[u8], remote: RemoteMemorySlice) {
        self.build_inline(wr_id, data, move |q| unsafe {
            (*q).wr_rdma_write.unwrap()(q, remote.rkey, remote.addr)
        })
    }

    /// Post an RDMA WRITE into `remote` carrying an inline payload and a 32-bit immediate (host byte
    /// order).
    ///
    /// See [`send_inline`](Self::send_inline) for the inline-data requirements.
    #[inline]
    pub fn write_imm_inline(self, wr_id: u64, data: &[u8], remote: RemoteMemorySlice, imm: u32) {
        self.build_inline(wr_id, data, move |q| unsafe {
            (*q).wr_rdma_write_imm.unwrap()(q, remote.rkey, remote.addr, imm.to_be())
        })
    }

    /// Post a SEND whose inline payload is gathered from several buffers.
    ///
    /// Like [`send_inline`](Self::send_inline), but concatenates `bufs` into one inline payload
    /// (saving a copy into a contiguous buffer first), so the queue pair must have enough inline
    /// capacity for their combined length. See [`send_inline`](Self::send_inline) for the
    /// inline-data requirements.
    #[inline]
    pub fn send_inline_list(self, wr_id: u64, bufs: &[io::IoSlice<'_>]) {
        self.build_inline_list(wr_id, bufs, |q| unsafe { (*q).wr_send.unwrap()(q) })
    }

    /// Post a SEND carrying a gathered inline payload and a 32-bit immediate (host byte order).
    ///
    /// See [`send_inline_list`](Self::send_inline_list) for the gathering behavior.
    #[inline]
    pub fn send_imm_inline_list(self, wr_id: u64, bufs: &[io::IoSlice<'_>], imm: u32) {
        self.build_inline_list(wr_id, bufs, move |q| unsafe {
            (*q).wr_send_imm.unwrap()(q, imm.to_be())
        })
    }

    /// Post an RDMA WRITE into `remote` whose inline payload is gathered from several buffers.
    ///
    /// See [`send_inline_list`](Self::send_inline_list) for the gathering behavior.
    #[inline]
    pub fn write_inline_list(
        self,
        wr_id: u64,
        bufs: &[io::IoSlice<'_>],
        remote: RemoteMemorySlice,
    ) {
        self.build_inline_list(wr_id, bufs, move |q| unsafe {
            (*q).wr_rdma_write.unwrap()(q, remote.rkey, remote.addr)
        })
    }

    /// Post an RDMA WRITE into `remote` carrying a gathered inline payload and a 32-bit immediate
    /// (host byte order).
    ///
    /// See [`send_inline_list`](Self::send_inline_list) for the gathering behavior.
    #[inline]
    pub fn write_imm_inline_list(
        self,
        wr_id: u64,
        bufs: &[io::IoSlice<'_>],
        remote: RemoteMemorySlice,
        imm: u32,
    ) {
        self.build_inline_list(wr_id, bufs, move |q| unsafe {
            (*q).wr_rdma_write_imm.unwrap()(q, remote.rkey, remote.addr, imm.to_be())
        })
    }

    /// Post a SEND carrying a 32-bit immediate (host byte order).
    #[inline]
    pub fn send_imm(self, wr_id: u64, local: &[LocalMemorySlice], imm: u32) {
        self.build(wr_id, local, move |q| unsafe {
            (*q).wr_send_imm.unwrap()(q, imm.to_be())
        })
    }

    /// Post an RDMA WRITE into `remote`.
    #[inline]
    pub fn write(self, wr_id: u64, local: &[LocalMemorySlice], remote: RemoteMemorySlice) {
        self.build(wr_id, local, move |q| unsafe {
            (*q).wr_rdma_write.unwrap()(q, remote.rkey, remote.addr)
        })
    }

    /// Post an RDMA WRITE carrying a 32-bit immediate (host byte order).
    #[inline]
    pub fn write_imm(
        self,
        wr_id: u64,
        local: &[LocalMemorySlice],
        remote: RemoteMemorySlice,
        imm: u32,
    ) {
        self.build(wr_id, local, move |q| unsafe {
            (*q).wr_rdma_write_imm.unwrap()(q, remote.rkey, remote.addr, imm.to_be())
        })
    }

    /// Post an RDMA READ from `remote` into `local`.
    #[inline]
    pub fn read(self, wr_id: u64, local: &[LocalMemorySlice], remote: RemoteMemorySlice) {
        self.build(wr_id, local, move |q| unsafe {
            (*q).wr_rdma_read.unwrap()(q, remote.rkey, remote.addr)
        })
    }

    /// Post an atomic compare-and-swap on the 8-byte value at `remote`.
    #[inline]
    pub fn atomic_cmp_swap(
        self,
        wr_id: u64,
        local: &[LocalMemorySlice],
        remote: RemoteMemorySlice,
        compare: u64,
        swap: u64,
    ) {
        self.build(wr_id, local, move |q| unsafe {
            (*q).wr_atomic_cmp_swp.unwrap()(q, remote.rkey, remote.addr, compare, swap)
        })
    }

    /// Post an atomic fetch-and-add on the 8-byte value at `remote`.
    #[inline]
    pub fn atomic_fetch_add(
        self,
        wr_id: u64,
        local: &[LocalMemorySlice],
        remote: RemoteMemorySlice,
        add: u64,
    ) {
        self.build(wr_id, local, move |q| unsafe {
            (*q).wr_atomic_fetch_add.unwrap()(q, remote.rkey, remote.addr, add)
        })
    }
}
/// A set of queue-pair attributes together with a mask of which of them are present.
///
/// Used with [`QueuePair::modify`] to change a queue pair's attributes (the general escape hatch
/// for transitions that [`PreparedQueuePair::handshake`] and friends do not cover), and returned by
/// [`QueuePair::query`]. Each `set_*` method records its field in the mask; [`modify`] reads only
/// the fields the mask marks as present, and after a [`query`] the getters are meaningful only for
/// the fields that were requested.
///
/// [`modify`]: QueuePair::modify
/// [`query`]: QueuePair::query
#[derive(Clone)]
pub struct QueuePairAttribute {
    attr: ffi::ibv_qp_attr,
    mask: ffi::ibv_qp_attr_mask,
}

impl Default for QueuePairAttribute {
    fn default() -> Self {
        Self::new()
    }
}

impl QueuePairAttribute {
    /// Create an empty set of attributes (no fields present).
    pub fn new() -> Self {
        QueuePairAttribute {
            attr: ffi::ibv_qp_attr::default(),
            mask: ffi::ibv_qp_attr_mask(0),
        }
    }

    /// Build attributes from a raw `ibv_qp_attr` and mask.
    ///
    /// This is for attributes produced elsewhere, for example the values the RDMA connection
    /// manager fills in via `rdma_init_qp_attr`.
    pub fn from_raw(attr: ffi::ibv_qp_attr, mask: ffi::ibv_qp_attr_mask) -> Self {
        QueuePairAttribute { attr, mask }
    }

    /// The underlying `ibv_qp_attr`. Escape hatch for fields this crate does not wrap.
    pub fn as_raw(&self) -> &ffi::ibv_qp_attr {
        &self.attr
    }

    /// The mask of which attributes are present.
    pub fn mask(&self) -> ffi::ibv_qp_attr_mask {
        self.mask
    }

    /// Set the next queue-pair state. Not every transition is valid; see [`QueuePair::modify`].
    pub fn set_state(&mut self, state: ffi::ibv_qp_state) -> &mut Self {
        self.attr.qp_state = state;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_STATE;
        self
    }

    /// Set the assumed current state, used to make a transition conditional on it.
    pub fn set_current_state(&mut self, state: ffi::ibv_qp_state) -> &mut Self {
        self.attr.cur_qp_state = state;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_CUR_STATE;
        self
    }

    /// Set the primary partition-key (P_Key) index.
    pub fn set_pkey_index(&mut self, pkey_index: u16) -> &mut Self {
        self.attr.pkey_index = pkey_index;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_PKEY_INDEX;
        self
    }

    /// Set the primary physical port number (ports are numbered from 1).
    pub fn set_port(&mut self, port_num: u8) -> &mut Self {
        self.attr.port_num = port_num;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_PORT;
        self
    }

    /// Set the remote-access flags (RC/UC only).
    pub fn set_access_flags(&mut self, access_flags: ffi::ibv_access_flags) -> &mut Self {
        self.attr.qp_access_flags = access_flags.0;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
        self
    }

    /// Set the path MTU (RC/UC only).
    pub fn set_path_mtu(&mut self, path_mtu: ffi::ibv_mtu) -> &mut Self {
        self.attr.path_mtu = path_mtu;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_PATH_MTU;
        self
    }

    /// Set the destination queue-pair number (24 bits; RC/UC only).
    pub fn set_dest_qp_num(&mut self, dest_qp_num: u32) -> &mut Self {
        self.attr.dest_qp_num = dest_qp_num;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_DEST_QPN;
        self
    }

    /// Set the receive-queue packet sequence number (24 bits).
    pub fn set_rq_psn(&mut self, rq_psn: u32) -> &mut Self {
        self.attr.rq_psn = rq_psn;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_RQ_PSN;
        self
    }

    /// Set the send-queue packet sequence number (24 bits).
    pub fn set_sq_psn(&mut self, sq_psn: u32) -> &mut Self {
        self.attr.sq_psn = sq_psn;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_SQ_PSN;
        self
    }

    /// Set the number of outstanding RDMA reads and atomics this queue pair issues as the initiator
    /// (RC only).
    pub fn set_max_rd_atomic(&mut self, max_rd_atomic: u8) -> &mut Self {
        self.attr.max_rd_atomic = max_rd_atomic;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC;
        self
    }

    /// Set the number of outstanding RDMA reads and atomics this queue pair handles as the
    /// destination (RC only).
    pub fn set_max_dest_rd_atomic(&mut self, max_dest_rd_atomic: u8) -> &mut Self {
        self.attr.max_dest_rd_atomic = max_dest_rd_atomic;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC;
        self
    }

    /// Set the minimum RNR-NAK timer.
    pub fn set_min_rnr_timer(&mut self, min_rnr_timer: u8) -> &mut Self {
        self.attr.min_rnr_timer = min_rnr_timer;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER;
        self
    }

    /// Set the ACK timeout (the actual time is `4.096 * 2^timeout` microseconds; 0 means infinite).
    pub fn set_timeout(&mut self, timeout: u8) -> &mut Self {
        self.attr.timeout = timeout;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_TIMEOUT;
        self
    }

    /// Set the retry count for the primary path.
    pub fn set_retry_count(&mut self, retry_count: u8) -> &mut Self {
        self.attr.retry_cnt = retry_count;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_RETRY_CNT;
        self
    }

    /// Set the RNR retry count (7 means retry infinitely).
    pub fn set_rnr_retry(&mut self, rnr_retry: u8) -> &mut Self {
        self.attr.rnr_retry = rnr_retry;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_RNR_RETRY;
        self
    }

    /// Set the Q_Key (UD only).
    pub fn set_qkey(&mut self, qkey: u32) -> &mut Self {
        self.attr.qkey = qkey;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_QKEY;
        self
    }

    /// Set the primary path's address vector, describing how to reach the remote queue pair.
    pub fn set_address_vector(&mut self, ah_attr: &AddressHandleAttribute) -> &mut Self {
        self.attr.ah_attr = ah_attr.attr;
        self.mask |= ffi::ibv_qp_attr_mask::IBV_QP_AV;
        self
    }

    /// The queue-pair state (the value set, or the one read back by [`QueuePair::query`]).
    pub fn state(&self) -> ffi::ibv_qp_state {
        self.attr.qp_state
    }

    /// The partition-key index.
    pub fn pkey_index(&self) -> u16 {
        self.attr.pkey_index
    }

    /// The physical port number.
    pub fn port(&self) -> u8 {
        self.attr.port_num
    }

    /// The remote-access flags.
    pub fn access_flags(&self) -> ffi::ibv_access_flags {
        ffi::ibv_access_flags(self.attr.qp_access_flags)
    }

    /// The path MTU. Meaningful only if it was set or queried.
    pub fn path_mtu(&self) -> ffi::ibv_mtu {
        self.attr.path_mtu
    }

    /// The destination queue-pair number.
    pub fn dest_qp_num(&self) -> u32 {
        self.attr.dest_qp_num
    }

    /// The receive-queue packet sequence number.
    pub fn rq_psn(&self) -> u32 {
        self.attr.rq_psn
    }

    /// The send-queue packet sequence number.
    pub fn sq_psn(&self) -> u32 {
        self.attr.sq_psn
    }

    /// The number of outstanding RDMA reads and atomics issued as the initiator.
    pub fn max_rd_atomic(&self) -> u8 {
        self.attr.max_rd_atomic
    }

    /// The number of outstanding RDMA reads and atomics handled as the destination.
    pub fn max_dest_rd_atomic(&self) -> u8 {
        self.attr.max_dest_rd_atomic
    }

    /// The minimum RNR-NAK timer.
    pub fn min_rnr_timer(&self) -> u8 {
        self.attr.min_rnr_timer
    }

    /// The ACK timeout.
    pub fn timeout(&self) -> u8 {
        self.attr.timeout
    }

    /// The primary-path retry count.
    pub fn retry_count(&self) -> u8 {
        self.attr.retry_cnt
    }

    /// The RNR retry count.
    pub fn rnr_retry(&self) -> u8 {
        self.attr.rnr_retry
    }

    /// The Q_Key.
    pub fn qkey(&self) -> u32 {
        self.attr.qkey
    }
}

/// The configured capacities of a queue pair, as returned by [`QueuePair::query`].
pub struct QueuePairInitAttribute {
    init_attr: ffi::ibv_qp_init_attr,
}

impl QueuePairInitAttribute {
    /// The maximum number of outstanding send work requests.
    pub fn max_send_wr(&self) -> u32 {
        self.init_attr.cap.max_send_wr
    }

    /// The maximum number of outstanding receive work requests.
    pub fn max_recv_wr(&self) -> u32 {
        self.init_attr.cap.max_recv_wr
    }

    /// The maximum number of scatter-gather entries per send work request.
    pub fn max_send_sge(&self) -> u32 {
        self.init_attr.cap.max_send_sge
    }

    /// The maximum number of scatter-gather entries per receive work request.
    pub fn max_recv_sge(&self) -> u32 {
        self.init_attr.cap.max_recv_sge
    }

    /// The maximum amount of inline data, in bytes.
    pub fn max_inline_data(&self) -> u32 {
        self.init_attr.cap.max_inline_data
    }

    /// The underlying `ibv_qp_init_attr`. Escape hatch for fields this crate does not wrap.
    pub fn as_raw(&self) -> &ffi::ibv_qp_init_attr {
        &self.init_attr
    }
}

/// The required and optional attribute-mask bits for a `cur -> next` transition of a queue pair of
/// the given type, or `None` if the transition is not valid.
///
/// This mirrors the kernel's `qp_state_table` (drivers/infiniband/core/verbs.c): every queue pair
/// may move to `RESET` or `ERR` from any state with only `IBV_QP_STATE`, and each type allows a
/// specific set of forward transitions. It is used only to turn an `EINVAL` from `ibv_modify_qp`
/// into a more precise [`Error`].
fn qp_transition_masks(
    qp_type: ffi::ibv_qp_type,
    cur: ffi::ibv_qp_state,
    next: ffi::ibv_qp_state,
) -> Option<(u32, u32)> {
    use ffi::ibv_qp_state::*;
    use ffi::ibv_qp_type::*;

    let state = ffi::ibv_qp_attr_mask::IBV_QP_STATE.0;
    let cur_state = ffi::ibv_qp_attr_mask::IBV_QP_CUR_STATE.0;
    let pkey = ffi::ibv_qp_attr_mask::IBV_QP_PKEY_INDEX.0;
    let port = ffi::ibv_qp_attr_mask::IBV_QP_PORT.0;
    let access = ffi::ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS.0;
    let qkey = ffi::ibv_qp_attr_mask::IBV_QP_QKEY.0;
    let av = ffi::ibv_qp_attr_mask::IBV_QP_AV.0;
    let path_mtu = ffi::ibv_qp_attr_mask::IBV_QP_PATH_MTU.0;
    let timeout = ffi::ibv_qp_attr_mask::IBV_QP_TIMEOUT.0;
    let retry = ffi::ibv_qp_attr_mask::IBV_QP_RETRY_CNT.0;
    let rnr_retry = ffi::ibv_qp_attr_mask::IBV_QP_RNR_RETRY.0;
    let rq_psn = ffi::ibv_qp_attr_mask::IBV_QP_RQ_PSN.0;
    let max_rd = ffi::ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC.0;
    let alt_path = ffi::ibv_qp_attr_mask::IBV_QP_ALT_PATH.0;
    let min_rnr = ffi::ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER.0;
    let sq_psn = ffi::ibv_qp_attr_mask::IBV_QP_SQ_PSN.0;
    let max_dest_rd = ffi::ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC.0;
    let mig = ffi::ibv_qp_attr_mask::IBV_QP_PATH_MIG_STATE.0;
    let dest_qpn = ffi::ibv_qp_attr_mask::IBV_QP_DEST_QPN.0;
    let rate = ffi::ibv_qp_attr_mask::IBV_QP_RATE_LIMIT.0;
    let sqd_async = ffi::ibv_qp_attr_mask::IBV_QP_EN_SQD_ASYNC_NOTIFY.0;

    // Any state may move to RESET or ERR with only IBV_QP_STATE.
    if let IBV_QPS_RESET | IBV_QPS_ERR = next {
        return Some((state, 0));
    }

    match qp_type {
        IBV_QPT_RC | IBV_QPT_XRC_SEND | IBV_QPT_XRC_RECV => match (cur, next) {
            (IBV_QPS_RESET, IBV_QPS_INIT) => Some((state | pkey | port | access, 0)),
            (IBV_QPS_INIT, IBV_QPS_INIT) => Some((0, pkey | port | access)),
            (IBV_QPS_INIT, IBV_QPS_RTR) => Some((
                state | av | path_mtu | dest_qpn | rq_psn | max_dest_rd | min_rnr,
                pkey | access | alt_path,
            )),
            (IBV_QPS_RTR, IBV_QPS_RTS) => Some((
                state | sq_psn | timeout | retry | rnr_retry | max_rd,
                cur_state | access | min_rnr | alt_path | mig,
            )),
            (IBV_QPS_RTS, IBV_QPS_RTS) => Some((0, cur_state | access | min_rnr | alt_path | mig)),
            (IBV_QPS_RTS, IBV_QPS_SQD) => Some((state, sqd_async)),
            (IBV_QPS_SQD, IBV_QPS_RTS) => {
                Some((state, cur_state | access | min_rnr | alt_path | mig))
            }
            (IBV_QPS_SQD, IBV_QPS_SQD) => Some((
                0,
                pkey | port
                    | access
                    | av
                    | max_rd
                    | min_rnr
                    | alt_path
                    | timeout
                    | retry
                    | rnr_retry
                    | max_dest_rd
                    | mig,
            )),
            _ => None,
        },
        IBV_QPT_UC => match (cur, next) {
            (IBV_QPS_RESET, IBV_QPS_INIT) => Some((state | pkey | port | access, 0)),
            (IBV_QPS_INIT, IBV_QPS_INIT) => Some((0, pkey | port | access)),
            (IBV_QPS_INIT, IBV_QPS_RTR) => Some((
                state | av | path_mtu | dest_qpn | rq_psn,
                pkey | access | alt_path,
            )),
            (IBV_QPS_RTR, IBV_QPS_RTS) => {
                Some((state | sq_psn, cur_state | access | alt_path | mig))
            }
            (IBV_QPS_RTS, IBV_QPS_RTS) => Some((0, cur_state | access | alt_path | mig)),
            (IBV_QPS_RTS, IBV_QPS_SQD) => Some((state, sqd_async)),
            (IBV_QPS_SQD, IBV_QPS_RTS) => Some((state, cur_state | access | alt_path | mig)),
            (IBV_QPS_SQD, IBV_QPS_SQD) => Some((0, pkey | port | access | av | alt_path | mig)),
            _ => None,
        },
        IBV_QPT_UD => match (cur, next) {
            (IBV_QPS_RESET, IBV_QPS_INIT) => Some((state | pkey | port | qkey, 0)),
            (IBV_QPS_INIT, IBV_QPS_INIT) => Some((0, pkey | port | qkey)),
            (IBV_QPS_INIT, IBV_QPS_RTR) => Some((state, pkey | qkey)),
            (IBV_QPS_RTR, IBV_QPS_RTS) => Some((state | sq_psn, cur_state | qkey)),
            (IBV_QPS_RTS, IBV_QPS_RTS) => Some((0, cur_state | qkey)),
            (IBV_QPS_RTS, IBV_QPS_SQD) => Some((state, sqd_async)),
            (IBV_QPS_SQD, IBV_QPS_RTS) => Some((state, cur_state | qkey)),
            (IBV_QPS_SQD, IBV_QPS_SQD) => Some((0, pkey | port | qkey)),
            (IBV_QPS_SQE, IBV_QPS_RTS) => Some((state, cur_state | qkey)),
            _ => None,
        },
        IBV_QPT_RAW_PACKET => match (cur, next) {
            (IBV_QPS_RESET, IBV_QPS_INIT) => Some((state | port, 0)),
            (IBV_QPS_INIT, IBV_QPS_INIT) => Some((0, port)),
            (IBV_QPS_INIT, IBV_QPS_RTR) => Some((state, 0)),
            (IBV_QPS_RTR, IBV_QPS_RTS) => Some((state, rate)),
            (IBV_QPS_RTS, IBV_QPS_RTS) => Some((0, rate)),
            (IBV_QPS_RTS, IBV_QPS_SQD) => Some((state, sqd_async)),
            (IBV_QPS_SQD, IBV_QPS_RTS) => Some((state, rate)),
            (IBV_QPS_SQD, IBV_QPS_SQD) => Some((0, port | rate)),
            _ => None,
        },
        _ => None,
    }
}

/// A fully initialized and ready `QueuePair`.
///
/// A queue pair is the actual object that sends and receives data in the RDMA architecture
/// (something like a socket). It's not exactly like a socket, however. A socket is an abstraction,
/// which is maintained by the network stack and doesn't have a physical resource behind it. A QP
/// is a resource of an RDMA device and a QP number can be used by one process at the same time
/// (similar to a socket that is associated with a specific TCP or UDP port number)
#[must_use = "QueuePair is immediately destroyed via drop() unless assigned to a variable"]
pub struct QueuePair {
    pub(crate) pd: Arc<ProtectionDomainInner>,
    pub(crate) _srq: Option<SharedReceiveQueue>,
    // Keep the completion queues alive while the queue pair references them; `ibv_destroy_cq` fails
    // with EBUSY if a queue pair is still attached.
    pub(crate) _send_cq: Arc<CompletionQueueInner>,
    pub(crate) _recv_cq: Arc<CompletionQueueInner>,
    pub(crate) qp: *mut ffi::ibv_qp,
    // The extended (doorbell) view of `qp`, used by the send path. `ibv_qp_to_qp_ex` is a cast, so
    // this aliases `qp` and lives exactly as long.
    pub(crate) qp_ex: *mut ffi::ibv_qp_ex,
}

unsafe impl Send for QueuePair {}
unsafe impl Sync for QueuePair {}

impl QueuePair {
    /// Returns the local QP number of this QueuePair.
    pub fn qp_num(&self) -> u32 {
        unsafe { *self.qp }.qp_num
    }

    /// Returns the underlying `ibv_qp` pointer.
    ///
    /// This is an escape hatch for verbs this crate does not yet wrap. The send path uses the
    /// extended (doorbell) interface; see [`as_raw_ex`](Self::as_raw_ex) for that view. The pointer
    /// is owned by this [`QueuePair`] and stays valid only while it is alive; do not destroy it.
    pub fn as_raw(&self) -> *mut ffi::ibv_qp {
        self.qp
    }

    /// Returns the underlying `ibv_qp_ex` pointer (the extended/doorbell view of this queue pair).
    ///
    /// This is an escape hatch for verbs this crate does not yet wrap. `ibv_qp_to_qp_ex` is a cast,
    /// so this aliases [`as_raw`](Self::as_raw) and lives exactly as long. The pointer stays valid
    /// only while this [`QueuePair`] is alive; do not destroy it.
    pub fn as_raw_ex(&self) -> *mut ffi::ibv_qp_ex {
        self.qp_ex
    }

    /// Modify this queue pair's attributes (`ibv_modify_qp`).
    ///
    /// This is the general escape hatch for transitions and attribute changes that
    /// [`PreparedQueuePair::handshake`], [`activate_ud`](PreparedQueuePair::activate_ud), and (with
    /// the `efa` feature) `activate_srd` do not cover: changing access flags at runtime, draining
    /// the send queue, moving the queue pair to `ERR` for teardown, setting a custom partition-key
    /// index or packet sequence number, and so on. Only the attributes whose mask bits are set in
    /// `attr` are applied.
    ///
    /// # Errors
    ///
    /// If the device rejects the transition with `EINVAL`, the crate consults its queue-pair state
    /// table and returns [`Error::InvalidQueuePairTransition`] or
    /// [`Error::InvalidQueuePairAttributeMask`] where it can pinpoint the problem, and
    /// [`Error::ModifyQueuePair`] otherwise.
    pub fn modify(&mut self, attr: &QueuePairAttribute) -> Result<()> {
        let mut a = attr.attr;
        let errno = unsafe { ffi::ibv_modify_qp(self.qp, &mut a as *mut _, attr.mask.0 as i32) };
        if errno == 0 {
            return Ok(());
        }
        if errno == nix::libc::EINVAL {
            let next = if attr.mask.0 & ffi::ibv_qp_attr_mask::IBV_QP_STATE.0 != 0 {
                attr.attr.qp_state
            } else {
                unsafe { (*self.qp).state }
            };
            return Err(self.diagnose_modify(attr.mask, next));
        }
        Err(Error::errno(errno, Error::ModifyQueuePair))
    }

    /// Query this queue pair's attributes (`ibv_query_qp`).
    ///
    /// `mask` selects which attributes to read; the returned [`QueuePairAttribute`]'s getters are
    /// meaningful only for the requested fields. The second return value describes the queue pair's
    /// configured capacities.
    pub fn query(
        &self,
        mask: ffi::ibv_qp_attr_mask,
    ) -> Result<(QueuePairAttribute, QueuePairInitAttribute)> {
        let mut attr = ffi::ibv_qp_attr::default();
        // `ibv_qp_init_attr` has no valid all-zero representation (its `qp_type` enum has no 0
        // variant), so zero the storage and only `assume_init` once `ibv_query_qp` has filled it in.
        let mut init_attr = std::mem::MaybeUninit::<ffi::ibv_qp_init_attr>::zeroed();
        let errno = unsafe {
            ffi::ibv_query_qp(
                self.qp,
                &mut attr as *mut _,
                mask.0 as i32,
                init_attr.as_mut_ptr(),
            )
        };
        if errno != 0 {
            return Err(Error::errno(errno, Error::QueryQueuePair));
        }
        let init_attr = unsafe { init_attr.assume_init() };
        Ok((
            QueuePairAttribute { attr, mask },
            QueuePairInitAttribute { init_attr },
        ))
    }

    /// Turn a rejected [`modify`](Self::modify) into a precise [`Error`], consulting the queue-pair
    /// state table for the actual queue-pair type and current state.
    fn diagnose_modify(&self, mask: ffi::ibv_qp_attr_mask, next: ffi::ibv_qp_state) -> Error {
        let raw = || Error::ModifyQueuePair(io::Error::from_raw_os_error(nix::libc::EINVAL));
        let cur = unsafe { (*self.qp).state };
        let qp_type = unsafe { (*self.qp).qp_type };
        // Only the types with a transition table can be diagnosed; others (e.g. driver/SRD) fall
        // back to the raw error.
        match qp_type {
            ffi::ibv_qp_type::IBV_QPT_RC
            | ffi::ibv_qp_type::IBV_QPT_UC
            | ffi::ibv_qp_type::IBV_QPT_UD
            | ffi::ibv_qp_type::IBV_QPT_RAW_PACKET
            | ffi::ibv_qp_type::IBV_QPT_XRC_SEND
            | ffi::ibv_qp_type::IBV_QPT_XRC_RECV => {}
            _ => return raw(),
        }
        match qp_transition_masks(qp_type, cur, next) {
            None => Error::InvalidQueuePairTransition { current: cur, next },
            Some((required, optional)) => {
                let invalid = mask.0 & !(required | optional);
                let needed = required & !mask.0;
                if invalid == 0 && needed == 0 {
                    raw()
                } else {
                    Error::InvalidQueuePairAttributeMask {
                        current: cur,
                        next,
                        invalid: ffi::ibv_qp_attr_mask(invalid),
                        needed: ffi::ibv_qp_attr_mask(needed),
                    }
                }
            }
        }
    }

    /// Posts a single send Work Request (WR) containing a scatter-gather list of local
    /// memory slices to the Send Queue of this Queue Pair.
    ///
    /// `wr_id` is a 64 bits value associated with this WR. If a Work Completion will be generated
    /// when this Work Request ends, it will contain this value.
    ///
    /// Internally, this is a convenience wrapper around [`start_send`](Self::start_send). The local memory
    /// slices will be sent as a single `ibv_send_wr` using `IBV_WR_SEND`. The send has
    /// `IBV_SEND_SIGNALED` set, so a work completion will also be triggered as a result of this send.
    /// # Safety
    ///
    /// See [`start_send`](Self::start_send) for more details on the asynchronous execution, safety, and errors.
    #[inline]
    pub unsafe fn post_send(&mut self, local: &[LocalMemorySlice], wr_id: u64) -> Result<()> {
        let mut batch = self.start_send();
        batch.signaled().send(wr_id, local);
        unsafe { batch.submit() }
    }

    /// Posts a single unreliable-datagram (UD) send, addressed by `ah` / `remote_qpn` /
    /// `remote_qkey`. The request is signaled, so it generates a work completion.
    ///
    /// Only valid on a UD queue pair (see [`PreparedQueuePair::activate_ud`]). A single UD queue
    /// pair can address many destinations by passing a different [`AddressHandle`] per call.
    ///
    /// # Safety
    ///
    /// See [`start_send`](Self::start_send). The address handle and the local buffers must remain valid until a
    /// work completion for this request has been retrieved.
    #[inline]
    pub unsafe fn post_send_ud(
        &mut self,
        local: &[LocalMemorySlice],
        ah: &AddressHandle,
        remote_qpn: u32,
        remote_qkey: u32,
        wr_id: u64,
    ) -> Result<()> {
        let mut batch = self.start_send();
        batch
            .signaled()
            .to(ah, remote_qpn, remote_qkey)
            .send(wr_id, local);
        unsafe { batch.submit() }
    }

    /// Posts a single receive Work Request (WR) containing a scatter-gather list of local
    /// memory slices to the Receive Queue of this Queue Pair.
    ///
    /// Generates a HW-specific Receive Request out of it and add it to the tail of the Queue
    /// Pair's Receive Queue without performing any context switch. The RDMA device will take one
    /// of those Work Requests as soon as an incoming opcode to that QP will consume a Receive
    /// Request (RR). If there is a failure in one of the WRs because the Receive Queue is full or
    /// one of the attributes in the WR is bad, it stops immediately and return the pointer to that
    /// WR.
    ///
    /// `wr_id` is a 64 bits value associated with this WR. When a Work Completion is generated
    /// when this Work Request ends, it will contain this value.
    ///
    /// Internally, the local memory slices will be received into as a single `ibv_recv_wr`.
    ///
    /// See also [RDMAmojo's `ibv_post_recv` documentation][1].
    ///
    /// # Safety
    ///
    /// The memory region can only be safely reused or dropped after the request is fully executed
    /// and a work completion has been retrieved from the corresponding completion queue (i.e.,
    /// until `CompletionQueue::poll` returns a completion for this receive).
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: Invalid value provided in the Work Request.
    ///  - `ENOMEM`: Receive Queue is full or not enough resources to complete this operation.
    ///  - `EFAULT`: Invalid value provided in `QueuePair`.
    ///
    /// [1]: http://www.rdmamojo.com/2013/02/02/ibv_post_recv/
    #[inline]
    pub unsafe fn post_receive(&mut self, local: &[LocalMemorySlice], wr_id: u64) -> Result<()> {
        unsafe { self.post_recv([RecvRequest::new(wr_id, local)]) }
    }

    /// Posts a batch of receive Work Requests to this Queue Pair's receive queue with a single
    /// `ibv_post_recv`.
    ///
    /// Receives have no doorbell form, so the requests are posted as a linked list. `recvs` is the
    /// caller's storage — a stack array or a reusable `Vec` — linked in place rather than copied, so
    /// posting allocates nothing. Each request is consumed by the device when a matching message
    /// arrives.
    ///
    /// On a UD queue pair the 40-byte GRH of an incoming message is placed at the front of the
    /// scatter buffers, so the payload starts at offset 40. If the queue pair uses a shared receive
    /// queue, post to the [`SharedReceiveQueue`] instead; its own receive queue is unused.
    ///
    /// # Safety
    ///
    /// Each referenced memory region must stay valid until a work completion has been polled for the
    /// corresponding `wr_id`.
    ///
    /// # Errors
    ///
    ///  - `EINVAL`: invalid value in one of the work requests.
    ///  - `ENOMEM`: the receive queue is full or out of resources.
    pub unsafe fn post_recv<'a>(&mut self, mut recvs: impl AsMut<[RecvRequest<'a>]>) -> Result<()> {
        let recvs = recvs.as_mut();
        if recvs.is_empty() {
            return Ok(());
        }
        // Link the requests into the list `ibv_post_recv` expects.
        for i in 0..recvs.len() - 1 {
            let next = &mut recvs[i + 1].wr as *mut ffi::ibv_recv_wr;
            recvs[i].wr.next = next;
        }
        recvs.last_mut().unwrap().wr.next = ptr::null_mut();

        let mut bad_wr: *mut ffi::ibv_recv_wr = ptr::null_mut();
        let ctx = unsafe { *self.qp }.context;
        let ops = &mut unsafe { *ctx }.ops;
        let errno = unsafe {
            ops.post_recv.as_mut().unwrap()(
                self.qp,
                &mut recvs[0].wr as *mut _,
                &mut bad_wr as *mut _,
            )
        };
        if errno != 0 {
            Err(Error::errno(errno, Error::PostReceive))
        } else {
            Ok(())
        }
    }

    #[inline]
    /// Remote RDMA write.
    ///
    /// Immediate data can be used to signal the completion of the write operation.
    /// The other side uses `post_recv` on a dummy buffer and gets the imm data from the work completion.
    ///
    /// Internally, this is a convenience wrapper around [`start_send`](Self::start_send).
    ///
    /// # Safety
    ///
    /// See [`start_send`](Self::start_send) for more details on the asynchronous execution, safety, and errors.
    pub unsafe fn post_write(
        &mut self,
        local: &[LocalMemorySlice],
        remote: RemoteMemorySlice,
        wr_id: u64,
        imm_data: Option<u32>,
    ) -> Result<()> {
        let mut batch = self.start_send();
        match imm_data {
            Some(imm) => batch.signaled().write_imm(wr_id, local, remote, imm),
            None => batch.signaled().write(wr_id, local, remote),
        };
        unsafe { batch.submit() }
    }

    #[inline]
    /// Remote RDMA read.
    ///
    /// RDMA read does not support immediate data.
    ///
    /// Internally, this is a convenience wrapper around [`start_send`](Self::start_send).
    ///
    /// # Safety
    ///
    /// See [`start_send`](Self::start_send) for more details on the asynchronous execution, safety, and errors.
    pub unsafe fn post_read(
        &mut self,
        local: &[LocalMemorySlice],
        remote: RemoteMemorySlice,
        wr_id: u64,
    ) -> Result<()> {
        let mut batch = self.start_send();
        batch.signaled().read(wr_id, local, remote);
        unsafe { batch.submit() }
    }

    /// Begin a batch of send work requests on this queue pair's send queue.
    ///
    /// Each builder method on the returned [`PostBatch`] (`send`, `write`, `read`, the atomics, ...)
    /// appends one request; chain [`signaled`](PostOp::signaled) to request a completion and
    /// [`to`](PostOp::to) to address a datagram send. Nothing reaches the device until
    /// [`submit`](PostBatch::submit), which rings the doorbell once for the whole batch.
    ///
    /// As with any send, the memory backing each scatter/gather slice must stay valid until a work
    /// completion has been polled for the request.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ibverbs::{QueuePair, LocalMemorySlice, RemoteMemorySlice};
    /// # unsafe fn f(qp: &mut QueuePair, payload: &[LocalMemorySlice], dest: RemoteMemorySlice, note: &[LocalMemorySlice]) -> ibverbs::Result<()> {
    /// let mut batch = qp.start_send();
    /// batch.write(1, payload, dest);
    /// batch.signaled().send(2, note);
    /// unsafe { batch.submit() }
    /// # }
    /// ```
    #[inline]
    pub fn start_send(&mut self) -> PostBatch<'_> {
        let qpx = self.qp_ex;
        unsafe { (*qpx).wr_start.unwrap()(qpx) };
        PostBatch {
            qpx,
            _qp: std::marker::PhantomData,
        }
    }
}

impl Drop for QueuePair {
    fn drop(&mut self) {
        // TODO: ibv_destroy_qp() fails if the QP is attached to a multicast group.
        let errno = unsafe { ffi::ibv_destroy_qp(self.qp) };
        if errno != 0 {
            let e = io::Error::from_raw_os_error(errno);
            panic!("{e}");
        }
    }
}

#[cfg(test)]
mod test_qp_transitions {
    use super::*;
    use ffi::ibv_qp_state::*;
    use ffi::ibv_qp_type::IBV_QPT_RC;

    fn bit(m: ffi::ibv_qp_attr_mask) -> u32 {
        m.0
    }

    #[test]
    fn reset_to_init_requires_pkey_port_access() {
        let (required, _optional) =
            qp_transition_masks(IBV_QPT_RC, IBV_QPS_RESET, IBV_QPS_INIT).unwrap();
        let expected = bit(ffi::ibv_qp_attr_mask::IBV_QP_STATE)
            | bit(ffi::ibv_qp_attr_mask::IBV_QP_PKEY_INDEX)
            | bit(ffi::ibv_qp_attr_mask::IBV_QP_PORT)
            | bit(ffi::ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS);
        assert_eq!(required, expected);
    }

    #[test]
    fn init_to_rts_is_not_a_valid_transition() {
        assert!(qp_transition_masks(IBV_QPT_RC, IBV_QPS_INIT, IBV_QPS_RTS).is_none());
    }

    #[test]
    fn any_state_to_reset_or_err_needs_only_state() {
        let state = bit(ffi::ibv_qp_attr_mask::IBV_QP_STATE);
        assert_eq!(
            qp_transition_masks(IBV_QPT_RC, IBV_QPS_RTS, IBV_QPS_ERR),
            Some((state, 0))
        );
        assert_eq!(
            qp_transition_masks(IBV_QPT_RC, IBV_QPS_INIT, IBV_QPS_RESET),
            Some((state, 0))
        );
    }
}
