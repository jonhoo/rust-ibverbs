include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

/// An ibverb work completion.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct ibv_wc {
    wr_id: u64,
    status: ibv_wc_status,
    opcode: ibv_wc_opcode,
    vendor_err: u32,
    byte_len: u32,

    /// Immediate data OR the local RKey that was invalidated depending on `wc_flags`.
    /// See `man ibv_poll_cq` for details.
    pub imm_data: u32,
    /// Local QP number of completed WR
    pub qp_num: u32,
    /// Source QP number (remote QP number) of completed WR (valid only for UD QPs)
    pub src_qp: u32,
    /// Flags of the completed WR
    pub wc_flags: ibv_wc_flags,
    /// P_Key index (valid only for GSI QPs)
    pub pkey_index: u16,
    /// Source LID
    pub slid: u16,
    /// Service Level
    pub sl: u8,
    /// DLID path bits (not applicable for multicast messages)
    pub dlid_path_bits: u8,
}

impl ibv_wc {
    /// Returns the ID of the completed work request.
    pub fn wr_id(&self) -> u64 {
        self.wr_id
    }

    /// Returns the number of bytes transferred for this work request.
    pub fn len(&self) -> usize {
        self.byte_len as usize
    }

    /// Check if this work requested completed successfully.
    pub fn is_valid(&self) -> bool {
        self.status == ibv_wc_status::IBV_WC_SUCCESS
    }

    /// Returns the work completion status and vendor error syndrome (`vendor_err`) if the work
    /// request did not completed successfully.
    pub fn error(&self) -> Option<(ibv_wc_status, u32)> {
        match self.status {
            ibv_wc_status::IBV_WC_SUCCESS => None,
            status => Some((status, self.vendor_err)),
        }
    }

    /// Returns the operation type specified in the completed WR.
    pub fn opcode(&self) -> ibv_wc_opcode {
        self.opcode
    }

    /// Returns the immediate data of this work completion if provided.
    ///
    /// Note that IMM is only returned if `IBV_WC_WITH_IMM` is set in `wc_flags`. If this is not
    /// the case, no immediate value was provided, and `imm_data` should be interpreted
    /// differently. See `man ibv_poll_cq` for details.
    pub fn imm_data(&self) -> Option<u32> {
        if self.is_valid() && (self.wc_flags.0 & IBV_WC_WITH_IMM.0 != 0) {
            Some(self.imm_data)
        } else {
            None
        }
    }
}

#[test]
fn bindgen_test_layout_ibv_wc() {
    assert_eq!(::std::mem::size_of::<ibv_wc>(),
               48usize,
               concat!("Size of: ", stringify!(ibv_wc)));
    assert_eq!(::std::mem::align_of::<ibv_wc>(),
               8usize,
               concat!("Alignment of ", stringify!(ibv_wc)));
    assert_eq!(unsafe { &(*(0 as *const ibv_wc)).wr_id as *const _ as usize },
               0usize,
               concat!("Alignment of field: ",
                       stringify!(ibv_wc),
                       "::",
                       stringify!(wr_id)));
    assert_eq!(unsafe { &(*(0 as *const ibv_wc)).status as *const _ as usize },
               8usize,
               concat!("Alignment of field: ",
                       stringify!(ibv_wc),
                       "::",
                       stringify!(status)));
    assert_eq!(unsafe { &(*(0 as *const ibv_wc)).opcode as *const _ as usize },
               12usize,
               concat!("Alignment of field: ",
                       stringify!(ibv_wc),
                       "::",
                       stringify!(opcode)));
    assert_eq!(unsafe { &(*(0 as *const ibv_wc)).vendor_err as *const _ as usize },
               16usize,
               concat!("Alignment of field: ",
                       stringify!(ibv_wc),
                       "::",
                       stringify!(vendor_err)));
    assert_eq!(unsafe { &(*(0 as *const ibv_wc)).byte_len as *const _ as usize },
               20usize,
               concat!("Alignment of field: ",
                       stringify!(ibv_wc),
                       "::",
                       stringify!(byte_len)));
    assert_eq!(unsafe { &(*(0 as *const ibv_wc)).qp_num as *const _ as usize },
               28usize,
               concat!("Alignment of field: ",
                       stringify!(ibv_wc),
                       "::",
                       stringify!(qp_num)));
    assert_eq!(unsafe { &(*(0 as *const ibv_wc)).src_qp as *const _ as usize },
               32usize,
               concat!("Alignment of field: ",
                       stringify!(ibv_wc),
                       "::",
                       stringify!(src_qp)));
    assert_eq!(unsafe { &(*(0 as *const ibv_wc)).wc_flags as *const _ as usize },
               36usize,
               concat!("Alignment of field: ",
                       stringify!(ibv_wc),
                       "::",
                       stringify!(wc_flags)));
    assert_eq!(unsafe { &(*(0 as *const ibv_wc)).pkey_index as *const _ as usize },
               40usize,
               concat!("Alignment of field: ",
                       stringify!(ibv_wc),
                       "::",
                       stringify!(pkey_index)));
    assert_eq!(unsafe { &(*(0 as *const ibv_wc)).slid as *const _ as usize },
               42usize,
               concat!("Alignment of field: ",
                       stringify!(ibv_wc),
                       "::",
                       stringify!(slid)));
    assert_eq!(unsafe { &(*(0 as *const ibv_wc)).sl as *const _ as usize },
               44usize,
               concat!("Alignment of field: ",
                       stringify!(ibv_wc),
                       "::",
                       stringify!(sl)));
    assert_eq!(unsafe { &(*(0 as *const ibv_wc)).dlid_path_bits as *const _ as usize },
               45usize,
               concat!("Alignment of field: ",
                       stringify!(ibv_wc),
                       "::",
                       stringify!(dlid_path_bits)));
}

impl Default for ibv_qp_cap {
    fn default() -> Self {
        ibv_qp_cap {
            max_send_wr: 0,
            max_recv_wr: 0,
            max_send_sge: 0,
            max_recv_sge: 0,
            max_inline_data: 0,
        }
    }
}

impl Default for ibv_gid {
    fn default() -> Self {
        ibv_gid {
            raw: Default::default(),
            global: Default::default(),
            bindgen_union_field: [0; 2],
        }
    }
}

impl Default for ibv_global_route {
    fn default() -> Self {
        ibv_global_route {
            dgid: ibv_gid::default(),
            flow_label: 0,
            sgid_index: 0,
            hop_limit: 0,
            traffic_class: 0,
        }
    }
}

impl Default for ibv_ah_attr {
    fn default() -> Self {
        ibv_ah_attr {
            grh: ibv_global_route::default(),
            dlid: 0,
            sl: 0,
            src_path_bits: 0,
            static_rate: 0,
            is_global: 0,
            port_num: 0,
        }
    }
}

impl Default for ibv_port_attr {
    fn default() -> Self {
        ibv_port_attr {
            state: ibv_port_state::IBV_PORT_NOP,
            max_mtu: ibv_mtu::IBV_MTU_256,
            active_mtu: ibv_mtu::IBV_MTU_256,
            gid_tbl_len: 0,
            port_cap_flags: 0,
            max_msg_sz: 0,
            bad_pkey_cntr: 0,
            qkey_viol_cntr: 0,
            pkey_tbl_len: 0,
            lid: 0,
            sm_lid: 0,
            lmc: 0,
            max_vl_num: 0,
            sm_sl: 0,
            subnet_timeout: 0,
            init_type_reply: 0,
            active_width: 0,
            active_speed: 0,
            phys_state: 0,
            link_layer: 0,
            reserved: 0,
        }
    }
}

impl Default for ibv_send_wr__bindgen_ty_1 {
    fn default() -> Self {
        ibv_send_wr__bindgen_ty_1 {
            rdma: Default::default(),
            atomic: Default::default(),
            ud: Default::default(),
            bindgen_union_field: [0; 4],
        }
    }
}

impl Default for ibv_send_wr__bindgen_ty_2 {
    fn default() -> Self {
        ibv_send_wr__bindgen_ty_2 {
            xrc: Default::default(),
            bindgen_union_field: 0,
        }
    }
}

impl Default for ibv_send_wr__bindgen_ty_3 {
    fn default() -> Self {
        ibv_send_wr__bindgen_ty_3 {
            bind_mw: Default::default(),
            tso: Default::default(),
            bindgen_union_field: [0; 6],
        }
    }
}

impl Default for ibv_qp_attr {
    fn default() -> Self {
        ibv_qp_attr {
            qp_state: ibv_qp_state::IBV_QPS_RESET,
            cur_qp_state: ibv_qp_state::IBV_QPS_RESET,
            path_mtu: ibv_mtu::IBV_MTU_256,
            path_mig_state: ibv_mig_state::IBV_MIG_MIGRATED,
            qkey: 0,
            rq_psn: 0,
            sq_psn: 0,
            dest_qp_num: 0,
            qp_access_flags: 0,
            cap: ibv_qp_cap::default(),
            ah_attr: ibv_ah_attr::default(),
            alt_ah_attr: ibv_ah_attr::default(),
            pkey_index: 0,
            alt_pkey_index: 0,
            en_sqd_async_notify: 0,
            sq_draining: 0,
            max_rd_atomic: 0,
            max_dest_rd_atomic: 0,
            min_rnr_timer: 0,
            port_num: 0,
            timeout: 0,
            retry_cnt: 0,
            rnr_retry: 0,
            alt_port_num: 0,
            alt_timeout: 0,
            rate_limit: 0,
        }
    }
}
impl Default for ibv_wc {
    fn default() -> Self {
        ibv_wc {
            wr_id: 0,
            status: ibv_wc_status::IBV_WC_GENERAL_ERR,
            opcode: ibv_wc_opcode::IBV_WC_LOCAL_INV,
            vendor_err: 0,
            byte_len: 0,
            imm_data: 0,
            qp_num: 0,
            src_qp: 0,
            wc_flags: ibv_wc_flags(0),
            pkey_index: 0,
            slid: 0,
            sl: 0,
            dlid_path_bits: 0,
        }
    }
}
