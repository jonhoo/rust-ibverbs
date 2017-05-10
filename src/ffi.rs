include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

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
            status: ibv_wc_status::IBV_WC_SUCCESS,
            opcode: ibv_wc_opcode::IBV_WC_LOCAL_INV,
            vendor_err: 0,
            byte_len: 0,
            __bindgen_anon_1: ibv_wc__bindgen_ty_1 {
                imm_data: Default::default(),
                invalidated_rkey: Default::default(),
                bindgen_union_field: 0,
            },
            qp_num: 0,
            src_qp: 0,
            wc_flags: 0,
            pkey_index: 0,
            slid: 0,
            sl: 0,
            dlid_path_bits: 0,
        }
    }
}
