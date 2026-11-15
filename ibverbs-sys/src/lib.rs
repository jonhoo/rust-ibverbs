#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(missing_docs)]
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

// `___ibv_query_port`, `ibv_create_cq_ex`, `ibv_create_qp_ex`, `ibv_advise_mr`,
// `ibv_query_rt_values_ex`, and `ibv_query_device_ex` are `static inline` in verbs.h: they reach
// the provider through the op table embedded in `verbs_context`, so there is no exported symbol
// for bindgen to bind. The functions below reimplement that dispatch (`verbs_get_ctx_op`'s
// availability check and op lookup).

/// The provider's implementation of the extended op `$op` on `$context`, or `None` when the
/// context is not an extended one, the provider leaves the op out, or its `verbs_context`
/// predates it (rdma-core's `verbs_get_ctx_op`).
///
/// Only a context embedded in a `verbs_context` has the extended ops, and libibverbs marks those
/// by setting `abi_compat` to `__VERBS_ABI_IS_EXTENDED` (all ones); nothing in front of any other
/// context may be read (`verbs_get_ctx`). New ops are added at the front of `verbs_context`
/// ("grows up"), so the runtime library's struct can be smaller than the vendored one, and the
/// vendored struct's start can then lie before the provider's allocation. Everything is therefore
/// addressed backwards from `$context`, which every libibverbs version embeds last with `sz` (the
/// runtime struct's size) immediately before it: first `sz`, then the op only if the runtime
/// struct is large enough to reach back to it (the C inline's size check). Expands to unsafe
/// operations; `$context` must be a valid `ibv_context`.
macro_rules! verbs_get_ctx_op {
    ($context:expr, $op:ident) => {{
        let context: *mut ibv_context = $context;
        if (*context).abi_compat.addr() != usize::MAX {
            None
        } else {
            let context = context as *mut u8;
            let sz = context
                .sub(::std::mem::size_of::<usize>())
                .cast::<usize>()
                .read();
            let at = ::std::mem::offset_of!(verbs_context, $op);
            if sz >= ::std::mem::size_of::<verbs_context>() - at {
                // The op lies `back` bytes before the context, inside the runtime struct.
                let back = ::std::mem::offset_of!(verbs_context, context) - at;
                /// The type of field `$op`, without projecting through a `verbs_context` pointer.
                fn op_ptr<T>(_: fn(&verbs_context) -> &T, at: *mut u8) -> *mut T {
                    at.cast()
                }
                op_ptr(|v| &v.$op, context.sub(back)).read()
            } else {
                None
            }
        }
    }};
}

// The errno values the shims report or recognize: Linux's, the only platform rdma-core targets,
// which numbers `ENOSYS` and `EOPNOTSUPP` differently on mips and sparc.
const MIPS: bool = cfg!(any(
    target_arch = "mips",
    target_arch = "mips32r6",
    target_arch = "mips64",
    target_arch = "mips64r6"
));
const SPARC: bool = cfg!(any(target_arch = "sparc", target_arch = "sparc64"));
const EINVAL: ::std::os::raw::c_int = 22;
const ENOSYS: ::std::os::raw::c_int = if MIPS {
    89
} else if SPARC {
    90
} else {
    38
};
const EOPNOTSUPP: ::std::os::raw::c_int = if MIPS {
    122
} else if SPARC {
    45
} else {
    95
};

/// Query a port's attributes, including the extended fields (rdma-core's `___ibv_query_port`,
/// which the `ibv_query_port` macro resolves to in C).
///
/// The exported `ibv_query_port` symbol that bindgen binds is the *compat* entry point: it fills
/// only the legacy prefix of `ibv_port_attr`, leaving `port_cap_flags2` and `active_speed_ex`
/// zeroed. This shim queries through the provider op with the full struct size, so those fields
/// are filled when the provider supports them, and falls back to the compat symbol (zeroing the
/// full struct first) when it does not — matching the C inline.
///
/// # Safety
///
/// `context` must be a valid device context and `port_attr` a valid pointer for an
/// `ibv_query_port` call.
#[inline]
pub unsafe fn ___ibv_query_port(
    context: *mut ibv_context,
    port_num: u8,
    port_attr: *mut ibv_port_attr,
) -> ::std::os::raw::c_int {
    unsafe {
        match verbs_get_ctx_op!(context, query_port) {
            Some(query_port) => query_port(
                context,
                port_num,
                port_attr,
                ::std::mem::size_of::<ibv_port_attr>(),
            ),
            None => {
                ::std::ptr::write_bytes(port_attr, 0, 1);
                ibv_query_port(context, port_num, port_attr.cast())
            }
        }
    }
}

/// Set `errno`, so the shims below can report failures the way the C inlines do (they set
/// `errno = EOPNOTSUPP` before returning null).
///
/// rdma-core targets only Linux, where both glibc and musl expose `__errno_location`. It takes
/// no arguments and always returns the calling thread's `errno` slot, so calling it is safe; only
/// the write through the returned pointer is not.
fn set_errno(err: ::std::os::raw::c_int) {
    unsafe extern "C" {
        safe fn __errno_location() -> *mut ::std::os::raw::c_int;
    }
    unsafe { *__errno_location() = err }
}

/// Create an extended completion queue (`ibv_create_cq_ex`).
///
/// Returns null with `errno` set to `EOPNOTSUPP` if the provider does not implement the extended
/// verb (matching the C inline).
///
/// # Safety
///
/// `context` and `cq_attr` must be valid pointers for an `ibv_create_cq_ex` call.
#[inline]
pub unsafe fn ibv_create_cq_ex(
    context: *mut ibv_context,
    cq_attr: *mut ibv_cq_init_attr_ex,
) -> *mut ibv_cq_ex {
    unsafe {
        match verbs_get_ctx_op!(context, create_cq_ex) {
            Some(create_cq_ex) => create_cq_ex(context, cq_attr),
            None => {
                set_errno(EOPNOTSUPP);
                ::std::ptr::null_mut()
            }
        }
    }
}

// `ibv_create_qp_ex` hands an `ibv_qp_init_attr_ex` to `ibv_create_qp` as the
// `ibv_qp_init_attr` it begins with, as the C inline does.
const _: () = {
    use ::std::mem::offset_of;
    assert!(
        offset_of!(ibv_qp_init_attr_ex, qp_context) == offset_of!(ibv_qp_init_attr, qp_context)
    );
    assert!(offset_of!(ibv_qp_init_attr_ex, send_cq) == offset_of!(ibv_qp_init_attr, send_cq));
    assert!(offset_of!(ibv_qp_init_attr_ex, recv_cq) == offset_of!(ibv_qp_init_attr, recv_cq));
    assert!(offset_of!(ibv_qp_init_attr_ex, srq) == offset_of!(ibv_qp_init_attr, srq));
    assert!(offset_of!(ibv_qp_init_attr_ex, cap) == offset_of!(ibv_qp_init_attr, cap));
    assert!(offset_of!(ibv_qp_init_attr_ex, qp_type) == offset_of!(ibv_qp_init_attr, qp_type));
    assert!(
        offset_of!(ibv_qp_init_attr_ex, sq_sig_all) == offset_of!(ibv_qp_init_attr, sq_sig_all)
    );
};

/// Create an extended queue pair (`ibv_create_qp_ex`).
///
/// Like the C inline, this creates a queue pair whose attributes extend the legacy ones by
/// nothing but the protection domain (`comp_mask` is exactly `IBV_QP_INIT_ATTR_PD`) with the
/// legacy `ibv_create_qp`, and otherwise returns null with `errno` set to `EOPNOTSUPP` if the
/// provider does not implement the extended verb.
///
/// # Safety
///
/// `context` and `qp_attr` must be valid pointers for an `ibv_create_qp_ex` call.
#[inline]
pub unsafe fn ibv_create_qp_ex(
    context: *mut ibv_context,
    qp_attr: *mut ibv_qp_init_attr_ex,
) -> *mut ibv_qp {
    unsafe {
        if (*qp_attr).comp_mask == ibv_qp_init_attr_mask::IBV_QP_INIT_ATTR_PD.0 {
            return ibv_create_qp((*qp_attr).pd, qp_attr.cast::<ibv_qp_init_attr>());
        }
        match verbs_get_ctx_op!(context, create_qp_ex) {
            Some(create_qp_ex) => create_qp_ex(context, qp_attr),
            None => {
                set_errno(EOPNOTSUPP);
                ::std::ptr::null_mut()
            }
        }
    }
}

/// Give advice about an address range in memory regions (`ibv_advise_mr`).
///
/// Returns `EOPNOTSUPP` if the provider does not implement the verb (matching the C inline).
///
/// # Safety
///
/// `pd` must be a valid protection domain, and `sg_list` must point to `num_sge` valid scatter/
/// gather entries describing ranges in memory regions registered under `pd`.
#[inline]
pub unsafe fn ibv_advise_mr(
    pd: *mut ibv_pd,
    advice: ib_uverbs_advise_mr_advice,
    flags: u32,
    sg_list: *mut ibv_sge,
    num_sge: u32,
) -> ::std::os::raw::c_int {
    unsafe {
        match verbs_get_ctx_op!((*pd).context, advise_mr) {
            Some(advise_mr) => advise_mr(pd, advice, flags, sg_list, num_sge),
            // The provider does not implement advise_mr; mirror the C inline's `return EOPNOTSUPP`.
            None => EOPNOTSUPP,
        }
    }
}

/// Query a device's real-time values, such as its free-running hardware clock
/// (`ibv_query_rt_values_ex`).
///
/// Returns `EOPNOTSUPP` if the provider does not implement the verb (matching the C inline).
///
/// # Safety
///
/// `context` and `values` must be valid pointers for an `ibv_query_rt_values_ex` call.
#[inline]
pub unsafe fn ibv_query_rt_values_ex(
    context: *mut ibv_context,
    values: *mut ibv_values_ex,
) -> ::std::os::raw::c_int {
    unsafe {
        match verbs_get_ctx_op!(context, query_rt_values) {
            Some(query_rt_values) => query_rt_values(context, values),
            // The provider does not implement query_rt_values; mirror the C inline's
            // `return EOPNOTSUPP`.
            None => EOPNOTSUPP,
        }
    }
}

/// Query extended device attributes (`ibv_query_device_ex`).
///
/// Falls back to `ibv_query_device` (filling in only `orig_attr`, with the rest zeroed) when the
/// provider does not implement the extended verb, mirroring the C inline. Returns `EINVAL` if a
/// non-null `input` carries an unsupported component mask.
///
/// # Safety
///
/// `context` must be a valid `ibv_context`. `input` may be null; if non-null it must point to a
/// valid `ibv_query_device_ex_input`. `attr` must point to a writable `ibv_device_attr_ex`.
#[inline]
pub unsafe fn ibv_query_device_ex(
    context: *mut ibv_context,
    input: *const ibv_query_device_ex_input,
    attr: *mut ibv_device_attr_ex,
) -> ::std::os::raw::c_int {
    // The only component mask the input may carry is reserved; reject anything set with EINVAL,
    // matching the C inline.
    if !input.is_null() && unsafe { (*input).comp_mask } != 0 {
        return EINVAL;
    }
    unsafe {
        if let Some(query_device_ex) = verbs_get_ctx_op!(context, query_device_ex) {
            let ret = query_device_ex(
                context,
                input,
                attr,
                ::std::mem::size_of::<ibv_device_attr_ex>(),
            );
            // EOPNOTSUPP or ENOSYS means the provider does not really implement the extended verb;
            // fall through to the legacy query like the C inline does.
            if ret != EOPNOTSUPP && ret != ENOSYS {
                return ret;
            }
        }
        // Legacy fallback: zero the whole struct, then fill only the base attributes.
        ::std::ptr::write_bytes(attr, 0, 1);
        ibv_query_device(context, &mut (*attr).orig_attr)
    }
}

// `ibv_poll_cq`, `ibv_req_notify_cq`, `ibv_post_send`, `ibv_post_recv`, and `ibv_post_srq_recv` are
// `static inline` as well, but dispatch through the legacy op table in `ibv_context` itself, which
// libibverbs fills completely (a stub returning `EOPNOTSUPP` stands in for any op the provider
// leaves out; every provider that creates completion queues implements `poll_cq`, for which that
// would read as a count), so they need no availability check. They read only the op pointer: other
// threads write other fields of the queue pair, the queue, and the context.

/// Poll a completion queue for up to `num_entries` work completions, copying them into `wc`
/// (`ibv_poll_cq`).
///
/// Returns the number of completions copied (0 when none are ready), or a negative value on
/// failure, which does not set `errno`.
///
/// # Safety
///
/// `cq` must be a valid completion queue, and `wc` must be valid for writes of `num_entries`
/// entries.
#[inline]
pub unsafe fn ibv_poll_cq(
    cq: *mut ibv_cq,
    num_entries: ::std::os::raw::c_int,
    wc: *mut ibv_wc,
) -> ::std::os::raw::c_int {
    unsafe { (*(*cq).context).ops.poll_cq.unwrap()(cq, num_entries, wc) }
}

/// Arm a completion queue so its next completion, or its next solicited one if `solicited_only`
/// is nonzero, raises a notification on its completion channel (`ibv_req_notify_cq`).
///
/// Returns 0 on success, or the `errno` value on failure.
///
/// # Safety
///
/// `cq` must be a valid completion queue.
#[inline]
pub unsafe fn ibv_req_notify_cq(
    cq: *mut ibv_cq,
    solicited_only: ::std::os::raw::c_int,
) -> ::std::os::raw::c_int {
    unsafe { (*(*cq).context).ops.req_notify_cq.unwrap()(cq, solicited_only) }
}

/// Post a linked list of send work requests to a queue pair's send queue (`ibv_post_send`).
///
/// Returns 0 on success, or the `errno` value on failure, with `bad_wr` set to the first request
/// that was not posted (the ones ahead of it were).
///
/// # Safety
///
/// `qp` must be a valid queue pair, `wr` a valid list of send work requests whose buffers stay
/// valid until they complete, and `bad_wr` valid for writes.
#[inline]
pub unsafe fn ibv_post_send(
    qp: *mut ibv_qp,
    wr: *mut ibv_send_wr,
    bad_wr: *mut *mut ibv_send_wr,
) -> ::std::os::raw::c_int {
    unsafe { (*(*qp).context).ops.post_send.unwrap()(qp, wr, bad_wr) }
}

/// Post a linked list of receive work requests to a queue pair's receive queue (`ibv_post_recv`).
///
/// Returns 0 on success, or the `errno` value on failure, with `bad_wr` set to the first request
/// that was not posted.
///
/// # Safety
///
/// `qp` must be a valid queue pair, `wr` a valid list of receive work requests whose buffers stay
/// valid until they complete, and `bad_wr` valid for writes.
#[inline]
pub unsafe fn ibv_post_recv(
    qp: *mut ibv_qp,
    wr: *mut ibv_recv_wr,
    bad_wr: *mut *mut ibv_recv_wr,
) -> ::std::os::raw::c_int {
    unsafe { (*(*qp).context).ops.post_recv.unwrap()(qp, wr, bad_wr) }
}

/// Post a linked list of receive work requests to a shared receive queue (`ibv_post_srq_recv`).
///
/// Returns 0 on success, or the `errno` value on failure, with `bad_recv_wr` set to the first
/// request that was not posted.
///
/// # Safety
///
/// `srq` must be a valid shared receive queue, `recv_wr` a valid list of receive work requests
/// whose buffers stay valid until they complete, and `bad_recv_wr` valid for writes.
#[inline]
pub unsafe fn ibv_post_srq_recv(
    srq: *mut ibv_srq,
    recv_wr: *mut ibv_recv_wr,
    bad_recv_wr: *mut *mut ibv_recv_wr,
) -> ::std::os::raw::c_int {
    unsafe { (*(*srq).context).ops.post_srq_recv.unwrap()(srq, recv_wr, bad_recv_wr) }
}

// `rdma_get_local_addr` and `rdma_get_peer_addr` are `static inline` in rdma_cma.h: they return
// pointers into the id's own `route.addr` storage, so there is no exported symbol for bindgen to
// bind. The functions below reimplement them.

/// The local address a connection-manager id is bound to (rdma_cma.h's inline
/// `rdma_get_local_addr`).
///
/// # Safety
///
/// `id` must be a valid `rdma_cm_id`. The returned pointer aliases `id`'s own storage and is
/// valid only as long as the id.
#[cfg(feature = "rdmacm")]
#[inline]
pub unsafe fn rdma_get_local_addr(id: *mut rdma_cm_id) -> *mut sockaddr {
    unsafe { &raw mut (*id).route.addr.__bindgen_anon_1.src_addr }
}

/// The remote address a connection-manager id is connected to (rdma_cma.h's inline
/// `rdma_get_peer_addr`).
///
/// # Safety
///
/// `id` must be a valid `rdma_cm_id`. The returned pointer aliases `id`'s own storage and is
/// valid only as long as the id.
#[cfg(feature = "rdmacm")]
#[inline]
pub unsafe fn rdma_get_peer_addr(id: *mut rdma_cm_id) -> *mut sockaddr {
    unsafe { &raw mut (*id).route.addr.__bindgen_anon_2.dst_addr }
}
