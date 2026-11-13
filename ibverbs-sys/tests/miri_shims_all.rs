//! Every sys shim that dispatches through `verbs_context`, on runtime layouts older than, equal
//! to, and (for the size check) too old for the vendored v65 headers. Under Miri this checks that
//! the lookup stays inside the provider's allocation and reads `sz` and the op only where the
//! runtime struct has them.
//!
//! cargo +nightly miri test -p ibverbs-sys --test miri_shims_all

#![cfg(target_pointer_width = "64")]

use std::cell::Cell;
use std::mem::{offset_of, size_of};
use std::os::raw::{c_int, c_void};

use ibverbs_sys as ffi;

thread_local! {
    /// How many provider ops the shims reached on this thread (the tests run in parallel).
    static CALLS: Cell<usize> = const { Cell::new(0) };
}

extern "C" fn fake_query_port(
    _: *mut ffi::ibv_context,
    _: u8,
    _: *mut ffi::ibv_port_attr,
    _: usize,
) -> c_int {
    CALLS.set(CALLS.get() + 1);
    0
}
extern "C" fn fake_create_cq_ex(
    _: *mut ffi::ibv_context,
    _: *mut ffi::ibv_cq_init_attr_ex,
) -> *mut ffi::ibv_cq_ex {
    CALLS.set(CALLS.get() + 1);
    std::ptr::dangling_mut()
}
extern "C" fn fake_create_qp_ex(
    _: *mut ffi::ibv_context,
    _: *mut ffi::ibv_qp_init_attr_ex,
) -> *mut ffi::ibv_qp {
    CALLS.set(CALLS.get() + 1);
    std::ptr::dangling_mut()
}
extern "C" fn fake_advise_mr(
    _: *mut ffi::ibv_pd,
    _: ffi::ib_uverbs_advise_mr_advice,
    _: u32,
    _: *mut ffi::ibv_sge,
    _: u32,
) -> c_int {
    CALLS.set(CALLS.get() + 1);
    0
}
extern "C" fn fake_query_rt_values(_: *mut ffi::ibv_context, _: *mut ffi::ibv_values_ex) -> c_int {
    CALLS.set(CALLS.get() + 1);
    0
}
extern "C" fn fake_query_device_ex(
    _: *mut ffi::ibv_context,
    _: *const ffi::ibv_query_device_ex_input,
    _: *mut ffi::ibv_device_attr_ex,
    _: usize,
) -> c_int {
    CALLS.set(CALLS.get() + 1);
    0
}

/// A runtime `verbs_context` with `slots` 8-byte slots before `sz`, every op the shims use placed
/// where that runtime has it (ops are prepended, so the distance from an op to `context` is the
/// same in every version), one calloc-like allocation starting at the first slot.
fn runtime_context(slots: usize) -> (Vec<*mut c_void>, *mut ffi::ibv_context) {
    let ctx_words = size_of::<ffi::ibv_context>() / 8;
    let mut mem = vec![std::ptr::null_mut::<c_void>(); slots + 1 + ctx_words];
    unsafe {
        mem.as_mut_ptr()
            .add(slots)
            .cast::<usize>()
            .write((slots + 1 + ctx_words) * 8)
    };
    let vendored = offset_of!(ffi::verbs_context, sz) / 8;
    let mut put = |at: usize, f: *mut c_void| {
        // Slots this runtime lacks are not there to fill.
        if let Some(idx) = (at / 8).checked_sub(vendored - slots) {
            mem[idx] = f;
        }
    };
    put(
        offset_of!(ffi::verbs_context, query_port),
        fake_query_port as *mut c_void,
    );
    put(
        offset_of!(ffi::verbs_context, create_cq_ex),
        fake_create_cq_ex as *mut c_void,
    );
    put(
        offset_of!(ffi::verbs_context, create_qp_ex),
        fake_create_qp_ex as *mut c_void,
    );
    put(
        offset_of!(ffi::verbs_context, advise_mr),
        fake_advise_mr as *mut c_void,
    );
    put(
        offset_of!(ffi::verbs_context, query_rt_values),
        fake_query_rt_values as *mut c_void,
    );
    put(
        offset_of!(ffi::verbs_context, query_device_ex),
        fake_query_device_ex as *mut c_void,
    );
    let ctx = unsafe { mem.as_mut_ptr().add(slots + 1) } as *mut ffi::ibv_context;
    unsafe { (*ctx).abi_compat = std::ptr::without_provenance_mut(usize::MAX) };
    (mem, ctx)
}

/// Calls every shim whose op `slots` covers; returns how many reached the provider's op.
fn dispatch_all(slots: usize) -> usize {
    CALLS.set(0);
    let (_mem, ctx) = runtime_context(slots);
    let vendored = offset_of!(ffi::verbs_context, sz) / 8;
    let has = |at: usize| at / 8 >= vendored - slots;
    unsafe {
        if has(offset_of!(ffi::verbs_context, query_port)) {
            let mut port = ffi::ibv_port_attr::default();
            assert_eq!(ffi::___ibv_query_port(ctx, 1, &mut port), 0);
        }
        if has(offset_of!(ffi::verbs_context, query_device_ex)) {
            let mut attr = ffi::ibv_device_attr_ex::default();
            assert_eq!(
                ffi::ibv_query_device_ex(ctx, std::ptr::null(), &mut attr),
                0
            );
        }
        // These four have no fallback through another libibverbs symbol, so they also run where
        // the runtime predates the op, and must then decline without reading behind `sz`.
        let mut cq_attr = ffi::ibv_cq_init_attr_ex::default();
        let cq = ffi::ibv_create_cq_ex(ctx, &mut cq_attr);
        assert_eq!(
            cq.is_null(),
            !has(offset_of!(ffi::verbs_context, create_cq_ex))
        );
        let mut qp_attr = ffi::ibv_qp_init_attr_ex {
            comp_mask: ffi::ibv_qp_init_attr_mask::IBV_QP_INIT_ATTR_PD.0
                | ffi::ibv_qp_init_attr_mask::IBV_QP_INIT_ATTR_SEND_OPS_FLAGS.0,
            ..Default::default()
        };
        let qp = ffi::ibv_create_qp_ex(ctx, &mut qp_attr);
        assert_eq!(
            qp.is_null(),
            !has(offset_of!(ffi::verbs_context, create_qp_ex))
        );
        let mut pd = ffi::ibv_pd {
            context: ctx,
            handle: 0,
        };
        let rc = ffi::ibv_advise_mr(
            &mut pd,
            ffi::ib_uverbs_advise_mr_advice::IB_UVERBS_ADVISE_MR_ADVICE_PREFETCH,
            0,
            std::ptr::null_mut(),
            0,
        );
        assert_eq!(rc == 0, has(offset_of!(ffi::verbs_context, advise_mr)));
        let mut values = ffi::ibv_values_ex::default();
        let rc = ffi::ibv_query_rt_values_ex(ctx, &mut values);
        assert_eq!(
            rc == 0,
            has(offset_of!(ffi::verbs_context, query_rt_values))
        );
    }
    CALLS.get()
}

#[test]
fn every_shim_on_every_runtime_layout() {
    let vendored = offset_of!(ffi::verbs_context, sz) / 8;
    assert_eq!(vendored, 45, "vendored rdma-core v65");
    // v65, v62/v63, v59..v61, v39..v58: all six ops present.
    for slots in [45, 43, 42, 39] {
        assert_eq!(dispatch_all(slots), 6, "{slots} slots");
    }
}

#[test]
fn a_runtime_that_predates_an_op_declines_it() {
    // Runtimes so old that the ops near the front are missing: each shim must decline (or fall
    // back) instead of reading in front of the provider's allocation.
    let vendored = offset_of!(ffi::verbs_context, sz) / 8;
    let first = offset_of!(ffi::verbs_context, advise_mr) / 8;
    // Just after `advise_mr` and `query_port` drop out, then fewer and fewer slots.
    for slots in [vendored - first - 1, 30, 20, 10, 1] {
        dispatch_all(slots);
    }
}
