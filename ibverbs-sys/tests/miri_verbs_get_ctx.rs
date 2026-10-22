//! The sys shims find a provider's extended ops in its `verbs_context`, which the *runtime*
//! libibverbs lays out. A provider allocates its context as
//! `struct xx_context { struct verbs_context ibv_ctx; ... }` (calloc'd, `ibv_ctx` first), and the
//! runtime library's `verbs_context` has as many op slots as that library version knows: new ops
//! are prepended, so with the vendored v65 headers (45 slots before `sz`) and an older runtime
//! library (v39..v58: 39 slots; v59..v61: 42; v62/v63: 43) the vendored struct's start lies before
//! the allocation. These tests dispatch through a shim on such a layout; under Miri they also check
//! that the dispatch stays inside the allocation:
//!
//! cargo +nightly miri test -p ibverbs-sys --test miri_verbs_get_ctx

// The layouts below count 8-byte op slots.
#![cfg(target_pointer_width = "64")]

use std::mem::{offset_of, size_of};
use std::os::raw::{c_int, c_void};

use ibverbs_sys as ffi;

extern "C" fn fake_query_rt_values(
    _ctx: *mut ffi::ibv_context,
    _values: *mut ffi::ibv_values_ex,
) -> c_int {
    0
}

/// Lay out a runtime `verbs_context` with `slots` op slots before `sz` (as an older or newer
/// libibverbs would), followed by `sz` and the `ibv_context`, in one allocation that starts at the
/// first op slot, exactly like a provider's calloc'd context. Returns the allocation and the
/// `ibv_context` pointer libibverbs would hand out.
fn runtime_context(slots: usize) -> (Vec<*mut c_void>, *mut ffi::ibv_context) {
    let ctx_words = size_of::<ffi::ibv_context>() / 8;
    // Pointer-typed slots, so the op pointer keeps its provenance.
    let mut mem = vec![std::ptr::null_mut::<c_void>(); slots + 1 + ctx_words];
    // `sz` = sizeof(the runtime verbs_context), as verbs_init_context sets it.
    unsafe {
        mem.as_mut_ptr()
            .add(slots)
            .cast::<usize>()
            .write((slots + 1 + ctx_words) * 8)
    };
    // Put query_rt_values where that runtime layout has it: the vendored layout's index, minus
    // the slots the runtime lacks at the front ("grows up": new ops are prepended).
    let vendored_slots = offset_of!(ffi::verbs_context, sz) / 8;
    let qrv = offset_of!(ffi::verbs_context, query_rt_values) / 8;
    let idx = qrv - (vendored_slots - slots);
    mem[idx] = fake_query_rt_values as *mut c_void;
    let base = mem.as_mut_ptr();
    let ctx = unsafe { base.add(slots + 1) } as *mut ffi::ibv_context;
    // `__VERBS_ABI_IS_EXTENDED`, as libibverbs marks every context it embeds in a `verbs_context`.
    unsafe { (*ctx).abi_compat = std::ptr::without_provenance_mut(usize::MAX) };
    (mem, ctx)
}

#[test]
fn same_version_runtime_is_fine() {
    let vendored_slots = offset_of!(ffi::verbs_context, sz) / 8;
    assert_eq!(vendored_slots, 45, "vendored rdma-core v65");
    let (_mem, ctx) = runtime_context(vendored_slots);
    let mut values = ffi::ibv_values_ex::default();
    let rc = unsafe { ffi::ibv_query_rt_values_ex(ctx, &mut values) };
    assert_eq!(rc, 0);
}

#[test]
fn older_runtime_stays_in_bounds() {
    // rdma-core v39..v58 (Ubuntu 22.04 ships 39, Ubuntu 24.04 ships 50): 39 slots before `sz`.
    let (_mem, ctx) = runtime_context(39);
    let mut values = ffi::ibv_values_ex::default();
    let rc = unsafe { ffi::ibv_query_rt_values_ex(ctx, &mut values) };
    assert_eq!(rc, 0);
}
