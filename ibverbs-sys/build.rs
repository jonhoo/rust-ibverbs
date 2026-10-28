use std::path::{Path, PathBuf};
use std::{env, fs};

/// The public rdma-core headers the bindings are generated from, each at the path its `#include`s
/// use and at its place in the vendored checkout. An rdma-core build only links these into its
/// include tree (nothing in them is generated), so laying them out is all the vendored build needs.
/// The package's `include` list in Cargo.toml ships exactly these.
const VENDORED_HEADERS: &[(&str, &str)] = &[
    ("infiniband/verbs.h", "libibverbs/verbs.h"),
    ("infiniband/verbs_api.h", "libibverbs/verbs_api.h"),
    ("infiniband/sa.h", "libibverbs/sa.h"),
    ("infiniband/tm_types.h", "libibverbs/tm_types.h"),
    (
        "infiniband/ib_user_ioctl_verbs.h",
        "kernel-headers/rdma/ib_user_ioctl_verbs.h",
    ),
    ("infiniband/efadv.h", "providers/efa/efadv.h"),
    ("infiniband/mlx5dv.h", "providers/mlx5/mlx5dv.h"),
    ("infiniband/mlx5_api.h", "providers/mlx5/mlx5_api.h"),
    (
        "infiniband/mlx5_user_ioctl_verbs.h",
        "kernel-headers/rdma/mlx5_user_ioctl_verbs.h",
    ),
    ("rdma/rdma_cma.h", "librdmacm/rdma_cma.h"),
    (
        "rdma/ib_user_verbs.h",
        "kernel-headers/rdma/ib_user_verbs.h",
    ),
    ("linux/stddef.h", "kernel-headers/linux/stddef.h"),
];

/// Lay the vendored headers out the way an installed rdma-core does, and return that include
/// directory. The copies keep their sources' modification times, so the `rerun-if-changed` lines
/// bindgen prints for them fire only when a vendored header itself changes.
fn vendored_include_dir() -> PathBuf {
    let vendor = Path::new("vendor/rdma-core");
    assert!(
        vendor.join("libibverbs/verbs.h").exists(),
        "the vendored rdma-core headers are missing: run `git submodule update --init` (vendor/rdma-core \
         is a git submodule), or set RDMA_CORE_INCLUDE_DIR to installed rdma-core headers"
    );
    let include =
        PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR is set by cargo")).join("include");
    for (to, from) in VENDORED_HEADERS {
        let from = vendor.join(from);
        let to = include.join(to);
        println!("cargo:rerun-if-changed={}", from.display());
        fs::create_dir_all(to.parent().expect("header paths have a directory"))
            .expect("failed to create the vendored include directory");
        fs::copy(&from, &to).unwrap_or_else(|e| panic!("failed to copy {}: {e}", from.display()));
        let modified = fs::metadata(&from)
            .and_then(|m| m.modified())
            .and_then(|t| fs::File::options().write(true).open(&to)?.set_modified(t));
        modified.unwrap_or_else(|e| panic!("failed to date {}: {e}", to.display()));
    }
    include
}

fn main() {
    println!("cargo:rustc-link-lib=ibverbs");

    let efa = env::var("CARGO_FEATURE_EFA").is_ok();
    if efa {
        // `efadv_create_qp_ex` and friends are exported from libefa.
        println!("cargo:rustc-link-lib=efa");
    }

    let mlx5 = env::var("CARGO_FEATURE_MLX5").is_ok();
    if mlx5 {
        println!("cargo:rustc-link-lib=mlx5");
    }

    let rdmacm = env::var("CARGO_FEATURE_RDMACM").is_ok();
    if rdmacm {
        // The `rdma_*` connection-manager functions are exported from librdmacm.
        println!("cargo:rustc-link-lib=rdmacm");
    }

    // What the bindings are generated from: installed rdma-core headers named by the caller, or the
    // vendored checkout's. Since the script prints `rerun-if-*` lines, cargo reruns it only for
    // these variables, the vendored headers, the headers bindgen reads (`CargoCallbacks`), and
    // this file.
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=RDMA_CORE_INCLUDE_DIR");
    println!("cargo:rerun-if-env-changed=RDMA_CORE_LIB_DIR");
    if let Ok(rdma_core_lib_dir) = env::var("RDMA_CORE_LIB_DIR") {
        println!("cargo:rustc-link-search=native={rdma_core_lib_dir}");
    }
    let rdma_core_include_dir = if let Some(dir) = env::var_os("RDMA_CORE_INCLUDE_DIR") {
        env::var("RDMA_CORE_LIB_DIR").expect(
            "When supplying RDMA_CORE_INCLUDE_DIR, you also need to supply RDMA_CORE_LIB_DIR",
        );
        PathBuf::from(dir)
    } else {
        vendored_include_dir()
    };
    // exported to dependents as `DEP_IBVERBS_INCLUDE`
    println!("cargo:include={}", rdma_core_include_dir.display());

    // Every header is reached through the include directory from one translation unit, so `verbs.h`
    // and the headers the features add come from the same rdma-core. Separate inputs would not do:
    // bindgen hands clang every input but the first as an `-include`, which clang reads before the
    // main file, so whichever `verbs.h` such an input pulls in would define the include guard first
    // and shadow the main one.
    let mut wrapper = String::from("#include <infiniband/verbs.h>\n");
    if efa {
        wrapper.push_str("#include <infiniband/efadv.h>\n");
    }
    if mlx5 {
        wrapper.push_str("#include <infiniband/mlx5dv.h>\n");
    }
    if rdmacm {
        wrapper.push_str("#include <rdma/rdma_cma.h>\n");
    }

    // generate the bindings
    let mut builder = bindgen::Builder::default()
        .header_contents("ibverbs_wrapper.h", &wrapper)
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        // Generate for the crate's MSRV and edition rather than for whichever toolchain runs the
        // build (bindgen's default), so the output does not vary with it and matches the Bazel
        // build's. `wrap_unsafe_ops` has nothing to act on today (the bindings contain no `unsafe
        // fn` bodies); it keeps any bindgen emits later, such as bitfield accessors, clean under
        // 2024's `unsafe_op_in_unsafe_fn`.
        .rust_target(bindgen::RustTarget::stable(85, 0).expect("1.85 is a known release"))
        .rust_edition(bindgen::RustEdition::Edition2024)
        .wrap_unsafe_ops(true)
        .clang_arg(format!("-I{}", rdma_core_include_dir.display()))
        .allowlist_function("ibv_.*")
        .allowlist_function("_ibv_.*")
        .allowlist_type("ibv_.*")
        // needed to reach the static-inline `ibv_create_cq_ex` / `ibv_create_qp_ex`, which dispatch
        // through the provider op table embedded in `verbs_context`
        .allowlist_type("verbs_context")
        // the `ibv_advise_mr_advice` / `ibv_advise_mr_flags` the public API spells are macro
        // aliases for these kernel enums, which `ibv_advise_mr` takes
        .allowlist_type("ib_uverbs_advise_mr_.*")
        .allowlist_var("IBV_LINK_LAYER_.*")
        .bitfield_enum("ibv_access_flags")
        .bitfield_enum("ibv_create_cq_wc_flags")
        .bitfield_enum("ibv_device_cap_flags")
        .bitfield_enum("ibv_odp_transport_cap_bits")
        .bitfield_enum("ibv_port_cap_flags")
        .bitfield_enum("ibv_port_cap_flags2")
        .bitfield_enum("ibv_qp_attr_mask")
        .bitfield_enum("ibv_qp_create_send_ops_flags")
        .bitfield_enum("ibv_qp_init_attr_mask")
        .bitfield_enum("ibv_qp_open_attr_mask")
        .bitfield_enum("ibv_raw_packet_caps")
        .bitfield_enum("ibv_rx_hash_fields")
        .bitfield_enum("ibv_send_flags")
        .bitfield_enum("ibv_srq_init_attr_mask")
        .bitfield_enum("ibv_wc_flags")
        .bitfield_enum("ibv_wq_attr_mask")
        .bitfield_enum("ibv_wq_flags")
        .bitfield_enum("ibv_xrcd_init_attr_mask")
        // C fills these in, and kernels and providers newer than the headers can hand back values
        // the headers do not name; a newtype holds any value, so neither that nor a zeroed
        // `Default` is ever an invalid enum
        .default_enum_style(bindgen::EnumVariation::NewType {
            is_bitfield: false,
            is_global: false,
        })
        .derive_default(true)
        .derive_debug(true)
        .prepend_enum_name(false)
        .blocklist_type("ibv_wc")
        .size_t_is_usize(true);

    if efa {
        // EFA SRD queue pairs are created through the provider's direct-verbs (`efadv`).
        builder = builder
            .allowlist_function("efadv_.*")
            .allowlist_type("efadv_.*")
            .allowlist_var("EFADV_QP_DRIVER_TYPE_.*");
    }

    if mlx5 {
        builder = builder
            .allowlist_function("mlx5dv_reg_dmabuf_mr")
            .allowlist_function("mlx5dv_get_data_direct_sysfs_path")
            .allowlist_type("mlx5_ib_uapi_reg_dmabuf_flags")
            .bitfield_enum("mlx5_ib_uapi_reg_dmabuf_flags");
    }

    if rdmacm {
        // The RDMA connection manager lives in `<rdma/rdma_cma.h>`.
        builder = builder
            .allowlist_function("rdma_.*")
            .allowlist_type("rdma_.*")
            // the `rdma_set_option` levels and option names are anonymous enums
            .allowlist_var("RDMA_OPTION_.*");
    }

    let bindings = builder.generate().expect("Unable to generate bindings");

    // write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Could not write bindings");
}
