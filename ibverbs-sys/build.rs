use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

/// The quoted names in `no_default.bzl`, whose only Starlark is a list of string literals.
fn no_default_types(bzl: &str) -> impl Iterator<Item = &str> {
    bzl.lines()
        .map(str::trim)
        .filter_map(|line| line.strip_prefix('"'))
        .map(|line| line.trim_end_matches(',').trim_end_matches('"'))
}

/// Configure the vendored rdma-core checkout with cmake and return the include directory it
/// generates.
///
/// rdma-core publishes its public headers into the build tree at cmake *configure* time
/// (`buildlib/publish_headers.cmake`), so configuring is all it takes to generate bindings from
/// them; nothing is compiled, and the crate links the system `libibverbs` regardless.
fn configure_vendored_rdma() -> PathBuf {
    let build_dir =
        PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR is set by cargo")).join("rdma-core");
    let cmake = env::var("CMAKE").unwrap_or_else(|_| "cmake".to_string());
    eprintln!("run cmake (configure only)");
    let output = Command::new(&cmake)
        .arg("-S")
        .arg("vendor/rdma-core")
        .arg("-B")
        .arg(&build_dir)
        .arg("-DNO_MAN_PAGES=1")
        .arg("-DNO_PYVERBS=1")
        // never installed, but the configure step bakes the prefix into its output, and a long
        // OUT_DIR-derived one used to overflow path limits on docs.rs (see #41)
        .arg("-DCMAKE_INSTALL_PREFIX=/usr")
        .output()
        .unwrap_or_else(|e| {
            panic!(
                "failed to run `{cmake}` to configure vendor/rdma-core (is cmake installed?): {e}"
            )
        });
    if !output.status.success() {
        eprintln!("{}", String::from_utf8_lossy(&output.stdout));
        eprintln!("{}", String::from_utf8_lossy(&output.stderr));
        panic!(
            "cmake failed to configure vendor/rdma-core ({})",
            output.status
        );
    }
    build_dir.join("include")
}

fn update_submodule() {
    if Path::new("vendor/rdma-core/CMakeLists.txt").exists() {
        // don't touch source dir if not necessary
    } else if Path::new(".git").is_dir() {
        // initialize and update submodules
        Command::new("git")
            .args(["submodule", "update", "--init"])
            .status()
            .expect("Failed to update submodules.");
    } else {
        assert!(
            Path::new("vendor/rdma-core").is_dir(),
            "vendor source not included"
        );
    }
}

fn main() {
    println!("cargo:rustc-link-lib=ibverbs");

    let efa = env::var("CARGO_FEATURE_EFA").is_ok();
    if efa {
        // `efadv_create_qp_ex` and friends are exported from libefa.
        println!("cargo:rustc-link-lib=efa");
    }

    let rdmacm = env::var("CARGO_FEATURE_RDMACM").is_ok();
    if rdmacm {
        // The `rdma_*` connection-manager functions are exported from librdmacm.
        println!("cargo:rustc-link-lib=rdmacm");
    }

    // Where `verbs.h`'s own includes (`<infiniband/verbs_api.h>` and friends) come from: either
    // pre-generated rdma-core headers named by the caller, or the ones cmake generates from the
    // vendored checkout.
    let rdma_core_include_dir = if let Ok(rdma_core_include_dir) = env::var("RDMA_CORE_INCLUDE_DIR")
    {
        let rdma_core_lib_dir = env::var("RDMA_CORE_LIB_DIR").expect(
            "When supplying RDMA_CORE_INCLUDE_DIR, you also need to supply RDMA_CORE_LIB_DIR",
        );
        println!("cargo:rustc-link-search=native={rdma_core_lib_dir}");
        PathBuf::from(rdma_core_include_dir)
    } else {
        update_submodule();
        configure_vendored_rdma()
    };
    // exported to dependents as `DEP_IBVERBS_INCLUDE`
    println!("cargo:include={}", rdma_core_include_dir.display());

    let ibverbs_header_dir = if let Ok(ibverbs_header_dir) = env::var("IBVERBS_HEADER_DIR") {
        ibverbs_header_dir
    } else {
        update_submodule();
        "vendor/rdma-core/libibverbs".to_string()
    };

    // generate the bindings
    eprintln!("run bindgen");
    let mut builder = bindgen::Builder::default()
        .header(format!("{ibverbs_header_dir}/verbs.h"))
        // the crate's edition and MSRV; `wrap_unsafe_ops` keeps the generated `unsafe fn` bodies
        // clean under 2024's `unsafe_op_in_unsafe_fn`
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
        .default_enum_style(bindgen::EnumVariation::Rust {
            non_exhaustive: false,
        })
        .derive_default(true)
        .derive_debug(true)
        .prepend_enum_name(false)
        .blocklist_type("ibv_wc")
        .size_t_is_usize(true);

    if efa {
        // EFA SRD queue pairs are created through the provider's direct-verbs (`efadv`), reached
        // via `<infiniband/efadv.h>` on the same include path as `verbs.h`.
        builder = builder
            .header_contents("efadv_wrapper.h", "#include <infiniband/efadv.h>")
            .allowlist_function("efadv_.*")
            .allowlist_type("efadv_.*")
            .allowlist_var("EFADV_QP_DRIVER_TYPE_.*");
    }

    if rdmacm {
        // The RDMA connection manager lives in `<rdma/rdma_cma.h>`, which itself includes `verbs.h`
        // (guarded, so the `ibv_*` types are still emitted only once) on the same include path.
        builder = builder
            .header_contents("rdmacm_wrapper.h", "#include <rdma/rdma_cma.h>")
            .allowlist_function("rdma_.*")
            .allowlist_type("rdma_.*")
            // the `rdma_set_option` levels and option names are anonymous enums
            .allowlist_var("RDMA_OPTION_.*");
    }

    // The types whose zero-filled fallback `Default` would be an invalid value; the list is shared
    // with the Bazel build, which loads the same file. It lives in the package directory, so
    // Cargo's default change tracking covers it (a `rerun-if-changed` here would replace that
    // tracking and drop the headers from it).
    let no_default = std::fs::read_to_string("no_default.bzl").expect("read no_default.bzl");
    for name in no_default_types(&no_default) {
        builder = builder.no_default(name);
    }

    let bindings = builder.generate().expect("Unable to generate bindings");

    // write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Could not write bindings");
}
