use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    // Check if we should use system headers (for Bazel or when IBVERBS_USE_SYSTEM_HEADERS is set)
    let use_system_headers = env::var("IBVERBS_USE_SYSTEM_HEADERS").is_ok()
        || env::var("BAZEL").is_ok()
        || !Path::new("vendor/rdma-core/CMakeLists.txt").exists();

    if use_system_headers {
        build_with_system_headers();
    } else {
        build_with_vendored_rdma_core();
    }
}

/// Build using system-installed libibverbs headers.
/// This is simpler and works in sandboxed environments like Bazel.
fn build_with_system_headers() {
    eprintln!("Using system headers for ibverbs bindings");
    
    // Link against system libibverbs
    println!("cargo:rustc-link-lib=ibverbs");
    
    // Try common header locations
    let header_paths = [
        "/usr/include/infiniband/verbs.h",
        "/usr/local/include/infiniband/verbs.h",
    ];
    
    let header = header_paths
        .iter()
        .find(|p| Path::new(p).exists())
        .expect("Could not find infiniband/verbs.h. Install libibverbs-dev or set IBVERBS_USE_SYSTEM_HEADERS=0");
    
    eprintln!("Using header: {}", header);
    
    // Generate bindings
    let bindings = bindgen::Builder::default()
        .header(*header)
        .allowlist_function("ibv_.*")
        .allowlist_function("_ibv_.*")
        .allowlist_type("ibv_.*")
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
        .size_t_is_usize(true)
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Could not write bindings");
}

/// Build using vendored rdma-core (original behavior).
/// This builds rdma-core from source to get the headers.
fn build_with_vendored_rdma_core() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("failed to get current directory");
    println!("cargo:include={manifest_dir}/vendor/rdma-core/build/include");
    println!("cargo:rustc-link-search=native={manifest_dir}/vendor/rdma-core/build/lib");
    println!("cargo:rustc-link-lib=ibverbs");

    if Path::new("vendor/rdma-core/CMakeLists.txt").exists() {
        // don't touch source dir if not necessary
    } else if Path::new(".git").is_dir() {
        // initialize and update submodules
        Command::new("git")
            .args(["submodule", "update", "--init"])
            .status()
            .expect("Failed to update submodules.");
    } else {
        panic!("vendor source not included");
    }

    // build vendor/rdma-core
    // note that we only build it to generate the bindings!
    eprintln!("run cmake");
    let built_in = cmake::Config::new("vendor/rdma-core")
        .define("NO_MAN_PAGES", "1")
        .define("CMAKE_INSTALL_PREFIX", "/usr")
        .no_build_target(true)
        .build();
    let built_in = built_in
        .to_str()
        .expect("build directory path is not valid UTF-8");

    // generate the bindings
    eprintln!("run bindgen");
    let bindings = bindgen::Builder::default()
        .header("vendor/rdma-core/libibverbs/verbs.h")
        .clang_arg(format!("-I{built_in}/include/"))
        .allowlist_function("ibv_.*")
        .allowlist_function("_ibv_.*")
        .allowlist_type("ibv_.*")
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
        .size_t_is_usize(true)
        .generate()
        .expect("Unable to generate bindings");

    // write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Could not write bindings");
}
