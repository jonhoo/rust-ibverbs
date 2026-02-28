use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn build_vendored_rdma() -> String {
    eprintln!("run cmake");
    let built_in = cmake::Config::new("vendor/rdma-core")
        .define("NO_MAN_PAGES", "1")
        // cmake crate defaults CMAKE_INSTALL_PREFIX to the output directory
        //
        //   https://github.com/rust-lang/cmake-rs/blob/94da9de2ea79ab6cad572e908864a160cf4847a9/src/lib.rs#L699-L703
        //
        // this results in overly long runtime paths on docs.rs, which then fail the build. it also
        // causes sadness for users trying to build since the bindings may fail to build for the
        // same reason (see https://github.com/jonhoo/rust-ibverbs/pull/41 for what was an
        // incomplete fix).
        //
        // since we never actually _install_ anything when building here, we should be able to
        // safely set this to any short path. simply by convention we set it to `/usr`.
        .define("CMAKE_INSTALL_PREFIX", "/usr")
        .no_build_target(true)
        .build();
    let built_in = built_in
        .into_os_string()
        .into_string()
        .expect("build directory path is not valid UTF-8");

    built_in
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

    let rdma_core_include_dir = if let Ok(rdma_core_include_dir) = env::var("RDMA_CORE_INCLUDE_DIR")
    {
        let rdma_core_lib_dir = env::var("RDMA_CORE_LIB_DIR").expect(
            "When supplying RDMA_CORE_INCLUDE_DIR, you also need to supply RDMA_CORE_LIB_DIR",
        );
        println!("cargo:include={rdma_core_include_dir}");
        println!("cargo:rustc-link-search=native={rdma_core_lib_dir}");
        rdma_core_include_dir
    } else {
        // build vendor/rdma-core
        // note that we only build it to generate the bindings!
        update_submodule();
        let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("failed to get current directory");
        println!("cargo:include={manifest_dir}/vendor/rdma-core/build/include");
        println!("cargo:rustc-link-search=native={manifest_dir}/vendor/rdma-core/build/lib");
        format!("{}/include/", build_vendored_rdma())
    };

    let ibverbs_header_dir = if let Ok(ibverbs_header_dir) = env::var("IBVERBS_HEADER_DIR") {
        ibverbs_header_dir
    } else {
        update_submodule();
        "vendor/rdma-core/libibverbs".to_string()
    };

    // generate the bindings
    eprintln!("run bindgen");
    let bindings = bindgen::Builder::default()
        .header(format!("{ibverbs_header_dir}/verbs.h"))
        .clang_arg(format!("-I{rdma_core_include_dir}"))
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
