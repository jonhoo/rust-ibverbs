use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
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
        assert!(
            Path::new("vendor/rdma-core").is_dir(),
            "vendor source not included"
        );
    }

    // build vendor/rdma-core
    eprintln!("run cmake");
    let built_in = cmake::Config::new("vendor/rdma-core")
        .define("IN_PLACE", "1")
        .define("NO_MAN_PAGES", "1")
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
        .allowlist_type("ibv_.*")
        .bitfield_enum("ibv_access_flags")
        .bitfield_enum("ibv_qp_attr_mask")
        .bitfield_enum("ibv_wc_flags")
        .bitfield_enum("ibv_send_flags")
        .bitfield_enum("ibv_port_cap_flags")
        .constified_enum_module("ibv_qp_type")
        .constified_enum_module("ibv_qp_state")
        .constified_enum_module("ibv_port_state")
        .constified_enum_module("ibv_wc_opcode")
        .constified_enum_module("ibv_wr_opcode")
        .constified_enum_module("ibv_wc_status")
        //.constified_enum_module("IBV_WC_.*")
        //.constified_enum_module("IBV_WR_.*")
        //.constified_enum_module("IBV_QPS_.*")
        //.constified_enum_module("IBV_PORT_.*")
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
