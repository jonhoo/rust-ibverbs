extern crate bindgen;

use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    println!("cargo:include=vendor/rdma-core/build/include");
    println!("cargo:rustc-link-search=native=vendor/rdma-core/build/lib");
    println!("cargo:rustc-link-lib=ibverbs");

    // build vendor/rdma-core
    Command::new("bash")
        .current_dir("vendor/rdma-core/")
        .args(&["build.sh"])
        .status()
        .expect("Failed to build vendor/rdma-core using build.sh");

    // generate the bindings
    let bindings = bindgen::Builder::default()
        .header("wrapper.h")
        .clang_arg("-Ivendor/rdma-core/build/include/")
        // https://github.com/servo/rust-bindgen/issues/550
        .blacklist_type("max_align_t")
        .whitelist_function("ibv_.*")
        .whitelist_type("ibv_.*")
        .bitfield_enum("ibv_access_flags")
        .bitfield_enum("ibv_qp_attr_mask")
        .bitfield_enum("ibv_wc_flags")
        .bitfield_enum("ibv_send_flags")
        .prepend_enum_name(false)
        .blacklist_type("ibv_wc")
        .generate()
        .expect("Unable to generate bindings");

    // write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Could not write bindings");
}
