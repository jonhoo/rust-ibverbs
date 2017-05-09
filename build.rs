extern crate bindgen;

use std::env;
use std::process::Command;
use std::path::PathBuf;

fn main() {
    println!("cargo:include=vendor/rdma-core/build/include");
    println!("cargo:rustc-link-search=native=vendor/rdma-core/build/lib");
    println!("cargo:rustc-link-lib=ibverbs");

    // build vendor/rdma-core
    Command::new("sh")
        .current_dir("vendor/rdma-core/")
        .args(&["build.sh"])
        .status()
        .expect("Failed to build vendor/rdma-core using build.sh");

    // generate the bindings
    let bindings = bindgen::Builder::default()
        .no_unstable_rust()
        .header("wrapper.h")
        .clang_arg("-Ivendor/rdma-core/build/include/")
        // https://github.com/servo/rust-bindgen/issues/550
        .hide_type("max_align_t")
        .whitelisted_function("ibv_.*")
        .generate()
        .expect("Unable to generate bindings");

    // write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Could not write bindings");
}
