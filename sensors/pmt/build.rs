use std::path::Path;

use cmake::Config;

fn main() {
    let project_path = Path::new("pmt").canonicalize().unwrap();
    let pmt = Config::new("pmt")
        .out_dir(&project_path)
        .define("PMT_BUILD_RAPL", "ON")
        .build();

    println!("cargo:rerun-if-changed=pmt/CMakeLists.txt");

    cxx_build::bridge("src/lib.rs")
        .include("pmt/include")
        .file("src/wrapper/wrapper.cc")
        .flag_if_supported("-std=c++17")
        .compile("pmt-rs");

    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=src/wrapper/wrapper.hpp");
    println!("cargo:rerun-if-changed=src/wrapper/wrapper.cc");

    println!("cargo:rustc-link-search={}", pmt.join("lib").display());
    println!("cargo:rustc-link-lib=dylib=pmt");
}
