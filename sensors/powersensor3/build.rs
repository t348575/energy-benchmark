fn main() {
    cxx_build::bridge("src/lib.rs")
        .file("src/wrapper/wrapper.cc")
        .flag_if_supported("-std=c++17")
        .compile("powersensor3");

    println!("cargo:rerun-if-changed=src/wrapper/wrapper.cc");
    println!("cargo:rerun-if-changed=src/wrapper/wrapper.hpp");
    println!("cargo:rerun-if-changed=src/lib.rs");

    println!("cargo:rustc-link-search=/usr/local/lib");
    println!("cargo:rustc-link-lib=static=PowerSensor");
    println!("cargo:rustc-link-lib=dylib=gomp");
}
