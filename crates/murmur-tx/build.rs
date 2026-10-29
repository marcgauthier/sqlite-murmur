//! Link the same vendored zstd archive as storage; never use system zstd.
fn main() {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let build =
        std::env::var("MURMUR_BUILD_DIR").unwrap_or_else(|_| format!("{manifest}/../../build-a1"));
    println!("cargo:rustc-link-search=native={build}/third-party/zstd/lib");
    println!("cargo:rustc-link-lib=static=zstd");
    println!("cargo:rerun-if-env-changed=MURMUR_BUILD_DIR");
    println!("cargo:rerun-if-changed={build}/third-party/zstd/lib/libzstd.a");
}
