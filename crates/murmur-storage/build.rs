//! Native linkage for `murmur-storage` and its Cargo tests.
//!
//! CMake builds the vendored native archives before invoking Cargo and passes
//! its build tree through `MURMUR_BUILD_DIR` (default `../../build-a1`). Rust
//! staticlib output bundles these static native archives; CMake also supplies
//! native libraries needed by other objects at the final application link.
//! Cargo tests link SQLite directly, requiring the host SQLite development
//! library or the local `/tmp/sqlitelib/libsqlite3.so` workaround.

fn main() {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let build = std::env::var("MURMUR_BUILD_DIR")
        .unwrap_or_else(|_| format!("{manifest}/../../build-a1"));
    for dir in [
        format!("{build}/third-party/rocksdb"),
        format!("{build}/third-party/zstd/lib"),
        format!("{build}/third-party/aegis"),
        "/tmp/sqlitelib".to_string(),
    ] {
        println!("cargo:rustc-link-search=native={dir}");
    }
    // Order matters: dependents first.
    println!("cargo:rustc-link-lib=static=rocksdb");
    println!("cargo:rustc-link-lib=static=zstd");
    if std::env::var_os("CARGO_FEATURE_AEGIS256").is_some() {
        println!("cargo:rustc-link-lib=static=aegis");
    }
    println!("cargo:rustc-link-lib=sqlite3");
    println!("cargo:rustc-link-lib=stdc++");
    println!("cargo:rustc-link-lib=dl");
    println!("cargo:rustc-link-lib=m");
    println!("cargo:rerun-if-env-changed=MURMUR_BUILD_DIR");
    println!("cargo:rerun-if-changed={build}/third-party/zstd/lib/libzstd.a");
}
