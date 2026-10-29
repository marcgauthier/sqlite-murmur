//! Test-only native linkage for `murmur-storage`.
//!
//! The CMake staticlib never links: its `rocksdb_*` / `ZSTD_*` /
//! `aegis256_*` / `sqlite3_*` references resolve from the vendored archives
//! at final link, exactly like the former C++ objects. Emitting
//! `rustc-link-lib` here is inert for `--crate-type staticlib` output, so
//! these directives only take effect for `cargo test` builds, which link
//! real binaries. Prerequisites: a configured CMake build tree (default
//! `../../build-a1`, override with `MURMUR_BUILD_DIR`) plus the system
//! SQLite in `/tmp/sqlitelib` (see the `/tmp/sqlitelib` workaround: the
//! host lacks `libsqlite3-dev`, so tests link a symlinked `libsqlite3.so`).

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
    println!("cargo:rustc-link-lib=static=aegis");
    println!("cargo:rustc-link-lib=sqlite3");
    println!("cargo:rustc-link-lib=stdc++");
    println!("cargo:rustc-link-lib=dl");
    println!("cargo:rustc-link-lib=m");
    println!("cargo:rerun-if-env-changed=MURMUR_BUILD_DIR");
}
