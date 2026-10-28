//! Build script for the loadable extension.
//!
//! Dynamic (default): record a `DT_NEEDED` on the system `libsqlite3` so
//! the manually declared session symbols in `session_sys` resolve even in
//! hosts that keep `libsqlite3` out of the global namespace (e.g. CPython
//! loads its `_sqlite3` module `RTLD_LOCAL`). The link is by SONAME
//! (`libsqlite3.so.0`), so at runtime it is the same library instance the
//! host uses — never a second SQLite.
//!
//! Static (`--features static-sqlite`): compile the vendored amalgamation
//! (`vendor/sqlite-amalgamation-*/sqlite3.c`) with session support and link
//! it privately (hidden visibility, no `DT_NEEDED` on any `libsqlite3`).
//! The session calls then run our own SQLite code on host-owned handles,
//! so the vendored version must match the host's SQLite version.
//!
//! Static (`--no-default-features`, for hosts that link the extension archive
//! directly): emit no SQLite link at all. Plain `sqlite3_*` calls and the
//! manual session declarations stay undefined in the archive and resolve
//! against the host's own static SQLite at the final link.
//!
//! Both cdylib variants export the built path as `MURMUR_EXT_CDYLIB` for
//! integration tests.

use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    #[cfg(feature = "static-sqlite")]
    link_static_sqlite();
    #[cfg(all(not(feature = "static-sqlite"), feature = "loadable"))]
    link_system_sqlite();

    // target/<profile>/libmurmur_sqlite_ext.<so|dylib|dll> for tests.
    // OUT_DIR = <target>/<profile>/build/<pkg>-<hash>/out
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR set by cargo"));
    let profile_dir = out_dir
        .ancestors()
        .nth(3)
        .expect("OUT_DIR has <target>/<profile> parents")
        .to_path_buf();
    let cdylib = profile_dir.join(format!(
        "{}murmur_sqlite_ext{}",
        env::consts::DLL_PREFIX,
        env::consts::DLL_SUFFIX
    ));
    println!("cargo:rustc-env=MURMUR_EXT_CDYLIB={}", cdylib.display());
}

#[cfg(all(not(feature = "static-sqlite"), feature = "loadable"))]
fn link_system_sqlite() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR set by cargo"));
    // The linker needs a `libsqlite3.so` link name; distros ship only the
    // versioned `libsqlite3.so.0` without the -dev package, so symlink it
    // into OUT_DIR (no sudo, no repo pollution).
    let system_lib = find_system_sqlite().expect("system libsqlite3.so.0 not found");
    let link_name = out_dir.join("libsqlite3.so");
    if !link_name.exists() {
        #[cfg(unix)]
        std::os::unix::fs::symlink(&system_lib, &link_name).expect("symlink libsqlite3");
    }
    println!("cargo:rustc-link-search=native={}", out_dir.display());
    println!("cargo:rustc-link-lib=dylib=sqlite3");
}

#[cfg(feature = "static-sqlite")]
fn link_static_sqlite() {
    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("manifest dir"));
    let version =
        env::var("MURMUR_SQLITE_AMALGAMATION").unwrap_or_else(|_| "3450100".to_string());
    let amalgam = manifest.join(format!("../../vendor/sqlite-amalgamation-{version}"));
    assert!(
        amalgam.join("sqlite3.c").exists(),
        "vendored amalgamation missing (see vendor/README.md)"
    );
    println!(
        "cargo:rerun-if-changed={}",
        amalgam.join("sqlite3.c").display()
    );
    // Hidden visibility keeps our SQLite private: no dynamic export, no
    // interposition with the host's symbols. Our plain `extern "C"` session
    // declarations resolve against this archive at link time.
    cc::Build::new()
        .file(amalgam.join("sqlite3.c"))
        .include(&amalgam)
        .define("SQLITE_ENABLE_SESSION", None)
        .define("SQLITE_ENABLE_PREUPDATE_HOOK", None)
        .flag_if_supported("-fvisibility=hidden")
        .opt_level(2)
        .compile("murmur_sqlite3");
}

#[cfg(all(not(feature = "static-sqlite"), feature = "loadable"))]
fn find_system_sqlite() -> Option<PathBuf> {
    // ldconfig output first, then well-known paths.
    if let Ok(out) = std::process::Command::new("ldconfig").arg("-p").output() {
        let text = String::from_utf8_lossy(&out.stdout);
        for line in text.lines() {
            if line.contains("libsqlite3.so.0") {
                if let Some(path) = line.split("=>").nth(1) {
                    let p = PathBuf::from(path.trim());
                    if p.exists() {
                        return Some(p);
                    }
                }
            }
        }
    }
    [
        "/lib/x86_64-linux-gnu/libsqlite3.so.0",
        "/usr/lib/x86_64-linux-gnu/libsqlite3.so.0",
        "/usr/lib/libsqlite3.so.0",
        "/lib64/libsqlite3.so.0",
    ]
    .into_iter()
    .map(PathBuf::from)
    .find(|p| p.exists())
}
