//! Link `libsqlite3` for foreign-host binaries without requiring the -dev
//! package: symlink the versioned system library into OUT_DIR so `-lsqlite3`
//! resolves. The `cargo:rustc-link-search` propagates to dependents.
use std::path::PathBuf;

fn main() {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR set by cargo"));
    let system_lib = find_system_sqlite().expect("system libsqlite3.so.0 not found");
    let link_name = out_dir.join("libsqlite3.so");
    if !link_name.exists() {
        #[cfg(unix)]
        std::os::unix::fs::symlink(&system_lib, &link_name).expect("symlink libsqlite3");
    }
    println!("cargo:rustc-link-search=native={}", out_dir.display());
    println!("cargo:rerun-if-changed=build.rs");
}

fn find_system_sqlite() -> Option<PathBuf> {
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
