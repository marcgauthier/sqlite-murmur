//! Static-variant smoke test: loads the vendored-SQLite `.so` and runs a
//! basic capture round-trip. Ignored by default — `dist.sh` runs it
//! explicitly against the just-built static artifact:
//!
//! ```sh
//! MURMUR_EXT_STATIC_CDYLIB=dist/…-static.so \
//!   cargo test --release -p murmur-sqlite-ext --test static_smoke -- --ignored
//! ```
//!
//! The static build's session code runs on host-owned handles, so the test
//! refuses to run when the host SQLite version differs from the vendored
//! one (3.45.1): a mismatch needs its own build, not a skipped test.

#[path = "common.rs"]
#[allow(dead_code)]
mod common;

use common::*;
use host_shim::Conn;

const VENDORED_VERSION: &str = "3.45.1";

fn static_cdylib() -> String {
    std::env::var("MURMUR_EXT_STATIC_CDYLIB").unwrap_or_else(|_| {
        panic!(
            "MURMUR_EXT_STATIC_CDYLIB is not set; build with \
             `cargo build --release -p murmur-sqlite-ext --features static-sqlite` \
             (or run ./dist.sh)"
        )
    })
}

#[test]
#[ignore]
fn static_variant_smoke() {
    let fx = setup();
    let conn = Conn::open(&fx.db).unwrap();
    let host_version = conn.query_text("SELECT sqlite_version()").unwrap();
    assert_eq!(
        host_version, VENDORED_VERSION,
        "host SQLite {host_version} != vendored {VENDORED_VERSION}: \
         rebuild the static variant for this host (see vendor/README.md)"
    );
    conn.load_extension(&static_cdylib(), None).unwrap();
    conn.exec(&format!("SELECT murmur_open('{}')", fx.spool))
        .unwrap();
    assert_eq!(
        conn.query_text("PRAGMA journal_mode").unwrap().as_str(),
        "wal"
    );
    conn.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    assert!(conn
        .query_text("SELECT murmur_enable_sync('t')")
        .unwrap()
        .starts_with("ext-"));
    conn.exec("BEGIN").unwrap();
    conn.exec("INSERT INTO t VALUES (1, 'static-hello')")
        .unwrap();
    assert!(conn
        .query_int_nullable("SELECT murmur_collect()")
        .unwrap()
        .is_some());
    conn.exec("COMMIT").unwrap();
    assert_eq!(conn.query_int("SELECT murmur_drain()").unwrap(), 1);
    let batch = conn
        .query_text("SELECT hex(murmur_export())")
        .unwrap();
    assert!(!batch.is_empty());
    conn.exec("SELECT murmur_close()").unwrap();
}
