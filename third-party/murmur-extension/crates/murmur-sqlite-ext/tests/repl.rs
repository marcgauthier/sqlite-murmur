//! Phase 4 replication suite: live QUIC sync and offline catch-up between
//! two extension nodes in one process (distinct databases, spools, ports).
//!
//! Node identities are spool-minted and random, so each test reads them via
//! `murmur_clock`/`murmur_repl_pubkey` and mints matching TLS certificates
//! on the fly (same rcgen profile as the engine's transfer tests).

#[path = "common.rs"]
#[allow(dead_code)]
mod common;

use common::*;
use host_shim::Conn;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, SanType,
};
use std::time::{Duration, Instant};

const DDL: &str = "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)";
const INTERVALS: &str = r#"{"anti_entropy_ms":500,"incoming_ms":250}"#;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn link(a: &Conn, b: &Conn) {
    let id = a.query_text("SELECT murmur_database_id()").unwrap();
    b.exec(&format!("SELECT murmur_set_database_id('{id}')")).unwrap();
}

fn dump(conn: &Conn) -> Vec<Vec<String>> {
    conn.query_dump("SELECT * FROM t ORDER BY id").unwrap()
}

fn write_txn(conn: &Conn, sql: &[&str]) {
    conn.exec("BEGIN").unwrap();
    for s in sql {
        conn.exec(s).unwrap();
    }
    let seq = conn.query_int_nullable("SELECT murmur_collect()").unwrap();
    assert!(seq.is_some(), "expected changes to collect");
    conn.exec("COMMIT").unwrap();
    assert_eq!(conn.query_int("SELECT murmur_drain()").unwrap(), 1);
}

fn node_hex(conn: &Conn) -> String {
    let id = conn.query_text("SELECT murmur_node_id()").unwrap();
    assert_eq!(id.len(), 32);
    assert_ne!(id, "00".repeat(16));
    id
}

fn pubkey(conn: &Conn) -> String {
    let key = conn.query_text("SELECT murmur_repl_pubkey()").unwrap();
    assert_eq!(key.len(), 64);
    key
}

struct Pki {
    ca: rcgen::Certificate,
    ca_key: KeyPair,
}

impl Pki {
    fn new() -> Self {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca_key = KeyPair::generate().unwrap();
        let ca = params.self_signed(&ca_key).unwrap();
        Self { ca, ca_key }
    }

    fn leaf(&self, node: &str) -> (Vec<u8>, Vec<u8>) {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.subject_alt_names = vec![SanType::URI(
            format!("replicateddb://node/{node}").try_into().unwrap(),
        )];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let cert = params.signed_by(&key, &self.ca, &self.ca_key).unwrap();
        (cert.der().to_vec(), key.serialize_der().to_vec())
    }
}

fn start_node(conn: &Conn, pki: &Pki, peers_json: &str) -> String {
    let (cert, key) = pki.leaf(&node_hex(conn));
    let creds = format!(
        r#"{{"chain":["{}"],"key":"{}","roots":["{}"]}}"#,
        hex(&cert),
        hex(&key),
        hex(pki.ca.der())
    );
    conn.query_text(&format!(
        "SELECT murmur_repl_start('127.0.0.1:0', '{creds}', '{peers_json}', '{INTERVALS}')"
    ))
    .unwrap()
}

fn peers_json(node: &str, pubkey: &str) -> String {
    format!(r#"{{"{node}":"{pubkey}"}}"#)
}

fn peer(a: &Conn, addr_b: &str) {
    a.exec(&format!("SELECT murmur_repl_add_peer('{addr_b}')"))
        .unwrap();
}

fn status(conn: &Conn) -> String {
    conn.query_text("SELECT murmur_repl_status()").unwrap()
}

fn stop(conn: &Conn) {
    assert_eq!(conn.query_int("SELECT murmur_repl_stop()").unwrap(), 0);
}

fn wait_for(desc: &str, timeout: Duration, mut cond: impl FnMut() -> bool) {
    let end = Instant::now() + timeout;
    while !cond() {
        assert!(
            Instant::now() < end,
            "timed out waiting for {desc}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn log_count(fx: &Fixture) -> usize {
    // Caller holds no open connection (Spool file lock).
    let store = open_spool(fx);
    let n = store.keys_with_prefix(b"07/replication/log/").len();
    store.close().unwrap();
    n
}

#[test]
fn live_sync_bidirectional() {
    let fa = setup();
    let fb = setup();
    let a = open_conn(&fa);
    let b = open_conn(&fb);
    link(&a, &b);
    for conn in [&a, &b] {
        conn.exec(DDL).unwrap();
        conn.exec("SELECT murmur_enable_sync('t')").unwrap();
    }
    // Written before transport starts: log-from-birth must carry it.
    write_txn(&a, &["INSERT INTO t VALUES (1, 'pre-start')"]);
    let pki = Pki::new();
    let addr_a = start_node(&a, &pki, &peers_json(&node_hex(&b), &pubkey(&b)));
    let addr_b = start_node(&b, &pki, &peers_json(&node_hex(&a), &pubkey(&a)));
    assert!(addr_a.starts_with("127.0.0.1:"), "{addr_a}");
    peer(&a, &addr_b);
    peer(&b, &addr_a);
    wait_for("b gets pre-start row", Duration::from_secs(30), || {
        dump(&b).len() == 1
    });
    assert_eq!(dump(&b), vec![vec!["1".to_string(), "pre-start".to_string()]]);
    write_txn(&b, &["INSERT INTO t VALUES (2, 'live-b')"]);
    wait_for("a gets live row", Duration::from_secs(30), || {
        dump(&a).len() == 2
    });
    write_txn(&a, &["INSERT INTO t VALUES (3, 'live-a')"]);
    wait_for("converge", Duration::from_secs(30), || {
        dump(&b).len() == 3
    });
    assert_eq!(dump(&a), dump(&b));
    assert_eq!(dump(&a).len(), 3);
    let sa = status(&a);
    assert!(sa.contains("\"peers\":1"), "{sa}");
    assert!(!sa.contains("\"last_error\":\""), "{sa}");
    for conn in [&a, &b] {
        stop(conn);
        conn.exec("SELECT murmur_close()").unwrap();
    }
    drop(a);
    drop(b);
    // Own batches plus relayed peer batches, all in the log.
    assert!(log_count(&fa) >= 2);
    assert!(log_count(&fb) >= 2);
}

#[test]
fn offline_peer_catches_up() {
    let fa = setup();
    let fb = setup();
    let a = open_conn(&fa);
    let b = open_conn(&fb);
    link(&a, &b);
    for conn in [&a, &b] {
        conn.exec(DDL).unwrap();
        conn.exec("SELECT murmur_enable_sync('t')").unwrap();
    }
    let pki = Pki::new();
    let addr_a = start_node(&a, &pki, &peers_json(&node_hex(&b), &pubkey(&b)));
    let addr_b = start_node(&b, &pki, &peers_json(&node_hex(&a), &pubkey(&a)));
    peer(&a, &addr_b);
    peer(&b, &addr_a);
    write_txn(&a, &["INSERT INTO t VALUES (1, 'one')"]);
    wait_for("live baseline", Duration::from_secs(30), || {
        dump(&b).len() == 1
    });
    // B goes deaf; A keeps writing.
    stop(&b);
    write_txn(&a, &["INSERT INTO t VALUES (2, 'two')"]);
    write_txn(&a, &["INSERT INTO t VALUES (3, 'three')"]);
    write_txn(&a, &["INSERT INTO t VALUES (4, 'four')"]);
    // B returns on a fresh port and pulls exactly the missed log range.
    let addr_b2 = start_node(&b, &pki, &peers_json(&node_hex(&a), &pubkey(&a)));
    peer(&a, &addr_b2);
    peer(&b, &addr_a);
    wait_for("b catches up", Duration::from_secs(60), || {
        dump(&b).len() == 4
    });
    assert_eq!(dump(&a), dump(&b));
    for conn in [&a, &b] {
        stop(conn);
        conn.exec("SELECT murmur_close()").unwrap();
    }
    drop(a);
    drop(b);
    // Exactly four origin transactions each way through the log (no
    // snapshots involved): A's own four, B's relayed four.
    assert_eq!(log_count(&fa), 4);
    assert_eq!(log_count(&fb), 4);
}

#[test]
fn large_batch_chunks_across_sequences() {
    let fa = setup();
    let fb = setup();
    let a = open_conn(&fa);
    let b = open_conn(&fb);
    link(&a, &b);
    for conn in [&a, &b] {
        conn.exec(DDL).unwrap();
        conn.exec("SELECT murmur_enable_sync('t')").unwrap();
    }
    // One transaction, ~4.8MB of values: a single log payload cannot hold
    // it, so the drain must split it across consecutive sequences.
    let big = "v".repeat(1500);
    let mut sql = String::from("INSERT INTO t VALUES ");
    for id in 1..=3000 {
        if id > 1 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("({id}, '{big}')"));
    }
    write_txn(&a, &[&sql]);
    let pki = Pki::new();
    let addr_a = start_node(&a, &pki, &peers_json(&node_hex(&b), &pubkey(&b)));
    let addr_b = start_node(&b, &pki, &peers_json(&node_hex(&a), &pubkey(&a)));
    peer(&a, &addr_b);
    peer(&b, &addr_a);
    wait_for("b converges on 3000 rows", Duration::from_secs(90), || {
        dump(&b).len() == 3000
    });
    assert_eq!(dump(&a), dump(&b));
    for conn in [&a, &b] {
        stop(conn);
        conn.exec("SELECT murmur_close()").unwrap();
    }
    drop(a);
    drop(b);
    assert!(
        log_count(&fa) >= 2,
        "expected the batch to split across sequences"
    );
}

fn progress(conn: &Conn) -> serde_json::Value {
    serde_json::from_str(&conn.query_text("SELECT murmur_repl_progress()").unwrap()).unwrap()
}

#[test]
fn repl_progress_tracks_origins() {
    let fa = setup();
    let fb = setup();
    let a = open_conn(&fa);
    let b = open_conn(&fb);
    link(&a, &b);
    for conn in [&a, &b] {
        conn.exec(DDL).unwrap();
        conn.exec("SELECT murmur_enable_sync('t')").unwrap();
    }
    assert!(progress(&a).as_array().unwrap().is_empty());
    write_txn(&a, &["INSERT INTO t VALUES (1, 'x')"]);
    write_txn(&a, &["INSERT INTO t VALUES (2, 'y')"]);
    // Local logging is visible without transport running.
    let pa = progress(&a);
    assert_eq!(pa.as_array().unwrap().len(), 1);
    assert_eq!(pa[0]["origin"].as_str().unwrap(), node_hex(&a));
    assert_eq!(pa[0]["applied"].as_u64().unwrap(), 2);
    assert_eq!(pa[0]["observed"].as_u64().unwrap(), 2);
    assert_eq!(pa[0]["retained_from"].as_u64().unwrap(), 1);
    assert_eq!(pa[0]["retained_through"].as_u64().unwrap(), 2);
    // Relayed origins appear after sync, without B writing anything.
    let pki = Pki::new();
    let addr_a = start_node(&a, &pki, &peers_json(&node_hex(&b), &pubkey(&b)));
    let addr_b = start_node(&b, &pki, &peers_json(&node_hex(&a), &pubkey(&a)));
    peer(&a, &addr_b);
    peer(&b, &addr_a);
    let node_a = node_hex(&a);
    wait_for("b applies a's log", Duration::from_secs(30), || {
        progress(&b).as_array().unwrap().iter().any(|p| {
            p["origin"].as_str() == Some(node_a.as_str()) && p["applied"].as_u64() == Some(2)
        })
    });
    let pb = progress(&b);
    assert_eq!(pb.as_array().unwrap().len(), 1);
    assert_eq!(pb[0]["origin"].as_str().unwrap(), node_a);
    assert_eq!(pb[0]["applied"].as_u64().unwrap(), 2);
    for conn in [&a, &b] {
        stop(conn);
        conn.exec("SELECT murmur_close()").unwrap();
    }
}

#[test]
fn repl_lifecycle() {
    let fx = setup();
    let conn = open_conn(&fx);
    conn.exec(DDL).unwrap();
    conn.exec("SELECT murmur_enable_sync('t')").unwrap();
    for sql in [
        "SELECT murmur_repl_add_peer('127.0.0.1:9')",
        "SELECT murmur_repl_status()",
        "SELECT murmur_repl_sync()",
    ] {
        let err = conn.exec(sql).unwrap_err();
        assert!(err.contains("not running"), "{err}");
    }
    let pki = Pki::new();
    let addr = start_node(&conn, &pki, "{}");
    assert!(addr.starts_with("127.0.0.1:"), "{addr}");
    let err = conn
        .query_text("SELECT murmur_repl_start('127.0.0.1:0', '{}', '{}')")
        .unwrap_err();
    assert!(err.contains("already running"), "{err}");
    let st = status(&conn);
    for key in ["address", "peers", "staged", "applied", "last_error"] {
        assert!(st.contains(&format!("\"{key}\":")) || st.contains(&format!("\"{key}\": ")), "{st}");
    }
    stop(&conn);
    stop(&conn);
    // Replication outlives the last connection: close, reopen, still there.
    start_node(&conn, &pki, "{}");
    conn.exec("SELECT murmur_close()").unwrap();
    drop(conn);
    let conn = open_conn(&fx);
    let st = status(&conn);
    assert!(st.contains("127.0.0.1:"), "{st}");
    stop(&conn);
    conn.exec("SELECT murmur_close()").unwrap();
    drop(conn);
    // Lock released: direct access works again.
    let store = open_spool(&fx);
    store.close().unwrap();
}

#[test]
fn repl_shared_across_connections() {
    let fx = setup();
    let a = open_conn(&fx);
    a.exec(DDL).unwrap();
    a.exec("SELECT murmur_enable_sync('t')").unwrap();
    let pki = Pki::new();
    let addr = start_node(&a, &pki, "{}");
    // A second connection on the same database sees the same node-level
    // handle: status works, starting again is refused.
    let b = Conn::open(&fx.db).unwrap();
    b.load_extension(&cdylib(), None).unwrap();
    b.exec(&format!("SELECT murmur_open('{}')", fx.spool))
        .unwrap();
    assert!(status(&b).contains(&addr));
    let err = b
        .query_text("SELECT murmur_repl_start('127.0.0.1:0', '{}', '{}')")
        .unwrap_err();
    assert!(err.contains("already running"), "{err}");
    // Either connection may stop it; both see it gone.
    stop(&b);
    let err = a
        .query_text("SELECT murmur_repl_status()")
        .unwrap_err();
    assert!(err.contains("not running"), "{err}");
    for conn in [&a, &b] {
        conn.exec("SELECT murmur_close()").unwrap();
    }
}

#[test]
fn database_id_locked_after_logging() {
    let fx = setup();
    let conn = open_conn(&fx);
    conn.exec(DDL).unwrap();
    conn.exec("SELECT murmur_enable_sync('t')").unwrap();
    // Same value is a no-op, even with history.
    let id = conn.query_text("SELECT murmur_database_id()").unwrap();
    conn.exec(&format!("SELECT murmur_set_database_id('{id}')"))
        .unwrap();
    write_txn(&conn, &["INSERT INTO t VALUES (1, 'x')"]);
    let other = "00".repeat(16);
    assert_ne!(other, id);
    let err = conn
        .exec(&format!("SELECT murmur_set_database_id('{other}')"))
        .unwrap_err();
    assert!(err.contains("locked"), "{err}");
    conn.exec("SELECT murmur_close()").unwrap();
}
