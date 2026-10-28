//! Phase 2 sync suite: adoption, capture, drain, and lifecycle, driven
//! through the foreign host against file-backed databases.
//!
//! Tests in this file serialize on `SERIAL`: the extension keeps process-wide
//! session state, so the session-count test needs a quiet process.

#[path = "common.rs"]
#[allow(dead_code)]
mod common;

use common::*;
use host_shim::Conn;
use murmur_crdt::model::{Value, Version};
use std::sync::{Mutex, MutexGuard};

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn collect_seq(conn: &Conn) -> Option<i64> {
    conn.query_int_nullable("SELECT murmur_collect()").unwrap()
}

#[test]
fn enable_collect_drain_roundtrip() {
    let _g = serial();
    let fx = setup();
    let conn = open_conn(&fx);
    conn.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    // Pre-existing rows are adopted by backfill.
    conn.exec("INSERT INTO t VALUES (0, 'old'), (2, 'gone')").unwrap();
    let id = conn.query_text("SELECT murmur_enable_sync('t')").unwrap();
    assert!(id.starts_with("ext-"), "{id}");

    conn.exec("BEGIN").unwrap();
    conn.exec("INSERT INTO t VALUES (1, 'new')").unwrap();
    conn.exec("UPDATE t SET v = 'older' WHERE id = 0").unwrap();
    conn.exec("DELETE FROM t WHERE id = 2").unwrap();
    assert_eq!(collect_seq(&conn), Some(1));
    conn.exec("COMMIT").unwrap();
    assert_eq!(conn.query_int("SELECT murmur_drain()").unwrap(), 1);
    assert_eq!(
        conn.query_int("SELECT COUNT(*) FROM _murmur_outbox").unwrap(),
        0
    );
    conn.exec("SELECT murmur_close()").unwrap();
    drop(conn);

    let store = open_spool(&fx);
    // Inserted row: fresh generation stamped with the batch version.
    let inserted = read_meta(&store, &id, &[int_value(1)]);
    let v = inserted
        .cells
        .iter()
        .find(|c| c.column == format!("{id}/v"))
        .unwrap();
    assert_eq!(v.value, Value::Text("new".into()));
    assert_eq!(inserted.generation, v.version);
    // Updated row: backfilled generation kept, touched cell newer.
    let updated = read_meta(&store, &id, &[int_value(0)]);
    let id_cell = updated
        .cells
        .iter()
        .find(|c| c.column == format!("{id}/id"))
        .unwrap();
    let v_cell = updated
        .cells
        .iter()
        .find(|c| c.column == format!("{id}/v"))
        .unwrap();
    assert_eq!(v_cell.value, Value::Text("older".into()));
    assert_eq!(updated.generation, Version::default());
    assert!(id_cell.version > Version::default());
    assert!(v_cell.version > id_cell.version);
    assert_eq!(v_cell.version, v.version, "one batch, one version");
    // Deleted row: gone from metadata, fenced by a tombstone.
    let key = murmur_crdt::model::encode(&vec![int_value(2)]).unwrap();
    assert!(store
        .get(&murmur_crdt::meta::row_key(&id, &key))
        .unwrap()
        .is_none());
    let tombs = tombstones(&store);
    assert_eq!(tombs.len(), 1);
    assert_eq!(tombs[0].table, id);
    assert_eq!(tombs[0].key, key);
    assert_eq!(tombs[0].generation, updated.generation);
    assert_eq!(tombs[0].version, v.version);
    // Exactly the two live rows carry metadata: the outbox itself was never
    // captured.
    assert_eq!(row_meta_count(&store), 2);
    store.close().unwrap();
}

#[test]
fn without_rowid_text_pk() {
    let _g = serial();
    let fx = setup();
    let conn = open_conn(&fx);
    conn.exec("CREATE TABLE w(k TEXT PRIMARY KEY, v BLOB) WITHOUT ROWID")
        .unwrap();
    let id = conn.query_text("SELECT murmur_enable_sync('w')").unwrap();
    conn.exec("BEGIN").unwrap();
    conn.exec("INSERT INTO w VALUES ('a', x'0102')").unwrap();
    assert_eq!(collect_seq(&conn), Some(1));
    conn.exec("COMMIT").unwrap();
    assert_eq!(conn.query_int("SELECT murmur_drain()").unwrap(), 1);
    conn.exec("SELECT murmur_close()").unwrap();
    drop(conn);

    let store = open_spool(&fx);
    let meta = read_meta(&store, &id, &[Value::Text("a".into())]);
    let v = meta
        .cells
        .iter()
        .find(|c| c.column == format!("{id}/v"))
        .unwrap();
    assert_eq!(v.value, Value::Blob(vec![1, 2]));
    store.close().unwrap();
}

#[test]
fn rejections() {
    let _g = serial();
    let fx = setup();
    let conn = open_conn(&fx);
    conn.exec("CREATE TABLE nopk(a, b)").unwrap();
    conn.exec("CREATE VIRTUAL TABLE ft USING fts5(x)").unwrap();
    let err = conn.query_text("SELECT murmur_enable_sync('nopk')").unwrap_err();
    assert!(err.contains("PRIMARY KEY"), "{err}");
    let err = conn.query_text("SELECT murmur_enable_sync('ft')").unwrap_err();
    assert!(err.contains("virtual"), "{err}");
    let err = conn
        .query_text("SELECT murmur_enable_sync('missing')")
        .unwrap_err();
    assert!(err.contains("not a table"), "{err}");
    let err = conn
        .query_text("SELECT murmur_enable_sync('_murmur_outbox')")
        .unwrap_err();
    assert!(err.contains("internal"), "{err}");
    conn.exec("SELECT murmur_close()").unwrap();
}

#[test]
fn multi_table_batch_shares_version() {
    let _g = serial();
    let fx = setup();
    let conn = open_conn(&fx);
    conn.exec("CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    conn.exec("CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    let ida = conn.query_text("SELECT murmur_enable_sync('a')").unwrap();
    let idb = conn.query_text("SELECT murmur_enable_sync('b')").unwrap();
    assert_ne!(ida, idb);
    conn.exec("BEGIN").unwrap();
    conn.exec("INSERT INTO a VALUES (1, 'x')").unwrap();
    conn.exec("INSERT INTO b VALUES (1, 'y')").unwrap();
    assert_eq!(collect_seq(&conn), Some(1));
    conn.exec("COMMIT").unwrap();
    assert_eq!(conn.query_int("SELECT murmur_drain()").unwrap(), 1);
    conn.exec("SELECT murmur_close()").unwrap();
    drop(conn);

    let store = open_spool(&fx);
    let ma = read_meta(&store, &ida, &[int_value(1)]);
    let mb = read_meta(&store, &idb, &[int_value(1)]);
    let va = ma.cells.iter().find(|c| c.column == format!("{ida}/v")).unwrap();
    let vb = mb.cells.iter().find(|c| c.column == format!("{idb}/v")).unwrap();
    assert_eq!(va.version, vb.version);
    store.close().unwrap();
}

#[test]
fn enable_idempotent_and_reopen_reattaches() {
    let _g = serial();
    let fx = setup();
    let conn = open_conn(&fx);
    conn.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    let first = conn.query_text("SELECT murmur_enable_sync('t')").unwrap();
    let second = conn.query_text("SELECT murmur_enable_sync('t')").unwrap();
    assert_eq!(first, second);
    let tables = conn.query_text("SELECT murmur_sync_tables()").unwrap();
    assert!(tables.contains("\"name\":\"t\""), "{tables}");
    assert!(tables.contains("\"enabled\":true"), "{tables}");
    conn.exec("SELECT murmur_close()").unwrap();
    drop(conn);

    // Reopen: previously adopted tables capture without re-enabling.
    let conn = open_conn(&fx);
    conn.exec("BEGIN").unwrap();
    conn.exec("INSERT INTO t VALUES (1, 'reopened')").unwrap();
    assert_eq!(collect_seq(&conn), Some(1));
    conn.exec("COMMIT").unwrap();
    assert_eq!(conn.query_int("SELECT murmur_drain()").unwrap(), 1);
    conn.exec("SELECT murmur_close()").unwrap();
    drop(conn);

    let store = open_spool(&fx);
    let meta = read_meta(&store, &first, &[int_value(1)]);
    let v = meta
        .cells
        .iter()
        .find(|c| c.column == format!("{first}/v"))
        .unwrap();
    assert_eq!(v.value, Value::Text("reopened".into()));
    store.close().unwrap();
}

#[test]
fn disable_stops_capture_and_reenable_backfills() {
    let _g = serial();
    let fx = setup();
    let conn = open_conn(&fx);
    conn.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    let id = conn.query_text("SELECT murmur_enable_sync('t')").unwrap();
    assert_eq!(conn.query_int("SELECT murmur_disable_sync('t')").unwrap(), 0);
    conn.exec("BEGIN").unwrap();
    conn.exec("INSERT INTO t VALUES (1, 'missed')").unwrap();
    assert_eq!(collect_seq(&conn), None, "detached writes are not captured");
    conn.exec("COMMIT").unwrap();
    let tables = conn.query_text("SELECT murmur_sync_tables()").unwrap();
    assert!(tables.contains("\"enabled\":false"), "{tables}");
    // Re-enable backfills the missed row; later writes merge onto it.
    assert_eq!(
        conn.query_text("SELECT murmur_enable_sync('t')").unwrap(),
        id
    );
    conn.exec("BEGIN").unwrap();
    conn.exec("UPDATE t SET v = 'found' WHERE id = 1").unwrap();
    assert_eq!(collect_seq(&conn), Some(1));
    conn.exec("COMMIT").unwrap();
    assert_eq!(conn.query_int("SELECT murmur_drain()").unwrap(), 1);
    conn.exec("SELECT murmur_close()").unwrap();
    drop(conn);

    let store = open_spool(&fx);
    let meta = read_meta(&store, &id, &[int_value(1)]);
    let v = meta
        .cells
        .iter()
        .find(|c| c.column == format!("{id}/v"))
        .unwrap();
    assert_eq!(v.value, Value::Text("found".into()));
    store.close().unwrap();
}

#[test]
fn collect_autocommit() {
    let _g = serial();
    let fx = setup();
    let conn = open_conn(&fx);
    conn.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    let id = conn.query_text("SELECT murmur_enable_sync('t')").unwrap();
    conn.exec("INSERT INTO t VALUES (1, 'loose')").unwrap();
    assert_eq!(collect_seq(&conn), Some(1));
    assert_eq!(conn.query_int("SELECT murmur_drain()").unwrap(), 1);
    conn.exec("SELECT murmur_close()").unwrap();
    drop(conn);

    let store = open_spool(&fx);
    read_meta(&store, &id, &[int_value(1)]);
    store.close().unwrap();
}

#[test]
fn close_and_backstop() {
    let _g = serial();
    let fx = setup();
    let count = |conn: &Conn| conn.query_int("SELECT murmur_debug_sessions()").unwrap();
    let probe = Conn::open(&fx.db).unwrap();
    probe.load_extension(&cdylib(), None).unwrap();
    // Fresh process state for this serial test: no sessions yet.
    let base = count(&probe);
    let a = open_conn(&fx);
    assert_eq!(count(&probe), base + 1);
    let b = open_conn(&fx);
    assert_eq!(count(&probe), base + 2);
    // Explicit close detaches one session...
    b.exec("SELECT murmur_close()").unwrap();
    assert_eq!(count(&probe), base + 1);
    // ...and dropping a connection without murmur_close still cleans up via
    // the connection-close backstop.
    drop(a);
    assert_eq!(count(&probe), base);
    drop(b);
    drop(probe);
}

#[test]
fn idempotent_redrain() {
    let _g = serial();
    let fx = setup();
    let conn = open_conn(&fx);
    conn.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    let id = conn.query_text("SELECT murmur_enable_sync('t')").unwrap();
    conn.exec("BEGIN").unwrap();
    conn.exec("INSERT INTO t VALUES (1, 'x')").unwrap();
    assert_eq!(collect_seq(&conn), Some(1));
    conn.exec("COMMIT").unwrap();
    // Snapshot the outbox row, drain, then replay the identical bytes as if
    // the ack had been lost to a crash.
    let row = conn
        .query_text(
            "SELECT millis || '|' || logical || '|' || hex(node) || '|' || hex(changeset) FROM _murmur_outbox WHERE seq = 1",
        )
        .unwrap();
    assert_eq!(conn.query_int("SELECT murmur_drain()").unwrap(), 1);
    let parts: Vec<&str> = row.split('|').collect();
    conn.exec(&format!(
        "INSERT INTO _murmur_outbox(millis, logical, node, changeset) VALUES ({}, {}, x'{}', x'{}')",
        parts[0], parts[1], parts[2], parts[3]
    ))
    .unwrap();
    assert_eq!(conn.query_int("SELECT murmur_drain()").unwrap(), 1);
    assert_eq!(
        conn.query_int("SELECT COUNT(*) FROM _murmur_outbox").unwrap(),
        0
    );
    conn.exec("SELECT murmur_close()").unwrap();
    drop(conn);

    let store = open_spool(&fx);
    let meta = read_meta(&store, &id, &[int_value(1)]);
    let v = meta
        .cells
        .iter()
        .find(|c| c.column == format!("{id}/v"))
        .unwrap();
    assert_eq!(v.value, Value::Text("x".into()));
    assert!(tombstones(&store).is_empty());
    store.close().unwrap();
}

#[test]
fn memory_db_rejected() {
    let _g = serial();
    let conn = Conn::open_memory().unwrap();
    conn.load_extension(&cdylib(), None).unwrap();
    let err = conn.exec("SELECT murmur_open('/tmp/x')").unwrap_err();
    assert!(err.contains("file-backed"), "{err}");
}

#[test]
fn keyed_open_and_mismatch() {
    let _g = serial();
    let fx = setup();
    let a = Conn::open(&fx.db).unwrap();
    a.load_extension(&cdylib(), None).unwrap();
    a.exec(&format!(
        "SELECT murmur_open_keyed('{}', zeroblob(32))",
        fx.spool
    ))
    .unwrap();
    // Same spool, same key: shares the store.
    let b = Conn::open(&fx.db).unwrap();
    b.load_extension(&cdylib(), None).unwrap();
    b.exec(&format!(
        "SELECT murmur_open_keyed('{}', zeroblob(32))",
        fx.spool
    ))
    .unwrap();
    // Same spool, different key: refused.
    let c = Conn::open(&fx.db).unwrap();
    c.load_extension(&cdylib(), None).unwrap();
    let err = c
        .exec(&format!(
            "SELECT murmur_open_keyed('{}', x'{}')",
            fx.spool,
            "01".repeat(32)
        ))
        .unwrap_err();
    assert!(err.contains("different key"), "{err}");
    // Wrong key size: refused.
    let err = c
        .exec(&format!(
            "SELECT murmur_open_keyed('{}', zeroblob(31))",
            fx.spool
        ))
        .unwrap_err();
    assert!(err.contains("32 bytes"), "{err}");
    for conn in [&a, &b] {
        conn.exec("SELECT murmur_close()").unwrap();
    }
}

#[test]
fn enable_inside_txn_rejected() {
    let _g = serial();
    let fx = setup();
    let conn = open_conn(&fx);
    conn.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    conn.exec("BEGIN").unwrap();
    let err = conn.query_text("SELECT murmur_enable_sync('t')").unwrap_err();
    assert!(err.contains("outside a transaction"), "{err}");
    conn.exec("ROLLBACK").unwrap();
    conn.exec("SELECT murmur_close()").unwrap();
}

fn clock(conn: &Conn) -> (u64, u64) {
    let s = conn.query_text("SELECT murmur_clock()").unwrap();
    let num = |key: &str| -> u64 {
        let i = s.find(key).unwrap() + key.len();
        s[i..]
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect::<String>()
            .parse()
            .unwrap()
    };
    (num("\"millis\":"), num("\"logical\":"))
}

#[test]
fn backfill_bumps_clock() {
    let _g = serial();
    let fx = setup();
    let conn = open_conn(&fx);
    let before = clock(&conn);
    conn.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.exec("INSERT INTO t VALUES (1, 'a'), (2, 'b')").unwrap();
    conn.exec("SELECT murmur_enable_sync('t')").unwrap();
    let after = clock(&conn);
    // The backfill version is durable clock state: without the bump, a
    // restart would re-mint versions at or below it for new data.
    assert!(after > before, "{after:?} > {before:?}");
    conn.exec("SELECT murmur_close()").unwrap();
}

#[test]
fn spool_sharing_across_dbs_rejected() {
    let _g = serial();
    let fx = setup();
    let a = open_conn(&fx);
    let other = fx.dir.path().join("other.db").to_str().unwrap().to_string();
    let b = Conn::open(&other).unwrap();
    b.load_extension(&cdylib(), None).unwrap();
    // Metadata keys carry no database qualifier: one spool, one database.
    let err = b.exec(&format!("SELECT murmur_open('{}')", fx.spool)).unwrap_err();
    assert!(err.contains("different database"), "{err}");
    drop(b);
    a.exec("SELECT murmur_close()").unwrap();
}

#[test]
fn wal_enforced() {
    let _g = serial();
    let fx = setup();
    let conn = Conn::open(&fx.db).unwrap();
    conn.load_extension(&cdylib(), None).unwrap();
    // First open inside a transaction cannot switch modes: loud refusal.
    conn.exec("BEGIN").unwrap();
    let err = conn
        .exec(&format!("SELECT murmur_open('{}')", fx.spool))
        .unwrap_err();
    assert!(err.contains("WAL"), "{err}");
    conn.exec("ROLLBACK").unwrap();
    conn.exec(&format!("SELECT murmur_open('{}')", fx.spool))
        .unwrap();
    assert_eq!(
        conn.query_text("PRAGMA journal_mode").unwrap().as_str(),
        "wal"
    );
    conn.exec("SELECT murmur_close()").unwrap();
    drop(conn);
    // The refused open left no trace: the store evicted fully.
    let store = open_spool(&fx);
    store.close().unwrap();
}

fn outbox_latest(conn: &Conn) -> (u64, u64, String) {
    let rows = conn
        .query_dump("SELECT millis, logical, hex(node) FROM _murmur_outbox ORDER BY seq DESC LIMIT 1")
        .unwrap();
    (
        rows[0][0].parse().unwrap(),
        rows[0][1].parse().unwrap(),
        rows[0][2].clone(),
    )
}

#[test]
fn ticks_monotonic_across_rollback() {
    let _g = serial();
    let fx = setup();
    let conn = open_conn(&fx);
    conn.exec("CREATE TABLE t(id INTEGER PRIMARY KEY)").unwrap();
    conn.exec("SELECT murmur_enable_sync('t')").unwrap();
    conn.exec("BEGIN").unwrap();
    conn.exec("INSERT INTO t VALUES (1)").unwrap();
    collect_seq(&conn);
    let v1 = outbox_latest(&conn);
    conn.exec("COMMIT").unwrap();
    // Burned by rollback: the version must still never be re-minted.
    conn.exec("BEGIN").unwrap();
    conn.exec("INSERT INTO t VALUES (2)").unwrap();
    collect_seq(&conn);
    let v2 = outbox_latest(&conn);
    conn.exec("ROLLBACK").unwrap();
    conn.exec("BEGIN").unwrap();
    conn.exec("INSERT INTO t VALUES (3)").unwrap();
    collect_seq(&conn);
    let v3 = outbox_latest(&conn);
    conn.exec("COMMIT").unwrap();
    assert_eq!(v1.2, v2.2);
    assert_eq!(v2.2, v3.2, "same node, time-ordered ticks");
    assert!(v1 < v2, "{v1:?} < {v2:?}");
    assert!(v2 < v3, "{v2:?} < {v3:?}");
    // Both in-transaction drains deferred their acks (caller held the write
    // lock); one autocommit drain reprocesses idempotently and clears all.
    assert_eq!(conn.query_int("SELECT murmur_drain()").unwrap(), 2);
    assert_eq!(
        conn.query_int("SELECT COUNT(*) FROM _murmur_outbox").unwrap(),
        0
    );
    conn.exec("SELECT murmur_close()").unwrap();
}
