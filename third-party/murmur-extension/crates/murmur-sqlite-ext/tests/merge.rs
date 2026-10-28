//! Phase 3 merge suite: two nodes exchange batches and converge.
//!
//! Transport is direct function calls (Phase 4 carries the same bytes over
//! QUIC). Winners in conflicting writes depend on wall-clock ticks, so most
//! tests assert bilateral equality — same SQLite content, same metadata
//! bytes, same clock — never which side won. The `later_*` tests fix the
//! tick order with a sleep and assert the deterministic winner; the fencing
//! tests document the engine rule that a same-generation tombstone fences a
//! row regardless of cell recency, so only re-insert resurrects.

#[path = "common.rs"]
#[allow(dead_code)]
mod common;

use common::*;
use host_shim::Conn;
use murmur_crdt::model::{RowState, Value, Version};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn export(conn: &Conn) -> Vec<u8> {
    conn.query_blob("SELECT murmur_export()").unwrap()
}

fn apply(conn: &Conn, batch: &[u8]) -> String {
    conn.query_text(&format!("SELECT murmur_apply(x'{}')", hex(batch)))
        .unwrap()
}

/// Link B to A's database identity; returns A's id.
fn link(a: &Conn, b: &Conn) -> String {
    let id = a.query_text("SELECT murmur_database_id()").unwrap();
    assert_eq!(id.len(), 32);
    b.exec(&format!("SELECT murmur_set_database_id('{id}')")).unwrap();
    assert_eq!(b.query_text("SELECT murmur_database_id()").unwrap(), id);
    id
}

fn dump(conn: &Conn, table: &str) -> Vec<Vec<String>> {
    conn.query_dump(&format!("SELECT * FROM {table} ORDER BY id"))
        .unwrap()
}

/// Every metadata record under 04/ and 05/, sorted: byte-level state proof.
fn meta_set(store: &spool::Store) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out: Vec<(Vec<u8>, Vec<u8>)> = ["04/", "05/meta/"]
        .into_iter()
        .flat_map(|p| store.keys_with_prefix(p.as_bytes()))
        .map(|k| {
            let v = store.get(&k).unwrap().unwrap();
            (k, v)
        })
        .collect();
    out.sort();
    out
}

fn clock_of(store: &spool::Store) -> murmur_crdt::model::Version {
    let bytes = store.get(b"00/clock/ext-v1").unwrap().unwrap();
    murmur_crdt::model::decode(&bytes).unwrap()
}

const DDL: &str = "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)";

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

#[test]
fn two_node_convergence() {
    let fa = setup();
    let fb = setup();
    let a = open_conn(&fa);
    let b = open_conn(&fb);
    link(&a, &b);
    for conn in [&a, &b] {
        conn.exec(DDL).unwrap();
        conn.exec("SELECT murmur_enable_sync('t')").unwrap();
    }
    // Divergent writes, including delete-vs-update on id 3.
    write_txn(&a, &[
        "INSERT INTO t VALUES (1, 'a1'), (2, 'a2'), (3, 'a3')",
        "UPDATE t SET v = 'a1x' WHERE id = 1",
        "DELETE FROM t WHERE id = 3",
    ]);
    write_txn(&b, &[
        "INSERT INTO t VALUES (1, 'b1'), (2, 'b2'), (3, 'b3')",
        "UPDATE t SET v = 'b1x' WHERE id = 1",
        "UPDATE t SET v = 'b2x' WHERE id = 2",
        "UPDATE t SET v = 'b3x' WHERE id = 3",
    ]);
    // Bidirectional sync.
    let summary = apply(&b, &export(&a));
    assert!(summary.contains("\"rows\""), "{summary}");
    let summary = apply(&a, &export(&b));
    assert!(summary.contains("\"rows\""), "{summary}");

    // Same user-visible content on both sides. Row *presence* is
    // deterministic here — A's id-3 DELETE nets against its own INSERT in
    // the same transaction, so no tombstone exists and every inserted id
    // survives — but the winning *values* depend on wall-clock tick order
    // and are asserted only for bilateral equality.
    let da = dump(&a, "t");
    assert_eq!(da, dump(&b, "t"));
    assert_eq!(
        da.iter().map(|r| r[0].clone()).collect::<Vec<_>>(),
        vec!["1".to_string(), "2".to_string(), "3".to_string()]
    );
    for conn in [&a, &b] {
        conn.exec("SELECT murmur_close()").unwrap();
    }
    drop(a);
    drop(b);

    let sa = open_spool(&fa);
    let sb = open_spool(&fb);
    assert_eq!(meta_set(&sa), meta_set(&sb));
    assert_eq!(clock_of(&sa), clock_of(&sb));
    sa.close().unwrap();
    sb.close().unwrap();
}

#[test]
fn double_apply_is_noop() {
    let fa = setup();
    let fb = setup();
    let a = open_conn(&fa);
    let b = open_conn(&fb);
    link(&a, &b);
    for conn in [&a, &b] {
        conn.exec(DDL).unwrap();
        conn.exec("SELECT murmur_enable_sync('t')").unwrap();
    }
    write_txn(&a, &["INSERT INTO t VALUES (1, 'x'), (2, 'y')"]);
    write_txn(&a, &["DELETE FROM t WHERE id = 1"]);
    let batch = export(&a);
    let first = apply(&b, &batch);
    assert_ne!(first, r#"{"deleted":0,"rows":0,"tombstones":0}"#, "{first}");
    let before = dump(&b, "t");
    // Same bytes again: empty patch, nothing rewritten.
    let second = apply(&b, &batch);
    assert_eq!(second, r#"{"deleted":0,"rows":0,"tombstones":0}"#, "{second}");
    assert_eq!(dump(&b, "t"), before);
    for conn in [&a, &b] {
        conn.exec("SELECT murmur_close()").unwrap();
    }
    drop(a);
    drop(b);

    let sb = open_spool(&fb);
    let again = meta_set(&sb);
    sb.close().unwrap();
    // Reopen and apply a third time through a fresh handle: still identical.
    let b = open_conn(&fb);
    assert_eq!(apply(&b, &batch), r#"{"deleted":0,"rows":0,"tombstones":0}"#);
    b.exec("SELECT murmur_close()").unwrap();
    drop(b);
    let sb = open_spool(&fb);
    assert_eq!(meta_set(&sb), again);
    sb.close().unwrap();
}

#[test]
fn loop_suppression_and_reverse_liveness() {
    let fa = setup();
    let fb = setup();
    let a = open_conn(&fa);
    let b = open_conn(&fb);
    link(&a, &b);
    for conn in [&a, &b] {
        conn.exec(DDL).unwrap();
        conn.exec("SELECT murmur_enable_sync('t')").unwrap();
    }
    write_txn(&a, &["INSERT INTO t VALUES (1, 'from-a')"]);
    apply(&b, &export(&a));
    assert_eq!(dump(&b, "t"), vec![vec!["1".to_string(), "from-a".to_string()]]);
    // Applied writes never enter the receiver's capture.
    assert_eq!(
        b.query_int("SELECT COUNT(*) FROM _murmur_outbox").unwrap(),
        0
    );
    assert_eq!(
        b.query_int_nullable("SELECT murmur_collect()").unwrap(),
        None
    );
    // ...while the receiver's own writes still flow back.
    write_txn(&b, &["INSERT INTO t VALUES (2, 'from-b')"]);
    apply(&a, &export(&b));
    assert_eq!(dump(&a, "t").len(), 2);
    assert_eq!(dump(&a, "t"), dump(&b, "t"));
    for conn in [&a, &b] {
        conn.exec("SELECT murmur_close()").unwrap();
    }
}

#[test]
fn database_mismatch() {
    let fa = setup();
    let fb = setup();
    let a = open_conn(&fa);
    let b = open_conn(&fb);
    for conn in [&a, &b] {
        conn.exec(DDL).unwrap();
        conn.exec("SELECT murmur_enable_sync('t')").unwrap();
    }
    write_txn(&a, &["INSERT INTO t VALUES (1, 'x')"]);
    let err = b
        .query_text(&format!("SELECT murmur_apply(x'{}')", hex(&export(&a))))
        .unwrap_err();
    assert!(err.contains("mismatch"), "{err}");
    // Malformed database ids are rejected without touching state.
    for bad in ["abc", &"zz".repeat(16), &"00".repeat(15)] {
        let err = b
            .exec(&format!("SELECT murmur_set_database_id('{bad}')"))
            .unwrap_err();
        assert!(err.contains("hex") || err.contains("32"), "{err}");
    }
    assert_ne!(
        a.query_text("SELECT murmur_database_id()").unwrap(),
        b.query_text("SELECT murmur_database_id()").unwrap()
    );
    for conn in [&a, &b] {
        conn.exec("SELECT murmur_close()").unwrap();
    }
}

#[test]
fn scope_filtering_extra_table() {
    let fa = setup();
    let fb = setup();
    let a = open_conn(&fa);
    let b = open_conn(&fb);
    link(&a, &b);
    for conn in [&a, &b] {
        conn.exec(DDL).unwrap();
        conn.exec("SELECT murmur_enable_sync('t')").unwrap();
    }
    // B adopts an extra table A never heard of.
    b.exec("CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    let idu = b.query_text("SELECT murmur_enable_sync('u')").unwrap();
    b.exec("BEGIN").unwrap();
    b.exec("INSERT INTO u VALUES (1, 'local-only')").unwrap();
    b.query_int_nullable("SELECT murmur_collect()").unwrap();
    b.exec("COMMIT").unwrap();
    b.exec("SELECT murmur_drain()").unwrap();
    write_txn(&a, &["INSERT INTO t VALUES (1, 'shared')"]);

    // A -> B: unknown-to-A table untouched, shared row arrives.
    apply(&b, &export(&a));
    assert_eq!(dump(&b, "u"), vec![vec!["1".to_string(), "local-only".to_string()]]);
    assert_eq!(dump(&b, "t").len(), 1);
    // B -> A: the extra table's rows are dropped, no error.
    apply(&a, &export(&b));
    assert_eq!(dump(&a, "t").len(), 1);
    assert_eq!(dump(&a, "t"), dump(&b, "t"));
    for conn in [&a, &b] {
        conn.exec("SELECT murmur_close()").unwrap();
    }
    drop(a);
    drop(b);
    // B's extra table metadata survived the round trip untouched.
    let sb = open_spool(&fb);
    let meta = read_meta(&sb, &idu, &[int_value(1)]);
    let v = meta
        .cells
        .iter()
        .find(|c| c.column == format!("{idu}/v"))
        .unwrap();
    assert_eq!(v.value, Value::Text("local-only".into()));
    sb.close().unwrap();
}

#[test]
fn self_apply_noop() {
    let fx = setup();
    let conn = open_conn(&fx);
    conn.exec(DDL).unwrap();
    conn.exec("SELECT murmur_enable_sync('t')").unwrap();
    write_txn(&conn, &["INSERT INTO t VALUES (1, 'x')"]);
    let batch = export(&conn);
    assert_eq!(
        apply(&conn, &batch),
        r#"{"deleted":0,"rows":0,"tombstones":0}"#
    );
    let err = conn.query_text("SELECT murmur_apply(x'')").unwrap_err();
    assert!(err.contains("bad batch"), "{err}");
    conn.exec("SELECT murmur_close()").unwrap();
}

#[test]
fn export_drains_first() {
    let fa = setup();
    let fb = setup();
    let a = open_conn(&fa);
    let b = open_conn(&fb);
    link(&a, &b);
    for conn in [&a, &b] {
        conn.exec(DDL).unwrap();
        conn.exec("SELECT murmur_enable_sync('t')").unwrap();
    }
    // Collect but deliberately never drain: the export must still carry it.
    a.exec("BEGIN").unwrap();
    a.exec("INSERT INTO t VALUES (1, 'fresh')").unwrap();
    a.query_int_nullable("SELECT murmur_collect()").unwrap();
    a.exec("COMMIT").unwrap();
    assert_eq!(
        a.query_int("SELECT COUNT(*) FROM _murmur_outbox").unwrap(),
        1
    );
    apply(&b, &export(&a));
    assert_eq!(dump(&b, "t").len(), 1);
    assert_eq!(
        a.query_int("SELECT COUNT(*) FROM _murmur_outbox").unwrap(),
        0,
        "export drained the outbox"
    );
    for conn in [&a, &b] {
        conn.exec("SELECT murmur_close()").unwrap();
    }
}

/// Sleep past a millisecond boundary so the next tick is strictly newer
/// in wall-clock millis, fixing the LWW winner deterministically.
fn tick_gap() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn synced_pair() -> (Fixture, Fixture, Conn, Conn) {
    let fa = setup();
    let fb = setup();
    let a = open_conn(&fa);
    let b = open_conn(&fb);
    link(&a, &b);
    for conn in [&a, &b] {
        conn.exec(DDL).unwrap();
        conn.exec("SELECT murmur_enable_sync('t')").unwrap();
    }
    write_txn(&a, &["INSERT INTO t VALUES (1, 'seed')"]);
    apply(&b, &export(&a));
    apply(&a, &export(&b));
    assert_eq!(dump(&a, "t"), dump(&b, "t"));
    assert_eq!(dump(&a, "t").len(), 1);
    (fa, fb, a, b)
}

fn exchange(a: &Conn, b: &Conn) {
    apply(&b, &export(&a));
    apply(&a, &export(&b));
}

#[test]
fn delete_fences_same_generation_update() {
    let (_fa, _fb, a, b) = synced_pair();
    write_txn(&b, &["UPDATE t SET v = 'b-new' WHERE id = 1"]);
    write_txn(&a, &["DELETE FROM t WHERE id = 1"]);
    exchange(&a, &b);
    assert!(dump(&a, "t").is_empty());
    assert!(dump(&b, "t").is_empty());
    for conn in [&a, &b] {
        conn.exec("SELECT murmur_close()").unwrap();
    }
}

#[test]
fn later_update_stays_fenced() {
    // Fencing compares generations, not versions: even a strictly newer
    // update to the same generation loses to the tombstone.
    let (_fa, _fb, a, b) = synced_pair();
    write_txn(&a, &["DELETE FROM t WHERE id = 1"]);
    tick_gap();
    write_txn(&b, &["UPDATE t SET v = 'too-late' WHERE id = 1"]);
    exchange(&a, &b);
    assert!(dump(&a, "t").is_empty());
    assert!(dump(&b, "t").is_empty());
    for conn in [&a, &b] {
        conn.exec("SELECT murmur_close()").unwrap();
    }
}

#[test]
fn later_cell_update_wins() {
    // Pure LWW with no tombstone: the strictly later cell wins everywhere.
    let (_fa, _fb, a, b) = synced_pair();
    write_txn(&a, &["UPDATE t SET v = 'a-new' WHERE id = 1"]);
    tick_gap();
    write_txn(&b, &["UPDATE t SET v = 'b-new' WHERE id = 1"]);
    exchange(&a, &b);
    let expect = vec![vec!["1".to_string(), "b-new".to_string()]];
    assert_eq!(dump(&a, "t"), expect);
    assert_eq!(dump(&b, "t"), expect);
    for conn in [&a, &b] {
        conn.exec("SELECT murmur_close()").unwrap();
    }
}

fn table_id(conn: &Conn) -> String {
    let s = conn.query_text("SELECT murmur_sync_tables()").unwrap();
    let key = "\"id\":\"";
    let i = s.find(key).unwrap() + key.len();
    s[i..].split('"').next().unwrap().to_string()
}

#[test]
fn crashed_apply_heals_on_reopen() {
    let fa = setup();
    let fb = setup();
    let a = open_conn(&fa);
    let b = open_conn(&fb);
    link(&a, &b);
    for conn in [&a, &b] {
        conn.exec(DDL).unwrap();
        conn.exec("SELECT murmur_enable_sync('t')").unwrap();
    }
    let table = table_id(&b);
    write_txn(&a, &["INSERT INTO t VALUES (1, 'a1'), (2, 'a2')"]);
    apply(&b, &export(&a));
    write_txn(&a, &["DELETE FROM t WHERE id = 1", "INSERT INTO t VALUES (3, 'a3')"]);
    apply(&b, &export(&a));
    assert_eq!(dump(&b, "t").len(), 2);
    b.exec("SELECT murmur_close()").unwrap();
    drop(b);

    // Recreate the exact crash window: Spool ahead (metadata committed),
    // SQLite behind (the second apply's writes never landed).
    let plain = Conn::open(&fb.db).unwrap();
    plain.exec("DELETE FROM t WHERE id = 3").unwrap();
    plain.exec("INSERT INTO t VALUES (1, 'stale')").unwrap();
    drop(plain);
    let store = open_spool(&fb);
    // Faithful to a real crash: only the stranded apply's patch rows.
    let rows: Vec<RowState> = [3i64]
        .iter()
        .map(|n| {
            let meta = read_meta(&store, &table, &[int_value(*n)]);
            RowState {
                table: table.clone(),
                key: murmur_crdt::model::decode(&meta.key).unwrap(),
                generation: meta.generation,
                cells: meta.cells,
            }
        })
        .collect();
    // Deletes carry only table + key; id 1's metadata is already gone.
    let deleted = vec![RowState {
        table: table.clone(),
        key: vec![int_value(1)],
        generation: Version::default(),
        cells: vec![],
    }];
    // Couples to sync::JOURNAL_KEY (the cdylib exposes no rlib to import).
    let journal = murmur_crdt::model::encode(&(&rows, &deleted)).unwrap();
    store
        .commit(
            &[spool::Mutation::put(
                b"00/apply/journal/ext-v1".to_vec(),
                journal,
            )],
            spool::Durability::Sync,
        )
        .unwrap();
    store.close().unwrap();

    // Reopen replays the journal before anything else runs: id 3 appears,
    // id 1's stale row disappears, id 2 is untouched.
    let b = open_conn(&fb);
    assert_eq!(
        dump(&b, "t"),
        vec![
            vec!["2".to_string(), "a2".to_string()],
            vec!["3".to_string(), "a3".to_string()],
        ]
    );
    b.exec("SELECT murmur_close()").unwrap();
    drop(b);

    // Journal cleared: a second reopen is a plain no-op open.
    let store = open_spool(&fb);
    assert!(store
        .get(b"00/apply/journal/ext-v1")
        .unwrap()
        .is_none());
    store.close().unwrap();
    let b = open_conn(&fb);
    assert_eq!(dump(&b, "t").len(), 2);
    b.exec("SELECT murmur_close()").unwrap();
}

#[test]
fn reinsert_after_delete_resurrects() {
    // Only a new generation escapes the fence. The exchange between the
    // delete and the re-insert is load-bearing: it carries A's clock to B,
    // so HLC causality (not wall luck) makes the new generation greater.
    let (_fa, _fb, a, b) = synced_pair();
    write_txn(&a, &["DELETE FROM t WHERE id = 1"]);
    exchange(&a, &b);
    assert!(dump(&a, "t").is_empty());
    assert!(dump(&b, "t").is_empty());
    write_txn(&b, &["INSERT INTO t VALUES (1, 'back')"]);
    exchange(&a, &b);
    let expect = vec![vec!["1".to_string(), "back".to_string()]];
    assert_eq!(dump(&a, "t"), expect);
    assert_eq!(dump(&b, "t"), expect);
    for conn in [&a, &b] {
        conn.exec("SELECT murmur_close()").unwrap();
    }
}
