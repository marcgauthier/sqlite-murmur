//! Phase 1 dependency-wiring proof: the extension crate resolves and links
//! the Murmur engine crates (`murmur-crdt`, `murmur-spool`,
//! `murmur-replication`) alongside `rusqlite`'s `loadable_extension` build
//! with no feature conflicts, and the shared conflict semantics behave.
use murmur_crdt::model::{
    Cell, Column, MergePolicy, ReplicaState, RowState, Schema, Table, Value, Version,
};

fn version(millis: u64, logical: u32) -> Version {
    Version {
        millis,
        logical,
        node: [1; 16],
    }
}

fn schema() -> Schema {
    Schema {
        tables: vec![Table {
            application_key: None,
            id: "t".into(),
            name: "t".into(),
            sql: "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)".into(),
            columns: vec![
                Column {
                    vector: None,
                    fts: false,
                    policy: MergePolicy::Lww,
                    id: "id".into(),
                    name: "id".into(),
                    primary_key: 1,
                    declared_type: "INTEGER".into(),
                    not_null: true,
                },
                Column {
                    vector: None,
                    fts: false,
                    policy: MergePolicy::Lww,
                    id: "v".into(),
                    name: "v".into(),
                    primary_key: 0,
                    declared_type: "TEXT".into(),
                    not_null: false,
                },
            ],
        }],
        ..Schema::default()
    }
}

fn state(schema: &Schema, value: &str, at: Version) -> ReplicaState {
    ReplicaState {
        database_id: [7; 16],
        schema: schema.clone(),
        rows: vec![RowState {
            table: "t".into(),
            key: vec![Value::Integer(1)],
            generation: version(1, 0),
            cells: vec![
                Cell {
                    column: "id".into(),
                    value: Value::Integer(1),
                    version: version(1, 0),
                },
                Cell {
                    column: "v".into(),
                    value: Value::Text(value.into()),
                    version: at,
                },
            ],
        }],
        tombstones: vec![],
        clock: version(1, 0),
    }
}

#[test]
fn crdt_join_picks_newer_cell() {
    let schema = schema();
    let a = state(&schema, "old", version(10, 0));
    let b = state(&schema, "new", version(20, 0));
    let joined = murmur_crdt::merge::join(&a, &b).unwrap();
    assert_eq!(joined.rows.len(), 1);
    let cell = joined.rows[0]
        .cells
        .iter()
        .find(|c| c.column == "v")
        .unwrap();
    assert_eq!(cell.value, Value::Text("new".into()));
    assert_eq!(cell.version, version(20, 0));
}

#[test]
fn crdt_clock_ticks_forward() {
    let first = Version::default().next([2; 16]).unwrap();
    assert!(first.millis > 0);
    assert_eq!(first.node, [2; 16]);
    let second = first.next([2; 16]).unwrap();
    assert!(second > first);
}

#[test]
fn crdt_metadata_keys_and_opt_in() {
    let key = murmur_crdt::meta::row_key("t", b"pk");
    let text = String::from_utf8(key).unwrap();
    assert!(text.starts_with("04/meta/row/t/"), "{text}");
    assert!(!murmur_crdt::table_replication::enabled(&schema(), "t"));
}

#[test]
fn spool_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let mut opts = spool::default_options(tmp.path().to_str().unwrap().to_string());
    opts.master_key = vec![7u8; 32];
    opts.reclaim_interval = None;
    let store = spool::open(&opts).unwrap();
    store.put(b"hello", b"world").unwrap();
    store.flush().unwrap(); // get() reads committed values; pending puts are excluded
    assert_eq!(store.get(b"hello").unwrap().as_deref(), Some(&b"world"[..]));
    store.close().unwrap();
}

#[test]
fn replication_codec_roundtrip() {
    let record = murmur_replication::Record::put(b"04/meta/row/t/x".to_vec(), vec![1, 2, 3]);
    let bytes = murmur_replication::encode(&record).unwrap();
    let back: murmur_replication::Record = murmur_replication::decode(&bytes).unwrap();
    assert_eq!(back.key, b"04/meta/row/t/x");
    assert_eq!(back.value, Some(vec![1, 2, 3]));
}
