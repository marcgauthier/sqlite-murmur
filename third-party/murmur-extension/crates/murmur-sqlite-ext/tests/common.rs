//! Shared fixtures for extension integration tests: temp file-backed
//! databases, direct Spool reads (valid only after the extension released
//! the store via `murmur_close`), and metadata decoding.
use host_shim::Conn;

pub fn cdylib() -> String {
    let path = env!("MURMUR_EXT_CDYLIB").to_string();
    let meta = std::fs::metadata(&path).unwrap_or_else(|_| panic!("cdylib missing at {path}"));
    // dlopen tests do not link the lib, so `cargo test --test <name>` alone
    // can run against a stale .so. Refuse instead of testing old code.
    let root = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let src = root.join("crates/murmur-sqlite-ext/src");
    let mut newest = std::time::SystemTime::UNIX_EPOCH;
    for entry in std::fs::read_dir(&src).unwrap() {
        let m = entry.unwrap().metadata().unwrap().modified().unwrap();
        newest = newest.max(m);
    }
    let built = meta.modified().unwrap();
    assert!(
        built >= newest,
        "stale cdylib: rebuild with `cargo build -p murmur-sqlite-ext` first \
         (cargo test --test <name> alone does not rebuild it)"
    );
    path
}

pub struct Fixture {
    pub dir: tempfile::TempDir,
    pub db: String,
    pub spool: String,
}

pub fn setup() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("test.db").to_str().unwrap().to_string();
    let spool = dir.path().join("spool").to_str().unwrap().to_string();
    Fixture { dir, db, spool }
}

/// Open a connection, load the extension, and attach replication state.
pub fn open_conn(fx: &Fixture) -> Conn {
    let conn = Conn::open(&fx.db).unwrap();
    conn.load_extension(&cdylib(), None).unwrap();
    conn.exec(&format!("SELECT murmur_open('{}')", fx.spool))
        .unwrap();
    conn
}

/// Read-only Spool access for assertions. The caller must have released
/// every extension connection on this spool (`murmur_close` + drop):
/// Spool file-locking rejects double opens.
pub fn open_spool(fx: &Fixture) -> spool::Store {
    let mut opts = spool::default_options(fx.spool.clone());
    opts.encryption = spool::Encryption::None;
    opts.reclaim_interval = None;
    spool::open(&opts).unwrap()
}

pub fn read_meta(
    store: &spool::Store,
    table_id: &str,
    key: &[murmur_crdt::model::Value],
) -> murmur_crdt::meta::RowMeta {
    let key_bytes = murmur_crdt::model::encode(&key.to_vec()).unwrap();
    let row_key = murmur_crdt::meta::row_key(table_id, &key_bytes);
    let bytes = store
        .get(&row_key)
        .unwrap()
        .unwrap_or_else(|| panic!("missing metadata for {table_id}/{key:?}"));
    murmur_crdt::model::decode(&bytes).unwrap()
}

pub fn tombstones(store: &spool::Store) -> Vec<murmur_crdt::model::Tombstone> {
    store
        .keys_with_prefix(b"05/meta/tomb/")
        .into_iter()
        .map(|k| {
            let bytes = store.get(&k).unwrap().unwrap();
            murmur_crdt::model::decode(&bytes).unwrap()
        })
        .collect()
}

pub fn row_meta_count(store: &spool::Store) -> usize {
    store.keys_with_prefix(b"04/meta/row/").len()
}

pub fn int_value(n: i64) -> murmur_crdt::model::Value {
    murmur_crdt::model::Value::Integer(n)
}
