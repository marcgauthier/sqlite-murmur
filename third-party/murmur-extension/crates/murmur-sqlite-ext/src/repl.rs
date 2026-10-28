//! QUIC replication: a `murmur_replication::Backend` over our Spool store.
//!
//! Log-from-birth, transport-on-demand: every drained batch and every
//! backfill signs its delta into the replication log (`07/replication/…`,
//! engine-identical keys and transaction format) whether or not transport
//! is running, so a late `murmur_repl_start` never loses history. Starting
//! transport only binds the QUIC endpoint and drives the engine's
//! push/anti-entropy over the log.
//!
//! Locking: raw key/value (`get`/`keys`/`commit`/`identity`) touches Spool
//! directly — Spool serializes internally — and never takes `CORE`, so the
//! engine's synchronous start path (`bind_signer`, `migrate_peers`) is safe
//! under it. Everything that reads-then-writes (`apply`, `snapshot`,
//! `schema`, `observe`, drains) takes `CORE`, serializing backend threads
//! with SQL threads. `Handle::close` joins the replication thread and must
//! never run under `CORE` (see `murmur_repl_stop`).

use murmur_crdt::logical::LogicalStore;
use murmur_crdt::model::{ReplicaState, RowState, Schema, Tombstone, Version};
use murmur_replication::protocol;
use std::sync::atomic::{AtomicU64, Ordering};

/// This node's replication signing key (32 bytes, minted at open).
pub const SIGN_KEY: &[u8] = b"00/node/signkey/ext-v1";

/// Log payload budget per transaction. Matches the engine's default batch
/// budget and sits well under the 8MB wire frame; over-sized batch deltas
/// split into consecutive sequences rather than failing.
const PAYLOAD_BYTES: usize = 4 << 20;

/// Load-or-create the signing key. Callers hold `CORE` (open, repl_start);
/// backend threads only ever read it.
pub fn ensure_signkey(store: &LogicalStore) -> Result<[u8; 32], String> {
    if let Some(bytes) = store.get(SIGN_KEY).map_err(|e| e.to_string())? {
        return bytes
            .try_into()
            .map_err(|_| "murmur: corrupt replication signing key".to_string());
    }
    let key: [u8; 32] = rand::random();
    store
        .commit(
            &[spool::Mutation::put(SIGN_KEY.to_vec(), key.to_vec())],
            spool::Durability::Sync,
        )
        .map_err(|e| e.to_string())?;
    Ok(key)
}

/// Read-only counterpart for backend threads: the key always exists past
/// open, so a miss is corruption, not a race.
pub fn read_signkey(store: &LogicalStore) -> Result<[u8; 32], String> {
    match store.get(SIGN_KEY).map_err(|e| e.to_string())? {
        Some(bytes) => bytes
            .try_into()
            .map_err(|_| "murmur: corrupt replication signing key".to_string()),
        None => Err("murmur: replication signing key missing (re-open the database)".into()),
    }
}

/// Normalize a schema for digests and payloads: enabled tables only,
/// sorted by ID with columns sorted and DDL text cleared, so same-shape
/// nodes agree no matter the adoption order or DDL spelling. The merge path
/// re-checks shapes per table; normalized bytes are only a discovery hint.
pub fn normalize_schema(schema: &Schema) -> Schema {
    let mut tables: Vec<_> = schema
        .tables
        .iter()
        .filter(|t| murmur_crdt::table_replication::enabled(schema, &t.id))
        .cloned()
        .collect();
    tables.sort_by(|a, b| a.id.cmp(&b.id));
    for table in &mut tables {
        table.columns.sort_by(|a, b| a.id.cmp(&b.id));
        table.sql.clear();
    }
    Schema {
        tables,
        ..schema.clone()
    }
}

/// Normalized schema bytes, loaded from the store.
pub fn normalized_schema(store: &LogicalStore) -> Result<Vec<u8>, String> {
    let schema = crate::schema_map::load_schema(store)?;
    murmur_crdt::model::encode(&normalize_schema(&schema)).map_err(|e| e.to_string())
}

/// Sign one batch delta into the origin log, mirroring the engine's
/// `sign_content` key-for-key (retained head, log, time, sequence,
/// progress, receipt). Empty deltas sign nothing and consume no sequence.
/// Over-sized deltas split into consecutive sequences sharing the batch
/// clock; receivers merge each transaction independently, so a partial
/// prefix is still a valid CRDT state.
///
/// Must run under `CORE`: sequence assignment is read-modify-write.
#[allow(clippy::too_many_arguments)]
pub fn sign_log(
    store: &LogicalStore,
    schema: &Schema,
    database: [u8; 16],
    node: [u8; 16],
    signkey: &[u8; 32],
    clock: Version,
    rows: Vec<RowState>,
    tombstones: Vec<Tombstone>,
) -> Result<Vec<spool::Mutation>, String> {
    if rows.is_empty() && tombstones.is_empty() {
        return Ok(Vec::new());
    }
    let schema_bytes = murmur_crdt::model::encode(schema).map_err(|e| e.to_string())?;
    // Size each item once, then greedily pack chunks under budget. An
    // oversized single item still ships alone (progress over budget).
    enum Item {
        Row(RowState),
        Tomb(Tombstone),
    }
    let mut items = Vec::with_capacity(rows.len() + tombstones.len());
    for row in rows {
        let size = murmur_crdt::model::encode(&row).map_err(|e| e.to_string())?.len();
        items.push((Item::Row(row), size));
    }
    for tomb in tombstones {
        let size = murmur_crdt::model::encode(&tomb).map_err(|e| e.to_string())?.len();
        items.push((Item::Tomb(tomb), size));
    }
    let mut chunks: Vec<(Vec<RowState>, Vec<Tombstone>)> = Vec::new();
    let mut chunk: (Vec<RowState>, Vec<Tombstone>) = (Vec::new(), Vec::new());
    let mut chunk_bytes = schema_bytes.len() + 1024;
    for (item, size) in items {
        if chunk_bytes + size > PAYLOAD_BYTES && (!chunk.0.is_empty() || !chunk.1.is_empty()) {
            chunks.push(std::mem::take(&mut chunk));
            chunk_bytes = schema_bytes.len() + 1024;
        }
        chunk_bytes += size;
        match item {
            Item::Row(row) => chunk.0.push(row),
            Item::Tomb(tomb) => chunk.1.push(tomb),
        }
    }
    if !chunk.0.is_empty() || !chunk.1.is_empty() {
        chunks.push(chunk);
    }

    let seq_key = protocol::sequence_key(node);
    let seq = store
        .get(&seq_key)
        .map_err(|e| e.to_string())?
        .map(|b| murmur_replication::decode::<u64>(&b))
        .transpose()
        .map_err(|e| e.to_string())?
        .unwrap_or(0);
    let head_key = protocol::retained_key(node);
    let mut head: protocol::Progress = match store.get(&head_key).map_err(|e| e.to_string())? {
        Some(b) => murmur_replication::decode(&b).map_err(|e| e.to_string())?,
        None => protocol::Progress {
            origin: node,
            applied: 0,
            observed: 0,
            retained_from: seq + 1,
            retained_through: 0,
        },
    };
    let chunks_len = chunks.len() as u64;
    seq.checked_add(chunks_len).ok_or_else(|| {
        "murmur: origin sequence exhausted".to_string()
    })?;
    let mut puts = Vec::new();
    for (i, (rows, tombs)) in chunks.into_iter().enumerate() {
        let sequence = seq + i as u64 + 1;
        let state = ReplicaState {
            database_id: database,
            schema: schema.clone(),
            rows,
            tombstones: tombs,
            clock,
        };
        let content = murmur_replication::Content {
            sqlite: Some(murmur_crdt::model::encode(&state).map_err(|e| e.to_string())?),
            records: Vec::new(),
            origin: node,
        };
        let mut tx = protocol::Transaction {
            database,
            origin: node,
            sequence,
            id: rand::random(),
            millis: clock.millis,
            logical: clock.logical,
            schema: protocol::schema_digest(&schema_bytes),
            payload_digest: [0; 32],
            payload: murmur_replication::encode(&content).map_err(|e| e.to_string())?,
            signature: Vec::new(),
        };
        tx.sign(signkey).map_err(|e| e.to_string())?;
        puts.push(spool::Mutation::put(
            murmur_replication::retention::time_key(node, sequence),
            murmur_replication::encode(&murmur_replication::retention::now_millis())
                .map_err(|e| e.to_string())?,
        ));
        puts.push(spool::Mutation::put(
            protocol::log_key(node, sequence),
            murmur_replication::encode(&tx).map_err(|e| e.to_string())?,
        ));
        puts.push(spool::Mutation::put(
            protocol::receipt_key(node, sequence),
            tx.digest().map_err(|e| e.to_string())?.to_vec(),
        ));
        head.applied = sequence;
        head.observed = head.observed.max(sequence);
        head.retained_through = sequence;
    }
    let last = head.retained_through;
    puts.push(spool::Mutation::put(
        seq_key,
        murmur_replication::encode(&last).map_err(|e| e.to_string())?,
    ));
    puts.push(spool::Mutation::put(
        protocol::progress_key(node),
        murmur_replication::encode(&last).map_err(|e| e.to_string())?,
    ));
    puts.push(spool::Mutation::put(
        head_key,
        murmur_replication::encode(&head).map_err(|e| e.to_string())?,
    ));
    Ok(puts)
}

/// Node-level replication backend: our Spool store plus our SQLite database.
/// Cloned into the engine's background workers; see the module docs for the
/// locking split between raw key/value (lock-free) and logical ops (`CORE`).
pub struct ReplBackend {
    store: LogicalStore,
    db_path: String,
    vfs: Option<String>,
    spool_path: std::path::PathBuf,
    db: murmur_replication::NodeId,
    node: murmur_replication::NodeId,
    last_reclaim_ms: AtomicU64,
}

impl ReplBackend {
    pub fn new(
        store: LogicalStore,
        db_path: String,
        vfs: Option<String>,
        spool_path: std::path::PathBuf,
        db: murmur_replication::NodeId,
        node: murmur_replication::NodeId,
    ) -> Self {
        Self {
            store,
            db_path,
            vfs,
            spool_path,
            db,
            node,
            last_reclaim_ms: AtomicU64::new(0),
        }
    }
}

impl murmur_replication::Backend for ReplBackend {
    fn identity(&self) -> (murmur_replication::NodeId, murmur_replication::NodeId) {
        // Read once at start under CORE; infallible forever after (this
        // method cannot fail, and panicking across engine threads — or,
        // worse, the start path under FFI — is not an option).
        (self.db, self.node)
    }

    fn get(&self, key: &[u8]) -> murmur_replication::Result<Option<Vec<u8>>> {
        self.store
            .get(key)
            .map_err(|e| murmur_replication::Error::Storage(e.to_string()))
    }

    fn keys(&self, prefix: &[u8]) -> murmur_replication::Result<Vec<Vec<u8>>> {
        Ok(self.store.keys_with_prefix(prefix))
    }

    fn commit(&self, records: Vec<murmur_replication::Record>) -> murmur_replication::Result<()> {
        let mutations: Vec<spool::Mutation> = records
            .into_iter()
            .map(|r| match r.value {
                Some(v) => spool::Mutation::put(r.key, v),
                None => spool::Mutation::delete(r.key),
            })
            .collect();
        self.store
            .commit(&mutations, spool::Durability::Sync)
            .map_err(|e| murmur_replication::Error::Storage(e.to_string()))
    }

    fn import_bridge(
        &self,
        _header: murmur_replication::bridge::Header,
        _payload: Vec<u8>,
    ) -> murmur_replication::Result<bool> {
        Ok(false)
    }

    fn release_ownership(
        &self,
        _table: String,
        _key: Vec<u8>,
        _column: Option<String>,
    ) -> murmur_replication::Result<bool> {
        Ok(false)
    }

    fn publish(&self, _records: Vec<murmur_replication::Record>) -> murmur_replication::Result<()> {
        // Only the files subsystem publishes; we start with files disabled.
        Err(murmur_replication::Error::Unavailable(
            "auxiliary publish unsupported".into(),
        ))
    }

    fn snapshot(&self) -> murmur_replication::Result<Vec<u8>> {
        let err = |e: String| murmur_replication::Error::Storage(e);
        let core = crate::state::lock();
        crate::outbox::drain_all(&core, &self.store, &self.db_path, self.vfs.as_deref())
            .map_err(err)?;
        let schema = crate::schema_map::load_schema(&self.store).map_err(err)?;
        let state = crate::sync::export_state_inner(&self.store, &schema).map_err(err)?;
        let sqlite = murmur_crdt::model::encode(&state)
            .map_err(|e| murmur_replication::Error::Storage(e.to_string()))?;
        let content = murmur_replication::Content {
            sqlite: Some(sqlite),
            records: Vec::new(),
            origin: self.node,
        };
        murmur_replication::encode(&content)
            .map_err(|e| murmur_replication::Error::Storage(e.to_string()))
    }

    fn snapshot_with_progress(
        &self,
    ) -> murmur_replication::Result<(Vec<u8>, Vec<murmur_replication::protocol::Progress>)> {
        Ok((self.snapshot()?, self.progress()?))
    }

    fn retention_checkpoint(&self) -> murmur_replication::Result<()> {
        // Nothing is ever pruned, so there is no coverage to checkpoint.
        Ok(())
    }

    fn prune_history(
        &self,
        _origin: murmur_replication::NodeId,
        _through: u64,
    ) -> murmur_replication::Result<u64> {
        // Infinite retention: the log is the catch-up mechanism (durable
        // snapshots are future work), so history is never removed.
        Ok(0)
    }

    fn reclaim_retention(&self) -> murmur_replication::Result<()> {
        // Compaction costs ~1s; the engine may call this every second.
        let now = murmur_replication::retention::now_millis();
        let last = self.last_reclaim_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last) < 60_000 {
            return Ok(());
        }
        if self
            .last_reclaim_ms
            .compare_exchange_weak(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return Ok(());
        }
        self.store
            .reclaim()
            .map_err(|e| murmur_replication::Error::Storage(e.to_string()))
    }

    fn schema(&self) -> murmur_replication::Result<Vec<u8>> {
        let _guard = crate::state::lock();
        normalized_schema(&self.store).map_err(murmur_replication::Error::Storage)
    }

    fn observe(&self, millis: u64, logical: u32) -> murmur_replication::Result<()> {
        let err = |e: String| murmur_replication::Error::Storage(e);
        let mut core = crate::state::lock();
        let entry = core
            .spools
            .get_mut(&self.spool_path)
            .ok_or_else(|| murmur_replication::Error::Unavailable("spool released".into()))?;
        let seen = Version {
            millis,
            logical,
            node: [0xff; 16],
        };
        let clock = crate::outbox::read_clock(&entry.store).map_err(err)?;
        if seen > clock.max(entry.last_tick) {
            entry.last_tick = seen;
            let bytes = murmur_crdt::model::encode(&seen)
                .map_err(|e| murmur_replication::Error::Storage(e.to_string()))?;
            entry
                .store
                .commit(
                    &[spool::Mutation::put(
                        crate::outbox::CLOCK_KEY.to_vec(),
                        bytes,
                    )],
                    spool::Durability::Sync,
                )
                .map_err(|e| murmur_replication::Error::Storage(e.to_string()))?;
        }
        Ok(())
    }

    fn accept_schema(&self, _schema: Vec<u8>) -> murmur_replication::Result<bool> {
        // Unreachable: we start with `accept_remote_schema: false`, so the
        // engine records manifests without adopting. Tables are adopted
        // locally via `murmur_enable_sync`; scope filtering in apply keeps
        // divergent-scope peers flowing.
        Ok(false)
    }

    fn apply(
        &self,
        payloads: Vec<Vec<u8>>,
        records: Vec<murmur_replication::Record>,
    ) -> murmur_replication::Result<bool> {
        let err = |e: String| murmur_replication::Error::Storage(e);
        let core = crate::state::lock();
        // Local capture first: remote effects merge only onto durable,
        // visible, logged local state.
        crate::outbox::drain_all(&core, &self.store, &self.db_path, self.vfs.as_deref())
            .map_err(err)?;
        let mut auxiliary = Vec::new();
        for payload in &payloads {
            let content: murmur_replication::Content = murmur_replication::decode(payload)
                .map_err(|e| murmur_replication::Error::Storage(e.to_string()))?;
            for record in &content.records {
                if !record.key.starts_with(b"07/replication/") {
                    return Err(murmur_replication::Error::Storage(
                        "auxiliary records unsupported".into(),
                    ));
                }
            }
            if let Some(bytes) = &content.sqlite {
                // Fresh baseline per payload: each merge sees the last.
                crate::sync::apply_batch(&self.store, &self.db_path, self.vfs.as_deref(), bytes)
                    .map_err(err)?;
            }
            auxiliary.extend(content.records);
        }
        for record in &records {
            if !record.key.starts_with(b"07/replication/") {
                return Err(murmur_replication::Error::Storage(
                    "auxiliary records unsupported".into(),
                ));
            }
        }
        auxiliary.extend(records);
        if !auxiliary.is_empty() {
            self.commit(auxiliary)?;
        }
        Ok(true)
    }
}

fn unhex(text: &str) -> Result<Vec<u8>, String> {
    if text.len() % 2 != 0 {
        return Err("murmur: malformed hex".into());
    }
    (0..text.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&text[i..i + 2], 16).map_err(|_| "murmur: malformed hex".to_string())
        })
        .collect()
}

fn hex_id<const N: usize>(text: &str, what: &str) -> Result<[u8; N], String> {
    let bytes = unhex(text)?;
    bytes
        .try_into()
        .map_err(|_| format!("murmur: {what} must be {} hex chars", N * 2))
}

fn hex_array(value: &serde_json::Value, field: &str) -> Result<Vec<Vec<u8>>, String> {
    let arr = value
        .get(field)
        .and_then(|v| v.as_array())
        .ok_or_else(|| format!("murmur: repl credentials need '{field}' array"))?;
    arr.iter()
        .map(|v| {
            v.as_str()
                .ok_or_else(|| format!("murmur: repl credentials '{field}' must be hex strings"))
                .and_then(unhex)
        })
        .collect()
}

/// Start QUIC replication for the calling connection's spool. Credentials
/// JSON: `{"chain":[hex..], "key":hex, "roots":[hex..]}` (DER certs/keys as
/// hex); peers JSON: `{"<nodehex>":"<pubkeyhex>"}`. The node's own identity
/// and signing key load from Spool (single source of truth, minted at open)
/// and self-register in `trusted_origins`. Optional intervals JSON
/// `{"anti_entropy_ms":N,"incoming_ms":N}` tunes the engine timers (tests).
pub fn repl_start(
    core: &mut crate::state::Core,
    db_key: usize,
    bind: &str,
    creds_json: &str,
    peers_json: &str,
    intervals_json: Option<&str>,
) -> Result<String, String> {
    let (store, db_path, vfs, spool_path) = {
        let state = core
            .conns
            .get(&db_key)
            .ok_or("murmur: call murmur_open first")?;
        (
            state.spool.clone(),
            state.db_path.clone(),
            state.vfs.clone(),
            state.spool_path.clone(),
        )
    };
    if core
        .spools
        .get(&spool_path)
        .is_some_and(|e| e.repl.is_some())
    {
        return Err("murmur: replication already running (murmur_repl_stop first)".into());
    }
    let db = crate::sync::database_id(&store)?;
    let node = crate::outbox::node_id(&store)?;
    let signkey = ensure_signkey(&store)?;
    let creds: serde_json::Value =
        serde_json::from_str(creds_json).map_err(|e| format!("murmur: bad repl credentials: {e}"))?;
    let key_hex = creds
        .get("key")
        .and_then(|v| v.as_str())
        .ok_or("murmur: repl credentials need 'key' hex")?;
    let credentials = murmur_replication::Credentials {
        chain: hex_array(&creds, "chain")?,
        private_key: unhex(key_hex)?.into(),
        roots: hex_array(&creds, "roots")?,
    };
    let peers: serde_json::Value =
        serde_json::from_str(peers_json).map_err(|e| format!("murmur: bad repl peers: {e}"))?;
    let peers = peers
        .as_object()
        .ok_or("murmur: repl peers must be a JSON object")?;
    let mut trusted = std::collections::BTreeMap::new();
    trusted.insert(
        node,
        murmur_replication::protocol::public_key(&signkey),
    );
    for (id_hex, pub_hex) in peers {
        let id: [u8; 16] = hex_id(id_hex, "peer node id")?;
        let pubkey: [u8; 32] = hex_id(
            pub_hex
                .as_str()
                .ok_or("murmur: repl peer pubkeys must be hex strings")?,
            "peer pubkey",
        )?;
        trusted.insert(id, pubkey);
    }
    let addr: std::net::SocketAddr =
        bind.parse().map_err(|e| format!("murmur: bad repl bind address: {e}"))?;
    let mut config = murmur_replication::Config::new(addr, credentials, signkey);
    config.trusted_origins = trusted;
    // Record-only schema flow: manifests ACK without adopting (tables are
    // adopted locally; scope filtering keeps divergent scopes flowing).
    config.accept_remote_schema = false;
    if let Some(json) = intervals_json {
        let iv: serde_json::Value =
            serde_json::from_str(json).map_err(|e| format!("murmur: bad repl intervals: {e}"))?;
        if let Some(ms) = iv.get("anti_entropy_ms").and_then(|v| v.as_u64()) {
            config.anti_entropy_interval = std::time::Duration::from_millis(ms);
        }
        if let Some(ms) = iv.get("incoming_ms").and_then(|v| v.as_u64()) {
            config.incoming_interval = std::time::Duration::from_millis(ms);
        }
    }
    let backend = std::sync::Arc::new(ReplBackend::new(
        store,
        db_path,
        vfs,
        spool_path.clone(),
        db,
        node,
    ));
    // Synchronous start-path backend calls (bind_signer, migrate_peers) are
    // raw key/value, safe under CORE; the worker thread only calls back
    // after peers and traffic exist.
    let handle =
        murmur_replication::start(config, backend).map_err(|e| format!("murmur: repl start failed: {e}"))?;
    let bound = handle
        .status()
        .address
        .map(|a| a.to_string())
        .unwrap_or_default();
    core.spools
        .get_mut(&spool_path)
        .ok_or("murmur: spool released")?
        .repl = Some(handle);
    Ok(bound)
}

/// Take the live handle out of the entry without stopping it: the caller
/// must drop `CORE` before `close()` (it joins the replication thread,
/// which may be inside a backend call holding `CORE`).
pub fn take_repl(
    core: &mut crate::state::Core,
    db_key: usize,
) -> Result<(Option<murmur_replication::Handle>, std::path::PathBuf), String> {
    let spool_path = core
        .conns
        .get(&db_key)
        .ok_or("murmur: call murmur_open first")?
        .spool_path
        .clone();
    let handle = core
        .spools
        .get_mut(&spool_path)
        .ok_or("murmur: spool released")?
        .repl
        .take();
    Ok((handle, spool_path))
}

fn live_handle(
    core: &crate::state::Core,
    db_key: usize,
) -> Result<murmur_replication::Handle, String> {
    let spool_path = core
        .conns
        .get(&db_key)
        .ok_or("murmur: call murmur_open first")?
        .spool_path
        .clone();
    core.spools
        .get(&spool_path)
        .and_then(|e| e.repl.clone())
        .ok_or("murmur: replication not running".into())
}

pub fn repl_add_peer(core: &crate::state::Core, db_key: usize, addr: &str) -> Result<(), String> {
    let handle = live_handle(core, db_key)?;
    let addr: std::net::SocketAddr =
        addr.parse().map_err(|e| format!("murmur: bad peer address: {e}"))?;
    handle
        .add_peer(addr)
        .map_err(|e| format!("murmur: repl add_peer failed: {e}"))
}

pub fn repl_status(core: &crate::state::Core, db_key: usize) -> Result<String, String> {
    let handle = live_handle(core, db_key)?;
    let status = handle.status();
    Ok(serde_json::json!({
        "address": status.address.map(|a| a.to_string()),
        "peers": status.peers,
        "staged": status.staged_transactions,
        "applied": status.applied_transactions,
        "last_error": status.last_error,
    })
    .to_string())
}

pub fn repl_sync(core: &crate::state::Core, db_key: usize) -> Result<(), String> {
    let handle = live_handle(core, db_key)?;
    handle
        .force_sync()
        .map_err(|e| format!("murmur: repl sync failed: {e}"))
}

/// Per-origin replication progress (our `tracked_peers`): works without
/// transport running, since local logging populates it from birth. JSON
/// array of `{origin, applied, observed, retained_from, retained_through}`
/// with hex origins, sorted.
pub fn repl_progress(core: &crate::state::Core, db_key: usize) -> Result<String, String> {
    let store = core
        .conns
        .get(&db_key)
        .ok_or("murmur: call murmur_open first")?
        .spool
        .clone();
    let mut origins = std::collections::BTreeMap::<[u8; 16], murmur_replication::protocol::Progress>::new();
    for key in store.keys_with_prefix(b"07/replication/retained/") {
        if let Some(bytes) = store.get(&key).map_err(|e| e.to_string())? {
            let p: murmur_replication::protocol::Progress =
                murmur_replication::decode(&bytes).map_err(|e| e.to_string())?;
            origins.insert(p.origin, p);
        }
    }
    let origin_of = |key: &[u8]| -> Result<[u8; 16], String> {
        let s = std::str::from_utf8(key).map_err(|_| "murmur: corrupt progress key".to_string())?;
        let hex = s.rsplit('/').next().unwrap_or("");
        hex_id(hex, "progress origin")
    };
    for key in store.keys_with_prefix(b"07/replication/applied/") {
        let Some(bytes) = store.get(&key).map_err(|e| e.to_string())? else {
            continue;
        };
        let origin = origin_of(&key)?;
        let applied: u64 =
            murmur_replication::decode(&bytes).map_err(|e| e.to_string())?;
        origins
            .entry(origin)
            .or_insert(murmur_replication::protocol::Progress {
                origin,
                applied: 0,
                observed: 0,
                retained_from: 0,
                retained_through: 0,
            })
            .applied = applied;
    }
    for key in store.keys_with_prefix(b"07/replication/observed/") {
        let Some(bytes) = store.get(&key).map_err(|e| e.to_string())? else {
            continue;
        };
        let origin = origin_of(&key)?;
        let observed: u64 =
            murmur_replication::decode(&bytes).map_err(|e| e.to_string())?;
        origins
            .entry(origin)
            .or_insert(murmur_replication::protocol::Progress {
                origin,
                applied: 0,
                observed: 0,
                retained_from: 0,
                retained_through: 0,
            })
            .observed = observed;
    }
    let list: Vec<serde_json::Value> = origins
        .values()
        .map(|p| {
            serde_json::json!({
                "origin": p.origin.iter().map(|b| format!("{b:02x}")).collect::<String>(),
                "applied": p.applied,
                "observed": p.observed.max(p.applied),
                "retained_from": p.retained_from,
                "retained_through": p.retained_through,
            })
        })
        .collect();
    Ok(serde_json::Value::Array(list).to_string())
}

/// Latest origin-log sequence for `origin` (0 when nothing is logged).
/// Pure read: the caller drains first (outbox rows become log entries only
/// on drain).
pub fn log_seq(store: &LogicalStore, origin_hex: &str) -> Result<u64, String> {
    let origin: [u8; 16] = hex_id(origin_hex, "log origin")?;
    Ok(store
        .get(&protocol::sequence_key(origin))
        .map_err(|e| e.to_string())?
        .map(|b| murmur_replication::decode::<u64>(&b))
        .transpose()
        .map_err(|e| e.to_string())?
        .unwrap_or(0))
}

/// One origin-log entry decoded to its SQLite fragment. No signature
/// verification: this is the node's own durable log, not wire input
/// (transport verifies on receipt).
fn log_fragment(
    store: &LogicalStore,
    origin: [u8; 16],
    seq: u64,
) -> Result<ReplicaState, String> {
    let bytes = store
        .get(&protocol::log_key(origin, seq))
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("murmur: log gap at {seq} (pruned or corrupt)"))?;
    let tx: protocol::Transaction =
        murmur_replication::decode(&bytes).map_err(|e| e.to_string())?;
    let content: murmur_replication::Content =
        murmur_replication::decode(&tx.payload).map_err(|e| e.to_string())?;
    let bytes = content
        .sqlite
        .ok_or("murmur: log entry without sqlite payload")?;
    murmur_crdt::model::decode(&bytes).map_err(|e| e.to_string())
}

/// Total SQLite rows logged for `origin` in (`from`..=`to`]. Pure read.
/// Split multi-sequence batches count every row exactly once.
pub fn log_rows(
    store: &LogicalStore,
    origin_hex: &str,
    from: i64,
    to: i64,
) -> Result<u64, String> {
    if from < 0 || to < 0 {
        return Err("murmur: log sequence bounds must be >= 0".into());
    }
    if from >= to {
        return Ok(0);
    }
    let origin: [u8; 16] = hex_id(origin_hex, "log origin")?;
    let mut rows = 0u64;
    for seq in (from as u64 + 1)..=(to as u64) {
        rows += log_fragment(store, origin, seq)?.rows.len() as u64;
    }
    Ok(rows)
}

/// Export `origin`'s log entries in (`from`..=latest] as one merged batch
/// for `murmur_apply`. Drains first (like `murmur_export`): undrained
/// outbox rows would otherwise be silently stale. Empty ranges encode a
/// valid no-op batch, so callers can export unconditionally per round.
pub fn log_export(
    core: &crate::state::Core,
    store: &LogicalStore,
    db_path: &str,
    vfs: Option<&str>,
    origin_hex: &str,
    from: i64,
) -> Result<Vec<u8>, String> {
    if from < 0 {
        return Err("murmur: log from_seq must be >= 0".into());
    }
    crate::outbox::drain_all(core, store, db_path, vfs)?;
    let origin: [u8; 16] = hex_id(origin_hex, "log origin")?;
    let latest = log_seq(store, origin_hex)?;
    let mut rows = Vec::new();
    let mut tombstones = Vec::new();
    let mut clock = crate::outbox::read_clock(store)?;
    let mut database = None;
    if (from as u64) < latest {
        for seq in (from as u64 + 1)..=latest {
            let frag = log_fragment(store, origin, seq)?;
            database.get_or_insert(frag.database_id);
            clock = clock.max(frag.clock);
            rows.extend(frag.rows);
            tombstones.extend(frag.tombstones);
        }
    }
    // Current local schema is a superset of every fragment's columns
    // (adoptions only add tables/columns), so merged rows always resolve.
    let schema = normalize_schema(&crate::schema_map::load_schema(store)?);
    let database_id = match database {
        Some(id) => id,
        None => crate::sync::database_id(store)?,
    };
    murmur_crdt::model::encode(&ReplicaState {
        database_id,
        schema,
        rows,
        tombstones,
        clock,
    })
    .map_err(|e| e.to_string())
}

/// This node's replication public key (hex): the value peers put in their
/// `trusted_origins` for us. Public by design; the seed never leaves Spool.
pub fn repl_pubkey(core: &crate::state::Core, db_key: usize) -> Result<String, String> {
    let store = core
        .conns
        .get(&db_key)
        .ok_or("murmur: call murmur_open first")?
        .spool
        .clone();
    let signkey = read_signkey(&store)?;
    Ok(murmur_replication::protocol::public_key(&signkey)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}
