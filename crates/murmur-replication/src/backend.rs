//! CRR-backed implementation of the reused `murmur_tx::Backend` trait.
//!
//! The archived transaction-protocol modules (snapshot, retention, queue,
//! verification, bridge, files, policies) are generic over `Backend`: a
//! kv record store plus progress/commit/snapshot/apply/schema operations.
//! This adapter implements that trait over the local CRR/RocksDB store:
//!
//! - `get`/`keys`/`commit` run on the `murmur_kv` table through the
//!   serialized SQLite worker (`Store`), so every reused module shares
//!   one durable record space with the CRR engine.
//! - `snapshot`/`apply` translate between transaction payloads and CRSB
//!   batch bytes (`Store::export`, `crsql_ingest_blob`).
//! - `observe` keeps a max-merged hybrid clock in `murmur_kv`.
//! - `schema`/`accept_schema` compare canonical CRR bytes; adoption
//!   arrives with WP-A. `import_bridge`/`release_ownership` (WP-B),
//!   durable snapshots and history pruning (WP-A) are honest stubs.
//!
//! All methods funnel through `Worker::call`: exactly one storage round
//! trip per call, never nested (a nested call would deadlock the worker).

use crate::{
    config::Config,
    protocol::Cursor,
    storage::{Store, Worker},
    *,
};
use murmur_tx::protocol::Progress;

pub(crate) struct CrrBackend {
    worker: Arc<Worker>,
    group: murmur_tx::NodeId,
    node: murmur_tx::NodeId,
    /// Export configuration; `None` while replication is stopped (kv,
    /// progress, clock, and wait paths stay available, snapshots fail).
    config: Option<Config>,
}

/// Logical-state snapshot: schema digest, source db version, and the full
/// CRSB export as one segment per `Store::export` batch. Postcard-encoded;
/// chunked for transfer by the snapshot module (WP-A).
#[derive(Serialize, Deserialize)]
struct CrrSnapshot {
    schema: [u8; 32],
    version: i64,
    segments: Vec<Vec<u8>>,
}

pub(crate) fn tx_err(e: Error) -> murmur_tx::Error {
    match e {
        Error::Invalid(s) => murmur_tx::Error::Invalid(s),
        Error::Protocol(s) => murmur_tx::Error::Protocol(s),
        Error::Auth(s) => murmur_tx::Error::Auth(s),
        Error::Unavailable(s) => murmur_tx::Error::Unavailable(s),
        Error::Overloaded => murmur_tx::Error::Overloaded,
        Error::Limit(_) => murmur_tx::Error::Overloaded,
        Error::Sql(e) => murmur_tx::Error::Storage(e.to_string()),
        Error::Io(e) => murmur_tx::Error::Io(e),
    }
}

impl CrrBackend {
    pub fn new(worker: Arc<Worker>, group: NodeId, node: NodeId, config: Config) -> Self {
        Self {
            worker,
            group,
            node,
            config: Some(config),
        }
    }

    /// Backend over stored identities while replication is stopped.
    /// Progress/clock/kv reads work; snapshots report unavailable.
    pub fn new_offline(worker: Arc<Worker>, group: NodeId, node: NodeId) -> Self {
        Self {
            worker,
            group,
            node,
            config: None,
        }
    }

    /// Export every CRR change from version 0 as bounded CRSB segments.
    /// Runs inside one worker job; the caller wraps it in snapshot bytes.
    fn export_full(store: &mut Store, cfg: &Config) -> Result<CrrSnapshot> {
        let schema = protocol::digest(&store.schema_bytes()?);
        let mut cursor = Cursor::default();
        let mut segments = Vec::new();
        let mut total = 0usize;
        loop {
            let (next, blob) = store.export(cursor, cfg)?;
            total += blob.len();
            if total > cfg.staging_bytes {
                return Err(Error::Overloaded);
            }
            segments.push(blob);
            if next.through.is_none() && next.after.is_none() {
                return Ok(CrrSnapshot {
                    schema,
                    version: next.from,
                    segments,
                });
            }
            if segments.len() > 1 << 20 {
                return Err(Error::Overloaded);
            }
            cursor = next;
        }
    }
}

impl murmur_tx::Backend for CrrBackend {
    fn identity(&self) -> (murmur_tx::NodeId, murmur_tx::NodeId) {
        (self.group, self.node)
    }

    fn get(&self, key: &[u8]) -> murmur_tx::Result<Option<Vec<u8>>> {
        let key = key.to_vec();
        self.worker
            .call(move |s| s.kv_get(&key))
            .map_err(tx_err)
    }

    fn get_many(&self, keys: &[Vec<u8>]) -> murmur_tx::Result<Vec<Option<Vec<u8>>>> {
        let keys = keys.to_vec();
        self.worker
            .call(move |s| s.kv_get_many(&keys))
            .map_err(tx_err)
    }

    fn get_many_bounded(
        &self,
        keys: &[Vec<u8>],
        max_bytes: usize,
    ) -> murmur_tx::Result<Vec<Option<Vec<u8>>>> {
        // Same truncation rule as the archived default, one round trip.
        let all = self.get_many(keys)?;
        let mut values = Vec::new();
        let mut bytes = 0usize;
        for value in all {
            let size = value.as_ref().map_or(0, Vec::len);
            if !values.is_empty() && bytes.saturating_add(size) > max_bytes {
                break;
            }
            bytes = bytes.saturating_add(size);
            values.push(value);
            if bytes >= max_bytes {
                break;
            }
        }
        Ok(values)
    }

    fn keys(&self, prefix: &[u8]) -> murmur_tx::Result<Vec<Vec<u8>>> {
        let prefix = prefix.to_vec();
        self.worker
            .call(move |s| s.kv_keys(&prefix))
            .map_err(tx_err)
    }

    fn keys_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> murmur_tx::Result<Vec<Vec<u8>>> {
        let prefix = prefix.to_vec();
        let after = after.map(<[u8]>::to_vec);
        self.worker
            .call(move |s| s.kv_keys_page(&prefix, after.as_deref(), limit))
            .map_err(tx_err)
    }

    fn progress(&self) -> murmur_tx::Result<Vec<Progress>> {
        // Single serialized read, per the trait's guidance, plus the live
        // local head: local commits never pass through Backend::commit
        // (they are application SQL), so the stored head alone would lag
        // the true db version and read-your-write waits would stall.
        // Best-effort: bare stores lack cr-sqlite and skip the merge.
        let node = self.node;
        self.worker
            .call(move |s| {
                let mut heads = s.tx_progress()?;
                let live: Option<u64> = s
                    .db
                    .query_row("SELECT crsql_db_version()", [], |r| {
                        r.get::<_, i64>(0)
                    })
                    .ok()
                    .and_then(|v| u64::try_from(v).ok());
                if let Some(v) = live {
                    match heads.iter_mut().find(|p| p.origin == node) {
                        Some(p) => {
                            p.applied = p.applied.max(v);
                            p.observed = p.observed.max(v);
                        }
                        None => heads.push(Progress {
                            origin: node,
                            applied: v,
                            observed: v,
                            retained_from: 0,
                            retained_through: 0,
                        }),
                    }
                }
                Ok(heads)
            })
            .map_err(tx_err)
    }

    fn commit(&self, records: Vec<murmur_tx::Record>) -> murmur_tx::Result<()> {
        self.worker
            .call(move |s| {
                s.kv_commit(records)?;
                s.flush()
            })
            .map_err(tx_err)
    }

    fn import_bridge(
        &self,
        _header: murmur_tx::bridge::Header,
        _payload: Vec<u8>,
    ) -> murmur_tx::Result<bool> {
        // TODO(WP-B): high-low ownership import over CRR state.
        Ok(false)
    }

    fn release_ownership(
        &self,
        _table: String,
        _key: Vec<u8>,
        _column: Option<String>,
    ) -> murmur_tx::Result<bool> {
        // TODO(WP-B): high-low ownership release over CRR state.
        Ok(false)
    }

    fn publish(&self, records: Vec<murmur_tx::Record>) -> murmur_tx::Result<()> {
        self.worker
            .call(move |s| {
                s.kv_commit(records)?;
                s.flush()
            })
            .map_err(tx_err)
    }

    fn snapshot(&self) -> murmur_tx::Result<Vec<u8>> {
        let cfg = self.config.clone().ok_or_else(|| {
            murmur_tx::Error::Unavailable("replication is stopped".into())
        })?;
        self.worker
            .call(move |s| {
                let snap = Self::export_full(s, &cfg)?;
                Ok(encode(&snap)?)
            })
            .map_err(tx_err)
    }

    fn snapshot_with_progress(
        &self,
    ) -> murmur_tx::Result<(Vec<u8>, Vec<Progress>)> {
        // One worker round trip: export loop and progress share the job
        // (never call back into the worker from inside it).
        let cfg = self.config.clone().ok_or_else(|| {
            murmur_tx::Error::Unavailable("replication is stopped".into())
        })?;
        self.worker
            .call(move |s| {
                let snap = Self::export_full(s, &cfg)?;
                let bytes = encode(&snap)?;
                let progress = s.tx_progress()?;
                Ok((bytes, progress))
            })
            .map_err(tx_err)
    }

    fn retention_checkpoint(&self) -> murmur_tx::Result<()> {
        // The CRR store is always a coherent image at commit boundaries;
        // checkpointing is the persistence barrier. WP-A adds clock floors.
        self.worker.call(|s| s.flush()).map_err(tx_err)
    }

    fn prune_history(&self, _origin: murmur_tx::NodeId, _through: u64) -> murmur_tx::Result<u64> {
        // TODO(WP-A): snapshot-coverage recheck + cr-sqlite clock/tombstone GC.
        Ok(0)
    }

    fn reclaim_retention(&self) -> murmur_tx::Result<()> {
        // Light pass until WP-A wires physical reclamation to retention GC.
        self.worker.call(|s| s.flush()).map_err(tx_err)
    }

    fn schema(&self) -> murmur_tx::Result<Vec<u8>> {
        self.worker.call(|s| s.schema_bytes()).map_err(tx_err)
    }

    fn apply(
        &self,
        payloads: Vec<Vec<u8>>,
        records: Vec<murmur_tx::Record>,
    ) -> murmur_tx::Result<bool> {
        // Payloads are CRSB batch bytes; records land in murmur_kv in the
        // same SQLite transaction, then one persistence barrier. Empty
        // payloads (control-only apply) skip ingestion; cr-sqlite makes
        // duplicate batches idempotent. The CRR store has no uncollected
        // local-capture state, so durable apply always reports true.
        self.worker
            .call(move |s| {
                let tx = s.db.transaction_with_behavior(
                    rusqlite::TransactionBehavior::Immediate,
                )?;
                for p in &payloads {
                    tx.query_row("SELECT crsql_ingest_blob(?)", [p], |_| Ok(()))?;
                }
                Store::kv_apply_tx(&tx, &records)?;
                tx.commit()?;
                s.flush()?;
                Ok(true)
            })
            .map_err(tx_err)
    }

    fn observe(&self, millis: u64, logical: u32) -> murmur_tx::Result<()> {
        self.worker
            .call(move |s| {
                s.clock_observe(millis, logical)?;
                s.flush()
            })
            .map_err(tx_err)?;
        Ok(())
    }

    fn accept_schema(&self, schema: Vec<u8>) -> murmur_tx::Result<bool> {
        // Re-decide inside one worker job: local DDL may have raced the
        // check. Additive applies; anything else declines (the caller
        // only records the manifest on true).
        self.worker
            .call(move |s| {
                let params = crate::schema::params_from(&s.db)?;
                if !params.accept_remote_schema {
                    return Ok(false);
                }
                let local = s.manifest_schema_bytes()?;
                match crate::schema::diff(&local, &schema)
                    .map_err(crate::wait::backend_err)?
                {
                    crate::schema::Diff::Additive(additive) => {
                        s.apply_schema_additive(&additive)?;
                        Ok(true)
                    }
                    crate::schema::Diff::Incompatible(_) => Ok(false),
                }
            })
            .map_err(tx_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use murmur_tx::Backend as _;
    use murmur_tx::protocol::{observed_key, progress_key, retained_key};

    fn dummy_config() -> Config {
        Config {
            group: [1; 16],
            node: [2; 16],
            site: [3; 16],
            bind: "127.0.0.1:0".parse().unwrap(),
            advertise: None,
            credentials: crate::config::Credentials {
                chain: Vec::new(),
                private_key: zeroize::Zeroizing::new(Vec::new()),
                roots: Vec::new(),
            },
            allowed_peers: BTreeSet::new(),
            allowed_networks: Vec::new(),
            bootstrap: Vec::new(),
            excluded: BTreeSet::new(),
            incarnation: 0,
            sessions: 32,
            queue_bytes: 1 << 20,
            batch_bytes: 1 << 20,
            max_batch_bytes: 1 << 20,
            staging_bytes: 4 << 20,
            staging_timeout_ms: 60000,
            poll_ms: 100,
            anti_entropy_ms: 10000,
            gossip_fanout: 3,
        }
    }

    /// Backend over a bare store (default VFS, no cr-sqlite): kv, clock,
    /// and progress paths only. The TempDir must outlive the backend.
    fn setup() -> (tempfile::TempDir, CrrBackend) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db").to_str().unwrap().to_owned();
        let worker = Worker::open_test(path).unwrap();
        let backend = CrrBackend::new(worker, [1; 16], [2; 16], dummy_config());
        (dir, backend)
    }

    #[test]
    fn kv_put_get_delete_roundtrip() {
        let (_dir, b) = setup();
        assert_eq!(b.identity(), ([1; 16], [2; 16]));
        assert_eq!(b.get(b"k1").unwrap(), None);
        b.commit(vec![murmur_tx::Record::put(b"k1".to_vec(), vec![9])])
            .unwrap();
        assert_eq!(b.get(b"k1").unwrap(), Some(vec![9]));
        b.commit(vec![murmur_tx::Record::delete(b"k1".to_vec())])
            .unwrap();
        assert_eq!(b.get(b"k1").unwrap(), None);
    }

    #[test]
    fn keys_prefix_scan_is_ordered() {
        let (_dir, b) = setup();
        b.publish(vec![
            murmur_tx::Record::put(b"07/a/2".to_vec(), vec![2]),
            murmur_tx::Record::put(b"07/a/1".to_vec(), vec![1]),
            murmur_tx::Record::put(b"07/b/1".to_vec(), vec![3]),
        ])
        .unwrap();
        assert_eq!(
            b.keys(b"07/a/").unwrap(),
            vec![b"07/a/1".to_vec(), b"07/a/2".to_vec()]
        );
        assert_eq!(b.keys(b"07/b/").unwrap(), vec![b"07/b/1".to_vec()]);
        assert_eq!(b.keys(b"07/none/").unwrap(), Vec::<Vec<u8>>::new());
        assert_eq!(b.keys(b"").unwrap().len(), 3);
    }

    #[test]
    fn keys_page_paginates_with_exclusive_cursor() {
        let (_dir, b) = setup();
        b.commit(
            (0..5)
                .map(|i| murmur_tx::Record::put(format!("p/{i}").into_bytes(), vec![i]))
                .collect(),
        )
        .unwrap();
        let page1 = b.keys_page(b"p/", None, 2).unwrap();
        assert_eq!(page1, vec![b"p/0".to_vec(), b"p/1".to_vec()]);
        let page2 = b.keys_page(b"p/", Some(&page1[1]), 2).unwrap();
        assert_eq!(page2, vec![b"p/2".to_vec(), b"p/3".to_vec()]);
        let page3 = b.keys_page(b"p/", Some(&page2[1]), 2).unwrap();
        assert_eq!(page3, vec![b"p/4".to_vec()]);
        let page4 = b.keys_page(b"p/", Some(&page3[0]), 2).unwrap();
        assert!(page4.is_empty());
    }

    #[test]
    fn get_many_preserves_order_and_nones() {
        let (_dir, b) = setup();
        b.commit(vec![murmur_tx::Record::put(b"m1".to_vec(), vec![1])])
            .unwrap();
        assert_eq!(
            b.get_many(&[b"m0".to_vec(), b"m1".to_vec(), b"m2".to_vec()])
                .unwrap(),
            vec![None, Some(vec![1]), None]
        );
    }

    #[test]
    fn get_many_bounded_applies_first_value_progress_rule() {
        let (_dir, b) = setup();
        b.commit(vec![
            murmur_tx::Record::put(b"v1".to_vec(), vec![0; 10]),
            murmur_tx::Record::put(b"v2".to_vec(), vec![0; 10]),
        ])
        .unwrap();
        // Zero budget still returns the first value for progress.
        let got = b
            .get_many_bounded(&[b"v1".to_vec(), b"v2".to_vec()], 0)
            .unwrap();
        assert_eq!(got.len(), 1);
        // Budget for exactly one value stops before the second.
        let got = b
            .get_many_bounded(&[b"v1".to_vec(), b"v2".to_vec()], 10)
            .unwrap();
        assert_eq!(got.len(), 1);
        // Missing keys consume no budget.
        let got = b
            .get_many_bounded(&[b"nope".to_vec(), b"v1".to_vec()], 10)
            .unwrap();
        assert_eq!(got, vec![None, Some(vec![0; 10])]);
    }

    #[test]
    fn progress_collects_per_origin_heads() {
        let (_dir, b) = setup();
        let o1 = [7u8; 16];
        let o2 = [8u8; 16];
        b.commit(vec![
            murmur_tx::Record::put(
                retained_key(o1),
                murmur_tx::encode(&Progress {
                    origin: o1,
                    applied: 1,
                    observed: 1,
                    retained_from: 4,
                    retained_through: 9,
                })
                .unwrap(),
            ),
            murmur_tx::Record::put(progress_key(o1), murmur_tx::encode(&10u64).unwrap()),
            murmur_tx::Record::put(observed_key(o1), murmur_tx::encode(&3u64).unwrap()),
            murmur_tx::Record::put(observed_key(o2), murmur_tx::encode(&6u64).unwrap()),
        ])
        .unwrap();
        let mut heads = b.progress().unwrap();
        heads.sort_by_key(|p| p.origin);
        assert_eq!(heads.len(), 2);
        // Applied head wins; observed floors at applied.
        assert_eq!(heads[0].origin, o1);
        assert_eq!(heads[0].applied, 10);
        assert_eq!(heads[0].observed, 10);
        assert_eq!(heads[0].retained_from, 4);
        assert_eq!(heads[0].retained_through, 9);
        // Observed-only origin reports nothing applied.
        assert_eq!(heads[1].origin, o2);
        assert_eq!(heads[1].applied, 0);
        assert_eq!(heads[1].observed, 6);
        // Empty store reports no origins.
        let (_dir2, empty) = setup();
        assert!(empty.progress().unwrap().is_empty());
    }

    #[test]
    fn observe_clock_is_monotonic() {
        let (_dir, b) = setup();
        b.observe(100, 1).unwrap();
        let raw = b.get(crate::storage::CLOCK_KEY).unwrap().unwrap();
        assert_eq!(crate::decode::<(u64, u32)>(&raw).unwrap(), (100, 1));
        // Regression attempt keeps the stored maximum.
        b.observe(50, 9).unwrap();
        let raw = b.get(crate::storage::CLOCK_KEY).unwrap().unwrap();
        assert_eq!(crate::decode::<(u64, u32)>(&raw).unwrap(), (100, 1));
        // Logical clock advances within the same millis.
        b.observe(100, 2).unwrap();
        let raw = b.get(crate::storage::CLOCK_KEY).unwrap().unwrap();
        assert_eq!(crate::decode::<(u64, u32)>(&raw).unwrap(), (100, 2));
    }

    #[test]
    fn checkpoints_and_stubs_behave() {
        let (_dir, b) = setup();
        b.retention_checkpoint().unwrap();
        b.reclaim_retention().unwrap();
        assert_eq!(b.prune_history([9; 16], 100).unwrap(), 0);
    }

    #[test]
    fn apply_records_without_payloads_is_durable() {
        let (_dir, b) = setup();
        assert!(
            b.apply(
                Vec::new(),
                vec![murmur_tx::Record::put(b"r1".to_vec(), vec![5])]
            )
            .unwrap()
        );
        assert_eq!(b.get(b"r1").unwrap(), Some(vec![5]));
    }

    #[test]
    fn offline_backend_reads_but_cannot_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db").to_str().unwrap().to_owned();
        let worker = Worker::open_test(path).unwrap();
        let b = CrrBackend::new_offline(worker, [1; 16], [2; 16]);
        assert!(b.progress().unwrap().is_empty());
        assert!(matches!(
            b.snapshot(),
            Err(murmur_tx::Error::Unavailable(_))
        ));
        assert!(matches!(
            b.snapshot_with_progress(),
            Err(murmur_tx::Error::Unavailable(_))
        ));
    }

    #[test]
    fn schema_paths_require_crr_tables() {
        // Bare store has no cr-sqlite CRRs: schema-dependent calls fail
        // closed here; CRR coverage lives in the C++ integration tests.
        let (_dir, b) = setup();
        assert!(b.schema().is_err());
        assert!(b.accept_schema(vec![0; 8]).is_err());
        assert!(b.snapshot().is_err());
        assert!(b.snapshot_with_progress().is_err());
    }
}
