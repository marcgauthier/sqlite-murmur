//! `rusqlite` [`murmur_files::store::ChunkStore`] over worker-routed jobs
//! (WP-C lead wiring, slice 3).
//!
//! File payload bytes live in `murmur_file_chunks` / `murmur_file_staging`
//! rows (constraint 9); each trait method is one worker job, so access
//! serializes through the storage worker like every other SQLite use.
//! Semantics mirror [`murmur_files::store::memory::MemoryStore`] exactly:
//! dense-from-zero slices, first-touch creation millis, resume offsets,
//! sorted listings. `murmur_file_transfers` rows are created alongside
//! staged chunks (creation/update millis); their metadata columns stay
//! NULL until the fetch-service split fills them at publish time.
use crate::{backend::tx_err, storage::Worker};
use murmur_files::store::{ChunkStore, ObjectId, SLICE_BYTES};
use murmur_tx::{Error, Result};
use rusqlite::OptionalExtension;
use std::sync::{Arc, Mutex};

/// Worker-routed chunk store. Cloneable handle for `Hosts::open`.
#[derive(Clone)]
pub struct WorkerChunkStore {
    worker: Arc<Worker>,
}

impl WorkerChunkStore {
    /// Build over the storage worker. Tables are created by
    /// `Store::create_tables` at open.
    pub fn new(worker: Arc<Worker>) -> Self {
        Self { worker }
    }
}

fn now_millis() -> u64 {
    murmur_tx::retention::now_millis()
}

impl ChunkStore for WorkerChunkStore {
    fn put_chunk(
        &self,
        digest: &[u8; 32],
        generation: u64,
        idx: u64,
        bytes: &[u8],
    ) -> Result<()> {
        let worker = self.worker.clone();
        let (digest, bytes) = (*digest, bytes.to_vec());
        worker
            .call(move |s| {
                let now = now_millis() as i64;
                s.db.execute(
                    "INSERT INTO murmur_file_chunks(digest,generation,idx,bytes,created_ms) VALUES(?1,?2,?3,?4,?5)
                     ON CONFLICT(digest,generation,idx) DO UPDATE SET bytes=excluded.bytes",
                    rusqlite::params![digest.as_slice(), generation as i64, idx as i64, bytes, now],
                )?;
                s.flush()
            })
            .map_err(tx_err)?;
        Ok(())
    }

    fn read_stream(
        &self,
        digest: &[u8; 32],
        generation: u64,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>> {
        let worker = self.worker.clone();
        let digest = *digest;
        worker
            .call(move |s| {
                let mut out = Vec::new();
                let mut pos = offset;
                let end = offset.saturating_add(len as u64);
                while pos < end {
                    let idx = pos / SLICE_BYTES as u64;
                    let within = (pos % SLICE_BYTES as u64) as usize;
                    let row: Option<Vec<u8>> = s
                        .db
                        .query_row(
                            "SELECT bytes FROM murmur_file_chunks WHERE digest=?1 AND generation=?2 AND idx=?3",
                            rusqlite::params![digest.as_slice(), generation as i64, idx as i64],
                            |r| r.get(0),
                        )
                        .optional()?;
                    let Some(row) = row else {
                        break;
                    };
                    if within >= row.len() {
                        break;
                    }
                    let take = (row.len() - within).min((end - pos) as usize);
                    out.extend_from_slice(&row[within..within + take]);
                    pos += take as u64;
                }
                Ok(out)
            })
            .map_err(tx_err)
    }

    fn chunk_count(&self, digest: &[u8; 32], generation: u64) -> Result<u64> {
        let worker = self.worker.clone();
        let digest = *digest;
        worker
            .call(move |s| {
                let mut q = s.db.prepare(
                    "SELECT idx FROM murmur_file_chunks WHERE digest=?1 AND generation=?2 ORDER BY idx",
                )?;
                let rows = q.query_map(
                    rusqlite::params![digest.as_slice(), generation as i64],
                    |r| r.get::<_, i64>(0),
                )?;
                let mut count = 0u64;
                for row in rows {
                    if row? as u64 != count {
                        break;
                    }
                    count += 1;
                }
                Ok(count)
            })
            .map_err(tx_err)
    }

    fn delete_object(&self, digest: &[u8; 32], generation: u64) -> Result<()> {
        let worker = self.worker.clone();
        let digest = *digest;
        worker
            .call(move |s| {
                s.db.execute(
                    "DELETE FROM murmur_file_chunks WHERE digest=?1 AND generation=?2",
                    rusqlite::params![digest.as_slice(), generation as i64],
                )?;
                s.flush()
            })
            .map_err(tx_err)?;
        Ok(())
    }

    fn live_objects(&self) -> Result<Vec<(ObjectId, u64)>> {
        let worker = self.worker.clone();
        worker
            .call(|s| {
                let mut q = s.db.prepare(
                    "SELECT digest,generation,min(created_ms) FROM murmur_file_chunks GROUP BY digest,generation ORDER BY digest,generation",
                )?;
                let rows = q.query_map([], |r| {
                    let digest: Vec<u8> = r.get(0)?;
                    let generation: i64 = r.get(1)?;
                    let created: i64 = r.get(2)?;
                    Ok((digest, generation, created))
                })?;
                let mut out = Vec::new();
                for row in rows {
                    let (digest, generation, created) = row?;
                    let digest: [u8; 32] = digest
                        .try_into()
                        .map_err(|_| crate::Error::Protocol("stored chunk digest".into()))?;
                    out.push(((digest, generation as u64), created as u64));
                }
                Ok(out)
            })
            .map_err(tx_err)
    }

    fn stage_chunk(&self, transfer: &[u8], idx: u64, bytes: &[u8]) -> Result<()> {
        if bytes.is_empty() {
            return Err(Error::Invalid("empty staged fragment".into()));
        }
        let worker = self.worker.clone();
        let (transfer, bytes) = (transfer.to_vec(), bytes.to_vec());
        worker
            .call(move |s| {
                let now = now_millis() as i64;
                s.db.execute(
                    "INSERT INTO murmur_file_staging(transfer,idx,bytes,created_ms) VALUES(?1,?2,?3,?4)
                     ON CONFLICT(transfer,idx) DO UPDATE SET bytes=excluded.bytes",
                    rusqlite::params![transfer, idx as i64, bytes, now],
                )?;
                s.db.execute(
                    "INSERT INTO murmur_file_transfers(transfer,created_ms,updated_ms) VALUES(?1,?2,?3)
                     ON CONFLICT(transfer) DO UPDATE SET updated_ms=excluded.updated_ms",
                    rusqlite::params![transfer, now, now],
                )?;
                s.flush()
            })
            .map_err(tx_err)?;
        Ok(())
    }

    fn staged_count(&self, transfer: &[u8]) -> Result<u64> {
        let worker = self.worker.clone();
        let transfer = transfer.to_vec();
        worker
            .call(move |s| {
                let total: Option<i64> = s.db.query_row(
                    "SELECT sum(length(bytes)) FROM murmur_file_staging WHERE transfer=?1",
                    rusqlite::params![transfer],
                    |r| r.get(0),
                )?;
                Ok(total.unwrap_or(0) as u64)
            })
            .map_err(tx_err)
    }

    fn read_staged(&self, transfer: &[u8], offset: u64, len: usize) -> Result<Vec<u8>> {
        let worker = self.worker.clone();
        let transfer = transfer.to_vec();
        worker
            .call(move |s| {
                let mut q = s.db.prepare(
                    "SELECT bytes FROM murmur_file_staging WHERE transfer=?1 ORDER BY idx",
                )?;
                let rows = q.query_map(rusqlite::params![transfer], |r| {
                    r.get::<_, Vec<u8>>(0)
                })?;
                let mut out = Vec::new();
                let mut pos = 0u64;
                let end = offset.saturating_add(len as u64);
                for row in rows {
                    let fragment = row?;
                    let start = pos;
                    pos += fragment.len() as u64;
                    if pos <= offset || start >= end {
                        continue;
                    }
                    let from = offset.saturating_sub(start) as usize;
                    let to = (end.saturating_sub(start) as usize).min(fragment.len());
                    out.extend_from_slice(&fragment[from..to]);
                }
                Ok(out)
            })
            .map_err(tx_err)
    }

    fn drop_staged(&self, transfer: &[u8]) -> Result<()> {
        let worker = self.worker.clone();
        let transfer = transfer.to_vec();
        worker
            .call(move |s| {
                s.db.execute(
                    "DELETE FROM murmur_file_staging WHERE transfer=?1",
                    rusqlite::params![transfer],
                )?;
                s.db.execute(
                    "DELETE FROM murmur_file_transfers WHERE transfer=?1",
                    rusqlite::params![transfer],
                )?;
                s.flush()
            })
            .map_err(tx_err)?;
        Ok(())
    }

    fn container_len(&self, digest: &[u8; 32], generation: u64) -> Result<Option<u64>> {
        let worker = self.worker.clone();
        let digest = *digest;
        worker
            .call(move |s| {
                // Dense-from-zero only, matching `chunk_count`: a gap ends
                // the served prefix so the header always equals the stream.
                let mut q = s.db.prepare(
                    "SELECT idx,length(bytes) FROM murmur_file_chunks WHERE digest=?1 AND generation=?2 ORDER BY idx",
                )?;
                let rows = q.query_map(
                    rusqlite::params![digest.as_slice(), generation as i64],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
                )?;
                let mut total = 0u64;
                let mut next = 0u64;
                let mut any = false;
                for row in rows {
                    let (idx, len) = row?;
                    if idx as u64 != next {
                        break;
                    }
                    any = true;
                    total += len as u64;
                    next += 1;
                }
                Ok(any.then_some(total))
            })
            .map_err(tx_err)
    }

    fn staged_identity(&self, transfer: &[u8]) -> Result<Option<[u8; 32]>> {
        let worker = self.worker.clone();
        let transfer = transfer.to_vec();
        worker
            .call(move |s| {
                let row: Option<Option<Vec<u8>>> =
                    s.db.query_row(
                        "SELECT identity FROM murmur_file_transfers WHERE transfer=?1",
                        rusqlite::params![transfer],
                        |r| r.get(0),
                    )
                    .optional()?;
                row.flatten()
                    .map(|bytes| {
                        bytes
                            .try_into()
                            .map_err(|_| crate::Error::Protocol("stored transfer identity".into()))
                    })
                    .transpose()
            })
            .map_err(tx_err)
    }

    fn set_staged_identity(&self, transfer: &[u8], identity: &[u8; 32]) -> Result<()> {
        let worker = self.worker.clone();
        let (transfer, identity) = (transfer.to_vec(), *identity);
        worker
            .call(move |s| {
                // Upsert: the source header precedes the first fragment, so
                // the transfer row may not exist yet.
                let now = now_millis() as i64;
                s.db.execute(
                    "INSERT INTO murmur_file_transfers(transfer,identity,created_ms,updated_ms) VALUES(?1,?2,?3,?3)
                     ON CONFLICT(transfer) DO UPDATE SET identity=excluded.identity,updated_ms=excluded.updated_ms",
                    rusqlite::params![transfer, identity.as_slice(), now],
                )?;
                s.flush()
            })
            .map_err(tx_err)?;
        Ok(())
    }

    fn expired_staged(&self, cutoff: u64) -> Result<Vec<Vec<u8>>> {
        let worker = self.worker.clone();
        worker
            .call(move |s| {
                // u64::MAX lists every live transfer (also dodges the
                // u64->i64 narrowing, which would turn it into -1).
                let sql = if cutoff == u64::MAX {
                    "SELECT transfer FROM murmur_file_staging GROUP BY transfer ORDER BY transfer"
                } else {
                    "SELECT transfer FROM murmur_file_staging GROUP BY transfer HAVING min(created_ms)<=?1 ORDER BY transfer"
                };
                let mut q = s.db.prepare(sql)?;
                let rows: Vec<Vec<u8>> = if cutoff == u64::MAX {
                    q.query_map([], |r| r.get::<_, Vec<u8>>(0))?
                        .collect::<std::result::Result<_, _>>()
                        .map_err(crate::Error::Sql)?
                } else {
                    q.query_map([cutoff as i64], |r| r.get::<_, Vec<u8>>(0))?
                        .collect::<std::result::Result<_, _>>()
                        .map_err(crate::Error::Sql)?
                };
                Ok(rows)
            })
            .map_err(tx_err)
    }
}

/// File metadata backend: a [`CrrBackend`](crate::backend::CrrBackend)
/// with live identity. Files work before replication starts, so pinned
/// boot-time ids would stamp zeros into metadata forever; `identity()`
/// reads `murmur_identity` live instead (zeros pre-start is the honest
/// value, post-start it tracks automatically with no Hosts rebuild).
/// Snapshots stay unavailable until [`FileBackend::upgrade`] installs the
/// online backend at replication start.
pub struct FileBackend {
    worker: Arc<Worker>,
    inner: Mutex<crate::backend::CrrBackend>,
}

impl FileBackend {
    pub fn new_offline(worker: Arc<Worker>) -> Self {
        Self {
            inner: Mutex::new(crate::backend::CrrBackend::new_offline(
                worker.clone(),
                [0; 16],
                [0; 16],
            )),
            worker,
        }
    }

    /// Install the online backend once replication starts (enables
    /// snapshots for bridge export). Idempotent across restarts.
    pub fn upgrade(&self, group: [u8; 16], node: [u8; 16], config: crate::config::Config) {
        *self.inner.lock().unwrap() =
            crate::backend::CrrBackend::new(self.worker.clone(), group, node, config);
    }
}

impl murmur_tx::Backend for FileBackend {
    fn identity(&self) -> (murmur_tx::NodeId, murmur_tx::NodeId) {
        self.worker
            .call(|s| Ok(s.file_identity()))
            .unwrap_or(([0; 16], [0; 16]))
    }

    fn get(&self, key: &[u8]) -> murmur_tx::Result<Option<Vec<u8>>> {
        self.inner.lock().unwrap().get(key)
    }

    fn get_many(&self, keys: &[Vec<u8>]) -> murmur_tx::Result<Vec<Option<Vec<u8>>>> {
        self.inner.lock().unwrap().get_many(keys)
    }

    fn get_many_bounded(
        &self,
        keys: &[Vec<u8>],
        max_bytes: usize,
    ) -> murmur_tx::Result<Vec<Option<Vec<u8>>>> {
        self.inner.lock().unwrap().get_many_bounded(keys, max_bytes)
    }

    fn keys(&self, prefix: &[u8]) -> murmur_tx::Result<Vec<Vec<u8>>> {
        self.inner.lock().unwrap().keys(prefix)
    }

    fn keys_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> murmur_tx::Result<Vec<Vec<u8>>> {
        self.inner.lock().unwrap().keys_page(prefix, after, limit)
    }

    fn progress(&self) -> murmur_tx::Result<Vec<murmur_tx::protocol::Progress>> {
        self.inner.lock().unwrap().progress()
    }

    fn commit(&self, records: Vec<murmur_tx::Record>) -> murmur_tx::Result<()> {
        self.inner.lock().unwrap().commit(records)
    }

    fn import_bridge(
        &self,
        header: murmur_tx::bridge::Header,
        payload: Vec<u8>,
    ) -> murmur_tx::Result<bool> {
        self.inner.lock().unwrap().import_bridge(header, payload)
    }

    fn release_ownership(
        &self,
        table: String,
        key: Vec<u8>,
        column: Option<String>,
    ) -> murmur_tx::Result<bool> {
        self.inner.lock().unwrap().release_ownership(table, key, column)
    }

    fn publish(&self, records: Vec<murmur_tx::Record>) -> murmur_tx::Result<()> {
        self.inner.lock().unwrap().publish(records)
    }

    fn snapshot(&self) -> murmur_tx::Result<Vec<u8>> {
        self.inner.lock().unwrap().snapshot()
    }

    fn snapshot_with_progress(
        &self,
    ) -> murmur_tx::Result<(Vec<u8>, Vec<murmur_tx::protocol::Progress>)> {
        self.inner.lock().unwrap().snapshot_with_progress()
    }

    fn create_snapshot(
        &self,
        recipient: murmur_tx::NodeId,
        config: &murmur_tx::Config,
    ) -> murmur_tx::Result<murmur_tx::snapshot::Manifest> {
        self.inner.lock().unwrap().create_snapshot(recipient, config)
    }

    fn acknowledge_snapshot(
        &self,
        recipient: murmur_tx::NodeId,
        transfer: murmur_tx::NodeId,
        digest: [u8; 32],
    ) -> murmur_tx::Result<()> {
        self.inner
            .lock()
            .unwrap()
            .acknowledge_snapshot(recipient, transfer, digest)
    }

    fn retention_checkpoint(&self) -> murmur_tx::Result<()> {
        self.inner.lock().unwrap().retention_checkpoint()
    }

    fn prune_history(
        &self,
        origin: murmur_tx::NodeId,
        through: u64,
    ) -> murmur_tx::Result<u64> {
        self.inner.lock().unwrap().prune_history(origin, through)
    }

    fn reclaim_retention(&self) -> murmur_tx::Result<()> {
        self.inner.lock().unwrap().reclaim_retention()
    }

    fn schema(&self) -> murmur_tx::Result<Vec<u8>> {
        self.inner.lock().unwrap().schema()
    }

    fn apply(
        &self,
        payloads: Vec<Vec<u8>>,
        records: Vec<murmur_tx::Record>,
    ) -> murmur_tx::Result<bool> {
        self.inner.lock().unwrap().apply(payloads, records)
    }

    fn observe(&self, millis: u64, logical: u32) -> murmur_tx::Result<()> {
        self.inner.lock().unwrap().observe(millis, logical)
    }

    fn accept_schema(&self, schema: Vec<u8>) -> murmur_tx::Result<bool> {
        self.inner.lock().unwrap().accept_schema(schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (tempfile::TempDir, WorkerChunkStore) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f.db").to_str().unwrap().to_owned();
        let worker = Worker::open_test(path).unwrap();
        (dir, WorkerChunkStore::new(worker))
    }

    #[test]
    fn chunks_round_trip_across_slices() {
        let (_dir, store) = setup();
        let digest = [7u8; 32];
        let full = vec![0x41u8; SLICE_BYTES];
        store.put_chunk(&digest, 3, 0, &full).unwrap();
        store.put_chunk(&digest, 3, 1, b"0123456789").unwrap();
        assert_eq!(store.chunk_count(&digest, 3).unwrap(), 2);
        let all = store
            .read_stream(&digest, 3, 0, SLICE_BYTES + 10)
            .unwrap();
        assert_eq!(all.len(), SLICE_BYTES + 10);
        assert!(all[..SLICE_BYTES].iter().all(|b| *b == 0x41));
        assert_eq!(&all[SLICE_BYTES..], b"0123456789");
        // Spanning read across the slice boundary, short at the end.
        let span = store
            .read_stream(&digest, 3, SLICE_BYTES as u64 - 6, 20)
            .unwrap();
        assert_eq!(span.len(), 16);
        assert_eq!(&span[..6], b"AAAAAA");
        assert_eq!(&span[6..], b"0123456789");
        // Past the end reads empty.
        assert!(store
            .read_stream(&digest, 3, SLICE_BYTES as u64 + 10, 5)
            .unwrap()
            .is_empty());
        // Unknown object reads empty with zero count.
        assert_eq!(store.chunk_count(&[9u8; 32], 3).unwrap(), 0);
        assert!(store
            .read_stream(&[9u8; 32], 3, 0, 5)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn chunk_count_is_dense_and_delete_clears() {
        let (_dir, store) = setup();
        let digest = [8u8; 32];
        store.put_chunk(&digest, 1, 0, b"a").unwrap();
        store.put_chunk(&digest, 1, 1, b"b").unwrap();
        store.put_chunk(&digest, 1, 3, b"d").unwrap();
        // Gap at 2 ends the dense object.
        assert_eq!(store.chunk_count(&digest, 1).unwrap(), 2);
        // First-touch creation millis survive later slices (pinned past
        // value, deterministic without sleeps).
        store
            .worker
            .call(move |s| {
                s.db.execute(
                    "UPDATE murmur_file_chunks SET created_ms=1000 WHERE digest=?1",
                    rusqlite::params![digest.as_slice()],
                )?;
                Ok(())
            })
            .unwrap();
        store.put_chunk(&digest, 1, 4, b"e").unwrap();
        let live = store.live_objects().unwrap();
        assert_eq!(live, vec![((digest, 1), 1000)]);
        store.delete_object(&digest, 1).unwrap();
        assert!(store.live_objects().unwrap().is_empty());
        assert_eq!(store.chunk_count(&digest, 1).unwrap(), 0);
        // Missing objects are a no-op.
        store.delete_object(&digest, 1).unwrap();
    }

    #[test]
    fn staging_fragments_concatenate_and_expire() {
        let (_dir, store) = setup();
        store.stage_chunk(b"t1", 0, b"hello ").unwrap();
        store.stage_chunk(b"t1", 1, b"world").unwrap();
        assert_eq!(store.staged_count(b"t1").unwrap(), 11);
        assert_eq!(
            store.read_staged(b"t1", 0, 11).unwrap(),
            b"hello world".to_vec()
        );
        assert_eq!(store.read_staged(b"t1", 6, 5).unwrap(), b"world".to_vec());
        assert!(store.read_staged(b"missing", 0, 5).unwrap().is_empty());
        assert!(store.stage_chunk(b"t2", 0, b"").is_err());
        // Backdate t1; t1 expires before the cutoff, t2 does not.
        store
            .worker
            .call(|s| {
                s.db.execute(
                    "UPDATE murmur_file_staging SET created_ms=1000 WHERE transfer=x'7431'",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        store.stage_chunk(b"t2", 0, b"xx").unwrap();
        assert_eq!(store.expired_staged(5000).unwrap(), vec![b"t1".to_vec()]);
        let all = store.expired_staged(u64::MAX).unwrap();
        assert_eq!(all, vec![b"t1".to_vec(), b"t2".to_vec()]);
        // Transfer rows track creation/update; metadata fills at publish.
        let row: (i64, i64, Option<Vec<u8>>) = store
            .worker
            .call(|s| {
                Ok(s.db.query_row(
                    "SELECT created_ms,updated_ms,identity FROM murmur_file_transfers WHERE transfer=x'7431'",
                    [],
                    |r| {
                        Ok::<_, rusqlite::Error>((
                            r.get::<_, i64>(0)?,
                            r.get::<_, i64>(1)?,
                            r.get::<_, Option<Vec<u8>>>(2)?,
                        ))
                    },
                )?)
            })
            .unwrap();
        assert!(row.0 > 0 && row.1 >= row.0 && row.2.is_none());
        store.drop_staged(b"t1").unwrap();
        assert_eq!(store.staged_count(b"t1").unwrap(), 0);
        assert_eq!(store.expired_staged(u64::MAX).unwrap(), vec![b"t2".to_vec()]);
        store.drop_staged(b"t1").unwrap();
    }

    #[test]
    fn file_backend_identity_is_live_and_kv_delegates() {
        use murmur_tx::Backend;
        let (_dir, store) = setup();
        let backend = FileBackend::new_offline(store.worker.clone());
        // Pre-start: honest zeros (no murmur_identity row yet).
        assert_eq!(backend.identity(), ([0; 16], [0; 16]));
        // kv round-trips through the shared murmur_kv table.
        assert_eq!(backend.get(b"08/files/meta/x").unwrap(), None);
        backend
            .commit(vec![murmur_tx::Record::put(
                b"08/files/meta/x".to_vec(),
                b"v".to_vec(),
            )])
            .unwrap();
        assert_eq!(backend.get(b"08/files/meta/x").unwrap(), Some(b"v".to_vec()));
        assert_eq!(backend.keys(b"08/files/meta/").unwrap(), vec![b"08/files/meta/x".to_vec()]);
        // Offline snapshots fail closed.
        assert!(backend.snapshot().is_err());
    }

    #[test]
    fn unconfigured_hosts_fail_closed() {
        let (_dir, store) = setup();
        let backend = std::sync::Arc::new(FileBackend::new_offline(store.worker.clone()));
        let store = std::sync::Arc::new(store);
        let hosts = murmur_files::hosts::Hosts::open(backend, store);
        // No file key provisioned at register → configure skipped →
        // every action fails closed (§7), reads included.
        let err = hosts.upload("n", std::path::Path::new("/nope")).unwrap_err();
        assert!(err.to_string().contains("not configured"), "{err}");
        let err = hosts.list(None).unwrap_err();
        assert!(err.to_string().contains("not configured"), "{err}");
        let err = hosts.bridge_process().unwrap_err();
        assert!(err.to_string().contains("not configured"), "{err}");
    }

    #[test]
    fn container_len_is_dense_prefix() {
        use murmur_files::store::ChunkStore;
        let (_dir, store) = setup();
        let digest = [0x10u8; 32];
        // Missing object reads as absent.
        assert_eq!(store.container_len(&digest, 1).unwrap(), None);
        assert_eq!(store.container_len(&[0x11u8; 32], 1).unwrap(), None);
        // Aligned slices: only slice 0 must be full-width for the byte
        // walk to reach slice 1 (unaligned shorts end the stream early).
        let full = vec![0x41u8; SLICE_BYTES];
        store.put_chunk(&digest, 1, 0, &full).unwrap();
        store.put_chunk(&digest, 1, 1, b"bb").unwrap();
        store.put_chunk(&digest, 1, 3, b"c").unwrap();
        // The gap at 2 ends the served prefix: the header must equal what
        // `read_stream` yields, never the stray row beyond.
        assert_eq!(
            store.container_len(&digest, 1).unwrap(),
            Some(SLICE_BYTES as u64 + 2)
        );
        assert_eq!(
            store
                .read_stream(&digest, 1, 0, SLICE_BYTES + 2)
                .unwrap()
                .len(),
            SLICE_BYTES + 2
        );
        store.put_chunk(&digest, 1, 2, b"dd").unwrap();
        assert_eq!(
            store.container_len(&digest, 1).unwrap(),
            Some(SLICE_BYTES as u64 + 5)
        );
        store.delete_object(&digest, 1).unwrap();
        assert_eq!(store.container_len(&digest, 1).unwrap(), None);
    }

    #[test]
    fn staged_identity_upserts_and_reads() {
        use murmur_files::store::ChunkStore;
        let (_dir, store) = setup();
        assert_eq!(store.staged_identity(b"u1").unwrap(), None);
        // The source header precedes the first fragment: setting an
        // identity creates the transfer row.
        store.set_staged_identity(b"u1", &[9u8; 32]).unwrap();
        assert_eq!(store.staged_identity(b"u1").unwrap(), Some([9u8; 32]));
        store.stage_chunk(b"u1", 0, b"x").unwrap();
        // Overwrite wins; drops clear the identity with the row.
        store.set_staged_identity(b"u1", &[8u8; 32]).unwrap();
        assert_eq!(store.staged_identity(b"u1").unwrap(), Some([8u8; 32]));
        store.drop_staged(b"u1").unwrap();
        assert_eq!(store.staged_identity(b"u1").unwrap(), None);
    }

    // WP-C fetch-path integration tests (grant b): the wired stack
    // `Hosts -> SqliteObjects -> WorkerChunkStore -> murmur_file_* tables`
    // on a real worker. Payload bytes are asserted in SQLite rows
    // (constraint 9); `/tmp` files below are harness fixtures (upload
    // sources, download sinks), never product state.
    use crate::storage::Worker as TestWorker;
    use murmur_files::adapt as test_adapt;
    use murmur_files::hosts::Hosts as TestHosts;
    use std::collections::HashMap as TestHashMap;

    const KEY0: [u8; 32] = [0x11; 32];
    const KEY1: [u8; 32] = [0x22; 32];

    fn test_knobs(pairs: &[(&str, &str)]) -> murmur_files::adapt::Knobs {
        let map: TestHashMap<String, String> = pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        murmur_files::adapt::Knobs::from_map(&|name| map.get(name).cloned()).unwrap()
    }

    struct Harness {
        dir: tempfile::TempDir,
        worker: Arc<TestWorker>,
        store: Arc<WorkerChunkStore>,
        hosts: TestHosts,
    }

    fn setup_hosts() -> Harness {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("w.db").to_str().unwrap().to_owned();
        let worker = TestWorker::open_test(path).unwrap();
        let store = Arc::new(WorkerChunkStore::new(worker.clone()));
        let backend = Arc::new(FileBackend::new_offline(worker.clone()));
        let hosts = TestHosts::open(backend, store.clone());
        let config = test_adapt::objects_config(&test_knobs(&[]), KEY0, None).unwrap();
        hosts.configure_files(config).unwrap();
        Harness {
            dir,
            worker,
            store,
            hosts,
        }
    }

    fn chunk_rows(worker: &Arc<TestWorker>) -> i64 {
        worker
            .call(|s| {
                Ok(s.db.query_row("SELECT COUNT(*) FROM murmur_file_chunks", [], |r| r.get(0))?)
            })
            .unwrap()
    }

    fn engine_config() -> murmur_tx::Config {
        murmur_tx::Config::new(
            "127.0.0.1:0".parse().unwrap(),
            murmur_tx::Credentials {
                chain: Vec::new(),
                private_key: zeroize::Zeroizing::new(Vec::new()),
                roots: Vec::new(),
            },
            [0; 32],
        )
    }

    #[test]
    fn upload_list_read_download_round_trip_over_real_tables() {
        let h = setup_hosts();
        let payload: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        let src = h.dir.path().join("src.bin");
        std::fs::write(&src, &payload).unwrap();
        let meta = h.hosts.upload("big", &src).unwrap();
        assert_eq!(meta.bytes, 100_000);
        assert_eq!(meta.generation, 0);
        // Constraint 9: a 100KB payload spans chunk rows in SQLite.
        assert!(chunk_rows(&h.worker) >= 2, "expected multi-slice storage");
        let listed = h.hosts.list(None).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].name, "big");
        assert!(h.hosts.list(Some("nope")).unwrap().is_empty());
        assert_eq!(
            h.hosts.metadata("big").unwrap().unwrap().digest,
            meta.digest
        );
        assert!(h.hosts.metadata("missing").unwrap().is_none());
        assert_eq!(h.hosts.availability("big").unwrap(), "local");
        assert_eq!(h.hosts.availability("missing").unwrap(), "unavailable");
        // Plaintext reads back exactly, including a span across the 64KiB slice seam.
        assert_eq!(h.hosts.read_chunk("big", 0, 100_000).unwrap(), payload);
        assert_eq!(
            h.hosts.read_chunk("big", 65_530, 10).unwrap(),
            payload[65_530..65_540]
        );
        let dest = h.dir.path().join("out.bin");
        assert_eq!(h.hosts.download("big", &dest).unwrap(), 100_000);
        assert_eq!(std::fs::read(&dest).unwrap(), payload);
    }

    #[test]
    fn availability_tracks_remote_peers() {
        let h = setup_hosts();
        let src = h.dir.path().join("r.bin");
        std::fs::write(&src, b"remote?").unwrap();
        let meta = h.hosts.upload("r", &src).unwrap();
        assert_eq!(h.hosts.availability("r").unwrap(), "local");
        // Simulate a peer-held object: metadata without local payload.
        h.worker
            .call(|s| {
                s.db.execute("DELETE FROM murmur_file_chunks", [])?;
                Ok(())
            })
            .unwrap();
        assert_eq!(h.hosts.availability("r").unwrap(), "unavailable");
        let peer = [5u8; 16];
        h.hosts
            .objects()
            .unwrap()
            .record_peer_availability(meta.digest, meta.generation, peer, true)
            .unwrap();
        assert_eq!(
            h.hosts.availability("r").unwrap(),
            format!("remote:{}", murmur_tx::hex(&peer))
        );
        h.hosts
            .objects()
            .unwrap()
            .record_peer_availability(meta.digest, meta.generation, peer, false)
            .unwrap();
        assert_eq!(h.hosts.availability("r").unwrap(), "unavailable");
    }

    #[test]
    fn delete_then_collect_reaps_chunks() {
        let h = setup_hosts();
        let src = h.dir.path().join("g.bin");
        std::fs::write(&src, b"bye").unwrap();
        h.hosts.upload("gone", &src).unwrap();
        assert_eq!(chunk_rows(&h.worker), 1);
        h.hosts.delete("gone").unwrap();
        assert!(h.hosts.list(None).unwrap().is_empty());
        assert!(h.hosts.metadata("gone").unwrap().unwrap().deleted);
        assert_eq!(h.hosts.collect(0).unwrap(), 1);
        assert_eq!(chunk_rows(&h.worker), 0);
        assert_eq!(h.hosts.collect(0).unwrap(), 0);
    }

    #[test]
    fn rotate_rekeys_one_step_and_status_clears() {
        let h = setup_hosts();
        let payload = b"rotate me".to_vec();
        let src = h.dir.path().join("d.bin");
        std::fs::write(&src, &payload).unwrap();
        h.hosts.upload("doc", &src).unwrap();
        // One-step rule: gen 0 -> gen 2 refuses even with a previous key.
        let bad = test_adapt::objects_config(
            &test_knobs(&[(test_adapt::FILES_GENERATION, "2")]),
            [0x33; 32],
            Some(KEY0),
        )
        .unwrap();
        assert!(h.hosts.configure_files(bad).is_err());
        // Gen 1 with the previous key opens; rotate re-seals the object.
        let next = test_adapt::objects_config(
            &test_knobs(&[(test_adapt::FILES_GENERATION, "1")]),
            KEY1,
            Some(KEY0),
        )
        .unwrap();
        h.hosts.configure_files(next).unwrap();
        let status = h.hosts.rotation_status().unwrap();
        assert_eq!(status.current_generation, 1);
        assert_eq!(status.old_generation_objects, 1);
        assert!(!status.previous_key_unused_locally);
        assert_eq!(h.hosts.rotate(10).unwrap(), 1);
        let meta = h.hosts.metadata("doc").unwrap().unwrap();
        assert_eq!(meta.generation, 1);
        assert_eq!(
            h.hosts.read_chunk("doc", 0, payload.len()).unwrap(),
            payload
        );
        // Old slices linger until collect, then the status goes clean.
        assert_eq!(h.hosts.rotation_status().unwrap().old_generation_objects, 1);
        assert!(h.hosts.collect(0).unwrap() >= 1);
        let status = h.hosts.rotation_status().unwrap();
        assert_eq!(status.old_generation_objects, 0);
        assert!(status.previous_key_unused_locally);
    }

    #[test]
    fn fetch_stays_fail_closed_without_engine_credentials() {
        let h = setup_hosts();
        let engine = engine_config();
        let err = h.hosts.start_fetch(&test_knobs(&[]), &engine).unwrap_err();
        assert!(err.to_string().contains("disabled"), "{err}");
        // Split landed (Phase-2): the next guard is engine TLS
        // credentials, not the retired store split. Bare creds fail
        // cleanly, the tick stays idle, stop stays a no-op.
        let on = test_knobs(&[(test_adapt::FETCH_BIND, "127.0.0.1:0")]);
        let err = h.hosts.start_fetch(&on, &engine).unwrap_err();
        assert!(err.to_string().contains("TLS credentials"), "{err}");
        assert!(!h.hosts.fetch_tick(vec![]).unwrap());
        h.hosts.stop_fetch();
    }

    // Self-signed P-256 test certificate (DER hex) minted for node
    // 1111..11, matching the murmur_identity node set below so the
    // fetch transport accepts it. Generated once with openssl (offline):
    // req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 with a
    // URI:replicateddb://node/1111..11 SAN and NO other extensions
    // (-config with an empty -extensions section); key exported
    // PKCS#8 DER. Plain end-entity form is REQUIRED: a CA:TRUE
    // self-signed cert is rejected as a TLS leaf (CaUsedAsEndEntity),
    // while the bare cert still anchors peer verification by SPKI.
    // Fixture bytes, not generated per run.
    // (Duplicated from murmur-files hosts.rs per the Phase-3b grant;
    // re-pointed at the Phase-4 EE bytes per the 4b micro-slice so the
    // tree holds exactly one node-11 cert.)
    const FETCH_CERT_HEX: &str = concat!(
        "3082019c30820142a0030201020214288f92c4bcc1c472a3bd2f865d9a66b3760d3ec5300a06082a8648ce3d040302301c311a301806035504030c11",
        "6d75726d75722d66657463682d74657374301e170d3236313032383232313334375a170d3336313032353232313334375a301c311a30180603550403",
        "0c116d75726d75722d66657463682d746573743059301306072a8648ce3d020106082a8648ce3d03010703420004f22e2d14d0c32a04f36c7a863958",
        "50e4dc1d26a8f2db7caf24a4d204854ab4581a65cdff26dcc5fb293b9f89e85bda3fa1f0c6c377cf273f403ce4389a844346a3623060303f0603551d",
        "110438303686347265706c69636174656464623a2f2f6e6f64652f313131313131313131313131313131313131313131313131313131313131313130",
        "1d0603551d0e041604144182d0ce1f4761c00dcf50652c06d2e64e4eed8a300a06082a8648ce3d0403020348003045022100a9196d5234d6a4ed34cd",
        "9213da6d53f672d84919d005ac2ed236bab350650c86022076b3ed1b95a37fa749f3dceb34932bef9d20051c3bf3d20cd1d49303494e9c73",
    );
    const FETCH_KEY_HEX: &str = concat!(
        "308187020100301306072a8648ce3d020106082a8648ce3d030107046d306b02010104203deb2d7498355af71dd2e58325d05a550a95b1cd6fe2d0e4",
        "48ea200f00876ae3a14403420004f22e2d14d0c32a04f36c7a86395850e4dc1d26a8f2db7caf24a4d204854ab4581a65cdff26dcc5fb293b9f89e85b",
        "da3fa1f0c6c377cf273f403ce4389a844346",
    );

    fn fetch_hex_bytes(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect()
    }

    /// Replication Config mirroring backend.rs `dummy_config`, with the
    /// fixture credentials installed and carry-proving (non-default)
    /// values for the converted fields.
    fn fetch_test_config() -> crate::config::Config {
        crate::config::Config {
            group: [1; 16],
            node: [0x11; 16],
            site: [3; 16],
            bind: "127.0.0.1:0".parse().unwrap(),
            advertise: None,
            credentials: crate::config::Credentials {
                chain: vec![fetch_hex_bytes(FETCH_CERT_HEX)],
                private_key: zeroize::Zeroizing::new(fetch_hex_bytes(FETCH_KEY_HEX)),
                roots: vec![fetch_hex_bytes(FETCH_CERT_HEX)],
            },
            allowed_peers: [[0x22; 16]].into_iter().collect(),
            allowed_networks: vec!["127.0.0.0/8".parse().unwrap()],
            bootstrap: Vec::new(),
            excluded: std::collections::BTreeSet::new(),
            incarnation: 1,
            sessions: 16,
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

    #[test]
    fn fetch_production_path_converts_and_runs_live() {
        let h = setup_hosts();
        // Endpoint node check runs against FileBackend's LIVE identity
        // (murmur_identity row), so set it to the fixture node first.
        h.worker
            .call(|s| {
                s.db.execute(
                    "INSERT INTO murmur_identity(id, node) VALUES(1, ?1)
                     ON CONFLICT(id) DO UPDATE SET node = excluded.node",
                    rusqlite::params![[0x11u8; 16].as_slice()],
                )?;
                s.flush()
            })
            .unwrap();
        let cfg = fetch_test_config();
        // Conversion carries exactly the fetch read-set (Phase-3a);
        // non-default values prove carry rather than coincidence.
        let tx = cfg.to_fetch_transport();
        assert_eq!(tx.bind, cfg.bind);
        assert_eq!(tx.sessions, 16);
        assert_eq!(tx.allowed_peers, cfg.allowed_peers);
        assert_eq!(tx.allowed_networks, cfg.allowed_networks);
        assert_eq!(tx.credentials.chain.len(), 1);
        assert_eq!(tx.credentials.roots.len(), 1);
        assert!(!tx.credentials.private_key.is_empty());
        assert!(tx.files.is_none());
        // Best-effort contract: disabled knobs error WITHOUT failing
        // the caller — Control::start holds `let _` over this call.
        let err = h.hosts.start_fetch(&test_knobs(&[]), &tx).unwrap_err();
        assert!(err.to_string().contains("disabled"), "{err}");
        // Live loop over the real tables: bind, one empty-source
        // round, clean stop, idle after.
        let on = test_knobs(&[(test_adapt::FETCH_BIND, "127.0.0.1:0")]);
        h.hosts.start_fetch(&on, &tx).unwrap();
        assert!(h.hosts.fetch_tick(vec![]).unwrap());
        h.hosts.stop_fetch();
        assert!(!h.hosts.fetch_tick(vec![]).unwrap());
    }

    #[test]
    fn staged_transfers_leave_identity_null_until_split() {
        use murmur_files::store::ChunkStore;
        let h = setup_hosts();
        h.store.stage_chunk(b"tx1", 0, b"frag").unwrap();
        assert_eq!(h.store.staged_count(b"tx1").unwrap(), 4);
        let identity: Option<Vec<u8>> = h
            .worker
            .call(|s| {
                Ok(s.db.query_row(
                    "SELECT identity FROM murmur_file_transfers WHERE transfer = ?1",
                    rusqlite::params![b"tx1".as_slice()],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert!(identity.is_none());
        h.store.drop_staged(b"tx1").unwrap();
        let rows: i64 = h
            .worker
            .call(|s| {
                Ok(s.db
                    .query_row("SELECT COUNT(*) FROM murmur_file_transfers", [], |r| {
                        r.get(0)
                    })?)
            })
            .unwrap();
        assert_eq!(rows, 0);
    }
}
