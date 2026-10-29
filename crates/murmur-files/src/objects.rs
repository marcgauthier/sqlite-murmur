//! SQLite-backed immutable file objects: the [`SqliteObjects`] store.
//!
//! This is the constraint-9 port of archived `murmur-tx` file objects. Every
//! algorithm mirrors the archived `Objects` method of the same name: metadata
//! keys and values in the Spool backend are byte-identical, SPFO framing and
//! key selection are verbatim, and only the payload addressing changes —
//! container slices rest in [`ChunkStore`](crate::store::ChunkStore) rows
//! addressed by `(digest, generation, idx)` instead of files addressed by
//! path. There is no staging directory: in-flight bytes rest in staging rows
//! under a transfer id, guarded against collection exactly like the archived
//! staging paths.
//!
//! Method names, error strings, and reservation math match the archive so the
//! lead's generic fetch-service split can bind to this surface without
//! semantic drift. Anything that only made sense for files (hard-link pins,
//! directory fsync, temp-file modes) is replaced by the store equivalent and
//! noted at the site.

use crate::crypto::{ByteSource, OpenReader, SealWriter};
use crate::store::{ChunkStore, ObjectId, SLICE_BYTES};
use murmur_tx::files::{Availability, KeyRotationStatus, Metadata};
use murmur_tx::{Backend, Error, NodeId, Record, Result, decode, encode, hex};
use crate::crypto_provider::Sha256;
use std::cell::Cell;
use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn check_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 4096 || name.contains('\0') {
        return Err(Error::Invalid("file name".into()));
    }
    Ok(())
}

fn meta_key(name: &str) -> Vec<u8> {
    format!("08/files/meta/{}", hex(&Sha256::digest(name.as_bytes()))).into_bytes()
}

fn availability_prefix(digest: [u8; 32], generation: u64) -> Vec<u8> {
    format!("08/files/availability/{}/{generation}/", hex(&digest)).into_bytes()
}

fn hexval(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        _ => None,
    }
}

fn decode_hex_node(bytes: &[u8]) -> Option<NodeId> {
    if bytes.len() != 32 {
        return None;
    }
    let mut node = [0; 16];
    for (i, pair) in bytes.chunks_exact(2).enumerate() {
        node[i] = (hexval(pair[0])? << 4) | hexval(pair[1])?;
    }
    Some(node)
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Store configuration: keys, generation, and byte limits. Fetch dials (bind
/// address, concurrency, interval) belong to the fetch service, not the store.
#[derive(Clone)]
pub struct ObjectsConfig {
    /// Current sealing key. Zeroized on drop; never logged or formatted.
    pub key: zeroize::Zeroizing<[u8; 32]>,
    /// Previous sealing key. Required while old-generation metadata exists.
    pub previous_key: Option<zeroize::Zeroizing<[u8; 32]>>,
    /// Current key generation; sealed objects carry this generation.
    pub generation: u64,
    /// Largest sealable plaintext object in bytes.
    pub max_file_bytes: u64,
    /// Shared staging capacity for uploads and peer fetches.
    pub max_staging_bytes: u64,
}

impl std::fmt::Debug for ObjectsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Files")
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

impl ObjectsConfig {
    /// Default limits (4GiB objects, 1GiB staging) at generation zero.
    pub fn new(key: [u8; 32]) -> Self {
        Self {
            key: zeroize::Zeroizing::new(key),
            previous_key: None,
            generation: 0,
            max_file_bytes: 4 << 30,
            max_staging_bytes: 1 << 30,
        }
    }

    /// Reject a zero object cap or staging below one slice.
    pub fn validate(&self) -> Result<()> {
        if self.max_file_bytes == 0 || self.max_staging_bytes < SLICE_BYTES as u64 {
            return Err(Error::Invalid("file limits".into()));
        }
        Ok(())
    }
}

/// A live pin or staged transfer the collector must not reap. The archived
/// guard set holds paths; paths do not exist here, so the set holds the store
/// identity directly. Semantics match: insert while bytes are in flight, drop
/// the guard to release.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ActiveEntry {
    Pinned(ObjectId),
    Staged(Vec<u8>),
}

/// Collector guard over pins and staged transfers. Dropping releases every
/// held entry, like the archived staging guard.
pub struct StagedTransfer {
    active: Arc<Mutex<BTreeSet<ActiveEntry>>>,
    entries: Vec<ActiveEntry>,
}

impl Drop for StagedTransfer {
    fn drop(&mut self) {
        let mut active = self.active.lock().unwrap();
        for entry in &self.entries {
            active.remove(entry);
        }
    }
}

/// A reservation shared by local uploads and background peer fetches.
pub struct StagingReservation {
    used: Arc<Mutex<u64>>,
    bytes: u64,
}

impl Drop for StagingReservation {
    fn drop(&mut self) {
        let mut used = self.used.lock().unwrap();
        *used = used.saturating_sub(self.bytes);
    }
}

/// Pull source over one stored object's container bytes. Gaps surface as
/// unexpected-EOF through the same conversion as a truncated archived file.
struct ChunkReader<'s, S: ChunkStore> {
    store: &'s S,
    digest: [u8; 32],
    generation: u64,
    pos: u64,
}

impl<S: ChunkStore> ByteSource for ChunkReader<'_, S> {
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        let data = self
            .store
            .read_stream(&self.digest, self.generation, self.pos, buf.len())?;
        if data.len() < buf.len() {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
        }
        buf.copy_from_slice(&data);
        self.pos += data.len() as u64;
        Ok(())
    }

    fn read_max(&mut self, buf: &mut [u8]) -> Result<usize> {
        let data = self
            .store
            .read_stream(&self.digest, self.generation, self.pos, buf.len())?;
        let n = data.len().min(buf.len());
        buf[..n].copy_from_slice(&data[..n]);
        self.pos += n as u64;
        Ok(n)
    }
}

/// Pull source over one staged transfer's bytes, for authenticating staged
/// containers before they are published.
struct StagedReader<'s, S: ChunkStore> {
    store: &'s S,
    transfer: Vec<u8>,
    pos: u64,
}

impl<S: ChunkStore> ByteSource for StagedReader<'_, S> {
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        let data = self
            .store
            .read_staged(&self.transfer, self.pos, buf.len())?;
        if data.len() < buf.len() {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
        }
        buf.copy_from_slice(&data);
        self.pos += data.len() as u64;
        Ok(())
    }

    fn read_max(&mut self, buf: &mut [u8]) -> Result<usize> {
        let data = self
            .store
            .read_staged(&self.transfer, self.pos, buf.len())?;
        let n = data.len().min(buf.len());
        buf[..n].copy_from_slice(&data[..n]);
        self.pos += n as u64;
        Ok(n)
    }
}

/// Immutable authenticated file objects over a [`ChunkStore`].
///
/// Metadata rides the Spool backend under the archived `08/files/meta/` keys
/// with the archived [`Metadata`] values; payload slices rest in the store.
/// All mutation takes the gate; verify paths are lock-free like the archive.
pub struct SqliteObjects<S> {
    config: ObjectsConfig,
    backend: Arc<dyn Backend>,
    store: S,
    error: Mutex<Option<String>>,
    gate: Mutex<()>,
    active: Arc<Mutex<BTreeSet<ActiveEntry>>>,
    staging_bytes: Arc<Mutex<u64>>,
}

impl<S: ChunkStore> SqliteObjects<S> {
    /// Open the store. Old-generation metadata requires the previous key and
    /// single-generation rotation steps, exactly like the archived open. There
    /// is no directory to create: SQLite tables are the lead's DDL.
    pub fn open(config: ObjectsConfig, backend: Arc<dyn Backend>, store: S) -> Result<Self> {
        config.validate()?;
        for key in backend.keys(b"08/files/meta/")? {
            if let Some(bytes) = backend.get(&key)? {
                let meta: Metadata = decode(&bytes)?;
                if !meta.deleted && meta.generation < config.generation {
                    if config.previous_key.is_none() {
                        return Err(Error::Invalid(
                            "previous file key is required while old-generation metadata exists"
                                .into(),
                        ));
                    }
                    if meta.generation.checked_add(1) != Some(config.generation) {
                        return Err(Error::Invalid(
                            "file keys must rotate one generation at a time".into(),
                        ));
                    }
                }
            }
        }
        Ok(Self {
            config,
            backend,
            store,
            error: Mutex::new(None),
            gate: Mutex::new(()),
            active: Arc::new(Mutex::new(BTreeSet::new())),
            staging_bytes: Arc::new(Mutex::new(0)),
        })
    }

    /// Borrow the chunk store. Same-crate only: the fetch-service
    /// `FetchObjects` backend (fetch_sqlite.rs) composes the serve/fetch
    /// primitives (`container_len`, staged identities, fragments) directly
    /// from the store seam instead of through pass-throughs.
    pub(crate) fn store(&self) -> &S {
        &self.store
    }

    /// Guard one staged transfer against collection while its bytes are in
    /// flight. Takes the gate like the archived staging guard so a concurrent
    /// collect cannot interleave. The lead's fetch service holds this across
    /// each peer transfer.
    pub fn protect_staging(&self, transfer: Vec<u8>) -> StagedTransfer {
        let _gate = self.gate.lock().unwrap();
        self.active
            .lock()
            .unwrap()
            .insert(ActiveEntry::Staged(transfer.clone()));
        StagedTransfer {
            active: self.active.clone(),
            entries: vec![ActiveEntry::Staged(transfer)],
        }
    }

    /// Pin one published object across streaming reads. The archived pin is a
    /// hard link; here the pin is a guard entry the collector skips.
    fn pin_object(&self, digest: [u8; 32], generation: u64) -> StagedTransfer {
        let entry = ActiveEntry::Pinned((digest, generation));
        self.active.lock().unwrap().insert(entry.clone());
        StagedTransfer {
            active: self.active.clone(),
            entries: vec![entry],
        }
    }

    /// Last background fetch failure, if any.
    pub fn last_fetch_error(&self) -> Option<String> {
        self.error.lock().unwrap().clone()
    }

    /// Record a background fetch failure for status reads. The lead's generic
    /// fetch service calls this; it stays reachable outside this crate.
    pub fn fetch_error(&self, error: &Error) {
        *self.error.lock().unwrap() = Some(error.to_string());
    }

    /// Seal `input` into a new immutable object registered under `name`.
    pub fn upload(&self, name: &str, input: impl Read) -> Result<Metadata> {
        let _gate = self.gate.lock().unwrap();
        self.upload_inner(name, input)
    }

    fn upload_inner(&self, name: &str, mut input: impl Read) -> Result<Metadata> {
        let mut upload = Upload::new(self, name)?;
        let mut buffer = zeroize::Zeroizing::new(vec![0; SLICE_BYTES]);
        loop {
            let n = input.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            upload.append(&buffer[..n])?;
        }
        upload.finish_inner(self)
    }

    /// Begin a bounded encrypted upload. No metadata is published until finish.
    pub fn begin_upload(&self, name: &str) -> Result<Upload<'_, S>> {
        let _gate = self.gate.lock().unwrap();
        Upload::new(self, name)
    }

    /// Publish a completed upload atomically with respect to local mutations.
    /// Rejects uploads begun against another store instance.
    pub fn finish_upload(&self, upload: Upload<'_, S>) -> Result<Metadata> {
        if !Arc::ptr_eq(&self.staging_bytes, &upload.used) {
            return Err(Error::Invalid(
                "upload belongs to another object store".into(),
            ));
        }
        let _gate = self.gate.lock().unwrap();
        upload.finish_inner(self)
    }

    /// Reserve shared staging capacity for an upload or peer fetch.
    pub fn reserve_staging(&self, bytes: u64) -> Result<StagingReservation> {
        let mut used = self.staging_bytes.lock().unwrap();
        if bytes > self.config.max_staging_bytes.saturating_sub(*used) {
            return Err(Error::Overloaded);
        }
        *used += bytes;
        Ok(StagingReservation {
            used: self.staging_bytes.clone(),
            bytes,
        })
    }

    /// Guard entries without taking the gate, for callers that already hold
    /// it (uploads seal under the gate like the archived temp-file insert).
    fn guard_entries(&self, entries: Vec<ActiveEntry>) -> StagedTransfer {
        self.active.lock().unwrap().extend(entries.iter().cloned());
        StagedTransfer {
            active: self.active.clone(),
            entries,
        }
    }

    /// Copy staged bytes into aligned object slices, reading the staging
    /// stream back by byte offset. Callers hold the gate.
    fn move_staged(&self, staged: &[u8], digest: &[u8; 32], generation: u64) -> Result<()> {
        let mut idx = 0u64;
        let mut pos = 0u64;
        loop {
            let slice = self.store.read_staged(staged, pos, SLICE_BYTES)?;
            if slice.is_empty() {
                break;
            }
            pos += slice.len() as u64;
            self.store.put_chunk(digest, generation, idx, &slice)?;
            idx += 1;
        }
        Ok(())
    }

    /// Pin and authenticate one immutable object before streaming it to a client.
    pub fn download(&self, name: &str) -> Result<Download<'_, S>> {
        let _gate = self.gate.lock().unwrap();
        let meta = self
            .metadata(name)?
            .filter(|m| !m.deleted)
            .ok_or_else(|| Error::Unavailable("file metadata missing".into()))?;
        if self.store.chunk_count(&meta.digest, meta.generation)? == 0 {
            return Err(Error::Unavailable(
                "file payload unavailable; retry after background fetching".into(),
            ));
        }
        let reader = ChunkReader {
            store: &self.store,
            digest: meta.digest,
            generation: meta.generation,
            pos: 0,
        };
        self.verify_input(reader, &meta, &mut |_| Ok(()))?;
        let reader = ChunkReader {
            store: &self.store,
            digest: meta.digest,
            generation: meta.generation,
            pos: 0,
        };
        // Reopen for chunked reads: the pre-verify above already enforced the
        // generation, so key selection here agrees with the archived pin path.
        let verifier = OpenReader::open(
            reader,
            &self.config.key,
            self.config.previous_key.as_deref(),
            self.config.generation,
            meta.bytes,
            meta.digest,
            self.config.max_file_bytes,
        )?;
        let pin = self.pin_object(meta.digest, meta.generation);
        Ok(Download {
            meta,
            verifier,
            offset: 0,
            pending: zeroize::Zeroizing::new(Vec::new()),
            pending_pos: 0,
            _pin: pin,
        })
    }

    /// Read one name registration, tombstones included.
    pub fn metadata(&self, name: &str) -> Result<Option<Metadata>> {
        check_name(name)?;
        self.backend
            .get(&meta_key(name))?
            .map(|b| decode(&b))
            .transpose()
    }

    /// List every live registration ordered by name.
    pub fn list(&self) -> Result<Vec<Metadata>> {
        let mut out = Vec::new();
        for key in self.backend.keys(b"08/files/meta/")? {
            if let Some(b) = self.backend.get(&key)? {
                let meta: Metadata = decode(&b)?;
                if !meta.deleted {
                    out.push(meta);
                }
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    /// True when live metadata names a complete payload: the stored slice
    /// count equals the sealed container size for the registered length.
    /// Stricter than the archived path check on purpose — a partial object
    /// (crashed seal, lost rows) reports unavailable until a fetch completes
    /// it, instead of failing mid-read.
    pub fn available(&self, name: &str) -> Result<bool> {
        Ok(match self.metadata(name)? {
            Some(m) if !m.deleted => {
                let expected = crate::crypto::expected_slices(m.bytes)?;
                self.store.chunk_count(&m.digest, m.generation)? == expected
            }
            _ => false,
        })
    }

    /// Local when the payload is present, remote when fresh peer sightings
    /// exist, unavailable otherwise. Sightings older than seven days retire.
    pub fn availability(&self, name: &str) -> Result<Availability> {
        let Some(meta) = self.metadata(name)? else {
            return Ok(Availability::Unavailable);
        };
        if meta.deleted {
            return Ok(Availability::Unavailable);
        }
        if self.available(name)? {
            return Ok(Availability::Local);
        }
        let prefix = availability_prefix(meta.digest, meta.generation);
        let cutoff = now().saturating_sub(Duration::from_secs(7 * 86400).as_millis() as u64);
        let mut peers = Vec::new();
        for key in self.backend.keys(&prefix)? {
            if let Some(bytes) = self.backend.get(&key)? {
                let seen: u64 = decode(&bytes)?;
                if seen >= cutoff {
                    if let Some(node) = key.rsplit(|b| *b == b'/').next().and_then(decode_hex_node)
                    {
                        peers.push(node);
                    }
                }
            }
        }
        peers.sort();
        peers.dedup();
        Ok(if peers.is_empty() {
            Availability::Unavailable
        } else {
            Availability::Remote(peers)
        })
    }

    /// Record or retract one peer's sighting of an object. Committed locally,
    /// never published, like the archive.
    pub fn record_peer_availability(
        &self,
        digest: [u8; 32],
        generation: u64,
        node: NodeId,
        available: bool,
    ) -> Result<()> {
        let mut key = availability_prefix(digest, generation);
        key.extend(hex(&node).as_bytes());
        let mutation = if available {
            Record::put(key, encode(&now())?)
        } else {
            Record::delete(key)
        };
        self.backend.commit(vec![mutation])
    }

    /// Tombstone one name registration. Payload slices stay for collection.
    pub fn delete(&self, name: &str) -> Result<()> {
        let _gate = self.gate.lock().unwrap();
        if let Some(mut meta) = self.metadata(name)? {
            meta.deleted = true;
            meta.millis = now();
            meta.node = self.backend.identity().1;
            self.backend
                .publish(vec![Record::put(meta_key(name), encode(&meta)?)])?;
        }
        Ok(())
    }

    /// Verify one registered object and stream its plaintext to `output`.
    pub fn read(&self, name: &str, mut output: impl Write) -> Result<Metadata> {
        let meta = self
            .metadata(name)?
            .filter(|m| !m.deleted)
            .ok_or_else(|| Error::Unavailable("file metadata missing".into()))?;
        if self.store.chunk_count(&meta.digest, meta.generation)? == 0 {
            return Err(Error::Unavailable("file payload unavailable".into()));
        }
        let reader = ChunkReader {
            store: &self.store,
            digest: meta.digest,
            generation: meta.generation,
            pos: 0,
        };
        let out = &mut output;
        self.verify_input(reader, &meta, &mut |b| {
            out.write_all(b).map_err(Error::from)
        })?;
        Ok(meta)
    }

    /// Verify and stream plaintext in bounded chunks, omitting an already
    /// exported prefix when resuming an external transfer.
    pub fn stream_plain_chunks(
        &self,
        name: &str,
        skip: u64,
        chunk_size: usize,
        mut emit: impl FnMut(u64, &[u8]) -> Result<()>,
    ) -> Result<(Metadata, u64)> {
        if chunk_size == 0 {
            return Err(Error::Invalid("zero file chunk size".into()));
        }
        let meta = self
            .metadata(name)?
            .filter(|m| !m.deleted)
            .ok_or_else(|| Error::Unavailable("file metadata missing".into()))?;
        if self.store.chunk_count(&meta.digest, meta.generation)? == 0 {
            return Err(Error::Unavailable("file payload unavailable".into()));
        }
        let reader = ChunkReader {
            store: &self.store,
            digest: meta.digest,
            generation: meta.generation,
            pos: 0,
        };
        let mut sink = ChunkWriter {
            skip,
            position: skip,
            total: 0,
            chunk_size,
            buffer: Vec::with_capacity(chunk_size),
            emit: &mut emit,
            error: None,
        };
        let verified = self.verify_input(reader, &meta, &mut |b| {
            sink.write_all(b).map_err(Error::from)
        });
        if let Some(error) = sink.error.take() {
            return Err(error);
        }
        verified?;
        if !sink.buffer.is_empty() {
            let start = sink.position;
            (sink.emit)(start, &sink.buffer)?;
            sink.position += sink.buffer.len() as u64;
        }
        Ok((meta, sink.total))
    }

    /// Verify one registered object against its metadata, streaming plaintext
    /// to `output`. The archived form takes a path; the store replaces it.
    pub fn verify(&self, meta: &Metadata, mut output: impl Write) -> Result<()> {
        if self.store.chunk_count(&meta.digest, meta.generation)? == 0 {
            return Err(Error::Unavailable("file payload unavailable".into()));
        }
        let reader = ChunkReader {
            store: &self.store,
            digest: meta.digest,
            generation: meta.generation,
            pos: 0,
        };
        self.verify_input(reader, meta, &mut |b| {
            output.write_all(b).map_err(Error::from)
        })
    }

    /// Authenticate one container stream and emit its plaintext through the
    /// sink. Lock-free like the archived verifier; every check is verbatim.
    fn verify_input(
        &self,
        reader: impl ByteSource,
        meta: &Metadata,
        sink: &mut dyn FnMut(&[u8]) -> Result<()>,
    ) -> Result<()> {
        let mut verifier = OpenReader::open(
            reader,
            &self.config.key,
            self.config.previous_key.as_deref(),
            self.config.generation,
            meta.bytes,
            meta.digest,
            self.config.max_file_bytes,
        )?;
        verifier.verify_to(sink)
    }

    /// Publish only a fully authenticated staged container. When the object
    /// already exists the staged bytes stay for collection, mirroring the
    /// archived rename-skip; the lead's fetch service holds the staging guard
    /// across its transfer.
    pub fn publish_fetched(&self, staged: &[u8], meta: &Metadata) -> Result<()> {
        let _gate = self.gate.lock().unwrap();
        let reader = StagedReader {
            store: &self.store,
            transfer: staged.to_vec(),
            pos: 0,
        };
        self.verify_input(reader, meta, &mut |_| Ok(()))?;
        if self.store.chunk_count(&meta.digest, meta.generation)? == 0 {
            self.move_staged(staged, &meta.digest, meta.generation)?;
            self.store.drop_staged(staged)?;
        }
        Ok(())
    }

    /// Re-encrypt up to `limit` old-generation objects with the current key.
    /// Metadata generation is the durable progress marker, so a restart resumes
    /// by scanning for the next stale entry.
    pub fn rotate_keys(&self, limit: usize) -> Result<usize> {
        let mut rotated = 0;
        for listed in self.list()? {
            if rotated >= limit {
                break;
            }
            if listed.generation == self.config.generation
                || self.store.chunk_count(&listed.digest, listed.generation)? == 0
            {
                continue;
            }
            let _guard = self.gate.lock().unwrap();
            let Some(current) = self.metadata(&listed.name)? else {
                continue;
            };
            if current.deleted
                || current.digest != listed.digest
                || current.generation == self.config.generation
            {
                continue;
            }
            let transfer = format!(".rotate-{}", hex(&crate::randomness::bytes::<16>())).into_bytes();
            let _active = self.guard_entries(vec![ActiveEntry::Staged(transfer.clone())]);
            let result = (|| -> Result<Metadata> {
                let store = &self.store;
                let staged = transfer.clone();
                let next = Cell::new(0u64);
                let mut seal = SealWriter::new(
                    &self.config.key,
                    self.config.generation,
                    self.config.max_file_bytes,
                    move |bytes: &[u8]| {
                        let idx = next.get();
                        next.set(idx + 1);
                        store.stage_chunk(&staged, idx, bytes)
                    },
                )?;
                let reader = ChunkReader {
                    store: &self.store,
                    digest: current.digest,
                    generation: current.generation,
                    pos: 0,
                };
                self.verify_input(reader, &current, &mut |b| seal.feed(b))?;
                let (digest, total) = seal.finish()?;
                self.move_staged(&transfer, &digest, self.config.generation)?;
                self.store.drop_staged(&transfer)?;
                let meta = Metadata {
                    name: current.name.clone(),
                    digest,
                    bytes: total,
                    generation: self.config.generation,
                    millis: now(),
                    node: self.backend.identity().1,
                    deleted: false,
                };
                self.backend
                    .publish(vec![Record::put(meta_key(&meta.name), encode(&meta)?)])?;
                Ok(meta)
            })();
            let _ = self.store.drop_staged(&transfer);
            result?;
            rotated += 1;
        }
        Ok(rotated)
    }

    /// Report whether this store still contains any object or staged transfer
    /// that may need an older file key. This is a local-store check.
    pub fn key_rotation_status(&self) -> Result<KeyRotationStatus> {
        let mut old_generation_metadata_missing_locally = 0;
        for meta in self.list()? {
            if meta.generation != self.config.generation
                && self.store.chunk_count(&meta.digest, meta.generation)? == 0
            {
                old_generation_metadata_missing_locally += 1;
            }
        }
        let mut old_generation_objects = 0;
        for ((_, generation), _) in self.store.live_objects()? {
            if generation != self.config.generation {
                old_generation_objects += 1;
            }
        }
        let staged_transfers = self.store.expired_staged(u64::MAX)?.len() as u64;
        let previous_key_unused_locally = old_generation_objects == 0
            && old_generation_metadata_missing_locally == 0
            && staged_transfers == 0;
        Ok(KeyRotationStatus {
            current_generation: self.config.generation,
            old_generation_objects,
            old_generation_metadata_missing_locally,
            staged_transfers,
            previous_key_unused_locally,
        })
    }

    /// Collect unreferenced encrypted objects and abandoned staging rows after
    /// a grace period. Metadata, including deletion tombstones, remains in Spool.
    pub fn collect(&self, grace: Duration) -> Result<usize> {
        let _guard = self.gate.lock().unwrap();
        let cutoff = now().saturating_sub(grace.as_millis().min(u64::MAX as u128) as u64);
        let mut live = BTreeSet::new();
        for key in self.backend.keys(b"08/files/meta/")? {
            if let Some(bytes) = self.backend.get(&key)? {
                let meta: Metadata = decode(&bytes)?;
                if !meta.deleted {
                    live.insert((meta.digest, meta.generation));
                }
            }
        }
        let mut removed = 0;
        for (id, created) in self.store.live_objects()? {
            if created > cutoff {
                continue;
            }
            if self
                .active
                .lock()
                .unwrap()
                .contains(&ActiveEntry::Pinned(id))
            {
                continue;
            }
            if !live.contains(&id) {
                self.store.delete_object(&id.0, id.1)?;
                removed += 1;
            }
        }
        for transfer in self.store.expired_staged(cutoff)? {
            if self
                .active
                .lock()
                .unwrap()
                .contains(&ActiveEntry::Staged(transfer.clone()))
            {
                continue;
            }
            self.store.drop_staged(&transfer)?;
            removed += 1;
        }
        Ok(removed)
    }
}

/// Incremental encrypted object writer. Dropping aborts: staged fragments are
/// dropped and no metadata changes, mirroring the archived temp-file removal.
pub struct Upload<'s, S: ChunkStore> {
    name: String,
    seal: Option<SealWriter<'s>>,
    generation: u64,
    reservation: StagingReservation,
    used: Arc<Mutex<u64>>,
    staging_max: u64,
    store: &'s S,
    transfer: Vec<u8>,
    committed: bool,
    _guard: StagedTransfer,
}

impl<'s, S: ChunkStore> Upload<'s, S> {
    fn new(objects: &'s SqliteObjects<S>, name: &str) -> Result<Self> {
        check_name(name)?;
        let reservation = objects.reserve_staging(128)?;
        let transfer = format!(".upload-{}", hex(&crate::randomness::bytes::<16>())).into_bytes();
        let guard = objects.guard_entries(vec![ActiveEntry::Staged(transfer.clone())]);
        let store = &objects.store;
        let staged = transfer.clone();
        let next = Cell::new(0u64);
        let seal = SealWriter::new(
            &objects.config.key,
            objects.config.generation,
            objects.config.max_file_bytes,
            move |bytes: &[u8]| {
                let idx = next.get();
                next.set(idx + 1);
                store.stage_chunk(&staged, idx, bytes)
            },
        )?;
        Ok(Self {
            name: name.into(),
            seal: Some(seal),
            generation: objects.config.generation,
            reservation,
            used: objects.staging_bytes.clone(),
            staging_max: objects.config.max_staging_bytes,
            store,
            transfer,
            committed: false,
            _guard: guard,
        })
    }

    /// Append plaintext, reserving container bytes plus per-frame overhead as
    /// the seal crosses frame boundaries. Reservation math is verbatim.
    pub fn append(&mut self, bytes: &[u8]) -> Result<()> {
        let seal = self.seal.as_mut().expect("upload seal consumed once");
        let total = seal
            .total()
            .checked_add(bytes.len() as u64)
            .ok_or(Error::Overloaded)?;
        if total > seal.max() {
            return Err(Error::Overloaded);
        }
        let reserve = bytes.len() as u64
            + (total.div_ceil(SLICE_BYTES as u64) - seal.total().div_ceil(SLICE_BYTES as u64)) * 20;
        {
            let mut used = self.used.lock().unwrap();
            if reserve > self.staging_max.saturating_sub(*used) {
                return Err(Error::Overloaded);
            }
            *used += reserve;
        }
        self.reservation.bytes += reserve;
        seal.feed(bytes)?;
        Ok(())
    }

    fn finish_inner(mut self, objects: &SqliteObjects<S>) -> Result<Metadata> {
        let Some(seal) = self.seal.take() else {
            return Err(Error::Protocol("upload finished twice".into()));
        };
        let (digest, total) = seal.finish()?;
        if objects.store.chunk_count(&digest, self.generation)? == 0 {
            objects.move_staged(&self.transfer, &digest, self.generation)?;
        }
        objects.store.drop_staged(&self.transfer)?;
        self.committed = true;
        let meta = Metadata {
            name: self.name.clone(),
            digest,
            bytes: total,
            generation: self.generation,
            millis: now(),
            node: objects.backend.identity().1,
            deleted: false,
        };
        objects
            .backend
            .publish(vec![Record::put(meta_key(&meta.name), encode(&meta)?)])?;
        Ok(meta)
    }
}

impl<S: ChunkStore> Drop for Upload<'_, S> {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self.store.drop_staged(&self.transfer);
        }
    }
}

/// A verified, pinned immutable object, consumed in bounded plaintext chunks.
pub struct Download<'s, S: ChunkStore> {
    /// Authenticated metadata; the pin below keeps the slices live.
    pub meta: Metadata,
    verifier: OpenReader<ChunkReader<'s, S>>,
    offset: u64,
    pending: zeroize::Zeroizing<Vec<u8>>,
    pending_pos: usize,
    _pin: StagedTransfer,
}

impl<S: ChunkStore> Download<'_, S> {
    /// Read the next plaintext chunk: `(start_offset, bytes, is_last)`.
    pub fn next(&mut self, limit: usize) -> Result<(u64, Vec<u8>, bool)> {
        if limit == 0 || limit > 1 << 20 {
            return Err(Error::Invalid("file chunk limit".into()));
        }
        let start = self.offset;
        let mut out = Vec::with_capacity(limit);
        while out.len() < limit && self.offset < self.meta.bytes {
            if self.pending.is_empty() {
                use zeroize::Zeroize;
                self.pending.zeroize();
                *self.pending = self.verifier.next_frame()?;
            }
            let n = (limit - out.len()).min(self.pending.len() - self.pending_pos);
            out.extend_from_slice(&self.pending[self.pending_pos..self.pending_pos + n]);
            self.pending_pos += n;
            self.offset += n as u64;
            if self.pending_pos == self.pending.len() {
                use zeroize::Zeroize;
                self.pending.zeroize();
                self.pending_pos = 0;
            }
        }
        Ok((start, out, self.offset == self.meta.bytes))
    }
}

/// Plaintext fan-out for resumed external transfers: skips the exported
/// prefix, then emits bounded `(offset, bytes)` chunks. Verbatim port of the
/// archived writer, including the error capture it reports back to the driver.
struct ChunkWriter<'a, F> {
    skip: u64,
    position: u64,
    total: u64,
    chunk_size: usize,
    buffer: Vec<u8>,
    emit: &'a mut F,
    error: Option<Error>,
}

impl<F: FnMut(u64, &[u8]) -> Result<()>> Write for ChunkWriter<'_, F> {
    fn write(&mut self, mut bytes: &[u8]) -> std::io::Result<usize> {
        let original = bytes.len();
        self.total = self.total.saturating_add(original as u64);
        let discard = (self.skip.min(bytes.len() as u64)) as usize;
        self.skip -= discard as u64;
        bytes = &bytes[discard..];
        while !bytes.is_empty() {
            let take = (self.chunk_size - self.buffer.len()).min(bytes.len());
            self.buffer.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.buffer.len() == self.chunk_size {
                if let Err(error) = (self.emit)(self.position, &self.buffer) {
                    self.error = Some(error);
                    return Err(std::io::Error::other("file chunk consumer failed"));
                }
                self.position += self.buffer.len() as u64;
                self.buffer.clear();
            }
        }
        Ok(original)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory::MemoryStore;
    use std::collections::BTreeMap;

    struct MemBackend {
        data: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
    }

    impl MemBackend {
        fn new() -> Self {
            Self {
                data: Mutex::new(BTreeMap::new()),
            }
        }

        fn store_records(&self, records: Vec<Record>) {
            let mut data = self.data.lock().unwrap();
            for record in records {
                match record.value {
                    Some(value) => {
                        data.insert(record.key, value);
                    }
                    None => {
                        data.remove(&record.key);
                    }
                }
            }
        }
    }

    impl Backend for MemBackend {
        fn identity(&self) -> (NodeId, NodeId) {
            ([0; 16], [9; 16])
        }

        fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            Ok(self.data.lock().unwrap().get(key).cloned())
        }

        fn keys(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
            let mut out: Vec<Vec<u8>> = self
                .data
                .lock()
                .unwrap()
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect();
            out.sort();
            Ok(out)
        }

        fn commit(&self, records: Vec<Record>) -> Result<()> {
            self.store_records(records);
            Ok(())
        }

        fn publish(&self, records: Vec<Record>) -> Result<()> {
            self.store_records(records);
            Ok(())
        }

        fn import_bridge(
            &self,
            _header: murmur_tx::bridge::Header,
            _payload: Vec<u8>,
        ) -> Result<bool> {
            Ok(false)
        }

        fn release_ownership(
            &self,
            _table: String,
            _key: Vec<u8>,
            _column: Option<String>,
        ) -> Result<bool> {
            Ok(false)
        }

        fn snapshot(&self) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }

        fn snapshot_with_progress(&self) -> Result<(Vec<u8>, Vec<murmur_tx::protocol::Progress>)> {
            Ok((Vec::new(), Vec::new()))
        }

        fn retention_checkpoint(&self) -> Result<()> {
            Ok(())
        }

        fn prune_history(&self, _origin: NodeId, _through: u64) -> Result<u64> {
            Ok(0)
        }

        fn reclaim_retention(&self) -> Result<()> {
            Ok(())
        }

        fn schema(&self) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }

        fn apply(&self, _payloads: Vec<Vec<u8>>, _records: Vec<Record>) -> Result<bool> {
            Ok(true)
        }

        fn observe(&self, _millis: u64, _logical: u32) -> Result<()> {
            Ok(())
        }

        fn accept_schema(&self, _schema: Vec<u8>) -> Result<bool> {
            Ok(true)
        }
    }

    fn key(seed: u8) -> [u8; 32] {
        [seed; 32]
    }

    /// Deterministic pseudo-random stream (xorshift; no fixture I/O).
    fn plaintext(len: usize, seed: u64) -> Vec<u8> {
        let mut x = seed | 1;
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x >> 33) as u8
            })
            .collect()
    }

    fn open(store: Arc<MemoryStore>, backend: Arc<MemBackend>) -> SqliteObjects<Arc<MemoryStore>> {
        SqliteObjects::open(ObjectsConfig::new(key(7)), backend, store).unwrap()
    }

    #[test]
    fn chunk_round_trip_multi_slice_plus_resume() {
        let backend = Arc::new(MemBackend::new());
        let store = Arc::new(MemoryStore::new());
        let objects = open(store.clone(), backend);
        // Frame-boundary sizes plus a multi-slice body; every size must seal
        // to exactly the predicted slice count and read back byte-exact.
        for (i, len) in [0, 1, 127, 128, 1000, 65535, 65536, 65537, 200 * 1024]
            .into_iter()
            .enumerate()
        {
            let name = format!("obj-{i}");
            let plain = plaintext(len, 0x1234 + i as u64);
            let meta = objects.upload(&name, plain.as_slice()).unwrap();
            assert_eq!(meta.bytes, len as u64);
            assert_eq!(meta.generation, 0);
            assert!(!meta.deleted);
            let expected = crate::crypto::expected_slices(len as u64).unwrap();
            assert_eq!(
                store.chunk_count(&meta.digest, 0).unwrap(),
                expected,
                "slice count for {len} bytes"
            );
            let mut out = Vec::new();
            let back = objects.read(&name, &mut out).unwrap();
            assert_eq!(back.digest, meta.digest);
            assert_eq!(out, plain, "round-trip for {len} bytes");
            assert!(objects.available(&name).unwrap());
        }
        // Chunked download assembles the multi-slice body exactly.
        let plain = plaintext(200 * 1024, 0x1234 + 8);
        let mut download = objects.download("obj-8").unwrap();
        let mut assembled = Vec::new();
        loop {
            let (_, chunk, last) = download.next(50_000).unwrap();
            assembled.extend_from_slice(&chunk);
            if last {
                break;
            }
        }
        assert_eq!(assembled, plain);
        // Resume-style skip streams the tail only, with absolute offsets.
        let mut tail = Vec::new();
        let (streamed, total) = objects
            .stream_plain_chunks("obj-8", 1000, 8192, |at, data| {
                assert_eq!(at, 1000 + tail.len() as u64);
                tail.extend_from_slice(data);
                Ok(())
            })
            .unwrap();
        assert_eq!(streamed.bytes, plain.len() as u64);
        assert_eq!(total, plain.len() as u64);
        assert_eq!(tail, &plain[1000..]);
        assert_eq!(objects.availability("obj-8").unwrap(), Availability::Local);
    }

    #[test]
    fn digest_and_length_auth_failures() {
        let backend = Arc::new(MemBackend::new());
        let store = Arc::new(MemoryStore::new());
        let objects = open(store.clone(), backend.clone());
        let plain = plaintext(90 * 1024, 99);
        let meta = objects.upload("doc", plain.as_slice()).unwrap();
        // Tampered slice fails closed on read.
        store
            .put_chunk(&meta.digest, 0, 0, &vec![0xAA; 1024])
            .unwrap();
        assert!(matches!(
            objects.read("doc", &mut Vec::new()),
            Err(Error::Auth(_))
        ));
        // Fresh object for metadata-tamper cases (slices key on digest).
        let meta2 = objects
            .upload("doc2", plaintext(1024, 7).as_slice())
            .unwrap();
        // Registered length mismatch fails closed.
        let mut bad = meta2.clone();
        bad.bytes += 1;
        backend
            .publish(vec![Record::put(meta_key("doc2"), encode(&bad).unwrap())])
            .unwrap();
        assert!(matches!(
            objects.read("doc2", &mut Vec::new()),
            Err(Error::Auth(_))
        ));
        // Registered digest mismatch reads as a missing payload, like the
        // archived path miss.
        let mut bad2 = meta2;
        bad2.digest[0] ^= 0xFF;
        backend
            .publish(vec![Record::put(meta_key("doc2"), encode(&bad2).unwrap())])
            .unwrap();
        assert!(matches!(
            objects.read("doc2", &mut Vec::new()),
            Err(Error::Unavailable(_))
        ));
    }

    #[test]
    fn rotation_across_generations() {
        let backend = Arc::new(MemBackend::new());
        let store = Arc::new(MemoryStore::new());
        let gen0 = open(store.clone(), backend.clone());
        let plain_a = plaintext(70_000, 1);
        let plain_b = plaintext(300, 2);
        let a0 = gen0.upload("a", plain_a.as_slice()).unwrap();
        gen0.upload("b", plain_b.as_slice()).unwrap();
        drop(gen0);
        let mut cfg1 = ObjectsConfig::new(key(8));
        cfg1.generation = 1;
        cfg1.previous_key = Some(zeroize::Zeroizing::new(key(7)));
        let gen1 = SqliteObjects::open(cfg1, backend.clone(), store.clone()).unwrap();
        assert_eq!(gen1.rotate_keys(10).unwrap(), 2);
        // Metadata advances a generation; digests are stable (same plaintext).
        let a1 = gen1.metadata("a").unwrap().unwrap();
        assert_eq!(a1.generation, 1);
        assert_eq!(a1.digest, a0.digest);
        let mut out = Vec::new();
        gen1.read("a", &mut out).unwrap();
        assert_eq!(out, plain_a);
        let mut out = Vec::new();
        gen1.read("b", &mut out).unwrap();
        assert_eq!(out, plain_b);
        // Old-generation slices stay until collected; the progress marker in
        // metadata makes a second rotation a no-op.
        assert!(store.chunk_count(&a0.digest, 0).unwrap() > 0);
        assert_eq!(gen1.rotate_keys(10).unwrap(), 0);
        assert!(gen1.collect(Duration::ZERO).unwrap() >= 2);
        assert_eq!(store.chunk_count(&a0.digest, 0).unwrap(), 0);
        // Opening a new generation without the previous key fails while old
        // metadata exists, and generations rotate one step at a time.
        let backend2 = Arc::new(MemBackend::new());
        let g0 = open(Arc::new(MemoryStore::new()), backend2.clone());
        g0.upload("x", plaintext(10, 3).as_slice()).unwrap();
        drop(g0);
        let mut missing_prev = ObjectsConfig::new(key(8));
        missing_prev.generation = 1;
        assert!(matches!(
            SqliteObjects::open(missing_prev, backend2.clone(), Arc::new(MemoryStore::new())),
            Err(Error::Invalid(_))
        ));
        let mut skipped = ObjectsConfig::new(key(9));
        skipped.generation = 2;
        skipped.previous_key = Some(zeroize::Zeroizing::new(key(8)));
        assert!(matches!(
            SqliteObjects::open(skipped, backend2, Arc::new(MemoryStore::new())),
            Err(Error::Invalid(_))
        ));
    }

    #[test]
    fn collect_orphans_and_expired_staging() {
        let backend = Arc::new(MemBackend::new());
        let store = Arc::new(MemoryStore::new());
        let objects = open(store.clone(), backend);
        let meta = objects
            .upload("temp", plaintext(5000, 5).as_slice())
            .unwrap();
        // Deletion tombstones the name; the payload becomes an orphan.
        objects.delete("temp").unwrap();
        assert!(!objects.available("temp").unwrap());
        assert_eq!(objects.collect(Duration::ZERO).unwrap(), 1);
        assert_eq!(store.chunk_count(&meta.digest, 0).unwrap(), 0);
        // Live objects survive collection.
        let live = objects
            .upload("live", plaintext(100, 6).as_slice())
            .unwrap();
        assert_eq!(objects.collect(Duration::ZERO).unwrap(), 0);
        assert!(store.chunk_count(&live.digest, 0).unwrap() > 0);
        // Abandoned staging is reaped; guarded staging survives.
        store.stage_chunk(b"abandoned", 0, b"partial").unwrap();
        store.stage_chunk(b"active", 0, b"partial").unwrap();
        let guard = objects.protect_staging(b"active".to_vec());
        assert_eq!(objects.collect(Duration::ZERO).unwrap(), 1);
        assert_eq!(store.staged_count(b"abandoned").unwrap(), 0);
        assert_eq!(store.staged_count(b"active").unwrap(), 7);
        drop(guard);
        assert_eq!(objects.collect(Duration::ZERO).unwrap(), 1);
        assert_eq!(store.staged_count(b"active").unwrap(), 0);
        // A pinned download survives deletion plus collection, like the
        // archived hard-link pin; releasing the pin reaps it.
        let download = objects.download("live").unwrap();
        objects.delete("live").unwrap();
        assert_eq!(objects.collect(Duration::ZERO).unwrap(), 0);
        drop(download);
        assert_eq!(objects.collect(Duration::ZERO).unwrap(), 1);
        assert_eq!(store.chunk_count(&live.digest, 0).unwrap(), 0);
    }

    #[test]
    fn rotation_status_counts() {
        let backend = Arc::new(MemBackend::new());
        let store = Arc::new(MemoryStore::new());
        let gen0 = open(store.clone(), backend.clone());
        gen0.upload("a", plaintext(1000, 11).as_slice()).unwrap();
        // Registered but never fetched: metadata without a payload.
        backend
            .publish(vec![Record::put(
                meta_key("ghost"),
                encode(&Metadata {
                    name: "ghost".into(),
                    digest: [0x42; 32],
                    bytes: 10,
                    generation: 0,
                    millis: 0,
                    node: [0; 16],
                    deleted: false,
                })
                .unwrap(),
            )])
            .unwrap();
        drop(gen0);
        let mut cfg1 = ObjectsConfig::new(key(8));
        cfg1.generation = 1;
        cfg1.previous_key = Some(zeroize::Zeroizing::new(key(7)));
        let gen1 = SqliteObjects::open(cfg1, backend.clone(), store.clone()).unwrap();
        let status = gen1.key_rotation_status().unwrap();
        assert_eq!(status.current_generation, 1);
        assert_eq!(status.old_generation_objects, 1);
        assert_eq!(status.old_generation_metadata_missing_locally, 1);
        assert_eq!(status.staged_transfers, 0);
        assert!(!status.previous_key_unused_locally);
        store.stage_chunk(b"orphan-transfer", 0, b"bytes").unwrap();
        assert_eq!(gen1.key_rotation_status().unwrap().staged_transfers, 1);
        // Rotation skips the unfetchable ghost; only its tombstone clears the
        // missing-locally count.
        gen1.rotate_keys(10).unwrap();
        gen1.collect(Duration::ZERO).unwrap();
        let mid = gen1.key_rotation_status().unwrap();
        assert_eq!(mid.old_generation_objects, 0);
        assert_eq!(mid.old_generation_metadata_missing_locally, 1);
        assert!(!mid.previous_key_unused_locally);
        gen1.delete("ghost").unwrap();
        assert!(
            gen1.key_rotation_status()
                .unwrap()
                .previous_key_unused_locally
        );
    }
}
