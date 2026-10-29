//! SQLite-backed payload seam: the `ChunkStore` trait plus an in-memory
//! implementation for unit tests.
//!
//! Rows are opaque 64KiB container slices; framing and crypto live in
//! [`crate::crypto`] and never see this trait. The production `rusqlite`
//! implementation lives in `murmur-replication` (lead wiring); this crate
//! must not depend on `rusqlite`.
//!
//! All methods take `&self` like [`murmur_tx::Backend`]: implementations hold
//! interior mutability (a mutex around the SQLite connection, as the lead's
//! storage worker serializes access today).

use murmur_tx::{Error, Result};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// Container slice size: one row per slice, matching the 64KiB sealed frame
/// plaintext unit so steady-state objects hold one row per frame plus header.
pub const SLICE_BYTES: usize = 64 << 10;

/// Object identity: plaintext digest plus the sealing key generation.
pub type ObjectId = ([u8; 32], u64);

/// Byte-slice payload store behind [`crate::objects::SqliteObjects`].
///
/// Object slices are content-addressed by `(digest, generation, idx)` and are
/// 64KiB-aligned: slice `idx` covers container bytes
/// `[idx * SLICE_BYTES, (idx + 1) * SLICE_BYTES)`, except the last slice of an
/// object, which may be short. Staging rows are append-ordered fragments that
/// concatenate in `idx` order; callers must not assume row alignment there.
pub trait ChunkStore: Send + Sync {
    /// Store one object slice. Overwriting an identical slice is a no-op;
    /// slices are immutable once the object is published.
    fn put_chunk(&self, digest: &[u8; 32], generation: u64, idx: u64, bytes: &[u8]) -> Result<()>;

    /// Read up to `len` container bytes starting at `offset`, assembling
    /// across slice rows. Returns a short or empty vector at the end.
    fn read_stream(
        &self,
        digest: &[u8; 32],
        generation: u64,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>>;

    /// Count object slices. Zero means the object payload is absent, whatever
    /// the metadata says.
    fn chunk_count(&self, digest: &[u8; 32], generation: u64) -> Result<u64>;

    /// Delete every slice of one object. Missing objects are a no-op.
    fn delete_object(&self, digest: &[u8; 32], generation: u64) -> Result<()>;

    /// List every stored object with the creation time (millis since the Unix
    /// epoch) of its first slice. Collection uses the timestamp as the grace
    /// cutoff for just-sealed objects whose metadata is not yet published, so
    /// the production table must record first-slice time.
    fn live_objects(&self) -> Result<Vec<(ObjectId, u64)>>;

    /// Append one staging fragment. Fragments concatenate in `idx` order and
    /// need not be slice-aligned; the first append records the transfer's
    /// creation time for the collection grace cutoff.
    fn stage_chunk(&self, transfer: &[u8], idx: u64, bytes: &[u8]) -> Result<()>;

    /// Total staged bytes for one transfer; doubles as the resume offset.
    fn staged_count(&self, transfer: &[u8]) -> Result<u64>;

    /// Read up to `len` staged bytes starting at `offset`, walking fragments
    /// in order. Returns a short or empty vector at the end.
    fn read_staged(&self, transfer: &[u8], offset: u64, len: usize) -> Result<Vec<u8>>;

    /// Drop every fragment of one transfer. Missing transfers are a no-op.
    fn drop_staged(&self, transfer: &[u8]) -> Result<()>;

    /// List staged transfer ids created at or before `cutoff` millis. Passing
    /// `u64::MAX` lists every live transfer (used for status counts).
    fn expired_staged(&self, cutoff: u64) -> Result<Vec<Vec<u8>>>;

    /// Total sealed-container bytes for one object, or `None` when the
    /// object has no slices. The fetch serve path needs the length upfront
    /// for the stream header; it must equal what `read_stream` yields from
    /// zero, so only the dense-from-zero prefix counts (a gap ends it,
    /// like [`ChunkStore::chunk_count`]).
    fn container_len(&self, digest: &[u8; 32], generation: u64) -> Result<Option<u64>>;

    /// Resume identity of a live staged transfer, if the source header has
    /// been seen. Fetch stores the 32-byte source-container identity here
    /// on first contact; a resumed transfer whose staged bytes lack an
    /// identity is discarded (crash rule: bytes without provenance are
    /// never resumed). `None` for unknown transfers too.
    fn staged_identity(&self, transfer: &[u8]) -> Result<Option<[u8; 32]>>;

    /// Record the source-container identity for a staged transfer,
    /// creating the transfer record when missing: the source header
    /// precedes the first fragment, so the row may not exist yet.
    /// Overwrites any previous identity.
    fn set_staged_identity(&self, transfer: &[u8], identity: &[u8; 32]) -> Result<()>;
}

/// Shared stores: reopening under a rotated config over the same rows, and a
/// future fetch service sharing the store with the object surface. `?Sized`
/// covers trait objects, so hosts hold `Arc<dyn ChunkStore>` without generics.
impl<S: ChunkStore + ?Sized> ChunkStore for Arc<S> {
    fn put_chunk(&self, digest: &[u8; 32], generation: u64, idx: u64, bytes: &[u8]) -> Result<()> {
        (**self).put_chunk(digest, generation, idx, bytes)
    }

    fn read_stream(
        &self,
        digest: &[u8; 32],
        generation: u64,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>> {
        (**self).read_stream(digest, generation, offset, len)
    }

    fn chunk_count(&self, digest: &[u8; 32], generation: u64) -> Result<u64> {
        (**self).chunk_count(digest, generation)
    }

    fn delete_object(&self, digest: &[u8; 32], generation: u64) -> Result<()> {
        (**self).delete_object(digest, generation)
    }

    fn live_objects(&self) -> Result<Vec<(ObjectId, u64)>> {
        (**self).live_objects()
    }

    fn stage_chunk(&self, transfer: &[u8], idx: u64, bytes: &[u8]) -> Result<()> {
        (**self).stage_chunk(transfer, idx, bytes)
    }

    fn staged_count(&self, transfer: &[u8]) -> Result<u64> {
        (**self).staged_count(transfer)
    }

    fn read_staged(&self, transfer: &[u8], offset: u64, len: usize) -> Result<Vec<u8>> {
        (**self).read_staged(transfer, offset, len)
    }

    fn drop_staged(&self, transfer: &[u8]) -> Result<()> {
        (**self).drop_staged(transfer)
    }

    fn expired_staged(&self, cutoff: u64) -> Result<Vec<Vec<u8>>> {
        (**self).expired_staged(cutoff)
    }

    fn container_len(&self, digest: &[u8; 32], generation: u64) -> Result<Option<u64>> {
        (**self).container_len(digest, generation)
    }

    fn staged_identity(&self, transfer: &[u8]) -> Result<Option<[u8; 32]>> {
        (**self).staged_identity(transfer)
    }

    fn set_staged_identity(&self, transfer: &[u8], identity: &[u8; 32]) -> Result<()> {
        (**self).set_staged_identity(transfer, identity)
    }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// In-memory [`ChunkStore`] for unit tests. Not for production: payloads must
/// rest in SQLite rows (constraint 9), never in process memory.
pub mod memory {
    use super::*;

    struct Staged {
        created_at: u64,
        fragments: BTreeMap<u64, Vec<u8>>,
        identity: Option<[u8; 32]>,
    }

    /// Test-only store. Every method locks one mutex; production serializes
    /// through the storage worker instead.
    pub struct MemoryStore {
        chunks: Mutex<BTreeMap<(ObjectId, u64), Vec<u8>>>,
        created: Mutex<BTreeMap<ObjectId, u64>>,
        staged: Mutex<BTreeMap<Vec<u8>, Staged>>,
    }

    impl MemoryStore {
        /// Empty store with no objects and no staged transfers.
        pub fn new() -> Self {
            Self {
                chunks: Mutex::new(BTreeMap::new()),
                created: Mutex::new(BTreeMap::new()),
                staged: Mutex::new(BTreeMap::new()),
            }
        }
    }

    impl Default for MemoryStore {
        fn default() -> Self {
            Self::new()
        }
    }

    impl ChunkStore for MemoryStore {
        fn put_chunk(
            &self,
            digest: &[u8; 32],
            generation: u64,
            idx: u64,
            bytes: &[u8],
        ) -> Result<()> {
            self.chunks
                .lock()
                .unwrap()
                .insert(((*digest, generation), idx), bytes.to_vec());
            self.created
                .lock()
                .unwrap()
                .entry((*digest, generation))
                .or_insert_with(now_millis);
            Ok(())
        }

        fn read_stream(
            &self,
            digest: &[u8; 32],
            generation: u64,
            offset: u64,
            len: usize,
        ) -> Result<Vec<u8>> {
            let chunks = self.chunks.lock().unwrap();
            let mut out = Vec::new();
            let mut pos = offset;
            let end = offset.saturating_add(len as u64);
            while pos < end {
                let idx = pos / SLICE_BYTES as u64;
                let within = (pos % SLICE_BYTES as u64) as usize;
                let Some(row) = chunks.get(&((*digest, generation), idx)) else {
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
        }

        fn chunk_count(&self, digest: &[u8; 32], generation: u64) -> Result<u64> {
            let chunks = self.chunks.lock().unwrap();
            let mut count = 0u64;
            // Slices are dense from zero; the first gap ends the object.
            while chunks.contains_key(&((*digest, generation), count)) {
                count += 1;
            }
            Ok(count)
        }

        fn delete_object(&self, digest: &[u8; 32], generation: u64) -> Result<()> {
            let mut chunks = self.chunks.lock().unwrap();
            let mut idx = 0u64;
            while chunks.remove(&((*digest, generation), idx)).is_some() {
                idx += 1;
            }
            self.created.lock().unwrap().remove(&(*digest, generation));
            Ok(())
        }

        fn live_objects(&self) -> Result<Vec<(ObjectId, u64)>> {
            let created = self.created.lock().unwrap();
            let mut out: Vec<(ObjectId, u64)> = created.iter().map(|(id, at)| (*id, *at)).collect();
            out.sort();
            Ok(out)
        }

        fn stage_chunk(&self, transfer: &[u8], idx: u64, bytes: &[u8]) -> Result<()> {
            if bytes.is_empty() {
                return Err(Error::Invalid("empty staged fragment".into()));
            }
            self.staged
                .lock()
                .unwrap()
                .entry(transfer.to_vec())
                .or_insert_with(|| Staged {
                    created_at: now_millis(),
                    fragments: BTreeMap::new(),
                    identity: None,
                })
                .fragments
                .insert(idx, bytes.to_vec());
            Ok(())
        }

        fn staged_count(&self, transfer: &[u8]) -> Result<u64> {
            let staged = self.staged.lock().unwrap();
            Ok(staged
                .get(transfer)
                .map(|t| t.fragments.values().map(|f| f.len() as u64).sum())
                .unwrap_or(0))
        }

        fn read_staged(&self, transfer: &[u8], offset: u64, len: usize) -> Result<Vec<u8>> {
            let staged = self.staged.lock().unwrap();
            let mut out = Vec::new();
            let Some(t) = staged.get(transfer) else {
                return Ok(out);
            };
            let mut pos = 0u64;
            let end = offset.saturating_add(len as u64);
            for fragment in t.fragments.values() {
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
        }

        fn drop_staged(&self, transfer: &[u8]) -> Result<()> {
            self.staged.lock().unwrap().remove(transfer);
            Ok(())
        }

        fn expired_staged(&self, cutoff: u64) -> Result<Vec<Vec<u8>>> {
            let staged = self.staged.lock().unwrap();
            let mut out: Vec<Vec<u8>> = staged
                .iter()
                .filter(|(_, t)| t.created_at <= cutoff)
                .map(|(id, _)| id.clone())
                .collect();
            out.sort();
            Ok(out)
        }

        fn container_len(&self, digest: &[u8; 32], generation: u64) -> Result<Option<u64>> {
            let chunks = self.chunks.lock().unwrap();
            // Dense-from-zero only, matching `chunk_count` and `read_stream`.
            if !chunks.contains_key(&((*digest, generation), 0)) {
                return Ok(None);
            }
            let mut total = 0u64;
            let mut idx = 0u64;
            while let Some(row) = chunks.get(&((*digest, generation), idx)) {
                total += row.len() as u64;
                idx += 1;
            }
            Ok(Some(total))
        }

        fn staged_identity(&self, transfer: &[u8]) -> Result<Option<[u8; 32]>> {
            Ok(self
                .staged
                .lock()
                .unwrap()
                .get(transfer)
                .and_then(|t| t.identity))
        }

        fn set_staged_identity(&self, transfer: &[u8], identity: &[u8; 32]) -> Result<()> {
            self.staged
                .lock()
                .unwrap()
                .entry(transfer.to_vec())
                .or_insert_with(|| Staged {
                    created_at: now_millis(),
                    fragments: BTreeMap::new(),
                    identity: None,
                })
                .identity = Some(*identity);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::memory::MemoryStore;
    use super::*;

    #[test]
    fn container_len_and_identity_round_trip() {
        let store = MemoryStore::new();
        let digest = [0x20u8; 32];
        assert_eq!(store.container_len(&digest, 1).unwrap(), None);
        assert_eq!(store.staged_identity(b"t").unwrap(), None);
        // Identity upserts before any fragment lands.
        store.set_staged_identity(b"t", &[7u8; 32]).unwrap();
        assert_eq!(store.staged_identity(b"t").unwrap(), Some([7u8; 32]));
        store.drop_staged(b"t").unwrap();
        assert_eq!(store.staged_identity(b"t").unwrap(), None);
        // Dense-prefix length only; the gap at 2 ends the served prefix.
        let full = vec![0x41u8; SLICE_BYTES];
        store.put_chunk(&digest, 1, 0, &full).unwrap();
        store.put_chunk(&digest, 1, 1, b"bb").unwrap();
        store.put_chunk(&digest, 1, 3, b"c").unwrap();
        assert_eq!(
            store.container_len(&digest, 1).unwrap(),
            Some(SLICE_BYTES as u64 + 2)
        );
        store.put_chunk(&digest, 1, 2, b"dd").unwrap();
        assert_eq!(
            store.container_len(&digest, 1).unwrap(),
            Some(SLICE_BYTES as u64 + 5)
        );
        store.delete_object(&digest, 1).unwrap();
        assert_eq!(store.container_len(&digest, 1).unwrap(), None);
    }
}
