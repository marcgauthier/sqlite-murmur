//! Fetch-service backend: [`FetchObjects`] over [`SqliteObjects`].
//!
//! This module is the WP-C fetch-split sqlite side (grant d-ii): it binds the
//! canonical `murmur_tx::filefetch` service loop to SQLite-backed payload
//! rows. Serve reads sealed containers through the store; fetch stages peer
//! bytes in transfer rows, authenticates them, and publishes verified
//! containers into object slices. The FF/1 wire protocol, quota math, and
//! retry discipline stay in `murmur-tx`; only payload addressing changes.
//!
//! Contract (all landed, ai-comms.md 1119-1122 sketch + 1133 approval):
//! - Trait: `murmur_tx::filefetch::FetchObjects` (generic `Service<O>`,
//!   `transfer_id` default keeps the `hex-hex.gN` stem).
//! - Store access: `SqliteObjects::store()` (`pub(crate)`, added for this
//!   backend) composes serve/fetch primitives directly from the [`ChunkStore`]
//!   seam — no pass-through layer.
//! - Staged identities are typed `Option<[u8; 32]>`; a recorded identity is
//!   always complete, so the resume rule keys on presence, not length.
//! - `discard_staged` drops fragments plus the transfer row (identity with
//!   it) and is a no-op when absent, matching the sqlite drop contract.
//! - Fragment indexes need strict increase per transfer, not density (both
//!   stores concatenate in idx order): appends key by the staged byte total,
//!   which non-empty appends keep strictly increasing, including across
//!   restarts. Single-writer per transfer holds by the service's digest
//!   guard, mirroring the archived append path.
//!
//! [`SqliteObjects`]: crate::objects::SqliteObjects
//! [`FetchObjects`]: murmur_tx::filefetch::FetchObjects

use crate::objects::{SqliteObjects, StagedTransfer, StagingReservation};
use crate::store::ChunkStore;
use murmur_tx::filefetch::FetchObjects;
use murmur_tx::files::Metadata;
use murmur_tx::{Error, NodeId, Result};
use std::sync::Arc;

/// Serve + fetch backend over SQLite object slices and staging rows.
///
/// Generic over the store so unit tests exercise the same code as
/// production: tests use `MemoryStore`, the host wires
/// `SqliteObjects<Arc<dyn ChunkStore>>`. Delegates use fully-qualified
/// inherent calls, mirroring the canonical filesystem backend.
impl<S: ChunkStore + 'static> FetchObjects for SqliteObjects<S> {
    /// Staging quota guard; released on drop like the archived reservation.
    type Reservation = StagingReservation;
    /// Collector guard over pins and staged transfers.
    type Guard = StagedTransfer;

    fn list(&self) -> Result<Vec<Metadata>> {
        SqliteObjects::list(self)
    }

    fn available(&self, name: &str) -> Result<bool> {
        SqliteObjects::available(self, name)
    }

    fn fetch_error(&self, error: &Error) {
        SqliteObjects::fetch_error(self, error)
    }

    fn reserve_staging(&self, bytes: u64) -> Result<Self::Reservation> {
        SqliteObjects::reserve_staging(self, bytes)
    }

    fn record_peer_availability(
        &self,
        digest: [u8; 32],
        generation: u64,
        node: NodeId,
        available: bool,
    ) -> Result<()> {
        SqliteObjects::record_peer_availability(self, digest, generation, node, available)
    }

    fn container_len(&self, digest: [u8; 32], generation: u64) -> Result<Option<u64>> {
        // Serve-side probe stays total: every store failure reads as absent,
        // exactly like the filesystem backend (the archived serve answered
        // Missing on every open failure). Backends must not diverge on wire.
        match self.store().container_len(&digest, generation) {
            Ok(len) => Ok(len),
            Err(_) => Ok(None),
        }
    }

    fn read_container(
        &self,
        digest: [u8; 32],
        generation: u64,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>> {
        self.store().read_stream(&digest, generation, offset, len)
    }

    fn resume_offset(&self, transfer: &[u8]) -> Result<u64> {
        // Crash rule: staged bytes without a recorded source identity cannot
        // be resumed, so fragments orphaned by a crash restart the transfer
        // from zero instead of resuming unauthenticated bytes.
        let store = self.store();
        let staged = store.staged_count(transfer)?;
        if staged > 0 && store.staged_identity(transfer)?.is_none() {
            store.drop_staged(transfer)?;
            return Ok(0);
        }
        Ok(staged)
    }

    fn staged_identity(&self, transfer: &[u8]) -> Result<Option<[u8; 32]>> {
        self.store().staged_identity(transfer)
    }

    fn set_staged_identity(&self, transfer: &[u8], identity: &[u8; 32]) -> Result<()> {
        self.store().set_staged_identity(transfer, identity)
    }

    fn append_staged(&self, transfer: &[u8], bytes: &[u8]) -> Result<()> {
        // The store rejects empty fragments; an empty wire chunk carries no
        // bytes, so skipping it preserves append semantics exactly.
        if bytes.is_empty() {
            return Ok(());
        }
        let store = self.store();
        let idx = store.staged_count(transfer)?;
        store.stage_chunk(transfer, idx, bytes)
    }

    fn publish_staged(&self, transfer: &[u8], meta: &Metadata) -> Result<()> {
        // Poisoned-container rule: an authentication failure must not leave
        // a complete staged container that later requests resume at its end.
        if let Err(error) = SqliteObjects::publish_fetched(self, transfer, meta) {
            let _ = <Self as FetchObjects>::discard_staged(self, transfer);
            return Err(error);
        }
        Ok(())
    }

    fn discard_staged(&self, transfer: &[u8]) -> Result<()> {
        self.store().drop_staged(transfer)
    }

    fn protect_staging(self: &Arc<Self>, transfer: &[u8]) -> Self::Guard {
        // Deref to the inherent `&self` guard explicitly: the trait method's
        // own `&Arc<Self>` receiver would otherwise win resolution.
        let inner: &Self = self;
        SqliteObjects::protect_staging(inner, transfer.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::objects::ObjectsConfig;
    use crate::store::ObjectId;
    use crate::store::SLICE_BYTES;
    use crate::store::memory::MemoryStore;
    use crate::stub::Stub;
    use murmur_tx::hex;
    use std::sync::Arc;

    /// Route through the trait impl exactly like the fetch service does.
    /// Inherent methods would win plain method-call resolution, so tests use
    /// fully-qualified dispatch to prove the trait bodies, not the surface
    /// they delegate to. (`protect_staging` takes `&Arc<Self>` and is called
    /// directly at its one use site instead.)
    macro_rules! fetch {
        ($o:expr, $m:ident ( $($a:expr),* $(,)? )) => {
            <SqliteObjects<MemoryStore> as FetchObjects>::$m(&$o, $($a),*)
        };
    }

    fn backend() -> Arc<Stub> {
        Arc::new(Stub::new())
    }

    fn objects(store: MemoryStore, backend: Arc<Stub>) -> SqliteObjects<MemoryStore> {
        SqliteObjects::open(ObjectsConfig::new([7; 32]), backend, store).unwrap()
    }

    /// Approved transfer-id stem: `{source_hex}-{digest_hex}.g{generation}`.
    fn transfer_id(source: &NodeId, digest: &[u8; 32], generation: u64) -> Vec<u8> {
        format!("{}-{}.g{}", hex(source), hex(digest), generation).into_bytes()
    }

    fn plaintext(n: usize) -> Vec<u8> {
        (0..n).map(|i| (i % 251) as u8).collect()
    }

    #[test]
    fn transfer_id_matches_canonical_default() {
        let source = [1; 16];
        let digest = [2; 32];
        let expected = format!("{}-{}.g3", "01".repeat(16), "02".repeat(32)).into_bytes();
        // The real provided default pins the stem; the local helper must
        // build byte-identical ids for every other test here.
        let canonical =
            <SqliteObjects<MemoryStore> as FetchObjects>::transfer_id(source, digest, 3);
        assert_eq!(canonical, expected);
        assert_eq!(transfer_id(&source, &digest, 3), expected);
    }

    #[test]
    fn serve_sealed_round_trip_across_slices() {
        let objs = objects(MemoryStore::new(), backend());
        let plain = plaintext(SLICE_BYTES * 2 + 12_345);
        let meta = objs.upload("big", plain.as_slice()).unwrap();

        let len = fetch!(objs, container_len(meta.digest, meta.generation))
            .unwrap()
            .expect("sealed object reports its container length");
        assert!(len > plain.len() as u64, "framing overhead present");

        let full = fetch!(
            objs,
            read_container(meta.digest, meta.generation, 0, len as usize)
        )
        .unwrap();
        assert_eq!(full.len() as u64, len);

        // Slices reassemble across the 64KiB boundary.
        let cut = (SLICE_BYTES + 7) as u64;
        let head = fetch!(
            objs,
            read_container(meta.digest, meta.generation, 0, cut as usize)
        )
        .unwrap();
        let tail = fetch!(
            objs,
            read_container(meta.digest, meta.generation, cut, len as usize)
        )
        .unwrap();
        assert_eq!([head, tail].concat(), full);

        // Reads past the end are short, never an error.
        let past = fetch!(objs, read_container(meta.digest, meta.generation, len, 64)).unwrap();
        assert!(past.is_empty());
    }

    #[test]
    fn serve_missing_is_none_and_empty() {
        let objs = objects(MemoryStore::new(), backend());
        assert!(fetch!(objs, container_len([9; 32], 0)).unwrap().is_none());
        assert!(
            fetch!(objs, read_container([9; 32], 0, 0, 64))
                .unwrap()
                .is_empty()
        );
    }

    /// Store that fails length probes: pins the serve-totality redirect —
    /// every `container_len` store error reads as absent, exactly like the
    /// filesystem backend. All other methods are unreachable: `open` on a
    /// fresh backend touches the metadata store only, and the probe under
    /// test calls `container_len` alone.
    struct FailingStore;

    impl ChunkStore for FailingStore {
        fn put_chunk(&self, _: &[u8; 32], _: u64, _: u64, _: &[u8]) -> Result<()> {
            unreachable!("failing store: put_chunk untouched")
        }

        fn read_stream(&self, _: &[u8; 32], _: u64, _: u64, _: usize) -> Result<Vec<u8>> {
            unreachable!("failing store: read_stream untouched")
        }

        fn chunk_count(&self, _: &[u8; 32], _: u64) -> Result<u64> {
            unreachable!("failing store: chunk_count untouched")
        }

        fn delete_object(&self, _: &[u8; 32], _: u64) -> Result<()> {
            unreachable!("failing store: delete_object untouched")
        }

        fn live_objects(&self) -> Result<Vec<(ObjectId, u64)>> {
            unreachable!("failing store: live_objects untouched")
        }

        fn stage_chunk(&self, _: &[u8], _: u64, _: &[u8]) -> Result<()> {
            unreachable!("failing store: stage_chunk untouched")
        }

        fn staged_count(&self, _: &[u8]) -> Result<u64> {
            unreachable!("failing store: staged_count untouched")
        }

        fn read_staged(&self, _: &[u8], _: u64, _: usize) -> Result<Vec<u8>> {
            unreachable!("failing store: read_staged untouched")
        }

        fn drop_staged(&self, _: &[u8]) -> Result<()> {
            unreachable!("failing store: drop_staged untouched")
        }

        fn expired_staged(&self, _: u64) -> Result<Vec<Vec<u8>>> {
            unreachable!("failing store: expired_staged untouched")
        }

        fn container_len(&self, _: &[u8; 32], _: u64) -> Result<Option<u64>> {
            Err(Error::Invalid("boom".into()))
        }

        fn staged_identity(&self, _: &[u8]) -> Result<Option<[u8; 32]>> {
            unreachable!("failing store: staged_identity untouched")
        }

        fn set_staged_identity(&self, _: &[u8], _: &[u8; 32]) -> Result<()> {
            unreachable!("failing store: set_staged_identity untouched")
        }
    }

    #[test]
    fn container_len_maps_store_errors_to_absent() {
        let objs =
            SqliteObjects::open(ObjectsConfig::new([7; 32]), backend(), FailingStore).unwrap();
        assert_eq!(
            <SqliteObjects<FailingStore> as FetchObjects>::container_len(&objs, [9; 32], 0)
                .unwrap(),
            None
        );
    }

    #[test]
    fn fetch_stage_resume_publish_lifecycle() {
        let backend = backend();
        let donor = objects(MemoryStore::new(), backend.clone());
        let plain = plaintext(100_000);
        let meta = donor.upload("report", plain.as_slice()).unwrap();
        let len = fetch!(donor, container_len(meta.digest, meta.generation))
            .unwrap()
            .unwrap();
        let container = fetch!(
            donor,
            read_container(meta.digest, meta.generation, 0, len as usize)
        )
        .unwrap();
        assert_eq!(container.len() as u64, len);

        // Fresh store, same metadata backend: the fetch side has no payload.
        let node = objects(MemoryStore::new(), backend);
        assert!(
            fetch!(node, container_len(meta.digest, meta.generation))
                .unwrap()
                .is_none()
        );

        let transfer = transfer_id(&[9; 16], &meta.digest, meta.generation);
        fetch!(node, set_staged_identity(&transfer, &[5; 32])).unwrap();

        // Chunks arrive in unaligned pieces; resume tracks staged bytes.
        let (first, rest) = container.split_at(40_000);
        fetch!(node, append_staged(&transfer, first)).unwrap();
        assert_eq!(fetch!(node, resume_offset(&transfer)).unwrap(), 40_000);
        let mid = rest.len() / 2;
        let (second, third) = rest.split_at(mid);
        fetch!(node, append_staged(&transfer, second)).unwrap();
        fetch!(node, append_staged(&transfer, third)).unwrap();
        assert_eq!(fetch!(node, resume_offset(&transfer)).unwrap(), len);

        // Publish installs the object and retires the transfer.
        fetch!(node, publish_staged(&transfer, &meta)).unwrap();
        assert!(fetch!(node, available("report")).unwrap());
        let mut out = Vec::new();
        node.read("report", &mut out).unwrap();
        assert_eq!(out, plain);
        assert_eq!(fetch!(node, resume_offset(&transfer)).unwrap(), 0);
        assert_eq!(fetch!(node, staged_identity(&transfer)).unwrap(), None);

        // Re-publishing an already-present object succeeds and leaves the
        // staged bytes for collection (archived rename-skip).
        fetch!(node, set_staged_identity(&transfer, &[5; 32])).unwrap();
        fetch!(node, append_staged(&transfer, &container)).unwrap();
        fetch!(node, publish_staged(&transfer, &meta)).unwrap();
        assert!(fetch!(node, resume_offset(&transfer)).unwrap() > 0);
    }

    #[test]
    fn resume_offset_enforces_identity_rule() {
        let objs = objects(MemoryStore::new(), backend());
        let t = |n: u8| transfer_id(&[n; 16], &[n; 32], 0);

        // Staged bytes without any recorded identity restart from zero.
        fetch!(objs, append_staged(&t(1), &[7; 100])).unwrap();
        assert_eq!(fetch!(objs, resume_offset(&t(1))).unwrap(), 0);
        // The bytes were discarded, not just masked: a late identity still
        // resumes at zero.
        fetch!(objs, set_staged_identity(&t(1), &[8; 32])).unwrap();
        assert_eq!(fetch!(objs, resume_offset(&t(1))).unwrap(), 0);

        // A recorded identity preserves the offset.
        fetch!(objs, append_staged(&t(2), &[7; 100])).unwrap();
        fetch!(objs, set_staged_identity(&t(2), &[8; 32])).unwrap();
        assert_eq!(fetch!(objs, resume_offset(&t(2))).unwrap(), 100);
        assert_eq!(fetch!(objs, staged_identity(&t(2))).unwrap(), Some([8; 32]));

        // Nothing staged resumes at zero with no identity required.
        assert_eq!(fetch!(objs, resume_offset(&t(3))).unwrap(), 0);
        assert_eq!(fetch!(objs, staged_identity(&t(3))).unwrap(), None);
    }

    #[test]
    fn poisoned_publish_discards_staged_bytes() {
        let backend = backend();
        let donor = objects(MemoryStore::new(), backend.clone());
        let meta = donor.upload("report", plaintext(1000).as_slice()).unwrap();
        let node = objects(MemoryStore::new(), backend);

        let transfer = transfer_id(&[9; 16], &meta.digest, meta.generation);
        fetch!(node, set_staged_identity(&transfer, &[5; 32])).unwrap();
        fetch!(node, append_staged(&transfer, &[0xAA; 200])).unwrap();
        assert!(fetch!(node, publish_staged(&transfer, &meta)).is_err());
        assert_eq!(fetch!(node, resume_offset(&transfer)).unwrap(), 0);
        assert_eq!(fetch!(node, staged_identity(&transfer)).unwrap(), None);
        assert!(
            fetch!(node, container_len(meta.digest, meta.generation))
                .unwrap()
                .is_none()
        );
        assert!(!fetch!(node, available("report")).unwrap());
    }

    #[test]
    fn discard_clears_bytes_and_identity_idempotently() {
        let objs = objects(MemoryStore::new(), backend());
        let transfer = transfer_id(&[1; 16], &[2; 32], 0);
        fetch!(objs, append_staged(&transfer, &[7; 100])).unwrap();
        fetch!(objs, set_staged_identity(&transfer, &[8; 32])).unwrap();
        fetch!(objs, discard_staged(&transfer)).unwrap();
        assert_eq!(fetch!(objs, resume_offset(&transfer)).unwrap(), 0);
        assert_eq!(fetch!(objs, staged_identity(&transfer)).unwrap(), None);
        // Idempotent: discarding twice is fine.
        fetch!(objs, discard_staged(&transfer)).unwrap();
    }

    #[test]
    fn unchanged_shapes_delegate() {
        let be = backend();
        let objs = objects(MemoryStore::new(), be);
        let meta = objs.upload("report", plaintext(100).as_slice()).unwrap();

        let listed = fetch!(objs, list()).unwrap();
        assert!(listed.iter().any(|m| m.name == "report"));
        assert!(fetch!(objs, available("report")).unwrap());
        assert!(!fetch!(objs, available("missing")).unwrap());

        fetch!(objs, fetch_error(&Error::Unavailable("boom".into())));
        assert!(objs.last_fetch_error().unwrap().contains("boom"));

        let _held = fetch!(objs, reserve_staging(1 << 20)).unwrap();
        assert!(fetch!(objs, reserve_staging(u64::MAX)).is_err());

        fetch!(
            objs,
            record_peer_availability(meta.digest, meta.generation, [5; 16], true)
        )
        .unwrap();
        fetch!(
            objs,
            record_peer_availability(meta.digest, meta.generation, [5; 16], false)
        )
        .unwrap();

        let _guard = <SqliteObjects<MemoryStore> as FetchObjects>::protect_staging(
            &Arc::new(objects(MemoryStore::new(), backend())),
            b"transfer",
        );
    }

    #[test]
    fn empty_append_is_noop() {
        let objs = objects(MemoryStore::new(), backend());
        let transfer = transfer_id(&[1; 16], &[2; 32], 0);
        fetch!(objs, append_staged(&transfer, &[])).unwrap();
        assert_eq!(fetch!(objs, resume_offset(&transfer)).unwrap(), 0);
    }
}
