# INTEGRATION-PROPOSAL — murmur-files (WP-C slice 3)

Checklist for the lead's wiring pass. All items below are lead-owned shared
files; this crate (`crates/murmur-files/**`) is delivered and frozen
pending review corrections. Slice-2 items (visibility flips, member 6, lock
edge, sources unit 12) all landed — this is the slice-3 delta only.

## 1. DDL (lead, posted shape)

`murmur_file_chunks(digest BLOB, generation INTEGER, idx INTEGER, bytes BLOB,
PRIMARY KEY(digest,generation,idx))`, `murmur_file_transfers`, and
`murmur_file_staging` per the slice-3 BUILD SPEC. Two trait-driven
requirements: chunk rows must record first-slice time (or an equivalent) so
`live_objects` can report creation millis for the collection grace cutoff,
and staging rows must record `created_at` for `expired_staged`.

## 2. `rusqlite` ChunkStore (murmur-replication)

Implement `murmur_files::store::ChunkStore` (9 methods, `&self` with interior
mutability like `Backend`): `put_chunk` upserts one chunk row (slices are
immutable once published; overwriting an identical slice is a no-op);
`read_stream` selects the ordered byte range across rows (short/empty at the
end); `chunk_count` counts dense slices from zero; `delete_object` deletes by
`(digest, generation)`; `live_objects` lists distinct objects with creation
millis; `stage_chunk` appends one fragment (fragments concatenate in `idx`
order and are NOT assumed slice-aligned); `staged_count` sums staged bytes
(the resume offset); `read_staged` walks fragments in order;
`drop_staged` deletes by transfer; `expired_staged(cutoff)` lists transfer
ids at or before `cutoff` (`u64::MAX` lists all). No new crates:
`rusqlite` is already a workspace dependency.

## 3. Host construction (sql.rs)

`Hosts::open(backend, store)` now takes the chunk store as
`Arc<dyn ChunkStore>` (no generics; the `?Sized` blanket impl covers the
trait object). `configure_files` takes `adapt::objects_config(&knobs, key,
previous)` — no `db_parent`, no fetch fields. Fetch dials stay in `Knobs`;
since the Phase-2 split `start_fetch` binds the live endpoint over
`SqliteObjects` (needs engine TLS credentials), `fetch_tick` runs one
round per call (idle-false until started), and `stop_fetch` closes the
endpoint. Bridge file export still fails with the canonical
not-configured error until the bridge split. Signatures were stable so no
wiring changes were needed at the split.

## 4. PRAGMA wiring (config.rs)

Route the 12 names in `adapt::PRAGMAS` (`murmur_files_dir` DROPPED —
`validate()` rejects it as unknown; existing DBs with a persisted value need
a lead decision: ignore, erase, or reject at open). All restart-applied:
persist to `murmur_config`, read back, apply at open.

## 5. SQL actions + tick (sql.rs, maintenance thread)

The 21 USAGE.md actions register against the shared `Hosts` unchanged.
`murmur_file_fetch()` is one manual `fetch_tick` round (1 = ran,
0 = idle); the §6 marker body was swapped by the lead at the Phase-2
accept and Phase-3 wires membership sources into the tick;
`murmur_bridge_export_file()` still errors until the bridge split. Keep
calling `fetch_tick` from the shared maintenance thread; the throttle
returned with the split.

## 6. Fetch-service split (murmur-tx, lead)

`filefetch::Service` and the bridge file attachment go generic over the
store surface (`SqliteObjects` exposes the full archived method surface with
identical names, error strings, and reservation math, so the trait binds
both it and archived `Objects`). `protect_staging`,
`record_peer_availability`, `fetch_error`, `reserve_staging`, and
`publish_fetched(transfer, meta)` are `pub` for this. Note: the
`murmur_file_transfers` identity/digest metadata columns have no `ChunkStore`
writer yet — the split should place transfer-identity tracking (fetch resume
sidecar) either behind a second seam or inside the fetch layer.

## 7. WP-D handoff (unchanged)

WP-D confirms: (a) file-key derivation feeding `objects_config(key,
previous)`; (b) `configure_bridge_json` schema v1 (or its replacement).
Until confirmed, file/bridge hosts stay unconfigured and fail closed.

## 8. Verification

- `cd crates && cargo test -p murmur-files --offline --frozen`
  (`CARGO_TARGET_DIR=/tmp/...`): 23 tests (8 adapt + 10 hosts + 5 objects),
  zero murmur-files warnings, `rustfmt --check` clean.
- Full workspace gate + `verify.py` must stay green; no C++ tests added in
  this slice (SQL surface is lead-wired; C++ pins follow the wire-up).
