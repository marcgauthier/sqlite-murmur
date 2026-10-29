# USAGE — murmur-files (WP-C slice 3)

File/bundle transport seams for sqlite-murmur. Wire formats stay canonical
in `murmur-tx`; this crate maps validated PRAGMAs onto configs, hosts the
driver lifecycles, implements the SQL-action handler logic the lead's
`sql.rs` wiring calls, and owns the SQLite-backed file-object store
(`objects::SqliteObjects` over an injected `store::ChunkStore`) that replaces
the archived filesystem layout under constraint 9. The fetch service and
bridge file flows await the lead's generic store split and fail closed until
then (see Host lifecycle).

## PRAGMA surface (constraint 8; all restart-applied)

Every knob the fetch driver needs is a `PRAGMA murmur_*` with default,
read-back, and `validate()` range (lead ruling 4). Effective values are read
at open; edits persist to `murmur_config` and apply on reopen (pending shown
until then, per the WP-F effective/pending precedent).

| Pragma | Default | Range / shape | Notes |
| --- | --- | --- | --- |
| `murmur_bridge_dir` | `""` | any text | `""` → `<db-parent>/murmur-bridge` |
| `murmur_files_generation` | `0` | any u64 | rotation rules enforced at open (previous key required, one step) |
| `murmur_max_file_bytes` | `4294967296` (4 GiB) | 1 MiB .. 32 GiB | per-object plaintext cap |
| `murmur_max_staging_bytes` | `1073741824` (1 GiB) | 64 KiB .. 32 GiB | shared upload/fetch staging reservation |
| `murmur_fetch_concurrency` | `2` | 1 .. 64 | concurrent fetch streams |
| `murmur_fetch_interval_ms` | `30000` | 1000 .. 86400000 | maintenance-thread tick gate |
| `murmur_fetch_bind` | `""` (DISABLED) | `""` or `host:port` | empty = no socket (embedded opt-in); docs recommend `127.0.0.1:7444` |
| `murmur_bridge_role` | `""` (disabled) | `""`, `low`, `high` | low = exporter, high = receiver |
| `murmur_bridge_stream` | `""` | `[A-Za-z0-9-_]{0,64}` | required (non-empty) once a role is set |
| `murmur_bridge_max_bundle_bytes` | `16777216` (16 MiB) | 4 KiB .. 16 MiB | sealed artifact cap |
| `murmur_bridge_max_payload_bytes` | `67108864` (64 MiB) | 4 KiB .. 64 MiB | plaintext payload cap |
| `murmur_bridge_staging_bytes` | `1073741824` (1 GiB) | ≥ 4096 AND ≥ max_bundle | coupled rule checked in `validate_coupled` + at build |

Rust entry points: `adapt::PRAGMAS`, `adapt::default_value`,
`adapt::validate`, `adapt::validate_coupled`, `adapt::Knobs::from_map`.

`murmur_files_dir` was dropped in slice 3 (constraint 9): no directory backs
the file store, and `validate()` rejects the name as unknown.

## SQL actions (names + signatures; lead wires)

File actions (backed by `Hosts` shims over `adapt::actions`):

| Action | Returns |
| --- | --- |
| `murmur_file_upload(name TEXT, path TEXT)` | metadata (name/digest/bytes/generation/millis/node) |
| `murmur_file_download(name TEXT, dest TEXT)` | bytes written (INTEGER) |
| `murmur_file_read(name TEXT, offset INTEGER, limit INTEGER ≤ 1 MiB)` | BLOB chunk |
| `murmur_file_delete(name TEXT)` | — |
| `murmur_file_list(pattern TEXT)` | rows; empty pattern = all (covers `search_files`) |
| `murmur_file_metadata(name TEXT)` | one JSON row (or NULL) |
| `murmur_file_availability(name TEXT)` | `local` \| `unavailable` \| `remote:<csv hex peers>` |
| `murmur_file_fetch()` | 1 = round ran, 0 = idle (service unstarted); sources from membership |
| `murmur_file_last_error()` | TEXT or NULL |
| `murmur_file_collect(grace_ms INTEGER)` | objects/staging rows removed (arg, not pragma) |
| `murmur_file_rotate(limit INTEGER)` | objects re-encrypted (arg, not pragma) |
| `murmur_file_rotation_status()` | JSON: current/old counts, staged, previous-unused flag |

Bridge actions:

| Action | Returns |
| --- | --- |
| `murmur_bridge_export(payload_hex TEXT)` | bundle path TEXT |
| `murmur_bridge_process()` | applied count INTEGER |
| `murmur_bridge_capture()` | exported count INTEGER |
| `murmur_bridge_stage(path TEXT)` | header JSON (stream/sequence/digest/…) |
| `murmur_bridge_export_file(name TEXT)` | fails closed until the store split (no files attach); durable-progress design unchanged for after |
| `murmur_bridge_export_snapshot()` | bundle path TEXT |
| `murmur_bridge_release(table TEXT, key_hex TEXT, col TEXT)` | 1 applied / 0 held (`col` NULL-able) |
| `murmur_bridge_configure(json TEXT)` | — (in-memory WP-D secrets; schema v1 below) |

Arg-carrying actions resubmitted per ruling 4 (only these take per-call
targets/credentials; everything else is a pragma):

```sql
murmur_bridge_publish_ftp(path TEXT, url TEXT, roots_pem BLOB, timeout_ms INTEGER)
-- ftps:// requires non-empty roots_pem (pinned private CA); ftp:// ignores
-- roots/timeout (plaintext, 30 s default). timeout_ms = 0 selects 30 s.

murmur_bridge_publish_http(path TEXT, url TEXT)
-- https:// or http:// only; enforced before any I/O.
```

## Host lifecycle (lead wiring)

```rust
let hosts = Hosts::open(crr_backend, rusqlite_chunk_store); // shared kv + chunk rows
let knobs = Knobs::from_map(&effective_pragmas)?;           // validated
hosts.configure_files(objects_config(&knobs, wpd_key, wpd_prev)?)?;
hosts.configure_bridge_json(&knobs, db_parent, wpd_json)?;  // or discrete calls
hosts.start_fetch(&knobs, &engine_transport_base)?;         // live since the store split (Phase 2)
// maintenance thread:
hosts.fetch_tick(membership_sources)?;                      // bool: round ran
```

- Unconfigured stores fail closed (`file/bridge storage is not configured`).
- Since the store split (Phase 2): `start_fetch` binds the fetch endpoint
  over `SqliteObjects` (guard order: disabled bind, unconfigured storage,
  missing engine TLS credentials — a bare engine errors, never panics);
  `fetch_tick` reports idle (`Ok(false)`) until started, then runs one
  round per call; `stop_fetch` closes the endpoint (no-op when stopped).
  `bridge_export_file` still fails with the canonical "bridge file storage
  is not configured" until the bridge split.
- Payload I/O runs on one lazily created `murmur-files` runtime thread, only
  while bridge publish is used. Default-disabled installs create zero threads
  and no socket.
- Bridge domain derives from `backend.identity().0` (no pragma).

## Secrets boundary (WP-D owns keys + trust)

- File object key + previous key: `[u8;32]` from the WP-D keyprovider into
  `objects_config`; wrapped in `Zeroizing` by the store config.
- Bridge signer / receiver secret / recipient / trusted domains, signers, and
  origins: via `configure_bridge_json` (schema v1, in-memory only, never
  persisted) or the discrete `configure_bridge_exporter/receiver` calls.
- FTP roots/timeout: per-call action args only.
- No `murmur_file_set_previous_key` action: previous keys are WP-D material
  and arrive with the config, not through SQL text.

## Mesh compatibility (unchanged, unowned)

ALPN `murmur-sqlite/files/1`, FF/1 frames (128 KiB cap, resume + identity
sidecar), SPFO `SPFO0001` containers, bridge bundle v1, kv namespaces
`08/files/*` + `09/bridge/*` — all canonical in `murmur-tx`, reused verbatim.

## SQLite storage (slice 3, constraint 9)

Metadata is byte-identical Backend kv (`08/files/meta/*`,
`08/files/availability/*`) and needs no migration. Payload containers rest in
chunk rows addressed by `(digest, generation, idx)`; each row holds ≤ 65536
container bytes and concatenation in `idx` order equals the archived `.spfo`
bytes. In-flight seals and peer transfers rest in staging rows under a
transfer id (the `.upload-*`/`.part` equivalents); the transfer metadata row
(`murmur_file_transfers`) is a lead-wiring concern for the fetch split.
`ChunkStore` maps 1:1 onto the lead's tables (`put_chunk` → upsert chunk row,
`read_stream` → ordered range select, `chunk_count` → COUNT(*),
`delete_object` → DELETE by object, `live_objects` → DISTINCT objects with
first-slice time, `stage_chunk`/`staged_count`/`read_staged`/`drop_staged` →
staging upsert/sum/range-select/delete, `expired_staged` → id list by
`created_at`). `MemoryStore` is the test-only BTreeMap implementation.
Reads stream slice-by-slice through the archived verifier; no path ever holds
a whole object or container in memory.
