# PROVENANCE — murmur-files (WP-C slice 3)

Reuse-only crate: every transport behavior comes from the read-only archive
via the canonical in-tree `murmur-tx` modules. Nothing here reimplements wire
formats, cryptography, or merge logic. Slice 3 adds a verbatim algorithm port
of the file-object store onto SQLite chunk rows — constraint 9 forbids the
archived filesystem layout, so only payload addressing changes (paths become
chunk rows); framing, key selection, reservations, and error strings are
unchanged.

## Archive sources (read-only, never edited)

Archive root: `/media/marc/2TB/Archives/MURMUR-RUST` (crate `murmur-replication`).

| Archive module | md5 | Lines | Disposition |
| --- | --- | --- | --- |
| `replication/src/files.rs` | `b89bcaaed03a2ea25df91b6a773411dc` | 239 | PORT algorithms verbatim into `src/objects.rs` + `src/crypto.rs` (constraint 9; canonical `Objects` is fs-concrete) — framing, key selection, reservations, error strings unchanged; only payload addressing moves to `ChunkStore` rows |
| `replication/src/filefetch.rs` | `a5dd86b5896128b715dda44d6ae4e012` | 115 | REUSE canonical `murmur_tx::filefetch` (no copy; visibility flipped by lead); generic store split pending (lead) |
| `replication/src/ftp.rs` | `2a0e45c00e91163a6106a2e0a3f2166b` | 994 | REUSE via `murmur_tx::bridge::publish_ftp` (no copy) |
| `replication/src/bridge.rs` | `b4d41b2c5683bd2d95053a878676f364` | 83 | REUSE canonical `murmur_tx::bridge` (no copy) |
| `src/files.rs` (Database facade) | — | 79 | DROP as module (verdict-approved); guards preserved as host errors |

Verified with `cmp` that each destination `crates/murmur-tx/src/<module>.rs`
is byte-identical to the archive file above (md5s match both sides).

## Why zero copies (WP-B precedent)

The lead-approved WP-B pattern is "RE-EXPORT/REUSE rather than copy/compile
again" for canonical `murmur-tx` modules. Copying `filefetch.rs` into this
crate would additionally force duplicating the TLS/QUIC endpoint constructor
(`transport::endpoint_with_alpn`: rustls server/client configs, `NodeVerifier`,
transport tuning) — mesh-handshake-critical code that must stay canonical.
Instead this crate requests the same visibility-only widening the lead already
applied for WP-A A2 (`retention::{peer,touch,retire,status}`):

1. `murmur-tx/src/filefetch.rs:11`: `pub(crate) struct Service` → `pub`
2. `murmur-tx/src/transport.rs:44`: `pub(crate) fn endpoint_with_alpn` → `pub`

Both are lead-owned shared files; requested in ai-comms 2026-10-28T12:10Z.
Methods/fields keep their exact archive shape; only the two visibilities widen.

## Owned files (this crate)

| File | Contents |
| --- | --- |
| `Cargo.toml` | `murmur-tx` path dep + `tokio`/`zeroize`/`serde_json`/`aws-lc-rs` (all locked/vendored per the lead's A5a note; zero new crates); `tempfile` dev-dep. No standalone lock (workspace member). No `rusqlite` (lead's impl lives in `murmur-replication`). |
| `src/lib.rs` | Crate seam; re-exports `murmur_tx::{Backend,Error,NodeId,Record,bridge,filefetch,files,ftp}` |
| `src/adapt.rs` | 12-pragma spec (`murmur_files_dir` dropped), `Knobs`, `objects_config` builder, hex helpers, `actions::` over `SqliteObjects` |
| `src/hosts.rs` | `Hosts` over the injected `ChunkStore` (configure + file/bridge shims), pending-split fetch guards, unattached bridge files |
| `src/objects.rs` | `SqliteObjects` full surface port + `Upload`/`Download`/guards + 5 slice-3 tests |
| `src/crypto.rs` | SPFO seal/verify port (`SealWriter`/`OpenReader`/`next_frame`, real postcard trailer sizing) |
| `src/store.rs` | `ChunkStore` trait + `Arc` sharing impl + test-only `memory::MemoryStore` |
| `src/stub.rs` | `#[cfg(test)]` in-memory `Backend` (`murmur-tx` test_support is `pub(crate)`, unusable externally) |
| `PROVENANCE.md` | This file |
| `USAGE.md` | PRAGMA table, SQL-action specs, host lifecycle, secrets boundary |
| `INTEGRATION-PROPOSAL.md` | Lead wiring checklist (member/lock/flips/SQL/pragmas/tick) |

## Fidelity notes

- Wire formats unchanged and unowned here: SPFO `SPFO0001` objects, FF/1
  frames over ALPN `murmur-sqlite/files/1`, bridge bundle v1
  (`MURMUR-BRIDGE-FILE-1`, `murmur/sqlite-bridge/v1`).
- Backend kv namespaces reused verbatim: `08/files/meta/`,
  `08/files/availability/`, `09/bridge/*`.
- Error type reused: every function returns `murmur_tx::Result`; no new seam.
- `src/files.rs` facade behaviors map 1:1 onto SQL actions (see USAGE.md);
  its closed/not-configured guards survive as host "not configured" errors.
- Deliberate local helpers (not archive logic): `resolve_dir`, hex decode,
  `private_file` dest mode, JSON trust parsing for the configure action.
- Slice-3 verbatim ports (archive `files.rs` line refs): `Upload::new`
  reservation + header layout, `append` reserve math, `flush_chunk` AAD,
  `finish_inner` terminal + meta publish, `verify_input` checks in order,
  `Download::next` chunking, `ChunkWriter` skip/emit/error capture,
  `rotate_keys` re-read guards, `collect` grace + live-set + guard skips,
  `key_rotation_status` counters, availability 7-day cutoff, peer-record
  commit (not publish), tombstone publish.
- Slice-3 deliberate deviations (all noted at the site): `available()` is
  chunk-*completeness* (count == sealed size), stricter than the archived
  path check, so partial objects report unavailable instead of failing
  mid-read; seals buffer through staging rows (no temp files) with the same
  guard semantics; download pins are guard entries (no hard links);
  `verify_to` takes a plaintext sink instead of `Write` so rotation
  seal-feed errors stay lossless; `container_len` sizes the postcard trailer
  with the real encoder rather than reimplementing the varint.
- Suspected archived `Download::next` use-after-zeroize issue disproved by a
  throwaway multi-frame probe (byte-exact across `next` calls): `Vec`
  zeroize clears, so the refill guard behaves; the port mirrors the shape.

## Test evidence

- `cd crates && cargo test -p murmur-files --offline --frozen`
  (`CARGO_TARGET_DIR=/tmp/...`, workspace lock): **23 passed, 0 failed, 0
  murmur-files warnings** (8 adapt + 10 hosts + 5 objects). Adapt: pragma
  ranges/defaults (12 names), coupled staging rule, dir shapes,
  disabled-transport defaults. Hosts: guards, files round-trip over
  `SqliteObjects`+`MemoryStore` incl. offset reads/pattern list/collect,
  fetch guards + pending-split error, bridge export→stage→process incl.
  quarantine, export_file pending-split failure, configure-JSON roles,
  pre-network publish rejection. Objects: multi-slice round-trip +
  resume at 9 sizes incl. frame boundaries, digest/length auth failures,
  rotation across generations incl. open guards, collect of orphans +
  expired staging incl. pin protection, rotation-status counts.
  `rustfmt --check` clean on all sources. Workspace `cargo check` stays
  green (no downstream references the changed APIs yet).

Dependency consolidation uses AWS-LC for randomness and file cryptography.
Original file formats are covered by compatibility fixtures.
