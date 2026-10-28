# Phase 5 — Packaging (done)

## Result

Shippable artifacts plus the user guide: `dist.sh` builds versioned
release dynamic + static extension binaries with checksums
(`dist/murmur-sqlite-ext-0.1.0-<triple>[-static].so`), the static variant
vendors SQLite 3.45.1 with hidden symbols and passes its smoke test, and
`USAGE.md` documents setup, the full SQL API, replication, limitations,
and a cr-sqlite feature map. Per user decisions: path dependencies stay
(no git migration), no API shim (replication feature parity instead,
verified from the cr-sqlite docs), static variant built now.

## Design

**`dist.sh`.** Release dynamic build → copy with versioned name →
release static build → copy → linkage assertions (dynamic keeps its
`libsqlite3` `DT_NEEDED`, static has none) → static smoke test against
the shipped static artifact → `SHA256SUMS`. `--target` cross-builds
(dynamic routinely; static needs a C toolchain for the target) and skips
execution steps that can't run foreign binaries. Version and triple come
from `Cargo.toml` and `rustc -vV`; nothing is hardcoded.

**Static variant** (`--features static-sqlite`). `build.rs` compiles
`vendor/sqlite-amalgamation-3450100/sqlite3.c` with
`SQLITE_ENABLE_SESSION` + `SQLITE_ENABLE_PREUPDATE_HOOK`,
`-fvisibility=hidden`, `-O2`, and links it privately — no host SQLite
dependency at all. The existing plain `extern "C"` session declarations
resolve against the archive at link time with no source changes. Hidden
visibility is load-bearing: without it our amalgamation's `sqlite3_*`
symbols would interpose with the host's. The amalgamation version is one
knob (`SQLITE_AMALGAMATION_VERSION` in `dist.sh` →
`MURMUR_SQLITE_AMALGAMATION` for `build.rs`, default `3450100`);
`vendor/README.md` records provenance (URL, SHA256s, flags, upgrade path).

**Same-version constraint.** The static build's session code runs on
host-owned handles, so vendored and host SQLite must match. 3.45.1 was
chosen to match this repo's test host (stock Ubuntu 3.45.1), keeping the
pairing verifiable; the smoke test asserts host `sqlite_version()` and
fails loudly otherwise instead of risking layout mismatch. Prefer the
dynamic build wherever the host exports `sqlite3session_*`.

**`USAGE.md`.** Quickstarts (single node, manual two-node, QUIC
two-node), complete SQL reference (23 functions), conflict semantics,
per-node DDL procedure, limitations/overhead (PKs, opt-in, outbox,
unbounded log, backfill, constraints, triggers, WAL, 1:1, no fractional
index), troubleshooting table, and the cr-sqlite feature map. Constraint
claims match the code: applies use PK `ON CONFLICT DO UPDATE`, non-PK
uniques fail loudly, FKs unchecked.

**Parity work.** Mapping cr-sqlite's documented replication features
(`as_crr`, `changes` pull/apply, site id, db version, tracked peers,
column LWW, row deletes, alter protocol, finalize, per-connection load)
against ours found one small gap worth closing: per-origin progress
(`tracked_peers`) → new `murmur_repl_progress()` (hex origins, works
offline, tested local + relayed). Deliberate non-gaps: watermark pull
(our incremental sync is the QUIC log, not version cursors), alter
protocol (disable → DDL → re-enable procedure), fractional index columns
(unsupported; engine lacks it too).

**Open rollback guard.** `open_impl` runs its post-spool tail
(replay/drain/ensure/schema/session) in a unit rolled back on failure
(spool ref + fresh db binding), and the WAL check moved ahead of all
state changes — failed opens leave zero trace (proven by test: a refused
open followed by close still fully evicts for direct Spool access).

## Test notes

- `tests/static_smoke.rs` is `#[ignore]` by design: it tests a separate
  artifact that plain `cargo build` doesn't produce. `dist.sh` runs it
  explicitly (`-- --ignored`) against the just-built static binary with
  `MURMUR_EXT_STATIC_CDYLIB`, including the host-version match assert.
  An unset env var fails loud with build instructions, never skips.
- `tests/repl.rs` gained the progress test (exact local watermarks,
  relayed origin visibility); `tests/sync.rs` gained WAL enforcement.

## Evidence

- 42 passing + 1 ignored-by-design, zero warnings (lib + tests): 12
  merge + 16 sync + 2 recovery + 7 repl + 5 wiring + 1 static smoke.
- `dist.sh` green: 13MB dynamic (`libsqlite3.so.0` NEEDED),
  14MB static (zero sqlite refs, session symbols local `t`),
  `SHA256SUMS` written, release static smoke passed.
