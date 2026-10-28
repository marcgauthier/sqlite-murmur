# Phase 2 — Capture + local durability (done)

## Result

The extension is now a working one-node CRDT capturer: adopt tables, collect
batches into a crash-safe outbox, drain them into Spool as Murmur metadata
(column versions, tombstones, clocks). Verified end to end, including
SIGKILL recovery.

## Design

**Single global core.** One `CORE` mutex guards all extension state
(connections, shared spool handles, DB→spool mapping). Every SQL function
holds it for its whole body: no lock ordering, fully serialized extension
ops. Correct-first; fine-grained locking is future work.

**Natural-key schema mapping** (`schema_map.rs`). Each user table maps to a
stable ID (`ext-` + name hash); columns to `{table}/{column}` IDs; the adopted
schema + opt-in flags persist in Spool (`00/schema/ext-v1`,
`00/schema/ext-flags-v1` — flags split out because the `Schema` wire codec
excludes them, as in the engine). PK-less tables, virtual tables, TEMP/
attached tables, and the outbox itself are rejected with precise errors.
Renames orphan IDs (documented: disable sync around DDL).

**Backfill on enable.** Enabling scans all current rows under `BEGIN
EXCLUSIVE` (after attaching capture, so concurrent commits land in the
session with newer versions and win) and writes one atomic Spool group at a
fresh tick. NULL-PK rows are skipped, matching session semantics. Re-enable
re-backfills; updates to never-backfilled rows fail loudly (`update has no
row metadata`) instead of silently diverging.

**Outbox protocol** (`outbox.rs`). `murmur_collect()` drains committed rows
first (tick monotonicity across connections), ticks the HLC, inserts
`(millis, logical, node, changeset)` in the caller's transaction, then resets
the session. Drain runs on a dedicated connection (sees only committed rows),
applies one atomic Spool group per outbox row (metadata + `max(clock,
batch)`), and acknowledges by deleting the row. Crash between commit and ack
replays identical bytes. Drain triggers: `murmur_open` (recovery),
`murmur_collect` (pre-tick catch-up), `murmur_drain` (explicit). No background
threads yet.

**Delta rules** (`delta.rs`) mirror the engine's `metadata::delta` op for op:
coalesce to net effect per row, tombstone deletes of known rows (generation
preserved), keep generation + stamp touched cells on update, fresh generation
on insert, PK-shape validation via `sqlite3changeset_pk`, unchanged UPDATE
columns detected via NULL `sqlite3changeset_new`.

**Lifecycle.** `murmur_close` detaches + evicts the spool at refcount zero
(closing releases the Spool file lock, so tests and handoffs can reopen).
A hidden `_murmur_conn_pin` function carries an `xDestroy` backstop that
cleans up when hosts close connections without `murmur_close`. One database
with two spool dirs, and one spool with two keys, are both refused.
`:memory:` databases are rejected (no stable path for the drain connection).

## Supporting crates

- `host-shim`: foreign-host SQLite client (hand FFI) shared by tests and
  `crash-host`; carries the `libsqlite3` link setup.
- `crash-host`: commits one collected row, then exits dirty or sleeps for
  SIGKILL — never drains.
- `murmur-crdt` gained `logical::LogicalStore` (moved from the engine; the
  extension needs identical blob-splitting). Engine suites re-verified green;
  Go inventory unchanged (847 symbols, audit passes).

## Test notes

- `cargo test -p murmur-sqlite-ext` (full package): `--test <name>` alone does
  NOT rebuild the cdylib (dlopen tests don't link it). `tests/common.rs`
  refuses to run against a stale `.so` instead of silently testing old code.
- `tests/sync.rs` serializes on a process lock (the session-count test needs
  a quiet process); each test uses unique tempdirs otherwise.
- Direct Spool reads in tests are valid only after `murmur_close` + drop
  (Spool file-locking rejects double opens); tests never write through the
  second handle.

## Evidence

- 19/19: 12 sync (roundtrip, WITHOUT ROWID, rejections, multi-table shared
  version, idempotent enable + reopen re-attach, disable/re-enable, autocommit
  collect, close + backstop, idempotent re-drain, :memory: + key + txn guards)
  + 2 recovery (dirty exit, SIGKILL) + 5 wiring.
- Engine default + replication suites green after the `LogicalStore` move.
