# Murmur SQLite CRDT extension

A SQLite CRDT extension (cr-sqlite-style features) built on the stock SQLite
Sessions API for change capture and the Murmur engine for HLC clocks,
per-column LWW merge, tombstones, Spool persistence, and QUIC replication.

Status: **Phases 0–5 complete** — linkage spike (`docs/PHASE0.md`),
`murmur-crdt` extraction (`docs/PHASE1.md`), capture + outbox durability
(`docs/PHASE2.md`), two-node merge (`docs/PHASE3.md`), QUIC replication
(`docs/PHASE4.md`), packaging (`docs/PHASE5.md`).

New here? Start with [`USAGE.md`](USAGE.md) (quickstarts, SQL reference,
replication setup, limitations); `docs/` holds the per-phase decision
records.

## Layout

- `crates/murmur-sqlite-ext/` — the loadable extension (`cdylib`) + tests.
- `crates/host-shim/` — foreign-host SQLite client shared by tests.
- `crates/crash-host/` — crash simulator for recovery tests.
- `USAGE.md` — user guide (setup, SQL API, replication, limitations).
- `dist.sh` — release packaging (versioned dynamic + static artifacts).
- `vendor/` — vendored SQLite amalgamation for the static variant.
- `docs/` — per-phase decision records.

## Build & test

No sudo, no `-dev` packages, no bindgen required: the extension uses
`rusqlite`'s prebuilt extension bindings and links the system `libsqlite3` by
SONAME (the build script symlinks `libsqlite3.so.0` into `OUT_DIR`).

```sh
cargo build --workspace
cargo test -p murmur-sqlite-ext   # full package: --test <name> alone skips the cdylib rebuild
./dist.sh                         # release artifacts → dist/ (dynamic + static + checksums)
```

Quick manual check (file-backed DB required):

```sh
sqlite3 /tmp/demo.db \
  ".load ./target/debug/libmurmur_sqlite_ext.so" \
  "SELECT murmur_open('/tmp/demo.spool');" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);" \
  "SELECT murmur_enable_sync('t');" \
  "BEGIN; INSERT INTO t VALUES (1, 'hi'); SELECT murmur_collect(); COMMIT;" \
  "SELECT murmur_drain(); SELECT murmur_close();"
```

Two-node exchange (grab `<dbid>` from A's `murmur_database_id()` and
`<hex>` from `hex(murmur_export())`):

```sh
sqlite3 /tmp/b.db \
  ".load ./target/debug/libmurmur_sqlite_ext.so" \
  "SELECT murmur_open('/tmp/b.spool');" \
  "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);" \
  "SELECT murmur_enable_sync('t');" \
  "SELECT murmur_set_database_id('<dbid>');" \
  "SELECT murmur_apply(x'<hex>');" \
  "SELECT * FROM t; SELECT murmur_close();"
```

## Host requirements

- SQLite built with `SQLITE_ENABLE_SESSION` + `SQLITE_ENABLE_PREUPDATE_HOOK`
  (stock Ubuntu 3.45.1 qualifies) exporting `sqlite3session_*` symbols.
- WAL-capable database files: `murmur_open` sets `journal_mode=WAL`
  (persistent, one-time) and refuses to run without it — rollback mode
  blocks second-connection reads during large write transactions, which
  wedges drains and applies (see `docs/PHASE4.md`).
- Hosts must use the shared `libsqlite3` (same SONAME the extension records
  as `DT_NEEDED`). Hosts with a statically linked SQLite need the future
  static-link variant, not the `.so`.
