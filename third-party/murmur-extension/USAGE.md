# murmur-sqlite-ext usage guide

A SQLite CRDT extension for multi-master replication: adopt tables, capture
writes with the stock Sessions API, merge with per-column LWW, and sync over
built-in QUIC or manual batch exchange. Same replication *features* as
cr-sqlite (adopt, backfill, versions, changesets, merge), completely
different machinery (sessions + HLC + Murmur log + Spool + QUIC).

No API compatibility with cr-sqlite is attempted — see
[cr-sqlite feature map](#cr-sqlite-feature-map) for how needs translate.

## Loading

```sql
.load ./libmurmur_sqlite_ext.so
```

Load first, on every connection that reads or writes synced tables —
writes through connections without the extension are invisible to capture.
Two builds exist (`dist.sh` produces both):

- **Dynamic** (default): uses the host's shared `libsqlite3`, which must
  export `sqlite3session_*` (`SQLITE_ENABLE_SESSION` +
  `SQLITE_ENABLE_PREUPDATE_HOOK`; stock Ubuntu 3.45.1 qualifies).
- **Static** (`-static` artifact): vendors SQLite 3.45.1 with session
  support compiled in (symbols hidden). For hosts whose SQLite is
  statically linked or lacks session symbols. The vendored version must
  match the host's SQLite version — see [Static variant](#static-variant).

`murmur_open` switches the database to WAL (persistent, one-time) and
refuses to run without it.

## Quickstart: one node

```sql
.load ./libmurmur_sqlite_ext.so
SELECT murmur_open('/var/lib/myapp/repl.spool');
CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
SELECT murmur_enable_sync('t');          -- returns stable table id 'ext-…'

BEGIN;
INSERT INTO t VALUES (1, 'hello');
SELECT murmur_collect();                 -- ships the txn's changes, returns outbox seq
COMMIT;
SELECT murmur_drain();                   -- folds committed batches into replication state
SELECT murmur_close();
```

`murmur_open(spool)` takes a spool directory (created if missing); all
replication metadata lives there, never in user tables. One database ↔ one
spool, strictly: opening one database with two spools (or one spool with
two databases or two keys) is refused.

## Quickstart: two nodes, manual sync

Both nodes need the same tables (run the same DDL on each) and the same
database id. Link **before the first write** — the id locks once history
is logged.

```sql
-- node A                              -- node B
SELECT murmur_open('a.spool');          SELECT murmur_open('b.spool');
CREATE TABLE t(...);                    CREATE TABLE t(...);
SELECT murmur_enable_sync('t');         SELECT murmur_enable_sync('t');
SELECT murmur_database_id();            -- 'f3e2…'
                                        SELECT murmur_set_database_id('f3e2…');

-- A writes, then ships a batch to B out of band (file, message queue, …)
BEGIN; INSERT INTO t VALUES (1,'a1');
SELECT murmur_collect(); COMMIT;
SELECT hex(murmur_export());            -- copy the hex…

                                        SELECT murmur_apply(x'<hex>');
                                        SELECT * FROM t;  -- 1|a1
```

`murmur_export` drains first, so it always carries committed work.
`murmur_apply` is idempotent (re-applying returns all zeros) and never
loops back into capture. Exchange in both directions to converge.

## Quickstart: two nodes, QUIC sync

Same setup as manual sync, plus transport. Each node needs a TLS identity
(DER chain/key/roots as hex JSON) whose leaf SAN is
`replicateddb://node/<node-id-hex>`, and every peer's `{node-id: pubkey}`:

```sql
SELECT murmur_node_id();      -- this node's id (for cert SANs + peer maps)
SELECT murmur_repl_pubkey();  -- this node's signing pubkey (for peer maps)

-- on each node (creds/peers JSON assembled by your tooling):
SELECT murmur_repl_start('127.0.0.1:0', '<creds>', '<peers>');
-- returns bound address, e.g. '127.0.0.1:43821'

-- connect them (either direction; both is fine):
SELECT murmur_repl_add_peer('<other address>');
SELECT murmur_repl_status();  -- {"address":…,"peers":1,"staged":0,"applied":N,"last_error":null}
```

Writes now flow automatically both ways, including history written before
transport started (the log starts at first drain, not at first start).
`SELECT murmur_repl_sync()` nudges an immediate round;
`SELECT murmur_repl_progress()` shows per-origin
`applied/observed/retained` watermarks (works offline too).
`SELECT murmur_repl_stop()` stops transport; replication state stays.

## SQL API reference

Setup and lifecycle:

| Function | Returns | Notes |
|---|---|---|
| `murmur_open(spool)` | `0` | Attach replication state. Errors inside a txn on first use (WAL switch); `:memory:` rejected. |
| `murmur_open_keyed(spool, key32)` | `0` | Same, with Spool encrypted under the 32-byte blob key. Key mismatch refused. |
| `murmur_close()` | `0` | Detach this connection. Replication keeps running headless if started; reopen finds it. |
| `murmur_database_id()` | hex id | This database's shared id (minted lazily). |
| `murmur_set_database_id(hex)` | `0` | Adopt a shared id. Refused once history is logged (same value is a no-op). |
| `murmur_node_id()` | hex id | This node's stable identity (HLC node + replication origin). |
| `murmur_repl_pubkey()` | hex pubkey | This node's signing pubkey for peer maps. |
| `murmur_clock()` | JSON | `{"millis","logical","node"}` — current HLC, incl. undrained work. |
| `murmur_debug_sessions()` | int | Live murmur sessions in this process (tests/diagnostics). |

Capture (per table opt-in):

| Function | Returns | Notes |
|---|---|---|
| `murmur_enable_sync(table)` | table id | Attach capture, backfill current rows, persist adoption. Outside a txn only. Idempotent (re-backfills). |
| `murmur_disable_sync(table)` | `0` | Stop capturing; kept metadata stays for future merges. |
| `murmur_collect()` | seq or NULL | Ship the txn's captured changes into the outbox (caller's txn). NULL when nothing changed. Inside a write txn. |
| `murmur_drain()` | count | Fold committed outbox rows into replication state + log. Any context; in-txn acks defer safely. |
| `murmur_sync_tables()` | JSON | `[{name, id, enabled}]` for adopted tables. |

Manual sync:

| Function | Returns | Notes |
|---|---|---|
| `murmur_export()` | blob | Whole enabled state as a portable batch (drains first). |
| `murmur_apply(blob)` | JSON | `{"rows","deleted","tombstones"}` counts merged+applied. Idempotent. Autocommit only. |

Transport:

| Function | Returns | Notes |
|---|---|---|
| `murmur_repl_start(bind, creds, peers[, intervals])` | bound addr | Start QUIC. `creds={"chain":[hex],"key":hex,"roots":[hex]}` (DER); `peers={"nodehex":"pubkeyhex"}`; `intervals={"anti_entropy_ms","incoming_ms"}` optional. Already-running refused. |
| `murmur_repl_stop()` | `0` | Stop transport (idempotent). Spool evicts if no connections remain. |
| `murmur_repl_add_peer(addr)` | `0` | Connect to `host:port`. |
| `murmur_repl_status()` | JSON | `{address, peers, staged, applied, last_error}`. |
| `murmur_repl_sync()` | `0` | Force an immediate sync round. |
| `murmur_repl_progress()` | JSON | Per-origin `[{origin, applied, observed, retained_from, retained_through}]`. Works offline. |

## Conflict semantics

- **Cells are last-writer-wins** by HLC version `(millis, logical, node)`.
  Concurrent writes to one cell converge deterministically everywhere.
- **Deletes fence by generation, not recency**: a same-generation
  tombstone always wins, even over strictly newer updates to that row.
  Only re-insert (a new generation, causally ordered after the delete)
  resurrects. Adopted rows share generation zero (the pre-adoption
  incarnation) with freshly ticked cells.
- **Inserts mint a fresh generation**; updates keep the generation and
  stamp touched cells.
- Same-transaction INSERT+DELETE of one row nets to nothing (Sessions
  semantics, before capture ever sees it).

## Schema changes

DDL is not replicated. To change a synced table's shape, on **each** node:

```sql
SELECT murmur_disable_sync('t');
ALTER TABLE t ADD COLUMN w TEXT;   -- same DDL on every node
SELECT murmur_enable_sync('t');    -- re-backfills current rows
```

Skipping disable risks `changeset/schema column mismatch` errors mid-DDL
(the live session validates shapes). Shape divergence between nodes fails
loudly at apply/merge time, never silently.

Renaming a table orphans its stable id (ids derive from names): disable
sync around renames and re-enable after on all nodes.

## Limitations and overhead

- **Primary keys required.** PK-less tables are rejected at enable;
  NULL-PK rows are skipped (backfill) or error (capture). Composite and
  `WITHOUT ROWID` keys work.
- **Opt-in per table.** Only enabled tables capture, merge, and sync.
  The outbox itself is never captured.
- **Outbox overhead.** Every collected transaction writes one
  `(version, changeset)` row, deleted on drain-ack. Steady-state cost is
  one extra row write per txn plus a Spool group commit per drained batch;
  undrained rows accumulate until `murmur_drain` (or collect/export/open,
  which all drain).
- **Unbounded log growth.** History is never pruned (retention is the
  catch-up mechanism), so the origin log and relayed logs grow with every
  batch. Monitor spool size; pruning/snapshots are future work.
- **Backfill cost.** Enabling scans the whole table under `BEGIN
  EXCLUSIVE` and commits one Spool group plus log entries sized with the
  data (large backfills split across log sequences automatically).
- **Constraints on synced tables.** Applies run with foreign keys
  unchecked and resolve PK conflicts by overwrite (`ON CONFLICT DO
  UPDATE`); non-PK `UNIQUE` violations fail the apply loudly. Keep the
  same discipline as cr-sqlite: no checked FKs, no non-PK uniques, no
  multi-column checks on synced tables.
- **Triggers.** App-connection cascades capture normally, but trigger
  side effects of *applied* writes onto other synced tables are not
  captured (no session on the apply path). Keep synced tables
  trigger-free or confine apply-path effects to unsynced tables.
- **One writer's extension.** Every connection that writes synced tables
  must load the extension and collect; the app may not use its own
  preupdate hook (Sessions forbids it); `murmur_apply` runs in autocommit.
- **WAL, file-backed, one spool.** WAL mode enforced; `:memory:` and
  read-only databases rejected; strict 1:1 database↔spool.
- **No fractional-index / counter / multi-value columns.** LWW only;
  the engine knows Min/Max/Counter policies but the extension assigns
  LWW everywhere today.

## cr-sqlite feature map

Same replication features, different names and machinery. No API
compatibility is provided or planned.

| Need (cr-sqlite) | murmur equivalent |
|---|---|
| `crsql_as_crr(table)` + backfill | `murmur_enable_sync(table)` + backfill |
| `crsql_changes` pull (`db_version` watermarks, site filter) | `murmur_export()` (whole state) for manual transport; the replication log + QUIC for incremental sync — no watermark pull API |
| `crsql_changes` apply (`INSERT`) | `murmur_apply(batch)` (idempotent) |
| `crsql_site_id()` | `murmur_node_id()` + `murmur_database_id()` (node vs database split) |
| `crsql_db_version()` watermark | `murmur_clock()` (observable HLC; not a pull cursor) |
| `crsql_tracked_peers` | `murmur_repl_progress()` (per-origin watermarks, works offline) |
| per-column versions + LWW | per-cell versions + LWW |
| row delete handling (OR-Set) | generation fencing (delete always wins in-generation) |
| `crsql_begin_alter` / `crsql_commit_alter` | disable → DDL → re-enable on each node (same DDL everywhere) |
| `crsql_finalize()` | `murmur_close()` (+ automatic backstop) + `murmur_repl_stop()` |
| load on every connection | same requirement |
| networking (external layers) | built-in QUIC (`murmur_repl_*`) — no external layer needed |
| fractional index columns | **not supported** |
| pre-built per-platform binaries | `dist.sh` (this repo; see below) |

## Static variant

`dist.sh` also builds `…-static.so`, which vendors SQLite 3.45.1
(session-enabled, symbols hidden) for hosts whose SQLite is statically
linked or lacks session entry points. The vendored SQLite **must match
the host's SQLite version**: session handles come from the host, and the
vendored session code assumes its own struct layouts. Mismatched hosts
need their own build (bump `SQLITE_AMALGAMATION_VERSION` in `dist.sh`
and re-vendor). Prefer the dynamic build whenever the host exports
`sqlite3session_*` — same-version pinning is one less thing to get wrong.

## Building

```sh
cargo build -p murmur-sqlite-ext        # dynamic .so → target/debug/
cargo test -p murmur-sqlite-ext         # full suite (41 tests)
./dist.sh                               # release dynamic + static, versioned names, checksums, static smoke
```

`dist.sh` writes `dist/murmur-sqlite-ext-<version>-<triple>[-static].so`
plus SHA256 sums, where `<triple>` is the host Rust target. Cross builds:
install the target (`rustup target add …`) plus a linker, then
`./dist.sh --target <triple>` (dynamic only cross-compiles routinely;
the static variant needs a C toolchain for the target too).

Vendored SQLite lives in `vendor/sqlite-amalgamation-<version>/`
(source + SHA256 + build flags recorded in `vendor/README.md`).

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `murmur requires WAL mode` | First `murmur_open` ran inside a transaction — open outside one; or the file can't switch modes (read-only). |
| `database id is locked once history is logged` | Link nodes (`murmur_set_database_id`) before the first write. |
| `database already open with a different spool/key` | 1:1 database↔spool enforced per process; close first. |
| `spool already open with a different database` | Same, reverse direction. |
| `changeset/schema column mismatch` | DDL under a live session — disable sync around DDL. |
| `update has no row metadata` | Updating rows never backfilled — re-enable the table. |
| `schema divergence on table '…'` | Nodes disagree on a table's shape — run the same DDL everywhere. |
| `replication not running` | `murmur_repl_start` first (on this spool). |
| `replication already running` | `murmur_repl_stop` first; one endpoint per spool. |
| `ambiguous node identity` at repl start | Cert SAN must be `replicateddb://node/<murmur_node_id>`, non-zero. |
| `murmur_apply` hangs then fails | Called inside a write txn — apply in autocommit. |
| `cannot start a transaction within a transaction` | `murmur_enable_sync` needs autocommit. |
