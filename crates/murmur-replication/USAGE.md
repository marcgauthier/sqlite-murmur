# SQL usage

## Setup

For the default static package, link `sqlite_murmur::sqlite_murmur`, register
your encryption key with `sqlite_murmur_register()`, and open application
connections with `sqlite_murmur_open()` or `sqlite_murmur_open_v2()`. CRR and
Murmur SQL interfaces are initialized automatically. Integrated connections to
the same database share one service, which stays alive until the final attached
application connection closes. Use `sqlite_murmur_close()` after finalizing
application statements; no manual extension loading/finalization is required.
A storage-only open skips replication initialization and metadata entirely.

For advanced low-level use with raw `sqlite3_open_v2()`, initialize cr-sqlite on
every application connection and initialize Murmur on one writable control
connection, outside a transaction. That control connection owns the service.
Do not mix manual and integrated initialization on the same handle.

For an optional loadable build (`SQLITE_MURMUR_BUILD_LOADABLE_EXTENSIONS=ON`),
load the companion cr-sqlite extension on application connections, then murmur
on the control connection:

```sql
SELECT load_extension('/absolute/path/murmur_crsqlite', 'sqlite3_crsqlite_init');
SELECT load_extension('/absolute/path/libmurmur_replication', 'sqlite3_murmurreplication_init');
PRAGMA murmur_crsqlite_extension = '/absolute/path/murmur_crsqlite.so';
```

Extension loading must be enabled by the host. The cr-sqlite companion path lets
the background worker initialize its own connection. It is unnecessary for the
static build. Libraries are supplied through the host SQLite extension API;
Murmur does not embed or use another SQLite instance. The Murmur shared library
remains loaded for process lifetime to keep VFS callbacks valid.

Applications create matching CRR schemas on every node:

```sql
CREATE TABLE records(id INTEGER PRIMARY KEY NOT NULL, body TEXT NOT NULL DEFAULT '');
SELECT crsql_as_crr('main', 'records');
```

Every CRR table participates. Ordinary tables, including the extension's
`murmur_*` tables, remain local. Reserve the `murmur_` prefix. Schema validation
compares ordered CRR table names and their SQLite-stored CREATE TABLE text;
use identical definitions, including constraints and defaults. Stop replication
on all nodes before applying coordinated schema migrations, then restart.

## Configuration

Set all replication settings with PRAGMAs; read a setting by omitting its value.
Configure in autocommit mode while replication is stopped. Settings are persisted
through a separate worker connection and take effect during PRAGMA preparation,
as with sqlite-murmur's other VFS PRAGMAs. Finalize PRAGMA statements before
starting the service. They are not transactional application SQL settings.
Unknown `murmur_*` PRAGMAs and invalid values return errors.

```sql
PRAGMA murmur_group_id = '11111111-1111-4111-8111-111111111111';
PRAGMA murmur_bind = '0.0.0.0:7443';
PRAGMA murmur_advertise = '192.168.1.20:7443';
PRAGMA murmur_bootstrap = '["192.168.1.10:7443"]';
PRAGMA murmur_certificate = '/etc/murmur/node.pem';
PRAGMA murmur_private_key = '/etc/murmur/node-key.pem';
PRAGMA murmur_ca = '/etc/murmur/ca.pem';
PRAGMA murmur_allowed_networks = '["192.168.1.0/24"]';
PRAGMA murmur_poll_ms;
SELECT murmur_start();
```

Certificates and private keys are PEM. Each node certificate needs a unique,
nonzero 128-bit node ID in exactly one URI SAN:
`replicateddb://node/00000000000000000000000000000001`. Certificates must be valid
for TLS client and server authentication and trusted by the supplied CA. Node IDs
are authenticated independently of IP names. Never share node certificates or
clone active cr-sqlite site identities between nodes.

Node identity is bound to the database on first start. Group identity cannot
change afterwards. To start a separate group, use a new database. Every node in
a group uses the same group ID and matching CRR definitions, but a different
certificate/node ID and cr-sqlite site ID. The site ID comes from cr-sqlite;
clients do not configure it independently.

| PRAGMA suffix (`murmur_…`) | Default | Meaning |
| --- | --- | --- |
| `group_id` | required | Nonzero 128-bit hexadecimal ID, optional UUID hyphens |
| `bind` | `0.0.0.0:7443` | UDP listening IP:port |
| `advertise` | bound address | Reachable IP:port or DNS `host:port` advertised through gossip; set explicitly for wildcard binds |
| `bootstrap` | `[]` | JSON array of seed IP:port or DNS `host:port` strings; names resolve once at start (restarts re-resolve) |
| `gossip_fanout` | `3` | Peers per gossip round, 1–32 |
| `certificate`, `private_key`, `ca` | required | PEM file paths; only paths persist in SQLite |
| `crsqlite_extension` | empty | Companion shared-library path for loadable worker initialization |
| `allowed_peers` | `[]` | JSON node-ID strings; empty allows any valid node certificate under the CA (the trusted-origins list) |
| `allowed_networks` | `[]` | JSON CIDR strings; empty allows any network |
| `poll_ms` | `100` | Committed-change polling interval |
| `anti_entropy_ms` | `10000` | Repair/retry interval after synchronization errors |
| `sessions` | `32` | Maximum connected peers / concurrent service requests, 1–256 |
| `batch_bytes` | `1048576` | Target CRSB batch size |
| `max_batch_bytes` | `67108864` | Hard batch limit, at most 64 MiB; a single larger change is rejected |
| `queue_bytes` | `67108864` | Transfer byte budget independently for outgoing and incoming traffic |
| `staging_bytes` | `268435456` | Aggregate incoming SQLite staging payload budget |
| `staging_timeout_ms` | `300000` | Expiration of abandoned staging |
| `writer_local_share` | `90` | Local writer share percent, 1–99; Remote share derives as 100 minus this |
| `writer_remote_share` | derived `10` | Read-only; assigning it is rejected |
| `writer_max_debt_ms` | `1000` | Bound on normalized Local/Remote service debt, 1–60000 ms |
| `writer_maintenance_reserve_every` | `100` | One grant in every N goes to waiting maintenance work, 1–1000000 |
| `retention_min_kept` | `1000` | Per-origin versions always kept below the retention floor, 1–1000000 |
| `retention_max_offline_ms` | `604800000` | Recently seen offline peers still pin floors, 60000–2592000000 ms |
| `retention_gc` | `1` | Run tombstone GC on the background pass (`0` moves floors only, no deletion) |
| `table_replication` | `{}` | Name-keyed opt-out map, e.g. `{"scratch": false}`; missing/true follows CRR state |
| `column_policies` | `{}` | Flat `table.column` map to `Lww`/`Counter`/`Set`/`Min`/`Max`; unknown names rejected |
| `snapshot_bytes` | `536870912` | Snapshot payload cap, 1 MiB–2 GiB |
| `snapshot_staging_bytes` | `536870912` | Per-direction snapshot staging cap, 1 MiB–2 GiB |
| `snapshot_lifetime_ms` | `604800000` | Transfer/pin lease, 1 h–30 d |
| `snapshot_staging_transfers` | `4096` | Staged transfer cap, 1–65536 |
| `snapshot_trusted_sources` | `[]` | JSON array of donor node ids; empty denies all snapshot donors |
| `accept_remote_schema` | `1` | Adopt signed schema manifests from peers (`0` keeps the local schema and fails pulls on mismatch) |
| `schema_master` | `` (empty) | Node id of the only adopted schema signer; empty adopts from any peer (legacy symmetric mode) |
| `schema_master_enabled` | `0` | This node signs schema manifests; requires `schema_master` to be its own id |

Polling/repair intervals accept 10–86400000 ms; staging expiry accepts
60000–86400000 ms so active transfers cannot expire prematurely. Byte settings
accept 64 KiB–1 GiB, subject to
`batch_bytes <= max_batch_bytes <= min(queue_bytes, staging_bytes, 64 MiB)`.
`bind` takes a numeric IPv4 or bracketed IPv6 socket address; `bootstrap`
and `advertise` additionally accept DNS `host:port` names (validated
offline, resolved once at start). At least one node must be reachable
through a seed address to join a group.
SWIM/gossip discovery is carried over authenticated QUIC; there is no mDNS or
NAT traversal. Gossip covers membership only — data moves through pulls, so
there are no Plumtree push trees. Seeds and administrator exclusions persist;
liveness is rechecked on restart. A service does not automatically restart
when the database reopens.

## Operations

```sql
SELECT murmur_start();                 -- 1 started, 0 already running
SELECT murmur_sync();                  -- request background synchronization
SELECT murmur_status();                -- JSON diagnostics and last error
SELECT murmur_health();                -- versioned health: replication, scheduler, storage, budgets
SELECT * FROM murmur_peers;            -- node_id,address,state,connected,incarnation
SELECT murmur_add_peer('192.168.1.30:7443');
SELECT murmur_remove_peer('00000000000000000000000000000003');
SELECT murmur_retire_peer('00000000000000000000000000000003');
SELECT murmur_progress();              -- JSON per-origin applied/observed heads
SELECT murmur_wait_applied('00000000000000000000000000000001', 42, 5000);
SELECT murmur_wait_lag(0, 5000);
SELECT murmur_snapshot_request('00000000000000000000000000000001');
SELECT murmur_gc();                    -- on-demand retention pass, returns tombstones pruned
SELECT murmur_backup('/path/to/dest'); -- online backup, 1 on success; dest must not exist
SELECT murmur_compact();               -- full-range compaction, 1 on success (blocking)
SELECT murmur_stop();                  -- 1 stopped, 0 already stopped
```

Adding a peer persists its seed address and schedules discovery. Removing a peer
persists an administrative exclusion and closes its connection; it does not
remove replicated rows. Exclusions currently have no public undo operation.
Retiring a peer additionally ends its retention obligation: retired peers no
longer pin history floors, and further contact from them is rejected from
retention bookkeeping. Like removal, retirement has no undo.
A background pass every 60 seconds advances per-origin retention floors from
peer acknowledgements (live or recently seen peers pin their acked versions;
connected-but-unknown peers pin everything) and snapshot pins, always
keeping the `retention_min_kept` tail. Floors surface as the
`retained_from`/`retained_through` window in `murmur_progress()` and the
`retention` block in `murmur_health()`. The same pass physically deletes
tombstone (`'-1'`) clock rows at or below the my-stream floor — the minimum
over live peers of what they acked of our stream, minus the kept tail — when
`retention_gc` is `1` (live cells keep their single clock row each; the clock
table is already a frontier, so only delete markers prune). Pruned history
advances a persisted GC watermark: pulls from older cursors fail loud
(`cursor pruned; snapshot required`) instead of converging through a silent
gap, and snapshots bypass the watermark by design (frontier state).
`SELECT murmur_gc()` runs the same pass on demand and returns tombstones
pruned. GC is ack-bounded only — clock rows carry no timestamps, so there is
no time knob. Abandoned staging rows past `staging_timeout_ms` are reaped on
the same pass.
Tables replicate when they are CRRs unless `table_replication` opts them
out (`false`); the map is read live on every batch, but changing any PRAGMA
still requires stopped replication. Disabled tables are skipped on export
and dropped before ingest on import, and destructive migration guards no
longer protect them (dropping a local-only table cannot strand peers).
Opt-out is not retroactive in either direction: rows written while a table
is disabled stay local permanently, and re-enabling publishes only
subsequently written rows — peers that need the gap take the snapshot
path. Flags follow table names (case-insensitive); renames drop flags.
`column_policies` records merge policies per column for the policy engine;
unknown tables/columns are inert until then, and JSON/list merge is
rejected (no archived implementation exists).
Snapshots carry a prefix-consistent full export (CRSB segments from
version 0) plus progress heads; the donor pins the covered history
until the transfer expires or the recipient catches up, and the
recipient installs pages through the idempotent apply path, then
continues with normal pulls. Manifests are authenticated by the mutual
TLS channel plus the `snapshot_trusted_sources` list (empty denies);
the archived message signatures have no embedded key to use and stay
empty. Donor and recipient schemas must match byte-for-byte; replicated
schema arrives separately. Expired pins, chunks, and receipts are swept
on the background pass.
`murmur_snapshot_request(donor)` queues a snapshot pull from a connected
peer (the donor must be listed in `snapshot_trusted_sources`); it
returns 1 once queued, and `murmur_status()`'s `snapshot` field reports
`running`, then `ok <transfer>` or `error: <cause>`. The donor serves
any authenticated group member. The manifest travels in one frame, so
transfers much past ~192MB are refused until fragmented manifests
land; concurrent snapshot pulls are serialized (one `running` at a
time). The recipient must already have matching table definitions —
the snapshot carries data, not DDL. Pulls are one-sided where snapshots
are exact: a donor serves a puller that carries local-extra tables
(drop-never legacy — the export only covers donor tables, all present
with identical DDL on the puller), vetted once per schema digest in the
background; shared-table drift, extra columns included, still rejects,
and pullers missing donor tables cannot be served. Two drop-diverged
directions stay unsupported until per-table filtering lands: a dropper
cannot pull a keeper's kept tables, and a keeper cannot snapshot from a
dropper — sync those pairs through a schema-matching intermediary.
Schema manifests replicate DDL from a master: the master signs its
canonical table list (local-only tables excluded) with a monotonic
sequence, and adopters apply additive changes only — new tables (created
and CRR-enabled) and added columns. Dropped tables, dropped or redefined
columns, and constraint changes are recorded but never adopted, as are
manifests from non-master signers when `schema_master` is set. Manifests
are authenticated by the mutual TLS channel (the signer must be the
authenticated peer); there are no separate signing keys. Data writes stay
masterless regardless of the schema master.
Dropping a CRR table leaves its cr-sqlite side tables behind, which abort
every changes feed read; the worker reaps orphaned `__crsql_clock` and
`__crsql_pks` tables automatically whenever the schema cookie moves (any
connection's DDL, plus once at open), pinning their max version into the
compaction floor key first so the max-derived version counter never
rewinds. Drop means drop: re-creating the table starts fresh.
`murmur_status()` also reports schema-vet outcomes (`vet_ok`/`vet_no`/
`vet_fail`) and serve outcomes (`serve_ok`/`serve_reject`/`serve_error`)
for mesh debugging.

## Backup

`SELECT murmur_backup(dest)` takes an online backup while the database
stays open: it flushes completed murmur transactions, checkpoints the
store into a new directory, and writes a `manifest.json` beside it
(`format_version`, `cipher`, `group_id`, `node_id`, `rocksdb_seq`,
`created_unix_ms` — never key material). The destination must not exist
(symlinks and dangling links refuse too); failures remove the partial
artifact. Backups serialize process-wide, work whether or not
replication is running, and require autocommit. `node_id` is zeros when
replication never initialized. Restore is the offline `murmur-admin`
tool, which refuses live databases and remaps identity so the restored
copy advertises a fresh writer while preserving history.

## Compaction and repair

`SELECT murmur_compact()` forces a full-range store compaction after
flushing completed transactions. It blocks the calling statement until
the merge finishes and requires autocommit. `sqlite_murmur_repair(path)`
is the offline C API for disaster recovery: it refuses live stores
(`SQLITE_BUSY` in-process, failure on a foreign LOCK) and missing paths,
and never initializes a missing directory. A `repair` CLI subcommand is
tracked follow-up work.
`murmur_progress()` reports durable per-origin heads (`origin`, `applied`,
`observed`, `retained_from`, `retained_through`); versions are the origin's
own database versions, and the local head tracks the live database version.
`murmur_wait_applied(origin, seq, timeout_ms)` blocks until the origin's
applied head reaches `seq` (read-your-write), and `murmur_wait_lag(bound,
timeout_ms)` blocks until the worst observed deficit is at most `bound`
(bounded staleness). Both return 1 when met, 0 on timeout; a zero timeout
polls once. Sequence zero returns immediately. Waits require autocommit and
work while stopped (heads stay readable); they return 0 when replication
never started. CRR batches mix forwarded origins, so heads currently
attribute to the batch sender; exact per-origin attribution follows the
table-flag release.
Writer scheduling covers Remote peer-apply jobs and Maintenance index/tick
jobs; application SQLite writes are not admitted
(`local_admission_enforced` is false in health) because no SQLite-core hook
exists yet. Scheduler PRAGMA changes persist immediately but take effect on
the next open: `murmur_health()` reports the live `effective_config` beside
the persisted `pending_config` so the split is visible. `murmur_health()`
also reports per-class admission stats, the local logical cursor,
staged bytes, and serving/receiving budget use; metrics sqlite-murmur
cannot measure (`durable_cursor`, `counters`, `worker_payload_bytes`)
stay null.
Management functions require autocommit. Stop waits for background shutdown;
closing the final integrated connection also stops the service. With manual
extension loading, the owning control connection controls its lifetime. Functions and the
peer virtual table are direct-only, unavailable from triggers or schema views.

Integrated applications finalize their statements and use
`sqlite_murmur_close()`. For manual initialization, stop Murmur before finalizing
cr-sqlite: finalize application statements, call `SELECT crsql_finalize()`, then
close connections.
A background worker owns a separate connection and finalizes it during cleanup.
One process owns each sqlite-murmur database, as required by the VFS.

## Delivery and durability

Murmur exchanges all nine CRR change fields using CRSB batches. Per-source
watermarks use that source database's versions; they are not global sequence
numbers. Batches include forwarded changes, so a reachable intermediary can
repair a peer without a direct connection to the original writer.

Incoming changes and receive progress commit atomically. The sqlite-murmur
persistence barrier completes before ACK. Outgoing state is also persisted
before sending a version boundary. Flush errors withhold ACK/progress publication
and appear in status. Power-loss durability requires `PRAGMA rocksdb_sync=1`;
with sync disabled, the barrier protects against process crashes but not all
machine/OS failures. Acknowledgement is per peer, not a cluster quorum guarantee.

Frames are bounded and large batches are chunked into 64 KiB pieces. Partial
incoming chunks live in local SQLite staging, are never applied independently,
and expire after interruption. Reconnection resumes from the last applied
batch; partial batches are retransmitted. Replays are safe through cr-sqlite.
Application transactions can span several replicated batches: remote atomicity
is a batch guarantee, not preservation of source transaction boundaries.

The extension does not replicate DDL, custom merge policies, or non-CRR tables,
and does not garbage-collect cr-sqlite tombstones/clocks. Schema mismatches stop
data exchange with that peer and remain visible in diagnostics. The transport
ALPN is `murmur-crsqlite/1`, incompatible with archived Spool-based Murmur.
