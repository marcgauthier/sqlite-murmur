# SQL usage

## Setup

Open a named database using the registered `sqlite-express` VFS. Initialize
cr-sqlite on every application connection. Initialize murmur-replication on
exactly one writable control connection, outside an explicit transaction.
Keep this connection open for the lifetime of the service.

For a loadable build, load the companion cr-sqlite extension on application
connections, then murmur on the control connection:

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
as with sqlite-express's other VFS PRAGMAs. Finalize PRAGMA statements before
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
| `advertise` | bound address | Reachable IP:port advertised through gossip; set explicitly for wildcard binds |
| `bootstrap` | `[]` | JSON array of seed IP:port strings |
| `certificate`, `private_key`, `ca` | required | PEM file paths; only paths persist in SQLite |
| `crsqlite_extension` | empty | Companion shared-library path for loadable worker initialization |
| `allowed_peers` | `[]` | JSON node-ID strings; empty allows any valid node certificate under the CA |
| `allowed_networks` | `[]` | JSON CIDR strings; empty allows any network |
| `poll_ms` | `100` | Committed-change polling interval |
| `anti_entropy_ms` | `10000` | Repair/retry interval after synchronization errors |
| `sessions` | `32` | Maximum connected peers / concurrent service requests, 1–256 |
| `batch_bytes` | `1048576` | Target CRSB batch size |
| `max_batch_bytes` | `67108864` | Hard batch limit, at most 64 MiB; a single larger change is rejected |
| `queue_bytes` | `67108864` | Transfer byte budget independently for outgoing and incoming traffic |
| `staging_bytes` | `268435456` | Aggregate incoming SQLite staging payload budget |
| `staging_timeout_ms` | `300000` | Expiration of abandoned staging |

Polling/repair intervals accept 10–86400000 ms; staging expiry accepts
60000–86400000 ms so active transfers cannot expire prematurely. Byte settings
accept 64 KiB–1 GiB, subject to
`batch_bytes <= max_batch_bytes <= min(queue_bytes, staging_bytes, 64 MiB)`.
Addresses are numeric IPv4 or bracketed IPv6 socket addresses, not DNS names.
At least one node must be reachable through a seed address to join a group.
SWIM/gossip discovery is carried over authenticated QUIC; there is no mDNS or
NAT traversal. Seeds and administrator exclusions persist; liveness is rechecked
on restart. A service does not automatically restart when the database reopens.

## Operations

```sql
SELECT murmur_start();                 -- 1 started, 0 already running
SELECT murmur_sync();                  -- request background synchronization
SELECT murmur_status();                -- JSON diagnostics and last error
SELECT * FROM murmur_peers;            -- node_id,address,state,connected,incarnation
SELECT murmur_add_peer('192.168.1.30:7443');
SELECT murmur_remove_peer('00000000000000000000000000000003');
SELECT murmur_stop();                  -- 1 stopped, 0 already stopped
```

Adding a peer persists its seed address and schedules discovery. Removing a peer
persists an administrative exclusion and closes its connection; it does not
remove replicated rows. Exclusions currently have no public undo operation.
Management functions require autocommit. Stop waits for background shutdown;
closing the owning control connection also stops the service. Functions and the
peer virtual table are direct-only, unavailable from triggers or schema views.

Stop Murmur before finalizing cr-sqlite on application connections. Finalize
application statements, call `SELECT crsql_finalize()`, then close connections.
A background worker owns a separate connection and finalizes it during cleanup.
One process owns each sqlite-express database, as required by the VFS.

## Delivery and durability

Murmur exchanges all nine CRR change fields using CRSB batches. Per-source
watermarks use that source database's versions; they are not global sequence
numbers. Batches include forwarded changes, so a reachable intermediary can
repair a peer without a direct connection to the original writer.

Incoming changes and receive progress commit atomically. The sqlite-express
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
