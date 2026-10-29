# CRDT core import: integration proposal

Slice 1 imports the archived pure conflict machinery. Production policy SQL,
metadata persistence and High/Low operations belong to slice 2, after lead
approval. Merely linking the crate does not replace cr-sqlite's conflict
resolution or enable policy columns.

## Dependency and build boundary

Add `murmur-crdt = { path = "../murmur-crdt", features = ["replication"] }`
to murmur-replication. Direct versions are exact archive pins: serde 1.0.229,
serde_json 1.0.154, postcard 1.1.3, sha2 0.10.9, uuid 1.28.0 (v5), rusqlite
0.40.2 (default-features=false). Dev rand 0.8.8 supports the archived batch
merge tests. All are already in the shared vendor tree after the WP-E vendor
merge. Parent Cargo.lock governs this path dependency; do not add a standalone
lock or in-tree target directory.

Do not enable bundled SQLite here. Preserve the consumer's static SQLite or
loadable API-table linkage. The archive's bundled SQLite dev dependency and
tempfile dependency are unnecessary for the imported pure tests. Tests on
this host use the coordinator's `/tmp/sqlite-lib` system SQLite link shim.

`replication` is the archive's original feature, not a network switch. It
enables join_batch, prepare_delta, and Min/Max extrema joins. Without it,
ordinary row merging uses the archive's non-replication behavior. Production
policy integration must enable it explicitly.

Lead-owned updates: parent dependency/lock, CMake source dependency glob,
sources.json provenance and source-files.sha256, verifier, root usage docs.
The crate creates no runtime options, database, worker, or listener.

## Proposed lib.rs seams (awaiting lead ruling)

- Retain logical.rs verbatim as provenance source but do not declare its
  module. It owns a physical Spool wrapper and real Store/Mutation/Durability
  calls; importing it would add a second persistence engine. RocksDB remains
  authoritative. Its Spool reopen test is excluded, not reported as passed.
- Compile verbatim policies.rs by providing its original crate-root imports
  (serde, BTreeMap/BTreeSet, NodeId=[u8;16]) and Protocol(String) in Error,
  including the original replication display message. Preserve the three
  CRDT Error variants. This adds a type seam, not conflict logic.

The alternative is a re-export of murmur_tx::policies, retaining the copied
policies.rs as an uncompiled reference. Lead approval selects this seam.
Every other imported CRDT module is byte-identical to the archive.

## Available reusable Rust surfaces

| Module | Reused surface |
| --- | --- |
| model | Version/HLC, Value/SQLite conversion, Cell/RowState/Tombstone/ReplicaState, schema models, merge-policy intent types, codec, quote/id helpers |
| merge | Pure join/prepare, feature-gated batch join/delta, deletion floors and changed-row patches |
| policies | PN Counter actor components, observed-remove Set tags, Causal joins |
| compare | Total value order and exact integer/real comparisons for extrema |
| managed | Deterministic managed UUID keys and row validation |
| meta | Stable row/tombstone keys and nullable schema-extension behavior |
| schema/schema_codec | PK order, index target parsing, compatible historical schema encoding |
| table_replication | Stable-id opt-in and row/tombstone/schema filtering |
| digest | Shared SHA-256 and hex helpers |

The causal policy types match the archive's serialization. They are distinct
Rust types from the same source already compiled in murmur-tx; choose one
canonical owner for production state and re-export it at integration rather
than silently converting through JSON. If wire boundaries need a temporary
bridge, use the original postcard codec and test byte equivalence explicitly.

## Query-plane deduplication

Once WP-E revisions finish, replace temporary murmur-query/crdt_compat copies
of model, managed, digest and schema helpers with these shared exports. Keep
query-specific Migration/TableMapping types in the query crate: they are
from archived src/model.rs, not crdt/model.rs. Query error conversions must
map Invalid/Corrupt/SchemaMismatch (and Protocol if the seam adds it)
structurally. Do not change error behavior by stringifying all categories.
Coordinate with the query owner before editing those files. The core
introduces no source-table scans or synchronous replication work.

## Production policy integration is still required

CRSB carries CRR row changes, while ReplicaState and causal component records
use the archived model. Do not feed policy columns blindly through existing
LWW crsql_ingest_blob and call that CRDT policy support. The engine must reuse
archived src/policy.rs operations and causal metadata, associate stable
table/column identities, and atomically persist metadata with visible row
changes. Counter increments are per database/node actor components; sets
need operation identity and observed-remove tags. Storing only materialized
counter totals or set JSON loses the archive's convergence semantics.

Preserve delete/reinsert generation fences and tombstones. Peer retries must
not count an operation twice. Pending High/Low exports and ownership changes
must be considered before pruning metadata. The archive's high_low.rs uses
Core/Spool worker types and cannot be declared unchanged in this pure crate;
its adapter belongs to the next assigned slice and the Backend bridge hooks.

No policy-setting C API is proposed. The lead must approve per-column policy
and ownership configuration PRAGMAs, defaults, range validation, read-back
and murmur_config persistence before wiring slice 2. Mutating one-shot
operations (counter increment, set add/remove, bridge import/export) can be
SQL actions; configuring their policies remains PRAGMA-only.

## Validation

After parent-lock integration, run in the root with shared offline vendor
configuration and an external target directory:

```sh
RUSTFLAGS='-L /tmp/sqlite-lib' cargo test -p murmur-crdt \
  --manifest-path crates/murmur-replication/Cargo.toml \
  --offline --frozen --features murmur-crdt/replication \
  --target-dir /tmp/sqlite-murmur-crdt-target \
  --config 'source.crates-io.replace-with="vendored-sources"' \
  --config "source.vendored-sources.directory=\"$PWD/crates/murmur-replication/vendor\""
```

Compare every imported source with its archive counterpart; only lib.rs may
differ. Preserve the archived ignored microbenchmark as ignored. Record
pure-crate evidence separately from later CRR/mesh integration tests: policy
convergence under partitions, duplicate/reordered delivery, atomic rollback,
reopen/replay, and High/Low ownership fencing remain downstream gates.
