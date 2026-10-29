# Source provenance

WP-E query plane (`murmur-query`): disposable search indexes (FTS5 + vec0),
live subscriptions, migrations, and managed keys. Ported from the
user-provided Murmur Rust implementation at
`/media/marc/2TB/Archives/MURMUR-RUST`, Git commit
`e5a19c25cceaf93375872e3a7f01f05ffc3659ac` (Release 1.0.0, 2026-10-21).

## Copied modules (md5 at copy time)

| File here | Archive source | md5 |
|---|---|---|
| `src/search.rs` | `src/search.rs` (850 lines) | `7f781571a10aaf415845718905066cee` |
| `src/subscription.rs` | `src/subscription.rs` (535 lines) | `d69156a2fd8ca850ba41d694734fd277` |
| `src/schema.rs` | `src/schema.rs` (925 lines) | `a0014b0acf5458e779de170cad6f6b71` |
| `src/primary_key.rs` | `src/primary_key.rs` (896 lines) | `0323e7a77ea803807613309727d938e5` |

`src/crdt_compat.rs` assembles minimal verbatim copies from
`crdt/src/{digest,model,managed,schema}.rs` plus `Migration`/`TableMapping`
from `src/model.rs` (lead-approved temporary duplication; see the REMOVAL
NOTE in the file — delete it in favor of the WP-B `murmur-crdt` crate when
that lands). Omitted as untouched by WP-E: `PolicyOperation`,
`PolicyIntent`, `Tombstone`, `ReplicaState`, `meta`, `merge`, `logical`,
`table_replication`. `src/metadata.rs` (Spool row metadata) was deliberately
NOT imported: WP-A/backend owns it.

## sqlite-murmur adaptations (all lead-approved)

- `search.rs`: archive `Options` replaced by local `SearchOptions` (same
  fields/defaults); the SQLite session-changeset feed
  (`enqueue`/`decode_pending`, `ChangesetIter`) replaced by
  `Manager::observe(table, keys)` driven from `crsql_changes`, so no
  `session` rusqlite feature is needed; added `set_explicit` (mirrors
  archive `set_search_indexes`) and `set_limits` (PRAGMA live-apply);
  visibility raised to `pub`.
- `search.rs` revision (lead-ordered, post-review): archive TEMP index
  tables replaced by MAIN-schema shadows (`murmur_fts_*` /
  `murmur_vec_*`, idempotent prefixing) because TEMP tables are
  per-connection while population runs on the worker connection.
  `create_sql`/`drop`/`delete`/`insert`/table-listing target main
  schema; `install()` adopts same-DDL tables, recreates on DDL
  change, and fails safe on ordinary-table collisions; `status()`
  reports physical names; `temp_store=MEMORY` dance removed (shadows
  ride the encrypted VFS); new `pub fn ensure_vec_module(c)` for
  per-connection vec0 registration. The archive `temp_store` unit
  test was replaced by physical-name/adopt/reconcile/collision
  tests.
- `subscription.rs`: `start_subscription` takes an explicit fetch closure,
  `&Arc<SubscriptionManager>`, and commit sequence instead of the archive
  `Database` handle; queue/manager/diff/worker logic unchanged. Commit
  sequence is `crsql_db_version()` per lead decision.
- `schema.rs`: `inspect` takes a `replicated: &dyn Fn(&str) -> bool`
  callback (WP-A flag seam) instead of reading the Spool-backed
  `table_replication` map; cr-sqlite (`%crsql%`, `%fractindex%`) and murmur
  (`murmur_*`) system objects exempt from inspection; session-capture
  `capture()` removed; `put_user_rows`/`delete_user_rows` ungated.
- `primary_key.rs`: managed-key helpers ungated; the `Database`-based
  recovery test dropped (no `Database` here), UUID reference vectors kept;
  added a connection-free rewrite/reference test.
- `src/adapt.rs` (new): WP-E PRAGMA spec, node-local search-declaration
  store (`murmur_search_columns`, replacing replicated `Column.fts`/
  `Column.vector` flags), SQL-action handlers, `crsql_db_version` helper,
  and the WP-A merge-engine seam (`merge_state` → `Unavailable`).

## Dependencies

Direct dependencies pin versions from the shared vendor tree: rusqlite 0.40.2,
serde 1.0.229, serde_json 1.0.154, postcard 1.1.3, aws-lc-rs 1.18.2,
thiserror 2.0.21, and test-only tempfile 3.27.0. `sqlite-vec =0.1.6` is optional
behind the default-on `vector` feature (its build dependency `cc` is vendored).
Managed UUID-v5 generation uses the internal AWS-LC compatibility helper;
`uuid` and `sha1_smol` are no longer active workspace dependencies. The parent
`vec0.c` from MURMUR-SQL was not copied.

The supplied archive has no top-level LICENSE or license field in its
manifests. This records provenance; it does not assign a new license to
the imported material. Retain any applicable author permissions when
redistributing.

Dependency consolidation uses AWS-LC for randomness and the internal UUID-v5
helper where needed. Original identifier formats are covered by compatibility
fixtures. The standalone `murmur-crdt` reference workspace remains unchanged.
