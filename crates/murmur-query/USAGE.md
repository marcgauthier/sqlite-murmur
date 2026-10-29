# murmur-query usage (WP-E, points 15–18)

Embedded query plane: disposable FTS5 + vec0 search indexes with
background maintenance, live subscriptions, migrations, and managed
keys. All behavior below is available once the lead merges the
integration proposal (`INTEGRATION-PROPOSAL.md`); the PRAGMA names,
defaults, and SQL actions are fixed by that proposal.

## PRAGMAs (constraint 8)

Every option is a `PRAGMA murmur_*` with read-back and `murmur_config`
persistence. Search limits apply live; subscription limits apply to new
subscriptions (`sub_max_history` needs a restart); migration/exec limits
apply per call.

| PRAGMA | Default | Range |
|---|---|---|
| `murmur_search_queue_bytes` | 8388608 | 65536..134217728 |
| `murmur_search_batch_rows` | 256 | 1..65536 |
| `murmur_search_interval_ms` | 100 | 10..86400000 |
| `murmur_sub_buffer_size` | 32 | 1..4096 |
| `murmur_sub_max_history` | 1024 | 16..100000 |
| `murmur_sub_emit_unchanged` | 0 | 0..1 |
| `murmur_migrate_chunk_rows` | 2000 | 1..100000 |
| `murmur_migrate_chunk_bytes` | 2097152 | 1024..67108864 |
| `murmur_exec_max_statements` | 1024 | 1..100000 |

Example:

```sql
PRAGMA murmur_search_batch_rows=512;
PRAGMA murmur_search_batch_rows;  -- read-back: 512
```

## Search (point 15)

Source tables need one `INTEGER PRIMARY KEY`. FTS columns must be `TEXT`;
vector columns must be `BLOB`s of little-endian float32 values
(`dimensions * 4` bytes, all finite). Index contents are disposable and
rebuild from source rows; declarations persist node-locally in
`murmur_search_columns` (never replicated).

```sql
-- Explicit FTS index over two columns (tokenizer: unicode61, ascii,
-- porter, or trigram). Queries use the physical shadow name, unqualified.
SELECT murmur_search_create_fts('docs_fts', 'docs', '["title","body"]', 'unicode61');
SELECT * FROM murmur_fts_docs_fts WHERE murmur_fts_docs_fts MATCH 'hello';

-- Explicit vector index (metric: l2, cosine, l1).
SELECT murmur_search_create_vec('docs_vec', 'docs', 'embedding', 128, 'cosine');
SELECT rowid, distance FROM murmur_vec_docs_vec
 WHERE embedding MATCH '[0.1,0.2,...]' AND k = 10 ORDER BY distance;

-- Column declarations (archive naming: murmur_fts_<tableid>,
-- murmur_vec_<tableid>_<columnid>).
SELECT murmur_column_fts('docs', 'title', 1);
SELECT murmur_column_vector('docs', 'embedding', 128, 'cosine');
SELECT murmur_column_vector('docs', 'embedding', NULL, NULL); -- clear

-- Blocking refresh + readiness (JSON array of SearchIndexStatus).
SELECT murmur_refresh_indexes();
SELECT murmur_search_status();

SELECT murmur_search_drop('docs_fts');  -- 1 when dropped
```

Index tables live in main schema as `murmur_fts_*` / `murmur_vec_*`
shadows (the archive's TEMP tables are per-connection and invisible to
app connections). Shadows persist across opens with their contents: an
unchanged definition is adopted as-is, a changed one is dropped and
recreated, and a same-named ordinary table fails safely without being
touched. Shadows ride the encrypted VFS and are excluded from schema
inspection and CRR handling by the `murmur_*` skip rules. The vec0
module registers per connection (every app connection plus the
worker), so all readers see vector indexes.

Background maintenance runs off the application path: the worker feeds
changed keys from `crsql_changes` and ticks the manager every
`murmur_search_interval_ms`, processing at most
`murmur_search_batch_rows` rows per turn. Budget overflow invalidates to a
rebuild, exactly as in the archive.

Limitations: FTS `MATCH` and vector `MATCH ... AND k = n` are the query
dialect (no ApproximateNN tuning knobs); declarations are node-local
(the archive replicates the declarations, not the contents).

## Live subscriptions (point 17)

C API (poll-based opaque handle; declarations land in the public header
via lead merge, implementation here):

```c
murmur_sub *murmur_subscribe(const char *sql, const char *tables_json,
                             const murmur_sub_opts *opts);
int murmur_sub_poll(murmur_sub *s, int timeout_ms, char **event_json);
void murmur_sub_close(murmur_sub *s);
```

Events are JSON: `Initial {rows, cursor}`, `Update {rows, changes,
cursor}` with per-key `added/updated/removed` diffs, or `Reset {cursor,
reason}` when a slow consumer overflows (buffer caps at
`murmur_sub_buffer_size` events), the schema resets, or the query fails.
Cursors bind `{database_id, epoch, sequence}` where sequence is
`crsql_db_version()`; resume cursors older than `murmur_sub_max_history`
commits are rejected as expired. Table filters (`tables_json`, empty =
all tables) mute unrelated commits.

## Migrations and managed keys (point 18)

```sql
-- Applies SQL atomically, then validates the resulting schema.
-- Mappings: '[{"old_name":"a","new_name":"b","columns":[["x","y"]]}]'.
SELECT murmur_migrate('ALTER TABLE docs ADD COLUMN body TEXT', '[]');
-- Returns the new schema-history hash (hex).
```

Rules (ported from the archive): every table needs a non-null primary
key; `VIRTUAL`/`CHECK`/`REFERENCES`/`GENERATED`/`AUTOINCREMENT` shapes,
non-PK unique constraints, and application triggers/views are rejected;
cr-sqlite (`%crsql%`, `%fractindex%`) and `murmur_*` system objects are
exempt. Dropping/renaming replicated tables or columns is rejected with
`disable replication first`; a table counts as replicated when it is a
CRR (WP-A per-table flags refine this when they land).

Managed keys: tables opted into replication use the deterministic UUID
column `id_murmur_primary_key` (same `murmur/replication-primary-key/v1`
scheme and reference vectors as the archive). Application `INSERT`s are
rewritten automatically; writing the managed column directly is rejected
(`SQLITE_AUTH`). Managed-key table rebuilds preserve CRR clocks (WP-A
reviews clock preservation per the approved split).

## Query ergonomics (point 16, adapted)

```sql
-- One-transaction batch with managed-key rewriting (value-complete SQL,
-- no bound parameters). Cap: murmur_exec_max_statements.
SELECT murmur_exec_json('["INSERT INTO docs VALUES(1,''a'')",
                          "INSERT INTO docs VALUES(2,''b'')"]');
-- {"statements":2,"rows_changed":2}

-- Change export for the WP-A merge engine (read-only).
SELECT murmur_export(42);
-- {"db_version":57,"changes":[{"table":...,"pk":"...","cid":...}, ...]}

-- Import seam: reports UNAVAILABLE until the WP-A merge engine lands.
SELECT murmur_import('...');
```

Streaming needs no API (SQLite prepared statements already stream);
worker-side statement caching uses `prepare_cached` internally. There is
no Go `database/sql` adapter (out of scope per brief).

## Embedding-worker integration recipe

The worker holds one `search::Manager` plus the current `Schema`:

1. On open: `ensure_search_tables`, `Schema` via `schema::inspect`,
   `Manager::register`, `Manager::new(search_options(...))`,
   `sync_schema`, and `primary_key::register` (managed-key UDFs).
   Call `search::ensure_vec_module` on every app connection too.
2. After each commit batch: read changed `(table, INTEGER pk)` pairs from
   `crsql_changes` since the last tick and call `manager.observe`.
3. Every `murmur_search_interval_ms`: `while manager.pending() {
   manager.tick }`, then publish `take_publications` and notify
   subscriptions (sequence: `crsql_db_version()`).
4. After PRAGMA changes to search limits: `manager.set_limits`.
5. After `set_explicit` (index CRUD): re-fetch `manager.guard` for the
   authorizer, and `notify_reset` subscribers.

## Errors

`Error::{Invalid, Corrupt, SchemaMismatch, Busy, Closed, ...}` mirror the
archive taxonomy; `Unavailable` marks the WP-A merge seam and maps to
WP-F `SQLITE_MURMUR_UNAVAILABLE`. UDF/SQL failures surface through the
embedding error path (WP-F mapping).
