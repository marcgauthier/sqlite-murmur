# WP-E integration proposal (lead merge)

All items below touch lead-owned/shared files. Proposed exact changes;
worker implemented and tested only the owned side
(`crates/murmur-query/**`, `tests/query_*`).

Evidence (owned side, this session, offline): `cargo test --offline
--frozen` → 23/23 green (default features) and 21/21 green
(`--no-default-features`, vector tests cfg'd out), after the
main-schema-shadow revision (temp_store test replaced by
physical-name/adopt/reconcile/collision tests); `cargo check` zero
warnings; `g++ -std=c++17 -Wall -Wextra -fsyntax-only` clean for both
C++ tests. Link note used: `RUSTFLAGS="-L /tmp/sqlite-lib"`. Runs used
a transient lockfile (deleted after) per the no-standalone-lock rule.

## 1. Cargo wiring + vendor + lock (lead)

- `crates/murmur-replication/Cargo.toml`: add
  `murmur-query = { path = "../murmur-query" }` (mirrors the
  `murmur-tx` path-dep; default features keep `vector` on).
- Vendor delta (exact, from lockfile diff): `uuid 1.28.0`
  (`7cc11863…`, features `v5`), `sha1_smol 1.0.1`, `sqlite-vec 0.1.6`
  (`4ec77b84…`). All other murmur-query deps resolve to the exact
  versions already vendored (pins in `murmur-query/Cargo.toml`).
  Suggested: `cargo vendor --offline` into
  `crates/murmur-replication/vendor/` + parent `Cargo.lock`
  refresh + `sources.json`/`verify.py` extension per constraint 5.
- No `Cargo.lock` in `murmur-query/` per the path-dep hygiene rule
  (mirrors `murmur-tx`/`murmur-sched`): all builds and tests resolve
  through the parent `murmur-replication/Cargo.lock`. Run this crate's
  unit tests via `cargo test --offline --frozen -p murmur-query` from
  `crates/murmur-replication/` once the vendor delta below lands.

## 2. PRAGMA wiring (lead, `config.rs`)

Fall through to the WP-E spec after local names, in both
`default_value` and `validate`:

```rust
// default_value:
_ => murmur_query::adapt::default_value(name).unwrap_or(return None) // adapt to local shape
// validate:
murmur_query::adapt::validate(name, value)? // for WP-E names
```

Names/defaults: `search_queue_bytes=8388608`,
`search_batch_rows=256`, `search_interval_ms=100`,
`sub_buffer_size=32`, `sub_max_history=1024`,
`sub_emit_unchanged=0`, `migrate_chunk_rows=2000`,
`migrate_chunk_bytes=2097152`, `exec_max_statements=1024`
(all in `murmur_config` with read-back; ranges in
`adapt::validate`). Live-apply: after a search-limit change, call
`manager.set_limits(queue, batch)` on the worker-held `Manager`.

## 3. SQL actions (lead, `sql.rs`)

Register (DIRECTONLY, autocommit-required like `murmur_start`) and
delegate to `murmur_query::adapt::actions` on the worker connection:

| SQL function | Handler |
|---|---|
| `murmur_search_create_fts(name,table,cols_json,tok)` | `create_fts` |
| `murmur_search_create_vec(name,table,col,dims,metric)` | `create_vec` |
| `murmur_search_drop(name)` | `drop_index` |
| `murmur_refresh_indexes()` | `refresh` |
| `murmur_search_status()` | `status` |
| `murmur_column_fts(table,col,enabled)` | `set_column_fts` |
| `murmur_column_vector(table,col,dims,metric)` | `set_column_vector` |
| `murmur_migrate(sql, mappings_json)` | `migrate` (+ flag callback §5) |
| `murmur_exec_json(statements_json)` | `exec_json` (+ `exec_max_statements` cap read) |
| `murmur_export(since_version)` | `export_changes` |
| `murmur_import(state_json)` | `merge_state` (Unavailable seam) |

## 4. Worker state + tick (lead, `storage.rs`)

Worker holds `search: murmur_query::search::Manager`,
`query_schema: murmur_query::Schema`, and
`sub_mgr: Arc<murmur_query::SubscriptionManager>` (id: murmur node id,
history: `murmur_sub_max_history`).

- Open: `ensure_search_tables` → `schema::inspect` →
  `Manager::register` → `Manager::new(search_options(...))` →
  `sync_schema` → `primary_key::register` (managed-key UDFs).
  Shadows persist in main schema, so `sync_schema` adopts them
  (no temp_store handling). Call
  `murmur_query::search::ensure_vec_module` on every app connection
  in `sql.rs register()` (vec0 is per-connection state).
- After each commit batch: changed `(table, INTEGER pk)` pairs from
  `crsql_changes` since the last tick → `manager.observe(table,
  &keys)`; then `while manager.pending() { manager.tick }` on the
  `murmur_search_interval_ms` cadence; `take_publications` +
  `sub_mgr.notify_commit(crsql_db_version(), tables)`.
- After `set_explicit` (index CRUD): re-fetch `manager.guard` for the
  authorizer and `sub_mgr.notify_reset("search indexes reconfigured")`.

## 5. Flag store (lead)

`schema::inspect`/`actions::migrate` take `&dyn Fn(&str) -> bool`
(table-ID → replicated). Until WP-A flags land: resolve table ID→name
via the held `Schema` and return `murmur_query::adapt::is_crr(c,
table)` (CRR clock-table probe). WP-A later refines with its flag map.

## 6. Subscription C API (lead header + Rust bridge)

Proposed surface (test `tests/query_subscribe.cpp` written against
it; final declarations hosted by lead in a public header):

```c
typedef struct murmur_sub murmur_sub;
typedef struct { int buffer_size; int emit_unchanged;
                 const char *resume_cursor_json; } murmur_sub_opts;
murmur_sub *murmur_subscribe(sqlite3 *db, const char *sql,
                             const char *tables_json,
                             const murmur_sub_opts *opts);
int murmur_sub_poll(murmur_sub *s, int timeout_ms, char **event_json);
void murmur_sub_free_event(char *event_json);
void murmur_sub_close(murmur_sub *s);
```

Bridge: `murmur_subscribe` builds a fetch closure over the worker read
path (run `sql`, serialize rows to JSON) and calls
`murmur_query::start_subscription(&sub_mgr, crsql_db_version(), fetch,
key_fn, opts)`; `key_fn` hashes the row JSON. Events serialize
`Initial/Update/Reset` to JSON. `notify_commit` (see §4) drives wakes;
`notify_reset` on schema/search reconfiguration.

## 7. CTest wiring (lead, `CMakeLists.txt`)

```cmake
add_executable(sqlite_murmur_query_search_test tests/query_search.cpp)
target_link_libraries(sqlite_murmur_query_search_test PRIVATE sqlite_murmur sqlite_murmur_crsqlite)
add_test(NAME sqlite_murmur_query_search COMMAND sqlite_murmur_query_search_test ${CMAKE_CURRENT_BINARY_DIR}/query_search.db)
add_executable(sqlite_murmur_query_subscribe_test tests/query_subscribe.cpp)
target_link_libraries(sqlite_murmur_query_subscribe_test PRIVATE sqlite_murmur sqlite_murmur_crsqlite)
add_test(NAME sqlite_murmur_query_subscribe COMMAND sqlite_murmur_query_subscribe_test ${CMAKE_CURRENT_BINARY_DIR}/query_subscribe.db)
```

(Link line mirrors `sqlite_murmur_replication_test`; adjust if the
integrated target shape differs after the A1 gate.)

## 8. Error mapping note (for WP-F)

`murmur_query::Error` mirrors the archive taxonomy minus `WrongKey`
(no crypto here) plus `Unavailable(String)` for the WP-A merge seam →
map to `SQLITE_MURMUR_UNAVAILABLE`. All other variants map 1:1 to the
approved WP-F codes.

## 9. Open threads

- Managed-key table rebuilds preserve CRR clocks (WP-E owns
  rewrite/validation; WP-A reviews clock preservation at its slice).
- `murmur_import` stays `Unavailable` until the WP-A merge engine
  lands; `murmur_export` shape is the agreed handoff.
- `crdt_compat.rs` removal once WP-B's `murmur-crdt` rlib lands.
