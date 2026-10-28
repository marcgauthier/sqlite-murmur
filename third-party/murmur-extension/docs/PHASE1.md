# Phase 1 — `murmur-crdt` extraction + engine wiring (done)

## Result

The shared conflict core now lives in a new `murmur-crdt` crate inside
MURMUR-RUST (`crdt/`). The engine consumes it with zero behavior change
(all suites green, test counts conserved), and the extension crate resolves
all three engine crates with no feature conflicts.

## The new crate (`MURMUR-RUST/crdt/`)

Pure, rusqlite-types-only core (no database, workers, or transport):

| Module | Contents | From |
|---|---|---|
| `model` | `Version` (HLC + `next`), `Value`, `Cell`, `RowState`, `Tombstone`, `Table`/`Column`/`Schema`/`ReplicaState`, `MergePolicy`, `encode`/`decode`/`id`/`quote`, vector-search types | `model.rs`, `search.rs` |
| `merge` | LWW row merge, `join`/`join_batch`, `prepare*` | `merge.rs` verbatim |
| `meta` | `RowMeta`, `04/meta/row` + `05/meta/tomb` key formats | `metadata.rs` (layout only) |
| `table_replication` | opt-in flags, `enabled`/`filter`/`filter_schema` | `table_replication.rs` (pure part) |
| `schema` | `pk_columns`, `index_table` | `schema.rs` helpers |
| `schema_codec` | positional wire codec for `Schema` | `schema_codec.rs` verbatim |
| `managed` | managed-UUID key scheme (`COLUMN`, `uuid`, `validate_row`) | `primary_key.rs` |
| `compare`, `digest` | Min/Max value order, SHA-256 helpers | `policy.rs`, `digest.rs` |

Deliberately left in the engine: changeset parsing (rusqlite-bound),
migration machinery, managed-key SQL rewriting, policy intents, store I/O
(`meta::read`/`put`), and replication-gated record filtering. The extension
gets natural-key counterparts in Phase 2 instead of inheriting managed keys.

`replication` feature flag mirrors the engine's (batch merge + extrema join).

## Engine rework (behavior-preserving)

- Every internal path kept working via re-exports (`model::Version`,
  `schema::pk_columns`, `metadata::row_key`, `merge::join`, …); `merge.rs` and
  `schema_codec.rs` deleted, `digest.rs` became a shim.
- New `From<murmur_crdt::Error>` bridge; 8 call sites convert error types
  (`storage`, `network`, `policy`, `high_low`).
- No new warnings (one fewer than baseline, verified by stash comparison).

## Go client fallout (one real addition)

`Version::next` went from `pub(crate)` to `pub` (the extension needs shared
clock-ticking). Per repo process this flowed through the full Go pipeline:

- `api_inventory.rs` now also scans `crdt/`; inventory diff was exactly the
  one new symbol (`sqlite_session::Version::next`).
- New native `version_next` dispatch op + Go `Version.Next` method (FFI-backed,
  so Go never reimplements conflict ordering) with native + Go tests.
- `api-inventory.json`, `api-coverage.json`, `ABI.md`, Go README table updated;
  audit passes (847 symbols); `go vet` clean; CI workflow now watches `crdt/**`.
- CHANGELOG `Unreleased` entry.

## Extension wiring (`tests/wiring.rs`)

Dev-deps on the three engine crates (absolute path deps, temporary — git
coordinates in Phase 5) with smoke tests: LWW join picks the newer cell, HLC
ticks forward, metadata key prefix + default-disabled opt-in, Spool
put/flush/get roundtrip, replication record codec roundtrip. This also proves
`rusqlite[loadable_extension]` unifies cleanly with `murmur-crdt`'s minimal
rusqlite (no `preupdate_hook`/`bundled` leakage).

## Evidence

- Engine default suite: 16 binaries green; replication suite: 20 green.
- Test conservation: baseline lib 43+6 ignored = post 38+5 (engine) + 5+1 (crdt).
- `murmur-crdt` standalone: 3 (default) / 5+1 ignored (replication).
- Native tests 5/5; full Go suite 21 packages ok; spool smoke 7/7.
- Extension: 8/8 (3 spike + 5 wiring).
