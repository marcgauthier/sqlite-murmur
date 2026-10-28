# Phase 4 — QUIC replication (done)

## Result

Nodes sync live over the engine's QUIC transport through a
`murmur_replication::Backend` over our Spool store: new SQL API
(`murmur_repl_start/stop/add_peer/status/sync/pubkey`, `murmur_node_id`),
log-from-birth publishing, offline catch-up from infinite retention, and
multi-connection node-level registration. Verified end to end: 41/41 tests
with real TLS/QUIC sockets, including a 3000-row chunked batch and exact
log-count proofs. WAL mode became a hard requirement along the way (below).

## Design

**Log-from-birth, transport-on-demand** (`repl.rs`, `outbox.rs`,
`schema_map.rs`). Every drained batch and every backfill signs its delta
into the origin log in the *same atomic commit* as its state, whether or
not transport runs — so a late `murmur_repl_start` never loses history and
state and log can never diverge. `sign_log` mirrors the engine's
`sign_content` key-for-key (retained head, log, time, sequence, progress,
receipt). Empty deltas sign nothing and consume no sequence; over-sized
deltas split into consecutive sequences sharing the batch clock (4MB
payload budget, well under the 8MB wire frame; receivers merge each
transaction independently, so a partial prefix is still valid CRDT state).

**Backend** (`ReplBackend`): raw key/value (`get`/`keys`/`commit`,
Spool-direct, no `CORE`) plus logical ops under `CORE` (`apply`,
`snapshot`, `schema`, `observe`). `apply` drains local capture first, then
merges each payload through the Phase 3 `apply_batch` (fresh baseline per
payload), persisting only `07/replication/` records and rejecting anything
else — we have no files/policies/bridge, so `publish` is `Unavailable` and
`import_bridge`/`release_ownership` decline. `observe` bumps clock and
`last_tick` with the `[0xff;16]` node marker, engine-style. Reclaim is a
60s-gated `store.reclaim()` (the store opens with no auto-reclaim).

**Retention without snapshots.** `prune_history` returns 0 and
`retention_checkpoint` is a no-op: history is never removed, so offline
peers always catch up through the log (`Need` ranges) and durable
snapshots (`create_snapshot`, default `Unavailable`) are never needed.
The price is unbounded log growth — documented, and the reason snapshots
stay on the future list.

**Record-only schema flow.** We start with `accept_remote_schema: false`,
so manifests ACK without adopting (tables stay locally adopted;
`accept_schema` is unreachable and declines). Schema bytes are normalized
(enabled tables sorted by ID, columns sorted, DDL cleared) so same-shape
nodes agree regardless of adoption order; the merge path re-checks shapes
per table anyway, and scope filtering keeps divergent-scope peers flowing.
Permanent digest mismatch costs only the engine's 5s `want_schema`
heartbeat — staging never gates on it.

**Locking.** Backend KV never takes `CORE`, so the engine's synchronous
start path (`bind_signer`, `migrate_peers`) is safe under it. Drains take a
`&Core` token parameter — compile-time proof that sequence assignment
(read-modify-write) never runs concurrently. `Handle::close` joins the
replication thread and never runs under `CORE` (`murmur_repl_stop` takes
the handle, drops the guard, closes, re-locks to evict).

**Lifecycle.** The handle lives node-level in `SpoolEntry`, shared by all
connections on the database (second connections see status, cannot
double-start, may stop). Last close with a running handle leaves a zombie
entry instead of evicting — the node keeps relaying headless and a reopen
finds its store; `evict_if_orphan` closes only at refs-zero plus
replication-stopped. `open_impl` rolls its tail back on failure (spool ref
plus db binding), so failed opens leave zero trace.

**Identity.** Node ID, database ID, and the 32-byte signing seed are
spool-minted under `CORE` at open; `murmur_node_id` and
`murmur_repl_pubkey` expose the public halves (peers config is
`{"<nodehex>":"<pubkeyhex>"}`; the seed never leaves Spool). Self registers
in `trusted_origins` automatically; TLS uses caller-supplied DER
chain/key/roots with `replicateddb://node/<hex>` SANs (same profile as the
engine's transfer tests). `set_database_id` locks once the origin log is
non-empty — log entries bind the id at sign time and are immutable, so
linking must precede the first write (same-value re-set stays a no-op).

**WAL is required.** Stress-testing large batches found that in rollback
mode a large *uncommitted* write transaction blocks second-connection
reads for its whole duration — sharp onset between 1MB and 2MB on stock
SQLite 3.45.1, reproduced with pure SQLite and no extension loaded. That
would wedge drains, exports, and applies, so `murmur_open` sets
`journal_mode=WAL` (persistent, one-time) and refuses loudly if the mode
doesn't stick — which on a fresh database means the first open ran inside
a transaction (the pragma is a silent no-op there). Under WAL readers
snapshot: the same probe answers in microseconds.

**Restrictions.** Standing from Phase 3: apply in autocommit, no
apply-path trigger cascades onto synced tables, every writer loads the
extension. New: unbounded log growth; no durable snapshots (long-offline
peers replay the full log range); engine↔extension sync is untested
(same protocol version and keyspace, but the engine errors where we
scope-drop — interop is future work, not a claim).

## Test notes

- `tests/repl.rs` (no `SERIAL`: disjoint fixtures/ports, real sockets):
  certs are minted per test from live node IDs (read-then-mint, since IDs
  are random), engine timers run at 500/250ms, and async arrival uses
  bounded `wait_for` polls (30–90s).
- Log-count assertions are exact where the protocol makes them so (4/4
  for offline catch-up — own four plus relayed four, no snapshots) and
  lower-bounded where retries could add nothing but never do.
- Stop replication before close in any test that reads Spool directly —
  a zombie entry holds the file lock by design (covered by the lifecycle
  test: close with replication running, reopen, status still live).
- Two nodes share one process `CORE` in tests, which is strictly harder
  than production (separate locks per process); no separate CLI check —
  the sockets, TLS handshake, and engine threads are already real.

## Evidence

- 41/41, zero warnings (lib + tests): 12 merge + 16 sync (new: WAL
  enforcement with zero-trace failed open) + 2 recovery + 6 repl (live
  bidirectional incl. pre-start history, offline catch-up with exact
  counts, 3000-row chunked batch, lifecycle incl. zombie reattach,
  multi-connection sharing, db-id lock) + 5 wiring.
- All pre-existing suites stayed green under WAL enforcement unchanged.
