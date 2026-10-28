# Phase 3 — Two-node merge (done)

## Result

Two databases converge through Murmur batches: export local state, apply
remote batches, resolve conflicts with engine-identical merge. New SQL API
(`murmur_export`, `murmur_apply`, `murmur_database_id`,
`murmur_set_database_id`, `murmur_clock`), loop-free application, HLC clock
propagation, and a crash journal that heals stranded applies on reopen.
Verified end to end: 34/34 tests plus a two-process CLI exchange in both
directions with idempotent re-apply.

## Design

**Receive path** (`sync.rs`) mirrors the engine (`network.rs`): validate,
filter to locally-enabled tables, normalize onto the local schema,
`merge::prepare`, commit. Two deliberate divergences, both forced by
independent adoption (nodes adopt tables on their own, no managed DDL):
unknown tables are *dropped* (scope intersection) where the engine errors,
and there is no `preserve_local` because the baseline reconstructs all
adopted tables. Genuine divergence (same table ID, different shape) and
remote rows missing non-null columns still error loudly; missing nullable
columns are padded with `Null` at version zero.

**HLC receive rule.** Even when the patch is empty, a peer-advanced clock
persists (`max(local, remote)`). This is load-bearing, not bookkeeping:
without it our next tick could re-mint a version the peer already used for
different data, and the merge would fail `Corrupt` on contact.

**Generation fencing is version-independent** (engine rule,
`merge.rs:153-157`: a row survives iff tombstone generation < row
generation). A same-generation tombstone fences the row no matter how new
its cells are: deletes always win within a generation, later updates never
resurrect. Only re-insert escapes, and only when HLC causality orders it
above the tombstone — the exchange *between* delete and re-insert is what
carries the clock across, not wall luck. Backfilled rows share generation
zero (the pre-adoption incarnation) with freshly-ticked cells.

**Tick protocol, hardened.** Collect still drains before ticking, but three
review findings changed the machinery around it:
`SpoolEntry.last_tick` makes ticks `max(persisted clock, last_tick)`,
covering versions minted-but-uncommitted by live peers (including writers
SQLite may admit beside the caller, e.g. `BEGIN CONCURRENT`); the backfill
commit now persists its own clock bump atomically with the metas (without
it a restart re-minted versions at or below the backfill); and drain acks
run fail-fast — when the caller holds the write lock, acks defer while
*processing continues*, so in-transaction collects after undrained commits
stay correct instead of wedging on `BUSY`. Deferred rows reprocess
idempotently and ack once the lock is free.

**Apply journal.** Metadata commits before SQLite application, but — the
review's key correction — re-merging after that commit yields an *empty*
patch (the baseline already has the rows), so no re-apply could ever heal
SQLite. The metadata commit therefore carries the pending write-set
(`patch.rows`, `patch.deleted`) at `00/apply/journal/ext-v1`, cleared once
SQLite confirms. `replay_journal` runs at open and atop every apply;
upserts/deletes by key make replay idempotent, so every crash landing is
safe: before the commit (nothing happened), after it (replay completes),
after SQLite but before the clear (replay re-applies identically). One key
suffices because applies serialize under `CORE`.

**Loop suppression.** Applies run on a dedicated connection with no
session attached: applied writes can never be captured back into the
outbox. Reverse-direction liveness is unaffected (each side's own writes
still capture and flow).

**Strict 1:1 spool↔database.** Metadata keys carry no database qualifier,
so sharing a spool across databases would merge same-named tables; the
reverse direction is now refused too. Bindings are process-lifetime (never
evicted), matching the existing forward guard.

**Export drains first.** Export reads Spool, so it drains on entry;
otherwise committed-but-undrained rows would make exports silently stale.

**Restrictions.** Apply in autocommit (inside a write txn the dedicated
connection blocks on the lock, then fails loudly — retryable, never
silent). Triggers on synced tables whose *apply-path* side effects land on
other synced tables are not captured (no session on the apply connection);
app-connection cascades capture normally. Every writer must load the
extension — writes that bypass capture are invisible by design, and journal
replay will overwrite them. Read-only databases remain unsupported (Phase 2
standing). Same-transaction INSERT+DELETE still nets to nothing (Sessions,
not us).

## Test notes

- `tests/merge.rs` policy: conflicting-write tests assert bilateral
  equality (same SQLite, byte-identical metadata, same clock), never the
  winner — except `later_*` tests, which fix the tick order with a 5ms
  sleep and assert the deterministic LWW winner, and fencing tests, which
  pin the generation rule from both sides.
- The crash-window test plants a real journal (rows built from live Spool
  metadata) over deliberately regressed SQLite and reopens: no timing,
  no SIGKILL, exact crash-state reproduction. Verified it fails with
  replay stubbed out.
- `tests/sync.rs` gained clock-bump-on-backfill, cross-DB spool refusal,
  and tick-monotonicity-across-rollback (which also proves deferred acks
  clear on the next autocommit drain).

## Evidence

- 34/34, zero warnings (lib + tests): 12 merge (convergence, double-apply
  no-op, loop suppression + reverse liveness, database mismatch, scope
  filtering, self-apply + empty-batch rejection, export freshness, fencing
  ×2, ordered LWW, re-insert resurrection, journal healing) + 15 sync + 2
  recovery + 5 wiring.
- Manual cross-process check (`/tmp/mur3`, since cleaned): two `sqlite3`
  processes, stable table IDs both sides, 365-byte batch A→B lands
  (`rows:1`), B→A converges to two rows, re-apply returns all zeros.
