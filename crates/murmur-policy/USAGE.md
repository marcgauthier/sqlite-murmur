# Column-policy kernels

This rlib contains pure policy helpers reused from Murmur. It provides exact
numeric/type comparison for extrema, archived causal projection/baseline
join/orphan-rebase helpers, and re-exports the existing
`murmur_tx::policies` PN-counter and observed-remove set implementation.
It creates no database, persistence store, logical clock, wire protocol,
carrier table, SQL function or PRAGMA by itself.

The delivery includes pure carrier validation and reduction, plus a schema
constant; it executes no SQL. Production policy-column integration remains
behind the A4/native-ingest gate in INTEGRATION-PROPOSAL.md. Native CRR
keeps ownership of row/cell LWW, versions and delete/reinsert lifecycle.
JSON field merging and ordered-list merging are not implemented by the
supplied archive and remain deferred; a set is not an ordered-list CRDT.

The canonical counter primitive is directly reusable:

```rust
use murmur_policy::policies::Counter;

let mut counter = Counter::default();
counter.add([1; 16], [2; 16], 3)?;
counter.add([1; 16], [2; 16], -1)?;
assert_eq!(counter.value()?, 2);
# Ok::<(), murmur_tx::Error>(())
```

`causal::State<Stamp>` accepts event-order tokens already established by
the native CRR host. It never creates or advances a clock. The host must
define the concrete stamp using native accepted baseline/ownership ordering,
including CRR's actual tie-breaking rules; an invented timestamp or just a
per-node counter is insufficient. Generation and scope use native CRR row
incarnations. The serialized adapted State has no approved production wire
or persistence format yet.

Callers must validate table/column/key/branch/incarnation identity before
`join_state`, and row/column identity before `rebase_orphan`. These low-level
archived helpers assume their host already selected matching records.
They may modify their target before returning an error. Work on a private
candidate and publish only on success within the host transaction; never
persist a partially failed candidate. The host must enforce row-deletion
fences and branch selection before projecting causal state.

Counter projections include the selected baseline and return Limit if the
result is outside SQLite's i64 range. Set projections are postcard-encoded
Value vectors in a Blob. A newer baseline removes old synthetic seed tags
while preserving committed operations; concurrent unobserved additions
survive observed-remove operations. Min/Max comparisons preserve exact
integer/real boundaries and reject NaN numeric comparisons.

`carrier::Row` accepts decoded SQL fields while retaining native packed PK
bytes. `Target.live` and its incarnation come from the native host: no causal
length parity is assumed. `reduce_causal` and `reduce_extremum` validate target
identity, actor IDs, policy kinds, canonical version-one payloads, tags, NaN
and a supplied aggregate input-byte budget. This budget must come from the
existing host PRAGMA budgets; it is not a new configuration knob or total
process-memory cap. The baseline state and decoder scratch are additional.

Incomplete payloads and noncurrent incarnations increment deferred_rows and
do not affect projections. A nonlive target returns no projection. The host
retains deferred records according to its native lifecycle/retention rules.
Malformed data fails the whole pure reduction; the input baseline is never
modified. Counter components max-join, set markers retain observed-remove
semantics, and Min/Max inspect all supplied candidates irrespective of their
native LWW result. A4 must supply those candidates before dropping losers.
The reducer does not allocate tags, initialize baselines, write progress or
apply projections. It does not implement snapshots, GC or ownership storage.

`high_low` adds pure ownership/trust/receipt/release helpers adapted from the
archive. `Owner<Stamp>` retains only a native raw target key, ownership and
release fences, and an optional value with native event metadata. Promotion
and release require accepted event tokens supplied by the host; stale tokens
are rejected. A newer release dominates delayed older-epoch promotions even
when their cell version is larger. Scope selection chooses High authority or
already-resolved native Low provenance. It does not merge or retain Low rows.
The archived `__row__` control marker is reserved; the host must handle that
namespace when exposing ownership of application columns.

`high_low::check_import` is called only after the canonical bridge authenticates
and decrypts a bundle. It checks HighReceiver role, the trusted-domain set,
payload database identity and an existing receipt loaded under receipt_key().
Identical receipts return Replay; conflicting digests fail. Fresh receipts
must be committed atomically with the native apply before acknowledgement.
The helper does not verify signatures, store receipts, enforce bundle sequence
gaps or publish progress. WP-C's existing bridge handles cryptography/gaps;
the native host owns atomic application and retention. No High/Low SQL action
or control-carrier schema is installed by these helpers.

Persistent policy components, ownership, receipts and provenance must live
in SQLite tables in the same database. The helpers create no sidecar files,
directories, second database or direct RocksDB metadata store. The host
commits control state and native row changes atomically; the final air-gap
artifact is the coordinator-approved transport exception, outside this crate.

Slice-4's draft `ownership` module provides versioned grant/release envelopes,
prior-event references, target-key verification and constructors/comparison
for explicitly supplied native CRR metadata. Its consumer signatures and
append-only DDL await coordinator approval; see [OWNERSHIP-API.md](OWNERSHIP-API.md).
It creates no carrier table and performs no persistence. Target keys must
not be used to overwrite distinct immutable events. Native stamps cannot
be ordered across targets or treated as independent logical clocks.

The proposed configuration surface is exclusively
`PRAGMA murmur_column_policies`, a persisted JSON map defaulting to `{}`
(ordinary LWW). It is not installed by this crate. Policy mutation actions,
carrier writes, projection filtering, snapshots and retention all need
downstream integration before that PRAGMA can be exposed as functional.

After the coordinator adds this workspace member and dependency edge, test
through crates/Cargo.lock using the existing offline vendor configuration:

```sh
cargo test -p murmur-policy --offline --frozen \
  --manifest-path crates/Cargo.toml \
  --target-dir /tmp/sqlite-murmur-policy-target \
  --config 'source.crates-io.replace-with="vendored-sources"' \
  --config "source.vendored-sources.directory=\"$PWD/crates/murmur-replication/vendor\""
```

The kernel tests need no SQLite library. No standalone lock or in-tree
target directory is used.
