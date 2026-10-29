# Integration proposal: keep policy kernels on CRR

This delivery implements the approved pure kernels and carrier validation/
reduction. Production SQLite hooks remain behind the coordinator's A4
ingest/projection review. The full murmur-crdt reference tree is not a dependency.

## Lead-owned build changes

The coordinator has added `murmur-policy` to crates/Cargo.toml members
and its workspace Cargo.lock. A murmur-replication path dependency and CMake
wiring are explicitly withheld until an A4 consumer exists. All registry
dependencies exist in the shared offline vendor tree. Include this crate's
source/manifest in CMake's Rust build dependencies when consumed; keep
sources.json provenance and the source checksum inventory updated. No new
vendored registry sources should be necessary.

The canonical Counter/Set/Tag/Causal types come from murmur-tx and have exact
Rust type identity across crates. The Value codec and extrema comparison
reuse the archive's minimal helpers. No SQLite dependency, Spool, duplicate
merge engine or HLC is added.

## Approved carrier schema shape (constant only; no SQL installation)

Only the exact reserved name `murmur_policy_components` is approved:

```sql
CREATE TABLE murmur_policy_components(
  target_table TEXT NOT NULL,
  target_column TEXT NOT NULL,
  target_pk BLOB NOT NULL,
  incarnation INTEGER NOT NULL,
  actor_database BLOB NOT NULL,
  actor_node BLOB NOT NULL,
  kind INTEGER NOT NULL,
  tag BLOB NOT NULL,
  payload BLOB NOT NULL DEFAULT X'',
  PRIMARY KEY(target_table,target_column,target_pk,incarnation,
              actor_database,actor_node,kind,tag)
) WITHOUT ROWID;
```

Target identities come from the WP-A catalog. target_pk retains the exact
native packed CRR key bytes, not a new Vec<Value> encoding. incarnation is
the host-reported native causal length; no parity rule is inferred by this
crate. Actor IDs are 16 bytes. kind v1 defines
1=PN actor component, 2=set add, 3=set remove, 4=min actor extremum,
5=max actor extremum. Set tags retain the archived database/node/transaction/
operation identity; aggregate component/extrema rows use an empty tag.
Only one payload column carries a versioned, bounded canonical causal/value
encoding, so partially paged multi-column state is not interpreted as complete.
An empty sentinel/default payload cannot materialize a projection.

Use a strict exception for this exact table/DDL in schema_bytes, never a
prefix exemption. Include carrier DDL and the canonical policy-map fingerprint
in peer compatibility. Internal carriers alone do not constitute an opted-in
application CRR. Validate key/payload agreement, policy kind, actor widths,
target key shape, incarnation, numeric validity and budgets. Reserved writes
and user CRR-registration of system names need explicit protection.

## Implemented pure reducer

`carrier::Row` represents the schema fields. `Target` supplies the host's
current raw target identity, incarnation and explicit live flag.
`reduce_causal` clones a selected baseline State, validates all rows and
their aggregate structured input-byte count, then max/union-joins components
using the canonical kernels. `reduce_extremum` validates and compares all
candidate values with compare.rs. Both defer incomplete/noncurrent rows and
return no projection when the host marks the target nonlive. The original
baseline is unchanged even if a later row or final projection fails.

Payload envelope v1 is postcard `(version: u8, payload: enum)`, whose variant
order is Counter{positive,negative}, SetAdd{tag,value}, SetRemove{tag}, Min,
Max. Row kinds are 1..5 independently of serde variant indices 0..4. Tags
must agree with their key; additions must match the tag's originating actor,
while removers may differ from that actor (including removal of synthetic
baseline tags). One add tag cannot name different values in the candidate.
Encoded bytes must round-trip canonically; unsupported versions, malformed
identities, policy mismatch, NaN and overflow fail with typed errors. Tests
pin representative v1 byte vectors before any host rollout.

The byte budget is supplied by existing host PRAGMA settings; the adapter
adds no knob. It counts all input-row strings/bytes plus two i64 fields,
checks before decoding each payload and makes no process-memory-cap claim.
Input rows, decoded values, baseline/candidate state and decoder scratch can
coexist. A4 still owns early packet admission and streaming/storage limits.

## CRSB and projection adapter boundary

WP-A's A4 parser owns native CRR key unpacking, stable IDs, source flags and
classification. Carrier export/import follows the TARGET application's local
opt-in flag. Policy projections are excluded from both wire directions;
all non-LWW input/state, including Min/Max, rides the carriers. Ordinary LWW
columns and row sentinels keep the existing native ingest behavior.

Proposed worker sequence is parse/validate/classify, native ingest of ordinary
cells/row lifecycle and carrier cells, recompute touched policy targets,
guarded projection write, update progress, commit, flush, ACK. Native row
causal length fences deletes/reinserts. Reordered PN components use max,
sets preserve observed-remove tags, extrema use archived exact comparisons.
Never merge received projected scalar totals as counter increments.

A Min/Max input must not disappear merely because native LWW would reject
an older column version. Monotone per-actor extrema and a reviewed pre-discard
reducer/validation hook address that boundary; the exact A4/ingest API is
still required. Projection writes need a reviewed CRR sync-bit/internal path
to avoid feedback. Filtering projected wire cells without carrier inputs for
every policy would lose extrema and is explicitly rejected.

Local operation SQL actions must mutate carriers and projection on the same
caller connection in one explicit transaction; calling Worker to write while
the caller holds SQLite locks can deadlock. Reject unguarded projection
UPDATEs. Initial baseline capture and concurrent-create selection must use
the native CRR winner. Do not infer a baseline from an already merged total.

The concrete native Stamp/control-order representation and initial baseline
selection need lead review before the host can instantiate causal::State for
persisted data. Native CRR value tie-breaking cannot be replaced casually by
an ordering of (col_version, site_id). This crate's generic stamp does not
make that decision or allocate chronology.

## Snapshot, retention and ownership

Snapshots include target-filtered components and observed-remove knowledge
before publishing covered progress. Deleting a user row does not immediately
delete policy carriers. Future/absent-incarnation records remain pending;
older records cannot project into a newer incarnation. No carrier/tag GC is
proposed for v1. Later cleanup requires a covering published snapshot plus
protected-peer/retirement floors and pending High/Low export protection.

The pure high_low module now adapts ownership/trust/receipts/release fences
from the archive, without Shadow/ReplicaState row merge or HLC. Its Owner,
OwnedValue and LowSource hold only control/value/native-provenance fields.
join_owner preserves (owner epoch, cell version) selection; promotion/release
accept host native tokens and reject stale inputs. select_projection and
permits_low_delete choose authority without editing rows. select_scope
preserves the archived control/provenance branch ordering over host tokens.

check_import receives an already authenticated Header, trusted domains and
the stored receipt bytes. It preserves role/domain validation, forwarded
origin receipt identity, digest conflict rejection and replay behavior.
The host reads receipt_key under a serialized native apply and commits a
Fresh receipt atomically with rows/control changes. The kernel never performs
crypto, gap tracking, schema adoption, publishing or persistence. Receipt
keys preserve the archived hash layout; fixed test goldens pin both origin
and bundle forms. New control-carrier schema and the concrete native Stamp
still require lead review. This delivery creates no owner/shadow tables.
WP-C owns bundle/file transport.

All persistent policy, ownership, receipt and provenance state belongs in
SQLite tables in the same database, per coordination constraint 9. Carrier
components use the approved CRR table; receipt keys address rows in the
existing in-database murmur_kv or a separately reviewed native control table.
No sidecar directory/file, second database or direct RocksDB metadata handle
is permitted. Commit receipts/control state atomically with the accepted
native row changes. Only the final air-gap artifact itself is the granted
out-of-database exception, and its transport belongs to WP-C.

## Configuration and validation gates

The approved configuration proposal is PRAGMA murmur_column_policies, JSON
map default `{}` = LWW. Persist/read back through murmur_config; validate known
non-PK targets, permitted policy names and safe mode transitions. No setting
C API. JSON/list policies are deferred because the supplied archive has no
implementation; reject those names rather than rename set semantics.

Kernel and carrier tests cover counter convergence/replay, observed-remove concurrency,
baseline seed replacement, orphan fences, invalid baselines/policy mismatch,
overflow, malformed/overbudget rows, native lifecycle deferral, canonical
encoding, candidate isolation, receipt replay/conflicts, release dominance,
authority/scope selection and exact numeric boundaries. They do not establish CRR
integration. Before marking policy support complete, test native LWW parity,
partitioned/reordered/replayed carriers, old-clock extrema, row deletion and
reinsert, local rollback/reopen, partial batches, table-flag filtering,
projection feedback, snapshot/retention recovery and High/Low release fences.

The coordinator owns shared Rust/sql/Store/CMake/docs edits. WP-A owns A4
classification, native key codec, flags, schema compatibility, snapshots and
retention. This worker owns only approved kernel files and, after review,
the carrier reducer adapter in this new crate.
