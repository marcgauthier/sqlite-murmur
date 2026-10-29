# Source provenance

Source archive: `/media/marc/2TB/Archives/MURMUR-RUST`, Git commit
`e5a19c25cceaf93375872e3a7f01f05ffc3659ac`. The archive remains read-only.

- `src/compare.rs` is byte-identical to `crdt/src/compare.rs`.
- `src/model.rs` copies only the five-variant Value enum and the encode/decode
  helper bodies from `crdt/src/model.rs`. Its imports are narrowed to this
  crate. It contains no Version, clock allocator, schema or row state.
- `src/causal.rs` extracts `projection`, `join_state`, and `rebase_orphan`
  from `src/policy.rs`, preserving their decision branches and arithmetic.
  Functions become public and generic over a host-supplied ordered Clone
  stamp. State fields representing archived Version become generic stamps;
  generation/scope become native CRR causal-length integers. Imports and
  formatting adapt to this crate; borrowed comparisons and explicit stamp
  clones permit host tokens containing native tie-break data without
  inventing a fixed clock representation. This State is not presented
  as archive-compatible persisted or wire data.
  The archived projection's identity `map_err(Error::from)` is omitted
  because this crate's decoder already returns the same Error type.
- `src/lib.rs` provides a minimal archived database Error subset
  (Invalid/Corrupt/Limit/SchemaMismatch with original messages) and re-exports canonical
  `murmur_tx::policies`. Counter/Set/Causal are not copied again.
- `tests/kernels.rs` supplies downstream semantic regressions. The original
  policy.rs hosts SQL/Core/Spool integration and has no inline unit tests;
  those host integrations are not imported or claimed as tested here.
- `src/carrier.rs` and `tests/carriers.rs` are downstream adapter glue:
  approved schema constant, canonical payload/identity/budget validation and
  pure reduction using the archived Counter/Set/compare/projection kernels.
  No conflict algorithm is replaced. Payload-v1 encoding has explicit test
  vectors. Native PK bytes remain opaque; the host supplies lifecycle without
  parity assumptions. There are no SQLite calls or tag/clock generators.
- `src/high_low.rs` adapts only the control branches from archived
  `src/high_low.rs`: owner comparison, promotion/release, authority selection,
  policy-scope selection, trusted-domain/role checks and receipt identity/
  duplicate/conflict checks. Owner keys become opaque native CRR PK bytes;
  cell metadata narrows to Value plus a host-supplied native stamp. LowSource
  holds only native provenance, not a Shadow or row. Clock allocation, schema
  mapping, row merge, database/spool calls and publishing are excluded.
  Existing invariant checks reject malformed targets and stale host events.
  Receipt IDs preserve the archive's SHA-256-then-hex layout by reusing
  murmur_tx's existing SHA-256 helper and hex function. No new hash is copied.
  `tests/high_low.rs` pins receipt-key goldens and ownership/trust semantics.
- `src/ownership.rs` is downstream encoding and native metadata adapter glue
  for the accepted append-only ownership design. It adds versioned event
  envelopes, prior-event references, target keys and same-target native
  stamp constructors/comparison. Existing digest/codec helpers are reused;
  native ordering follows inspected CRR metadata rules, never an imported
  HLC. There is no SQL or automatic metadata read. Draft API/schema approval
  status and exact signatures are recorded in OWNERSHIP-API.md; accompanying
  tests in tests/ownership.rs cover pure serialization/validation/ordering.

Not imported: row merge, row metadata, logical/physical Spool storage,
replication schema codecs, schema history, HLC generation, duplicate hashes,
or a new transport. Existing cr-sqlite owns native replication lifecycle.
High/Low control-carrier storage and native ingest/projection wiring remain lead-owned
behind WP-A A4; the pure carrier schema shape/reducer grant is approved.

Manifest pins come from the archive Cargo.lock: serde 1.0.229,
thiserror 2.0.21, postcard 1.1.3. murmur-tx is the existing path dependency.
The full dependency closure already exists in the shared offline vendor
tree. Workspace integration in crates/Cargo.toml and its Cargo.lock
belongs to the coordinator; no standalone Cargo.lock or in-tree build output
is added. The ownership slice introduces no new dependencies.

The archive's crdt manifest declares MIT licensing. The archived root and
replication manifests do not declare a license, and policy.rs has no license
header. This record does not assign a new license to imported material.
