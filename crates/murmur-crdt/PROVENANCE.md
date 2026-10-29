# Source provenance

Imported from `/media/marc/2TB/Archives/MURMUR-RUST`, Git commit
`e5a19c25cceaf93375872e3a7f01f05ffc3659ac`. The archive is read-only.

All source modules are copied byte-for-byte from `crdt/src/`;
`policies.rs` comes from `replication/src/policies.rs`. Proposed `lib.rs`
host seams are recorded in INTEGRATION-PROPOSAL.md and await lead approval.

| Destination | Archive SHA-256 |
| --- | --- |
| `src/compare.rs` | `c0f3483ec7f91b83970d1ca7cb23213c87063bc3482060a7b3f675137f3cfad5` |
| `src/digest.rs` | `7fee887d67d0f458e087d4cc10b622878e67126b6f955835bcd1e2dba9be42c7` |
| `src/logical.rs` | `76ae25779b8b1739c89d26bcfea420256db4d536196493c35f66901a22cd40ef` |
| `src/managed.rs` | `ffb04169bb6c41fff126568f596142758e0611c95ebd7fa81244670f3b0eebf0` |
| `src/merge.rs` | `0b03da0d5880dd0f272a853cb6ebb7415263c9927d7927607ce7793c05623112` |
| `src/meta.rs` | `dd4eeeaa37b29651ac0b59607c59e4cdad65fe2f5de002d674d218bc84aca4b8` |
| `src/model.rs` | `464428a3031896e23bc169711bb121dab659c58ac1daa405e02358d984dfb915` |
| `src/schema.rs` | `d9ee0a24d3db967155cecb4e324bbdaac64d778a4db163e4eef628539188133e` |
| `src/schema_codec.rs` | `a559b8522a3cb774cac14d138a1e07e364549115ce4ab30b845c134c47f11856` |
| `src/table_replication.rs` | `80c23253572c8701cd739ec8bd7ec608e50c09a8d3b5dabb4c5206fca3531f02` |
| `src/policies.rs` | `dfe1a78fa4f5a9585fa9cc06e0d0c822213b8cf0caae53c8bb1ce560c3da1693` |

The archived `crdt/Cargo.toml` declares MIT licensing; that declaration is
retained. The copied replication policy file has no separate license header.
This provenance record does not relicense imported material.

Manifest adaptations: exact archive dependency pins, rlib output, omit
Spool (the host-specific logical module will be excluded on approval),
omit bundled SQLite dev features, retain rand for archived batch-merge tests.
No standalone Cargo.lock or in-tree target directory is used. Registry
dependencies and their transitive closure already exist in the shared vendor
tree; parent Cargo.lock and vendor changes are coordinator-owned.
