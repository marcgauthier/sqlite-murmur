# Source provenance

This is the sqlite-express adaptation of the user-provided Murmur Rust replication
implementation at `/media/marc/2TB/Archives/MURMUR-RUST`, Git commit
`e5a19c25cceaf93375872e3a7f01f05ffc3659ac`.

`src/membership.rs` and `src/transport.rs` were imported from `replication/src/`
and formatted locally. The session orchestration, SWIM probe/relay behavior and
framing design in `src/engine.rs` and `src/protocol.rs` are adapted from that
implementation. The Spool backend, original transaction protocol, signatures,
schema authority, CRDT engine, snapshots, file services and bridge were excluded.
The SQLite worker, CRR batching, metadata, SQL interface and VFS integration are
new sqlite-express code. No PostgreSQL server or wire protocol is included.

The supplied archive has no top-level LICENSE or license field in its replication
Cargo manifest. This records provenance; it does not assign a new license to the
imported material. Retain any applicable author permissions when redistributing.

Cargo dependencies are pinned by Cargo.lock and copied under vendor/ with Cargo
checksums and upstream license files. Configure/build do not download packages.
