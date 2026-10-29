# murmur-replication

Rust SQLite extension for automatic masterless CRR replication over mutual-TLS
QUIC. The client interface is SQLite PRAGMAs and SQL functions. QUIC serves peer
replication only. There is no PostgreSQL wire protocol or client TCP listener.

See [USAGE.md](USAGE.md) for configuration, lifecycle and restrictions, and
[PROVENANCE.md](PROVENANCE.md) for imported Murmur sources.

Build from the sqlite-murmur repository root:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
  -DSQLITE_MURMUR_ENABLE_CRSQLITE=ON -DSQLITE_MURMUR_ENABLE_MURMUR=ON
cmake --build build --parallel 4
ctest --test-dir build --output-on-failure
```

Murmur uses installed stable Rust; cr-sqlite separately uses the existing
nightly-2023-10-05 toolchain and libclang. Cargo builds use vendored crates and
`--frozen`. Build outputs stay in the CMake build directory. Runtime storage is
provided exclusively by the host SQLite and sqlite-murmur VFS.

Applications link only `sqlite_murmur::sqlite_murmur` and call
`sqlite_murmur_open()`: static CRR and Murmur registration is automatic.
`sqlite_murmur_close()` handles CRR cleanup; the final application connection
stops the shared replication service. Networking starts only through SQL.
The compatibility target `sqlite_murmur::murmur_replication` remains available
for low-level callers that initialize extensions manually.

Loadable artifacts require `SQLITE_MURMUR_BUILD_LOADABLE_EXTENSIONS=ON` and are
not needed for single-executable applications. Loadable users load the
companion `murmur_crsqlite` library first on their application connections, then
load `libmurmur_replication` on one control connection. Both forms expose the
same SQL interface. SQL configuration replaces native management APIs and
configuration files. Certificate files are supplied by path through PRAGMAs.
