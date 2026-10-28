# Vendored SQLite

`sqlite-amalgamation-3450100/` holds the C sources for the **static**
extension variant (`cargo build --features static-sqlite`), which links
its own session-enabled SQLite instead of the host's `libsqlite3`.

- Upstream: https://www.sqlite.org/2024/sqlite-amalgamation-3450100.zip
- Zip SHA256:
  `5592243caf28b2cdef41e6ab58d25d653dfc53deded8450eb66072c929f030c4`
- `sqlite3.c` SHA256:
  `1a206854aa9fe0ccc1b609f5cfce67eb52ac0a8f5aa2f5e853f7c3ed84c710ab`
- `sqlite3.h` SHA256:
  `41e066ccd4f89e938f136ceb48c996c54dc381b59fe566dea48864c3750b779e`
- `SQLITE_VERSION_NUMBER`: 3045001 (verified in `sqlite3.h`)
- License: public domain (see the header comment in `sqlite3.h`)
- Build flags (see `crates/murmur-sqlite-ext/build.rs`):
  `-DSQLITE_ENABLE_SESSION -DSQLITE_ENABLE_PREUPDATE_HOOK`,
  `-fvisibility=hidden`, `-O2`

Only `sqlite3.c` and `sqlite3.h` are kept (`shell.c`/`sqlite3ext.h` from
the zip are not needed to build the library).

## Why 3.45.1

The static build's session code operates on host-owned connection
handles, so the vendored SQLite must match the host's SQLite version.
3.45.1 matches this repo's test host (stock Ubuntu 3.45.1), which keeps
the pairing verifiable here. For any other host version: download that
version's amalgamation into a sibling directory, set
`SQLITE_AMALGAMATION_VERSION` in `dist.sh` (it exports
`MURMUR_SQLITE_AMALGAMATION` for `build.rs`; default `3450100`),
rebuild, and re-run the static smoke test on that host.
