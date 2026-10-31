# HC-tree vs SQLite-Murmur benchmark: integration status

This branch aims to compare **SQLite-Murmur** against **HC-tree replacing
SQLite inside Murmur**, keeping the same RocksDB-backed Murmur VFS,
compression/encryption and cr-sqlite local change tracking.

## Current blocker (2026-10-31)

The integrated configuration is **not valid yet**. In GitHub Actions
[run 41177147507](https://github.com/marcgauthier/sqlite-murmur/actions/runs/41177147507),
the HC-tree smoke test created a regular POSIX database file, not the
RocksDB directory expected from Murmur. The source audit showed direct
`open()`, `pwrite()`, `ftruncate()` and `mmap()` calls in HC-tree's
`src/hct_file.c`. Supplying `vfs=sqlite-murmur` in the SQLite URI does
**not** intercept these calls. The benchmark correctly rejects this
configuration; no integrated HC-tree throughput result is verified.

The runner now writes `hctree-storage-audit.txt` and fails early with
`integrated-storage-blocker.txt` instead of rebuilding RocksDB for a
known-invalid comparison. An adapter marker
`HCTREE_MURMUR_IO_ADAPTER` in the patched HC-tree `hct_file.c` enables
the integrated build attempt, but does **not** bypass runtime validation.

## Reproducible source and CI behavior

The HC-tree source is pinned to Fossil revision
`e3820e788797940ae4556b35f7c6fd190d61d852c30637b9d3ab583b91223c5a`,
recorded in the SHA-256-verified artifact of
[run 41185447569](https://github.com/marcgauthier/sqlite-murmur/actions/runs/41185447569).
The runner verifies `manifest.uuid` after extraction and rejects a mismatch.
The workflow is still manually dispatchable, but push-triggered retries are
restricted to `benchmarks/hctree/adapter/**` until there is a real adapter.
This avoids spending CI time on a configuration known to fail preflight.

## Work required before performance numbers are valid

1. Implement an HC-tree file-I/O adapter that routes **both** database
   and `-pagemap` persistence through Murmur's storage layer. HC-tree's
   POSIX file descriptors, memory maps, resizing and direct writes cannot
   simply be renamed to SQLite VFS methods; mapped writes, file growth,
   read visibility and flush ordering must be implemented and tested.
2. Preserve HC-tree's expected page-map and database-file consistency
   across close/reopen, transactions, rollback and concurrent writers.
   Prove that no stray POSIX database or page-map file is created.
3. Verify Murmur RocksDB statistics and encrypted/compressed storage are
   nonempty, that the HC-tree file signature is read through the Murmur
   backend, and that reopened row counts and `PRAGMA integrity_check`
   succeed. Add crash/recovery testing before treating the result as a
   production-grade storage option. Upstream HC-tree explicitly warns
   that its prototype does **not** recover from OS crashes/power loss.
4. Only then run batch sizes **1 and 100**, with cr-sqlite tracking
   **off and on**, on both SQLite-Murmur and integrated HC-tree-Murmur.
   Accept a case only when its smoke test, reopen/integrity checks,
   change-count checks, `BENCH_DONE` and result artifact all pass.

## What the current suite measures

The benchmark uses 50,000 seed rows, one connection, 80% primary-key
reads and 20% inserts, a fixed payload and a 1 GiB SQLite cache target.
`replication=on` means **local cr-sqlite change tracking** in both
Murmur variants, not live QUIC replication or HC-tree's native
leader/follower journal. Separate end-to-end replication tests would
be needed to measure network throughput.

Standalone HC-tree measurements are reference numbers only and must
never be reported as integrated HC-tree-Murmur. The benchmark currently
uses different durability policies for SQLite disk and Murmur; compare
their throughput with that caveat.

Upstream HC-tree documentation:
https://sqlite.org/hctree/doc/hctree/doc/hctree/index.html
