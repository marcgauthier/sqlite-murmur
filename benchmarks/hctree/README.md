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

## Experimental persistent-file VFS bridge (2026-10-31)

A first C bridge in benchmarks/hctree/adapter now implements open, read,
write, truncate, size, sync, explicit commit and close against a named
SQLite VFS. Both the main database and -pagemap are opened as independent
persistent main-file stores. Murmur xSync is deferred, so the bridge uses
the commit-phase-two and flush controls to persist completed writes.

The CTest suite now tests Unix-VFS reopen/truncation and actual Murmur
RocksDB persistence/reopen and nonempty persisted-block statistics. This
is only the file I/O foundation: it does NOT intercept HC-tree POSIX calls,
provide mmap semantics or atomic commit across the two stores. Until
those missing features are implemented, integrated HC-tree measurements
remain invalid. The expensive benchmark workflow triggers only when
adapter/READY changes, not on intermediate bridge work.

## Explicit dirty-page window experiment (2026-10-31)

`hct_murmur_vfs_window.[ch]` provides a **copy-backed** page window
loaded through the Murmur VFS. Callers explicitly mark modified ranges,
flush only affected 4 KiB pages, and commit the underlying store. A
discard operation drops unflushed edits. CI exercises persistence of a
dirty window after a cold RocksDB restart, zero-fill beyond EOF, and
discard without modifying the page-map.

**This is not `mmap(MAP_SHARED)`.** HC-tree still needs an audited rewrite
of every mapped-page access, write notification, visibility/locking
semantics, and crash-consistent main/page-map commit. Do not enable the
integrated benchmark or report HC-tree throughput on the strength of
this experimental window alone.

## Exact dirty-byte window ranges (2026-10-31)

The copy-backed window now records sparse, coalesced **byte ranges**, not
whole dirty pages. Two independent windows loaded from the same snapshot can
flush changes to different bytes in the same 4 KiB page without overwriting
one another's unrelated modifications. The RocksDB bridge integration test
checks those writes after a cold restart, including two distinct pages and
read-only dirty-mark rejection. A small standalone ASan/UBSan test also
exercised the range-merging implementation. This does **not** establish
MAP_SHARED coherence, simultaneous writes to the same bytes, transactional
rollback, or atomic commits across the main and page-map stores. HC-tree
still is not wired to the bridge and benchmark results remain blocked.

## Sequential stale-window conflict check (2026-10-31)

Writable copy-backed windows now retain a snapshot of their original bytes.
Before writing any dirty ranges, a flush rereads those ranges and returns
`SQLITE_BUSY_SNAPSHOT` if another completed flush modified the same bytes.
The bridge test exercises a same-byte conflict and reusing a window after its
own successful flush, with cold RocksDB reopen verification. This is **not**
an atomic compare-and-swap or a locking protocol: simultaneous flushes can
race between verification and write. Nor are main and page-map commits atomic.
HC-tree itself is not yet wired to this bridge, so no integrated benchmark
throughput is valid.
