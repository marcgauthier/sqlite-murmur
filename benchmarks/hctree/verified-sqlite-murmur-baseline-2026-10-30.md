# Verified SQLite-Murmur baseline (2026-10-30)

This report verifies **only the standard SQLite-Murmur side** of the proposed
HC-tree-vs-SQLite-Murmur comparison. It is **not** a completed integrated
HC-tree benchmark and must not be used to infer HC-tree-Murmur throughput.

## Provenance

- GitHub Actions: https://github.com/marcgauthier/sqlite-murmur/actions/runs/41143094949
- Original workflow artifact: https://github.com/marcgauthier/sqlite-murmur/actions/runs/41143094949/artifacts/13020562903
- Artifact name: `hctree-comparison-results`
- Artifact ZIP SHA-256 (matches GitHub Actions artifact digest):
  `c1691f71d9dec38686d9f663b435b3cc4108f75da23297fb2da819c3a2bc9f49`
- Source commit recorded in `revisions.txt`:
  `22c8acb541f2b79518415d4cdc1f0fa0c40eacc9`
- The four standard Murmur results below were recorded on 2026-10-30.
  They have **not** been rerun on the current branch tip.

## Verified standard SQLite-Murmur results

| cr-sqlite tracking | Inserts/transaction | Operations/second | Writes/second | Writes | Tracked changes |
|---|---:|---:|---:|---:|---:|
| Off | 1 | 446,987.71 | 89,397.54 | 5,363,853 | 0 |
| Off | 100 | 835,317.81 | 167,063.56 | 10,023,900 | 0 |
| On | 1 | 258,083.73 | 51,616.75 | 3,097,005 | 3,097,005 |
| On | 100 | 437,257.21 | 87,451.44 | 5,247,100 | 5,247,100 |

Each operation count is `reads + writes = 5 * writes`; each transaction
contains one or 100 inserts and four reads per insert. These figures are
60-second active-workload throughput, not network replication throughput.

## Artifact verification performed

The ZIP SHA-256 was checked against the digest returned by GitHub Actions.
Each of the four `murmur-{off,on}-{1,100}.txt` files was independently
parsed and checked for exactly one `BENCH_CSV` record, matching
`BENCH_HEADER`, `BENCH_DONE`, `BENCH_SUITE_DONE`, no `BENCH_ERROR`,
60+ seconds of measurement, `backend=murmur`, the requested batch and
tracking mode, and internally consistent reads, writes, operations,
transactions and operations/second. Tracked changes equal timed writes in
the ON cases and zero in the OFF cases. The source implementation at the
recorded commit also checks persisted reopen row counts, cr-sqlite changes
and `PRAGMA integrity_check` before emitting `BENCH_DONE`.

Individual text-file SHA-256 digests:

- `murmur-off-1.txt`: `27ea6225d1a397396d990d119d3aa034e397d25e54c91466b93713305ec74399`
- `murmur-off-100.txt`: `8b5c097470db97fbfd5ee6f2088bea3d8d482894c032dcb7569f2e92640c86cb`
- `murmur-on-1.txt`: `76261778c110b52aa48d8ad0f9993241b2f87c797d84c94e7b60adb5b348de68`
- `murmur-on-100.txt`: `a3462dd83c23ef5995bbb2bd2bad00a290621ce4c9b74596ec154a3bbc6ceade`

## Important exclusions

The same older workflow artifact also contains files labeled
`integrated-hctree-*.txt`. Those **are not valid integrated HC-tree-Murmur
results**. Later source auditing demonstrated that HC-tree uses direct
POSIX file I/O and mmap rather than Murmur's RocksDB VFS. The current
workflow correctly blocks this integration. Do not compare those mislabeled
numbers with this baseline.

The tracking ON mode initializes local cr-sqlite change tracking; it does
not establish a QUIC peer or measure end-to-end Murmur replication.
The workload uses one connection, 50,000 seed rows, 80% reads/20% writes,
a 1 GiB SQLite cache target, AEGIS-256 and zstd3 on Murmur, and a
100 ms background flush interval. Commit durability must be assessed
separately before comparing to other engines.

## Remaining acceptance gate

No integrated HC-tree-Murmur number is publishable until both the main
database and `-pagemap` use the RocksDB-backed storage path, the
encrypted/compressed store is populated, and the integrated configuration
passes reopen, integrity, change-tracking and persistence checks for all
four combinations of tracking mode and batch size.
