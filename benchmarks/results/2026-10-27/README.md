# Local benchmark and profiling evidence — October 27, 2026

Release build on Intel Core i5-6500 (four physical cores). Each raw result is a
fresh 60-second run with one SQLite connection, 80% random reads / 20% inserts,
50,000 seeded rows, and a repeated highly compressible 70-byte payload.
cr-sqlite replication tracking is enabled; no network transfer is timed.

Settings: AEGIS-256, Zstd3, 100 ms flush, 4 KiB SQLite pages, 64 MiB VFS cache,
1 GiB SQLite cache, 32 MiB RocksDB cache. Four compression/encryption workers
feed one ordered RocksDB WriteBatch writer. Seed work and final verification
are outside the timer; final persistence is recorded separately in each result.
Every run validates timed changes through crsql_changes, reopened counts, and
SQLite integrity. File sizes include seed rows and replication metadata and
reflect different row counts across fixed-duration runs.

## Incremental experiments

| Change | Inserts/transaction | Before ops/sec | After ops/sec | Difference |
| --- | ---: | ---: | ---: | ---: |
| VFS sidecar access | 1 | 111,778 | 164,581 | +47.24% |
| VFS sidecar access, fresh bulk pair | 100 | 381,406 | 383,048 | +0.43% |
| RAM-journal reuse | 1 | 165,603 | 177,655 | +7.28% |
| RAM-journal reuse | 100 | 377,640 | 376,384 | -0.33% |
| Direct pending-page reads | 1 | 180,588 | 183,693 | +1.72% |
| Direct pending-page reads | 100 | 362,876 | 372,226 | +2.58% |

These are single trials per configuration. Small differences may reflect normal
run variation; they do not establish a reliable gain or regression. Sidecar access
uses the saved single-insert baseline and a fresh paired bulk check. Its initial
bulk run was 371,127 versus a saved 384,495 baseline; the included fresh pair
checked that apparent regression. Later experiments retain all earlier changes
and use fresh consecutive before/after pairs. Raw files are named for each case.

## Profile before these three changes

Two additional 60-second runs sampled hardware user-space cycles at 199 Hz
through the central 58 seconds, excluding seeding, final persistence and
verification. The separate optimized Rust profiling archive added unwind/debug
metadata; application code and defaults were unchanged. There were 11,938 and
12,430 samples, with none lost. Period weighting was used.

| Sampled cycles | Batch 1 | Batch 100 |
| --- | ---: | ---: |
| Foreground VFS work | 50.88% | 3.67% |
| SQLite and cr-sqlite, excluding VFS | 45.36% | 90.09% |
| All background work | 2.90% | 5.87% |

The remaining cycles are benchmark/runtime work. User-cycle percentages exclude
kernel time. The SQLite thread used 47.66 s user + 10.13 s kernel CPU at batch 1,
and 57.21 s user + 0.67 s kernel CPU at batch 100, in each 58-second window.
A separate short syscall diagnostic found 8,503 filesystem probes each for RAM
journal and unsupported WAL paths; after the sidecar change it found zero.
Those traced runs include setup/verification and are not throughput comparisons.

The profile prioritized direct sidecar access, journal allocation/copying, and
unnecessary pending-page scratch copies. At batch 100, the cr-sqlite insert trigger
accounted for 42.59% inclusive user cycles, including nested SQLite calls. These
inclusive figures overlap other SQLite functions and must not be summed with the
exclusive table. Future bulk optimization should investigate key/clock metadata
operations and measured SQLite mutex costs, preserving replication semantics and
thread safety. The profile does not justify adding RocksDB writer threads.

Full call chains were unavailable for 2.88% / 5.87% of cycle weight, almost all
background samples. Thread totals and sampled leaf functions include them; their
full caller attribution is incomplete. Large raw perf files and executable/build
artifacts are deliberately kept outside Git. Capture and analysis were performed
with perf record/report/script and perf stat.

## Recommendation

Keep the tested encryption/compression/cache/flush defaults. Batch 100 inserts
when transaction boundaries and latency requirements allow it. The current
benchmark measures local SQL operation completion; asynchronous COMMIT can still
precede durable RocksDB persistence. These results do not predict other payloads,
cache-miss-heavy datasets, remote replication, or other hardware.
