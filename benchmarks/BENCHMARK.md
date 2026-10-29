# SQLite Murmur benchmark results

The default benchmark compares encrypted, compressed sqlite-murmur against plain
SQLite on disk and in memory, with and without local cr-sqlite change tracking.
The older measurements below are retained as history; they are Murmur-only and
must not be presented as measurements of the new comparison matrix.

## Comparison matrix and methodology

The `benchmark` executable runs both groups by default:

| Group | Replication | Backend | Inserts/transaction |
| --- | --- | --- | --- |
| Test 1 | Disabled | sqlite-murmur | 1 and 100 |
| Test 1 | Disabled | Plain SQLite disk, 1 GiB page cache | 1 and 100 |
| Test 1 | Disabled | Plain SQLite `:memory:` | 1 and 100 |
| Test 2 | Local cr-sqlite tracking | sqlite-murmur | 1 and 100 |
| Test 2 | Local cr-sqlite tracking | Plain SQLite disk, 1 GiB page cache | 1 and 100 |
| Test 2 | Local cr-sqlite tracking | Plain SQLite `:memory:` | 1 and 100 |

All 12 cases use the same bundled SQLite library, schema, prepared statements,
50,000 seeded rows, deterministic random ID generator, and fixed highly
compressible text payload. Each case performs 80% primary-key reads / 20% inserts
through one connection for 60 measured seconds by default. One insert and four
reads form a group. At 1x the insert autocommits; at 100x a transaction wraps 100
inserts and 400 interleaved reads. The last full transaction completes before
stopping. Setup, seed persistence, and verification are outside the timer.
Test 2 registers the table as a CRR before seeding and measures local tracking;
it excludes network transport and incoming application.

Murmur always enables encryption (AEGIS-256 by default, AES-256-GCM optional) and
compression (Zstd3 by default). `--compression=none` is rejected, including for
single incoming-ingestion runs. Plain SQLite opens through its native VFS and
never enables encryption or compression. Every backend uses a 1 GiB SQLite page
cache target in the comparison suites, which grows on demand; Murmur additionally
uses its 64 MiB VFS cache target and 32 MiB RocksDB cache. SQLite pages default to
4 KiB. Vendored SQLite and cr-sqlite sources live under `third-party/`.

| Backend | Journal | Synchronous | Persistence |
| --- | --- | --- | --- |
| Murmur | DELETE | FULL | 100 ms background interval by default; final Murmur checkpoint |
| Plain disk SQLite | WAL | NORMAL | Automatic WAL checkpoints plus final cache flush and WAL truncate checkpoint |
| Plain memory SQLite | MEMORY | OFF | Volatile; no persistence |

These policies have different commit durability guarantees. Murmur acknowledges
commits before background persistence despite its SQLite FULL setting. WAL/NORMAL
does not synchronize each commit. Memory throughput has no disk durability cost.
The report gives both active throughput and throughput including final persistence;
for memory, persistence time and disk bytes are zero.

Each case emits `BENCH_CONFIG`, `BENCH_HEADER`, `BENCH_CSV`, and `BENCH_DONE`.
CSV rows identify backend, group, replication, and batch size. Each group prints
throughput, persistence-inclusive throughput, mean transaction latency, logical
bytes, disk bytes, and Murmur/SQLite ratios for matching batch sizes. A ratio above
1 means Murmur was faster in that trial. `BENCH_SUITE_DONE` confirms all requested
cases completed. Unsupported Murmur-specific statistics are `NA` for plain SQLite.
Disk sizes include regular storage files after close, not cumulative write traffic.
At 1x transaction latency times just the insert; at 100x it includes interleaved
reads. Different cases insert different numbers of rows; compare bytes per row
alongside file sizes. Compression savings include seed data.

Disk databases are reopened for row-count, integrity, and replication-change
checks; memory databases are checked before closing. No new 60-second comparison
measurements are recorded here yet. Use the commands below or the CI artifact to
obtain a complete report; short smoke runs verify behavior, not performance.

## Historical Murmur-only mixed results — October 28, 2026

Latest incoming-replication results (October 28, 2026), using 60 seconds of
**destination-active time**, with source generation/export/packing excluded:

| Changes/transaction | Row-by-row ops/sec | CRSB blob ops/sec | Blob change |
| ---: | ---: | ---: | ---: |
| 1 | 104,101 | 71,480 | -31.34% |
| 100 | 259,031 | 231,426 | -10.66% |

The destination does four reads per applied row; both modes commit once per
batch. Blob mode calls `SELECT crsql_ingest_blob(?)` with all nine exported fields
packed as CRSB v1. For this single-column C++ workload, prepared row application
with batches of 100 was fastest. These single trials do not test multi-column
row grouping or Python call overhead. Source preparation increases wall time,
and destination flushing continues during excluded work; do not compare these
active-time rates directly with local-insert rates below.

[Incoming-ingestion methodology, disk sizes and raw reports](results/2026-10-28/ingest-README.md).

Latest local results (October 28, 2026) with replication tracking enabled and
the vendored cr-sqlite-2 snapshot:

| Inserts/transaction | Operations/sec | Inserts/sec | RocksDB files MiB |
| ---: | ---: | ---: | ---: |
| 1 | 178,531 | 35,706 | 69.96 |
| 100 | 372,677 | 74,535 | 135.25 |

Intel Core i5-6500, four physical cores, Release build. AEGIS-256, Zstd3,
100 ms flush, 4 KiB pages, 64 MiB VFS cache, 1 GiB SQLite cache, 32 MiB RocksDB
cache. Four compression/encryption workers feed one ordered RocksDB writer.
Replication means local change tracking; network transport is excluded.

These are individual trials with highly compressible repeated 70-byte payloads.
Setup/seed data and final verification are excluded; final persistence is reported
separately. Every run checks exported change counts, reopening and integrity.
Disk sizes contain different row counts. Prefer batches of 100 where transaction
boundaries permit; small differences between trials may be normal variation.

[Replacement-library comparison and raw results](results/2026-10-28/README.md)
include the original v0.16.3 baseline. Other CPU workloads were active during these
single paired trials; timings are provisional.

[Raw before/after results and profiling summary](results/2026-10-27/README.md)
document the three VFS optimizations and their measurement limits.

## Historical CI benchmark — October 27, 2026

Run: https://github.com/marcgauthier/sqlite-express/actions/runs/40564652887

| Operation (ops/s) | 1 connection | 4 connections | 16 connections |
|---|---:|---:|---:|
| INSERT, one SQL transaction per row | 36,855 | 29,115 | 13,645 |
| INSERT, 100 rows per SQL transaction | 651,870 | 466,668 | 153,897 |
| SELECT by primary key | 81,482 | 43,277 | 43,678 |
| Mixed: 80% SELECT / 20% INSERT | 77,698 | 31,043 | 8,724 |

**Environment:** GitHub Actions Ubuntu 24.04 runner, AMD EPYC 7763, 4 virtual CPUs,
15 GiB RAM. One RocksDB-backed SQLite Murmur database shared across all
connections. The final database contained **512,000 rows**; reopen, count and
`PRAGMA integrity_check` passed.

**Configuration:** 4,096-byte VFS blocks, encrypted 16,384-page cache target,
250 ms background flush interval, Zstd level 3, AES-256-GCM, RocksDB sync
enabled, SQLite rollback-journal DELETE mode, 70-byte text payload.

**Workload sizes at scale 10** (same total operations for each connection count):

- 30,000 individual autocommit INSERTs;
- 100,000 INSERTs grouped in transactions of 100;
- 300,000 random SELECTs by primary key from 50,000 preloaded rows;
- 120,000 mixed operations, exactly 80% primary-key SELECT and 20% single-row
  INSERT, all on one database.

Each worker uses its own SQLite connection and prepared statements. The timer
starts after worker setup. Throughput counts *successful* SQL operations;
a failed or incomplete workload aborts the benchmark. The final RocksDB flush,
verification query, and integrity check are **excluded** from timed throughput.

SQLite Murmur deliberately acknowledges writes before the periodic background
storage flush. These are **SQL operation completion rates**, not durable
write-through rates. Results on shared CI hosts vary, and these measurements
are not a replacement for repeated long-duration benchmarks on dedicated
hardware.

## Reproduce the current benchmark

See [build prerequisites](../README.md#dependencies-and-build) and
[optional cr-sqlite setup](../README.md#cr-sqlite-tracking-and-ingestion-benchmarks). Once the
pinned Rust toolchain and libclang compiler tooling are installed, builds use
vendored sources offline:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DSQLITE_MURMUR_ENABLE_CRSQLITE=ON
cmake --build build --parallel 2
ctest --test-dir build --output-on-failure
./build/benchmark                         # Both groups: 12 x 60 seconds, plus setup/checks
./build/benchmark --suite=no-replication   # Test 1: six cases
./build/benchmark --suite=crsqlite         # Test 2: six cases
./build/benchmark --seconds=0.01           # Quick verification of the full matrix
# One custom case (batch/backend/replication/ingestion overrides require single):
./build/benchmark --suite=single --backend=sqlite-disk --replication=on --batch-size=100
./build/benchmark --suite=single --backend=murmur --replication=on --batch-size=100
# Incoming replication; --seconds counts destination active time:
./build/benchmark --suite=single --ingest=blob --batch-size=100
./build/benchmark --suite=single --ingest=row --batch-size=100
```

Single mode defaults to Murmur, one insert per transaction, and replication off.
Comparison suites require the 1 GiB SQLite page-cache target; use single mode for
cache overrides. AES-only builds must supply `--cipher=aes256gcm` for Murmur cases.
Builds without cr-sqlite can run `--suite=no-replication`; the default full suite
and `--suite=crsqlite` fail clearly. See the [full option list](../README.md#benchmark).

Use `--suite=no-replication` for comparisons without CRR tracking. The historical CI
matrix above used the previous `sqlite_murmur_bench` executable and different
settings; its measurements are retained as history and are not directly comparable
to the current local mixed workload. The current GitHub Actions workflow runs the
full comparison matrix, validates all 12 cases, and uploads the raw report.

## Interpretation of historical results

The historical CI benchmark demonstrated that with its SQLite DELETE-journal VFS,
one connection generally delivers the highest throughput. Batched INSERTs
are much faster than autocommit INSERTs. At 16 connections, the shared
single-writer locking and rollback-journal contention substantially decrease
mixed workload throughput. The 16-connection tests also oversubscribe the
four available runner CPUs.

Further benchmarking should include percentiles and repeated trials, cache-
miss-heavy query sets, worker scaling on dedicated hardware, and crash/power-
loss fault injection.
