# SQLite Express benchmark results

These measurements are **SQLite Express only**, with no comparisons to other database engines.

## Current 60-second mixed benchmark

The current executable is `benchmark`: one connection, 80% random primary-key
reads / 20% inserts, configurable inserts per transaction. All library sources,
including optional cr-sqlite, are vendored under `third-party/`.

Latest local results with replication tracking enabled:

| Inserts/transaction | Operations/sec | Inserts/sec | RocksDB files MiB |
| ---: | ---: | ---: | ---: |
| 1 | 183,693 | 36,739 | 71.88 |
| 100 | 372,226 | 74,445 | 135.09 |

Intel Core i5-6500, four physical cores, Release build. AEGIS-256, Zstd3,
100 ms flush, 4 KiB pages, 64 MiB VFS cache, 1 GiB SQLite cache, 32 MiB RocksDB
cache. Four compression/encryption workers feed one ordered RocksDB writer.
Replication means local change tracking; network transport is excluded.

These are individual trials with highly compressible repeated 70-byte payloads.
Setup/seed data and final verification are excluded; final persistence is reported
separately. Every run checks exported change counts, reopening and integrity.
Disk sizes contain different row counts. Prefer batches of 100 where transaction
boundaries permit; small differences between trials may be normal variation.

[Raw before/after results and profiling summary](benchmarks/results/2026-10-27/README.md)
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
15 GiB RAM. One RocksDB-backed SQLite Express database shared across all
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

SQLite Express deliberately acknowledges writes before the periodic background
storage flush. These are **SQL operation completion rates**, not durable
write-through rates. Results on shared CI hosts vary, and these measurements
are not a replacement for repeated long-duration benchmarks on dedicated
hardware.

## Reproduce the current benchmark

See [build prerequisites](README.md#dependencies-and-build) and
[optional cr-sqlite setup](README.md#optional-cr-sqlite-replication). Once the
pinned Rust toolchain and libclang compiler tooling are installed, builds use
vendored sources offline:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DSQLITE_EXPRESS_ENABLE_CRSQLITE=ON
cmake --build build --parallel 2
ctest --test-dir build --output-on-failure
./build/benchmark --replication=on --batch-size=1
./build/benchmark --replication=on --batch-size=100
```

Use `--replication=off` for a baseline without CRR tracking. The historical CI
matrix above used the previous `sqlite_express_bench` executable and different
settings; its measurements are retained as history and are not directly comparable
to the current local mixed workload. The current GitHub Actions workflow runs the
default single-connection benchmark.

## Interpretation

The historical CI benchmark demonstrated that with its SQLite DELETE-journal VFS,
one connection generally delivers the highest throughput. Batched INSERTs
are much faster than autocommit INSERTs. At 16 connections, the shared
single-writer locking and rollback-journal contention substantially decrease
mixed workload throughput. The 16-connection tests also oversubscribe the
four available runner CPUs.

Further benchmarking should include percentiles and repeated trials, cache-
miss-heavy query sets, worker scaling on dedicated hardware, and crash/power-
loss fault injection.
