# SQLite Express benchmark results

These measurements are **SQLite Express only**, with no comparisons to other database engines.

## Sustained benchmark — October 27, 2026

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
\`PRAGMA integrity_check\` passed.

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

## Reproduce

Build with CMake, RocksDB, SQLite3, Zstd, and OpenSSL installed:

\`\`\`bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
  -DSQLITE_EXPRESS_BUILD_TESTS=OFF \
  -DSQLITE_EXPRESS_BUILD_BENCHMARKS=ON
cmake --build build --parallel
./build/sqlite_express_bench --scale=10
\`\`\`

You can also run the **SQLite Express benchmark (single database)** workflow
using GitHub Actions' **Run workflow** control.

## Interpretation

The benchmark demonstrates that with the current SQLite DELETE-journal VFS,
one connection generally delivers the highest throughput. Batched INSERTs
are much faster than autocommit INSERTs. At 16 connections, the shared
single-writer locking and rollback-journal contention substantially decrease
mixed workload throughput. The 16-connection tests also oversubscribe the
four available runner CPUs.

Further benchmarking should include percentiles and repeated trials, cache-
miss-heavy query sets, worker scaling on dedicated hardware, and crash/power-
loss fault injection.
