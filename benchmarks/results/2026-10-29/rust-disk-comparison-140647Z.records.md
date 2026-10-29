# Rust storage comparison — October 29, 2026

Eight disk-backed cases from the full benchmark of the migrated Rust storage implementation against plain SQLite,
with a comparison to the earlier C/C++ numbers supplied by the user. The benchmark
driver remains C++; the linked storage and open/close implementations are Rust.

## Run and configuration

```bash
cmake --build build-a1 --target benchmark --parallel 2
./build-a1/benchmark --suite=all --seconds=60
```

The run began with the former 12-case matrix. At the user’s request, the memory
cases were removed while it was running. After all eight disk-backed cases completed,
we stopped before the remaining memory cases and extracted the eight unchanged
configuration/header/CSV/completion records into the linked records file. This file
is an extraction, not a newly completed eight-case stdout log. The driver now defaults
to eight cases; the disk measurement loops and settings are unchanged.

Intel Core i5-6500 (four physical cores), Release build, 60 measured seconds per case,
50,000 seeded rows, 80% reads / 20% inserts, 4 KiB SQLite pages, 1 GiB SQLite page-cache
target. Murmur uses AEGIS-256, Zstd3, a 100 ms flush interval, 64 MiB VFS cache,
32 MiB RocksDB cache, and four background workers. Plain SQLite has no encryption
or compression. Disk SQLite uses WAL/NORMAL.
Test 2 measures local cr-sqlite tracking, without network transport.

[Extracted measurement records](rust-disk-comparison-140647Z.records.txt) · [CSV](rust-disk-comparison-140647Z.records.csv) · [Build/run metadata](rust-disk-comparison-140647Z.records.json) · [User-provided C/C++ baseline](rust-disk-comparison-140647Z.records.baseline.json)

All eight retained records passed report validation; each case also performed row-count, integrity,
and (where enabled) replication-count verification. Disk verification reopens the database.

## Measured results

### Test 1: no replication

| Backend | Inserts/tx | Ops/sec | Ops/sec incl. persistence | Mean tx ms | Logical MB | Disk MB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| murmur | 1 | 402,228 | 400,267 | 0.0051 | 392.7 | 42.9 |
| sqlite-disk | 1 | 100,892 | 100,891 | 0.0319 | 101.7 | 101.7 |
| murmur | 100 | 899,942 | 898,078 | 0.5555 | 873.7 | 95.3 |
| sqlite-disk | 100 | 656,012 | 655,950 | 0.7621 | 638.0 | 638.0 |

| Inserts/tx | Murmur / disk | Murmur / disk incl. persistence |
| ---: | ---: | ---: |
| 1 | 3.99× | 3.97× |
| 100 | 1.37× | 1.37× |

### Test 2: cr-sqlite local change tracking

| Backend | Inserts/tx | Ops/sec | Ops/sec incl. persistence | Mean tx ms | Logical MB | Disk MB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| murmur | 1 | 191,243 | 190,919 | 0.0179 | 348.7 | 78.3 |
| sqlite-disk | 1 | 29,204 | 29,133 | 0.1520 | 59.2 | 59.2 |
| murmur | 100 | 363,866 | 360,023 | 1.3740 | 656.3 | 138.4 |
| sqlite-disk | 100 | 250,090 | 250,071 | 1.9992 | 450.9 | 450.9 |

| Inserts/tx | Murmur / disk | Murmur / disk incl. persistence |
| ---: | ---: | ---: |
| 1 | 6.55× | 6.55× |
| 100 | 1.45× | 1.44× |

## Compared with the earlier C/C++ tables

| Replication | Backend | Inserts/tx | Previous C/C++ ops/sec | Current ops/sec | Change | Change incl. persistence |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| off | murmur | 1 | 351,934 | 402,228 | +14.29% | +14.02% |
| off | sqlite-disk | 1 | 114,294 | 100,892 | -11.73% | -11.71% |
| off | murmur | 100 | 855,545 | 899,942 | +5.19% | +5.19% |
| off | sqlite-disk | 100 | 669,723 | 656,012 | -2.05% | -2.06% |
| on | murmur | 1 | 195,672 | 191,243 | -2.26% | -2.27% |
| on | sqlite-disk | 1 | 29,397 | 29,204 | -0.65% | -0.90% |
| on | murmur | 100 | 424,931 | 363,866 | -14.37% | -14.21% |
| on | sqlite-disk | 100 | 276,462 | 250,090 | -9.54% | -9.54% |

These are single trials, not confidence intervals or a paired C++/Rust rerun.
The previous values come from the conversation; their raw configuration and machine-load
metadata were not supplied. Changes in plain SQLite baselines help show run-to-run variation;
deltas cannot be attributed solely to the migration. The independently recorded paired
[storage-port results](README.md) cover replication-on Murmur cases only.

MB uses decimal units. Fixed-duration cases insert different numbers of rows, so total
database sizes are not equal-work storage comparisons. At 1x, transaction latency times
only the insert; at 100x it includes interleaved reads. Murmur background persistence
and SQLite WAL/NORMAL have different durability guarantees.
