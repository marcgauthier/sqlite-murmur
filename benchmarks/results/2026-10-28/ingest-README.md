# Incoming replication benchmark — October 28, 2026

This measures the cr-sqlite-2 **destination ingest** path, including the new
`SELECT crsql_ingest_blob(?)` API. The earlier local-tracking results in this
directory remain a different workload.

Intel Core i5-6500, four physical cores, Release build; vendored snapshot
`cr-sqlite-2-43ab94ca-fb39cc559120`. AEGIS-256, Zstd3 before encryption,
100 ms flush, 4 KiB pages, 64 MiB VFS cache, 1 GiB destination SQLite cache,
32 MiB RocksDB cache, four preparation workers and one RocksDB WriteBatch writer.
50,000 destination seed rows; fixed, highly compressible 70-byte payloads.

| Changes/transaction | Apply method | Total ops/sec | Applied rows/sec | Destination RocksDB MiB | Wall seconds |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | row | 104,101 | 20,820 | 41.48 | 186.87 |
| 1 | blob | 71,480 | 14,296 | 28.97 | 151.67 |
| 100 | row | 259,031 | 51,806 | 95.25 | 112.05 |
| 100 | blob | 231,426 | 46,285 | 85.59 | 108.59 |

Blob versus row application at batch 1: **-31.34%** mixed throughput.

Blob versus row application at batch 100: **-10.66%** mixed throughput.

The highest measured mixed throughput was **row application, batch 100**.
Prefer batching when transaction boundaries permit; this one-column schema does
not exercise grouping many changed columns of the same row. These are single
trials on a machine with other CPU workloads active, so small differences do not
establish a stable advantage. File sizes reflect different numbers of inserted
rows; they are not an equal-work compression comparison.

## Timing and workload

Each trial accumulates 60 seconds of **destination-active time**: BEGIN, incoming
application, COMMIT, then four random primary-key destination reads per newly
imported row. A write counts one applied record/body change, not one API call.
Both methods commit once per batch, including batch 1. Reads follow the commit.
No incoming IDs are replayed, so no-op ingestion cannot inflate the rates.

A separate ordinary in-memory SQLite source creates fresh records, exports all
nine columns verbatim from `SELECT * FROM crsql_changes WHERE db_version>?`, and
packs CRSB v1. Both modes perform exactly this source preparation outside the
active timer. Row mode binds the captured SQLite values one change at a time;
blob mode binds the packed batch once and checks the returned applied count.
The source has no encrypted VFS or disk files. There are two SQLite connections,
one to the source and one to the destination. Network transport is not simulated.

Wall time includes source generation, export, packing and cleanup. Raw output
also reports source preparation, packed bytes, applied changes, transaction count
and duration, final persistence, wall throughput including persistence, logical
size and compression savings. Disk measurements cover the destination only.
Background destination flushing continues during excluded source work. These
active-time results must not be compared directly with continuous local-insert
rates or interpreted as durable throughput.

## Validation

All eight CTest checks pass. New coverage checks C++ CRSB bytes against the
vendored pure-Python `pack_changes`, including typed/empty values and embedded
NULs. Blob versus row differential tests check base rows, exported clocks,
database version and rows impacted across insert/update/delete/replay. Malformed
input rejection, empty batches and encrypted reopening are covered. Each full
benchmark checks exported change counts, applied counts in blob mode, reopened
row counts and SQLite integrity. Source inventory verification remains clean.

## Reproduce

With the optional cr-sqlite build enabled:

```bash
./build/benchmark --seconds=60 --ingest=row --batch-size=1
./build/benchmark --seconds=60 --ingest=blob --batch-size=1
./build/benchmark --seconds=60 --ingest=row --batch-size=100
./build/benchmark --seconds=60 --ingest=blob --batch-size=100
```

The original local-insert workload remains `--ingest=local` (default), optionally
with `--replication=on` for local tracking only. Raw incoming-ingestion reports
are the four `ingest-*-batch*.txt` files beside this document.
