# cr-sqlite-2 replacement benchmark — October 28, 2026

Updated source: **cr-sqlite-2-43ab94ca-fb39cc559120**.

Copied the working tree from `../cr-sqlite-2`, based on upstream commit `43ab94cac537d080aac7cb7628a435c02ee9e268`, including its uncommitted tracked and untracked source changes. The source fingerprint is `fb39cc55912043121d64d663a4622fe1fc3eba40f84ff66911682bb3b2e92596`. The pinned sqlite-rs-embedded submodule and locked Cargo packages are unchanged. Source-control metadata, local .env files and ignored build outputs are excluded. The installed source is an ordinary vendored tree, not a live dependency on the sibling directory.

[New incoming blob/row ingestion results](ingest-README.md) measure the new API separately.

## Same workload, fresh before/after pairs

Intel Core i5-6500, four physical cores, Release build. One connection, 60 seconds per run, 80% random primary-key reads / 20% inserts. Local replication tracking enabled. 50,000 seed rows and repeated highly compressible 70-byte payloads. Settings remain AEGIS-256, Zstd3, 100 ms flush, 4 KiB pages, 64 MiB VFS cache, 1 GiB SQLite cache and 32 MiB RocksDB cache. Four compression/encryption workers feed one ordered RocksDB WriteBatch writer. The three prior VFS optimizations are retained in both builds.

The before executable contains upstream cr-sqlite v0.16.3; the after executable contains the identified cr-sqlite-2 snapshot. Benchmark output now reports its compiled source identity rather than a hardcoded release string. New staged/blob remote-ingestion APIs are not used in the timed local-insert workload.

| Inserts/transaction | Original ops/sec | cr-sqlite-2 ops/sec | Change | Original inserts/sec | Updated inserts/sec | Original disk MiB | Updated disk MiB |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 181,885 | 178,531 | -1.84% | 36,377 | 35,706 | 71.21 | 69.96 |
| 100 | 322,673 | 372,677 | +15.50% | 64,535 | 74,535 | 117.65 | 135.25 |

These are single paired trials on a machine with other CPU workloads active, not confidence intervals or isolated hardware measurements. The -1.84% single-insert difference is small and does not establish a regression. The bulk run measured +15.50%; repeat on idle hardware before treating it as a stable speedup. Do not compare these fresh runs directly with yesterday’s absolute rates. File sizes reflect different inserted row counts, not an equal-work storage comparison.

Each run verified every timed insert through crsql_changes, reopened counts and SQLite integrity. Seeding, final export/count verification and reopening are excluded from throughput timing. Final persistence is reported separately in the raw results. No network transport or remote merge is timed.

## Validation

All seven CTest checks pass with replication enabled, including AES/AEGIS VFS integration, concurrency, storage race/failure checks, benchmark defaults/reporting, source integrity and the expanded replication integration test. The latter covers insert/update/delete/replay, staged bulk ingestion and duplicate replay, rollback of rows/clocks/version, repeated equal-value updates, cross-connection version invalidation, and reopening. The compiled crsql_sha() is checked against the vendored snapshot identity.

A separate old → new → old → new persisted-database check passed: original-library creation, updated-library read/update/rollback, original-library read/write, and updated-library final verification, with integrity checks. Replication-disabled benchmark smoke checks also pass. The VFS source and its prior three optimizations were not changed.

## Reproduce

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DSQLITE_EXPRESS_ENABLE_CRSQLITE=ON
cmake --build build --parallel 2
ctest --test-dir build --output-on-failure
./build/benchmark --seconds=60 --replication=on --batch-size=1
./build/benchmark --seconds=60 --replication=on --batch-size=100
```

See the main README for the pinned Rust nightly and libclang build prerequisites. Sources and Cargo dependencies are local and verified; CMake does not download libraries. Raw before/after results are included alongside this report. The current source manifest and source-files.sha256 record the exact replacement.
