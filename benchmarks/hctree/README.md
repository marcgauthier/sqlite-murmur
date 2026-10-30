# SQLite Murmur vs HC-tree, 1 and 100 inserts per transaction

The GitHub workflow runs eight 60-second cases sequentially on the same
Ubuntu runner, with 50,000 pre-seeded rows, one connection, 80% primary-key
reads / 20% new inserts, the same 69-byte compressible payload, 1 GiB page
cache target, and deterministic random lookup pattern.

Two backends: Rust sqlite-murmur with RocksDB, encryption, compression;
and SQLite's official HC-tree branch, built from Fossil.
Replication OFF measures ordinary writes. Replication ON measures local
Murmur cr-sqlite change-tracking vs HC-tree native leader journaling.
**These are not equivalent replication algorithms**, and **neither runs
live inter-node replication**. HC-tree leader mode commits using the
required native sqlite3_hct_journal_leader_commit API and records a
replayable SQL string. No stock SQLite fallback is permitted.

Results appear in the Actions artifact as summary.csv, logs and version data.
HC-tree is an experimental backend with documented crash/power-loss recovery
limitations; its close time and Murmur's final RocksDB checkpoint are
not equivalent durable persistence barriers. This tests only one
connection, not HC-tree's multi-writer scaling.

See https://sqlite.org/hctree/doc/hctree/doc/hctree/index.html and
https://sqlite.org/hctree/doc/hctree/doc/hctree/replication2.md .
