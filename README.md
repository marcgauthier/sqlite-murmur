# sqlite-express

Experimental **SQLite Virtual File System** backed by RocksDB, with memory-first
writes, page coalescing, Zstandard compression and AES-256-GCM encryption.

> **Status: prototype.** Do not use for critical data until the crash/fault-injection
> suite and multi-connection stress tests have been completed. The VFS deliberately
> weakens durability: SQLite may report COMMIT before a RocksDB flush completes.

## Architecture

- SQLite's main database file is virtual. The path supplied to SQLite becomes a
  **RocksDB directory**; it is not an ordinary SQLite file.
- VFS `xWrite` updates an encrypted in-memory 4096-byte block. Repeated writes to
  the same block overwrite the pending version.
- Rollback journals and SQLite temporary files are kept in RAM.
- After SQLite finishes a **DELETE-mode rollback-journal transaction**, the latest
  encrypted blocks become eligible for persistence.
- Every **250 ms** (default), a background worker compresses blocks with Zstd level 3,
  encrypts using AES-256-GCM with fresh random nonces, and applies a **single RocksDB
  WriteBatch** containing all changed blocks and the database size.
- Reads hit the encrypted cache first, then RocksDB on a cache miss. Decryption
  happens per read; SQLite's *own* pager cache is separate and may hold plaintext.
- One RocksDB DB object can serve multiple SQLite connections in the same process;
  RocksDB supports concurrent operations against that object, but SQLite still has
  **one SQL writer per database at a time**.

## Current compatibility restrictions

**Only rollback-journal `journal_mode=DELETE` is supported.** The VFS rejects changes
to `WAL`, `TRUNCATE`, `PERSIST`, `MEMORY`, and `OFF` via the file-control pragma
hook. SQLite's main database is stored in 4096-byte VFS *storage blocks*, regardless
of the SQLite logical page size. Rollback journals, temporary files and their data
are **not durable**, by design. The first implementation targets **one process** per
database; filesystem-independent cross-process locking is not implemented.

Atomic RocksDB batches provide a coherent persisted database image at completed
commit boundaries. Operating-system/power-loss durability still depends on RocksDB
WAL and the `rocksdb_sync` option. With asynchronous writes, a machine power failure
may lose **more than the configured 250 ms**. The 250 ms interval is a scheduling
interval, not an upper bound on loss or flush latency.

## Dependencies and build

Linux/macOS with C++17, CMake, SQLite3, RocksDB, Zstd and OpenSSL development
packages. On Debian/Ubuntu:

```bash
sudo apt-get install cmake g++ libsqlite3-dev librocksdb-dev libzstd-dev libssl-dev
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

## Usage

```cpp
#include "sqlite_express.h"
#include <sqlite3.h>

unsigned char key[32] = {/* load a securely generated 256-bit key */};
sqlite_express_config cfg = {};
cfg.key = key;
cfg.flush_interval_ms = 250;  // 0 = default 250
cfg.cache_pages = 16384;      // 0 = default 16384
cfg.rocksdb_sync = 1;         // durable RocksDB batch on each background flush
int rc = sqlite_express_register(&cfg);
if (rc != SQLITE_OK) return rc;

sqlite3 *db = nullptr;
rc = sqlite3_open_v2("data.express", &db,
    SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "sqlite-express");
// `data.express` is a RocksDB DIRECTORY, not a SQLite file.
if (rc == SQLITE_OK) sqlite3_exec(db,
    "PRAGMA journal_mode=DELETE; PRAGMA mmap_size=0;", nullptr, nullptr, nullptr);
// Run SQL as normal.
sqlite3_close(db);  // normal close flushes remaining committed changes
sqlite_express_unregister();
```

The key must be the **same on every reopen**; a different key causes authenticated
decrypt failure. Never persist the key beside the database unprotected. Only page
**values** are encrypted: RocksDB key names/metadata and the directory contents
still leak some structure.

## API

See `include/sqlite_express.h` for `sqlite_express_register`,
`sqlite_express_flush` and `sqlite_express_unregister`.

## Roadmap before production

1. Fault-injection at every write, delete and commit point; power-loss replay tests.
2. Stress tests with hundreds of parallel readers and writers, transaction rollbacks,
   low cache caps, full-disk and wrong-key scenarios.
3. Memory high-watermark policy, early flush / backpressure and observability.
4. WAL-mode support only with its own commit-frame and checkpoint-aware recovery.
5. Optional encrypted metadata and secure key lifecycle / platform key storage.

License: MIT.
