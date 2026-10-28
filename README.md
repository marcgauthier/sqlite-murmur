# sqlite-murmur

Experimental **SQLite Virtual File System** backed by RocksDB, with memory-first
writes, page coalescing, Zstandard compression and AES-256-GCM or AEGIS-256 encryption.

> **Status: prototype.** Do not use for critical data until the crash/fault-injection
> suite and multi-connection stress tests have been completed. The VFS deliberately
> weakens durability: SQLite may report COMMIT before a RocksDB flush completes.

## Architecture

- SQLite's main database file is virtual. The path supplied to SQLite becomes a
  **RocksDB directory**; it is not an ordinary SQLite file.
- VFS `xWrite` copies SQLite's data into owned, reusable fixed-size **4096-byte
  buffers**. It performs no compression or encryption. SQLite can reuse its
  source memory immediately after the callback returns. Repeated writes to the
  same block replace the pending immutable copy.
- Complete aligned 4096-byte block overwrites skip loading/decrypting the old
  block. Partial blocks retain read-modify-write to preserve untouched bytes.
- Rollback journals and SQLite temporary files are kept in RAM.
- After SQLite finishes a **DELETE-mode rollback-journal transaction**, the latest
  copied blocks become eligible for persistence. Uncommitted pages are never
  submitted to RocksDB.
- A background coordinator captures completed transactions. A configurable pool
  compresses their pages with Zstd level 3 and encrypts them with fresh nonces in
  parallel. The coordinator then applies one atomic, ordered **RocksDB WriteBatch**
  containing the changed blocks and database size. The C API interval defaults to
  250 ms; the benchmark uses your configured 100 ms default.
- Reads use the latest pending copy when present, otherwise the encrypted VFS cache
  or RocksDB. Clean cached pages remain encrypted and are authenticated per VFS
  read. SQLite's own pager cache is separate and may retain plaintext.
- One RocksDB DB object can serve multiple SQLite connections in the same process;
  RocksDB supports concurrent operations against that object, but SQLite still has
  **one SQL writer per database at a time**.

## Concurrency and buffer reuse

Pending page copies and cached encrypted records are immutable shared versions.
Foreground reads capture references and file-size/generation metadata under a short
Store mutex, then copy pending data or decrypt/decompress stored data outside it.
A concurrent write or truncation makes the read retry. A stale read cannot put a
plaintext snapshot back into the clean cache after persistence.

Foreground writes and truncation prepare replacement blocks privately, then
publish them under the Store mutex. A separate mutation mutex orders writes,
truncation, transaction completion and cipher selection without blocking readers
or the flush workers. Full-block overwrites skip the old-data read. Partial writes
still load the previous contents when necessary to preserve untouched bytes.

The fixed-size page-buffer pool retains up to `config.cache_pages` idle 4 KiB buffers
(64 MiB at the benchmark defaults), allocated on demand. Ownership
passes to the pending batch without another page copy; a buffer is cleansed and
reused only after its last reference is released. Pending/in-flight committed pages
reaching `config.cache_pages` wake the coordinator and apply backpressure to further
writes until a batch completes. Active transactions can exceed this target because
their pages must not be flushed before commit. This is a backlog target, not a total
process-memory cap. Background errors wake blocked writers with an error.

`PRAGMA background_threads` selects **1 through 256** compression/encryption workers,
shared by connections to the same Store. The default is the detected physical core
count on Linux, capped at 256; other platforms use the hardware concurrency count.
There is one additional coordinator thread. Settings reset on reopen; changing the
worker count waits for any current batch and replaces the pool safely:

```sql
PRAGMA background_threads;       -- query current worker count
PRAGMA background_threads = 4;   -- also supports 8, 16, etc.
```

Each worker owns a reusable Zstd compression context, scratch buffer and decode
workspace. Pages remain independent Zstd frames. Compression/encryption and RocksDB
writes run off the SQLite thread, including explicit flush/checkpoint requests and
close-time draining. Final RocksDB batch writes remain ordered: concurrent writes
of overlapping page versions could otherwise let an older commit overwrite a newer
one, and splitting batches would lose atomic size/truncation updates.

RAM rollback-journal storage is reused across transactions. Logical file size is
separate from allocated storage, so deletion/reopening yields an empty journal
without reallocating or zero-initializing each subsequent write. Sparse gaps and
truncate/regrow regions remain zero-filled. Each Store retains at most one idle
journal allocation of at most 1 MiB; larger allocations are released. The active
journal can grow beyond that limit. Plaintext is cleansed before a buffer returns
to the pool, and outstanding handles prevent premature reuse. Journal visibility
and DELETE-mode commit/rollback behavior remain unchanged.

Pending plaintext reads copy only the requested slice directly from their pinned
immutable page buffer into SQLite's output buffer. They do not copy or wipe a full
scratch page, and acquire no decode workspace. Missing zero blocks likewise fill
the requested output directly. Generation checks still retry writes/truncations
that race the copy. Encrypted reads acquire decode workspaces lazily and retain
authentication, decompression and scratch cleansing. Plaintext page buffers are
still cleansed before pool reuse after their last reference is released.

Foreground operations that need decoding lease workspaces (at most eight idle).
Explicit plaintext buffers and compression scratch are cleansed on release and
failure. This does not erase SQLite's plaintext cache or guarantee erasure of
third-party contexts' internal state. Pending copies remain plaintext until their
last reference is released after processing.

Encrypted-record capacity is also recycled after its last reference is released,
with at most 128 idle buffers retained. Shared-pointer control blocks may still
allocate. Cache eviction keeps exact LRU order, skips redundant MRU touches, and
avoids scanning an all-dirty cache when no entry can be evicted.

A failed flush retains ownership of its committed snapshot without allocating
recovery nodes. A retry writes that snapshot before draining newer completed
changes, preserving write/trim order. Only cache entries still pointing to the
flushed version are marked clean. The shared Store mutex is not held during
background compression, encryption or RocksDB writes.

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

All third-party library source is checked into [`third-party/`](third-party/README.md):
SQLite's amalgamation, RocksDB, Zstd, libaegis and OpenSSL (AES-256-GCM and secure
randomness). A checkout configures and builds without downloading dependencies or
using installed library packages. Only the platform toolchain and build tools are
needed. On Debian/Ubuntu:

```bash
sudo apt-get install cmake g++ make perl python3
python3 third-party/verify.py
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel 2
ctest --test-dir build --output-on-failure
./build/benchmark
```

The first build compiles the bundled libraries and takes longer than later builds.
OpenSSL uses its own out-of-tree Perl/GNU Make build; other libraries use CMake.
`SQLITE_MURMUR_OPENSSL_JOBS` controls OpenSSL's parallelism (default 2).
Linux and macOS builds use their native compilers and system runtime libraries.
Native Windows/cross-compilation is not configured by this OpenSSL wrapper.

AEGIS-256 and the `benchmark` executable are enabled by default. To build an
AES-only library, set `-DSQLITE_MURMUR_ENABLE_AEGIS256=OFF`; its benchmark can run
with `--cipher=aes256gcm`. `-DSQLITE_MURMUR_BUILD_BENCHMARKS=OFF` omits the executable.
Dependency versions, upstream license files, archive checksums and update
instructions are recorded in `third-party/`. Generated files stay under `build/`.

## Optional Murmur session replication

Enable `SQLITE_MURMUR_ENABLE_MURMUR_EXT=ON` to build the Rust session
replication extension in
[`third-party/murmur-extension`](third-party/murmur-extension/README.md)
statically into the benchmark. It captures table changes with the SQLite
Sessions API, keeps CRDT metadata and HLC clocks in a Spool directory beside
the database, and merges with per-column last-writer-wins. The extension also
offers built-in QUIC transport (`murmur_repl_*`); the benchmark uses only
local capture and manual log exchange. Its engine crates
(murmur-crdt/spool/replication) are local path dependencies. The older
cr-sqlite-based integration under `third-party/murmur-replication` is
currently unwired pending adaptation. Install a modern Rust toolchain once,
then the build uses only local sources (`cargo --frozen` with offline
configuration):

```bash
rustup toolchain install stable --profile minimal
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DSQLITE_MURMUR_ENABLE_MURMUR_EXT=ON
cmake --build build --parallel 2
ctest --test-dir build --output-on-failure
./build/benchmark --replication=on --batch-size=1
./build/benchmark --replication=on --batch-size=100
```

Replication defaults to **off**; `--replication=off` provides the baseline. Each
run uses 80% reads / 20% inserts for 60 seconds, excluding seeding and verification.
When enabled, `records` is adopted with `murmur_enable_sync` before seeding; every
timed transaction captures its session changeset with `murmur_collect` and folds it
with `murmur_drain`. `tracked_changes` verifies that each timed insert was logged;
reopening also checks the persisted log count. Reported sizes cover the RocksDB
files plus the Spool directory. These runs measure **local replication tracking**,
without peer transport or remote change application in the timed loop.

To measure **incoming replication**, use:

```bash
./build/benchmark --ingest=log --batch-size=1
./build/benchmark --ingest=log --batch-size=100
```

`--ingest=log` enables replication automatically. A separate file-backed source
database inserts new records and exports the incremental replication log with
`SELECT murmur_log_export(origin, version)`; the destination applies each batch
with `SELECT murmur_apply(?)`, which commits internally. Four random destination
reads per imported row follow, preserving 80% reads / 20% successfully applied
row writes. The seeded destination starts with 50,000 locally created records;
incoming IDs are new and never replayed.

The 60-second timer sums **destination apply and read work only**. Source inserts,
collection, export and batch cleanup are excluded. The source uses an ordinary
SQLite file; only the destination uses the encrypted RocksDB VFS.
`connections=2` includes that source connection. `wall_seconds`,
`source_prepare_seconds`, `packed_bytes` and `applied_changes` are reported;
`wall_ops_per_second_including_persistence` includes source preparation, cleanup
and final destination persistence. Wall duration exceeds 60 seconds. Destination
background flushing continues during excluded source work, so these active-time
rates must not be compared directly to continuous local-insert rates. No network
is simulated. Disk size and compression statistics cover the destination only.

CMake clients in this source build can link `sqlite_murmur::murmur_ext` alongside
`sqlite_murmur::sqlite_murmur` and include `sqlite_murmur_ext.h`. Initialize the
extension on each connection with `sqlite3_murmur_ext_init(db, &error, nullptr)`,
then `SELECT murmur_open(spool_directory)` (one spool per database), adopt tables
with `SELECT murmur_enable_sync('records')`, capture each write transaction with
`SELECT murmur_collect()` before COMMIT, and fold batches with
`SELECT murmur_drain()`. Exchange incremental batches with
`murmur_log_export`/`murmur_apply`, and call `SELECT murmur_close()` before
closing the connection. The optional extension CMake target is currently provided
for source builds, not the installed package.

## Usage

```cpp
#include "sqlite_murmur.h"
#include <sqlite3.h>

unsigned char key[32] = {/* load a securely generated 256-bit key */};
sqlite_murmur_config cfg = {};
cfg.key = key;
cfg.flush_interval_ms = 250;  // 0 = default 250
cfg.cache_pages = 16384;      // 0 = default 16384
cfg.rocksdb_sync = 1;         // durable RocksDB batch on each background flush
int rc = sqlite_murmur_register(&cfg);
if (rc != SQLITE_OK) return rc;

sqlite3 *db = nullptr;
rc = sqlite3_open_v2("data.murmur", &db,
    SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "sqlite-murmur");
// `data.murmur` is a RocksDB DIRECTORY, not a SQLite file.
if (rc == SQLITE_OK) sqlite3_exec(db,
    "PRAGMA journal_mode=DELETE; PRAGMA mmap_size=0;", nullptr, nullptr, nullptr);
// Run SQL as normal.
sqlite3_close(db);  // normal close flushes remaining committed changes
sqlite_murmur_unregister();
```

The key must be the **same on every reopen**; a different key causes authenticated
decrypt failure. Never persist the key beside the database unprotected. Only page
**values** are encrypted: RocksDB key names/metadata and the directory contents
still leak some structure.

## Runtime PRAGMAs

After opening a connection through the `sqlite-murmur` VFS:

```sql
PRAGMA flush_interval_ms = 100;   -- background persistence interval in milliseconds
PRAGMA flush_interval_ms;         -- read current interval
PRAGMA flush_interval_ms = 0;     -- restore the 250 ms default

PRAGMA cipher = 'aes256gcm';       -- default; select before creating tables
-- Or, with libaegis support enabled:
PRAGMA cipher = 'aegis256';
PRAGMA cipher;                    -- read the selected cipher

PRAGMA compression = 'zstd3';      -- default; compress before encrypting
PRAGMA compression = 'none';       -- disable compression
PRAGMA compression;               -- read current selection
```

`flush_interval_ms` accepts unsigned integers from 0 through 4294967295. It applies
to every connection sharing this database's Store, and updating it restarts the
background timer. It is not persisted: after all connections close, the next open
uses the interval supplied to `sqlite_murmur_register`. The interval is not a
maximum durability delay; memory pressure can trigger earlier persistence.
`sqlite_murmur_flush` explicitly persists completed transactions.

`cipher` accepts `aes256gcm` or `aegis256` (case insensitive). Choose it on an empty
database before creating tables or starting transactions. It is shared by all
connections and stored in RocksDB metadata for automatic selection on reopen.
Changing the algorithm after database writes is rejected; this PRAGMA does not
migrate or re-encrypt an existing database. Read-only connections cannot set it.
Both algorithms use the existing 32-byte key. AES-256-GCM retains the original
`EXP1` records with 12-byte nonces and 16-byte tags; AEGIS-256 uses `EXP2` records
with 32-byte nonces and 32-byte tags. Old AES databases without cipher metadata
continue to open as AES-256-GCM.

`compression` accepts `none` or `zstd1` through `zstd22`, case insensitive.
It changes the encoding of future persistence batches for every connection
sharing the Store. Existing raw and compressed records remain readable; changing
this setting does not rewrite them. It is not persisted and resets to `zstd3`
when the Store reopens. Read-only connections can query but cannot assign it.
Compression runs **before encryption**; RocksDB compression remains disabled.

### RocksDB tuning

These PRAGMAs query or change the live RocksDB backend. For example:

```sql
PRAGMA rocksdb_sync = 1;
PRAGMA rocksdb_write_buffer_size = 67108864;  -- 64 MiB per memtable
PRAGMA rocksdb_max_write_buffer_number = 2;
PRAGMA rocksdb_block_cache_bytes = 33554432;  -- 32 MiB read cache
PRAGMA rocksdb_max_background_jobs = 2;
PRAGMA rocksdb_write_buffer_size;            -- query effective value
```

| PRAGMA | Accepted values | Initial value |
| --- | --- | --- |
| `rocksdb_sync` | `0` or `1` | `config.rocksdb_sync` |
| `rocksdb_write_buffer_size` | Integer bytes, at least 65,536 | RocksDB default (typically 64 MiB) |
| `rocksdb_max_write_buffer_number` | Integer from 2 through 2,147,483,647 | RocksDB default (typically 2) |
| `rocksdb_block_cache_bytes` | Integer bytes, at least 0 | 32 MiB |
| `rocksdb_max_background_jobs` | Integer from 1 through 2,147,483,647 | RocksDB default (typically 2) |

Byte sizes must fit both the platform's `size_t` and a signed 64-bit integer.
Assignments return the effective value; invalid values and backend errors are
reported as SQL errors. Read-only connections can query but cannot assign these
settings. All connections sharing a database see the same values; other databases
retain their own settings.

Changes apply to the open Store only. After its last connection closes, reopening
uses the registration configuration and RocksDB defaults again; sqlite-murmur
does not reload tuning values from RocksDB's generated OPTIONS files.

`rocksdb_sync=1` syncs subsequent sqlite-murmur WriteBatches to disk. Switching to
it does not itself flush pending changes or sync earlier batches. SQLite COMMIT
still uses the configured deferred persistence interval. RocksDB WAL remains
enabled for both values.

Write-buffer options use RocksDB's `SetOptions`; background jobs use
`SetDBOptions`. Memtable size changes affect future memtables and do not resize
an existing one. These setters may write OPTIONS files, so configure them when
tuning a database rather than on each SQL operation. Block-cache capacity changes
immediately; pinned blocks can temporarily exceed the capacity, including at zero.
Neither the RocksDB block cache nor its memtables impose a total process-memory
limit. SQLite and the VFS maintain their own caches.

SQLite's own page cache is configured separately with `PRAGMA cache_size`:
positive values specify SQLite pages, negative values specify approximately KiB.
For example, `PRAGMA cache_size=-65536` requests approximately 64 MiB per connection.
This does not change the VFS block cache (`config.cache_pages`, shared
by connections) or RocksDB's block cache.

## API

See `include/sqlite_murmur.h` for `sqlite_murmur_register`,
`sqlite_murmur_flush`, `sqlite_murmur_get_stats`, `sqlite_murmur_checkpoint` and
`sqlite_murmur_unregister`.

## Benchmark

There is one executable: `benchmark`. It performs **80% random primary-key reads /
20% inserts** at full speed through a single SQLite connection (autocommit by default).
Incoming replication can instead be measured with `--ingest=row|blob`, described above.

```bash
./build/benchmark
# Override settings for one run:
./build/benchmark --seconds=60 --flush-interval-ms=10 --cache-pages=16384 \
  --compression=none --cipher=aes256gcm --page-size=8192
```

| Option | Default | Accepted values |
| --- | --- | --- |
| `--seconds` | 60 | Greater than 0, up to 3600; fractional values supported |
| `--batch-size` | 1 | Inserts per transaction, from 1 through 1,000,000 |
| `--flush-interval-ms` | 100 | Positive integer through 4294967295 |
| `--cache-pages` | 16384 | Positive count of **4 KiB VFS blocks** |
| `--sqlite-cache-kib` | 1048576 | SQLite cache target in KiB, 0 through 2147483647 |
| `--compression` | `zstd3` | `none` or `zstd1` through `zstd22` |
| `--cipher` | `aegis256` | `aegis256` or `aes256gcm` |
| `--page-size` | 4096 | SQLite page size: powers of two from 512 through 65536 |
| `--background-threads` | `auto` | Physical-core default, or 1 through 256 preparation workers |
| `--replication` | `off` | `on` enables local cr-sqlite tracking in an enabled build |
| `--ingest` | `local` | `local` inserts; `row` or `blob` times destination replication, enabling cr-sqlite |

The default SQLite page-cache target is **1 GiB**, configured with
`PRAGMA cache_size=-1048576` (negative values specify KiB), equivalent to
262,144 pages at the default 4 KiB SQLite page size. It grows on demand
and retains decrypted SQLite pages. This setting is applied after SQLite page
size selection, so the target stays 1 GiB across supported page sizes.
`--sqlite-cache-kib=0` selects the previously tested zero-cache setting.

The separate VFS cache defaults to **16,384 × 4096 bytes = 64 MiB** of
block data, plus metadata and encryption overhead. Clean entries are encrypted;
pending write copies are plaintext. Committed backlog pressure triggers early
persistence and backpressure; active transactions can exceed this target. RocksDB's separate 32 MiB block cache remains unchanged.
Changing SQLite's page size does not change the VFS's 4 KiB storage block size.

Each run creates a fresh temporary database with 50,000 seeded rows. It then
performs one insert and four random reads across all existing rows per group,
without sleeps or rate limiting. With `--batch-size=1`, each insert uses autocommit.
With larger batches, `BEGIN IMMEDIATE` / `COMMIT` wraps that many groups: a batch
of 100 contains 100 inserts and 400 interleaved reads. The last full transaction
finishes before stopping, preserving the exact 80/20 ratio and batch size.

Compare single inserts against bulk transactions with:

```bash
./build/benchmark --batch-size=1 > batch1.txt
./build/benchmark --batch-size=100 > batch100.txt
```

The report includes committed write transaction count and mean write transaction
duration. For autocommit this times just the insert; for bulk transactions it
includes BEGIN, the inserts and interleaved reads, and COMMIT. The bulk duration
therefore also reflects how long the single-writer lock is held; it is not a
per-insert latency. Fixed-duration runs insert different numbers of rows, so
compare file bytes per row as well as final file size.

Payloads are fixed, highly compressible 70-byte
text. Setup and initial persistence are excluded from the timer. Background
flushes run normally; memory pressure can also trigger early persistence.

The report shows total/read/write operations per second, logical SQLite size,
RocksDB regular-file sizes in bytes and MiB, compression selection and payload
savings across persisted blocks (including seeding). Final persistence time and
throughput including that time are emitted in `BENCH_CSV`, preceded by a
`BENCH_CONFIG` line containing the actual run settings. Compression savings are
zero when disabled. The final file size includes SST, WAL and metadata; it is
not cumulative disk-write traffic or allocated filesystem blocks.

Reopening checks row count and database integrity before deleting the temporary
database. For a short test, use `./build/benchmark --seconds=0.01`. CI builds the
vendored sources, runs tests, then runs and uploads one default 60-second result.

The C API `sqlite_murmur_get_stats(path, &stats)` snapshots cumulative per-Store
VFS/persistence counters and RocksDB WAL, SST-flush and compaction write counters.
Counters reset on reopen; missing Stores return `SQLITE_NOTFOUND`, and null
arguments return `SQLITE_MISUSE`. RocksDB tickers exclude filesystem/device
amplification and are collected without timer or histogram instrumentation.

`sqlite_murmur_checkpoint(path)` persists completed transactions, flushes RocksDB
memtables and waits up to 30 seconds for compaction (`SQLITE_BUSY` on timeout).
The memtable flush itself waits for its I/O to finish. Active SQLite transactions
are excluded. Stop concurrent writers when using it as a measurement barrier.
This is heavier than `sqlite_murmur_flush`, which does not force SST creation.

## Roadmap before production

1. Fault-injection at every write, delete and commit point; power-loss replay tests.
2. Stress tests with hundreds of parallel readers and writers, transaction rollbacks,
   low cache caps, full-disk and wrong-key scenarios.
3. Total-memory accounting and backlog observability, including large active transactions.
4. WAL-mode support only with its own commit-frame and checkpoint-aware recovery.
5. Optional encrypted metadata and secure key lifecycle / platform key storage.

License: MIT.
