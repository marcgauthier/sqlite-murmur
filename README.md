# sqlite-murmur

Embedded SQLite-compatible database library with RocksDB persistence,
Zstandard compression, AES-256-GCM or AEGIS-256 encryption, and built-in
cr-sqlite/Murmur replication over QUIC. All third-party libraries link statically
into the consuming application's executable. No extension files are required.

> **Status: prototype.** Do not use for critical data until the crash/fault-injection
> suite and multi-connection stress tests have been completed. The VFS deliberately
> weakens durability: SQLite may report COMMIT before a RocksDB flush completes.

## Use from an application

Link one public CMake target, whether using this repository as a subdirectory or
using an installed package:

```cmake
# Source checkout:
add_subdirectory(path/to/sqlite-murmur)
# Or installed package: find_package(sqlite_murmur CONFIG REQUIRED)
target_link_libraries(my_app PRIVATE sqlite_murmur::sqlite_murmur)
```

Include one header, register your encryption key once, then use the integrated
open/close API and ordinary SQLite SQL functions:

```c
#include <sqlite_murmur.h>

/* key is an application-supplied 32-byte encryption key. Check return codes. */
sqlite_murmur_config config = {key, 250, 16384, 1};
sqlite_murmur_register(&config);
sqlite3 *db = NULL;
sqlite_murmur_open("database-directory", &db);
sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS records("
                 "id INTEGER PRIMARY KEY NOT NULL, body TEXT DEFAULT '')",
             NULL, NULL, NULL);
/* Configure PRAGMA murmur_* and SELECT murmur_start() to enable networking. */
sqlite_murmur_close(db);
sqlite_murmur_unregister();
```

`sqlite_murmur_open()` selects the encrypted RocksDB VFS and initializes the
built-in CRR and Murmur SQL interfaces. Multiple integrated connections to the
same database share one replication service; closing the final connection stops
it. Finalize application statements before `sqlite_murmur_close()`. Subsequent
SQL uses the standard `sqlite3_prepare`, bind, step, exec and finalize APIs.
Plain `sqlite3_open()` still uses SQLite's ordinary default VFS: use the
sqlite-murmur open functions for encrypted RocksDB storage.

Networking remains stopped until the application configures replication through
PRAGMAs and calls `SELECT murmur_start()`. Only tables explicitly converted with
`SELECT crsql_as_crr('main','records')` replicate. Matching schemas and node
certificates are application configuration, not separately deployed libraries.

`sqlite_murmur_open_v2()` accepts SQLite open flags and optional storage
settings. Supply `cipher`/`compression` in its options to apply them before
extension metadata is created. To benchmark or use storage without replication,
set `options.flags = SQLITE_MURMUR_OPEN_STORAGE_ONLY`; this skips both CRR and
Murmur initialization and their metadata. Read-only opens also skip replication
initialization. Murmur benchmark connections use this storage-only public API
and explicitly enable cr-sqlite for replication cases.

A complete C application and independent installed-package CMake project are in
[`examples/consumer`](examples/consumer). The source layout stays conventional:
`include/` for the public API, `src/` for integration/VFS code, `third-party/` for
vendored sources, `cmake/` for static build integration, and `tests/` and
`benchmarks/` for consumers. Build artifacts and platform-specific static archives
stay outside version control; GitHub checkouts build them from vendored sources.

Rust storage and replication use the pinned, vendored AWS-LC provider for
AES-256-GCM, HKDF-SHA256, SHA-256, TLS, and cryptographic randomness. Managed
UUID-v5 identifiers use an internal compatibility helper backed by AWS-LC.
Storage and transport share bindings
to the vendored C zstd archive. These provider changes preserve existing
page records, key envelopes, file objects, and replication formats. See
[`VENDORS.md`](VENDORS.md) for dependency ownership and remaining transitive crates.

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

The workspace includes a small internal `ipnet` compatibility crate for CIDR
allowlists and HTTP proxy exclusions. Its root `[patch.crates-io]` also redirects
`hyper-util` to this implementation. Rust consumers building outside this
workspace must apply the same patch to select the internal subset. Its source
provenance and retained licenses are in `crates/murmur-ipnet/PROVENANCE.md`.

AWS-LC builds its vendored native sources using CMake and a C/C++ toolchain;
Perl is also required by the native build. Storage-only builds need these
prerequisites as well. No dependency downloads are required. For direct Cargo
tests, first build the native libraries with CMake, then select that build tree
with `MURMUR_BUILD_DIR=/absolute/path/to/build`. The default is `build-a1` at the
repository root. Transport tests link its vendored zstd archive rather than a
system zstd installation. Direct Cargo tests also need a host SQLite development
library.

All third-party library source is checked into [`third-party/`](third-party/README.md):
SQLite's amalgamation, RocksDB, Zstd, libaegis, cr-sqlite and Murmur's
locked Rust dependencies. A checkout configures and builds without downloading dependencies or
using installed library packages. Only the platform toolchain and build tools are
needed. On Debian/Ubuntu:

```bash
sudo apt-get install cmake g++ make perl python3 libclang-dev openssl pkg-config binutils
rustup toolchain install stable --profile minimal
rustup toolchain install nightly-2023-10-05 --profile minimal
python3 third-party/verify.py
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel 2
ctest --test-dir build --output-on-failure
./build/benchmark
```

The first build compiles the bundled libraries and takes longer than later builds.
Vendored OpenSSL is no longer built, linked, or installed. The system `openssl`
command remains a test tool for generating and inspecting certificates.
Linux and macOS builds use their native compilers and system runtime libraries.
Native Windows/cross-compilation is not configured by the project build.

AEGIS-256, cr-sqlite and Murmur are enabled by default and link statically.
Loadable extension libraries are disabled by default; advanced users can request
`-DSQLITE_MURMUR_BUILD_LOADABLE_EXTENSIONS=ON`. The optional Murmur loadable
module resolves SQLite and `sqlite_murmur_file_key_*` symbols from its host;
CMake hosts must enable symbol exports with `ENABLE_EXPORTS ON`, as the loadable
test driver does. Third-party linkage needs no shared libraries; normal platform runtime libraries (such as libc/libstdc++)
remain system dependencies. This does not force a fully static OS runtime.

Tests and the `benchmark` executable are enabled for a standalone checkout and
default to off when imported with `add_subdirectory`. To build an
AES-only library, set `-DSQLITE_MURMUR_ENABLE_AEGIS256=OFF`; its benchmark can run
with `--cipher=aes256gcm`. `-DSQLITE_MURMUR_BUILD_BENCHMARKS=OFF` omits the executable.
Dependency versions, upstream license files, archive checksums and update
instructions are recorded in `third-party/`. Generated files stay under `build/`.

## Automatic QUIC replication with Murmur

The default build includes `SQLITE_MURMUR_ENABLE_MURMUR=ON` and
`SQLITE_MURMUR_ENABLE_CRSQLITE=ON`, integrating the Rust replication code in
[`crates/murmur-replication`](crates/murmur-replication/README.md).
It discovers peers using bootstrap addresses and SWIM/gossip, authenticates them
with mutual TLS, and automatically exchanges cr-sqlite changes over QUIC with
persistent progress, retries and acknowledgements after VFS persistence.

Clients supply all settings through `PRAGMA murmur_*` and use SQLite SQL for
start/stop, synchronization and peer administration. There is no PostgreSQL wire
protocol or separate database server. Static integration is the default;
loadable builds are an explicit optional artifact. See the complete [SQL usage guide](crates/murmur-replication/USAGE.md)
for certificates, PRAGMAs, durability and lifecycle requirements.

## cr-sqlite tracking and ingestion benchmarks

The updated **cr-sqlite-2** working-tree snapshot from `../cr-sqlite-2` is vendored
in `third-party/cr-sqlite`, including its local tracked/untracked source changes,
pinned Rust submodule and locked Cargo packages. It is based on upstream commit
`43ab94cac537d080aac7cb7628a435c02ee9e268`, with local write/merge optimizations and
staged/blob bulk-ingest APIs. Its exact content fingerprint and provenance are
recorded in `third-party/sources.json`; `sqlite-murmur-revision.txt`, `crsql_sha()`
and benchmark `crsqlite_version` identify the captured local revision. Updating the
sibling checkout does not change this build until its sources are explicitly
revendored. This snapshot keeps the existing upstream version string internally;
it is not represented as a new official upstream release. The optional build uses
our SQLite amalgamation, rather than compiling a second SQLite. Install compiler tooling once, then the build
uses only local sources (`cargo --frozen` with offline configuration):

```bash
sudo apt-get install libclang-dev
rustup toolchain install nightly-2023-10-05 --profile minimal
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DSQLITE_MURMUR_ENABLE_CRSQLITE=ON
cmake --build build --parallel 2
ctest --test-dir build --output-on-failure
./build/benchmark --suite=single --replication=on --batch-size=1
./build/benchmark --suite=single --replication=on --batch-size=100
```

The default comparison suite runs both replication-off and replication-on groups.
Single runs default to replication off; `--suite=single --replication=off` selects
that baseline. Each case uses 80% reads / 20% inserts for 60 seconds, excluding seeding and verification.
When enabled, `records` is registered as a CRR before seeding; every insert runs
cr-sqlite triggers and maintains replication clocks. `tracked_changes` verifies
that each timed insert can be exported through `crsql_changes`; reopening also
checks the persisted change count. File sizes include replication metadata.
These runs measure **local replication tracking**, without peer transport or
remote change application in the timed loop. The replication integration test
separately exchanges changes between two encrypted databases and checks inserts,
updates, deletes, idempotent replay, staged bulk ingestion, rollback, cross-connection
version-cache invalidation and reopening.

To measure **incoming replication** with the new API, use:

```bash
./build/benchmark --suite=single --ingest=blob --batch-size=1
./build/benchmark --suite=single --ingest=blob --batch-size=100
# Same incoming changes, applied one INSERT per change for comparison:
./build/benchmark --suite=single --ingest=row --batch-size=100
```

`--ingest=row|blob` enables replication automatically. A separate in-memory source
inserts new records, exports **all nine columns verbatim** with
`SELECT * FROM crsql_changes WHERE db_version>?`, and packs CRSB v1 bytes in C++
using the format from `crsql_correctness.bulk.pack_changes`. Blob mode binds the
result to `SELECT crsql_ingest_blob(?)`, checks its applied count and commits.
Row mode binds the same captured values to `INSERT INTO crsql_changes`. Each
batch uses one explicit destination transaction in both modes, including batch 1.
Four random destination reads per imported row follow the commit, preserving
80% reads / 20% successfully applied row writes. The seeded destination starts
with 50,000 locally created records; incoming IDs are new and never replayed.

The 60-second timer sums **destination apply, commit and read work only**.
Source inserts, export, packing and batch cleanup are excluded. Both modes
perform the same export and packing work outside that timer. The source uses
ordinary in-memory SQLite; only the destination uses the encrypted RocksDB VFS.
`connections=2` includes that source connection. `wall_seconds`,
`source_prepare_seconds`, `packed_bytes` and `applied_changes` are reported;
`wall_ops_per_second_including_persistence` includes source preparation, cleanup
and final destination persistence. Wall duration exceeds 60 seconds. Destination
background flushing continues during excluded source work, so these active-time
rates must not be compared directly to continuous local-insert rates. No network
is simulated. Disk size and compression statistics cover the destination only.

Tests compare the C++ packer byte-for-byte with Python, including NULL, integer,
float, UTF-8/embedded NUL, empty text and blob values. Differential replication
tests compare blob and row application of inserts, updates, deletion and replay,
including base rows, exported clocks, database versions and rows impacted.
Malformed input rejection and encrypted reopening are checked as well.

For advanced manual initialization on raw SQLite handles, CMake clients can
link `sqlite_murmur::crsqlite` and include `sqlite_murmur_crsqlite.h`. Configure
the VFS cipher/page size before initializing the extension on each connection
with `sqlite3_crsqlite_init(db, &error, nullptr)`. Convert tables using
`SELECT crsql_as_crr('main','records')` (the explicit schema avoids the upstream
single-argument path). CRR primary keys must be non-null; other NOT NULL columns
need defaults. Low-level callers can exchange the nine `crsql_changes` fields
using their own transport; integrated applications use Murmur's QUIC transport.
With manual initialization, finalize application statements, call
`SELECT crsql_finalize()`, then close the connection. These compatibility targets
are also available in the installed package. The recommended application API
initializes and finalizes the built-in features automatically.

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
rc = sqlite_murmur_open("data.murmur", &db);
// `data.murmur` is a RocksDB DIRECTORY, not a SQLite file.
if (rc == SQLITE_OK) sqlite3_exec(db,
    "PRAGMA journal_mode=DELETE; PRAGMA mmap_size=0;", nullptr, nullptr, nullptr);
// Run SQL as normal.
sqlite_murmur_close(db);  // finalize built-in features and close the database
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
Integrated opens create internal replication tables, so select a new database's
cipher through `sqlite_murmur_open_options.cipher` before that initialization.
With storage-only/raw VFS opens, the early cipher PRAGMA remains available.
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

### Key envelope (`murmur_kdf*`)

Databases without an envelope encrypt pages directly with the registered
32-byte key (legacy path, unchanged). Sealing a keys.enc v3 envelope moves
page crypto to a dedicated data key while the registered key becomes the
KDF master:

```sql
PRAGMA murmur_kdf;                -- 'direct' until sealed
PRAGMA murmur_kdf_time = 5;       -- stage before sealing (optional)
PRAGMA murmur_kdf = 'direct';     -- seal the envelope now (write-once)
PRAGMA murmur_kdf_time;           -- read back from the sealed header
SELECT murmur_key_inventory();    -- non-secret JSON: seq, kdf, keys[]
```

| PRAGMA | Accepted values | Default |
| --- | --- | --- |
| `murmur_kdf` | `direct` or `argon2id` | `direct` |
| `murmur_kdf_time` | Integer 1 through 32 | `3` |
| `murmur_kdf_memory_kib` | Integer 1024 through 16777216 | `65536` |
| `murmur_kdf_threads` | Integer 1 through 32 | `4` |

Setting `murmur_kdf` seals immediately with the staged (or default) time /
memory / threads, so set those first when the defaults do not suit — but
note the cost params only survive an `argon2id` seal: `direct` envelopes
record zeros (Direct-HKDF has no cost params; this matches the archived
v3 format). All four are write-once after the seal, require autocommit, a
read-write connection, and stopped replication — and unlike `cipher`,
sealing is allowed on a live store: pages written before the seal stay
readable via the master fallback until a flush lazily re-encrypts them
with the data key. The envelope bytes live in RocksDB metadata beside the
cipher name
(readable pre-key; the envelope authenticates itself), never in
`murmur_config`. A wrong registered key fails the open; `argon2id` without
a vendored Argon2 unit fails closed with an explanatory error. Key
rotation and full-store rekey arrive with the offline `rekey` command.

### Files and bridges (`murmur_file_*`, `murmur_bridge_*`)

Large objects replicate as content-chunked rows plus `murmur_kv` metadata
— never the filesystem, except the operator-chosen air-gap bridge
directory. File payload crypto (SPFO) needs a provisioned group file key:

```c
sqlite_murmur_register_file_key(current32, previous_or_NULL32);
```

Without provisioning, every file action fails closed. The twelve knobs
are restart-applied PRAGMAs (persisted, read back, applied at next open):

| PRAGMA | Default | Meaning |
| --- | --- | --- |
| `murmur_bridge_dir` | `""` | Bridge bundle dir, relative to the DB parent (air-gap exception) |
| `murmur_files_generation` | `0` | Active file-key generation (rotation: previous required, one step) |
| `murmur_max_file_bytes` | `4294967296` | Largest single file (1 MiB–32 GiB) |
| `murmur_max_staging_bytes` | `4294967296` | Staging ceiling (1 MiB–64 GiB) |
| `murmur_fetch_concurrency` | `8` | Fetch fan-out (1–64) |
| `murmur_fetch_interval_ms` | `5000` | Fetch tick throttle (≥1000 ms) |
| `murmur_fetch_bind` | `""` | Fetch endpoint bind; empty = disabled (embedded opt-in) |
| `murmur_bridge_role` | `""` | Bridge role (`low` or `high` when bridging) |
| `murmur_bridge_stream` | `""` | Bridge stream name (`[A-Za-z0-9_-]{1,64}`) |
| `murmur_bridge_max_bundle_bytes` | `67108864` | Bundle ceiling (≥4 KiB) |
| `murmur_bridge_max_payload_bytes` | `67108864` | Payload ceiling (≥4 KiB) |
| `murmur_bridge_staging_bytes` | `134217728` | Bundle staging (≥4 KiB; must cover one bundle) |

`murmur_files_dir` is accepted and persisted but inert (deprecated: chunk
rows replaced the payload directory). Twelve `murmur_file_*` actions
(upload, download, read ≤1 MiB, delete, list, metadata, availability,
fetch, last_error, collect, rotate, rotation_status) and ten
`murmur_bridge_*` actions (export, process, capture, stage, export_file,
export_snapshot, release, configure, publish_ftp, publish_http) drive the
surface; side-effecting actions require autocommit. `murmur_file_fetch()`
reports 0 until the fetch split lands; bridge file export fails closed
until then. Full action reference:
[USAGE](crates/murmur-files/USAGE.md).

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

The `benchmark` executable runs two comparison groups: **test 1 without
replication**, and **test 2 with local cr-sqlite change tracking**. Each group
runs these four cases sequentially through the same bundled SQLite library:

| Backend | Inserts per transaction | Encryption | Compression |
| --- | --- | --- | --- |
| sqlite-murmur | 1 and 100 | Always enabled | Always enabled |
| Plain SQLite on disk, 1 GiB SQLite page-cache target | 1 and 100 | Disabled | Disabled |

```bash
./build/benchmark                         # All eight cases, 60 measured seconds each
./build/benchmark --suite=no-replication   # Test 1 only
./build/benchmark --suite=crsqlite         # Test 2 only
./build/benchmark --seconds=0.01           # Quick matrix and integrity check
# One configurable case:
./build/benchmark --suite=single --backend=murmur --batch-size=100 \
  --flush-interval-ms=10 --compression=zstd6 --cipher=aes256gcm
```

See [benchmark methodology and historical results](benchmarks/BENCHMARK.md).
Both comparison suites require a 1 GiB SQLite cache target, configured after page
size selection using `PRAGMA cache_size=-1048576`; this grows on demand. Murmur
also has a 64 MiB VFS block cache target and a 32 MiB RocksDB block cache.

| Option | Default | Accepted values |
| --- | --- | --- |
| `--suite` | `all` | `all`, `no-replication`, `crsqlite`, `single` |
| `--seconds` | 60 per case | Greater than 0, up to 3600; fractional values supported |
| `--backend` | `murmur` | Single mode: `murmur`, `sqlite-disk` |
| `--batch-size` | 1 in single mode | Single mode: inserts per transaction, 1 through 1,000,000 |
| `--replication` | `off` in single mode | Single mode: `off`, `on` for local tracking |
| `--ingest` | `local` | Single mode: `local`, `row`, `blob`; incoming modes require Murmur and enable cr-sqlite |
| `--flush-interval-ms` | 100 | Positive integer through 4294967295; Murmur only |
| `--cache-pages` | 16384 | Positive count of 4 KiB VFS blocks; Murmur only |
| `--sqlite-cache-kib` | 1048576 | 0 through 2147483647; nondefault values require single mode |
| `--compression` | `zstd3` | `zstd1` through `zstd22`; Murmur only; disabling compression is rejected |
| `--cipher` | `aegis256` | `aegis256` or `aes256gcm`; Murmur only |
| `--page-size` | 4096 | Powers of two from 512 through 65536 |
| `--background-threads` | `auto` | Physical-core default, or 1 through 256 preparation workers; Murmur only |

Each fresh database starts with 50,000 rows containing a fixed, highly
compressible payload. The timed workload performs one insert and four random
primary-key reads per group. At 1x each insert autocommits; at 100x a transaction
contains 100 inserts and 400 interleaved reads. Complete the final transaction
before stopping. Setup, seeding, and verification are excluded. Incoming
`row`/`blob` single runs instead measure destination-active time as described above.

Murmur always encrypts and compresses its destination storage, defaulting to
AEGIS-256 and Zstd3, with DELETE journaling, `synchronous=FULL`, and a 100 ms
background persistence interval. Plain disk SQLite uses its native VFS,
WAL journaling and `synchronous=NORMAL`, with normal automatic checkpoints.
Its final persistence barrier flushes the page cache and truncates/checkpoints
the WAL. These configurations have different commit durability guarantees;
Murmur's FULL setting does not make its acknowledged commits wait for background persistence.

Every case emits `BENCH_CONFIG`, `BENCH_HEADER`, `BENCH_CSV`, and `BENCH_DONE`.
CSV rows identify suite, backend, replication, and transaction size. Each group
also prints a comparison table and Murmur-to-SQLite ratios at matching batch sizes.
`BENCH_SUITE_DONE` marks successful completion of the entire request. Reports
include total/read/write throughput, final persistence time and inclusive
throughput, mean write transaction time, logical size, and total `disk_bytes`.
Murmur-specific statistics use `NA` for plain SQLite. The 1x transaction timer
covers the insert only;
the 100x timer includes interleaved reads. Compression savings include seeding.
Fixed-duration cases create different row counts, so compare bytes per row as well
as total size. Disk sizes include regular database/storage files after close;
they do not measure cumulative write traffic or allocated filesystem blocks.

All cases verify row counts and integrity after reopening. Replicated cases also
verify exported change counts.
A build without cr-sqlite can run test 1 but rejects test 2 and the default full
suite. CI validates and uploads the full eight-case, 60-second-per-case report.

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
