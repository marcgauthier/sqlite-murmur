#pragma once

#include <stddef.h>
#include <stdint.h>
#include <sqlite3.h>

#ifdef __cplusplus
extern "C" {
#endif

/* The caller owns key memory. Exactly 32 bytes are copied during registration.
 * flush_interval_ms=0 selects 250 ms; cache_pages=0 selects 16384.
 * PRAGMA flush_interval_ms overrides the interval for an open database Store;
 * it resets to this config value when the Store is reopened.
 * PRAGMA rocksdb_sync overrides batch syncing for an open Store; it also
 * resets to the registration value on reopen. Other RocksDB runtime PRAGMAs
 * are documented in README.md.
 * PRAGMA background_threads selects 1..256 parallel compression/encryption
 * workers, defaulting to physical cores on Linux (hardware concurrency elsewhere).
 * cache_pages also controls committed write-backlog pressure; active transactions
 * may exceed it. Pending page copies are plaintext; clean cache entries encrypted.
 * PRAGMA cipher selects aes256gcm (default) or optional aegis256 on an empty
 * database before creating tables. The cipher is remembered on reopen.
 * PRAGMA compression selects none or zstd1..zstd22 for future persistence
 * batches. It resets to zstd3 on reopen; stored records remain readable.
 * rocksdb_sync=1 syncs each completed RocksDB batch; 0 uses RocksDB WAL
 * without fsync, which can lose additional batches after a power failure. */
typedef struct sqlite_express_config {
    const unsigned char *key;
    unsigned flush_interval_ms;
    size_t cache_pages;
    int rocksdb_sync;
} sqlite_express_config;

/* Registers the VFS under the name "sqlite-express". */
int sqlite_express_register(const sqlite_express_config *config);

/* Persist all COMPLETED transactions still waiting for the background worker.
 * path must be the same database name passed to sqlite3_open_v2.
 * In-progress transactions are never flushed. Processing runs on background
 * threads; this call waits for completion and reports any background error. */
int sqlite_express_flush(const char *path);

/* Cumulative diagnostics for one open Store. Counters reset on reopen.
 * Compression/record counters include only successfully persisted page versions.
 * RocksDB write counters exclude filesystem/device write amplification. */
typedef struct sqlite_express_stats {
    uint64_t logical_size_bytes;
    uint64_t vfs_write_bytes;
    uint64_t vfs_block_writes;
    uint64_t flush_batches;
    uint64_t persisted_blocks;
    uint64_t uncompressed_bytes;
    uint64_t compressed_payload_bytes;
    uint64_t encrypted_record_bytes;
    uint64_t rocksdb_wal_bytes;
    uint64_t rocksdb_flush_bytes;
    uint64_t rocksdb_compaction_bytes;
} sqlite_express_stats;

/* Snapshot counters without flushing. Same path convention as flush(). */
int sqlite_express_get_stats(const char *path, sqlite_express_stats *out);

/* Diagnostic persistence barrier: flush completed SQLite transactions, flush
 * RocksDB memtables and wait up to 30 seconds for background compactions.
 * Does not persist active SQLite transactions. Call without concurrent writers
 * for a settled measurement. A background-work timeout returns SQLITE_BUSY. */
int sqlite_express_checkpoint(const char *path);

/* Returns SQLITE_BUSY if sqlite-express database handles are still open. */
int sqlite_express_unregister(void);

/* Extension bridge, scoped to one main-database file handle. PRAGMA callbacks
 * run during preparation: never execute SQL on the calling connection. The
 * callback allocates *result with sqlite3_malloc; SQLite owns it afterwards.
 * The VFS owns context after successful registration and destroys it on close.
 * Only one handler may be installed per handle. */
#define SQLITE_EXPRESS_FCNTL_EXTENSION 0x535801
#define SQLITE_EXPRESS_FCNTL_FLUSH 0x535802
typedef struct sqlite_express_extension {
    void *context;
    int (*pragma)(void *context, const char *name, const char *value, char **result);
    void (*destroy)(void *context);
} sqlite_express_extension;

#ifdef __cplusplus
}
#endif
