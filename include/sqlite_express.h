#pragma once

#include <stddef.h>
#include <sqlite3.h>

#ifdef __cplusplus
extern "C" {
#endif

/* The caller owns key memory. Exactly 32 bytes are copied during registration.
 * flush_interval_ms=0 selects 250 ms; cache_pages=0 selects 16384.
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
 * In-progress transactions are never flushed. */
int sqlite_express_flush(const char *path);

/* Returns SQLITE_BUSY if sqlite-express database handles are still open. */
int sqlite_express_unregister(void);

#ifdef __cplusplus
}
#endif
