#pragma once
#include <sqlite3.h>
#ifdef __cplusplus
extern "C" {
#endif
/* Initialize on one writable sqlite-express control connection in autocommit
 * mode. Static callers pass NULL for api; release errors with sqlite3_free().
 * Configure with PRAGMA murmur_*; operate with SELECT murmur_start()/stop()/sync().
 * Keep this connection open while replication is active; closing it stops workers.
 * Call murmur_stop() before finalizing cr-sqlite on application connections. */
int sqlite3_murmurreplication_init(sqlite3 *db, char **error, const sqlite3_api_routines *api);
#ifdef __cplusplus
}
#endif
