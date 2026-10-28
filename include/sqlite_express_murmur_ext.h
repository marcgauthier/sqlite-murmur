#ifndef SQLITE_EXPRESS_MURMUR_EXT_H
#define SQLITE_EXPRESS_MURMUR_EXT_H
#include <sqlite3.h>
#ifdef __cplusplus
extern "C" {
#endif
/* Link sqlite_express::murmur_ext (SQLITE_EXPRESS_ENABLE_MURMUR_EXT=ON).
 * Call once per connection after opening to register the murmur_* SQL
 * functions. Pass NULL for api; free any error with sqlite3_free().
 * Then SELECT murmur_open(spool) (a directory path), adopt tables with
 * SELECT murmur_enable_sync('table'), capture each write transaction with
 * SELECT murmur_collect() before COMMIT, and fold batches with
 * SELECT murmur_drain(). Sync peers incrementally with
 * murmur_log_export/murmur_apply, or wholesale with murmur_export.
 * Call SELECT murmur_close() before sqlite3_close().
 */
int sqlite3_murmur_ext_init(sqlite3 *db, char **error, const sqlite3_api_routines *api);
#ifdef __cplusplus
}
#endif
#endif
