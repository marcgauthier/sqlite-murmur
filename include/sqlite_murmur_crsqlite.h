#ifndef SQLITE_MURMUR_CRSQLITE_H
#define SQLITE_MURMUR_CRSQLITE_H
#include <sqlite3.h>
#ifdef __cplusplus
extern "C" {
#endif
/* Link sqlite_murmur::crsqlite (SQLITE_MURMUR_ENABLE_CRSQLITE=ON).
 * Configure the empty VFS database first, then initialize each connection.
 * Use SELECT crsql_as_crr('main','table_name') to enable a table.
 * Call SELECT crsql_finalize() after finalizing application statements and
 * before sqlite3_close(). Pass NULL for api; free any error with sqlite3_free().
 */
int sqlite3_crsqlite_init(sqlite3 *db, char **error, const sqlite3_api_routines *api);
#ifdef __cplusplus
}
#endif
#endif
