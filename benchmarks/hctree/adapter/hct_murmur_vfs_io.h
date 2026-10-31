/* Experimental HC-tree I/O bridge: not an mmap or transaction adapter. */
#ifndef HCT_MURMUR_VFS_IO_H
#define HCT_MURMUR_VFS_IO_H
#include <sqlite3.h>
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif
typedef struct HctVfsFile HctVfsFile;
/* Process-local recursive guard. Every bridge I/O operation participates.
 * Callers holding the guard may invoke hct_vfs_* methods recursively.
 * Not a cross-process or main+pagemap transaction lock. */
int hct_vfs_lock(void);
void hct_vfs_unlock(void);
/* Pin a file while a copy-backed window refers to it. Close returns
 * SQLITE_BUSY until all pins are released. This does not protect callers
 * racing to create new windows against a concurrently freed file handle. */
int hct_vfs_pin_window(HctVfsFile *file);
void hct_vfs_unpin_window(HctVfsFile *file);
/* Both files use MAIN_DB to select persistent stores in sqlite-murmur.
 * Independent main and pagemap stores are NOT atomically committed.
 */
int hct_vfs_open(const char *vfs_name, const char *path, int create, HctVfsFile **out);
int hct_vfs_read(HctVfsFile *file, void *data, int bytes, int64_t offset);
int hct_vfs_write(HctVfsFile *file, const void *data, int bytes, int64_t offset);
int hct_vfs_truncate(HctVfsFile *file, int64_t size);
int hct_vfs_size(HctVfsFile *file, int64_t *size);
int hct_vfs_sync(HctVfsFile *file);
int hct_vfs_commit(HctVfsFile *file);
int hct_vfs_close(HctVfsFile *file);
#ifdef __cplusplus
}
#endif
#endif
