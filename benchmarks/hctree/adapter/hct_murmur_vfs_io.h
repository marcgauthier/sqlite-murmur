/* Experimental HC-tree I/O bridge: not an mmap or transaction adapter. */
#ifndef HCT_MURMUR_VFS_IO_H
#define HCT_MURMUR_VFS_IO_H
#include <sqlite3.h>
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif
typedef struct HctVfsFile HctVfsFile;
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
