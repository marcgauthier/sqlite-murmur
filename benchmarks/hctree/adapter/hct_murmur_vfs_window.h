/* Explicit dirty-page windows for experimental HC-tree/Murmur I/O.
 * NOT POSIX MAP_SHARED: callers must mark every modified range and flush.
 * No concurrent mapping coherence or atomic main+pagemap commit is provided.
 */
#ifndef HCT_MURMUR_VFS_WINDOW_H
#define HCT_MURMUR_VFS_WINDOW_H
#include "hct_murmur_vfs_io.h"
#include <stddef.h>
#ifdef __cplusplus
extern "C" {
#endif
typedef struct HctVfsWindow HctVfsWindow;
int hct_vfs_window_open(HctVfsFile *file, int64_t offset, size_t bytes,
                        int writable, HctVfsWindow **out);
void *hct_vfs_window_data(HctVfsWindow *window);
int hct_vfs_window_dirty(HctVfsWindow *window, size_t offset, size_t bytes);
int hct_vfs_window_flush(HctVfsWindow *window);
/* Flushes pending writes. On failure the window remains live for retry. */
int hct_vfs_window_close(HctVfsWindow *window);
/* Discards unflushed writes, for explicit rollback/error cleanup. */
void hct_vfs_window_discard(HctVfsWindow *window);
#ifdef __cplusplus
}
#endif
#endif
