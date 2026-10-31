#include "hct_murmur_vfs_window.h"
#include <limits.h>
#include <stdint.h>
#include <string.h>
#define HCT_WINDOW_PAGE 4096u
struct HctVfsWindow {
  HctVfsFile *file;
  int64_t offset;
  size_t bytes;
  size_t pages;
  unsigned char *data;
  unsigned char *dirty;
  int writable;
};
void hct_vfs_window_discard(HctVfsWindow *window) {
  if (!window) return;
  sqlite3_free(window->dirty);
  sqlite3_free(window->data);
  sqlite3_free(window);
}
int hct_vfs_window_open(HctVfsFile *file, int64_t offset, size_t bytes,
                        int writable, HctVfsWindow **out) {
  HctVfsWindow *w;
  int rc;
  if (!out) return SQLITE_MISUSE;
  *out = NULL;
  if (!file || offset < 0 || bytes == 0 || bytes > INT_MAX ||
      (uint64_t)offset > (uint64_t)INT64_MAX - bytes ||
      (writable != 0 && writable != 1)) return SQLITE_MISUSE;
  w = sqlite3_malloc64(sizeof(*w));
  if (!w) return SQLITE_NOMEM;
  memset(w, 0, sizeof(*w));
  w->file = file;
  w->offset = offset;
  w->bytes = bytes;
  w->pages = (bytes + HCT_WINDOW_PAGE - 1) / HCT_WINDOW_PAGE;
  w->writable = writable;
  w->data = sqlite3_malloc64(bytes);
  w->dirty = sqlite3_malloc64(w->pages);
  if (!w->data || !w->dirty) {
    hct_vfs_window_discard(w);
    return SQLITE_NOMEM;
  }
  memset(w->dirty, 0, w->pages);
  /* SQLite VFS xRead zero-fills past EOF and returns SHORT_READ. */
  rc = hct_vfs_read(file, w->data, (int)bytes, offset);
  if (rc != SQLITE_OK && rc != SQLITE_IOERR_SHORT_READ) {
    hct_vfs_window_discard(w);
    return rc;
  }
  *out = w;
  return SQLITE_OK;
}
void *hct_vfs_window_data(HctVfsWindow *window) {
  return window ? window->data : NULL;
}
int hct_vfs_window_dirty(HctVfsWindow *w, size_t offset, size_t bytes) {
  size_t first, last, i;
  if (!w || !w->writable || !bytes || offset >= w->bytes ||
      bytes > w->bytes - offset) return SQLITE_MISUSE;
  first = offset / HCT_WINDOW_PAGE;
  last = (offset + bytes - 1) / HCT_WINDOW_PAGE;
  for (i = first; i <= last; ++i) w->dirty[i] = 1;
  return SQLITE_OK;
}
int hct_vfs_window_flush(HctVfsWindow *w) {
  size_t i = 0;
  if (!w) return SQLITE_MISUSE;
  if (!w->writable) return SQLITE_OK;
  while (i < w->pages) {
    size_t first, end, from, to;
    int rc;
    if (!w->dirty[i]) { ++i; continue; }
    first = i;
    while (i < w->pages && w->dirty[i]) ++i;
    end = i;
    from = first * HCT_WINDOW_PAGE;
    to = end * HCT_WINDOW_PAGE;
    if (to > w->bytes) to = w->bytes;
    rc = hct_vfs_write(w->file, w->data + from, (int)(to - from),
                       w->offset + (int64_t)from);
    if (rc != SQLITE_OK) return rc;
    memset(w->dirty + first, 0, end - first);
  }
  /* Caller must explicitly hct_vfs_commit() after flushing both files.
   * The two independent stores still have no atomic transaction boundary. */
  return SQLITE_OK;
}
int hct_vfs_window_close(HctVfsWindow *w) {
  int rc;
  if (!w) return SQLITE_MISUSE;
  rc = hct_vfs_window_flush(w);
  if (rc == SQLITE_OK) hct_vfs_window_discard(w);
  return rc;
}
