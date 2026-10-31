#include "hct_murmur_vfs_window.h"
#include <limits.h>
#include <stdint.h>
#include <string.h>

/* Sparse, exact byte ranges. Flushing whole 4 KiB snapshot pages would
 * overwrite unrelated writes from another independently opened window. */
typedef struct HctDirtySpan {
  size_t begin, end; /* Half-open byte range relative to window->data. */
  struct HctDirtySpan *next;
} HctDirtySpan;
struct HctVfsWindow {
  HctVfsFile *file;
  int64_t offset;
  size_t bytes;
  int64_t original_size; /* Detect resize since the window snapshot. */
  unsigned char *data;
  unsigned char *baseline; /* Snapshot for sequential conflict detection. */
  HctDirtySpan *dirty;
  int writable;
};
void hct_vfs_window_discard(HctVfsWindow *window) {
  HctDirtySpan *span, *next;
  if (!window) return;
  for (span = window->dirty; span; span = next) {
    next = span->next;
    sqlite3_free(span);
  }
  sqlite3_free(window->baseline);
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
  w->writable = writable;
  rc = hct_vfs_size(file, &w->original_size);
  if (rc != SQLITE_OK) {
    hct_vfs_window_discard(w);
    return rc;
  }
  w->data = sqlite3_malloc64(bytes);
  if (!w->data) {
    hct_vfs_window_discard(w);
    return SQLITE_NOMEM;
  }
  /* SQLite VFS xRead zero-fills past EOF and returns SHORT_READ. */
  rc = hct_vfs_read(file, w->data, (int)bytes, offset);
  if (rc != SQLITE_OK && rc != SQLITE_IOERR_SHORT_READ) {
    hct_vfs_window_discard(w);
    return rc;
  }
  if (writable) {
    w->baseline = sqlite3_malloc64(bytes);
    if (!w->baseline) {
      hct_vfs_window_discard(w);
      return SQLITE_NOMEM;
    }
    memcpy(w->baseline, w->data, bytes);
  }
  *out = w;
  return SQLITE_OK;
}
void *hct_vfs_window_data(HctVfsWindow *window) {
  return window ? window->data : NULL;
}
int hct_vfs_window_dirty(HctVfsWindow *w, size_t offset, size_t bytes) {
  HctDirtySpan **link, *span, *merged;
  size_t begin, end;
  if (!w || !w->writable || !bytes || offset >= w->bytes ||
      bytes > w->bytes - offset) return SQLITE_MISUSE;
  /* Allocate before modifying the list: OOM must leave dirty data tracked. */
  merged = sqlite3_malloc64(sizeof(*merged));
  if (!merged) return SQLITE_NOMEM;
  begin = offset;
  end = offset + bytes;
  link = &w->dirty;
  while (*link && (*link)->end < begin) link = &(*link)->next;
  while (*link && (*link)->begin <= end) {
    span = *link;
    if (span->begin < begin) begin = span->begin;
    if (span->end > end) end = span->end;
    *link = span->next;
    sqlite3_free(span);
  }
  merged->begin = begin;
  merged->end = end;
  merged->next = *link;
  *link = merged;
  return SQLITE_OK;
}
int hct_vfs_window_flush(HctVfsWindow *w) {
  HctDirtySpan *span;
  unsigned char current[4096];
  int rc;
  if (!w) return SQLITE_MISUSE;
  if (!w->writable || !w->dirty) return SQLITE_OK;

  /* Protect the entire check-and-write sequence, including all dirty spans.
   * Without this, simultaneous flushes can both pass their baseline checks
   * before either writes. All bridge reads/writes/truncates participate in
   * this process-local recursive lock. It does not protect bypassing I/O,
   * other processes, or atomicity across main and -pagemap stores. */
  rc = hct_vfs_lock();
  if (rc != SQLITE_OK) return rc;
  {
    int64_t current_size = -1;
    rc = hct_vfs_size(w->file, &current_size);
    if (rc != SQLITE_OK) goto done;
    if (current_size != w->original_size) {
      rc = SQLITE_BUSY_SNAPSHOT;
      goto done;
    }
  }
  /* Check ALL dirty ranges before writing any. An optimistic conflict
   * must not partially apply a multi-span window. */
  for (span = w->dirty; span; span = span->next) {
    size_t pos = span->begin;
    while (pos < span->end) {
      size_t remaining = span->end - pos;
      int n = (int)(remaining < sizeof(current) ? remaining : sizeof(current));
      rc = hct_vfs_read(w->file, current, n, w->offset + (int64_t)pos);
      if (rc != SQLITE_OK && rc != SQLITE_IOERR_SHORT_READ) goto done;
      if (memcmp(current, w->baseline + pos, (size_t)n) != 0) {
        rc = SQLITE_BUSY_SNAPSHOT;
        goto done;
      }
      pos += (size_t)n;
    }
  }
  while ((span = w->dirty) != NULL) {
    /* Write only explicitly dirtied bytes, never a stale page snapshot. */
    rc = hct_vfs_write(w->file, w->data + span->begin,
                       (int)(span->end - span->begin),
                       w->offset + (int64_t)span->begin);
    if (rc != SQLITE_OK) goto done;
    if (w->offset + (int64_t)span->end > w->original_size)
      w->original_size = w->offset + (int64_t)span->end;
    memcpy(w->baseline + span->begin, w->data + span->begin,
           span->end - span->begin);
    w->dirty = span->next;
    sqlite3_free(span);
  }
  rc = SQLITE_OK;
done:
  hct_vfs_unlock();
  /* Caller must hct_vfs_commit() after flushing both files. The two
   * independent stores still have no atomic transaction boundary. */
  return rc;
}
int hct_vfs_window_close(HctVfsWindow *w) {
  int rc;
  if (!w) return SQLITE_MISUSE;
  rc = hct_vfs_window_flush(w);
  if (rc == SQLITE_OK) hct_vfs_window_discard(w);
  return rc;
}
