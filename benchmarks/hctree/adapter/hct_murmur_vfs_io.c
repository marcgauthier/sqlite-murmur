#define _POSIX_C_SOURCE 200809L
#include "hct_murmur_vfs_io.h"
#include "sqlite_murmur.h"
#include <limits.h>
#include <stddef.h>
#include <string.h>
#include <pthread.h>
struct HctVfsFile { sqlite3_file *handle; int murmur; unsigned int windows; };

/* Temporary process-wide recursive lock for the experimental bridge.
 * A window flush must compare its snapshot and write all dirty ranges as
 * one indivisible operation relative to other bridge reads/writes/resizes.
 * Recursive because window_flush calls the public hct_vfs_* methods.
 * This does NOT coordinate other processes or SQLite/HC-tree I/O that
 * bypasses the bridge. Replace with per-store transaction coordination
 * before performance benchmarking. */
static pthread_once_t hct_lock_once = PTHREAD_ONCE_INIT;
static pthread_mutex_t hct_lock;
static int hct_lock_init_rc = 0;
static void hct_lock_init(void) {
  pthread_mutexattr_t attr;
  int rc = pthread_mutexattr_init(&attr);
  if (rc == 0) {
    rc = pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    if (rc == 0) rc = pthread_mutex_init(&hct_lock, &attr);
    pthread_mutexattr_destroy(&attr);
  }
  hct_lock_init_rc = rc;
}
int hct_vfs_lock(void) {
  if (pthread_once(&hct_lock_once, hct_lock_init) != 0 ||
      hct_lock_init_rc != 0 || pthread_mutex_lock(&hct_lock) != 0)
    return SQLITE_IOERR;
  return SQLITE_OK;
}
void hct_vfs_unlock(void) { pthread_mutex_unlock(&hct_lock); }
int hct_vfs_pin_window(HctVfsFile *file) {
  int rc;
  if (!file) return SQLITE_MISUSE;
  rc = hct_vfs_lock();
  if (rc != SQLITE_OK) return rc;
  if (file->windows == UINT_MAX) rc = SQLITE_FULL;
  else { ++file->windows; rc = SQLITE_OK; }
  hct_vfs_unlock();
  return rc;
}
void hct_vfs_unpin_window(HctVfsFile *file) {
  if (!file || hct_vfs_lock() != SQLITE_OK) return;
  if (file->windows) --file->windows;
  hct_vfs_unlock();
}
int hct_vfs_open(const char *vfs_name, const char *path, int create,
                 HctVfsFile **out) {
  sqlite3_vfs *vfs;
  HctVfsFile *file;
  int flags, actual_flags = 0, rc;
  if (!out || !path || !*path || !vfs_name || !*vfs_name) return SQLITE_MISUSE;
  *out = NULL;
  vfs = sqlite3_vfs_find(vfs_name);
  if (!vfs) return SQLITE_NOTFOUND;
  if (vfs->szOsFile < (int)sizeof(sqlite3_file)) return SQLITE_MISUSE;
  file = sqlite3_malloc64(sizeof(*file));
  if (!file) return SQLITE_NOMEM;
  file->murmur = strcmp(vfs_name, "sqlite-murmur") == 0;
  file->windows = 0;
  file->handle = sqlite3_malloc64((sqlite3_uint64)vfs->szOsFile);
  if (!file->handle) { sqlite3_free(file); return SQLITE_NOMEM; }
  memset(file->handle, 0, (size_t)vfs->szOsFile);
  /* Auxiliary files are transient in Murmur: pagemap needs its own main store. */
  flags = SQLITE_OPEN_MAIN_DB | SQLITE_OPEN_READWRITE;
  if (create) flags |= SQLITE_OPEN_CREATE;
  rc = vfs->xOpen(vfs, path, file->handle, flags, &actual_flags);
  if (rc != SQLITE_OK) {
    sqlite3_free(file->handle);
    sqlite3_free(file);
    return rc;
  }
  if (!file->handle->pMethods || !file->handle->pMethods->xClose ||
      !file->handle->pMethods->xRead || !file->handle->pMethods->xWrite ||
      !file->handle->pMethods->xTruncate ||
      !file->handle->pMethods->xFileSize || !file->handle->pMethods->xSync) {
    if (file->handle->pMethods && file->handle->pMethods->xClose)
      file->handle->pMethods->xClose(file->handle);
    sqlite3_free(file->handle);
    sqlite3_free(file);
    return SQLITE_MISUSE;
  }
  if (actual_flags & SQLITE_OPEN_READONLY) {
    file->handle->pMethods->xClose(file->handle);
    sqlite3_free(file->handle);
    sqlite3_free(file);
    return SQLITE_READONLY;
  }
  *out = file;
  return SQLITE_OK;
}
int hct_vfs_read(HctVfsFile *file, void *data, int bytes, int64_t offset) {
  if (!file || !data || bytes < 0 || offset < 0 || offset > INT64_MAX - bytes)
    return SQLITE_MISUSE;
  int rc = hct_vfs_lock();
  if (rc != SQLITE_OK) return rc;
  rc = file->handle->pMethods->xRead(file->handle, data, bytes, offset);
  hct_vfs_unlock();
  return rc;
}
int hct_vfs_write(HctVfsFile *file, const void *data, int bytes, int64_t offset) {
  if (!file || !data || bytes < 0 || offset < 0 || offset > INT64_MAX - bytes)
    return SQLITE_MISUSE;
  int rc = hct_vfs_lock();
  if (rc != SQLITE_OK) return rc;
  rc = file->handle->pMethods->xWrite(file->handle, data, bytes, offset);
  hct_vfs_unlock();
  return rc;
}
int hct_vfs_truncate(HctVfsFile *file, int64_t size) {
  if (!file || size < 0) return SQLITE_MISUSE;
  int rc = hct_vfs_lock();
  if (rc != SQLITE_OK) return rc;
  rc = file->handle->pMethods->xTruncate(file->handle, size);
  hct_vfs_unlock();
  return rc;
}
int hct_vfs_size(HctVfsFile *file, int64_t *size) {
  sqlite3_int64 value = 0;
  int rc;
  if (!file || !size) return SQLITE_MISUSE;
  rc = hct_vfs_lock();
  if (rc != SQLITE_OK) return rc;
  rc = file->handle->pMethods->xFileSize(file->handle, &value);
  if (rc == SQLITE_OK) *size = (int64_t)value;
  hct_vfs_unlock();
  return rc;
}
int hct_vfs_sync(HctVfsFile *file) {
  if (!file) return SQLITE_MISUSE;
  int rc = hct_vfs_lock();
  if (rc != SQLITE_OK) return rc;
  rc = file->handle->pMethods->xSync(file->handle, SQLITE_SYNC_NORMAL);
  hct_vfs_unlock();
  return rc;
}
int hct_vfs_commit(HctVfsFile *file) {
  int rc;
  if (!file) return SQLITE_MISUSE;
  rc = hct_vfs_lock();
  if (rc != SQLITE_OK) return rc;
  rc = hct_vfs_sync(file);
  if (rc == SQLITE_OK && file->murmur) {
    if (!file->handle->pMethods->xFileControl) rc = SQLITE_MISUSE;
    else {
      rc = file->handle->pMethods->xFileControl(
          file->handle, SQLITE_FCNTL_COMMIT_PHASETWO, NULL);
      if (rc == SQLITE_OK)
        rc = file->handle->pMethods->xFileControl(
            file->handle, SQLITE_MURMUR_FCNTL_FLUSH, NULL);
    }
  }
  hct_vfs_unlock();
  return rc;
}
int hct_vfs_close(HctVfsFile *file) {
  int rc;
  if (!file) return SQLITE_MISUSE;
  rc = hct_vfs_lock();
  if (rc != SQLITE_OK) return rc;
  /* Never free the backing sqlite3_file while a window can still flush. */
  if (file->windows) {
    hct_vfs_unlock();
    return SQLITE_BUSY;
  }
  rc = file->handle->pMethods->xClose(file->handle);
  hct_vfs_unlock();
  sqlite3_free(file->handle);
  sqlite3_free(file);
  return rc;
}
