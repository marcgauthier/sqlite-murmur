#include "hct_murmur_vfs_io.h"
#include "sqlite_murmur.h"
#include <limits.h>
#include <stddef.h>
#include <string.h>
struct HctVfsFile { sqlite3_file *handle; int murmur; };
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
  return file->handle->pMethods->xRead(file->handle, data, bytes, offset);
}
int hct_vfs_write(HctVfsFile *file, const void *data, int bytes, int64_t offset) {
  if (!file || !data || bytes < 0 || offset < 0 || offset > INT64_MAX - bytes)
    return SQLITE_MISUSE;
  return file->handle->pMethods->xWrite(file->handle, data, bytes, offset);
}
int hct_vfs_truncate(HctVfsFile *file, int64_t size) {
  if (!file || size < 0) return SQLITE_MISUSE;
  return file->handle->pMethods->xTruncate(file->handle, size);
}
int hct_vfs_size(HctVfsFile *file, int64_t *size) {
  sqlite3_int64 value = 0;
  int rc;
  if (!file || !size) return SQLITE_MISUSE;
  rc = file->handle->pMethods->xFileSize(file->handle, &value);
  if (rc == SQLITE_OK) *size = (int64_t)value;
  return rc;
}
int hct_vfs_sync(HctVfsFile *file) {
  if (!file) return SQLITE_MISUSE;
  return file->handle->pMethods->xSync(file->handle, SQLITE_SYNC_NORMAL);
}
int hct_vfs_commit(HctVfsFile *file) {
  int rc;
  if (!file) return SQLITE_MISUSE;
  rc = hct_vfs_sync(file);
  if (rc != SQLITE_OK) return rc;
  if (!file->murmur) return SQLITE_OK;
  if (!file->handle->pMethods->xFileControl) return SQLITE_MISUSE;
  rc = file->handle->pMethods->xFileControl(
      file->handle, SQLITE_FCNTL_COMMIT_PHASETWO, NULL);
  if (rc != SQLITE_OK) return rc;
  return file->handle->pMethods->xFileControl(
      file->handle, SQLITE_MURMUR_FCNTL_FLUSH, NULL);
}
int hct_vfs_close(HctVfsFile *file) {
  int rc;
  if (!file) return SQLITE_MISUSE;
  rc = file->handle->pMethods->xClose(file->handle);
  sqlite3_free(file->handle);
  sqlite3_free(file);
  return rc;
}
