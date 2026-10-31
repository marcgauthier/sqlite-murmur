#define _POSIX_C_SOURCE 200809L
#include "hct_murmur_vfs_io.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#define REQUIRE(e) do { if (!(e)) { fprintf(stderr, "failed: %s\n", #e); exit(1); } } while (0)
#define CK(e) do { int r = (e); if (r != SQLITE_OK) { fprintf(stderr, "%s: %d\n", #e, r); exit(1); } } while (0)
static void verify(const char *path, const char *magic, int64_t offset) {
  HctVfsFile *file = NULL;
  char buf[32] = {0};
  int64_t size = -1;
  CK(hct_vfs_open("unix", path, 0, &file));
  CK(hct_vfs_size(file, &size));
  REQUIRE(size == offset + (int64_t)strlen(magic));
  CK(hct_vfs_read(file, buf, (int)strlen(magic), offset));
  REQUIRE(strcmp(buf, magic) == 0);
  CK(hct_vfs_close(file));
}
int main(void) {
  char tmp[] = "/tmp/hct-murmur-vfs-XXXXXX";
  char mainpath[512], pagemappath[512];
  HctVfsFile *mainfile = NULL, *pagemap = NULL;
  char readback[32] = {0};
  int64_t size = -1;
  if (!mkdtemp(tmp)) { perror("mkdtemp"); return 1; }
  REQUIRE(snprintf(mainpath, sizeof(mainpath), "%s/main.db", tmp) < (int)sizeof(mainpath));
  REQUIRE(snprintf(pagemappath, sizeof(pagemappath), "%s/main.db-pagemap", tmp) < (int)sizeof(pagemappath));
  REQUIRE(hct_vfs_open("not-a-real-vfs", mainpath, 1, &mainfile) == SQLITE_NOTFOUND);
  REQUIRE(mainfile == NULL);
  REQUIRE(hct_vfs_open("unix", mainpath, 0, &mainfile) != SQLITE_OK);
  REQUIRE(mainfile == NULL);
  CK(hct_vfs_open("unix", mainpath, 1, &mainfile));
  CK(hct_vfs_open("unix", pagemappath, 1, &pagemap));
  CK(hct_vfs_write(mainfile, "hctree", 6, 0));
  CK(hct_vfs_write(pagemap, "page-map", 8, 8192));
  CK(hct_vfs_read(mainfile, readback, 6, 0));
  REQUIRE(strcmp(readback, "hctree") == 0);
  CK(hct_vfs_size(pagemap, &size));
  REQUIRE(size == 8200);
  CK(hct_vfs_commit(mainfile));
  CK(hct_vfs_commit(pagemap));
  CK(hct_vfs_close(mainfile));
  CK(hct_vfs_close(pagemap));
  verify(mainpath, "hctree", 0);
  verify(pagemappath, "page-map", 8192);
  CK(hct_vfs_open("unix", pagemappath, 0, &pagemap));
  CK(hct_vfs_truncate(pagemap, 1024));
  CK(hct_vfs_commit(pagemap));
  CK(hct_vfs_close(pagemap));
  CK(hct_vfs_open("unix", pagemappath, 0, &pagemap));
  CK(hct_vfs_size(pagemap, &size));
  REQUIRE(size == 1024);
  CK(hct_vfs_close(pagemap));
  unlink(mainpath); unlink(pagemappath); rmdir(tmp);
  puts("HCT_VFS_IO_TEST_PASS,unix,persistent-main-and-pagemap");
  return 0;
}
