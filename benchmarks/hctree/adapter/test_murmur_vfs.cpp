// Real Murmur/RocksDB bridge test; NOT an integrated HC-tree mmap benchmark.
#include "hct_murmur_vfs_io.h"
#include "hct_murmur_vfs_window.h"
#include "sqlite_murmur.h"
#include <array>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <unistd.h>
namespace fs = std::filesystem;
static void ck(int rc, const char *step) {
  if (rc != SQLITE_OK) throw std::runtime_error(std::string(step) + ": " + std::to_string(rc));
}
int main() {
  const fs::path root = fs::temp_directory_path() /
      ("hct-murmur-vfs-bridge-" + std::to_string(getpid()));
  const std::string main = (root / "main.db").string();
  const std::string pagemap = main + "-pagemap";
  HctVfsFile *a = nullptr, *b = nullptr;
  std::array<unsigned char, 32> key{};
  for (unsigned i = 0; i < key.size(); ++i) key[i] = static_cast<unsigned char>(i + 1);
  sqlite_murmur_config config{};
  config.key = key.data();
  config.flush_interval_ms = 250;
  config.cache_pages = 64;
  config.rocksdb_sync = 1;
  try {
    fs::create_directories(root);
    ck(sqlite_murmur_register(&config), "register VFS");
    ck(hct_vfs_open("sqlite-murmur", main.c_str(), 1, &a), "open main");
    ck(hct_vfs_open("sqlite-murmur", pagemap.c_str(), 1, &b), "open pagemap");
    std::array<char, 4096> page{};
    std::memcpy(page.data(), "hctree", 6);
    ck(hct_vfs_write(a, page.data(), static_cast<int>(page.size()), 0), "write main");
    page.fill(0);
    std::memcpy(page.data(), "page-map", 8);
    ck(hct_vfs_write(b, page.data(), static_cast<int>(page.size()), 0), "write pagemap");
    ck(hct_vfs_commit(a), "commit main");
    ck(hct_vfs_commit(b), "commit pagemap");
    // Explicit dirty-window access through the RocksDB VFS, including EOF
    // zero fill. This is NOT a substitute for POSIX MAP_SHARED semantics.
    HctVfsWindow *window = nullptr;
    ck(hct_vfs_window_open(a, 0, 8192, 1, &window), "open writable main window");
    auto *mapped = static_cast<char *>(hct_vfs_window_data(window));
    if (std::memcmp(mapped, "hctree", 6) || mapped[4100] != 0)
      throw std::runtime_error("window snapshot/EOF zero fill mismatch");
    std::memcpy(mapped + 4100, "window", 6);
    ck(hct_vfs_window_dirty(window, 4100, 6), "mark dirty main page");
    ck(hct_vfs_window_close(window), "flush and close main window");
    window = nullptr;
    ck(hct_vfs_commit(a), "commit window write");
    ck(hct_vfs_window_open(b, 0, 4096, 1, &window), "open pagemap window");
    mapped = static_cast<char *>(hct_vfs_window_data(window));
    std::memcpy(mapped, "discard", 7);
    ck(hct_vfs_window_dirty(window, 0, 7), "mark discarded pagemap write");
    hct_vfs_window_discard(window);
    window = nullptr;

    // Two windows start from the same snapshot. Writing whole dirty 4 KiB
    // pages would lose the first window's unrelated bytes when the second
    // window flushes. Exact dirty byte ranges must survive cold reopening.
    HctVfsWindow *left = nullptr, *right = nullptr;
    ck(hct_vfs_window_open(a, 0, 8192, 1, &left), "open left snapshot");
    ck(hct_vfs_window_open(a, 0, 8192, 1, &right), "open right snapshot");
    auto *leftData = static_cast<char *>(hct_vfs_window_data(left));
    auto *rightData = static_cast<char *>(hct_vfs_window_data(right));
    leftData[20] = 'L';
    leftData[4104] = '1';
    rightData[21] = 'R';
    rightData[4105] = '2';
    ck(hct_vfs_window_dirty(left, 20, 1), "left first byte");
    ck(hct_vfs_window_dirty(left, 4104, 1), "left second page");
    ck(hct_vfs_window_dirty(right, 21, 1), "right first byte");
    ck(hct_vfs_window_dirty(right, 4105, 1), "right second page");
    ck(hct_vfs_window_close(left), "flush left snapshot");
    ck(hct_vfs_window_close(right), "flush right snapshot");
    ck(hct_vfs_commit(a), "commit overlapping snapshots");
    ck(hct_vfs_window_open(a, 0, 4096, 0, &window), "open read-only snapshot");
    if (hct_vfs_window_dirty(window, 1, 1) != SQLITE_MISUSE)
      throw std::runtime_error("read-only window accepted dirty marking");
    ck(hct_vfs_window_close(window), "close read-only snapshot");
    window = nullptr;

    sqlite_murmur_stats s1{}, s2{};
    ck(sqlite_murmur_get_stats(main.c_str(), &s1), "main stats");
    ck(sqlite_murmur_get_stats(pagemap.c_str(), &s2), "pagemap stats");
    if (!s1.persisted_blocks || !s2.persisted_blocks)
      throw std::runtime_error("both stores must persist RocksDB blocks");
    ck(hct_vfs_close(a), "close main"); a = nullptr;
    ck(hct_vfs_close(b), "close pagemap"); b = nullptr;
    if (!fs::is_directory(main) || !fs::is_directory(pagemap))
      throw std::runtime_error("expected RocksDB directories, not POSIX files");
    // A close/reopen under the same VFS registration can reuse live caches.
    // Tear down the entire Murmur registry to prove a cold RocksDB reopen.
    ck(sqlite_murmur_unregister(), "unregister before cold reopen");
    ck(sqlite_murmur_register(&config), "register after cold restart");
    ck(hct_vfs_open("sqlite-murmur", main.c_str(), 0, &a), "reopen main");
    ck(hct_vfs_open("sqlite-murmur", pagemap.c_str(), 0, &b), "reopen pagemap");
    page.fill(0);
    ck(hct_vfs_read(a, page.data(), static_cast<int>(page.size()), 0), "read main");
    if (std::memcmp(page.data(), "hctree", 6)) throw std::runtime_error("main mismatch");
    if (page[20] != 'L' || page[21] != 'R')
      throw std::runtime_error("overlapping first-page windows lost writes");
    std::array<char, 6> windowRead{};
    ck(hct_vfs_read(a, windowRead.data(), 6, 4100), "read persisted dirty window");
    if (std::memcmp(windowRead.data(), "window", 6))
      throw std::runtime_error("dirty window page did not persist");
    std::array<char, 2> overlappingRead{};
    ck(hct_vfs_read(a, overlappingRead.data(), 2, 4104),
       "read overlapping second-page windows");
    if (std::memcmp(overlappingRead.data(), "12", 2))
      throw std::runtime_error("overlapping second-page windows lost writes");
    page.fill(0);
    ck(hct_vfs_read(b, page.data(), static_cast<int>(page.size()), 0), "read pagemap");
    if (std::memcmp(page.data(), "page-map", 8)) throw std::runtime_error("pagemap mismatch");
    int64_t size = -1;
    ck(hct_vfs_size(a, &size), "main size after cold restart");
    if (size != 8192)
      throw std::runtime_error("main window size mismatch after cold restart");
    ck(hct_vfs_size(b, &size), "pagemap size after cold restart");
    if (size != static_cast<int64_t>(page.size()))
      throw std::runtime_error("pagemap size mismatch after cold restart");
    // Verify a second independent persistence cycle, including truncation.
    ck(hct_vfs_truncate(b, 2048), "truncate pagemap");
    ck(hct_vfs_commit(b), "commit pagemap truncation");
    ck(hct_vfs_close(a), "close reopened main"); a = nullptr;
    ck(hct_vfs_close(b), "close reopened pagemap"); b = nullptr;
    ck(sqlite_murmur_unregister(), "unregister before second cold reopen");
    ck(sqlite_murmur_register(&config), "register for second cold reopen");
    ck(hct_vfs_open("sqlite-murmur", main.c_str(), 0, &a), "second reopen main");
    ck(hct_vfs_open("sqlite-murmur", pagemap.c_str(), 0, &b), "second reopen pagemap");
    ck(hct_vfs_size(b, &size), "truncated pagemap size after cold restart");
    if (size != 2048) throw std::runtime_error("pagemap truncation did not persist");
    page.fill(0);
    ck(hct_vfs_read(a, page.data(), static_cast<int>(page.size()), 0), "main read after second cold restart");
    if (std::memcmp(page.data(), "hctree", 6))
      throw std::runtime_error("main changed during pagemap truncation");
    ck(hct_vfs_close(a), "close second reopened main"); a = nullptr;
    ck(hct_vfs_close(b), "close second reopened pagemap"); b = nullptr;
    ck(sqlite_murmur_unregister(), "unregister");
    fs::remove_all(root);
    std::cout << "HCT_MURMUR_BRIDGE_PASS,cold-reopen-main-and-pagemap,truncation,dirty-window,overlapping-windows\n";
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "HCT_MURMUR_BRIDGE_FAIL," << e.what() << "\n";
    if (a) hct_vfs_close(a);
    if (b) hct_vfs_close(b);
    sqlite_murmur_unregister();
    fs::remove_all(root);
    return 1;
  }
}
