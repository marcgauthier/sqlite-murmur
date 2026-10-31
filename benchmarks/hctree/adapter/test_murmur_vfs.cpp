// Real Murmur/RocksDB bridge test; NOT an integrated HC-tree mmap benchmark.
#include "hct_murmur_vfs_io.h"
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
    sqlite_murmur_stats s1{}, s2{};
    ck(sqlite_murmur_get_stats(main.c_str(), &s1), "main stats");
    ck(sqlite_murmur_get_stats(pagemap.c_str(), &s2), "pagemap stats");
    if (!s1.persisted_blocks || !s2.persisted_blocks)
      throw std::runtime_error("both stores must persist RocksDB blocks");
    ck(hct_vfs_close(a), "close main"); a = nullptr;
    ck(hct_vfs_close(b), "close pagemap"); b = nullptr;
    if (!fs::is_directory(main) || !fs::is_directory(pagemap))
      throw std::runtime_error("expected RocksDB directories, not POSIX files");
    ck(hct_vfs_open("sqlite-murmur", main.c_str(), 0, &a), "reopen main");
    ck(hct_vfs_open("sqlite-murmur", pagemap.c_str(), 0, &b), "reopen pagemap");
    page.fill(0);
    ck(hct_vfs_read(a, page.data(), static_cast<int>(page.size()), 0), "read main");
    if (std::memcmp(page.data(), "hctree", 6)) throw std::runtime_error("main mismatch");
    page.fill(0);
    ck(hct_vfs_read(b, page.data(), static_cast<int>(page.size()), 0), "read pagemap");
    if (std::memcmp(page.data(), "page-map", 8)) throw std::runtime_error("pagemap mismatch");
    ck(hct_vfs_close(a), "close reopened main"); a = nullptr;
    ck(hct_vfs_close(b), "close reopened pagemap"); b = nullptr;
    ck(sqlite_murmur_unregister(), "unregister");
    fs::remove_all(root);
    std::cout << "HCT_MURMUR_BRIDGE_PASS,persistent-main-and-pagemap\n";
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
