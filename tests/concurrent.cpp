#include "sqlite_murmur.h"

#include <sqlite3.h>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

static bool execute(sqlite3 *db, const std::string &sql) {
    return sqlite3_exec(db, sql.c_str(), nullptr, nullptr, nullptr) == SQLITE_OK;
}

int main() {
    unsigned char key[32];
    for (unsigned i = 0; i != 32; ++i) key[i] = static_cast<unsigned char>(42 + i);
    const auto id = std::chrono::steady_clock::now().time_since_epoch().count();
    const std::string path = (std::filesystem::temp_directory_path() /
        ("sqlite-murmur-concurrent-" + std::to_string(id))).string();
    sqlite_murmur_config opts{key, 25, 64, 1};
    if (sqlite_murmur_register(&opts) != SQLITE_OK) return 1;
    auto open = [&] {
        sqlite3 *db = nullptr;
        const int rc = sqlite3_open_v2(path.c_str(), &db,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
            "sqlite-murmur");
        if (rc != SQLITE_OK) {
            if (db) sqlite3_close(db);
            return static_cast<sqlite3 *>(nullptr);
        }
        sqlite3_busy_timeout(db, 10000);
        return db;
    };
    sqlite3 *mainDb = open();
    if (!mainDb || !execute(mainDb, "CREATE TABLE tally(id INTEGER PRIMARY KEY, value TEXT)")) {
        std::cerr << "setup failed\n";
        return 1;
    }
    constexpr int kThreads = 4;
    constexpr int kInserts = 100;
    std::atomic<int> failures{0};
    std::vector<std::thread> workers;
    for (int tid = 0; tid < kThreads; ++tid) {
        workers.emplace_back([&, tid] {
            sqlite3 *db = open();
            if (!db) { ++failures; return; }
            for (int i = 0; i < kInserts; ++i) {
                const std::string sql = "INSERT INTO tally VALUES(" +
                    std::to_string(tid * kInserts + i + 1) + ",'x')";
                bool done = false;
                for (int attempt = 0; attempt < 100 && !done; ++attempt) {
                    done = execute(db, sql);
                    if (!done) std::this_thread::sleep_for(std::chrono::milliseconds(2));
                }
                if (!done) { ++failures; break; }
            }
            if (sqlite3_close(db) != SQLITE_OK) ++failures;
        });
    }
    // Change storage tuning while other connections and the flush worker write.
    for (int i = 0; i < 6; ++i) {
        if (!execute(mainDb, "PRAGMA rocksdb_sync=" + std::to_string(i % 2)) ||
            !execute(mainDb, "PRAGMA rocksdb_block_cache_bytes=" + std::to_string((i % 2) * 1048576)) ||
            !execute(mainDb, "PRAGMA rocksdb_write_buffer_size=1048576") ||
            !execute(mainDb, "PRAGMA rocksdb_max_write_buffer_number=3") ||
            !execute(mainDb, "PRAGMA rocksdb_max_background_jobs=3")) ++failures;
        sqlite_murmur_stats snapshot{};
        if (sqlite_murmur_get_stats(path.c_str(), &snapshot) != SQLITE_OK) ++failures;
    }
    for (auto &t : workers) t.join();
    if (failures != 0) {
        std::cerr << "parallel inserts failed: " << failures << '\n';
        return 1;
    }
    sqlite3_stmt *stmt = nullptr;
    if (sqlite3_prepare_v2(mainDb, "SELECT COUNT(*) FROM tally", -1,
                          &stmt, nullptr) != SQLITE_OK) return 1;
    int count = sqlite3_step(stmt) == SQLITE_ROW ? sqlite3_column_int(stmt, 0) : -1;
    sqlite3_finalize(stmt);
    if (count != kThreads * kInserts) {
        std::cerr << "parallel writes lost: " << count << '\n';
        return 1;
    }
    if (sqlite_murmur_flush(path.c_str()) != SQLITE_OK) return 1;
    sqlite3_close(mainDb);
    mainDb = open();
    if (!mainDb) return 1;
    stmt = nullptr;
    if (sqlite3_prepare_v2(mainDb, "SELECT COUNT(*) FROM tally", -1,
                          &stmt, nullptr) != SQLITE_OK) return 1;
    count = sqlite3_step(stmt) == SQLITE_ROW ? sqlite3_column_int(stmt, 0) : -1;
    sqlite3_finalize(stmt);
    sqlite3_close(mainDb);
    if (sqlite_murmur_unregister() != SQLITE_OK) return 1;
    std::filesystem::remove_all(path);
    if (count != kThreads * kInserts) {
        std::cerr << "reopened writes lost: " << count << '\n';
        return 1;
    }
    std::cout << "Concurrent SQLite handles: " << kThreads
              << "; durable rows: " << count << '\n';
    return 0;
}
