// Storage concurrency through the public C API: parallel writers with
// disjoint ranges, readers observing only committed full-row states,
// tuning changes during writes, rollback correctness, VACUUM (truncate
// paths), stats monotonicity, and reopen durability — per cipher. The
// previous white-box suite (#including src/sqlite_murmur.cpp for lock
// hooks, LRU/pool internals, and fault injection) moved into the Rust
// crate's own tests with the storage migration; this file keeps the
// behavior those hooks protected observable from outside: no lost or
// torn writes under concurrency, and persistence across reopen.

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

namespace {

namespace fs = std::filesystem;

void require(bool ok, const std::string &msg) {
    if (!ok) throw std::runtime_error("storage test: " + msg);
}

bool exec_once(sqlite3 *db, const std::string &sql) {
    return sqlite3_exec(db, sql.c_str(), nullptr, nullptr, nullptr) ==
           SQLITE_OK;
}

// SQLITE_BUSY under writer contention is expected: retry, fail only when
// the statement never goes through.
bool exec_retry(sqlite3 *db, const std::string &sql, int attempts = 200) {
    for (int i = 0; i < attempts; ++i) {
        if (exec_once(db, sql)) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return false;
}

int scalar_int(sqlite3 *db, const std::string &sql) {
    sqlite3_stmt *stmt = nullptr;
    require(sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) ==
                SQLITE_OK,
            "prepare " + sql);
    int value = -1;
    if (sqlite3_step(stmt) == SQLITE_ROW) value = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    return value;
}

sqlite3 *open_db(const std::string &path) {
    sqlite3 *db = nullptr;
    const int rc = sqlite3_open_v2(path.c_str(), &db,
                                   SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
                                       SQLITE_OPEN_FULLMUTEX,
                                   "sqlite-murmur");
    if (rc != SQLITE_OK) {
        if (db) sqlite3_close(db);
        return nullptr;
    }
    sqlite3_busy_timeout(db, 10000);
    return db;
}

void use_cipher(sqlite3 *db, const std::string &cipher) {
    require(exec_once(db, "PRAGMA cipher='" + cipher + "'"),
            "select " + cipher);
}

// N writers x M rows, disjoint id ranges, while the main connection
// retunes the store (worker count, RocksDB knobs) and polls stats.
// Verifies the exact row count live and after flush + reopen.
void parallel_writers(const fs::path &root, const std::string &cipher,
                      int threads, int rows) {
    const std::string path =
        (root / ("pw-" + cipher + "-t" + std::to_string(threads))).string();
    sqlite3 *main = open_db(path);
    require(main != nullptr, "open " + cipher);
    use_cipher(main, cipher);
    require(exec_once(main, "CREATE TABLE tally(id INTEGER PRIMARY KEY, v "
                            "TEXT)"),
            "create tally");
    std::atomic<int> failures{0};
    std::vector<std::thread> workers;
    for (int tid = 0; tid < threads; ++tid) {
        workers.emplace_back([&, tid] {
            sqlite3 *db = open_db(path);
            if (!db) {
                ++failures;
                return;
            }
            for (int i = 0; i < rows; ++i) {
                const std::string sql =
                    "INSERT INTO tally VALUES(" +
                    std::to_string(tid * rows + i + 1) + ",'x')";
                if (!exec_retry(db, sql)) {
                    ++failures;
                    break;
                }
            }
            if (sqlite3_close(db) != SQLITE_OK) ++failures;
        });
    }
    const char *tunables[] = {"PRAGMA background_threads=1",
                              "PRAGMA background_threads=8",
                              "PRAGMA rocksdb_sync=1",
                              "PRAGMA rocksdb_sync=0",
                              "PRAGMA rocksdb_max_background_jobs=3",
                              "PRAGMA compression_level=6",
                              "PRAGMA compression_level=3"};
    for (const char *pragma : tunables) {
        if (!exec_retry(main, pragma, 50)) ++failures;
        sqlite_murmur_stats snapshot{};
        if (sqlite_murmur_get_stats(path.c_str(), &snapshot) != SQLITE_OK)
            ++failures;
    }
    for (auto &t : workers) t.join();
    require(failures == 0, "parallel inserts failed for " + cipher);
    const int want = threads * rows;
    require(scalar_int(main, "SELECT COUNT(*) FROM tally") == want,
            "parallel writes lost for " + cipher);
    require(sqlite_murmur_flush(path.c_str()) == SQLITE_OK,
            "flush " + cipher);
    require(sqlite3_close(main) == SQLITE_OK, "close " + cipher);
    main = open_db(path);
    require(main != nullptr, "reopen " + cipher);
    require(scalar_int(main, "SELECT COUNT(*) FROM tally") == want,
            "reopened writes lost for " + cipher);
    require(scalar_int(main, "SELECT COUNT(*) FROM tally WHERE v != 'x'") ==
                0,
            "corrupt values for " + cipher);
    require(sqlite3_close(main) == SQLITE_OK, "close2 " + cipher);
}

// Writers flip (a, b) pairs between (k, k) states; readers scanning the
// table must only ever see a == b rows (no torn multi-column writes).
void read_during_write(const fs::path &root, const std::string &cipher) {
    const std::string path = (root / ("rw-" + cipher)).string();
    sqlite3 *main = open_db(path);
    require(main != nullptr, "open rw " + cipher);
    use_cipher(main, cipher);
    require(exec_once(main, "CREATE TABLE pair(id INTEGER PRIMARY KEY, a "
                            "INTEGER, b INTEGER)"),
            "create pair");
    require(exec_once(main, "INSERT INTO pair SELECT value, value, value FROM "
                            "json_each('"
                            "[0,1,2,3,4,5,6,7,8,9]')") ||
                exec_once(main, "WITH RECURSIVE c(x) AS (SELECT 0 UNION ALL "
                                "SELECT x+1 FROM c WHERE x < 49) INSERT INTO "
                                "pair SELECT x, x, x FROM c"),
            "seed pair");
    std::atomic<bool> stop{false};
    std::atomic<int> torn{0};
    std::atomic<int> failures{0};
    std::thread writer([&] {
        sqlite3 *db = open_db(path);
        if (!db) {
            ++failures;
            return;
        }
        for (int round = 0; round < 40 && !failures; ++round) {
            if (!exec_retry(db, "UPDATE pair SET a = a + 1000, b = b + 1000"))
                ++failures;
        }
        if (sqlite3_close(db) != SQLITE_OK) ++failures;
    });
    std::vector<std::thread> readers;
    for (int r = 0; r < 3; ++r) {
        readers.emplace_back([&] {
            sqlite3 *db = open_db(path);
            if (!db) {
                ++failures;
                return;
            }
            while (!stop) {
                sqlite3_stmt *stmt = nullptr;
                if (sqlite3_prepare_v2(db, "SELECT a, b FROM pair", -1, &stmt,
                                       nullptr) != SQLITE_OK) {
                    ++failures;
                    break;
                }
                int rc;
                while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
                    if (sqlite3_column_int(stmt, 0) !=
                        sqlite3_column_int(stmt, 1))
                        ++torn;
                }
                sqlite3_finalize(stmt);
                if (rc != SQLITE_DONE) ++failures;
            }
            if (sqlite3_close(db) != SQLITE_OK) ++failures;
        });
    }
    writer.join();
    stop = true;
    for (auto &t : readers) t.join();
    require(failures == 0, "read/write race failed for " + cipher);
    require(torn == 0, "torn row observed for " + cipher);
    require(scalar_int(main, "SELECT COUNT(*) FROM pair WHERE a != b") == 0,
            "torn row persisted for " + cipher);
    require(sqlite_murmur_flush(path.c_str()) == SQLITE_OK,
            "flush rw " + cipher);
    require(sqlite3_close(main) == SQLITE_OK, "close rw " + cipher);
}

// One large transaction rolls back cleanly, then commits; VACUUM (full
// VFS rewrite with truncate/extend paths) preserves every row.
void rollback_and_vacuum(const fs::path &root, const std::string &cipher) {
    const std::string path = (root / ("rv-" + cipher)).string();
    sqlite3 *db = open_db(path);
    require(db != nullptr, "open rv " + cipher);
    use_cipher(db, cipher);
    require(exec_once(db, "CREATE TABLE bulk(id INTEGER PRIMARY KEY, v TEXT)"),
            "create bulk");
    require(exec_once(db, "BEGIN"), "begin");
    for (int i = 0; i < 2000; ++i) {
        require(exec_once(db, "INSERT INTO bulk VALUES(" + std::to_string(i) +
                                 ",'" + std::string(200, 'x') + "')"),
                "bulk insert");
    }
    require(exec_once(db, "ROLLBACK"), "rollback");
    require(scalar_int(db, "SELECT COUNT(*) FROM bulk") == 0,
            "rollback leaked rows for " + cipher);
    require(exec_once(db, "BEGIN"), "begin2");
    for (int i = 0; i < 2000; ++i) {
        require(exec_once(db, "INSERT INTO bulk VALUES(" + std::to_string(i) +
                                 ",'" + std::string(200, 'y') + "')"),
                "bulk insert2");
    }
    require(exec_once(db, "COMMIT"), "commit");
    require(scalar_int(db, "SELECT COUNT(*) FROM bulk") == 2000,
            "commit lost rows for " + cipher);
    require(exec_once(db, "DELETE FROM bulk WHERE id % 2 = 0"), "delete half");
    require(exec_once(db, "VACUUM"), "vacuum");
    require(scalar_int(db, "SELECT COUNT(*) FROM bulk") == 1000,
            "vacuum lost rows for " + cipher);
    require(scalar_int(db, "SELECT COUNT(*) FROM bulk WHERE v != '" +
                               std::string(200, 'y') + "'") == 0,
            "vacuum corrupted rows for " + cipher);
    require(sqlite_murmur_flush(path.c_str()) == SQLITE_OK,
            "flush rv " + cipher);
    require(sqlite3_close(db) == SQLITE_OK, "close rv " + cipher);
    db = open_db(path);
    require(db != nullptr, "reopen rv " + cipher);
    require(scalar_int(db, "SELECT COUNT(*) FROM bulk") == 1000,
            "reopen lost vacuumed rows for " + cipher);
    require(sqlite3_close(db) == SQLITE_OK, "close rv2 " + cipher);
}

// Stats move monotonically: batches and persisted blocks grow across
// writes + flushes, and the logical size tracks the data.
void stats_monotonic(const fs::path &root, const std::string &cipher) {
    const std::string path = (root / ("st-" + cipher)).string();
    sqlite3 *db = open_db(path);
    require(db != nullptr, "open st " + cipher);
    use_cipher(db, cipher);
    require(exec_once(db, "CREATE TABLE s(id INTEGER PRIMARY KEY, v TEXT)"),
            "create s");
    sqlite_murmur_stats before{};
    require(sqlite_murmur_get_stats(path.c_str(), &before) == SQLITE_OK,
            "stats before");
    for (int i = 0; i < 100; ++i) {
        require(exec_once(db, "INSERT INTO s VALUES(" + std::to_string(i) +
                                 ",'" + std::string(500, 'z') + "')"),
                "stats insert");
    }
    require(sqlite_murmur_flush(path.c_str()) == SQLITE_OK, "stats flush");
    sqlite_murmur_stats after{};
    require(sqlite_murmur_get_stats(path.c_str(), &after) == SQLITE_OK,
            "stats after");
    require(after.flush_batches > before.flush_batches, "batches not growing");
    require(after.persisted_blocks > before.persisted_blocks,
            "persisted blocks not growing");
    require(after.logical_size_bytes > before.logical_size_bytes,
            "logical size not growing");
    require(after.encrypted_record_bytes > 0, "no encrypted bytes reported");
    require(scalar_int(db, "SELECT COUNT(*) FROM s") == 100, "stats lost rows");
    require(sqlite3_close(db) == SQLITE_OK, "close st " + cipher);
}

} // namespace

int main() {
    unsigned char key[32];
    for (unsigned i = 0; i < 32; ++i)
        key[i] = static_cast<unsigned char>(i + 17);
    const auto id =
        std::chrono::steady_clock::now().time_since_epoch().count();
    const fs::path root =
        fs::temp_directory_path() / ("sqlite-murmur-storage-" +
                                     std::to_string(id));
    std::error_code ec;
    fs::create_directories(root, ec);
    require(!ec, "create scratch dir");
    try {
        sqlite_murmur_config opts{key, 60000, 32, 1};
        require(sqlite_murmur_register(&opts) == SQLITE_OK, "register VFS");
        std::vector<std::string> ciphers{"aes256gcm"};
#ifdef SQLITE_MURMUR_HAS_AEGIS256
        ciphers.push_back("aegis256");
#endif
        for (const auto &cipher : ciphers) {
            for (int threads : {1, 4, 8})
                parallel_writers(root, cipher, threads, 50);
            read_during_write(root, cipher);
            rollback_and_vacuum(root, cipher);
            stats_monotonic(root, cipher);
        }
        require(sqlite_murmur_unregister() == SQLITE_OK, "unregister VFS");
        fs::remove_all(root, ec);
        std::cout << "Storage concurrency: parallel writers, read/write "
                     "races, rollback/vacuum, stats monotonicity passed\n";
        return 0;
    } catch (const std::exception &e) {
        std::cerr << e.what() << "\nscratch kept at " << root << '\n';
        return 1;
    }
}