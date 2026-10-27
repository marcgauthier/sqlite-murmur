// SQLite Express standalone microbenchmarks. No other database is benchmarked.
// All workers share the same SQLite Express RocksDB-backed database.
#include "sqlite_express.h"
#include <sqlite3.h>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {
using Clock = std::chrono::steady_clock;
std::atomic<uint64_t> nextId{1000000};
constexpr int kSeedRows = 50000;
constexpr const char *kPayload = "sqlite-express-benchmark-payload-abcdefghijklmnopqrstuvwxyz-0123456789";
std::string path;

void check(int rc, sqlite3 *db, const char *where) {
    if (rc != SQLITE_OK) throw std::runtime_error(std::string(where) + ": " +
        (db ? sqlite3_errmsg(db) : sqlite3_errstr(rc)) + " (" + std::to_string(rc) + ")");
}

sqlite3 *openDB() {
    sqlite3 *db = nullptr;
    int rc = sqlite3_open_v2(path.c_str(), &db,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, "sqlite-express");
    if (rc != SQLITE_OK) {
        std::string what = db ? sqlite3_errmsg(db) : sqlite3_errstr(rc);
        if (db) sqlite3_close(db);
        throw std::runtime_error("open: " + what);
    }
    sqlite3_busy_timeout(db, 15000);
    return db;
}

void exec(sqlite3 *db, const char *sql) {
    char *error = nullptr;
    const int rc = sqlite3_exec(db, sql, nullptr, nullptr, &error);
    if (rc != SQLITE_OK) {
        std::string message = error ? error : sqlite3_errmsg(db);
        sqlite3_free(error);
        throw std::runtime_error(std::string(sql) + ": " + message);
    }
}

class Connection {
public:
    sqlite3 *db = openDB();
    ~Connection() { if (db) sqlite3_close(db); }
    Connection(const Connection &) = delete;
    Connection &operator=(const Connection &) = delete;
    Connection() = default;
};

class Statement {
public:
    sqlite3_stmt *stmt = nullptr;
    sqlite3 *db = nullptr;
    Statement(sqlite3 *conn, const char *sql) : db(conn) {
        check(sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr), db, "prepare");
    }
    ~Statement() { sqlite3_finalize(stmt); }
    void reset() {
        (void)sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);
    }
    void insert(uint64_t id) {
        for (int attempt = 0; attempt < 500; ++attempt) {
            reset();
            check(sqlite3_bind_int64(stmt, 1, static_cast<sqlite3_int64>(id)), db, "bind id");
            check(sqlite3_bind_text(stmt, 2, kPayload, -1, SQLITE_STATIC), db, "bind payload");
            const int rc = sqlite3_step(stmt);
            if (rc == SQLITE_DONE) return;
            if ((rc == SQLITE_BUSY || rc == SQLITE_LOCKED) && attempt < 499) {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
                continue;
            }
            throw std::runtime_error(std::string("insert: ") + sqlite3_errmsg(db)
                                     + " rc=" + std::to_string(rc));
        }
    }
    void select(uint64_t id) {
        for (int attempt = 0; attempt < 500; ++attempt) {
            reset();
            check(sqlite3_bind_int64(stmt, 1, static_cast<sqlite3_int64>(id)), db, "bind PK");
            const int rc = sqlite3_step(stmt);
            if (rc == SQLITE_ROW) {
                // Check that the result was actually materialized.
                if (sqlite3_column_bytes(stmt, 0) <= 0)
                    throw std::runtime_error("empty PK result");
                // Release the SHARED read lock before the next operation.
                sqlite3_reset(stmt);
                return;
            }
            if ((rc == SQLITE_BUSY || rc == SQLITE_LOCKED) && attempt < 499) {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
                continue;
            }
            throw std::runtime_error(std::string("select PK: ") + sqlite3_errmsg(db)
                                     + " rc=" + std::to_string(rc));
        }
    }
};

uint64_t rnd(uint64_t &x) {
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    return x * 2685821657736338717ULL;
}

void populate() {
    Connection db;
    exec(db.db, "PRAGMA journal_mode=DELETE");
    exec(db.db, "PRAGMA mmap_size=0");
    exec(db.db, "CREATE TABLE records(id INTEGER PRIMARY KEY, body TEXT NOT NULL)");
    Statement insert(db.db, "INSERT INTO records(id,body) VALUES(?1,?2)");
    exec(db.db, "BEGIN IMMEDIATE");
    for (int i = 1; i <= kSeedRows; ++i) insert.insert(i);
    exec(db.db, "COMMIT");
    check(sqlite_express_flush(path.c_str()), db.db, "initial persistence");
}

struct Result {
    std::string name;
    int threads;
    uint64_t operations;
    double seconds;
    double opsPerSecond() const { return operations / seconds; }
};

template<class Function>
Result run(const char *name, int threads, int total, Function fn) {
    std::vector<std::thread> workers;
    std::vector<std::exception_ptr> errors(threads);
    std::atomic<bool> go{false};
    std::atomic<int> ready{0};
    const auto startSetup = Clock::now();
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([&, t] {
            try {
                Connection db;
                Statement ins(db.db, "INSERT INTO records(id,body) VALUES(?1,?2)");
                Statement sel(db.db, "SELECT body FROM records WHERE id=?1");
                ++ready;
                while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
                const int first = total * t / threads;
                const int last = total * (t+1) / threads;
                fn(db.db, ins, sel, t, first, last);
            } catch (...) {
                errors[t] = std::current_exception();
                ++ready; // release the coordinator if initialization failed
            }
        });
    }
    while (ready < threads) std::this_thread::yield();
    (void)startSetup;
    auto started = Clock::now();
    go.store(true, std::memory_order_release);
    for (auto &thread : workers) thread.join();
    auto finished = Clock::now();
    for (const auto &error : errors) if (error) std::rethrow_exception(error);
    return {name, threads, static_cast<uint64_t>(total),
            std::chrono::duration<double>(finished - started).count()};
}

int countRows() {
    Connection db;
    sqlite3_stmt *stmt = nullptr;
    check(sqlite3_prepare_v2(db.db, "SELECT COUNT(*) FROM records", -1, &stmt, nullptr),
          db.db, "count prepare");
    if (sqlite3_step(stmt) != SQLITE_ROW) {
        sqlite3_finalize(stmt);
        throw std::runtime_error("count query failed");
    }
    int n = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    return n;
}

void print(const Result &r) {
    std::cout << std::fixed << std::setprecision(3)
              << "BENCH_CSV," << r.name << ',' << r.threads << ','
              << r.operations << ',' << r.seconds << ','
              << std::setprecision(1) << r.opsPerSecond() << std::endl;
}
} // namespace

int main(int argc, char **argv) {
    // --scale=N multiplies workload size; defaults to 1 for CI.
    int scale = 1;
    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);
        if (arg.rfind("--scale=", 0) == 0) scale = std::stoi(arg.substr(8));
    }
    if (scale < 1 || scale > 20) return 2;

    const auto stamp = Clock::now().time_since_epoch().count();
    path = (std::filesystem::temp_directory_path() /
        ("sqlite-express-bench-" + std::to_string(stamp))).string();
    unsigned char key[32]{};
    for (int i = 0; i < 32; ++i) key[i] = static_cast<unsigned char>(i + 11);
    sqlite_express_config config{};
    config.key = key;
    config.flush_interval_ms = 250;
    config.cache_pages = 16384;
    config.rocksdb_sync = 1;
    try {
        check(sqlite_express_register(&config), nullptr, "register VFS");
        populate();
        std::cout << "BENCH_INFO,db=sqlite-express,connections=1/4/16,"
                  << "flush_interval_ms=250,cache_pages=16384,"
                  << "zstd_level=3,cipher=AES-256-GCM,rocksdb_sync=1,"
                  << "row_payload_bytes=" << std::char_traits<char>::length(kPayload)
                  << ",seed_rows=" << kSeedRows << ",scale=" << scale << std::endl;
        std::cout << "BENCH_HEADER,workload,connections,operations,seconds,ops_per_second"
                  << std::endl;
        const int insertOps = 3000 * scale;
        const int batchOps = 10000 * scale;
        const int queryOps = 30000 * scale;
        const int mixedOps = 12000 * scale;
        uint64_t expectedCount = kSeedRows;
        for (int threads : {1, 4, 16}) {
            const auto a = run("insert_autocommit", threads, insertOps,
                [](sqlite3 *, Statement &ins, Statement &, int, int begin, int end) {
                    // Each INSERT is one transaction.
                    const auto first = nextId.fetch_add(end - begin);
                    for (int n = begin; n < end; ++n) ins.insert(first + (n - begin));
                });
            expectedCount += insertOps;
            print(a);

            const auto b = run("insert_batch100", threads, batchOps,
                [](sqlite3 *db, Statement &ins, Statement &, int, int begin, int end) {
                    const auto first = nextId.fetch_add(end - begin);
                    for (int pos = begin; pos < end;) {
                        exec(db, "BEGIN IMMEDIATE");
                        const int stop = std::min(end, pos + 100);
                        for (int n = pos; n < stop; ++n) ins.insert(first + (n - begin));
                        exec(db, "COMMIT");
                        pos = stop;
                    }
                });
            expectedCount += batchOps;
            print(b);

            const auto c = run("select_primary_key", threads, queryOps,
                [](sqlite3 *, Statement &, Statement &sel, int tid, int begin, int end) {
                    uint64_t rng = (1234 + static_cast<uint64_t>(tid)) * 0x9e3779b97f4a7c15ULL;
                    for (int n = begin; n < end; ++n)
                        sel.select(1 + rnd(rng) % kSeedRows);
                });
            print(c);

            const auto d = run("mixed_80read_20write", threads, mixedOps,
                [](sqlite3 *, Statement &ins, Statement &sel, int tid, int begin, int end) {
                    uint64_t rng = (991 + static_cast<uint64_t>(tid)) * 0x9e3779b97f4a7c15ULL;
                    // Exactly 20% INSERT and 80% PK SELECT, spread over threads.
                    const uint64_t first = nextId.fetch_add((end+4)/5 - (begin+4)/5);
                    uint64_t localWritten = 0;
                    for (int n = begin; n < end; ++n) {
                        if (n % 5 == 0) ins.insert(first + localWritten++);
                        else sel.select(1 + rnd(rng) % kSeedRows);
                    }
                });
            const uint64_t mixedWrites = (mixedOps + 4) / 5;
            expectedCount += mixedWrites;
            print(d);

            if (countRows() != static_cast<int>(expectedCount))
                throw std::runtime_error("row count mismatch; incomplete operations");
        }
        // Persistence isn't timed. Confirm on-disk rows survive a reopen.
        check(sqlite_express_flush(path.c_str()), nullptr, "final flush");
        if (countRows() != static_cast<int>(expectedCount))
            throw std::runtime_error("reopen row count mismatch");
        check(sqlite_express_unregister(), nullptr, "unregister VFS");
        std::filesystem::remove_all(path);
        std::cout << "BENCH_DONE,expected_rows=" << expectedCount << std::endl;
        return 0;
    } catch (const std::exception &e) {
        std::cerr << "BENCH_ERROR," << e.what() << std::endl;
        // Do not delete the directory on failure: useful for CI diagnostics.
        return 1;
    }
}
