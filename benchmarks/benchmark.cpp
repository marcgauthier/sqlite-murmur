// A single configurable full-speed run: 80% reads and 20% inserts.
#include "sqlite_express.h"
#include <chrono>
#include <cmath>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>
#include <limits>
#include <stdexcept>
#include <string>

#ifdef SQLITE_EXPRESS_HAS_CRSQLITE
#include "sqlite_express_crsqlite.h"
#endif
namespace {
using Clock = std::chrono::steady_clock;
struct CloseDatabase {
    bool replication = false;
    void operator()(sqlite3 *db) const {
        if (replication) sqlite3_exec(db, "SELECT crsql_finalize()", nullptr, nullptr, nullptr);
        sqlite3_close(db);
    }
};
using Database = std::unique_ptr<sqlite3, CloseDatabase>;
void enableReplication(Database &db, bool enabled) {
    if (!enabled) return;
#ifdef SQLITE_EXPRESS_HAS_CRSQLITE
    char *error = nullptr;
    int rc = sqlite3_crsqlite_init(db.get(), &error, nullptr);
    std::string message = error ? error : sqlite3_errmsg(db.get());
    sqlite3_free(error);
    if (rc != SQLITE_OK) throw std::runtime_error("initialize cr-sqlite: " + message);
    db.get_deleter().replication = true;
#else
    throw std::runtime_error("replication requires SQLITE_EXPRESS_ENABLE_CRSQLITE=ON");
#endif
}
constexpr uint64_t kSeedRows = 50000;
constexpr char kPayload[] = "sqlite-express-benchmark-payload-abcdefghijklmnopqrstuvwxyz-0123456789";

void check(int rc, sqlite3 *db, const char *operation) {
    if (rc != SQLITE_OK)
        throw std::runtime_error(std::string(operation) + ": " + (db ? sqlite3_errmsg(db) : sqlite3_errstr(rc)));
}
Database open(const std::string &path, unsigned sqliteCacheKiB) {
    sqlite3 *raw = nullptr;
    const int rc = sqlite3_open_v2(path.c_str(), &raw,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, "sqlite-express");
    Database db(raw);
    check(rc, db.get(), "open database");
    const auto cachePragma = "PRAGMA cache_size=-" + std::to_string(sqliteCacheKiB);
    check(sqlite3_exec(db.get(), cachePragma.c_str(), nullptr, nullptr, nullptr),
          db.get(), "set SQLite cache_size");
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
class Statement {
    sqlite3 *db;
    std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> statement{nullptr, sqlite3_finalize};
public:
    Statement(sqlite3 *connection, const char *sql) : db(connection) {
        sqlite3_stmt *raw = nullptr;
        const int rc = sqlite3_prepare_v2(db, sql, -1, &raw, nullptr);
        statement.reset(raw);
        check(rc, db, "prepare");
    }
    sqlite3_stmt *get() { return statement.get(); }
    void reset() { check(sqlite3_reset(get()), db, "reset statement"); }
    void insert(uint64_t id) {
        reset();
        check(sqlite3_bind_int64(get(), 1, static_cast<sqlite3_int64>(id)), db, "bind ID");
        check(sqlite3_bind_text(get(), 2, kPayload, sizeof(kPayload) - 1, SQLITE_STATIC), db, "bind body");
        if (sqlite3_step(get()) != SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(db));
    }
    void read(uint64_t id) {
        reset();
        check(sqlite3_bind_int64(get(), 1, static_cast<sqlite3_int64>(id)), db, "bind ID");
        if (sqlite3_step(get()) != SQLITE_ROW) throw std::runtime_error(sqlite3_errmsg(db));
        const auto *body = sqlite3_column_text(get(), 0);
        if (!body || sqlite3_column_bytes(get(), 0) != sizeof(kPayload) - 1 ||
            std::memcmp(body, kPayload, sizeof(kPayload) - 1) != 0)
            throw std::runtime_error("incorrect read payload");
        reset(); // Release SQLite's read lock before the next autocommit insert.
    }
};
uint64_t randomID(uint64_t &state, uint64_t rows) {
    state ^= state >> 12; state ^= state << 25; state ^= state >> 27;
    return 1 + (state * 2685821657736338717ULL) % rows;
}
int64_t scalar(sqlite3 *db, const char *sql) {
    Statement statement(db, sql);
    if (sqlite3_step(statement.get()) != SQLITE_ROW) throw std::runtime_error(sqlite3_errmsg(db));
    return sqlite3_column_int64(statement.get(), 0);
}
void verify(const std::string &path, uint64_t rows, unsigned sqliteCacheKiB, bool replication) {
    auto db = open(path, sqliteCacheKiB);
    enableReplication(db, replication);
    if (replication && scalar(db.get(), "SELECT count(*) FROM crsql_changes WHERE \"table\"='records' AND cid='body'") != rows)
        throw std::runtime_error("reopened replication change count mismatch");
    if (scalar(db.get(), "SELECT count(*) FROM records") != rows)
        throw std::runtime_error("reopened row count mismatch");
    Statement integrity(db.get(), "PRAGMA integrity_check");
    if (sqlite3_step(integrity.get()) != SQLITE_ROW ||
        std::strcmp(reinterpret_cast<const char *>(sqlite3_column_text(integrity.get(), 0)), "ok") != 0)
        throw std::runtime_error("reopened integrity check failed");
}
} // namespace

int main(int argc, char **argv) {
    double duration = 60;
    unsigned flushInterval = 100;
    size_t cachePages = 16384;
    unsigned sqliteCacheKiB = 1048576;
    unsigned pageSize = 4096;
    std::string cipher = "aegis256";
    std::string compression = "zstd3";
    unsigned compressionLevel = 3;
    unsigned batchSize = 1;
    unsigned backgroundThreads = 0;
    bool replication = false;
    const auto number = [](const std::string &value, uint64_t maximum) {
        uint64_t parsed = 0;
        if (value.empty()) throw std::runtime_error("missing numeric value");
        for (char c : value) {
            if (c < '0' || c > '9' || parsed > maximum / 10 ||
                (parsed == maximum / 10 && static_cast<unsigned>(c - '0') > maximum % 10))
                throw std::runtime_error("invalid numeric value: " + value);
            parsed = parsed * 10 + c - '0';
        }
        if (parsed == 0) throw std::runtime_error("numeric value must be positive");
        return parsed;
    };
    try {
        for (int i = 1; i < argc; ++i) {
            const std::string arg(argv[i]);
            if (arg == "--help") {
                std::cout << "Usage: benchmark [--seconds=60] [--flush-interval-ms=100] [--cache-pages=16384] [--sqlite-cache-kib=1048576]\n"
                          << "                 [--compression=zstd3|none|zstd1..zstd22] [--cipher=aegis256|aes256gcm] [--page-size=4096] [--batch-size=1] [--replication=off|on] [--background-threads=auto|1..256]\n";
                return 0;
            }
            if (arg.rfind("--seconds=", 0) == 0) {
                const auto value = arg.substr(10);
                size_t used = 0;
                duration = std::stod(value, &used);
                if (used != value.size() || !std::isfinite(duration) || duration <= 0 || duration > 3600)
                    throw std::runtime_error("seconds must be greater than zero and at most 3600");
            } else if (arg.rfind("--flush-interval-ms=", 0) == 0) {
                flushInterval = static_cast<unsigned>(number(arg.substr(20), std::numeric_limits<unsigned>::max()));
            } else if (arg.rfind("--sqlite-cache-kib=", 0) == 0) {
                const auto value = arg.substr(19);
                sqliteCacheKiB = value == "0" ? 0 : static_cast<unsigned>(number(value, 2147483647));
            } else if (arg.rfind("--cache-pages=", 0) == 0) {
                cachePages = static_cast<size_t>(number(arg.substr(14), std::numeric_limits<size_t>::max() / 4096));
            } else if (arg.rfind("--background-threads=", 0) == 0) {
                const auto value = arg.substr(21);
                backgroundThreads = value == "auto" ? 0 : static_cast<unsigned>(number(value, 256));
            } else if (arg.rfind("--replication=", 0) == 0) {
                const auto value = arg.substr(14);
                if (value != "on" && value != "off") throw std::runtime_error("replication must be on or off");
                replication = value == "on";
            } else if (arg.rfind("--batch-size=", 0) == 0) {
                batchSize = static_cast<unsigned>(number(arg.substr(13), 1000000));
            } else if (arg.rfind("--page-size=", 0) == 0) {
                pageSize = static_cast<unsigned>(number(arg.substr(12), 65536));
                if (pageSize < 512 || (pageSize & (pageSize - 1)) != 0)
                    throw std::runtime_error("page size must be a power of two from 512 through 65536");
            } else if (arg.rfind("--cipher=", 0) == 0) {
                cipher = arg.substr(9);
                if (cipher != "aegis256" && cipher != "aes256gcm") throw std::runtime_error("invalid cipher");
            } else if (arg.rfind("--compression=", 0) == 0) {
                compression = arg.substr(14);
                if (compression == "none") compressionLevel = 0;
                else if (compression.rfind("zstd", 0) == 0) {
                    compressionLevel = static_cast<unsigned>(number(compression.substr(4), 22));
                    compression = "zstd" + std::to_string(compressionLevel);
                } else throw std::runtime_error("compression must be none or zstd1 through zstd22");
            } else throw std::runtime_error("unknown argument: " + arg);
        }
#ifndef SQLITE_EXPRESS_HAS_CRSQLITE
        if (replication) throw std::runtime_error("replication requires SQLITE_EXPRESS_ENABLE_CRSQLITE=ON");
#endif
#ifndef SQLITE_EXPRESS_HAS_AEGIS256
        if (cipher == "aegis256") throw std::runtime_error("aegis256 requires SQLITE_EXPRESS_ENABLE_AEGIS256=ON; use --cipher=aes256gcm for AES only");
#endif
    } catch (const std::exception &error) {
        std::cerr << "BENCH_ERROR," << error.what() << '\n';
        return 2;
    }

    const std::string path = (std::filesystem::temp_directory_path() /
        ("sqlite-express-benchmark-" + std::to_string(Clock::now().time_since_epoch().count()))).string();
    try {
        unsigned char key[32];
        for (int i = 0; i < 32; ++i) key[i] = static_cast<unsigned char>(i + 11);
        sqlite_express_config config{key, flushInterval, cachePages, 1};
        check(sqlite_express_register(&config), nullptr, "register VFS");
        uint64_t writes = 0;
        uint64_t transactions = 0;
        uint64_t rocksdbCacheBytes = 0;
        uint64_t trackedChanges = 0;
        double seconds = 0, persistenceSeconds = 0, transactionSeconds = 0;
        sqlite_express_stats stats{};
        {
            auto db = open(path, sqliteCacheKiB);
            const std::string pragmas = "PRAGMA page_size=" + std::to_string(pageSize) +
                "; PRAGMA cipher='" + cipher + "'; PRAGMA compression='" + compression +
                "'; PRAGMA flush_interval_ms=" + std::to_string(flushInterval) +
                "; PRAGMA journal_mode=DELETE; PRAGMA mmap_size=0; PRAGMA cache_size=-" + std::to_string(sqliteCacheKiB) + ";";
            exec(db.get(), pragmas.c_str());
            if (backgroundThreads) exec(db.get(), ("PRAGMA background_threads=" + std::to_string(backgroundThreads)).c_str());
            backgroundThreads = static_cast<unsigned>(scalar(db.get(), "PRAGMA background_threads"));
            rocksdbCacheBytes = static_cast<uint64_t>(scalar(db.get(), "PRAGMA rocksdb_block_cache_bytes"));
            if (scalar(db.get(), "PRAGMA page_size") != pageSize ||
                scalar(db.get(), "PRAGMA flush_interval_ms") != flushInterval ||
                scalar(db.get(), "PRAGMA cache_size") != -static_cast<int64_t>(sqliteCacheKiB))
                throw std::runtime_error("benchmark configuration did not take effect");
            enableReplication(db, replication);
            exec(db.get(), "CREATE TABLE records(id INTEGER PRIMARY KEY NOT NULL, body TEXT NOT NULL DEFAULT '')");
            if (replication) exec(db.get(), "SELECT crsql_as_crr('main','records')");
            Statement insert(db.get(), "INSERT INTO records(id,body) VALUES(?1,?2)");
            Statement read(db.get(), "SELECT body FROM records WHERE id=?1");
            exec(db.get(), "BEGIN IMMEDIATE");
            for (uint64_t id = 1; id <= kSeedRows; ++id) insert.insert(id);
            exec(db.get(), "COMMIT");
            check(sqlite_express_checkpoint(path.c_str()), db.get(), "persist seed data");
            std::cout << "BENCH_CONFIG,cipher=" << cipher << ",flush_interval_ms=" << flushInterval
                      << ",page_size=" << pageSize << ",sqlite_cache_size=" << -static_cast<int64_t>(sqliteCacheKiB)
                      << ",sqlite_cache_bytes=" << static_cast<uint64_t>(sqliteCacheKiB) * 1024
                      << ",vfs_block_size=4096,cache_pages=" << cachePages
                      << ",cache_bytes=" << static_cast<uint64_t>(cachePages) * 4096
                      << ",rocksdb_cache_bytes=" << rocksdbCacheBytes
                      << ",compression=" << compression << ",zstd_level=" << compressionLevel
                      << ",background_threads=" << backgroundThreads
                      << ",replication=" << (replication ? "on" : "off")
                      << ",crsqlite_version=" << (replication ? "0.16.3" : "none")
                      << ",connections=1,rocksdb_sync=1,batch_size=" << batchSize << ",seed_rows=" << kSeedRows
                      << ",payload_bytes=" << sizeof(kPayload) - 1 << ",target_seconds=" << duration << std::endl;
            const int64_t seedVersion = replication ? scalar(db.get(), "SELECT crsql_db_version()") : 0;
            uint64_t random = 1234 * 0x9e3779b97f4a7c15ULL;
            const auto start = Clock::now();
            do {
                const auto transactionStart = Clock::now();
                if (batchSize > 1) exec(db.get(), "BEGIN IMMEDIATE");
                for (unsigned n = 0; n < batchSize; ++n) {
                    insert.insert(kSeedRows + ++writes);
                    if (batchSize == 1)
                        transactionSeconds += std::chrono::duration<double>(Clock::now() - transactionStart).count();
                    for (int i = 0; i < 4; ++i) read.read(randomID(random, kSeedRows + writes));
                }
                if (batchSize > 1) {
                    exec(db.get(), "COMMIT");
                    transactionSeconds += std::chrono::duration<double>(Clock::now() - transactionStart).count();
                }
                ++transactions;
            } while (std::chrono::duration<double>(Clock::now() - start).count() < duration);
            const auto stop = Clock::now();
            seconds = std::chrono::duration<double>(stop - start).count();
            check(sqlite_express_checkpoint(path.c_str()), db.get(), "final persistence");
            persistenceSeconds = std::chrono::duration<double>(Clock::now() - stop).count();
            if (replication) {
                const auto query = "SELECT count(*) FROM crsql_changes WHERE \"table\"='records' AND cid='body' AND db_version>" + std::to_string(seedVersion);
                trackedChanges = scalar(db.get(), query.c_str());
                if (trackedChanges != writes) throw std::runtime_error("timed replication change count mismatch");
            }
            check(sqlite_express_get_stats(path.c_str(), &stats), db.get(), "read storage statistics");
        }
        uint64_t diskBytes = 0;
        for (const auto &entry : std::filesystem::recursive_directory_iterator(path))
            if (entry.is_regular_file()) diskBytes += entry.file_size();
        verify(path, kSeedRows + writes, sqliteCacheKiB, replication);
        check(sqlite_express_unregister(), nullptr, "unregister VFS");
        std::filesystem::remove_all(path);
        const uint64_t operations = writes * 5;
        const double savings = stats.uncompressed_bytes ?
            100.0 * (1.0 - static_cast<double>(stats.compressed_payload_bytes) / stats.uncompressed_bytes) : 0;
        std::cout << "BENCH_HEADER,seconds,reads,writes,operations,ops_per_second,read_ops_per_second,write_ops_per_second,persistence_seconds,ops_per_second_including_persistence,logical_database_bytes,rocksdb_files_bytes,zstd_level,compression_savings_pct,transactions,mean_write_transaction_ms,tracked_changes\n"
                  << std::fixed << std::setprecision(6)
                  << "BENCH_CSV," << seconds << ',' << writes * 4 << ',' << writes << ',' << operations << ','
                  << operations / seconds << ',' << writes * 4 / seconds << ',' << writes / seconds << ','
                  << persistenceSeconds << ',' << operations / (seconds + persistenceSeconds) << ','
                  << stats.logical_size_bytes << ',' << diskBytes << ',' << compressionLevel << ',' << savings
                  << ',' << transactions << ',' << transactionSeconds * 1000 / transactions << ',' << trackedChanges << '\n'
                  << std::setprecision(2)
                  << "\n80% reads / 20% inserts, " << batchSize << " inserts/transaction, " << cipher << ", " << flushInterval
                  << " ms flush, " << pageSize << " byte SQLite pages\n"
                  << "VFS cache: " << cachePages << " x 4096 byte blocks ("
                  << static_cast<double>(cachePages) / 256 << " MiB target plus overhead)\n"
                  << "SQLite cache_size: " << -static_cast<int64_t>(sqliteCacheKiB) << " ("
                  << static_cast<double>(sqliteCacheKiB) / 1024 << " MiB target)\n"
                  << "RocksDB cache: " << rocksdbCacheBytes / 1048576.0 << " MiB\n"
                  << "Background compression/encryption workers: " << backgroundThreads << '\n'
                  << "Replication change tracking: " << (replication ? "on (local only)" : "off") << ", tracked timed inserts: " << trackedChanges << '\n'
                  << "Duration: " << seconds << " seconds\n"
                  << "Operations: " << operations << " (" << writes * 4 << " reads, " << writes << " writes)\n"
                  << "Operations/second: " << operations / seconds << '\n'
                  << "Write transactions: " << transactions << ", mean duration: " << transactionSeconds * 1000 / transactions
                  << " ms" << (batchSize > 1 ? " (includes interleaved reads)" : " (autocommit insert only)") << '\n'
                  << "Final persistence: " << persistenceSeconds << " seconds\n"
                  << "SQLite logical size: " << stats.logical_size_bytes << " bytes (" << stats.logical_size_bytes / 1048576.0 << " MiB)\n"
                  << "RocksDB file size: " << diskBytes << " bytes (" << diskBytes / 1048576.0 << " MiB)\n"
                  << "Compression: " << compression << ", " << savings << "% payload savings across persisted blocks (includes seeding)\n"
                  << "BENCH_DONE\n";
        return 0;
    } catch (const std::exception &error) {
        std::cerr << "BENCH_ERROR," << error.what() << ",database=" << path << std::endl;
        (void)sqlite_express_unregister();
        return 1;
    }
}
