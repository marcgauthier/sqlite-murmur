// One configurable run: 80% destination reads and 20% local or replicated row writes.
#include "sqlite_murmur.h"
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
#include <system_error>
#include <vector>

#ifdef SQLITE_MURMUR_HAS_MURMUR_EXT
#include "sqlite_murmur_ext.h"
#endif
#ifndef SQLITE_MURMUR_EXT_VERSION
#define SQLITE_MURMUR_EXT_VERSION "unavailable"
#endif
namespace {
using Clock = std::chrono::steady_clock;
struct CloseDatabase {
    bool replication = false;
    void operator()(sqlite3 *db) const {
        if (replication) {
#ifdef SQLITE_MURMUR_HAS_MURMUR_EXT
            sqlite3_exec(db, "SELECT murmur_close()", nullptr, nullptr, nullptr);
#endif
        }
        sqlite3_close(db);
    }
};
using Database = std::unique_ptr<sqlite3, CloseDatabase>;
std::string quote(const std::string &text) {
    std::string out;
    for (char c : text) {
        if (c == '\'') out += '\'';
        out += c;
    }
    return out;
}
void enableMurmur(Database &db, const std::string &spool, bool enabled) {
    if (!enabled) return;
#ifdef SQLITE_MURMUR_HAS_MURMUR_EXT
    char *error = nullptr;
    int rc = sqlite3_murmur_ext_init(db.get(), &error, nullptr);
    std::string message = error ? error : sqlite3_errmsg(db.get());
    sqlite3_free(error);
    if (rc != SQLITE_OK) throw std::runtime_error("initialize murmur extension: " + message);
    char *openError = nullptr;
    const std::string open = "SELECT murmur_open('" + quote(spool) + "')";
    rc = sqlite3_exec(db.get(), open.c_str(), nullptr, nullptr, &openError);
    message = openError ? openError : sqlite3_errmsg(db.get());
    sqlite3_free(openError);
    if (rc != SQLITE_OK) throw std::runtime_error("murmur_open: " + message);
    db.get_deleter().replication = true;
#else
    (void)spool;
    throw std::runtime_error("replication requires SQLITE_MURMUR_ENABLE_MURMUR_EXT=ON");
#endif
}
constexpr uint64_t kSeedRows = 50000;
constexpr char kPayload[] = "sqlite-murmur-benchmark-payload-abcdefghijklmnopqrstuvwxyz-0123456789";

void check(int rc, sqlite3 *db, const char *operation) {
    if (rc != SQLITE_OK)
        throw std::runtime_error(std::string(operation) + ": " + (db ? sqlite3_errmsg(db) : sqlite3_errstr(rc)));
}
Database open(const std::string &path, unsigned sqliteCacheKiB, const char *vfs = "sqlite-murmur") {
    sqlite3 *raw = nullptr;
    const int rc = sqlite3_open_v2(path.c_str(), &raw,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, vfs);
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
std::string textScalar(sqlite3 *db, const std::string &sql) {
    Statement statement(db, sql.c_str());
    if (sqlite3_step(statement.get()) != SQLITE_ROW) throw std::runtime_error(sqlite3_errmsg(db));
    const auto *text = sqlite3_column_text(statement.get(), 0);
    if (!text) throw std::runtime_error("expected text result");
    return std::string(reinterpret_cast<const char *>(text), sqlite3_column_bytes(statement.get(), 0));
}
int64_t jsonField(const std::string &json, const char *field) {
    const std::string key = std::string("\"") + field + "\":";
    const auto at = json.find(key);
    if (at == std::string::npos) throw std::runtime_error("apply summary missing " + key);
    return std::stoll(json.substr(at + key.size()));
}
uint64_t treeBytes(const std::string &path) {
    uint64_t bytes = 0;
    std::error_code ec;
    for (const auto &entry : std::filesystem::recursive_directory_iterator(path, ec))
        if (!ec && entry.is_regular_file(ec)) bytes += entry.file_size(ec);
    return bytes;
}
void verify(const std::string &path, const std::string &spool, uint64_t rows,
            unsigned sqliteCacheKiB, bool replication, bool local) {
    auto db = open(path, sqliteCacheKiB);
    enableMurmur(db, spool, replication);
    if (replication && local) {
        const std::string origin = textScalar(db.get(), "SELECT murmur_node_id()");
        const std::string seq = "SELECT murmur_log_seq('" + origin + "')";
        const std::string logged = "SELECT murmur_log_rows('" + origin + "',0,(" + seq + "))";
        if (scalar(db.get(), logged.c_str()) != static_cast<int64_t>(rows))
            throw std::runtime_error("reopened replication log row count mismatch");
    }
    if (scalar(db.get(), "SELECT count(*) FROM records") != static_cast<int64_t>(rows))
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
    std::string ingest = "local";
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
                          << "                 [--compression=zstd3|none|zstd1..zstd22] [--cipher=aegis256|aes256gcm] [--page-size=4096] [--batch-size=1] [--replication=off|on] [--ingest=local|log] [--background-threads=auto|1..256]\n"
                          << "log ingest enables replication; --seconds measures destination active time, excluding source export.\n";
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
            } else if (arg.rfind("--ingest=", 0) == 0) {
                ingest = arg.substr(9);
                if (ingest != "local" && ingest != "log") throw std::runtime_error("ingest must be local or log");
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
        if (ingest != "local") replication = true;
#ifndef SQLITE_MURMUR_HAS_MURMUR_EXT
        if (replication) throw std::runtime_error("replication requires SQLITE_MURMUR_ENABLE_MURMUR_EXT=ON");
#endif
#ifndef SQLITE_MURMUR_HAS_AEGIS256
        if (cipher == "aegis256") throw std::runtime_error("aegis256 requires SQLITE_MURMUR_ENABLE_AEGIS256=ON; use --cipher=aes256gcm for AES only");
#endif
    } catch (const std::exception &error) {
        std::cerr << "BENCH_ERROR," << error.what() << '\n';
        return 2;
    }

    const std::string path = (std::filesystem::temp_directory_path() /
        ("sqlite-murmur-benchmark-" + std::to_string(Clock::now().time_since_epoch().count()))).string();
    const std::string spool = path + ".spool";
    const std::string sourcePath = path + ".source.db";
    const std::string sourceSpool = path + ".source.spool";
    const auto cleanup = [&] {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
        std::filesystem::remove_all(spool, ec);
        std::filesystem::remove_all(sourcePath, ec);
        std::filesystem::remove_all(sourcePath + "-wal", ec);
        std::filesystem::remove_all(sourcePath + "-shm", ec);
        std::filesystem::remove_all(sourceSpool, ec);
    };
    try {
        unsigned char key[32];
        for (int i = 0; i < 32; ++i) key[i] = static_cast<unsigned char>(i + 11);
        sqlite_murmur_config config{key, flushInterval, cachePages, 1};
        check(sqlite_murmur_register(&config), nullptr, "register VFS");
        uint64_t writes = 0;
        uint64_t transactions = 0;
        uint64_t rocksdbCacheBytes = 0;
        uint64_t trackedChanges = 0;
        double seconds = 0, persistenceSeconds = 0, transactionSeconds = 0, wallSeconds = 0, sourceSeconds = 0;
        uint64_t packedBytes = 0, appliedChanges = 0, spoolBytes = 0;
        sqlite_murmur_stats stats{};
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
            enableMurmur(db, spool, replication);
            exec(db.get(), "CREATE TABLE records(id INTEGER PRIMARY KEY NOT NULL, body TEXT NOT NULL DEFAULT '')");
            if (replication) exec(db.get(), "SELECT murmur_enable_sync('records')");
            Statement insert(db.get(), "INSERT INTO records(id,body) VALUES(?1,?2)");
            Statement read(db.get(), "SELECT body FROM records WHERE id=?1");
            exec(db.get(), "BEGIN IMMEDIATE");
            for (uint64_t id = 1; id <= kSeedRows; ++id) insert.insert(id);
            if (replication) exec(db.get(), "SELECT murmur_collect()");
            exec(db.get(), "COMMIT");
            if (replication) exec(db.get(), "SELECT murmur_drain()");
            check(sqlite_murmur_checkpoint(path.c_str()), db.get(), "persist seed data");
            Database source(nullptr);
            std::string sourceOrigin;
            if (ingest != "local") {
                sqlite3 *raw = nullptr;
                check(sqlite3_open_v2(sourcePath.c_str(), &raw,
                    SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, nullptr),
                    raw, "open source");
                source.reset(raw);
                enableMurmur(source, sourceSpool, true);
                const std::string dbid = textScalar(db.get(), "SELECT murmur_database_id()");
                exec(source.get(), ("SELECT murmur_set_database_id('" + dbid + "')").c_str());
                exec(source.get(), "CREATE TABLE records(id INTEGER PRIMARY KEY NOT NULL, body TEXT NOT NULL DEFAULT '')");
                exec(source.get(), "SELECT murmur_enable_sync('records')");
                sourceOrigin = textScalar(source.get(), "SELECT murmur_node_id()");
            }
            std::cout << "BENCH_CONFIG,cipher=" << cipher << ",flush_interval_ms=" << flushInterval
                      << ",page_size=" << pageSize << ",sqlite_cache_size=" << -static_cast<int64_t>(sqliteCacheKiB)
                      << ",sqlite_cache_bytes=" << static_cast<uint64_t>(sqliteCacheKiB) * 1024
                      << ",vfs_block_size=4096,cache_pages=" << cachePages
                      << ",cache_bytes=" << static_cast<uint64_t>(cachePages) * 4096
                      << ",rocksdb_cache_bytes=" << rocksdbCacheBytes
                      << ",compression=" << compression << ",zstd_level=" << compressionLevel
                      << ",background_threads=" << backgroundThreads
                      << ",replication=" << (replication ? "on" : "off")
                      << ",murmur_version=" << (replication ? SQLITE_MURMUR_EXT_VERSION : "none")
                      << ",ingest=" << ingest << ",timing=" << (ingest == "local" ? "wall" : "destination_active")
                      << ",connections=" << (source ? 2 : 1) << ",source_storage=" << (source ? "file" : "none")
                      << ",rocksdb_sync=1,batch_size=" << batchSize << ",seed_rows=" << kSeedRows
                      << ",payload_bytes=" << sizeof(kPayload) - 1 << ",target_seconds=" << duration << std::endl;
            std::string ownOrigin;
            int64_t seedVersion = 0;
            if (replication) {
                ownOrigin = textScalar(db.get(), "SELECT murmur_node_id()");
                seedVersion = scalar(db.get(), ("SELECT murmur_log_seq('" + ownOrigin + "')").c_str());
            }
            uint64_t random = 1234 * 0x9e3779b97f4a7c15ULL;
            const auto start = Clock::now();
            if (ingest == "local") {
                if (!replication) {
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
                } else {
                    // Capture must commit atomically with its writes, so every
                    // replicated transaction is explicit and drains after commit.
                    do {
                        const auto transactionStart = Clock::now();
                        exec(db.get(), "BEGIN IMMEDIATE");
                        for (unsigned n = 0; n < batchSize; ++n) {
                            insert.insert(kSeedRows + ++writes);
                            for (int i = 0; i < 4; ++i) read.read(randomID(random, kSeedRows + writes));
                        }
                        exec(db.get(), "SELECT murmur_collect()");
                        exec(db.get(), "COMMIT");
                        exec(db.get(), "SELECT murmur_drain()");
                        transactionSeconds += std::chrono::duration<double>(Clock::now() - transactionStart).count();
                        ++transactions;
                    } while (std::chrono::duration<double>(Clock::now() - start).count() < duration);
                }
                seconds = std::chrono::duration<double>(Clock::now() - start).count();
            } else {
                Statement sourceInsert(source.get(), "INSERT INTO records(id,body) VALUES(?1,?2)");
                Statement exportStmt(source.get(), "SELECT murmur_log_export(?1,?2)");
                Statement applyStmt(db.get(), "SELECT murmur_apply(?1)");
                int64_t version = scalar(source.get(), ("SELECT murmur_log_seq('" + sourceOrigin + "')").c_str());
                do {
                    const auto sourceStart = Clock::now();
                    exec(source.get(), "BEGIN IMMEDIATE");
                    for (unsigned n = 0; n < batchSize; ++n) sourceInsert.insert(kSeedRows + writes + n + 1);
                    exec(source.get(), "SELECT murmur_collect()");
                    exec(source.get(), "COMMIT");
                    exportStmt.reset();
                    check(sqlite3_bind_text(exportStmt.get(), 1, sourceOrigin.c_str(), -1, SQLITE_TRANSIENT), source.get(), "bind export origin");
                    check(sqlite3_bind_int64(exportStmt.get(), 2, version), source.get(), "bind export version");
                    if (sqlite3_step(exportStmt.get()) != SQLITE_ROW) throw std::runtime_error(sqlite3_errmsg(source.get()));
                    const auto *blobData = sqlite3_column_blob(exportStmt.get(), 0);
                    const int blobSize = sqlite3_column_bytes(exportStmt.get(), 0);
                    std::vector<unsigned char> batch(blobSize);
                    if (blobSize) std::memcpy(batch.data(), blobData, blobSize);
                    exportStmt.reset();
                    const int64_t nextVersion = scalar(source.get(), ("SELECT murmur_log_seq('" + sourceOrigin + "')").c_str());
                    const std::string counted = "SELECT murmur_log_rows('" + sourceOrigin + "'," +
                        std::to_string(version) + "," + std::to_string(nextVersion) + ")";
                    if (scalar(source.get(), counted.c_str()) != batchSize)
                        throw std::runtime_error("exported change count mismatch");
                    version = nextVersion;
                    packedBytes += batch.size();
                    // Binding the batch is part of destination work. Source
                    // generation, collection and export are excluded.
                    sourceSeconds += std::chrono::duration<double>(Clock::now() - sourceStart).count();
                    const auto activeStart = Clock::now();
                    applyStmt.reset();
                    check(sqlite3_bind_blob64(applyStmt.get(), 1, batch.data(), batch.size(), SQLITE_STATIC), db.get(), "bind log batch");
                    if (sqlite3_step(applyStmt.get()) != SQLITE_ROW) throw std::runtime_error(sqlite3_errmsg(db.get()));
                    const auto *summary = sqlite3_column_text(applyStmt.get(), 0);
                    const int64_t applied = jsonField(summary ? reinterpret_cast<const char *>(summary) : "", "rows");
                    if (applied != batchSize) throw std::runtime_error("apply row count mismatch");
                    applyStmt.reset();
                    check(sqlite3_clear_bindings(applyStmt.get()), db.get(), "release batch");
                    transactionSeconds += std::chrono::duration<double>(Clock::now() - activeStart).count();
                    writes += batchSize;
                    appliedChanges += static_cast<uint64_t>(applied);
                    for (uint64_t i = 0; i < uint64_t(batchSize) * 4; ++i) read.read(randomID(random, kSeedRows + writes));
                    seconds += std::chrono::duration<double>(Clock::now() - activeStart).count();
                    ++transactions;
                } while (seconds < duration);
            }
            const auto stop = Clock::now();
            wallSeconds = std::chrono::duration<double>(stop - start).count();
            check(sqlite_murmur_checkpoint(path.c_str()), db.get(), "final persistence");
            persistenceSeconds = std::chrono::duration<double>(Clock::now() - stop).count();
            if (replication) {
                if (ingest == "local") {
                    exec(db.get(), "SELECT murmur_drain()");
                    const int64_t endVersion = scalar(db.get(), ("SELECT murmur_log_seq('" + ownOrigin + "')").c_str());
                    const std::string query = "SELECT murmur_log_rows('" + ownOrigin + "'," +
                        std::to_string(seedVersion) + "," + std::to_string(endVersion) + ")";
                    trackedChanges = static_cast<uint64_t>(scalar(db.get(), query.c_str()));
                } else {
                    trackedChanges = appliedChanges;
                }
                if (trackedChanges != writes) throw std::runtime_error("timed replication change count mismatch");
            }
            check(sqlite_murmur_get_stats(path.c_str(), &stats), db.get(), "read storage statistics");
        }
        uint64_t diskBytes = treeBytes(path);
        spoolBytes = treeBytes(spool);
        verify(path, spool, kSeedRows + writes, sqliteCacheKiB, replication, ingest == "local");
        check(sqlite_murmur_unregister(), nullptr, "unregister VFS");
        cleanup();
        const uint64_t operations = writes * 5;
        const double savings = stats.uncompressed_bytes ?
            100.0 * (1.0 - static_cast<double>(stats.compressed_payload_bytes) / stats.uncompressed_bytes) : 0;
        std::cout << "BENCH_HEADER,seconds,reads,writes,operations,ops_per_second,read_ops_per_second,write_ops_per_second,persistence_seconds,ops_per_second_including_persistence,logical_database_bytes,rocksdb_files_bytes,spool_bytes,zstd_level,compression_savings_pct,transactions,mean_write_transaction_ms,tracked_changes,wall_seconds,source_prepare_seconds,packed_bytes,applied_changes,wall_ops_per_second_including_persistence\n"
                  << std::fixed << std::setprecision(6)
                  << "BENCH_CSV," << seconds << ',' << writes * 4 << ',' << writes << ',' << operations << ','
                  << operations / seconds << ',' << writes * 4 / seconds << ',' << writes / seconds << ','
                  << persistenceSeconds << ',' << operations / (seconds + persistenceSeconds) << ','
                  << stats.logical_size_bytes << ',' << diskBytes << ',' << spoolBytes << ',' << compressionLevel << ',' << savings
                  << ',' << transactions << ',' << transactionSeconds * 1000 / transactions << ',' << trackedChanges << ',' << wallSeconds << ',' << sourceSeconds << ',' << packedBytes << ',' << appliedChanges << ',' << operations / (wallSeconds + persistenceSeconds) << '\n'
                  << std::setprecision(2)
                  << "\n80% reads / 20% destination row writes, " << batchSize << " inserts/transaction, " << cipher << ", " << flushInterval
                  << " ms flush, " << pageSize << " byte SQLite pages\n"
                  << "VFS cache: " << cachePages << " x 4096 byte blocks ("
                  << static_cast<double>(cachePages) / 256 << " MiB target plus overhead)\n"
                  << "SQLite cache_size: " << -static_cast<int64_t>(sqliteCacheKiB) << " ("
                  << static_cast<double>(sqliteCacheKiB) / 1024 << " MiB target)\n"
                  << "RocksDB cache: " << rocksdbCacheBytes / 1048576.0 << " MiB\n"
                  << "Background compression/encryption workers: " << backgroundThreads << '\n'
                  << "Replication change tracking: " << (replication ? (ingest == "local" ? "murmur session capture (local only)" : "murmur log incoming apply") : "off") << ", tracked timed inserts: " << trackedChanges << '\n'
                  << "Duration: " << seconds << " measured seconds; " << wallSeconds << " wall seconds; source preparation " << sourceSeconds << " seconds\n"
                  << "Operations: " << operations << " (" << writes * 4 << " reads, " << writes << " writes)\n"
                  << "Operations/second: " << operations / seconds << '\n'
                  << "Write transactions: " << transactions << ", mean duration: " << transactionSeconds * 1000 / transactions
                  << " ms" << (ingest != "local" ? " (incoming apply; reads excluded)" : batchSize > 1 || replication ? " (includes interleaved reads)" : " (autocommit insert only)") << '\n'
                  << "Final persistence: " << persistenceSeconds << " seconds\n"
                  << "SQLite logical size: " << stats.logical_size_bytes << " bytes (" << stats.logical_size_bytes / 1048576.0 << " MiB)\n"
                  << "RocksDB file size: " << diskBytes << " bytes (" << diskBytes / 1048576.0 << " MiB)\n"
                  << "Murmur spool size: " << spoolBytes << " bytes (" << spoolBytes / 1048576.0 << " MiB)\n"
                  << "Compression: " << compression << ", " << savings << "% payload savings across persisted blocks (includes seeding)\n"
                  << "BENCH_DONE\n";
        return 0;
    } catch (const std::exception &error) {
        std::cerr << "BENCH_ERROR," << error.what() << ",database=" << path << std::endl;
        (void)sqlite_murmur_unregister();
        cleanup();
        return 1;
    }
}
