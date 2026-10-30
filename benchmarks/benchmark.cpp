// Backend comparison suite: 80% destination reads and 20% local or replicated row writes.
#include "sqlite_murmur.h"
#include "replication_blob.h"
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
#include <vector>

#ifdef SQLITE_MURMUR_HAS_CRSQLITE
#include "sqlite_murmur_crsqlite.h"
#endif
#ifndef SQLITE_MURMUR_CRSQLITE_VERSION
#define SQLITE_MURMUR_CRSQLITE_VERSION "unavailable"
#endif
namespace {
using Clock = std::chrono::steady_clock;
struct CloseDatabase {
    bool replication = false;
    bool integrated = false;
    void operator()(sqlite3 *db) const {
        if (integrated) { sqlite_murmur_close(db); return; }
        if (replication) sqlite3_exec(db, "SELECT crsql_finalize()", nullptr, nullptr, nullptr);
        sqlite3_close(db);
    }
};
using Database = std::unique_ptr<sqlite3, CloseDatabase>;
void enableReplication(Database &db, bool enabled) {
    if (!enabled) return;
#ifdef SQLITE_MURMUR_HAS_CRSQLITE
    char *error = nullptr;
    int rc = sqlite3_crsqlite_init(db.get(), &error, nullptr);
    std::string message = error ? error : sqlite3_errmsg(db.get());
    sqlite3_free(error);
    if (rc != SQLITE_OK) throw std::runtime_error("initialize cr-sqlite: " + message);
    db.get_deleter().replication = true;
#else
    throw std::runtime_error("replication requires SQLITE_MURMUR_ENABLE_CRSQLITE=ON");
#endif
}
constexpr uint64_t kSeedRows = 50000;
constexpr char kPayload[] = "sqlite-murmur-benchmark-payload-abcdefghijklmnopqrstuvwxyz-0123456789";

void check(int rc, sqlite3 *db, const char *operation) {
    if (rc != SQLITE_OK)
        throw std::runtime_error(std::string(operation) + ": " + (db ? sqlite3_errmsg(db) : sqlite3_errstr(rc)));
}
Database open(const std::string &path, unsigned sqliteCacheKiB, bool murmur) {
    sqlite3 *raw = nullptr;
    // This benchmark is a consumer of the same public API as applications.
    // Storage-only opens never initialize replication metadata or start QUIC.
    sqlite_murmur_open_options options{SQLITE_MURMUR_OPEN_STORAGE_ONLY, nullptr, nullptr};
    int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX;
    std::string filename = path;
#ifdef SQLITE_MURMUR_HCTREE_ENGINE
    // HC-tree is opt-in per connection. Compiling the HC-tree amalgamation alone
    // does not select its storage format.
    filename = "file:" + path + "?hctree=1";
    flags |= SQLITE_OPEN_URI;
#endif
    const int rc = murmur ? sqlite_murmur_open_v2(filename.c_str(), &raw, flags, &options)
                          : sqlite3_open_v2(filename.c_str(), &raw, flags, nullptr);
    Database db(raw, CloseDatabase{false, murmur});
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
void verify(sqlite3 *db, uint64_t rows, bool replication) {
    if (replication && scalar(db, "SELECT count(*) FROM crsql_changes WHERE \"table\"='records' AND cid='body'") != rows)
        throw std::runtime_error("reopened replication change count mismatch");
    if (scalar(db, "SELECT count(*) FROM records") != rows)
        throw std::runtime_error("reopened row count mismatch");
    Statement integrity(db, "PRAGMA integrity_check");
    if (sqlite3_step(integrity.get()) != SQLITE_ROW ||
        std::strcmp(reinterpret_cast<const char *>(sqlite3_column_text(integrity.get(), 0)), "ok") != 0)
        throw std::runtime_error("reopened integrity check failed");
}

std::string textScalar(sqlite3 *db, const char *sql) {
    Statement statement(db, sql);
    if (sqlite3_step(statement.get()) != SQLITE_ROW || !sqlite3_column_text(statement.get(), 0))
        throw std::runtime_error(std::string("missing setting: ") + sql);
    return reinterpret_cast<const char *>(sqlite3_column_text(statement.get(), 0));
}
void persist(sqlite3 *db, const std::string &path, bool murmur) {
    if (murmur) check(sqlite_murmur_checkpoint(path.c_str()), db, "persist Murmur database");
    else {
        check(sqlite3_db_cacheflush(db), db, "flush SQLite cache");
        check(sqlite3_wal_checkpoint_v2(db, "main", SQLITE_CHECKPOINT_TRUNCATE, nullptr, nullptr),
              db, "checkpoint SQLite WAL");
    }
}
struct Options {
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
    std::string suite = "all";
    std::string backend = "murmur";
};
struct Result {
    std::string backend;
    unsigned batchSize;
    double throughput, persistedThroughput, transactionMs;
    uint64_t logicalBytes, diskBytes;
};
Result run(Options options) {
    auto &[duration, flushInterval, cachePages, sqliteCacheKiB, pageSize, cipher,
           compression, compressionLevel, batchSize, backgroundThreads,
           replication, ingest, suite, backend] = options;
    const bool murmur = backend == "murmur";
    const std::string path = (std::filesystem::temp_directory_path() /
        ("sqlite-murmur-benchmark-" + std::to_string(Clock::now().time_since_epoch().count()))).string();
    struct Cleanup {
        std::string path;
        bool murmur;
        ~Cleanup() {
            if (murmur) (void)sqlite_murmur_unregister();
            std::error_code error;
            std::filesystem::remove_all(path, error);
            std::filesystem::remove(path + "-wal", error);
            std::filesystem::remove(path + "-shm", error);
            std::filesystem::remove(path + "-journal", error);
        }
    } cleanup{path, murmur};
    unsigned char key[32];
    for (int i = 0; i < 32; ++i) key[i] = static_cast<unsigned char>(i + 11);
    sqlite_murmur_config config{key, flushInterval, cachePages, 1};
    if (murmur) check(sqlite_murmur_register(&config), nullptr, "register VFS");
    uint64_t writes = 0;
    uint64_t transactions = 0;
    uint64_t rocksdbCacheBytes = 0;
    uint64_t trackedChanges = 0;
    double seconds = 0, persistenceSeconds = 0, transactionSeconds = 0, wallSeconds = 0, sourceSeconds = 0;
    uint64_t packedBytes = 0, appliedChanges = 0;
    sqlite_murmur_stats stats{};
    {
        auto db = open(path, sqliteCacheKiB, murmur);
        exec(db.get(), ("PRAGMA page_size=" + std::to_string(pageSize)).c_str());
        exec(db.get(), "PRAGMA mmap_size=0");
        if (murmur) {
            exec(db.get(), ("PRAGMA cipher='" + cipher + "'; PRAGMA compression='" + compression +
                "'; PRAGMA flush_interval_ms=" + std::to_string(flushInterval) +
                "; PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL").c_str());
            if (backgroundThreads) exec(db.get(), ("PRAGMA background_threads=" + std::to_string(backgroundThreads)).c_str());
            backgroundThreads = static_cast<unsigned>(scalar(db.get(), "PRAGMA background_threads"));
            rocksdbCacheBytes = static_cast<uint64_t>(scalar(db.get(), "PRAGMA rocksdb_block_cache_bytes"));
            if (textScalar(db.get(), "PRAGMA cipher") != cipher ||
                textScalar(db.get(), "PRAGMA compression") != compression ||
                scalar(db.get(), "PRAGMA flush_interval_ms") != flushInterval)
                throw std::runtime_error("Murmur encryption/compression configuration did not take effect");
        } else {
            exec(db.get(), "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL");
        }
        // Set the KiB target after page size selection for every backend.
        exec(db.get(), ("PRAGMA cache_size=-" + std::to_string(sqliteCacheKiB)).c_str());
        const auto journal = textScalar(db.get(), "PRAGMA journal_mode");
        const auto synchronous = scalar(db.get(), "PRAGMA synchronous");
        if (scalar(db.get(), "PRAGMA page_size") != pageSize ||
            scalar(db.get(), "PRAGMA cache_size") != -static_cast<int64_t>(sqliteCacheKiB) ||
            journal != (murmur ? "delete" : "wal") ||
            synchronous != (murmur ? 2 : 1))
            throw std::runtime_error("benchmark configuration did not take effect");
        if (!replication && scalar(db.get(), "SELECT count(*) FROM sqlite_schema WHERE name GLOB 'crsql*' OR name GLOB 'murmur_*'") != 0)
            throw std::runtime_error("storage benchmark unexpectedly initialized replication metadata");
        enableReplication(db, replication);
        exec(db.get(), "CREATE TABLE records(id INTEGER PRIMARY KEY NOT NULL, body TEXT NOT NULL DEFAULT '')");
        if (replication) exec(db.get(), "SELECT crsql_as_crr('main','records')");
        Statement insert(db.get(), "INSERT INTO records(id,body) VALUES(?1,?2)");
        Statement read(db.get(), "SELECT body FROM records WHERE id=?1");
        exec(db.get(), "BEGIN IMMEDIATE");
        for (uint64_t id = 1; id <= kSeedRows; ++id) insert.insert(id);
        exec(db.get(), "COMMIT");
        persist(db.get(), path, murmur);
#ifdef SQLITE_MURMUR_HCTREE_ENGINE
        // Verify the actual HC-tree file signature through Murmur's VFS,
        // rather than accidentally benchmarking the stock SQLite btree.
        sqlite3_file *mainFile = nullptr;
        check(sqlite3_file_control(db.get(), "main", SQLITE_FCNTL_FILE_POINTER, &mainFile),
              db.get(), "get HC-tree main file");
        if (!mainFile || !mainFile->pMethods || !mainFile->pMethods->xRead)
            throw std::runtime_error("HC-tree main file unavailable");
        char magic[16]{};
        check(mainFile->pMethods->xRead(mainFile, magic, sizeof(magic), 0),
              db.get(), "read HC-tree file signature");
        if (std::memcmp(magic, "HCtree database ", 16) != 0)
            throw std::runtime_error("HC-tree engine built but database is not HC-tree format");
        std::cout << "BENCH_ENGINE,hctree\\n";
#else
        std::cout << "BENCH_ENGINE,standard\\n";
#endif

        Database source(nullptr);
        if (ingest != "local") {
            sqlite3 *raw = nullptr;
            check(sqlite3_open_v2(":memory:", &raw, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), raw, "open source");
            source.reset(raw);
            enableReplication(source, true);
            exec(source.get(), "CREATE TABLE records(id INTEGER PRIMARY KEY NOT NULL, body TEXT NOT NULL DEFAULT ''); SELECT crsql_as_crr('main','records')");
        }
        std::cout << "BENCH_CONFIG,suite=" << suite << ",backend=" << backend
                  << ",encryption=" << (murmur ? "on" : "off")
                  << ",journal_mode=" << journal << ",synchronous=" << synchronous
                  << ",persistence=" << (murmur ? "periodic_flush" : "wal_checkpoint")
                  << ",cipher=" << (murmur ? cipher : "none") << ",flush_interval_ms=" << (murmur ? std::to_string(flushInterval) : "NA")
                  << ",page_size=" << pageSize << ",sqlite_cache_size=" << -static_cast<int64_t>(sqliteCacheKiB)
                  << ",sqlite_cache_bytes=" << static_cast<uint64_t>(sqliteCacheKiB) * 1024
                  << ",vfs_block_size=" << (murmur ? "4096" : "NA") << ",cache_pages=" << (murmur ? std::to_string(cachePages) : "NA")
                  << ",cache_bytes=" << (murmur ? std::to_string(static_cast<uint64_t>(cachePages) * 4096) : "NA")
                  << ",rocksdb_cache_bytes=" << (murmur ? std::to_string(rocksdbCacheBytes) : "NA")
                  << ",compression=" << (murmur ? compression : "none") << ",zstd_level=" << (murmur ? compressionLevel : 0)
                  << ",background_threads=" << (murmur ? std::to_string(backgroundThreads) : "NA")
                  << ",replication=" << (replication ? "on" : "off")
                  << ",crsqlite_version=" << (replication ? SQLITE_MURMUR_CRSQLITE_VERSION : "none")
                  << ",ingest=" << ingest << ",timing=" << (ingest == "local" ? "wall" : "destination_active")
                  << ",connections=" << (source ? 2 : 1) << ",source_storage=" << (source ? "memory" : "none")
                  << ",rocksdb_sync=" << (murmur ? "1" : "NA") << ",batch_size=" << batchSize << ",seed_rows=" << kSeedRows
                  << ",payload_bytes=" << sizeof(kPayload) - 1 << ",target_seconds=" << duration << std::endl;
        const int64_t seedVersion = replication ? scalar(db.get(), "SELECT crsql_db_version()") : 0;
        uint64_t random = 1234 * 0x9e3779b97f4a7c15ULL;
        const auto start = Clock::now();
        if (ingest == "local") {
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
            seconds = std::chrono::duration<double>(Clock::now() - start).count();
        } else {
            Statement sourceInsert(source.get(), "INSERT INTO records(id,body) VALUES(?1,?2)");
            Statement changes(source.get(), "SELECT * FROM crsql_changes WHERE db_version>?1");
            Statement rowApply(db.get(), "INSERT INTO crsql_changes VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)");
            Statement blobApply(db.get(), "SELECT crsql_ingest_blob(?1)");
            int64_t version = 0;
            do {
                const auto sourceStart = Clock::now();
                exec(source.get(), "BEGIN IMMEDIATE");
                for (unsigned n = 0; n < batchSize; ++n) sourceInsert.insert(kSeedRows + writes + n + 1);
                exec(source.get(), "COMMIT");
                changes.reset();
                check(sqlite3_bind_int64(changes.get(), 1, version), source.get(), "bind export version");
                auto batch = replication_blob::capture(changes.get());
                version = scalar(source.get(), "SELECT crsql_db_version()");
                if (batch.rows.size() != batchSize) throw std::runtime_error("exported change count mismatch");
                packedBytes += batch.blob.size();
                // Binding the blob is part of destination work. Export, copying,
                // and packing are excluded identically for both apply methods.
                sourceSeconds += std::chrono::duration<double>(Clock::now() - sourceStart).count();
                const auto activeStart = Clock::now();
                exec(db.get(), "BEGIN IMMEDIATE");
                if (ingest == "blob") {
                    blobApply.reset();
                    check(sqlite3_bind_blob64(blobApply.get(), 1, batch.blob.data(), batch.blob.size(), SQLITE_STATIC), db.get(), "bind CRSB blob");
                    if (sqlite3_step(blobApply.get()) != SQLITE_ROW || sqlite3_column_int64(blobApply.get(), 0) != batchSize)
                        throw std::runtime_error(std::string("blob apply count/error: ") + sqlite3_errmsg(db.get()));
                    blobApply.reset();
                    check(sqlite3_clear_bindings(blobApply.get()), db.get(), "release blob");
                } else {
                    for (const auto &row : batch.rows) {
                        rowApply.reset();
                        for (int i = 0; i < 9; ++i) check(sqlite3_bind_value(rowApply.get(), i+1, row[i].get()), db.get(), "bind incoming change");
                        if (sqlite3_step(rowApply.get()) != SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(db.get()));
                    }
                }
                exec(db.get(), "COMMIT");
                transactionSeconds += std::chrono::duration<double>(Clock::now() - activeStart).count();
                writes += batchSize;
                appliedChanges += batch.rows.size();
                for (uint64_t i = 0; i < uint64_t(batchSize) * 4; ++i) read.read(randomID(random, kSeedRows + writes));
                seconds += std::chrono::duration<double>(Clock::now() - activeStart).count();
                ++transactions;
            } while (seconds < duration);
        }
        const auto stop = Clock::now();
        wallSeconds = std::chrono::duration<double>(stop - start).count();
        persist(db.get(), path, murmur);
        persistenceSeconds = std::chrono::duration<double>(Clock::now() - stop).count();
        if (replication) {
            const auto query = "SELECT count(*) FROM crsql_changes WHERE \"table\"='records' AND cid='body' AND db_version>" + std::to_string(seedVersion);
            trackedChanges = scalar(db.get(), query.c_str());
            if (trackedChanges != writes) throw std::runtime_error("timed replication change count mismatch");
        }
        if (murmur) check(sqlite_murmur_get_stats(path.c_str(), &stats), db.get(), "read storage statistics");
        else stats.logical_size_bytes = scalar(db.get(), "PRAGMA page_count") * uint64_t(pageSize);
    }
    uint64_t diskBytes = 0;
    if (murmur) {
        for (const auto &entry : std::filesystem::recursive_directory_iterator(path))
            if (entry.is_regular_file()) diskBytes += entry.file_size();
    } else {
        for (const auto &suffix : {"", "-wal", "-shm", "-journal"})
            if (std::filesystem::exists(path + suffix)) diskBytes += std::filesystem::file_size(path + suffix);
    }
    {
        auto db = open(path, sqliteCacheKiB, murmur);
        enableReplication(db, replication);
        verify(db.get(), kSeedRows + writes, replication);
    }
    if (murmur) {
        check(sqlite_murmur_unregister(), nullptr, "unregister VFS");
        cleanup.murmur = false;
    }
    const uint64_t operations = writes * 5;
    const double savings = stats.uncompressed_bytes ?
        100.0 * (1.0 - static_cast<double>(stats.compressed_payload_bytes) / stats.uncompressed_bytes) : 0;
    std::cout << "BENCH_HEADER,seconds,reads,writes,operations,ops_per_second,read_ops_per_second,write_ops_per_second,persistence_seconds,ops_per_second_including_persistence,logical_database_bytes,rocksdb_files_bytes,disk_bytes,zstd_level,compression_savings_pct,transactions,mean_write_transaction_ms,tracked_changes,wall_seconds,source_prepare_seconds,packed_bytes,applied_changes,wall_ops_per_second_including_persistence,suite,backend,replication,batch_size\n"
              << std::fixed << std::setprecision(6)
              << "BENCH_CSV," << seconds << ',' << writes * 4 << ',' << writes << ',' << operations << ','
              << operations / seconds << ',' << writes * 4 / seconds << ',' << writes / seconds << ','
              << persistenceSeconds << ',' << operations / (seconds + persistenceSeconds) << ','
              << stats.logical_size_bytes << ',' << (murmur ? std::to_string(diskBytes) : "NA") << ',' << diskBytes << ',' << (murmur ? compressionLevel : 0) << ',' << (murmur ? std::to_string(savings) : "NA")
              << ',' << transactions << ',' << transactionSeconds * 1000 / transactions << ',' << trackedChanges << ',' << wallSeconds << ',' << sourceSeconds << ',' << packedBytes << ',' << appliedChanges << ',' << operations / (wallSeconds + persistenceSeconds)
              << ',' << suite << ',' << backend << ',' << (replication ? "on" : "off") << ',' << batchSize << '\n'
              << "BENCH_DONE\n";
    return {backend, batchSize, operations / seconds, operations / (seconds + persistenceSeconds),
            transactionSeconds * 1000 / transactions, stats.logical_size_bytes, diskBytes};
}
void comparison(const std::vector<Result> &results, bool replication) {
    std::cout << "\nTest " << (replication ? 2 : 1) << ": "
              << (replication ? "cr-sqlite local change tracking" : "no replication") << "\n"
              << "| Backend | Inserts/tx | Ops/s | Ops/s incl. persistence | Mean tx ms | Logical bytes | Disk bytes |\n"
              << "|---|---:|---:|---:|---:|---:|---:|\n";
    for (const auto &r : results)
        std::cout << "| " << r.backend << " | " << r.batchSize << " | " << r.throughput << " | "
                  << r.persistedThroughput << " | " << r.transactionMs << " | " << r.logicalBytes << " | " << r.diskBytes << " |\n";
    std::cout << "| Inserts/tx | Murmur / SQLite disk ops/s | Murmur / disk incl. persistence |\n"
              << "|---:|---:|---:|\n";
    for (unsigned batch : {1u, 100u}) {
        const Result *murmur = nullptr, *disk = nullptr;
        for (const auto &r : results) if (r.batchSize == batch) {
            if (r.backend == "murmur") murmur = &r;
            else if (r.backend == "sqlite-disk") disk = &r;
        }
        std::cout << "| " << batch << " | " << murmur->throughput / disk->throughput << "x | "
                  << murmur->persistedThroughput / disk->persistedThroughput << "x |\n";
    }
}
} // namespace

int main(int argc, char **argv) {
    Options options;
    auto &[duration, flushInterval, cachePages, sqliteCacheKiB, pageSize, cipher,
           compression, compressionLevel, batchSize, backgroundThreads,
           replication, ingest, suite, backend] = options;
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
    bool singleOverride = false, backendOverride = false, cacheOverride = false;
    try {
        for (int i = 1; i < argc; ++i) {
            const std::string arg(argv[i]);
            if (arg == "--help") {
                std::cout << "Usage: benchmark [--suite=all|no-replication|crsqlite|single] [--seconds=60] [--flush-interval-ms=100] [--cache-pages=16384] [--sqlite-cache-kib=1048576]\n"
                          << "                 [--compression=zstd3|zstd1..zstd22] [--cipher=aegis256|aes256gcm] [--page-size=4096] [--batch-size=1] [--replication=off|on] [--ingest=local|row|blob] [--background-threads=auto|1..256]\n"
                          << "Default: 8 cases, 60 seconds each. --suite=single supports --backend=murmur|sqlite-disk.\nBatch/replication/ingest/backend overrides require --suite=single; row/blob ingest enables replication; --seconds measures destination active time, excluding source export/packing.\n";
                return 0;
            }
            if (arg.rfind("--suite=", 0) == 0) {
                suite = arg.substr(8);
                if (suite != "all" && suite != "no-replication" && suite != "crsqlite" && suite != "single")
                    throw std::runtime_error("invalid suite");
            } else if (arg.rfind("--backend=", 0) == 0) {
                backendOverride = true;
                backend = arg.substr(10);
                if (backend != "murmur" && backend != "sqlite-disk")
                    throw std::runtime_error("invalid backend");
            } else if (arg.rfind("--seconds=", 0) == 0) {
                const auto value = arg.substr(10);
                size_t used = 0;
                duration = std::stod(value, &used);
                if (used != value.size() || !std::isfinite(duration) || duration <= 0 || duration > 3600)
                    throw std::runtime_error("seconds must be greater than zero and at most 3600");
            } else if (arg.rfind("--flush-interval-ms=", 0) == 0) {
                flushInterval = static_cast<unsigned>(number(arg.substr(20), std::numeric_limits<unsigned>::max()));
            } else if (arg.rfind("--sqlite-cache-kib=", 0) == 0) {
                cacheOverride = true;
                const auto value = arg.substr(19);
                sqliteCacheKiB = value == "0" ? 0 : static_cast<unsigned>(number(value, 2147483647));
            } else if (arg.rfind("--cache-pages=", 0) == 0) {
                cachePages = static_cast<size_t>(number(arg.substr(14), std::numeric_limits<size_t>::max() / 4096));
            } else if (arg.rfind("--background-threads=", 0) == 0) {
                const auto value = arg.substr(21);
                backgroundThreads = value == "auto" ? 0 : static_cast<unsigned>(number(value, 256));
            } else if (arg.rfind("--ingest=", 0) == 0) {
                singleOverride = true;
                ingest = arg.substr(9);
                if (ingest != "local" && ingest != "row" && ingest != "blob") throw std::runtime_error("ingest must be local, row or blob");
            } else if (arg.rfind("--replication=", 0) == 0) {
                singleOverride = true;
                const auto value = arg.substr(14);
                if (value != "on" && value != "off") throw std::runtime_error("replication must be on or off");
                replication = value == "on";
            } else if (arg.rfind("--batch-size=", 0) == 0) {
                singleOverride = true;
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
                if (compression.rfind("zstd", 0) == 0) {
                    compressionLevel = static_cast<unsigned>(number(compression.substr(4), 22));
                    compression = "zstd" + std::to_string(compressionLevel);
                } else throw std::runtime_error("compression must be zstd1 through zstd22; compression is always enabled for Murmur");
            } else throw std::runtime_error("unknown argument: " + arg);
        }
        if (suite != "single" && (singleOverride || backendOverride))
            throw std::runtime_error("batch/replication/ingest/backend overrides require --suite=single");
        if (suite != "single" && cacheOverride && sqliteCacheKiB != 1048576)
            throw std::runtime_error("comparison suites require a 1 GiB SQLite cache; use --suite=single to override");
        if (ingest != "local" && backend != "murmur")
            throw std::runtime_error("incoming ingestion requires --backend=murmur");
        if (ingest != "local") replication = true;
#ifndef SQLITE_MURMUR_HAS_CRSQLITE
        if (replication || suite == "all" || suite == "crsqlite") throw std::runtime_error("replication requires SQLITE_MURMUR_ENABLE_CRSQLITE=ON");
#endif
#ifndef SQLITE_MURMUR_HAS_AEGIS256
        if (cipher == "aegis256" && (suite != "single" || backend == "murmur")) throw std::runtime_error("aegis256 requires SQLITE_MURMUR_ENABLE_AEGIS256=ON; use --cipher=aes256gcm for AES only");
#endif
    } catch (const std::exception &error) {
        std::cerr << "BENCH_ERROR," << error.what() << '\n';
        return 2;
    }

    try {
        std::cout << "Persistence: Murmur uses DELETE/FULL with " << flushInterval
                  << " ms background persistence; SQLite disk uses WAL/NORMAL with a final checkpoint.\n"
                  << "These policies have different commit durability guarantees. A 1x transaction times the insert; 100x includes interleaved reads.\n";
        if (suite == "single") run(options);
        else {
            for (bool replicated : {false, true}) {
                if ((suite == "no-replication" && replicated) || (suite == "crsqlite" && !replicated)) continue;
                std::vector<Result> results;
                for (const auto *storage : {"murmur", "sqlite-disk"}) {
                    for (unsigned batch : {1u, 100u}) {
                        auto current = options;
                        current.backend = storage;
                        current.batchSize = batch;
                        current.replication = replicated;
                        current.suite = replicated ? "crsqlite" : "no-replication";
                        results.push_back(run(current));
                    }
                }
                comparison(results, replicated);
            }
        }
        std::cout << "BENCH_SUITE_DONE\n";
        return 0;
    } catch (const std::exception &error) {
        std::cerr << "BENCH_ERROR," << error.what() << '\n';
        return 1;
    }
}
