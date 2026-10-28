#include "sqlite_murmur.h"

#include <sqlite3.h>
#include <rocksdb/db.h>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

static void require(bool ok, const std::string &what) {
    if (!ok) throw std::runtime_error(what);
}

static void alignedWriteTests(const std::string &path, const std::string &cipher) {
    constexpr size_t blockSize = 4096;
    const auto key = [](unsigned block) {
        std::string result(9, '\0');
        result[0] = 'p';
        result[8] = static_cast<char>(block);
        return result;
    };
    const auto rawStore = [&] {
        rocksdb::DB *raw = nullptr;
        rocksdb::Options options;
        options.create_if_missing = true;
        require(rocksdb::DB::Open(options, path, &raw).ok(), "open aligned-write backing store");
        return std::unique_ptr<rocksdb::DB>(raw);
    };
    {
        auto store = rawStore();
        require(store->Put(rocksdb::WriteOptions(), "meta:size", std::to_string(5 * blockSize)).ok(), "seed size");
        require(store->Put(rocksdb::WriteOptions(), "meta:cipher", cipher).ok(), "seed cipher");
        for (unsigned i = 0; i < 5; ++i)
            require(store->Put(rocksdb::WriteOptions(), key(i), "invalid encrypted record").ok(), "seed unreadable block");
    }
    struct RawFile {
        std::vector<std::max_align_t> storage;
        sqlite3_file *file = nullptr;
        explicit RawFile(const std::string &filename) {
            auto *vfs = sqlite3_vfs_find("sqlite-murmur");
            require(vfs != nullptr, "find VFS");
            storage.resize((vfs->szOsFile + sizeof(std::max_align_t) - 1) / sizeof(std::max_align_t));
            auto *candidate = reinterpret_cast<sqlite3_file *>(storage.data());
            require(vfs->xOpen(vfs, filename.c_str(), candidate,
                              SQLITE_OPEN_MAIN_DB | SQLITE_OPEN_READWRITE, nullptr) == SQLITE_OK, "open raw VFS file");
            file = candidate;
        }
        ~RawFile() { if (file) file->pMethods->xClose(file); }
        void write(const std::string &bytes, sqlite3_int64 offset) {
            require(file->pMethods->xWrite(file, bytes.data(), static_cast<int>(bytes.size()), offset) == SQLITE_OK,
                    "overwrite complete blocks without reading unreadable old records");
        }
        std::string read(size_t length, sqlite3_int64 offset) {
            std::string bytes(length, '\0');
            require(file->pMethods->xRead(file, bytes.data(), static_cast<int>(length), offset) == SQLITE_OK,
                    "read overwritten blocks");
            return bytes;
        }
        void commit(const std::string &filename) {
            require(file->pMethods->xFileControl(file, SQLITE_FCNTL_COMMIT_PHASETWO, nullptr) == SQLITE_OK,
                    "complete raw transaction");
            require(sqlite_murmur_flush(filename.c_str()) == SQLITE_OK, "persist raw transaction");
        }
    };
    std::string expected = std::string(blockSize, 'A') + std::string(2 * blockSize, 'B');
    {
        RawFile file(path);
        // Any old-block load/decrypt would fail on the deliberately bad records.
        file.write(std::string(blockSize, 'A'), blockSize);
        file.write(std::string(2 * blockSize, 'B'), 2 * blockSize);
        require(file.read(expected.size(), blockSize) == expected, "single and multiple aligned blocks");
        file.commit(path);
    }
    {
        auto store = rawStore();
        require(store->Put(rocksdb::WriteOptions(), key(2), "invalid encrypted record").ok(), "poison middle block");
    }
    {
        RawFile file(path);
        // Preserve a partial head/tail while skipping the unreadable full middle block.
        file.write(std::string(2 * blockSize, 'X'), blockSize + 100);
        expected.replace(100, 2 * blockSize, std::string(2 * blockSize, 'X'));
        file.write(std::string(512, 'Y'), blockSize); // Aligned but partial: preserve the remaining bytes.
        expected.replace(0, 512, std::string(512, 'Y'));
        require(file.read(expected.size(), blockSize) == expected, "partial head, middle and tail contents");
        const char byte = 'Z';
        require(file.file->pMethods->xWrite(file.file, &byte, 1, 17) == SQLITE_IOERR_READ,
                "partial writes must authenticate and load old bytes");
        file.commit(path);
    }
    {
        RawFile file(path);
        require(file.read(expected.size(), blockSize) == expected, "overwrites survive reopen");
    }
    std::filesystem::remove_all(path);
}

static void sql(sqlite3 *db, const char *query) {
    char *error = nullptr;
    int rc = sqlite3_exec(db, query, nullptr, nullptr, &error);
    if (rc != SQLITE_OK) {
        std::string message = error ? error : sqlite3_errmsg(db);
        sqlite3_free(error);
        throw std::runtime_error(std::string(query) + ": " + message);
    }
}

static int scalar(sqlite3 *db, const char *query) {
    sqlite3_stmt *stmt = nullptr;
    require(sqlite3_prepare_v2(db, query, -1, &stmt, nullptr) == SQLITE_OK,
            std::string("prepare: ") + sqlite3_errmsg(db));
    require(sqlite3_step(stmt) == SQLITE_ROW,
            std::string("step: ") + sqlite3_errmsg(db));
    int result = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    return result;
}

static std::string scalarText(sqlite3 *db, const char *query) {
    sqlite3_stmt *stmt = nullptr;
    require(sqlite3_prepare_v2(db, query, -1, &stmt, nullptr) == SQLITE_OK,
            std::string("prepare: ") + sqlite3_errmsg(db));
    require(sqlite3_step(stmt) == SQLITE_ROW,
            std::string("step: ") + sqlite3_errmsg(db));
    const auto *v = sqlite3_column_text(stmt, 0);
    std::string result = v ? reinterpret_cast<const char *>(v) : "";
    sqlite3_finalize(stmt);
    return result;
}

static sqlite3 *openMurmur(const std::string &path) {
    sqlite3 *db = nullptr;
    int rc = sqlite3_open_v2(path.c_str(), &db,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
        "sqlite-murmur");
    if (rc != SQLITE_OK) {
        std::string msg = db ? sqlite3_errmsg(db) : "no SQLite handle";
        if (db) sqlite3_close(db);
        throw std::runtime_error("open: " + msg);
    }
    return db;
}

static void sidecarAccessTests(const std::string &path) {
    auto *vfs = sqlite3_vfs_find("sqlite-murmur");
    require(vfs != nullptr, "find VFS for sidecar access checks");
    const auto access = [&](const std::string &name, int expected) {
        for (int flags : {SQLITE_ACCESS_EXISTS, SQLITE_ACCESS_READ, SQLITE_ACCESS_READWRITE}) {
            int exists = -1;
            require(vfs->xAccess(vfs, name.c_str(), flags, &exists) == SQLITE_OK && exists == expected,
                    "VFS access result for " + name);
        }
    };
    sqlite3 *db = openMurmur(path);
    access(path, 1);
    access(path + "-missing", 0);
    // Real sidecar artifacts must not override this VFS's RAM journal/WAL state.
    std::filesystem::create_directory(path + "-journal");
    std::filesystem::create_directory(path + "-wal");
    std::filesystem::create_directory(path + "-unowned-journal");
    access(path + "-journal", 0);
    access(path + "-wal", 0);
    access(path + "-unowned-journal", 1);
    // Preserve main-file handling even when its name looks like another DB's sidecar.
    sqlite3 *suffixDb = openMurmur(path + "-wal");
    access(path + "-wal", 1);
    require(sqlite3_close(suffixDb) == SQLITE_OK, "close suffix-named database");
    access(path + "-wal", 0);
    sql(db, "CREATE TABLE access_test(value INTEGER); INSERT INTO access_test VALUES(1)");
    sql(db, "BEGIN IMMEDIATE; UPDATE access_test SET value=2");
    access(path + "-journal", 1);
    sql(db, "ROLLBACK");
    access(path + "-journal", 0);
    require(scalar(db, "SELECT value FROM access_test") == 1, "RAM rollback journal restores data");
    require(sqlite3_close(db) == SQLITE_OK, "close sidecar-access database");
    // Expired registrations do not suppress ordinary filesystem access.
    access(path + "-journal", 1);
    access(path + "-wal", 1);
    for (const auto &name : {path, path + "-journal", path + "-wal", path + "-unowned-journal"})
        std::filesystem::remove_all(name);
}

static void reject(sqlite3 *db, const char *query) {
    char *error = nullptr;
    const int rc = sqlite3_exec(db, query, nullptr, nullptr, &error);
    sqlite3_free(error);
    require(rc != SQLITE_OK, std::string("must reject: ") + query);
}

static void rejectRead(const std::string &path) {
    sqlite3 *db = nullptr;
    const int rc = sqlite3_open_v2(path.c_str(), &db,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX, "sqlite-murmur");
    if (rc == SQLITE_OK) reject(db, "SELECT count(*) FROM events");
    if (db) require(sqlite3_close(db) == SQLITE_OK, "close rejected read");
}

static uint64_t persistedSequence(const std::string &path) {
    rocksdb::DB *raw = nullptr;
    require(rocksdb::DB::OpenForReadOnly(rocksdb::Options(), path, &raw).ok(), "read persisted sequence");
    std::unique_ptr<rocksdb::DB> store(raw);
    return store->GetLatestSequenceNumber();
}

static void statsTests(sqlite3 *db, const std::string &path, const std::string &cipher) {
    require(sqlite_murmur_checkpoint(path.c_str()) == SQLITE_OK, "diagnostic checkpoint");
    sqlite_murmur_stats before{}, during{}, after{};
    require(sqlite_murmur_get_stats(path.c_str(), &before) == SQLITE_OK, "snapshot statistics");
    require(before.persisted_blocks > 0 && before.flush_batches > 0, "persisted counters recorded");
    require(before.uncompressed_bytes == before.persisted_blocks * 4096, "raw block accounting");
    require(before.compressed_payload_bytes < before.uncompressed_bytes, "compression savings recorded");
    const uint64_t overhead = cipher == "aes256gcm" ? 33 : 69;
    require(before.encrypted_record_bytes == before.compressed_payload_bytes + overhead * before.persisted_blocks,
            "encryption overhead accounting");
    require(before.rocksdb_wal_bytes > 0 && before.rocksdb_flush_bytes > 0, "RocksDB write counters");
    require(sqlite_murmur_get_stats(nullptr, &after) == SQLITE_MISUSE, "null stats path");
    require(sqlite_murmur_get_stats(path.c_str(), nullptr) == SQLITE_MISUSE, "null stats output");
    require(sqlite_murmur_checkpoint(nullptr) == SQLITE_MISUSE, "null checkpoint path");
    require(sqlite_murmur_get_stats((path + "-missing").c_str(), &after) == SQLITE_NOTFOUND,
            "missing statistics Store");
    require(sqlite_murmur_checkpoint((path + "-missing").c_str()) == SQLITE_NOTFOUND,
            "missing checkpoint Store");

    const int cacheSize = scalar(db, "PRAGMA cache_size");
    sql(db, "PRAGMA flush_interval_ms=60000");
    sql(db, "PRAGMA cache_size=1");
    const uint64_t sequence = persistedSequence(path);
    sql(db, "BEGIN IMMEDIATE");
    sql(db, "UPDATE noise SET payload=randomblob(16384)");
    require(sqlite_murmur_get_stats(path.c_str(), &during) == SQLITE_OK, "active transaction counters");
    require(during.vfs_write_bytes > before.vfs_write_bytes, "force uncommitted pager spill");
    require(during.vfs_block_writes > before.vfs_block_writes, "VFS block writes counted");
    require(sqlite_murmur_checkpoint(path.c_str()) == SQLITE_OK, "checkpoint during active transaction");
    require(sqlite_murmur_get_stats(path.c_str(), &after) == SQLITE_OK, "snapshot after active checkpoint");
    require(after.persisted_blocks == before.persisted_blocks &&
            after.compressed_payload_bytes == before.compressed_payload_bytes &&
            after.encrypted_record_bytes == before.encrypted_record_bytes &&
            after.flush_batches == before.flush_batches, "active pages excluded from persistence counters");
    require(persistedSequence(path) == sequence, "active transaction never reaches RocksDB");
    sql(db, "ROLLBACK");
    sql(db, ("PRAGMA cache_size=" + std::to_string(cacheSize)).c_str());
    sql(db, "PRAGMA flush_interval_ms=250");
    require(sqlite_murmur_checkpoint(path.c_str()) == SQLITE_OK, "settle rollback");
}

static void compressionTests(sqlite3 *db, const std::string &path) {
    require(scalarText(db, "PRAGMA compression") == "zstd3", "default compression");
    for (const char *query : {"PRAGMA compression='zstd0'", "PRAGMA compression='zstd23'",
                              "PRAGMA compression='zstd3junk'", "PRAGMA compression='unknown'"})
        reject(db, query);
    require(sqlite_murmur_checkpoint(path.c_str()) == SQLITE_OK, "settle before compression tests");
    sqlite_murmur_stats before{}, after{};
    require(sqlite_murmur_get_stats(path.c_str(), &before) == SQLITE_OK, "compression baseline");
    sql(db, "PRAGMA compression=none");
    sqlite3 *peer = openMurmur(path);
    require(scalarText(peer, "PRAGMA compression") == "none", "compression shared across connections");
    sql(db, "CREATE TABLE compression_cases(payload BLOB)");
    sql(db, "INSERT INTO compression_cases VALUES(zeroblob(16384))");
    require(sqlite_murmur_checkpoint(path.c_str()) == SQLITE_OK, "persist uncompressed blocks");
    require(sqlite_murmur_get_stats(path.c_str(), &after) == SQLITE_OK, "uncompressed counters");
    require(after.uncompressed_bytes > before.uncompressed_bytes &&
            after.compressed_payload_bytes - before.compressed_payload_bytes ==
            after.uncompressed_bytes - before.uncompressed_bytes, "disabled compression stores raw blocks");
    before = after;
    sql(peer, "PRAGMA compression='zstd6'");
    sql(db, "UPDATE compression_cases SET payload=zeroblob(32768)");
    require(sqlite_murmur_checkpoint(path.c_str()) == SQLITE_OK, "persist Zstd blocks");
    require(sqlite_murmur_get_stats(path.c_str(), &after) == SQLITE_OK, "Zstd counters");
    require(after.compressed_payload_bytes - before.compressed_payload_bytes <
            after.uncompressed_bytes - before.uncompressed_bytes, "Zstd level override compresses data");
    require(sqlite3_close(peer) == SQLITE_OK, "close compression peer");
    sql(db, "PRAGMA compression='ZSTD3'");
}

static void rocksdbPragmaTests(sqlite3 *db, const std::string &path) {
    struct Setting { const char *name; const char *value; const char *tooSmall; const char *tooLarge; };
    const Setting settings[] = {
        {"background_threads", "4", "0", "257"},
        {"rocksdb_sync", "0", "-1", "2"},
        {"rocksdb_write_buffer_size", "1048576", "65535", "18446744073709551616"},
        {"rocksdb_max_write_buffer_number", "3", "1", "2147483648"},
        {"rocksdb_block_cache_bytes", "2097152", "-1", "18446744073709551616"},
        {"rocksdb_max_background_jobs", "3", "0", "2147483648"},
    };
    sqlite3 *peer = openMurmur(path);
    const std::string otherPath = path + "-independent";
    sqlite3 *other = openMurmur(otherPath);
    sqlite3 *readOnly = nullptr;
    require(sqlite3_open_v2(path.c_str(), &readOnly, SQLITE_OPEN_READONLY, "sqlite-murmur") == SQLITE_OK,
            "open read-only peer");
    for (const auto &setting : settings) {
        const std::string query = std::string("PRAGMA ") + setting.name;
        const std::string original = scalarText(db, query.c_str());
        require(!original.empty(), "RocksDB setting has a result");
        require(scalarText(db, (query + "=" + setting.value).c_str()) == setting.value,
                "RocksDB setter returns effective value");
        require(scalarText(peer, query.c_str()) == setting.value, "RocksDB setting shared by peers");
        require(scalarText(readOnly, query.c_str()) == setting.value, "read-only peer can query");
        require(scalarText(other, query.c_str()) == original, "RocksDB setting isolated per database");
        reject(readOnly, (query + "=" + original).c_str());
        for (const char *bad : {setting.tooSmall, setting.tooLarge, "'abc'", "'12ms'", "''", "1.5", "' 2'"})
            reject(db, (query + "=" + bad).c_str());
        require(scalarText(db, query.c_str()) == setting.value, "invalid value leaves setting unchanged");
        require(scalarText(peer, (query + "=" + original).c_str()) == original, "peer restores setting");
    }
    require(scalar(db, "PRAGMA rocksdb_block_cache_bytes=0") == 0, "zero cache capacity supported");
    require(scalar(db, "PRAGMA ROCKSDB_BLOCK_CACHE_BYTES=33554432") == 33554432, "case insensitive name");
    // Leave settings changed to verify that reopening uses registration/library defaults.
    for (const auto &setting : settings)
        sql(db, (std::string("PRAGMA ") + setting.name + "=" + setting.value).c_str());
    require(sqlite3_close(readOnly) == SQLITE_OK, "close read-only peer");
    require(sqlite3_close(peer) == SQLITE_OK, "close RocksDB peer");
    require(sqlite3_close(other) == SQLITE_OK, "close independent database");
    std::filesystem::remove_all(otherPath);
}

// Mutate real persisted ciphertext, not the SQLite pager's plaintext cache.
static void tamperTests(const std::string &path, const std::string &cipher) {
    const std::string pageKey = std::string("p") + std::string(8, '\0');
    std::string original;
    {
        rocksdb::DB *raw = nullptr;
        require(rocksdb::DB::Open(rocksdb::Options(), path, &raw).ok(), "open raw store");
        std::unique_ptr<rocksdb::DB> store(raw);
        require(store->Get(rocksdb::ReadOptions(), pageKey, &original).ok(), "read encrypted page");
    }
    require(original.substr(0, 4) == (cipher == "aes256gcm" ? "EXP1" : "EXP2"),
            "persisted encryption format");
    const size_t nonceSize = cipher == "aes256gcm" ? 12 : 32;
    for (size_t offset : {size_t(0), size_t(4), size_t(5), 5 + nonceSize, original.size() - 1,
                          original.size()}) {
        std::string corrupted = original;
        if (offset == original.size()) corrupted.resize(5); // truncated record
        else corrupted[offset] ^= 1;
        {
            rocksdb::DB *raw = nullptr;
            require(rocksdb::DB::Open(rocksdb::Options(), path, &raw).ok(), "open for tamper");
            std::unique_ptr<rocksdb::DB> store(raw);
            require(store->Put(rocksdb::WriteOptions(), pageKey, corrupted).ok(), "tamper page");
        }
        rejectRead(path);
    }
    {
        rocksdb::DB *raw = nullptr;
        require(rocksdb::DB::Open(rocksdb::Options(), path, &raw).ok(), "open for restore");
        std::unique_ptr<rocksdb::DB> store(raw);
        require(store->Put(rocksdb::WriteOptions(), pageKey, original).ok(), "restore page");
    }
}

int main(int argc, char **argv) {
    const std::string cipher = argc > 1 ? argv[1] : "aes256gcm";
    const auto id = std::chrono::steady_clock::now().time_since_epoch().count();
    const std::string path = (std::filesystem::temp_directory_path() /
        ("sqlite-murmur-test-" + std::to_string(id))).string();

    unsigned char key[32];
    for (size_t i = 0; i < sizeof(key); ++i) key[i] = static_cast<unsigned char>(i + 1);
    sqlite_murmur_config config{};
    config.key = key;
    config.flush_interval_ms = 250;
    config.cache_pages = 32; // exercise eviction
    config.rocksdb_sync = 1;
    try {
        require(sqlite_murmur_register(&config) == SQLITE_OK, "register VFS");
        sidecarAccessTests(path + "-access");
        alignedWriteTests(path + "-writes", cipher);
        std::cerr << "TEST first open\n";
        sqlite3 *db = openMurmur(path);
        require(sqlite_murmur_unregister() == SQLITE_BUSY,
                "unregister must refuse open handles");
        require(scalar(db, "PRAGMA mmap_size") == 0, "no mmap");
        require(scalar(db, "PRAGMA page_size") > 0, "SQLite pages available");
        require(scalar(db, "PRAGMA flush_interval_ms") == 250, "default flush interval");
        require(scalar(db, "PRAGMA flush_interval_ms=10") == 10, "set interval");
        sqlite3 *peer = openMurmur(path);
        require(scalar(peer, "PRAGMA flush_interval_ms") == 10, "interval shared by connections");
        require(scalar(peer, "PRAGMA flush_interval_ms=0") == 250, "zero restores default");
        require(scalar(db, "PRAGMA flush_interval_ms") == 250, "peer updates interval");
        require(sqlite3_close(peer) == SQLITE_OK, "close peer");
        for (const char *query : {"PRAGMA flush_interval_ms=-1", "PRAGMA flush_interval_ms='abc'",
                                  "PRAGMA flush_interval_ms='12ms'", "PRAGMA flush_interval_ms=1.5",
                                  "PRAGMA flush_interval_ms=4294967296", "PRAGMA flush_interval_ms=''"})
            reject(db, query);
        require(scalar(db, "PRAGMA flush_interval_ms") == 250, "invalid interval leaves setting unchanged");
        require(scalarText(db, "PRAGMA cipher") == "aes256gcm", "default cipher");
        reject(db, "PRAGMA cipher='unknown'");
#ifdef SQLITE_MURMUR_TEST_NO_AEGIS256
        reject(db, "PRAGMA cipher='aegis256'");
#endif
        sql(db, ("PRAGMA cipher='" + cipher + "'").c_str());
        require(scalarText(db, "PRAGMA cipher") == cipher, "selected cipher");
        require(sqlite3_close(db) == SQLITE_OK, "close empty database");
        db = openMurmur(path);
        require(scalarText(db, "PRAGMA cipher") == cipher, "empty database remembers cipher");
        sql(db, "PRAGMA journal_mode=DELETE");
        char *error = nullptr;
        const int walRc = sqlite3_exec(db, "PRAGMA journal_mode=WAL", nullptr,
                                        nullptr, &error);
        require(walRc != SQLITE_OK, "WAL must be rejected");
        sqlite3_free(error);

        sql(db, "CREATE TABLE events(id INTEGER PRIMARY KEY, body TEXT NOT NULL)");
        rocksdbPragmaTests(db, path);
        reject(db, cipher == "aes256gcm" ? "PRAGMA cipher='aegis256'" : "PRAGMA cipher='aes256gcm'");
        sql(db, "BEGIN");
        for (int i = 0; i < 300; ++i) {
            std::string query = "INSERT INTO events(id,body) VALUES(" +
                std::to_string(i) + ",'body " + std::to_string(i) + "')";
            sql(db, query.c_str());
        }
        sql(db, "COMMIT");
        // Exercise encrypted storage for incompressible, multi-block content too.
        sql(db, "CREATE TABLE noise(payload BLOB)");
        sql(db, "INSERT INTO noise VALUES(randomblob(16384))");
        statsTests(db, path, cipher);
        compressionTests(db, path);
        require(scalar(db, "SELECT count(*) FROM events") == 300, "count after commit");
        sql(db, "BEGIN");
        sql(db, "DELETE FROM events WHERE id < 100");
        sql(db, "ROLLBACK");
        require(scalar(db, "SELECT count(*) FROM events") == 300,
                "rollback must restore row versions");

        // Coalescing: overwrite the same pages in multiple completed transactions.
        for (int n = 0; n < 6; ++n) {
            std::string statement = "UPDATE events SET body='final " +
                std::to_string(n) + "' WHERE id=2";
            sql(db, statement.c_str());
        }
        require(sqlite_murmur_flush(path.c_str()) == SQLITE_OK,
                "manual flush of committed pages");
        sql(db, "PRAGMA flush_interval_ms=60000");
        const uint64_t before = persistedSequence(path);
        sql(db, "UPDATE events SET body='timer flush' WHERE id=3");
        sql(db, "PRAGMA flush_interval_ms=10");
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (persistedSequence(path) == before && std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        require(persistedSequence(path) > before, "updated interval wakes worker and persists data");
        require(sqlite3_close(db) == SQLITE_OK, "close database");

        std::cerr << "TEST reopen after flush\n";
        db = openMurmur(path);
        sqlite_murmur_stats reopenedStats{};
        require(sqlite_murmur_get_stats(path.c_str(), &reopenedStats) == SQLITE_OK, "reopen statistics");
        require(reopenedStats.flush_batches == 0 && reopenedStats.vfs_write_bytes == 0 &&
                reopenedStats.rocksdb_wal_bytes == 0, "counters reset on reopen");
        require(scalarText(db, "PRAGMA cipher") == cipher, "cipher remembered on reopen");
        require(scalar(db, "PRAGMA flush_interval_ms") == 250, "interval resets on reopen");
        require(scalar(db, "PRAGMA rocksdb_sync") == 1, "sync resets to registration config");
        require(scalar(db, "PRAGMA rocksdb_block_cache_bytes") == 33554432, "block cache resets on reopen");
        const rocksdb::Options defaults;
        require(scalar(db, "PRAGMA rocksdb_write_buffer_size") == static_cast<int>(defaults.write_buffer_size),
                "write buffer size resets on reopen");
        require(scalar(db, "PRAGMA rocksdb_max_write_buffer_number") == defaults.max_write_buffer_number,
                "write buffer count resets on reopen");
        require(scalar(db, "PRAGMA rocksdb_max_background_jobs") == defaults.max_background_jobs,
                "background jobs resets on reopen");
        require(scalar(db, "SELECT count(*) FROM events") == 300,
                "reopen count after a RocksDB restore");
        require(scalar(db, "SELECT length(payload) FROM noise") == 16384,
                "reopen incompressible content");
        require(scalar(db, "SELECT length(payload) FROM compression_cases") == 32768,
                "reopen data after switching compression");
        require(scalarText(db, "PRAGMA compression") == "zstd3", "compression resets on reopen");
        require(scalar(db, "SELECT count(*) FROM events WHERE body='final 5'") == 1,
                "reopen most recent coalesced value");
        require(scalar(db, "SELECT count(*) FROM events WHERE body='timer flush'") == 1,
                "background flush persisted latest value");
        require(scalarText(db, "PRAGMA integrity_check") == "ok",
                "SQLite database integrity after recovery");
        require(sqlite3_close(db) == SQLITE_OK, "close reopen");

        require(sqlite_murmur_unregister() == SQLITE_OK, "unregister");
        // An incorrect key must not silently return arbitrary plaintext.
        unsigned char wrongKey[32] = {};
        std::cerr << "TEST bad key\n";
        config.key = wrongKey;
        require(sqlite_murmur_register(&config) == SQLITE_OK, "register wrong key");
        std::cerr << "TEST reopen bad key\n";
        db = nullptr;
        const int openRc = sqlite3_open_v2(path.c_str(), &db,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
            "sqlite-murmur");
        // SQLite may read page 1 during sqlite3_open_v2 itself, so the
        // authentication error is valid at either open or first statement.
        if (openRc == SQLITE_OK) {
            sqlite3_stmt *stmt = nullptr;
            const int rc = sqlite3_prepare_v2(db, "SELECT count(*) FROM events", -1,
                                               &stmt, nullptr);
            require(rc != SQLITE_OK, "bad key must reject database read");
            if (stmt) sqlite3_finalize(stmt);
        }
        if (db) sqlite3_close(db);
        require(sqlite_murmur_unregister() == SQLITE_OK, "unregister wrong key");
        config.key = key;
        require(sqlite_murmur_register(&config) == SQLITE_OK, "register for tamper tests");
        tamperTests(path, cipher);
        db = openMurmur(path);
        require(scalarText(db, "PRAGMA integrity_check") == "ok", "restored ciphertext intact");
        require(sqlite3_close(db) == SQLITE_OK, "close restored database");
        require(sqlite_murmur_unregister() == SQLITE_OK, "unregister after tamper tests");
        std::filesystem::remove_all(path);
        std::cout << "sqlite-murmur integration test passed\n";
        return 0;
    } catch (const std::exception &e) {
        std::cerr << "sqlite-murmur test failure: " << e.what() << '\n';
        std::filesystem::remove_all(path);
        return 1;
    }
}
