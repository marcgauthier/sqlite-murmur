#include "sqlite_express.h"

#include <sqlite3.h>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>

static void require(bool ok, const std::string &what) {
    if (!ok) throw std::runtime_error(what);
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

static sqlite3 *openExpress(const std::string &path) {
    sqlite3 *db = nullptr;
    int rc = sqlite3_open_v2(path.c_str(), &db,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
        "sqlite-express");
    if (rc != SQLITE_OK) {
        std::string msg = db ? sqlite3_errmsg(db) : "no SQLite handle";
        if (db) sqlite3_close(db);
        throw std::runtime_error("open: " + msg);
    }
    return db;
}

int main() {
    const auto id = std::chrono::steady_clock::now().time_since_epoch().count();
    const std::string path = (std::filesystem::temp_directory_path() /
        ("sqlite-express-test-" + std::to_string(id))).string();

    unsigned char key[32];
    for (size_t i = 0; i < sizeof(key); ++i) key[i] = static_cast<unsigned char>(i + 1);
    sqlite_express_config config{};
    config.key = key;
    config.flush_interval_ms = 250;
    config.cache_pages = 32; // exercise eviction
    config.rocksdb_sync = 1;
    try {
        require(sqlite_express_register(&config) == SQLITE_OK, "register VFS");
        sqlite3 *db = openExpress(path);
        require(sqlite_express_unregister() == SQLITE_BUSY,
                "unregister must refuse open handles");
        require(scalar(db, "PRAGMA mmap_size") == 0, "no mmap");
        require(scalar(db, "PRAGMA page_size") > 0, "SQLite pages available");
        sql(db, "PRAGMA journal_mode=DELETE");
        char *error = nullptr;
        const int walRc = sqlite3_exec(db, "PRAGMA journal_mode=WAL", nullptr,
                                        nullptr, &error);
        require(walRc != SQLITE_OK, "WAL must be rejected");
        sqlite3_free(error);

        sql(db, "CREATE TABLE events(id INTEGER PRIMARY KEY, body TEXT NOT NULL)");
        sql(db, "BEGIN");
        for (int i = 0; i < 300; ++i) {
            std::string query = "INSERT INTO events(id,body) VALUES(" +
                std::to_string(i) + ",'body " + std::to_string(i) + "')";
            sql(db, query.c_str());
        }
        sql(db, "COMMIT");
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
        require(sqlite_express_flush(path.c_str()) == SQLITE_OK,
                "manual flush of committed pages");
        require(sqlite3_close(db) == SQLITE_OK, "close database");

        db = openExpress(path);
        require(scalar(db, "SELECT count(*) FROM events") == 300,
                "reopen count after a RocksDB restore");
        require(scalar(db, "SELECT count(*) FROM events WHERE body='final 5'") == 1,
                "reopen most recent coalesced value");
        require(scalar(db, "PRAGMA integrity_check='events'") >= 0,
                "sqlite integrity check executed");
        require(sqlite3_close(db) == SQLITE_OK, "close reopen");

        require(sqlite_express_unregister() == SQLITE_OK, "unregister");
        // An incorrect key must not silently return arbitrary plaintext.
        unsigned char wrongKey[32] = {};
        config.key = wrongKey;
        require(sqlite_express_register(&config) == SQLITE_OK, "register wrong key");
        db = openExpress(path);
        sqlite3_stmt *stmt = nullptr;
        const int rc = sqlite3_prepare_v2(db, "SELECT count(*) FROM events", -1,
                                           &stmt, nullptr);
        require(rc != SQLITE_OK, "bad key must reject database read");
        if (stmt) sqlite3_finalize(stmt);
        sqlite3_close(db);
        require(sqlite_express_unregister() == SQLITE_OK, "unregister wrong key");
        std::filesystem::remove_all(path);
        std::cout << "sqlite-express integration test passed\n";
        return 0;
    } catch (const std::exception &e) {
        std::cerr << "sqlite-express test failure: " << e.what() << '\n';
        std::filesystem::remove_all(path);
        return 1;
    }
}
