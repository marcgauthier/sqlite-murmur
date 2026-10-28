#include "sqlite_murmur.h"
#ifdef SQLITE_MURMUR_HAS_CRSQLITE
#include "sqlite_murmur_crsqlite.h"
#endif
#ifdef SQLITE_MURMUR_HAS_MURMUR
extern "C" int sqlite3_murmurreplication_attach(sqlite3 *, char **, const sqlite3_api_routines *);
#endif

namespace {
int configure(sqlite3 *db, const char *name, const char *value) {
    if (!value) return SQLITE_OK;
    char *sql = sqlite3_mprintf("PRAGMA %s=%Q", name, value);
    if (!sql) return SQLITE_NOMEM;
    const int rc = sqlite3_exec(db, sql, nullptr, nullptr, nullptr);
    sqlite3_free(sql);
    return rc;
}
#if defined(SQLITE_MURMUR_HAS_CRSQLITE) || defined(SQLITE_MURMUR_HAS_MURMUR)
int extensionResult(int rc, char *error) {
    if (rc != SQLITE_OK && error) sqlite3_log(rc, "sqlite-murmur initialization: %s", error);
    sqlite3_free(error);
    return rc;
}
#endif
}

extern "C" int sqlite_murmur_open(const char *path, sqlite3 **db) {
    return sqlite_murmur_open_v2(path, db,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, nullptr);
}

extern "C" int sqlite_murmur_open_v2(const char *path, sqlite3 **db, int flags,
                                     const sqlite_murmur_open_options *options) {
    if (!db) return SQLITE_MISUSE;
    *db = nullptr;
    if (!path || !*path || (options && (options->flags & ~SQLITE_MURMUR_OPEN_STORAGE_ONLY)))
        return SQLITE_MISUSE;
    int rc = sqlite3_open_v2(path, db, flags, "sqlite-murmur");
    if (rc != SQLITE_OK) return rc;
    rc = configure(*db, "cipher", options ? options->cipher : nullptr);
    if (rc == SQLITE_OK) rc = configure(*db, "compression", options ? options->compression : nullptr);
    if (rc != SQLITE_OK) return rc;
    if ((options && (options->flags & SQLITE_MURMUR_OPEN_STORAGE_ONLY)) ||
        sqlite3_db_readonly(*db, "main") != 0) return SQLITE_OK;
#ifdef SQLITE_MURMUR_HAS_CRSQLITE
    char *error = nullptr;
    rc = sqlite3_crsqlite_init(*db, &error, nullptr);
    rc = extensionResult(rc, error);
    if (rc != SQLITE_OK) return rc;
#endif
#ifdef SQLITE_MURMUR_HAS_MURMUR
    char *murmurError = nullptr;
    rc = sqlite3_murmurreplication_attach(*db, &murmurError, nullptr);
    rc = extensionResult(rc, murmurError);
#endif
    return rc;
}

extern "C" int sqlite_murmur_close(sqlite3 *db) {
    if (!db) return SQLITE_OK;
#ifdef SQLITE_MURMUR_HAS_CRSQLITE
    // Storage-only and read-only handles may not have CRR functions registered.
    sqlite3_stmt *finalize = nullptr;
    if (sqlite3_prepare_v2(db, "SELECT crsql_finalize()", -1, &finalize, nullptr) == SQLITE_OK) {
        const int rc = sqlite3_step(finalize);
        sqlite3_finalize(finalize);
        if (rc != SQLITE_ROW && rc != SQLITE_DONE) return rc;
    }
#endif
    return sqlite3_close(db);
}
