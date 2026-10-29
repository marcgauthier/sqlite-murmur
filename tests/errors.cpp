#include "sqlite_murmur_errors.h"

#include <cstring>
#include <iostream>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>

namespace {
void require(bool condition, const char *message) {
    if (!condition) throw std::runtime_error(message);
}

struct ErrorCase {
    int code;
    int primary;
    const char *name;
};

const ErrorCase cases[] = {
    {SQLITE_MURMUR_INVALID, SQLITE_ERROR, "Invalid"},
    {SQLITE_MURMUR_CORRUPT, SQLITE_CORRUPT, "Corrupt"},
    {SQLITE_MURMUR_WRONG_KEY, SQLITE_NOTADB, "WrongKey"},
    {SQLITE_MURMUR_SCHEMA_MISMATCH, SQLITE_SCHEMA, "SchemaMismatch"},
    {SQLITE_MURMUR_BUSY, SQLITE_BUSY, "Busy"},
    {SQLITE_MURMUR_CLOSED, SQLITE_MISUSE, "Closed"},
    {SQLITE_MURMUR_CALLBACK_PANICKED, SQLITE_ABORT, "CallbackPanicked"},
    {SQLITE_MURMUR_STORAGE, SQLITE_IOERR, "Storage"},
    {SQLITE_MURMUR_IO, SQLITE_IOERR, "Io"},
    {SQLITE_MURMUR_LIMIT, SQLITE_TOOBIG, "Limit"},
    {SQLITE_MURMUR_PROTOCOL, SQLITE_PROTOCOL, "Protocol"},
    {SQLITE_MURMUR_AUTH, SQLITE_AUTH, "Auth"},
    {SQLITE_MURMUR_UNAVAILABLE, SQLITE_CANTOPEN, "Unavailable"},
    {SQLITE_MURMUR_OVERLOADED, SQLITE_BUSY, "Overloaded"},
    {SQLITE_MURMUR_CANCELED, SQLITE_INTERRUPT, "Canceled"},
};

void raise_error(sqlite3_context *ctx, int, sqlite3_value **args) {
    sqlite3_result_error(ctx, "typed Murmur failure", -1);
    sqlite3_result_error_code(ctx, sqlite3_value_int(args[0]));
}

void check_sqlite_error(sqlite3 *db, int code, int extended) {
    require(sqlite3_extended_result_codes(db, extended) == SQLITE_OK,
            "cannot set extended result mode");
    sqlite3_stmt *raw = nullptr;
    require(sqlite3_prepare_v2(db, "SELECT raise_murmur_error(?1)", -1,
                              &raw, nullptr) == SQLITE_OK,
            "cannot prepare error function");
    std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> stmt(raw, sqlite3_finalize);
    require(sqlite3_bind_int(stmt.get(), 1, code) == SQLITE_OK, "cannot bind code");
    const int rc = sqlite3_step(stmt.get());
    require(rc == (extended ? code : (code & 255)), "step lost error code");
    require(sqlite3_errcode(db) == (extended ? code : (code & 255)),
            "incorrect connection error code");
    require(sqlite3_extended_errcode(db) == code, "incorrect extended error code");
    require(std::strcmp(sqlite3_errmsg(db), "typed Murmur failure") == 0,
            "error message lost");
    // Reset reports the previous failure, then makes the statement reusable.
    require(sqlite3_reset(stmt.get()) == (extended ? code : (code & 255)),
            "reset lost error code");
    require(sqlite3_bind_int(stmt.get(), 1, SQLITE_MURMUR_INVALID) == SQLITE_OK,
            "cannot rebind after error");
    require(sqlite3_step(stmt.get()) == (extended ? SQLITE_MURMUR_INVALID : SQLITE_ERROR),
            "statement cannot be reused after failure");
}
} // namespace

int main() {
    try {
        std::set<int> unique_codes;
        for (const auto &test : cases) {
            require(unique_codes.insert(test.code).second, "duplicate error code");
            const char *name = sqlite_murmur_error_name(test.code);
            require(name && std::strcmp(name, test.name) == 0, "incorrect error name");
            require(sqlite_murmur_error_primary(test.code) == test.primary,
                    "incorrect primary classification");
            // A recognized discriminator with an unrelated low byte is unknown.
            require(sqlite_murmur_error_name(test.code ^ 0xff) == nullptr,
                    "recognized a malformed extended code");
        }
        for (int code : {SQLITE_OK, SQLITE_ROW, SQLITE_DONE, SQLITE_ERROR,
                         SQLITE_IOERR_READ, SQLITE_CONSTRAINT_UNIQUE,
                         (0x5310 << 8) | SQLITE_ERROR, -1}) {
            require(sqlite_murmur_error_name(code) == nullptr,
                    "misclassified native or unknown error");
            require(sqlite_murmur_error_primary(code) == (code & 255),
                    "incorrect native primary code");
        }

        sqlite3 *raw = nullptr;
        const int open_rc = sqlite3_open(":memory:", &raw);
        std::unique_ptr<sqlite3, decltype(&sqlite3_close)> db(raw, sqlite3_close);
        require(open_rc == SQLITE_OK, "cannot open SQLite");
        require(sqlite3_create_function_v2(db.get(), "raise_murmur_error", 1,
                    SQLITE_UTF8, nullptr, raise_error, nullptr, nullptr, nullptr) == SQLITE_OK,
                "cannot register error function");
        for (const auto &test : cases) {
            check_sqlite_error(db.get(), test.code, 0);
            check_sqlite_error(db.get(), test.code, 1);
        }
        // Native extended failures retain their code and remain unclassified.
        check_sqlite_error(db.get(), SQLITE_CONSTRAINT_UNIQUE, 1);
        require(sqlite3_exec(db.get(), "CREATE TABLE t(x UNIQUE); INSERT INTO t VALUES(1)",
                            nullptr, nullptr, nullptr) == SQLITE_OK, "cannot create native test");
        require(sqlite3_exec(db.get(), "INSERT INTO t VALUES(1)", nullptr, nullptr, nullptr)
                    == SQLITE_CONSTRAINT_UNIQUE, "native unique code changed");
        require(sqlite_murmur_error_name(sqlite3_extended_errcode(db.get())) == nullptr,
                "native constraint was labeled Murmur");
        require(sqlite3_exec(db.get(), "SELECT 1", nullptr, nullptr, nullptr) == SQLITE_OK,
                "connection unusable after failure");
        std::cout << "Murmur error categories, SQLite propagation, native errors and reuse passed\n";
        return 0;
    } catch (const std::exception &e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
