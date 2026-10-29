#pragma once

#include <sqlite3.h>

/* Taxonomy provenance: /media/marc/2TB/Archives/MURMUR-RUST,
 * src/lib.rs (Error), replication/src/lib.rs (Error), spool/src/error.rs
 * (ErrorKind), clients/go/native/src/lib.rs (error mapping).
 * This is the SQLite C ABI projection of those Rust categories. Rust error
 * implementations remain authoritative; native SQLite codes pass through.
 *
 * Murmur uses extension discriminators 0x5301..0x530f. The low byte retains
 * SQLite's primary code. These are application-defined extended codes, not
 * SQLite upstream assignments. Reserved categories need not be emitted by
 * every build or operation. Never infer a category from an error message.
 *
 * Call sqlite3_extended_result_codes(db, 1) to receive extended return codes.
 * sqlite3_extended_errcode(db) also reports them with that setting disabled.
 * sqlite3_errstr() describes the primary code; error_name() below identifies
 * the Murmur category. Use sqlite3_errmsg(db) for the operation's message.
 */
typedef enum sqlite_murmur_error_code {
    SQLITE_MURMUR_INVALID = (0x5301 << 8) | SQLITE_ERROR,
    SQLITE_MURMUR_CORRUPT = (0x5302 << 8) | SQLITE_CORRUPT,
    SQLITE_MURMUR_WRONG_KEY = (0x5303 << 8) | SQLITE_NOTADB,
    SQLITE_MURMUR_SCHEMA_MISMATCH = (0x5304 << 8) | SQLITE_SCHEMA,
    SQLITE_MURMUR_BUSY = (0x5305 << 8) | SQLITE_BUSY,
    SQLITE_MURMUR_CLOSED = (0x5306 << 8) | SQLITE_MISUSE,
    SQLITE_MURMUR_CALLBACK_PANICKED = (0x5307 << 8) | SQLITE_ABORT,
    SQLITE_MURMUR_STORAGE = (0x5308 << 8) | SQLITE_IOERR,
    SQLITE_MURMUR_IO = (0x5309 << 8) | SQLITE_IOERR,
    SQLITE_MURMUR_LIMIT = (0x530a << 8) | SQLITE_TOOBIG,
    SQLITE_MURMUR_PROTOCOL = (0x530b << 8) | SQLITE_PROTOCOL,
    SQLITE_MURMUR_AUTH = (0x530c << 8) | SQLITE_AUTH,
    SQLITE_MURMUR_UNAVAILABLE = (0x530d << 8) | SQLITE_CANTOPEN,
    SQLITE_MURMUR_OVERLOADED = (0x530e << 8) | SQLITE_BUSY,
    SQLITE_MURMUR_CANCELED = (0x530f << 8) | SQLITE_INTERRUPT
} sqlite_murmur_error_code;

#ifdef __cplusplus
extern "C" {
#endif

/* Returns a static Rust category name, or NULL for any unrecognized code,
 * including native SQLite codes and unknown Murmur extensions. No allocation,
 * database access, or mutable global state. Safe to call concurrently. */
const char *sqlite_murmur_error_name(int code);

/* Returns code & 0xff for native and Murmur SQLite result codes alike. */
int sqlite_murmur_error_primary(int code);

#ifdef __cplusplus
}
#endif
