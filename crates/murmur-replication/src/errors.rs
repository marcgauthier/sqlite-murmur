//! Typed result codes projecting the archived error taxonomy onto SQLite
//! extended result codes.
//!
//! Canonical values live in `include/sqlite_murmur_errors.h`; the constants
//! below mirror them (the C++ suite pins the values, the Rust suite pins
//! this mapping). Every murmur code keeps its SQLite primary in the low
//! byte, so `code & 0xff` stays meaningful to generic handlers. UDF
//! failures surface through `rusqlite::Error::SqliteFailure`, which writes
//! the extended code plus the message.
//!
//! Ownership (CORRUPT-unless-verified): Rust emits only categories it can
//! verify. WRONG_KEY versus CORRUPT is decided by the C++ VFS decrypt
//! path, which must prove a key mismatch before reporting WRONG_KEY and
//! report CORRUPT otherwise. BUSY/CLOSED/CANCELED/SCHEMA_MISMATCH are
//! C-side or reserved for future emitters.

use crate::Error;
use rusqlite::ffi;

pub(crate) const INVALID: i32 = (0x5301 << 8) | ffi::SQLITE_ERROR;
pub(crate) const CORRUPT: i32 = (0x5302 << 8) | ffi::SQLITE_CORRUPT;
pub(crate) const WRONG_KEY: i32 = (0x5303 << 8) | ffi::SQLITE_NOTADB;
pub(crate) const SCHEMA_MISMATCH: i32 = (0x5304 << 8) | ffi::SQLITE_SCHEMA;
pub(crate) const BUSY: i32 = (0x5305 << 8) | ffi::SQLITE_BUSY;
pub(crate) const CLOSED: i32 = (0x5306 << 8) | ffi::SQLITE_MISUSE;
pub(crate) const CALLBACK_PANICKED: i32 = (0x5307 << 8) | ffi::SQLITE_ABORT;
pub(crate) const STORAGE: i32 = (0x5308 << 8) | ffi::SQLITE_IOERR;
pub(crate) const IO: i32 = (0x5309 << 8) | ffi::SQLITE_IOERR;
pub(crate) const LIMIT: i32 = (0x530A << 8) | ffi::SQLITE_TOOBIG;
pub(crate) const PROTOCOL: i32 = (0x530B << 8) | ffi::SQLITE_PROTOCOL;
pub(crate) const AUTH: i32 = (0x530C << 8) | ffi::SQLITE_AUTH;
pub(crate) const UNAVAILABLE: i32 = (0x530D << 8) | ffi::SQLITE_CANTOPEN;
pub(crate) const OVERLOADED: i32 = (0x530E << 8) | ffi::SQLITE_BUSY;
pub(crate) const CANCELED: i32 = (0x530F << 8) | ffi::SQLITE_INTERRUPT;

/// Extended result code for a replication error. Native SQLite failures
/// (including user SQL run by batch exec) pass through untouched so
/// generic handlers keep working; non-SQLite rusqlite errors are
/// internal data-path failures and map to STORAGE.
pub(crate) fn murmur_code(e: &Error) -> i32 {
    match e {
        Error::Invalid(_) => INVALID,
        Error::Protocol(_) => PROTOCOL,
        Error::Auth(_) => AUTH,
        Error::Unavailable(_) => UNAVAILABLE,
        Error::Overloaded => OVERLOADED,
        Error::Limit(_) => LIMIT,
        Error::Io(_) => IO,
        Error::Sql(rusqlite::Error::SqliteFailure(f, _)) => f.extended_code,
        Error::Sql(_) => STORAGE,
    }
}

/// Map archived scheduler errors onto local errors. Busy lands on
/// Overloaded (its primary is SQLITE_BUSY); a closed scheduler reads
/// as Unavailable since only teardown closes it.
pub(crate) fn sched_err(e: murmur_sched::Error) -> crate::Error {
    match e {
        murmur_sched::Error::Invalid(s) => crate::Error::Invalid(s),
        murmur_sched::Error::Busy => crate::Error::Overloaded,
        murmur_sched::Error::Closed => {
            crate::Error::Unavailable("writer scheduler closed".into())
        }
    }
}

/// Category name matching `sqlite_murmur_error_name`; native passthrough
/// reports "Native" (the C helper returns NULL for non-murmur codes).
pub(crate) fn murmur_kind(e: &Error) -> &'static str {
    match e {
        Error::Invalid(_) => "Invalid",
        Error::Protocol(_) => "Protocol",
        Error::Auth(_) => "Auth",
        Error::Unavailable(_) => "Unavailable",
        Error::Overloaded => "Overloaded",
        Error::Limit(_) => "Limit",
        Error::Io(_) => "Io",
        Error::Sql(rusqlite::Error::SqliteFailure(_, _)) => "Native",
        Error::Sql(_) => "Storage",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_match_canonical_header_values() {
        // Pinned against include/sqlite_murmur_errors.h.
        assert_eq!(INVALID, 0x530101);
        assert_eq!(CORRUPT, 0x53020B);
        assert_eq!(WRONG_KEY, 0x53031A);
        assert_eq!(SCHEMA_MISMATCH, 0x530411);
        assert_eq!(BUSY, 0x530505);
        assert_eq!(CLOSED, 0x530615);
        assert_eq!(CALLBACK_PANICKED, 0x530704);
        assert_eq!(STORAGE, 0x53080A);
        assert_eq!(IO, 0x53090A);
        assert_eq!(LIMIT, 0x530A12);
        assert_eq!(PROTOCOL, 0x530B0F);
        assert_eq!(AUTH, 0x530C17);
        assert_eq!(UNAVAILABLE, 0x530D0E);
        assert_eq!(OVERLOADED, 0x530E05);
        assert_eq!(CANCELED, 0x530F09);
    }

    #[test]
    fn mapping_covers_local_variants() {
        use rusqlite::ffi;
        let cases = [
            (Error::Invalid("x".into()), INVALID, "Invalid", ffi::SQLITE_ERROR),
            (
                Error::Protocol("x".into()),
                PROTOCOL,
                "Protocol",
                ffi::SQLITE_PROTOCOL,
            ),
            (Error::Auth("x".into()), AUTH, "Auth", ffi::SQLITE_AUTH),
            (
                Error::Unavailable("x".into()),
                UNAVAILABLE,
                "Unavailable",
                ffi::SQLITE_CANTOPEN,
            ),
            (Error::Overloaded, OVERLOADED, "Overloaded", ffi::SQLITE_BUSY),
            (Error::Limit("x".into()), LIMIT, "Limit", ffi::SQLITE_TOOBIG),
            (
                Error::Io(std::io::Error::other("x")),
                IO,
                "Io",
                ffi::SQLITE_IOERR,
            ),
        ];
        for (e, code, kind, primary) in cases {
            assert_eq!(murmur_code(&e), code);
            assert_eq!(murmur_kind(&e), kind);
            // Low byte preserves the SQLite primary so generic handlers
            // keep working; the facility lives in the high bytes.
            assert_eq!(code & 0xff, primary);
        }
    }

    #[test]
    fn native_failures_pass_through_others_map_to_storage() {
        let native = Error::Sql(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(19),
            Some("UNIQUE".into()),
        ));
        assert_eq!(murmur_code(&native), 19);
        assert_eq!(murmur_kind(&native), "Native");
        let internal = Error::Sql(rusqlite::Error::InvalidQuery);
        assert_eq!(murmur_code(&internal), STORAGE);
        assert_eq!(murmur_kind(&internal), "Storage");
    }
}
