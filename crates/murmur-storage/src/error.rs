//! Error taxonomy projection. Rust port of `src/sqlite_murmur_errors.cpp`.
//!
//! Canonical C ABI: `include/sqlite_murmur_errors.h`. Murmur uses extension
//! discriminators `0x5301..=0x530f` with the SQLite primary in the low
//! byte. Native SQLite codes pass through [`primary`] and are unknown to
//! [`name`], exactly like the C++ implementation.

use std::ffi::{CStr, c_char, c_int};
use std::ptr;

// SQLite primary result codes (`third-party/sqlite/sqlite3.h`). These are
// ABI-stable upstream values; the C++ suite (`tests/errors.cpp`) pins the
// composed codes against the real macros on every build.
pub(crate) const SQLITE_OK: i32 = 0;
pub(crate) const SQLITE_ERROR: i32 = 1;
const SQLITE_ABORT: i32 = 4;
const SQLITE_BUSY: i32 = 5;
const SQLITE_INTERRUPT: i32 = 9;
const SQLITE_IOERR: i32 = 10;
pub(crate) const SQLITE_CORRUPT: i32 = 11;
const SQLITE_CANTOPEN: i32 = 14;
const SQLITE_PROTOCOL: i32 = 15;
const SQLITE_SCHEMA: i32 = 17;
const SQLITE_TOOBIG: i32 = 18;
pub(crate) const SQLITE_MISUSE: i32 = 21;
pub(crate) const SQLITE_AUTH: i32 = 23;
const SQLITE_NOTADB: i32 = 26;
pub(crate) const SQLITE_IOERR_WRITE: i32 = SQLITE_IOERR | (3 << 8);

/// Murmur extended result codes, mirroring `sqlite_murmur_error_code`.
pub const INVALID: i32 = (0x5301 << 8) | SQLITE_ERROR;
pub const CORRUPT: i32 = (0x5302 << 8) | SQLITE_CORRUPT;
pub const WRONG_KEY: i32 = (0x5303 << 8) | SQLITE_NOTADB;
pub const SCHEMA_MISMATCH: i32 = (0x5304 << 8) | SQLITE_SCHEMA;
pub const BUSY: i32 = (0x5305 << 8) | SQLITE_BUSY;
pub const CLOSED: i32 = (0x5306 << 8) | SQLITE_MISUSE;
pub const CALLBACK_PANICKED: i32 = (0x5307 << 8) | SQLITE_ABORT;
pub const STORAGE: i32 = (0x5308 << 8) | SQLITE_IOERR;
pub const IO: i32 = (0x5309 << 8) | SQLITE_IOERR;
pub const LIMIT: i32 = (0x530A << 8) | SQLITE_TOOBIG;
pub const PROTOCOL: i32 = (0x530B << 8) | SQLITE_PROTOCOL;
pub const AUTH: i32 = (0x530C << 8) | SQLITE_AUTH;
pub const UNAVAILABLE: i32 = (0x530D << 8) | SQLITE_CANTOPEN;
pub const OVERLOADED: i32 = (0x530E << 8) | SQLITE_BUSY;
pub const CANCELED: i32 = (0x530F << 8) | SQLITE_INTERRUPT;

/// Static category name for a result code, or `None` for any unrecognized
/// code, including native SQLite codes and malformed Murmur extensions.
#[must_use]
pub fn name(code: i32) -> Option<&'static CStr> {
    match code {
        INVALID => Some(c"Invalid"),
        CORRUPT => Some(c"Corrupt"),
        WRONG_KEY => Some(c"WrongKey"),
        SCHEMA_MISMATCH => Some(c"SchemaMismatch"),
        BUSY => Some(c"Busy"),
        CLOSED => Some(c"Closed"),
        CALLBACK_PANICKED => Some(c"CallbackPanicked"),
        STORAGE => Some(c"Storage"),
        IO => Some(c"Io"),
        LIMIT => Some(c"Limit"),
        PROTOCOL => Some(c"Protocol"),
        AUTH => Some(c"Auth"),
        UNAVAILABLE => Some(c"Unavailable"),
        OVERLOADED => Some(c"Overloaded"),
        CANCELED => Some(c"Canceled"),
        _ => None,
    }
}

/// Low-byte SQLite primary for native and Murmur result codes alike.
#[must_use]
pub const fn primary(code: i32) -> i32 {
    code & 0xff
}

/// C ABI: static category name, or NULL for unrecognized codes.
/// No allocation or global state; safe to call concurrently.
#[unsafe(no_mangle)]
pub extern "C" fn sqlite_murmur_error_name(code: c_int) -> *const c_char {
    name(code).map_or(ptr::null(), CStr::as_ptr)
}

/// C ABI: `code & 0xff`.
#[unsafe(no_mangle)]
pub extern "C" fn sqlite_murmur_error_primary(code: c_int) -> c_int {
    primary(code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_match_canonical_header_values() {
        // Pinned against include/sqlite_murmur_errors.h (same values the
        // C++ suite derives from the SQLITE_* macros).
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
    fn names_and_primaries_round_trip() {
        let cases = [
            (INVALID, "Invalid", SQLITE_ERROR),
            (CORRUPT, "Corrupt", SQLITE_CORRUPT),
            (WRONG_KEY, "WrongKey", SQLITE_NOTADB),
            (SCHEMA_MISMATCH, "SchemaMismatch", SQLITE_SCHEMA),
            (BUSY, "Busy", SQLITE_BUSY),
            (CLOSED, "Closed", SQLITE_MISUSE),
            (CALLBACK_PANICKED, "CallbackPanicked", SQLITE_ABORT),
            (STORAGE, "Storage", SQLITE_IOERR),
            (IO, "Io", SQLITE_IOERR),
            (LIMIT, "Limit", SQLITE_TOOBIG),
            (PROTOCOL, "Protocol", SQLITE_PROTOCOL),
            (AUTH, "Auth", SQLITE_AUTH),
            (UNAVAILABLE, "Unavailable", SQLITE_CANTOPEN),
            (OVERLOADED, "Overloaded", SQLITE_BUSY),
            (CANCELED, "Canceled", SQLITE_INTERRUPT),
        ];
        for (code, expected, primary_code) in cases {
            assert_eq!(name(code).unwrap().to_str().unwrap(), expected);
            assert_eq!(primary(code), primary_code);
            // A recognized discriminator with an unrelated low byte is unknown.
            assert_eq!(name(code ^ 0xff), None);
        }
    }

    #[test]
    fn native_and_unknown_codes_are_unnamed() {
        for code in [0, 1, 100, 101, 10 | (2 << 8), 19 | (6 << 8), 0x531001, -1] {
            assert_eq!(name(code), None);
            assert_eq!(primary(code), code & 255);
        }
    }

    #[test]
    fn c_abi_returns_null_terminated_or_null() {
        // SAFETY: the returned pointers are static storage or null.
        unsafe {
            let named = sqlite_murmur_error_name(INVALID);
            assert!(!named.is_null());
            assert_eq!(CStr::from_ptr(named).to_str().unwrap(), "Invalid");
            assert!(sqlite_murmur_error_name(1).is_null());
            assert_eq!(sqlite_murmur_error_primary(INVALID), SQLITE_ERROR);
        }
    }
}
