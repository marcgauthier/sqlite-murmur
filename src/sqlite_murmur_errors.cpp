#include "sqlite_murmur_errors.h"

/* Project the archived Rust Error variants; see provenance in the header.
 * Match full codes so a discriminator with the wrong primary is rejected. */
extern "C" const char *sqlite_murmur_error_name(int code) {
    switch (code) {
    case SQLITE_MURMUR_INVALID: return "Invalid";
    case SQLITE_MURMUR_CORRUPT: return "Corrupt";
    case SQLITE_MURMUR_WRONG_KEY: return "WrongKey";
    case SQLITE_MURMUR_SCHEMA_MISMATCH: return "SchemaMismatch";
    case SQLITE_MURMUR_BUSY: return "Busy";
    case SQLITE_MURMUR_CLOSED: return "Closed";
    case SQLITE_MURMUR_CALLBACK_PANICKED: return "CallbackPanicked";
    case SQLITE_MURMUR_STORAGE: return "Storage";
    case SQLITE_MURMUR_IO: return "Io";
    case SQLITE_MURMUR_LIMIT: return "Limit";
    case SQLITE_MURMUR_PROTOCOL: return "Protocol";
    case SQLITE_MURMUR_AUTH: return "Auth";
    case SQLITE_MURMUR_UNAVAILABLE: return "Unavailable";
    case SQLITE_MURMUR_OVERLOADED: return "Overloaded";
    case SQLITE_MURMUR_CANCELED: return "Canceled";
    default: return nullptr;
    }
}

extern "C" int sqlite_murmur_error_primary(int code) {
    return code & 0xff;
}
