// Live query subscriptions (point 17). Poll-based C API over the Rust
// subscription manager: subscribe with a SELECT plus watched tables,
// then poll for Initial/Update/Reset events as JSON.
//
// The SELECT runs on the replication worker connection when events are
// produced, so it must not reference TEMP objects or connection-local
// state. Watched tables are matched against committed table names;
// commits touching only other tables stay silent. JSON row encoding is
// {"column": value} with integers/floats as numbers, text as strings,
// blobs as lowercase hex strings, and NULLs as null.
//
// Threading: a subscription may be polled from only one thread at a
// time. murmur_sub_close joins the subscription worker; polling from
// another thread during close is undefined behavior. An open
// subscription pins its database worker: close all subscriptions
// before closing the database and unregistering the VFS.
#pragma once
#include <sqlite3.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct murmur_sub murmur_sub;

typedef struct {
    int buffer_size;             // default 32; clamped to >= 1
    int emit_unchanged;          // default 0; nonzero emits no-change events
    const char *resume_cursor_json; // default NULL; resume from a prior cursor
} murmur_sub_opts;

// Open a subscription. Returns NULL when murmur is not registered on db,
// when the arguments are invalid, or when worker initialization fails.
// opts may be NULL for defaults.
murmur_sub *murmur_subscribe(sqlite3 *db, const char *sql,
                             const char *tables_json,
                             const murmur_sub_opts *opts);

// Wait up to timeout_ms for the next event (< 0 waits indefinitely).
// Returns 1 with *event_json set (release with murmur_sub_free_event),
// 0 on timeout (*event_json set to NULL), or a negative code when the
// subscription is closed or has failed (*event_json set to NULL).
int murmur_sub_poll(murmur_sub *s, int timeout_ms, char **event_json);

// Release an event string returned by murmur_sub_poll. NULL-safe.
void murmur_sub_free_event(char *event_json);

// Close a subscription and join its worker. NULL-safe.
void murmur_sub_close(murmur_sub *s);

#ifdef __cplusplus
}
#endif
