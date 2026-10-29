#pragma once

// murmur-admin: offline-only operational CLI (WP-D slice D3a). All commands
// refuse live targets (fcntl probe on the store LOCK plus open as backstop)
// and never open a live database read-write. Key material travels from
// --key-hex into sqlite_murmur_register memory only; it is wiped after
// registration and never printed, logged, or embedded in JSON output.
//
// Implemented in Rust (crates/murmur-admin); this header declares its
// stable C ABI. The previous C++ API (AdminOpts/AdminKey, injected
// ostreams) was removed with the migration; the `murmur_admin_` names
// are intentionally distinct so old C++ call sites fail loudly at link
// time instead of silently mis-binding a changed signature. Capture
// buffers (`char **`) replace the streams: each call sets them to
// NUL-terminated text (possibly empty; NULL discards a stream) and the
// caller releases them with murmur_admin_free_str.

#ifdef __cplusplus
extern "C" {
#endif

// Command options (mirrors the old AdminOpts). All strings are
// NUL-terminated; NULL means empty.
typedef struct murmur_admin_opts {
    int json;
    int is_backup; // verify --backup: expect artifact manifest
    const char *key_hex;     // 64 hex chars (32 bytes), required
    const char *key_file;    // path to file containing 64 hex chars or 32 raw bytes
    const char *new_key_hex; // 64 hex chars for rekey
    const char *new_key_file; // path to file containing new key
    int fresh_node;
    int fresh_db; // reserved: group/database identity (see restore)
    const char *fresh_node_hex; // 32 hex chars when explicitly given
    const char *fresh_db_hex;
} murmur_admin_opts_t;

// Parsed key bytes. Wipe with murmur_admin_wipe_key once registered.
typedef struct murmur_admin_key {
    unsigned char bytes[32];
    int ok;
} murmur_admin_key_t;

// 1 on success (out filled), 0 on bad input. Either pointer may be NULL.
int murmur_admin_parse_key_hex(const char *hex, murmur_admin_key_t *out);
void murmur_admin_wipe_key(murmur_admin_key_t *key); // null-safe
// 1 on success (out holds 16 bytes), 0 on bad input.
int murmur_admin_parse_id16(const char *hex, unsigned char out[16]);

// Register the VFS store key for this process (idempotent for the same
// key; a different key is refused). Commands call this internally;
// test setup calls it once before creating scratch databases.
// Returns 1 on success, 0 on failure (detail in *err).
int murmur_admin_ensure_registered(const char *key_hex, char **err);

// 1 when another process holds the store lock (detail set), 0 when
// free (detail set). Not enforcement: the subsequent open is the
// backstop.
int murmur_admin_store_locked(const char *dir, char **detail);

// Recursive copy with 0600/0700 discipline; dest must not exist; any
// failure removes dest. Returns 0 on success, 1 on failure (msg set).
int murmur_admin_copy_tree(const char *src, const char *dest, char **msg);

// Each command returns a process exit code (0 ok, 1 failure) and writes
// human or --json output to the captures.
int murmur_admin_cmd_verify(const char *target,
                            const murmur_admin_opts_t *opts, char **out,
                            char **err);
int murmur_admin_cmd_restore(const char *backup, const char *dest,
                             const murmur_admin_opts_t *opts, char **out,
                             char **err);
int murmur_admin_cmd_status(const char *dir, const murmur_admin_opts_t *opts,
                            char **out, char **err);
int murmur_admin_cmd_inspect(const char *dir, const murmur_admin_opts_t *opts,
                             char **out, char **err);
int murmur_admin_cmd_repair(const char *dir, const murmur_admin_opts_t *opts,
                            char **out, char **err);
int murmur_admin_cmd_keys(const char *dir, const murmur_admin_opts_t *opts,
                          char **out, char **err);
int murmur_admin_cmd_rekey(const char *dir, const murmur_admin_opts_t *opts,
                           char **out, char **err);

// argv dispatch (used by main). Returns process exit code.
int murmur_admin_main(int argc, char **argv, char **out, char **err);

// Release a capture buffer from any murmur_admin_* call. Null-safe.
void murmur_admin_free_str(char *s);

#ifdef __cplusplus
} // extern "C"
#endif
