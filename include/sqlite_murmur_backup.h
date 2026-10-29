#pragma once

// Online hot backup helper. Implemented in Rust
// (crates/murmur-storage/src/backup.rs); this header declares its stable
// C ABI. The previous C++ API (sqlite_murmur_backup_run /
// sqlite_murmur_compact_run over rocksdb::DB*) was removed when the
// storage layer migrated to Rust; the `_rs_` names are intentionally
// distinct so old C++ call sites fail loudly at link time instead of
// silently mis-binding a changed signature.
//
// Provenance (constraint 3): adapted from archive MURMUR-RUST
// src/backup.rs. Kept (structure): dest-must-not-exist +
// remove-on-any-failure from copy_store_dir/copy_checkpoint_files; 0600
// file / fsync data + dir discipline from copy_file_sync/sync_dir.
// Dropped: all Spool-directory mechanics (manifest/keys.enc/segments
// layout, COMPLETE marker) — the destination store is a RocksDB
// directory, so the copy step is rocksdb::Checkpoint. Lead rulings: A
// (manifest travels inside the artifact, R2 precedent; never key
// material), B (FCTL opcode 0x535803, defined in sqlite_murmur.h by lead
// merge), C (Checkpoint copies cross-filesystem; document the perf note,
// no same-fs rule).

#include <sqlite3.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Argument block for SQLITE_MURMUR_FCNTL_BACKUP (opcode lives in
// sqlite_murmur.h). Plain-old-data with C layout.
struct sqlite_murmur_backup_args {
    const char *dest;        // Backup directory. Must not exist; created here.
    unsigned char group[16]; // murmur_config.group_id, manifest identity only.
    unsigned char node[16];  // murmur_identity.node, manifest identity only.
};

// Manifest constants. The manifest carries identity + consistency markers
// only; it must never carry key material (enforced by tests/backup.cpp,
// which rejects any field outside this schema).
#define SQLITE_MURMUR_BACKUP_MANIFEST "manifest.json"
#define SQLITE_MURMUR_BACKUP_FORMAT_VERSION 1

// Checkpoint callback: create a consistent RocksDB checkpoint of the
// store behind `ctx` at `dest`, reporting its sequence number in `*seq`.
// Returns SQLITE_OK or a SQLITE_* error. The in-tree store passes its
// C-API handle as `ctx`; out-of-tree callers (tests) pass a
// `rocksdb::DB *` (as `void *`) and adapt with rocksdb::Checkpoint.
typedef int (*sqlite_murmur_backup_checkpoint_fn)(void *ctx, const char *dest,
                                                  uint64_t *seq);

// Compaction callback: force a full-range synchronous RocksDB
// compaction of the store behind `ctx`. Blocking: large stores stall
// here. Returns SQLITE_OK or a SQLITE_* error.
typedef int (*sqlite_murmur_backup_compact_fn)(void *ctx);

// Run a consistent online backup: checkpoint via `checkpoint`, then
// manifest.json (0600, fsynced, dir fsynced). `cipher` is the
// NUL-terminated cipher name (charset-checked); `group_id`/`node_id`
// are 16 bytes each. `seq_out` (optional) receives the checkpoint
// sequence number on success only. Returns SQLITE_OK on success (dest
// holds checkpoint + manifest) or a SQLITE_* error; on ANY failure
// dest is removed (no partial artifact). Null dest/cipher/ids/ctx or
// a null callback returns SQLITE_MISUSE without touching anything.
int sqlite_murmur_backup_rs_run(const char *dest, const char *cipher,
                                const unsigned char *group_id,
                                const unsigned char *node_id,
                                uint64_t *seq_out, void *ctx,
                                sqlite_murmur_backup_checkpoint_fn checkpoint);

// Force a full-range RocksDB compaction for space-amplification control
// (WP-D slice D3b; FCTL 0x535804 dispatches here after Store::flush).
// Unlike checkpoint(), which only flushes memtables, this compacts the
// full key range synchronously. Null ctx/callback returns SQLITE_MISUSE;
// callback failure returns SQLITE_IOERR_WRITE.
int sqlite_murmur_backup_rs_compact(void *ctx,
                                    sqlite_murmur_backup_compact_fn compact);

#ifdef __cplusplus
} // extern "C"
#endif
