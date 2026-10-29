#pragma once

// Provenance (constraint 3): adapted from archive MURMUR-RUST src/backup.rs.
// Kept (structure): dest-must-not-exist + remove-on-any-failure from
// copy_store_dir/copy_checkpoint_files; 0600 file / fsync data + dir
// discipline from copy_file_sync/sync_dir. Dropped: all Spool-directory
// mechanics (manifest/keys.enc/segments layout, COMPLETE marker) — the
// destination store is a RocksDB directory, so the copy step is
// rocksdb::Checkpoint. No archive code is copied verbatim; the
// manifest.json schema below is new. Lead rulings: A (manifest travels
// inside the artifact, R2 precedent; never key material), B (FCTL opcode
// 0x535803, defined in sqlite_murmur.h by lead merge), C (Checkpoint
// copies cross-filesystem; document the perf note, no same-fs rule).

#include <cstdint>
#include <sqlite3.h>

namespace rocksdb {
class DB;
}

// Argument block for SQLITE_MURMUR_FCNTL_BACKUP (opcode lives in
// sqlite_murmur.h). Plain-old-data with C layout: the Rust murmur_backup()
// UDF fills this (repr(C)) from murmur tables and passes its address as
// the sqlite3_file_control argument.
struct sqlite_murmur_backup_args {
    const char *dest;       // Backup directory. Must not exist; created here.
    unsigned char group[16]; // murmur_config.group_id, manifest identity only.
    unsigned char node[16];  // murmur_identity.node, manifest identity only.
};

// Manifest constants. The manifest carries identity + consistency markers
// only; it must never carry key material (enforced by tests/backup.cpp,
// which rejects any field outside this schema).
#define SQLITE_MURMUR_BACKUP_MANIFEST "manifest.json"
#define SQLITE_MURMUR_BACKUP_FORMAT_VERSION 1

// Run a consistent online backup: RocksDB checkpoint of db into args->dest,
// then manifest.json (0600, fsynced, dir fsynced). The caller flushes
// completed murmur transactions first (Store::flush in the FCTL dispatch),
// so the checkpoint observes every completed transaction.
// Returns SQLITE_OK on success (dest holds checkpoint + manifest) or a
// SQLITE_* error; on ANY failure dest is removed (no partial artifact).
// seq_out (optional) receives the checkpoint sequence number recorded in
// the manifest. All inputs are validated; null/empty/bad input returns
// SQLITE_MISUSE/SQLITE_ERROR without touching the filesystem.
int sqlite_murmur_backup_run(rocksdb::DB *db, const char *cipher,
                             const sqlite_murmur_backup_args *args,
                             std::uint64_t *seq_out);

// Force a full-range RocksDB compaction for space-amplification control
// (WP-D slice D3b; FCTL 0x535804 dispatches here after Store::flush).
// This is a store-native operation, not an archive port: unlike
// checkpoint(), which only flushes memtables and waits for automatic
// compactions, this compacts the full key range synchronously. Blocking:
// large stores stall here, which callers must document. Null db returns
// SQLITE_MISUSE; compaction failure returns SQLITE_IOERR_WRITE.
int sqlite_murmur_compact_run(rocksdb::DB *db);
