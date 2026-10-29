#pragma once

// murmur-admin: offline-only operational CLI (WP-D slice D3a). All commands
// refuse live targets (fcntl probe on the store LOCK plus open as backstop)
// and never open a live database read-write. Key material travels from
// --key-hex into sqlite_murmur_register memory only; it is wiped after
// registration and never printed, logged, or embedded in JSON output.

#include <cstdint>
#include <string>
#include <vector>

struct AdminOpts {
    bool json = false;
    bool is_backup = false; // verify --backup: expect artifact manifest
    std::string key_hex;    // 64 hex chars (32 bytes), required
    std::string key_file;   // path to file containing 64 hex chars or 32 raw bytes
    std::string new_key_hex; // 64 hex chars for rekey
    std::string new_key_file; // path to file containing new key
    bool fresh_node = false;
    bool fresh_db = false; // reserved: group/database identity (see restore)
    std::string fresh_node_hex; // 32 hex chars when explicitly given
    std::string fresh_db_hex;
};

// Parsed key bytes. Wipe with admin_wipe() once registered.
struct AdminKey {
    unsigned char bytes[32];
    bool ok = false;
};

AdminKey admin_parse_key_hex(const std::string &hex);
void admin_wipe(AdminKey &key);
bool admin_parse_id16(const std::string &hex, unsigned char out[16]);

// Register the VFS store key for this process (idempotent for the same
// key; a different key is refused). Commands call this internally;
// test setup calls it once before creating scratch databases.
bool admin_ensure_registered(const std::string &key_hex, std::ostream &err);

// True when another process holds the store lock (diagnostic probe).
// Not enforcement: the subsequent open is the backstop.
bool admin_store_locked(const std::string &dir, std::string &detail);

// Recursive copy with 0600/0700 discipline; dest must not exist; any
// failure removes dest. Returns 0 on success, 1 on failure (msg set).
int admin_copy_tree(const std::string &src, const std::string &dest,
                    std::string &msg);

// Each command returns a process exit code (0 ok, 1 failure) and prints
// human or --json output. Out/err streams are injectable for tests.
int admin_cmd_verify(const std::string &target, const AdminOpts &opts,
                     std::ostream &out, std::ostream &err);
int admin_cmd_restore(const std::string &backup, const std::string &dest,
                      const AdminOpts &opts, std::ostream &out,
                      std::ostream &err);
int admin_cmd_status(const std::string &dir, const AdminOpts &opts,
                     std::ostream &out, std::ostream &err);
int admin_cmd_inspect(const std::string &dir, const AdminOpts &opts,
                      std::ostream &out, std::ostream &err);
int admin_cmd_repair(const std::string &dir, const AdminOpts &opts,
                     std::ostream &out, std::ostream &err);
int admin_cmd_keys(const std::string &dir, const AdminOpts &opts,
                   std::ostream &out, std::ostream &err);
int admin_cmd_rekey(const std::string &dir, const AdminOpts &opts,
                    std::ostream &out, std::ostream &err);

// argv dispatch (used by main). Returns process exit code.
int admin_main(int argc, char **argv, std::ostream &out, std::ostream &err);
