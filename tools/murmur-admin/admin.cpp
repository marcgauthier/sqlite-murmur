// murmur-admin implementation (WP-D slice D3a). Offline-only: every command
// probes the store LOCK first and opens targets read-only, except restore,
// which rewrites identity on the destination COPY through a temporary
// STORAGE_ONLY handle. Provenance: command surface adapted from archive
// MURMUR-RUST src/bin/admin.rs (status/inspect/verify/restore/keys verbs,
// --json, --key-hex, --fresh-*-id flags, exit 0/1); all storage mechanics
// are destination-native (sqlite_murmur_* C API, no Spool). The archive's
// --passphrase (plain SHA256) is deliberately NOT ported: passphrases
// arrive with the D2 Argon2id protector. Full notes in PROVENANCE.md.

#include "admin.h"

#include <sqlite_murmur.h>
#include <sqlite3.h>

#include <cctype>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>
#include <sstream>

#ifndef _WIN32
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#endif

namespace {

namespace fs = std::filesystem;

const char *kVersion = "0.1.0";

int hex_val(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

bool hex_to_bytes(const std::string &hex, unsigned char *out, std::size_t n) {
    if (hex.size() != n * 2) return false;
    for (std::size_t i = 0; i < n; ++i) {
        const int hi = hex_val(hex[2 * i]);
        const int lo = hex_val(hex[2 * i + 1]);
        if (hi < 0 || lo < 0) return false;
        out[i] = static_cast<unsigned char>((hi << 4) | lo);
    }
    return true;
}

std::string bytes_to_hex(const unsigned char *bytes, std::size_t n) {
    static const char *digits = "0123456789abcdef";
    std::string out;
    out.reserve(n * 2);
    for (std::size_t i = 0; i < n; ++i) {
        out.push_back(digits[bytes[i] >> 4]);
        out.push_back(digits[bytes[i] & 0xf]);
    }
    return out;
}

std::string json_escape(const std::string &s) {
    std::string out;
    for (char c : s) {
        switch (c) {
        case '"': out += "\\\""; break;
        case '\\': out += "\\\\"; break;
        case '\n': out += "\\n"; break;
        case '\r': out += "\\r"; break;
        case '\t': out += "\\t"; break;
        default:
            if (static_cast<unsigned char>(c) < 0x20)
                out += "\\u00" + bytes_to_hex(
                           reinterpret_cast<const unsigned char *>(&c), 1);
            else
                out.push_back(c);
        }
    }
    return out;
}

bool random_bytes(unsigned char *out, std::size_t n) {
#ifndef _WIN32
    std::ifstream rnd("/dev/urandom", std::ios::binary);
    if (!rnd) return false;
    rnd.read(reinterpret_cast<char *>(out), static_cast<std::streamsize>(n));
    return static_cast<bool>(rnd);
#else
    (void)out;
    (void)n;
    return false;
#endif
}

// Recursive size + file count (follows nothing; LOCK and journals counted).
void dir_usage(const fs::path &dir, std::uint64_t &bytes, std::size_t &files) {
    std::error_code ec;
    for (fs::recursive_directory_iterator it(dir, ec), end; it != end; it.increment(ec)) {
        if (ec) break;
        std::error_code ec2;
        if (it->is_regular_file(ec2)) {
            ++files;
            bytes += it->file_size(ec2);
        }
    }
}

struct DbCloser {
    void operator()(sqlite3 *db) const {
        if (db) sqlite_murmur_close(db);
    }
};
using DbPtr = std::unique_ptr<sqlite3, DbCloser>;

bool exec_all(sqlite3 *db, const char *sql, std::string &msg) {
    char *err = nullptr;
    const int rc = sqlite3_exec(db, sql, nullptr, nullptr, &err);
    if (rc != SQLITE_OK) {
        msg = err ? err : sqlite3_errstr(rc);
        sqlite3_free(err);
        return false;
    }
    return true;
}

// First column of the first row as text (empty when no rows).
bool query_text(sqlite3 *db, const std::string &sql, std::string &out,
                std::string &msg) {
    sqlite3_stmt *st = nullptr;
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &st, nullptr) != SQLITE_OK) {
        msg = sqlite3_errmsg(db);
        return false;
    }
    bool ok = true;
    const int rc = sqlite3_step(st);
    if (rc == SQLITE_ROW) {
        const auto *t = sqlite3_column_text(st, 0);
        out = t ? reinterpret_cast<const char *>(t) : "";
    } else if (rc != SQLITE_DONE) {
        msg = sqlite3_errmsg(db);
        ok = false;
    }
    sqlite3_finalize(st);
    return ok;
}

bool query_blob(sqlite3 *db, const std::string &sql, std::vector<unsigned char> &out,
                bool &is_null, std::string &msg) {
    sqlite3_stmt *st = nullptr;
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &st, nullptr) != SQLITE_OK) {
        msg = sqlite3_errmsg(db);
        return false;
    }
    bool ok = true;
    const int rc = sqlite3_step(st);
    if (rc == SQLITE_ROW) {
        if (sqlite3_column_type(st, 0) == SQLITE_NULL) {
            is_null = true;
        } else {
            is_null = false;
            const auto *p = static_cast<const unsigned char *>(
                sqlite3_column_blob(st, 0));
            const int n = sqlite3_column_bytes(st, 0);
            out.assign(p, p + (n > 0 ? n : 0));
        }
    } else if (rc != SQLITE_DONE) {
        msg = sqlite3_errmsg(db);
        ok = false;
    } else {
        is_null = true;
    }
    sqlite3_finalize(st);
    return ok;
}

bool query_int(sqlite3 *db, const std::string &sql, long long &out, bool &has_row,
               std::string &msg) {
    sqlite3_stmt *st = nullptr;
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &st, nullptr) != SQLITE_OK) {
        msg = sqlite3_errmsg(db);
        return false;
    }
    bool ok = true;
    const int rc = sqlite3_step(st);
    if (rc == SQLITE_ROW) {
        has_row = true;
        out = sqlite3_column_int64(st, 0);
    } else if (rc == SQLITE_DONE) {
        has_row = false;
    } else {
        msg = sqlite3_errmsg(db);
        ok = false;
    }
    sqlite3_finalize(st);
    return ok;
}

std::string quote_ident(const std::string &name) {
    std::string out = "\"";
    for (char c : name) {
        if (c == '"') out += "\"\"";
        else out.push_back(c);
    }
    return out + "\"";
}

} // namespace

AdminKey admin_parse_key_hex(const std::string &hex) {
    AdminKey key;
    if (hex_to_bytes(hex, key.bytes, sizeof(key.bytes))) key.ok = true;
    return key;
}

void admin_wipe(AdminKey &key) {
    volatile unsigned char *p = key.bytes;
    for (std::size_t i = 0; i < sizeof(key.bytes); ++i) p[i] = 0;
    key.ok = false;
}

bool admin_parse_id16(const std::string &hex, unsigned char out[16]) {
    return hex_to_bytes(hex, out, 16);
}

bool admin_store_locked(const std::string &dir, std::string &detail) {
#ifndef _WIN32
    const std::string lock = (fs::path(dir) / "LOCK").string();
    const int fd = ::open(lock.c_str(), O_RDONLY);
    if (fd < 0) {
        detail = "no LOCK file (not a live store or not yet created)";
        return false;
    }
    // RocksDB holds LOCK via fcntl (see env/fs_posix.cc); a flock() probe
    // would miss it (independent lock spaces on Linux). A read lock
    // conflicts with the holder's exclusive lock and needs only O_RDONLY,
    // so this probe also works on read-only directories.
    struct flock fl;
    std::memset(&fl, 0, sizeof(fl));
    fl.l_type = F_RDLCK;
    fl.l_whence = SEEK_SET;
    const int rc = ::fcntl(fd, F_SETLK, &fl);
    const int err = errno;
    ::close(fd);
    if (rc == 0) {
        detail = "LOCK is free";
        return false;
    }
    if (err == EACCES || err == EAGAIN) {
        detail = "LOCK is held by a live process";
        return true;
    }
    detail = "LOCK probe failed: " + std::string(std::strerror(err));
    return false;
#else
    (void)dir;
    detail = "LOCK probe unsupported on this platform (open is the backstop)";
    return false;
#endif
}

int admin_copy_tree(const std::string &src, const std::string &dest,
                    std::string &msg) {
    std::error_code ec;
    if (!fs::is_directory(src, ec) || ec) {
        msg = "source is not a directory: " + src;
        return 1;
    }
    if (fs::exists(fs::symlink_status(dest, ec)) && !ec) {
        msg = "destination already exists: " + dest;
        return 1;
    }
    fs::create_directories(dest, ec);
    if (ec) {
        msg = "cannot create destination: " + dest + ": " + ec.message();
        return 1;
    }
    const auto cleanup = [&] {
        std::error_code ec2;
        fs::remove_all(dest, ec2);
    };
    for (fs::recursive_directory_iterator it(src, ec), end; it != end;
         it.increment(ec)) {
        if (ec) {
            msg = "cannot scan source: " + ec.message();
            cleanup();
            return 1;
        }
        std::error_code ec2;
        const fs::path rel = fs::relative(it->path(), src, ec2);
        if (ec2 || rel.empty()) {
            msg = "cannot relativize source entry";
            cleanup();
            return 1;
        }
        const fs::path out = fs::path(dest) / rel;
        const auto st = it->symlink_status(ec2);
        if (ec2) {
            msg = "cannot stat source entry: " + ec2.message();
            cleanup();
            return 1;
        }
        if (fs::is_directory(st)) {
            fs::create_directories(out, ec2);
            if (ec2) {
                msg = "cannot create directory: " + ec2.message();
                cleanup();
                return 1;
            }
            fs::permissions(out, fs::perms::owner_all, fs::perm_options::replace,
                            ec2);
        } else if (fs::is_regular_file(st)) {
            fs::copy_file(it->path(), out,
                          fs::copy_options::overwrite_existing, ec2);
            if (ec2) {
                msg = "cannot copy file: " + ec2.message();
                cleanup();
                return 1;
            }
            fs::permissions(out,
                            fs::perms::owner_read | fs::perms::owner_write,
                            fs::perm_options::replace, ec2);
        } else {
            msg = "refusing non-regular source entry: " + it->path().string();
            cleanup();
            return 1;
        }
    }
    if (ec) {
        msg = "cannot scan source: " + ec.message();
        cleanup();
        return 1;
    }
    return 0;
}

namespace {

bool read_key_file(const std::string &path, std::string &hex_out, std::ostream &err) {
    std::ifstream f(path, std::ios::binary);
    if (!f.is_open()) {
        err << "Error: cannot open key file " << path << "\n";
        return false;
    }
    std::string content((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    while (!content.empty() && (content.back() == '\n' || content.back() == '\r' ||
                                content.back() == ' ' || content.back() == '\t')) {
        content.pop_back();
    }
    if (content.size() == 64) {
        hex_out = content;
        return true;
    }
    if (content.size() == 32) {
        hex_out = bytes_to_hex(reinterpret_cast<const unsigned char *>(content.data()), 32);
        return true;
    }
    err << "Error: key file " << path << " must contain 64 hex characters or 32 raw bytes\n";
    return false;
}

// Register the VFS key for this process. The key buffer is the caller's to
// wipe afterwards; register() copies exactly 32 bytes.
bool register_key_impl(const std::string &key_hex, std::ostream &err) {
    AdminKey key = admin_parse_key_hex(key_hex);
    if (!key.ok) {
        err << "Error: --key-hex must be 64 hex characters (32 bytes)\n";
        return false;
    }
    // The VFS registers once per process (second register is MISUSE). The
    // CLI runs one command per process, but in-process suites call several:
    // reuse is allowed only when the key matches the first registration,
    // so a different key can never silently inherit the wrong store key.
    static bool registered = false;
    static unsigned char first[32] = {0};
    if (registered) {
        const bool match =
            std::memcmp(first, key.bytes, sizeof(first)) == 0;
        admin_wipe(key);
        if (!match) {
            err << "Error: a different store key is already registered\n";
            return false;
        }
        return true;
    }
    sqlite_murmur_config cfg{};
    cfg.key = key.bytes;
    cfg.flush_interval_ms = 0; // default 250ms
    cfg.cache_pages = 0;       // default 16384
    cfg.rocksdb_sync = 1;      // maintenance tool: durable batches
    const int rc = sqlite_murmur_register(&cfg);
    if (rc == SQLITE_OK) {
        std::memcpy(first, key.bytes, sizeof(first));
        registered = true;
    }
    admin_wipe(key);
    if (rc != SQLITE_OK) {
        err << "Error: cannot register store key (" << rc << ")\n";
        return false;
    }
    return true;
}

bool open_readonly(const std::string &dir, DbPtr &db, std::ostream &err) {
    sqlite3 *raw = nullptr;
    const int rc = sqlite_murmur_open_v2(
        dir.c_str(), &raw, SQLITE_OPEN_READONLY | SQLITE_OPEN_FULLMUTEX,
        nullptr);
    if (rc != SQLITE_OK || !raw) {
        err << "Error: cannot open store read-only (" << rc
            << "); is it locked by a live process or not a database?\n";
        if (raw) sqlite3_close(raw);
        return false;
    }
    db.reset(raw);
    return true;
}

// integrity_check must return exactly one row containing "ok".
bool check_integrity(sqlite3 *db, std::vector<std::string> &rows,
                     std::string &msg) {
    sqlite3_stmt *st = nullptr;
    if (sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &st, nullptr) !=
        SQLITE_OK) {
        msg = sqlite3_errmsg(db);
        return false;
    }
    bool ok = true;
    for (;;) {
        const int rc = sqlite3_step(st);
        if (rc == SQLITE_ROW) {
            const auto *t = sqlite3_column_text(st, 0);
            rows.emplace_back(t ? reinterpret_cast<const char *>(t) : "");
        } else if (rc == SQLITE_DONE) {
            break;
        } else {
            msg = sqlite3_errmsg(db);
            ok = false;
            break;
        }
    }
    sqlite3_finalize(st);
    return ok;
}

bool has_table(sqlite3 *db, const std::string &name) {
    sqlite3_stmt *st = nullptr;
    bool found = false;
    if (sqlite3_prepare_v2(db,
                           "SELECT 1 FROM sqlite_schema WHERE type='table' "
                           "AND name=?1",
                           -1, &st, nullptr) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, name.c_str(), -1, SQLITE_TRANSIENT);
        found = sqlite3_step(st) == SQLITE_ROW;
    }
    sqlite3_finalize(st);
    return found;
}

// Verify the D1 artifact manifest (ruling A): exact schema, supported
// version, known cipher. Identity fields are returned for the report.
bool check_manifest(const std::string &dir, std::string &cipher,
                    std::string &group, std::string &node,
                    std::string &seq, std::string &msg) {
    const fs::path path = fs::path(dir) / "manifest.json";
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        msg = "manifest.json missing (not a murmur_backup artifact?)";
        return false;
    }
    std::ostringstream ss;
    ss << in.rdbuf();
    const std::string body = ss.str();
    std::set<std::string> keys;
    for (std::size_t i = 0; i + 2 < body.size(); ++i) {
        if (body[i] != '"') continue;
        const std::size_t end = body.find('"', i + 1);
        if (end == std::string::npos) break;
        std::size_t j = end + 1;
        while (j < body.size() && body[j] == ' ') ++j;
        if (j < body.size() && body[j] == ':')
            keys.insert(body.substr(i + 1, end - i - 1));
        i = end;
    }
    const std::set<std::string> want = {"format_version", "cipher", "group_id",
                                        "node_id", "rocksdb_seq",
                                        "created_unix_ms"};
    if (keys != want) {
        msg = "manifest.json has an unexpected schema";
        return false;
    }
    const auto field = [&](const std::string &name, std::string &out) {
        const std::string key = "\"" + name + "\":\"";
        const std::size_t at = body.find(key);
        if (at == std::string::npos) return false;
        const std::size_t end = body.find('"', at + key.size());
        if (end == std::string::npos) return false;
        out = body.substr(at + key.size(), end - at - key.size());
        return true;
    };
    const auto number = [&](const std::string &name, std::string &out) {
        const std::string key = "\"" + name + "\":";
        const std::size_t at = body.find(key);
        if (at == std::string::npos) return false;
        std::size_t end = at + key.size();
        while (end < body.size() &&
               (std::isdigit(static_cast<unsigned char>(body[end])) != 0))
            ++end;
        if (end == at + key.size()) return false;
        out = body.substr(at + key.size(), end - at - key.size());
        return true;
    };
    std::string version;
    if (!number("format_version", version) || version != "1") {
        msg = "manifest.json has an unsupported format_version";
        return false;
    }
    if (!field("cipher", cipher) || (cipher != "aes256gcm" && cipher != "aegis256")) {
        msg = "manifest.json has an unknown cipher";
        return false;
    }
    if (!field("group_id", group) || !field("node_id", node) ||
        !number("rocksdb_seq", seq)) {
        msg = "manifest.json is missing identity fields";
        return false;
    }
    return true;
}

bool refuse_if_locked(const std::string &dir, std::ostream &err) {
    std::string detail;
    if (admin_store_locked(dir, detail)) {
        err << "Error: refusing a live target: " << dir << " (" << detail
            << "). Stop the database first; this tool is offline-only.\n";
        return true;
    }
    return false;
}

struct VerifyResult {
    bool valid = false;
    std::string mode;
    std::string database_id;
    std::string node_id;
    std::string cipher;
    std::string seq;
    std::size_t tables_checked = 0;
    std::vector<std::string> errors;
};

bool run_verify_core(const std::string &target, bool backup_mode,
                     VerifyResult &out, std::ostream &err) {
    out.mode = backup_mode ? "backup" : "database";
    DbPtr db;
    if (!open_readonly(target, db, err)) {
        out.errors.emplace_back("cannot open target read-only");
        return false;
    }
    std::vector<std::string> rows;
    std::string msg;
    if (!check_integrity(db.get(), rows, msg)) {
        out.errors.push_back("integrity_check failed: " + msg);
        return false;
    }
    if (rows.size() != 1 || rows[0] != "ok") {
        for (const auto &r : rows) out.errors.push_back("integrity: " + r);
        return false;
    }
    long long tables = 0;
    bool has_row = false;
    if (query_int(db.get(),
                  "SELECT COUNT(*) FROM sqlite_schema WHERE type='table'",
                  tables, has_row, msg) &&
        has_row) {
        out.tables_checked = static_cast<std::size_t>(tables);
    }
    if (backup_mode) {
        std::string cipher, group, node, seq;
        if (!check_manifest(target, cipher, group, node, seq, msg)) {
            out.errors.push_back(msg);
            return false;
        }
        out.cipher = cipher;
        out.database_id = group;
        out.node_id = node;
        out.seq = seq;
    } else if (has_table(db.get(), "murmur_identity")) {
        std::vector<unsigned char> node;
        bool is_null = true;
        if (query_blob(db.get(), "SELECT node FROM murmur_identity WHERE id=1",
                       node, is_null, msg) &&
            !is_null && node.size() == 16) {
            out.node_id = bytes_to_hex(node.data(), node.size());
        }
    }
    out.valid = true;
    return true;
}

void print_verify(const VerifyResult &r, const std::string &target, bool json,
                  std::ostream &out) {
    if (json) {
        out << "{\"target\":\"" << json_escape(target) << "\",\"valid\":"
            << (r.valid ? "true" : "false") << ",\"mode\":\"" << r.mode << "\"";
        if (!r.database_id.empty())
            out << ",\"database_id\":\"" << r.database_id << "\"";
        if (!r.node_id.empty()) out << ",\"node_id\":\"" << r.node_id << "\"";
        if (!r.cipher.empty()) out << ",\"cipher\":\"" << r.cipher << "\"";
        if (!r.seq.empty()) out << ",\"rocksdb_seq\":" << r.seq;
        out << ",\"tables_checked\":" << r.tables_checked << ",\"errors\":[";
        for (std::size_t i = 0; i < r.errors.size(); ++i)
            out << (i ? "," : "") << "\"" << json_escape(r.errors[i]) << "\"";
        out << "]}\n";
        return;
    }
    if (r.valid) {
        out << "PASS: " << (r.mode == "backup" ? "Backup" : "Database")
            << " storage integrity verified at " << target << "\n";
        if (!r.node_id.empty()) out << "Node ID:         " << r.node_id << "\n";
        if (!r.database_id.empty())
            out << "Database ID:     " << r.database_id << "\n";
        if (!r.cipher.empty()) out << "Cipher:          " << r.cipher << "\n";
        out << "Tables Checked:  " << r.tables_checked << "\n";
    } else {
        out << "FAIL: verification failed at " << target << ":\n";
        for (const auto &e : r.errors) out << "  - " << e << "\n";
    }
}

// Minimal extractors over the F3 inventory schema (single-sourced with
// murmur_key_inventory(); the schema is pinned by the admin suite).
// Missing/unparseable fields yield empty; callers omit those lines.
std::string inv_string(const std::string &body, const std::string &key) {
    const std::string needle = "\"" + key + "\":\"";
    const std::size_t at = body.find(needle);
    if (at == std::string::npos) return "";
    const std::size_t begin = at + needle.size();
    const std::size_t end = body.find('"', begin);
    if (end == std::string::npos) return "";
    return body.substr(begin, end - begin);
}

std::string inv_number(const std::string &body, const std::string &key) {
    const std::string needle = "\"" + key + "\":";
    const std::size_t at = body.find(needle);
    if (at == std::string::npos) return "";
    const std::size_t begin = at + needle.size();
    std::size_t end = begin;
    while (end < body.size() &&
           ((body[end] >= '0' && body[end] <= '9') || body[end] == '-'))
        ++end;
    if (end == begin) return "";
    return body.substr(begin, end - begin);
}

std::vector<std::string> inv_key_ids(const std::string &body) {
    std::vector<std::string> ids;
    std::size_t from = 0;
    for (;;) {
        const std::size_t at = body.find("\"id\":", from);
        if (at == std::string::npos) break;
        std::size_t begin = at + 5;
        std::size_t end = begin;
        while (end < body.size() && body[end] >= '0' && body[end] <= '9')
            ++end;
        if (end == begin) break;
        ids.push_back(body.substr(begin, end - begin));
        from = end;
    }
    return ids;
}

void print_keys(const std::string &body, const std::string &dir,
                std::ostream &out) {
    if (body.find("\"sealed\":true") == std::string::npos) {
        out << "No key envelope at " << dir << " (legacy unsealed store)\n";
        return;
    }
    out << "Key envelope at " << dir << ":\n";
    out << "Sealed:   yes\n";
    const std::string kdf = inv_string(body, "kdf");
    if (!kdf.empty()) out << "KDF:      " << kdf << "\n";
    const std::string seq = inv_number(body, "seq");
    if (!seq.empty()) out << "Seq:      " << seq << "\n";
    const std::string current = inv_number(body, "current");
    if (!current.empty()) out << "Current:  " << current << "\n";
    const std::vector<std::string> ids = inv_key_ids(body);
    if (!ids.empty()) {
        out << "Keys:     ";
        for (std::size_t i = 0; i < ids.size(); ++i)
            out << (i ? ", " : "") << ids[i];
        out << "\n";
    }
}

} // namespace

bool admin_ensure_registered(const std::string &key_hex, std::ostream &err) {
    return register_key_impl(key_hex, err);
}

namespace {
bool register_key(const AdminOpts &opts, std::ostream &err) {
    if (opts.key_hex.empty() && !opts.key_file.empty()) {
        std::string hex;
        if (!read_key_file(opts.key_file, hex, err)) return false;
        return register_key_impl(hex, err);
    }
    return register_key_impl(opts.key_hex, err);
}
} // namespace

int admin_cmd_verify(const std::string &target, const AdminOpts &opts,
                     std::ostream &out, std::ostream &err) {
    if (refuse_if_locked(target, err)) return 1;
    if (!register_key(opts, err)) return 1;
    VerifyResult r;
    run_verify_core(target, opts.is_backup, r, err);
    print_verify(r, target, opts.json, out);
    return r.valid ? 0 : 1;
}

namespace {

// Identity rewrite on the destination COPY (temp STORAGE_ONLY handle).
// Approved procedure (lead D3a ruling, codex-corrected): single-transaction
// MOVE of ordinal 0 -> N, remap of every __crsql_clock site_id 0 -> N,
// install of the new UUID at ordinal 0, murmur_identity update, commit.
// Clocks are the only ordinal-bearing metadata (tracked_peers uses UUID
// blobs; the changes vtab exposes resolved UUIDs).
bool rewrite_identity(sqlite3 *db, const unsigned char *new_node,
                      const std::string &new_group_hex, std::string &msg) {
    const bool move_site = new_node != nullptr;
    const bool move_group = !new_group_hex.empty();
    if (!exec_all(db, "BEGIN IMMEDIATE", msg)) return false;
    bool ok = false;
    do {
        if (move_site && has_table(db, "crsql_site_id")) {
            std::vector<unsigned char> old;
            bool is_null = true;
            if (!query_blob(db,
                            "SELECT site_id FROM crsql_site_id WHERE ordinal=0",
                            old, is_null, msg) ||
                is_null || old.size() != 16) {
                msg = "no ordinal-0 site to rehome";
                break;
            }
            sqlite3_stmt *st = nullptr;
            if (sqlite3_prepare_v2(db,
                                   "SELECT COUNT(*) FROM crsql_site_id "
                                   "WHERE site_id=?1",
                                   -1, &st, nullptr) != SQLITE_OK) {
                msg = sqlite3_errmsg(db);
                break;
            }
            sqlite3_bind_blob(st, 1, new_node, 16, SQLITE_TRANSIENT);
            bool precheck = false;
            if (sqlite3_step(st) == SQLITE_ROW)
                precheck = sqlite3_column_int64(st, 0) == 0;
            sqlite3_finalize(st);
            if (!precheck) {
                msg = "fresh site UUID already present";
                break;
            }
            long long mx = 0;
            bool has_row = false;
            if (!query_int(db, "SELECT max(ordinal) FROM crsql_site_id", mx,
                           has_row, msg) ||
                !has_row) {
                msg = "cannot allocate site ordinal";
                break;
            }
            const long long fresh = mx + 1;
            std::ostringstream move;
            move << "UPDATE crsql_site_id SET ordinal=" << fresh
                 << " WHERE ordinal=0";
            if (!exec_all(db, move.str().c_str(), msg)) break;
            // Remap every clock table (suffix match in C++; LIKE '_' would
            // overmatch, and identifiers are quoted defensively).
            sqlite3_stmt *tables = nullptr;
            if (sqlite3_prepare_v2(db,
                                   "SELECT name FROM sqlite_schema WHERE "
                                   "type='table'",
                                   -1, &tables, nullptr) != SQLITE_OK) {
                msg = sqlite3_errmsg(db);
                break;
            }
            bool remap_ok = true;
            while (sqlite3_step(tables) == SQLITE_ROW) {
                const auto *t = sqlite3_column_text(tables, 0);
                const std::string name =
                    t ? reinterpret_cast<const char *>(t) : "";
                const std::string suffix = "__crsql_clock";
                if (name.size() < suffix.size() ||
                    name.compare(name.size() - suffix.size(), suffix.size(),
                                 suffix) != 0)
                    continue;
                std::ostringstream upd;
                upd << "UPDATE " << quote_ident(name) << " SET site_id="
                    << fresh << " WHERE site_id=0";
                if (!exec_all(db, upd.str().c_str(), msg)) {
                    remap_ok = false;
                    break;
                }
            }
            sqlite3_finalize(tables);
            if (!remap_ok) break;
            sqlite3_stmt *ins = nullptr;
            if (sqlite3_prepare_v2(db,
                                   "INSERT INTO crsql_site_id(site_id, "
                                   "ordinal) VALUES(?1, 0)",
                                   -1, &ins, nullptr) != SQLITE_OK) {
                msg = sqlite3_errmsg(db);
                break;
            }
            sqlite3_bind_blob(ins, 1, new_node, 16, SQLITE_TRANSIENT);
            const int irc = sqlite3_step(ins);
            sqlite3_finalize(ins);
            if (irc != SQLITE_DONE) {
                msg = sqlite3_errmsg(db);
                break;
            }
        }
        if (!exec_all(db,
                      "CREATE TABLE IF NOT EXISTS murmur_identity(id INTEGER "
                      "PRIMARY KEY CHECK(id=1),node BLOB,site BLOB,"
                      "incarnation INTEGER NOT NULL DEFAULT 0)",
                      msg))
            break;
        if (!exec_all(db, "INSERT OR IGNORE INTO murmur_identity(id) VALUES(1)",
                      msg))
            break;
        if (move_site) {
            sqlite3_stmt *up = nullptr;
            const bool has_site_table = has_table(db, "crsql_site_id");
            const char *sql =
                has_site_table
                    ? "UPDATE murmur_identity SET node=?1,site=?1,"
                      "incarnation=incarnation+1 WHERE id=1"
                    : "UPDATE murmur_identity SET node=?1,site=NULL,"
                      "incarnation=incarnation+1 WHERE id=1";
            if (sqlite3_prepare_v2(db, sql, -1, &up, nullptr) != SQLITE_OK) {
                msg = sqlite3_errmsg(db);
                break;
            }
            sqlite3_bind_blob(up, 1, new_node, 16, SQLITE_TRANSIENT);
            const int urc = sqlite3_step(up);
            sqlite3_finalize(up);
            if (urc != SQLITE_DONE) {
                msg = sqlite3_errmsg(db);
                break;
            }
        }
        if (move_group) {
            if (!exec_all(db,
                          "CREATE TABLE IF NOT EXISTS murmur_config(name TEXT "
                          "PRIMARY KEY,value TEXT NOT NULL)",
                          msg))
                break;
            sqlite3_stmt *up = nullptr;
            if (sqlite3_prepare_v2(db,
                                   "INSERT INTO murmur_config(name,value) "
                                   "VALUES('group_id',?1) ON CONFLICT(name) DO "
                                   "UPDATE SET value=excluded.value",
                                   -1, &up, nullptr) != SQLITE_OK) {
                msg = sqlite3_errmsg(db);
                break;
            }
            sqlite3_bind_text(up, 1, new_group_hex.c_str(), -1,
                              SQLITE_TRANSIENT);
            const int urc = sqlite3_step(up);
            sqlite3_finalize(up);
            if (urc != SQLITE_DONE) {
                msg = sqlite3_errmsg(db);
                break;
            }
        }
        ok = true;
    } while (false);
    if (!exec_all(db, ok ? "COMMIT" : "ROLLBACK", msg)) return false;
    return ok;
}

bool resolve_fresh_id(const std::string &hex, const char *flag,
                      unsigned char out[16], std::ostream &err) {
    if (!hex.empty()) {
        if (!admin_parse_id16(hex, out)) {
            err << "Error: " << flag << " must be 32 hex characters\n";
            return false;
        }
        return true;
    }
    for (int attempt = 0; attempt < 4; ++attempt) {
        if (!random_bytes(out, 16)) {
            err << "Error: cannot draw random identity\n";
            return false;
        }
        bool zero = true;
        for (int i = 0; i < 16; ++i) zero = zero && out[i] == 0;
        if (!zero) return true;
    }
    err << "Error: cannot draw non-zero random identity\n";
    return false;
}

void print_restore(const std::string &backup, const std::string &dest,
                   const std::string &new_node, const std::string &new_group,
                   bool json, std::ostream &out) {
    if (json) {
        out << "{\"backup\":\"" << json_escape(backup) << "\",\"destination\":\""
            << json_escape(dest) << "\",\"success\":true";
        if (!new_node.empty()) out << ",\"new_node_id\":\"" << new_node << "\"";
        if (!new_group.empty())
            out << ",\"new_database_id\":\"" << new_group << "\"";
        out << "}\n";
        return;
    }
    out << "PASS: Database restored successfully.\n";
    out << "Backup Source:    " << backup << "\n";
    out << "Destination:      " << dest << "\n";
    if (!new_node.empty()) out << "New Node ID:      " << new_node << "\n";
    if (!new_group.empty()) out << "New Database ID:  " << new_group << "\n";
}

void print_usage(std::ostream &out) {
    out << "Murmur Operational Admin CLI (v" << kVersion << ")\n"
        << "\nUsage:\n"
        << "  murmur-admin <command> [options] [arguments]\n"
        << "\nCommands (all offline-only; live targets are refused):\n"
        << "  status   <data-dir>                Identity and storage summary\n"
        << "  inspect  <data-dir>                Schema and config dump\n"
        << "  verify   <data-dir> [--backup]     Open, integrity_check, manifest\n"
        << "  restore  <backup-dir> <dest-dir>   Copy + optional fresh identity\n"
        << "  repair   <data-dir>                Recover a damaged store\n"
        << "  keys     <data-dir>                Show the key envelope\n"
        << "  rekey    <data-dir>                Full offline rekey to new envelope/master\n"
        << "\nOptions:\n"
        << "  --json                            Structured JSON output\n"
        << "  --key-hex=<hex>                   32-byte key (64 hex chars)\n"
        << "  --key-file=<path>                 File with 64 hex chars or 32 raw bytes\n"
        << "  --new-key-hex=<hex>               New 32-byte master key for rekey\n"
        << "  --new-key-file=<path>             File with new master key for rekey\n"
        << "  --backup                          Expect a backup artifact\n"
        << "  --fresh-node-id[=<hex>]            Rehome to a new node id\n"
        << "  --fresh-db-id[=<hex>]              Rehome to a new database id\n"
        << "  -v, --version                     Print version\n"
        << "  -h, --help                        Print help\n";
}

bool secret_name(const std::string &name) {
    std::string lower;
    for (char c : name)
        lower.push_back(static_cast<char>(std::tolower(
            static_cast<unsigned char>(c))));
    return lower.find("private_key") != std::string::npos ||
           lower.find("secret") != std::string::npos ||
           lower.find("password") != std::string::npos ||
           lower.find("certificate") != std::string::npos;
}

} // namespace

int admin_cmd_restore(const std::string &backup, const std::string &dest,
                      const AdminOpts &opts, std::ostream &out,
                      std::ostream &err) {
    if (refuse_if_locked(backup, err)) return 1;
    unsigned char new_node[16] = {0};
    unsigned char new_group[16] = {0};
    std::string new_node_hex, new_group_hex;
    if (opts.fresh_node &&
        !resolve_fresh_id(opts.fresh_node_hex, "--fresh-node-id", new_node,
                          err))
        return 1;
    if (opts.fresh_db &&
        !resolve_fresh_id(opts.fresh_db_hex, "--fresh-db-id", new_group, err))
        return 1;
    if (!register_key(opts, err)) return 1;
    std::string msg;
    if (admin_copy_tree(backup, dest, msg) != 0) {
        err << "Error: restore copy failed: " << msg << "\n";
        return 1;
    }
    const auto fail_remove = [&](const std::string &why) {
        std::error_code ec;
        fs::remove_all(dest, ec);
        err << "Error: restore failed: " << why << "\n";
        return 1;
    };
    if (opts.fresh_node || opts.fresh_db) {
        sqlite3 *raw = nullptr;
        sqlite_murmur_open_options open_opts{};
        open_opts.flags = SQLITE_MURMUR_OPEN_STORAGE_ONLY;
        const int rc = sqlite_murmur_open_v2(
            dest.c_str(), &raw,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX, &open_opts);
        if (rc != SQLITE_OK || !raw) {
            if (raw) sqlite3_close(raw);
            return fail_remove("cannot open destination copy");
        }
        DbPtr db(raw);
        new_node_hex =
            opts.fresh_node ? bytes_to_hex(new_node, 16) : std::string();
        new_group_hex =
            opts.fresh_db ? bytes_to_hex(new_group, 16) : std::string();
        if (!rewrite_identity(db.get(), opts.fresh_node ? new_node : nullptr,
                              new_group_hex, msg)) {
            return fail_remove(msg.empty() ? "identity rewrite failed" : msg);
        }
        db.reset();
    }
    VerifyResult r;
    std::ostringstream quiet;
    if (!run_verify_core(dest, true, r, quiet) || !r.valid) {
        std::string why = msg;
        if (!r.errors.empty()) why = r.errors.front();
        return fail_remove(why.empty() ? "restored copy failed verification"
                                       : why);
    }
    print_restore(backup, dest, new_node_hex, new_group_hex, opts.json, out);
    return 0;
}

int admin_cmd_status(const std::string &dir, const AdminOpts &opts,
                     std::ostream &out, std::ostream &err) {
    if (refuse_if_locked(dir, err)) return 1;
    if (!register_key(opts, err)) return 1;
    DbPtr db;
    if (!open_readonly(dir, db, err)) return 1;
    std::string msg, node, site, group, cipher = "unknown";
    long long incarnation = 0, tables = 0;
    bool has_row = false;
    if (has_table(db.get(), "murmur_identity")) {
        std::vector<unsigned char> blob;
        bool is_null = true;
        if (query_blob(db.get(), "SELECT node FROM murmur_identity WHERE id=1",
                       blob, is_null, msg) &&
            !is_null && blob.size() == 16)
            node = bytes_to_hex(blob.data(), blob.size());
        if (query_blob(db.get(), "SELECT site FROM murmur_identity WHERE id=1",
                       blob, is_null, msg) &&
            !is_null && blob.size() == 16)
            site = bytes_to_hex(blob.data(), blob.size());
        long long inc = 0;
        if (query_int(db.get(),
                      "SELECT incarnation FROM murmur_identity WHERE id=1", inc,
                      has_row, msg) &&
            has_row)
            incarnation = inc;
    }
    if (has_table(db.get(), "murmur_config")) {
        std::string value;
        if (query_text(db.get(),
                       "SELECT value FROM murmur_config WHERE name='group_id'",
                       value, msg))
            group = value;
    }
    {
        std::string value;
        if (query_text(db.get(), "PRAGMA cipher", value, msg) && !value.empty())
            cipher = value;
    }
    if (query_int(db.get(),
                  "SELECT COUNT(*) FROM sqlite_schema WHERE type='table'",
                  tables, has_row, msg) &&
        !has_row)
        tables = 0;
    std::uint64_t disk_bytes = 0;
    std::size_t files = 0;
    dir_usage(dir, disk_bytes, files);
    if (opts.json) {
        out << "{\"data_dir\":\"" << json_escape(dir) << "\"";
        if (!node.empty()) out << ",\"node_id\":\"" << node << "\"";
        if (!site.empty()) out << ",\"site_id\":\"" << site << "\"";
        if (!group.empty()) out << ",\"group_id\":\"" << json_escape(group)
                                 << "\"";
        out << ",\"incarnation\":" << incarnation << ",\"cipher\":\""
            << json_escape(cipher) << "\",\"tables\":" << tables
            << ",\"disk_bytes\":" << disk_bytes << ",\"files\":" << files
            << "}\n";
        return 0;
    }
    out << "=== Murmur Database Status ===\n";
    out << "Data Directory:      " << dir << "\n";
    if (!node.empty()) out << "Node ID:             " << node << "\n";
    if (!site.empty()) out << "Site ID:             " << site << "\n";
    if (!group.empty()) out << "Group ID:            " << group << "\n";
    out << "Incarnation:         " << incarnation << "\n";
    out << "Cipher:              " << cipher << "\n";
    out << "Tables:              " << tables << "\n";
    out << "Files On Disk:       " << files << " files (" << disk_bytes
        << " bytes)\n";
    return 0;
}

int admin_cmd_inspect(const std::string &dir, const AdminOpts &opts,
                      std::ostream &out, std::ostream &err) {
    if (refuse_if_locked(dir, err)) return 1;
    if (!register_key(opts, err)) return 1;
    DbPtr db;
    if (!open_readonly(dir, db, err)) return 1;
    struct Table {
        std::string name;
        long long columns = 0;
        bool crr = false;
    };
    std::vector<Table> tables;
    {
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db.get(),
                               "SELECT name FROM sqlite_schema WHERE "
                               "type='table' ORDER BY name",
                               -1, &st, nullptr) == SQLITE_OK) {
            while (sqlite3_step(st) == SQLITE_ROW) {
                const auto *t = sqlite3_column_text(st, 0);
                Table table;
                table.name = t ? reinterpret_cast<const char *>(t) : "";
                tables.push_back(table);
            }
        }
        sqlite3_finalize(st);
    }
    for (auto &t : tables) {
        std::string msg;
        long long ncols = 0;
        bool has_row = false;
        if (query_int(db.get(),
                      "SELECT COUNT(*) FROM pragma_table_info(" +
                          quote_ident(t.name) + ")",
                      ncols, has_row, msg) &&
            has_row)
            t.columns = ncols;
        t.crr = has_table(db.get(), t.name + "__crsql_clock");
    }
    std::vector<std::pair<std::string, std::string>> config;
    if (has_table(db.get(), "murmur_config")) {
        sqlite3_stmt *st = nullptr;
        if (sqlite3_prepare_v2(db.get(),
                               "SELECT name,value FROM murmur_config ORDER "
                               "BY name",
                               -1, &st, nullptr) == SQLITE_OK) {
            while (sqlite3_step(st) == SQLITE_ROW) {
                const auto *n = sqlite3_column_text(st, 0);
                const auto *v = sqlite3_column_text(st, 1);
                std::string name = n ? reinterpret_cast<const char *>(n) : "";
                std::string value =
                    v ? reinterpret_cast<const char *>(v) : "";
                // Never emit key material: config rows can carry private
                // keys and certificates. Fail closed on any secret-like
                // name rather than maintaining an allowlist.
                if (secret_name(name)) value = "<redacted>";
                config.emplace_back(name, value);
            }
        }
        sqlite3_finalize(st);
    }
    std::uint64_t disk_bytes = 0;
    std::size_t files = 0;
    dir_usage(dir, disk_bytes, files);
    if (opts.json) {
        out << "{\"data_dir\":\"" << json_escape(dir) << "\",\"tables\":[";
        for (std::size_t i = 0; i < tables.size(); ++i) {
            out << (i ? "," : "") << "{\"name\":\""
                << json_escape(tables[i].name) << "\",\"columns\":"
                << tables[i].columns << ",\"crr\":"
                << (tables[i].crr ? "true" : "false") << "}";
        }
        out << "],\"config\":{";
        for (std::size_t i = 0; i < config.size(); ++i) {
            out << (i ? "," : "") << "\"" << json_escape(config[i].first)
                << "\":\"" << json_escape(config[i].second) << "\"";
        }
        out << "},\"disk_bytes\":" << disk_bytes << ",\"files\":" << files
            << "}\n";
        return 0;
    }
    out << "=== Murmur Storage Inspection ===\n";
    out << "Data Directory:      " << dir << "\n";
    out << "Files On Disk:       " << files << " files (" << disk_bytes
        << " bytes)\n";
    out << "Tables Registered:   " << tables.size() << "\n";
    for (const auto &t : tables)
        out << "  - " << t.name << " (columns: " << t.columns
            << (t.crr ? ", crr" : "") << ")\n";
    if (!config.empty()) {
        out << "--- murmur_config ---\n";
        for (const auto &kv : config)
            out << "  " << kv.first << " = " << kv.second << "\n";
    }
    return 0;
}

int admin_cmd_repair(const std::string &dir, const AdminOpts &opts,
                     std::ostream &out, std::ostream &err) {
    if (refuse_if_locked(dir, err)) return 1;
    if (!register_key(opts, err)) return 1;
    // Repair works at the LSM level (records stay opaque), so no key use
    // beyond registration; the key stays uniform across commands.
    const int rc = sqlite_murmur_repair(dir.c_str());
    if (rc == SQLITE_OK) {
        if (opts.json) {
            out << "{\"data_dir\":\"" << json_escape(dir)
                << "\",\"repaired\":true}\n";
        } else {
            out << "PASS: Store recovered at " << dir << "\n";
        }
        return 0;
    }
    if (rc == SQLITE_BUSY) {
        err << "Error: refusing a live target: " << dir
            << " (store is open; stop the database first)\n";
        return 1;
    }
    if (rc == SQLITE_CANTOPEN) {
        err << "Error: cannot repair " << dir
            << " (missing or not a database; nothing was created)\n";
        return 1;
    }
    err << "Error: repair failed for " << dir << " (" << rc << ")\n";
    return 1;
}

int admin_cmd_keys(const std::string &dir, const AdminOpts &opts,
                   std::ostream &out, std::ostream &err) {
    if (refuse_if_locked(dir, err)) return 1;
    if (!register_key(opts, err)) return 1;
    char *json = nullptr;
    const int rc = sqlite_murmur_key_inventory(dir.c_str(), &json);
    if (rc == SQLITE_BUSY) {
        err << "Error: refusing a live target: " << dir
            << " (store is open; stop the database first)\n";
        return 1;
    }
    if (rc == SQLITE_CANTOPEN) {
        err << "Error: cannot read keys from " << dir
            << " (missing or not a database; nothing was created)\n";
        return 1;
    }
    if (rc != SQLITE_OK || !json) {
        sqlite3_free(json);
        err << "Error: key inventory failed for " << dir << " (" << rc
            << ")\n";
        return 1;
    }
    const std::string body(json);
    sqlite3_free(json);
    if (opts.json) {
        out << body << "\n"; // verbatim: single-sourced F3 schema
    } else {
        print_keys(body, dir, out);
    }
    return 0;
}

int admin_cmd_rekey(const std::string &dir, const AdminOpts &opts,
                    std::ostream &out, std::ostream &err) {
    if (refuse_if_locked(dir, err)) return 1;

    std::error_code ec;
    if (!fs::is_directory(dir, ec) || ec) {
        err << "Error: cannot rekey " << dir
            << " (missing or not a database; nothing was created)\n";
        return 1;
    }

    std::string old_hex = opts.key_hex;
    if (old_hex.empty() && !opts.key_file.empty()) {
        if (!read_key_file(opts.key_file, old_hex, err)) return 1;
    }
    AdminKey old_key = admin_parse_key_hex(old_hex);
    if (!old_key.ok) {
        err << "Error: --key-hex or --key-file must provide 32 key bytes (64 hex characters)\n";
        return 1;
    }

    std::string new_hex = opts.new_key_hex;
    if (new_hex.empty() && !opts.new_key_file.empty()) {
        if (!read_key_file(opts.new_key_file, new_hex, err)) {
            admin_wipe(old_key);
            return 1;
        }
    }
    AdminKey new_key = admin_parse_key_hex(new_hex);
    if (!new_key.ok) {
        admin_wipe(old_key);
        err << "Error: --new-key-hex or --new-key-file must provide 32 key bytes (64 hex characters)\n";
        return 1;
    }

    const int rc = sqlite_murmur_rekey(dir.c_str(), old_key.bytes, new_key.bytes, nullptr);
    admin_wipe(old_key);
    admin_wipe(new_key);

    if (rc == SQLITE_OK) {
        if (opts.json) {
            out << "{\"rekeyed\":true}\n";
        } else {
            out << "PASS: Database at " << dir << " rekeyed successfully under fresh envelope.\n";
        }
        return 0;
    }
    if (rc == SQLITE_BUSY) {
        err << "Error: refusing a live target: " << dir
            << " (store is open; stop the database first)\n";
        return 1;
    }
    if (rc == SQLITE_CANTOPEN) {
        err << "Error: cannot rekey " << dir
            << " (missing or not a database; nothing was created)\n";
        return 1;
    }
    if (rc == SQLITE_AUTH) {
        err << "Error: cannot open with provided key\n";
        return 1;
    }
    if (rc == SQLITE_ERROR) {
        err << "Error: cannot rekey " << dir
            << " (unsealed store, or sealed store with unsupported cipher/KDF)\n";
        return 1;
    }
    err << "Error: rekey failed (" << rc << "); retry rekey with the same keys to complete\n";
    return 1;
}

int admin_main(int argc, char **argv, std::ostream &out, std::ostream &err) {
    AdminOpts opts;
    std::vector<std::string> positional;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "-h" || arg == "--help") {
            print_usage(out);
            return 0;
        }
        if (arg == "-v" || arg == "--version") {
            out << "murmur-admin version " << kVersion << "\n";
            return 0;
        }
        if (arg == "--json") {
            opts.json = true;
        } else if (arg == "--backup") {
            opts.is_backup = true;
        } else if (arg.rfind("--key-hex=", 0) == 0) {
            opts.key_hex = arg.substr(10);
        } else if (arg == "--key-hex" && i + 1 < argc) {
            opts.key_hex = argv[++i];
        } else if (arg.rfind("--key-file=", 0) == 0) {
            opts.key_file = arg.substr(11);
        } else if (arg == "--key-file" && i + 1 < argc) {
            opts.key_file = argv[++i];
        } else if (arg.rfind("--new-key-hex=", 0) == 0) {
            opts.new_key_hex = arg.substr(14);
        } else if (arg == "--new-key-hex" && i + 1 < argc) {
            opts.new_key_hex = argv[++i];
        } else if (arg.rfind("--new-key-file=", 0) == 0) {
            opts.new_key_file = arg.substr(15);
        } else if (arg == "--new-key-file" && i + 1 < argc) {
            opts.new_key_file = argv[++i];
        } else if (arg == "--fresh-node-id") {
            opts.fresh_node = true;
        } else if (arg.rfind("--fresh-node-id=", 0) == 0) {
            opts.fresh_node = true;
            opts.fresh_node_hex = arg.substr(16);
        } else if (arg == "--fresh-db-id") {
            opts.fresh_db = true;
        } else if (arg.rfind("--fresh-db-id=", 0) == 0) {
            opts.fresh_db = true;
            opts.fresh_db_hex = arg.substr(13);
        } else if (!arg.empty() && arg[0] == '-') {
            err << "Error: unknown option: " << arg << "\n";
            return 1;
        } else {
            positional.push_back(arg);
        }
    }
    if (positional.empty()) {
        print_usage(out);
        return 0;
    }
    if (opts.key_hex.empty() && !opts.key_file.empty()) {
        std::string hex;
        if (!read_key_file(opts.key_file, hex, err)) return 1;
        opts.key_hex = hex;
    }
    if (admin_parse_key_hex(opts.key_hex).ok == false) {
        err << "Error: --key-hex=<64 hex chars> or --key-file=<path> is required\n";
        return 1;
    }
    if (!opts.fresh_node_hex.empty()) {
        unsigned char tmp[16];
        if (!admin_parse_id16(opts.fresh_node_hex, tmp)) {
            err << "Error: --fresh-node-id must be 32 hex characters\n";
            return 1;
        }
    }
    if (!opts.fresh_db_hex.empty()) {
        unsigned char tmp[16];
        if (!admin_parse_id16(opts.fresh_db_hex, tmp)) {
            err << "Error: --fresh-db-id must be 32 hex characters\n";
            return 1;
        }
    }
    const std::string cmd = positional[0];
    const std::vector<std::string> cmd_args(positional.begin() + 1,
                                            positional.end());
    if (cmd == "verify") {
        if (cmd_args.size() != 1) {
            err << "Error: usage: murmur-admin verify <data-dir> [--backup] "
                   "--key-hex=<hex>\n";
            return 1;
        }
        return admin_cmd_verify(cmd_args[0], opts, out, err);
    }
    if (cmd == "restore") {
        if (cmd_args.size() != 2) {
            err << "Error: usage: murmur-admin restore <backup-dir> "
                   "<dest-dir> [--fresh-node-id[=hex]] [--fresh-db-id[=hex]] "
                   "--key-hex=<hex>\n";
            return 1;
        }
        return admin_cmd_restore(cmd_args[0], cmd_args[1], opts, out, err);
    }
    if (cmd == "status") {
        if (cmd_args.size() != 1) {
            err << "Error: usage: murmur-admin status <data-dir> "
                   "--key-hex=<hex>\n";
            return 1;
        }
        return admin_cmd_status(cmd_args[0], opts, out, err);
    }
    if (cmd == "inspect") {
        if (cmd_args.size() != 1) {
            err << "Error: usage: murmur-admin inspect <data-dir> "
                   "--key-hex=<hex>\n";
            return 1;
        }
        return admin_cmd_inspect(cmd_args[0], opts, out, err);
    }
    if (cmd == "repair") {
        if (cmd_args.size() != 1) {
            err << "Error: usage: murmur-admin repair <data-dir> "
                   "--key-hex=<hex>\n";
            return 1;
        }
        return admin_cmd_repair(cmd_args[0], opts, out, err);
    }
    if (cmd == "keys") {
        if (cmd_args.size() != 1) {
            err << "Error: usage: murmur-admin keys <data-dir> "
                   "--key-hex=<hex>\n";
            return 1;
        }
        return admin_cmd_keys(cmd_args[0], opts, out, err);
    }
    if (cmd == "rekey") {
        if (cmd_args.size() != 1) {
            err << "Error: usage: murmur-admin rekey <data-dir> "
                   "(--key-hex=<hex>|--key-file=<path>) "
                   "(--new-key-hex=<hex>|--new-key-file=<path>) [--json]\n";
            return 1;
        }
        if (opts.new_key_hex.empty() && !opts.new_key_file.empty()) {
            std::string hex;
            if (!read_key_file(opts.new_key_file, hex, err)) return 1;
            opts.new_key_hex = hex;
        }
        if (admin_parse_key_hex(opts.new_key_hex).ok == false) {
            err << "Error: --new-key-hex=<64 hex chars> or --new-key-file=<path> is required for rekey\n";
            return 1;
        }
        return admin_cmd_rekey(cmd_args[0], opts, out, err);
    }
    err << "Error: unknown command: \"" << cmd
        << "\". Run 'murmur-admin --help' for usage.\n";
    return 1;
}

