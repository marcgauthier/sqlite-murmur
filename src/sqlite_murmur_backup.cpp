// Online hot backup helper (WP-D slice D1). See include/sqlite_murmur_backup.h
// for provenance and the caller contract.

#include "sqlite_murmur_backup.h"

#include <rocksdb/db.h>
#include <rocksdb/utilities/checkpoint.h>

#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#endif

namespace {

namespace fs = std::filesystem;

// True when nothing exists at dest (file, dir, or symlink). A dangling
// symlink still refuses: writing through attacker-placed links is never
// acceptable for a backup path. Unstatable parents report through the
// checkpoint call itself, which fails without creating anything.
bool path_is_free(const char *dest) {
    std::error_code ec;
    const auto st = fs::symlink_status(dest, ec);
    if (ec) return true;
    (void)st;
    return false;
}

bool cipher_charset_ok(const char *cipher) {
    for (const char *c = cipher; *c; ++c) {
        const bool lower = *c >= 'a' && *c <= 'z';
        const bool digit = *c >= '0' && *c <= '9';
        if (!lower && !digit) return false;
    }
    return true;
}

std::string to_hex(const unsigned char *bytes, std::size_t n) {
    static const char *digits = "0123456789abcdef";
    std::string out;
    out.reserve(n * 2);
    for (std::size_t i = 0; i < n; ++i) {
        out.push_back(digits[bytes[i] >> 4]);
        out.push_back(digits[bytes[i] & 0xf]);
    }
    return out;
}

#ifndef _WIN32
// Best-effort durable write: 0600 file, fsync data, fsync containing dir.
bool write_file_sync(const fs::path &path, const std::string &body) {
    const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (fd < 0) return false;
    bool ok = true;
    std::size_t off = 0;
    while (off < body.size()) {
        const ssize_t n =
            ::write(fd, body.data() + off, body.size() - off);
        if (n <= 0) {
            ok = false;
            break;
        }
        off += static_cast<std::size_t>(n);
    }
    if (ok && ::fsync(fd) != 0) ok = false;
    if (::close(fd) != 0) ok = false;
    if (!ok) {
        std::error_code ec;
        fs::remove(path, ec);
        return false;
    }
    const int dfd = ::open(path.parent_path().c_str(), O_RDONLY | O_DIRECTORY);
    if (dfd >= 0) {
        (void)::fsync(dfd);
        (void)::close(dfd);
    }
    return true;
}
#else
bool write_file_sync(const fs::path &path, const std::string &body) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) return false;
    out << body;
    out.flush();
    return static_cast<bool>(out);
}
#endif

int write_manifest(const char *dest, const char *cipher,
                   const unsigned char group[16], const unsigned char node[16],
                   std::uint64_t seq) {
    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    std::string body = "{\"format_version\":";
    body += std::to_string(SQLITE_MURMUR_BACKUP_FORMAT_VERSION);
    body += ",\"cipher\":\"";
    body += cipher;
    body += "\",\"group_id\":\"";
    body += to_hex(group, 16);
    body += "\",\"node_id\":\"";
    body += to_hex(node, 16);
    body += "\",\"rocksdb_seq\":";
    body += std::to_string(seq);
    body += ",\"created_unix_ms\":";
    body += std::to_string(now_ms);
    body += "}\n";
    const fs::path path = fs::path(dest) / SQLITE_MURMUR_BACKUP_MANIFEST;
    if (!write_file_sync(path, body)) return SQLITE_IOERR_WRITE;
#ifndef _WIN32
    // Sync the parent so the new directory entry itself is durable (the
    // archive backup does the same for its destination parents).
    const fs::path parent = fs::path(dest).parent_path();
    if (!parent.empty()) {
        const int pfd = ::open(parent.c_str(), O_RDONLY | O_DIRECTORY);
        if (pfd >= 0) {
            (void)::fsync(pfd);
            (void)::close(pfd);
        }
    }
#endif
    return SQLITE_OK;
}

void remove_best_effort(const char *dest) {
    std::error_code ec;
    fs::remove_all(dest, ec);
}

} // namespace

int sqlite_murmur_backup_run(rocksdb::DB *db, const char *cipher,
                             const sqlite_murmur_backup_args *args,
                             std::uint64_t *seq_out) {
    if (!db || !cipher || !*cipher || !args || !args->dest || !*args->dest)
        return SQLITE_MISUSE;
    if (!cipher_charset_ok(cipher)) return SQLITE_ERROR;
    if (!path_is_free(args->dest)) return SQLITE_ERROR;

    rocksdb::Checkpoint *raw = nullptr;
    if (!rocksdb::Checkpoint::Create(db, &raw).ok() || !raw)
        return SQLITE_IOERR_WRITE;
    std::unique_ptr<rocksdb::Checkpoint> checkpoint(raw);
    std::uint64_t seq = 0;
    if (!checkpoint->CreateCheckpoint(args->dest, 0, &seq).ok()) {
        remove_best_effort(args->dest);
        return SQLITE_IOERR_WRITE;
    }
    if (write_manifest(args->dest, cipher, args->group, args->node, seq) !=
        SQLITE_OK) {
        remove_best_effort(args->dest);
        return SQLITE_IOERR_WRITE;
    }
    if (seq_out) *seq_out = seq;
    return SQLITE_OK;
}

int sqlite_murmur_compact_run(rocksdb::DB *db) {
    if (!db) return SQLITE_MISUSE;
    rocksdb::CompactRangeOptions options;
    if (!db->CompactRange(options, nullptr, nullptr).ok())
        return SQLITE_IOERR_WRITE;
    return SQLITE_OK;
}
