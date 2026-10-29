// WP-D slice D1 tests: online hot backup helper. These tests cover the
// helper contract directly (checkpoint + manifest + cleanup) through its
// stable C ABI (include/sqlite_murmur_backup.h, implemented in
// crates/murmur-storage/src/backup.rs). The checkpoint/compact callbacks
// adapt a C++ rocksdb::DB* handle (the tests link RocksDB); the in-tree
// store passes its C-API handle instead. The FCTL-dispatch /
// murmur_backup() UDF path is lead-merged.

#include "sqlite_murmur_backup.h"

#include <rocksdb/db.h>
#include <rocksdb/utilities/checkpoint.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>

namespace {

namespace fs = std::filesystem;

void require(bool ok, const std::string &msg) {
    if (!ok) throw std::runtime_error("backup test: " + msg);
}

extern "C" int backup_checkpoint_create(void *ctx, const char *dest,
                                        std::uint64_t *seq) {
    rocksdb::DB *db = static_cast<rocksdb::DB *>(ctx);
    rocksdb::Checkpoint *raw = nullptr;
    if (!rocksdb::Checkpoint::Create(db, &raw).ok() || !raw)
        return SQLITE_IOERR_WRITE;
    std::unique_ptr<rocksdb::Checkpoint> checkpoint(raw);
    if (!checkpoint->CreateCheckpoint(dest, 0, seq).ok())
        return SQLITE_IOERR_WRITE;
    return SQLITE_OK;
}

extern "C" int backup_compact_full_range(void *ctx) {
    rocksdb::DB *db = static_cast<rocksdb::DB *>(ctx);
    rocksdb::CompactRangeOptions options;
    if (!db->CompactRange(options, nullptr, nullptr).ok())
        return SQLITE_IOERR_WRITE;
    return SQLITE_OK;
}

int run_backup(void *db, const char *cipher,
               const sqlite_murmur_backup_args *args, std::uint64_t *seq) {
    if (!args) return SQLITE_MISUSE;
    return sqlite_murmur_backup_rs_run(args->dest, cipher, args->group,
                                       args->node, seq, db,
                                       backup_checkpoint_create);
}

int run_compact(void *db) {
    return sqlite_murmur_backup_rs_compact(db, backup_compact_full_range);
}

// Minimal top-level "key": extractor for the flat manifest object.
std::set<std::string> manifest_keys(const std::string &body) {
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
    return keys;
}

std::string read_all(const fs::path &p) {
    std::ifstream in(p, std::ios::binary);
    require(static_cast<bool>(in), "cannot open " + p.string());
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

std::string hex_of(const unsigned char *bytes, std::size_t n) {
    static const char *digits = "0123456789abcdef";
    std::string out;
    for (std::size_t i = 0; i < n; ++i) {
        out.push_back(digits[bytes[i] >> 4]);
        out.push_back(digits[bytes[i] & 0xf]);
    }
    return out;
}

void fill_args(sqlite_murmur_backup_args &args, const std::string &dest) {
    args.dest = dest.c_str();
    for (int i = 0; i < 16; ++i) {
        args.group[i] = static_cast<unsigned char>(0x10 + i);
        args.node[i] = static_cast<unsigned char>(0xa0 + i);
    }
}

} // namespace

int main() {
    char tmpl[] = "/tmp/sqlite-murmur-backup-test-XXXXXX";
    require(mkdtemp(tmpl) != nullptr, "cannot create temp dir");
    const fs::path root(tmpl);
    try {
        // 1. Bad input is rejected without touching the filesystem.
        {
            sqlite_murmur_backup_args args{};
            const std::string dest = (root / "nope").string();
            fill_args(args, dest);
            require(run_backup(nullptr, "aes256gcm", &args, nullptr) ==
                        SQLITE_MISUSE,
                    "null ctx accepted");
            require(run_backup(nullptr, "aes256gcm", nullptr, nullptr) ==
                        SQLITE_MISUSE,
                    "null args accepted");
            require(sqlite_murmur_backup_rs_compact(nullptr, nullptr) ==
                        SQLITE_MISUSE,
                    "null compact ctx/callback accepted");
            // A dummy non-null handle is never dereferenced on the
            // validation path: empty dest / cipher / bad charset fail first.
            void *dummy = reinterpret_cast<void *>(0x1);
            sqlite_murmur_backup_args bad = args;
            bad.dest = "";
            require(run_backup(dummy, "aes256gcm", &bad, nullptr) ==
                        SQLITE_MISUSE,
                    "empty dest accepted");
            require(run_backup(dummy, "", &args, nullptr) == SQLITE_MISUSE,
                    "empty cipher accepted");
            require(run_backup(dummy, "aes256gcm\",evil\":1,\"x\":\"", &args,
                               nullptr) == SQLITE_ERROR,
                    "cipher charset injection accepted");
            require(!fs::exists(dest), "rejected call created dest");
        }

        // 2. Existing destination (file or dir) is refused, never clobbered.
        {
            const fs::path file = root / "file-dest";
            const fs::path dir = root / "dir-dest";
            {
                std::ofstream out(file);
                out << "sentinel";
            }
            fs::create_directory(dir);
            void *dummy = reinterpret_cast<void *>(0x1);
            sqlite_murmur_backup_args args{};
            std::string dest = file.string();
            fill_args(args, dest);
            require(run_backup(dummy, "aes256gcm", &args, nullptr) ==
                        SQLITE_ERROR,
                    "existing file dest accepted");
            dest = dir.string();
            fill_args(args, dest);
            require(run_backup(dummy, "aes256gcm", &args, nullptr) ==
                        SQLITE_ERROR,
                    "existing dir dest accepted");
            require(read_all(file) == "sentinel", "existing file clobbered");
        }

        // 3. Round trip: checkpoint of a live handle + manifest, reopenable.
        {
            const std::string src = (root / "srcdb").string();
            const std::string dest = (root / "backup").string();
            rocksdb::Options options;
            options.create_if_missing = true;
            rocksdb::DB *db = nullptr;
            require(rocksdb::DB::Open(options, src, &db).ok(), "cannot open srcdb");
            require(db->Put(rocksdb::WriteOptions(), "k1", "v1").ok(), "put k1");
            require(db->Put(rocksdb::WriteOptions(), "k2", "v2").ok(), "put k2");
            require(db->Put(rocksdb::WriteOptions(), "k3", "v3").ok(), "put k3");
            require(db->Flush(rocksdb::FlushOptions()).ok(), "flush srcdb");

            sqlite_murmur_backup_args args{};
            fill_args(args, dest);
            std::uint64_t seq = 0;
            require(run_backup(db, "aes256gcm", &args, &seq) == SQLITE_OK,
                    "backup run failed");
            require(seq > 0, "checkpoint seq not reported");
            delete db;

            const fs::path manifest = fs::path(dest) / "manifest.json";
            require(fs::is_regular_file(manifest), "manifest missing");
            const auto perms = fs::status(manifest).permissions() & fs::perms::all;
            require(perms == (fs::perms::owner_read | fs::perms::owner_write),
                    "manifest not 0600");
            const std::string body = read_all(manifest);
            const std::set<std::string> want = {"format_version", "cipher", "group_id",
                                                "node_id", "rocksdb_seq",
                                                "created_unix_ms"};
            require(manifest_keys(body) == want, "manifest schema drift: " + body);
            require(body.find("\"cipher\":\"aes256gcm\"") != std::string::npos,
                    "manifest cipher wrong");
            require(body.find(hex_of(args.group, 16)) != std::string::npos,
                    "manifest group wrong");
            require(body.find(hex_of(args.node, 16)) != std::string::npos,
                    "manifest node wrong");
            require(body.find("\"rocksdb_seq\":" + std::to_string(seq)) !=
                        std::string::npos,
                    "manifest seq mismatch");

            rocksdb::DB *reopened = nullptr;
            require(rocksdb::DB::Open(options, dest, &reopened).ok(),
                    "checkpoint does not reopen");
            for (int i = 1; i <= 3; ++i) {
                std::string value;
                const std::string key = "k" + std::to_string(i);
                require(reopened->Get(rocksdb::ReadOptions(), key, &value).ok(),
                        "checkpoint lost " + key);
                require(value == "v" + std::to_string(i), "checkpoint value wrong");
            }
            delete reopened;
        }

        // 4b. D3b: forced full-range compaction merges SSTs.
        {
            require(run_compact(nullptr) == SQLITE_MISUSE,
                    "null ctx compact accepted");
            const std::string dir = (root / "compactdb").string();
            rocksdb::Options options;
            options.create_if_missing = true;
            options.disable_auto_compactions = true;
            rocksdb::DB *db = nullptr;
            require(rocksdb::DB::Open(options, dir, &db).ok(),
                    "cannot open compactdb");
            for (int round = 0; round < 2; ++round) {
                for (int i = 0; i < 50; ++i) {
                    const std::string key =
                        "r" + std::to_string(round) + "k" +
                        std::to_string(i);
                    require(db->Put(rocksdb::WriteOptions(), key, "payload")
                                .ok(),
                            "compact put failed");
                }
                require(db->Flush(rocksdb::FlushOptions()).ok(), "flush");
            }
            std::string before;
            require(db->GetProperty("rocksdb.num-files-at-level0", &before),
                    "cannot count L0 files");
            require(before == "2", "expected 2 L0 files, got " + before);
            require(run_compact(db) == SQLITE_OK, "compact run failed");
            std::string after;
            require(db->GetProperty("rocksdb.num-files-at-level0", &after),
                    "cannot recount L0 files");
            require(after == "0", "compaction did not drain L0: " + after);
            std::string value;
            require(db->Get(rocksdb::ReadOptions(), "r1k49", &value).ok() &&
                        value == "payload",
                    "compact lost data");
            delete db;
        }

        // 4. Mid-run failure leaves no partial artifact behind.
        {
            const std::string src = (root / "srcdb2").string();
            const std::string dest =
                (root / "missing-parent" / "backup").string();
            rocksdb::Options options;
            options.create_if_missing = true;
            rocksdb::DB *db = nullptr;
            require(rocksdb::DB::Open(options, src, &db).ok(), "cannot open srcdb2");
            sqlite_murmur_backup_args args{};
            fill_args(args, dest);
            require(run_backup(db, "aes256gcm", &args, nullptr) !=
                        SQLITE_OK,
                    "impossible dest accepted");
            delete db;
            require(!fs::exists(dest), "partial artifact left behind");
            require(!fs::exists(root / "missing-parent"), "parent created");
        }

        std::error_code ec;
        fs::remove_all(root, ec);
        std::cout << "Murmur backup helper: validation, dest-exists refusal, "
                     "round trip, manifest schema, compaction, failure "
                     "cleanup passed\n";
        return 0;
    } catch (const std::exception &e) {
        std::cerr << e.what() << "\nscratch kept at " << tmpl << '\n';
        return 1;
    }
}
