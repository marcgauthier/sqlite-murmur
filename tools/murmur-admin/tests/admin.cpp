// WP-D slice D3a tests: murmur-admin offline CLI. In-process suite against
// the static library (same link as the CLI binary). Databases are created
// STORAGE_ONLY with manual crsql init (full replication open requires TLS
// config); the restore tests then exercise the approved MOVE rehome.
// Lead merge: add_executable(murmur-admin ...) + this file as
// tools/murmur-admin/tests/admin.cpp + add_test (ruling H).

#include "../admin.h"

#include <sqlite_murmur.h>
#include <sqlite_murmur_crsqlite.h>
#include <sqlite_murmur_keyring.h>
#include <sqlite3.h>

#include <rocksdb/db.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#ifndef _WIN32
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace {

namespace fs = std::filesystem;

const char *kKeyHex =
    "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
const char *kNewKeyHex =
    "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100";

void require(bool ok, const std::string &msg) {
    if (!ok) throw std::runtime_error("admin test: " + msg);
}

bool contains(const std::string &hay, const std::string &needle) {
    return hay.find(needle) != std::string::npos;
}

AdminOpts test_opts() {
    AdminOpts opts;
    opts.key_hex = kKeyHex;
    return opts;
}

void exec_ok(sqlite3 *db, const char *sql) {
    char *err = nullptr;
    const int rc = sqlite3_exec(db, sql, nullptr, nullptr, &err);
    const std::string msg = err ? err : sqlite3_errstr(rc);
    sqlite3_free(err);
    require(rc == SQLITE_OK, std::string("exec failed: ") + sql + ": " + msg);
}

bool select_ok(sqlite3 *db, const char *sql) {
    char *err = nullptr;
    const int rc = sqlite3_exec(db, sql, nullptr, nullptr, &err);
    sqlite3_free(err);
    return rc == SQLITE_OK;
}

sqlite3 *open_storage(const std::string &path) {
    sqlite3 *raw = nullptr;
    sqlite_murmur_open_options opts{};
    opts.flags = SQLITE_MURMUR_OPEN_STORAGE_ONLY;
    const int orc = sqlite_murmur_open_v2(
        path.c_str(), &raw, SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX,
        &opts);
    require(orc == SQLITE_OK,
            "cannot open storage db: " + std::to_string(orc));
    char *err = nullptr;
    require(sqlite3_crsqlite_init(raw, &err, nullptr) == SQLITE_OK,
            "cannot init crsql");
    return raw;
}

// Open that tolerates fail-closed layers: nullptr when open itself
// refused the db (each corrupt case owns its path, so a refused open
// needs no close).
struct MaybeDb {
    sqlite3 *db;
    int rc;
};

MaybeDb open_storage_maybe(const std::string &path) {
    sqlite3 *raw = nullptr;
    sqlite_murmur_open_options opts{};
    opts.flags = SQLITE_MURMUR_OPEN_STORAGE_ONLY;
    const int orc = sqlite_murmur_open_v2(
        path.c_str(), &raw, SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX,
        &opts);
    if (orc != SQLITE_OK) return {nullptr, orc};
    char *err = nullptr;
    if (sqlite3_crsqlite_init(raw, &err, nullptr) != SQLITE_OK) {
        sqlite3_free(err);
        sqlite_murmur_close(raw);
        return {nullptr, SQLITE_ERROR};
    }
    return {raw, SQLITE_OK};
}

// Fail-closed at any layer: open refused with IOERR/CORRUPT, or the
// read itself fails.
void require_closed(MaybeDb m, const char *sql, const std::string &msg) {
    if (!m.db) {
        const int primary = m.rc & 0xff;
        require(primary == SQLITE_IOERR || primary == SQLITE_CORRUPT,
                msg + " (open rc " + std::to_string(m.rc) + ")");
        return;
    }
    require(!select_ok(m.db, sql), msg);
    require(sqlite_murmur_close(m.db) == SQLITE_OK,
            "cannot close refused db");
}

void seal_direct(sqlite3 *db) {
    sqlite_murmur_keyring_args args{};
    args.op = SQLITE_MURMUR_KEYRING_SET;
    args.param = SQLITE_MURMUR_KEYRING_PARAM_KDF;
    args.value = 0; // direct
    require(sqlite3_file_control(db, "main", SQLITE_MURMUR_FCNTL_KEYRING,
                                 &args) == SQLITE_OK,
            "seal failed");
}

struct KeyringResult {
    int rc;
    unsigned long long value;
    std::string json;
};

KeyringResult keyring_op(sqlite3 *db, int op) {
    sqlite_murmur_keyring_args args{};
    args.op = op;
    KeyringResult r{sqlite3_file_control(db, "main",
                                         SQLITE_MURMUR_FCNTL_KEYRING, &args),
                    0, ""};
    r.value = args.value;
    if (args.json) {
        r.json = args.json;
        sqlite3_free(args.json);
    }
    return r;
}

// All page records ('p' + u64be keyspace) via a raw open. The lib must
// hold no open handle on path (RocksDB LOCK).
std::vector<std::pair<std::string, std::string>> scan_pages(
    const std::string &path) {
    rocksdb::DB *db = nullptr;
    rocksdb::Options options;
    require(rocksdb::DB::Open(options, path, &db).ok(), "cannot raw-open db");
    std::vector<std::pair<std::string, std::string>> out;
    std::unique_ptr<rocksdb::Iterator> it(
        db->NewIterator(rocksdb::ReadOptions()));
    std::string lo(1, 'p');
    lo.append(8, '\0');
    for (it->Seek(lo); it->Valid(); it->Next()) {
        const rocksdb::Slice k = it->key();
        if (k.size() != 9 || k[0] != 'p') break;
        const rocksdb::Slice v = it->value();
        out.emplace_back(k.ToString(), v.ToString());
    }
    require(it->status().ok(), "raw scan failed");
    it.reset(); // iterator must die before its DB
    delete db;
    return out;
}

// Rewrite every page record through fn (in place, same keys). Returns
// the number of records fn changed.
template <typename F>
std::size_t rewrite_pages(const std::string &path, F fn) {
    rocksdb::DB *db = nullptr;
    rocksdb::Options options;
    require(rocksdb::DB::Open(options, path, &db).ok(), "cannot raw-open db");
    std::unique_ptr<rocksdb::Iterator> it(
        db->NewIterator(rocksdb::ReadOptions()));
    std::string lo(1, 'p');
    lo.append(8, '\0');
    std::size_t changed = 0;
    for (it->Seek(lo); it->Valid(); it->Next()) {
        const rocksdb::Slice k = it->key();
        if (k.size() != 9 || k[0] != 'p') break;
        const rocksdb::Slice v = it->value();
        std::string nv(v.data(), v.size());
        if (!fn(nv)) continue;
        require(db->Put(rocksdb::WriteOptions(), k, nv).ok(),
                "raw put failed");
        ++changed;
    }
    require(it->status().ok(), "raw rewrite scan failed");
    it.reset(); // iterator must die before its DB
    delete db;
    return changed;
}

// Scratch database with one CRR table and two rows. Returns the ordinal-0
// site UUID (hex) for rehome assertions.
std::string make_crr_db(const std::string &path) {
    sqlite3 *raw = nullptr;
    sqlite_murmur_open_options opts{};
    opts.flags = SQLITE_MURMUR_OPEN_STORAGE_ONLY;
    require(sqlite_murmur_open_v2(path.c_str(), &raw,
                                  SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
                                      SQLITE_OPEN_FULLMUTEX,
                                  &opts) == SQLITE_OK,
            "cannot create scratch db");
    char *err = nullptr;
    require(sqlite3_crsqlite_init(raw, &err, nullptr) == SQLITE_OK,
            "cannot init crsql");
    exec_ok(raw, "CREATE TABLE t(a INTEGER PRIMARY KEY NOT NULL, v TEXT)");
    exec_ok(raw, "SELECT crsql_as_crr('t')");
    exec_ok(raw, "INSERT INTO t VALUES(1,'one'),(2,'two')");
    sqlite3_stmt *st = nullptr;
    require(sqlite3_prepare_v2(raw,
                               "SELECT hex(site_id) FROM crsql_site_id "
                               "WHERE ordinal=0",
                               -1, &st, nullptr) == SQLITE_OK,
            "cannot read site");
    require(sqlite3_step(st) == SQLITE_ROW, "no ordinal-0 site");
    const auto *t = sqlite3_column_text(st, 0);
    const std::string site = t ? reinterpret_cast<const char *>(t) : "";
    sqlite3_finalize(st);
    require(site.size() == 32, "bad site uuid");
    exec_ok(raw, "SELECT crsql_finalize()");
    require(sqlite_murmur_close(raw) == SQLITE_OK, "cannot close scratch db");
    return site;
}

std::set<std::string> json_keys(const std::string &body) {
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

std::string query_one(const std::string &path, const std::string &sql) {
    sqlite3 *raw = nullptr;
    sqlite_murmur_open_options opts{};
    opts.flags = SQLITE_MURMUR_OPEN_STORAGE_ONLY;
    require(sqlite_murmur_open_v2(path.c_str(), &raw,
                                  SQLITE_OPEN_READONLY | SQLITE_OPEN_FULLMUTEX,
                                  &opts) == SQLITE_OK,
            "cannot reopen " + path);
    sqlite3_stmt *st = nullptr;
    std::string out;
    if (sqlite3_prepare_v2(raw, sql.c_str(), -1, &st, nullptr) == SQLITE_OK &&
        sqlite3_step(st) == SQLITE_ROW) {
        const auto *t = sqlite3_column_text(st, 0);
        out = t ? reinterpret_cast<const char *>(t) : "";
    }
    sqlite3_finalize(st);
    sqlite_murmur_close(raw);
    return out;
}

void write_file(const fs::path &p, const std::string &body) {
    std::ofstream out(p, std::ios::binary | std::ios::trunc);
    require(static_cast<bool>(out), "cannot write " + p.string());
    out << body;
    out.flush();
    require(static_cast<bool>(out), "cannot flush " + p.string());
}

} // namespace

int main() {
    char tmpl[] = "/tmp/sqlite-murmur-admin-test-XXXXXX";
    require(mkdtemp(tmpl) != nullptr, "cannot create temp dir");
    const fs::path root(tmpl);
    try {
        // Register once for the process; every command reuses the key.
        {
            std::ostringstream err;
            require(admin_ensure_registered(kKeyHex, err),
                    "test setup cannot register: " + err.str());
        }
        // 1. CLI parsing and key validation (no filesystem touched).
        {
            std::ostringstream out, err;
            const char *argv[] = {"murmur-admin", "--help"};
            require(admin_main(2, const_cast<char **>(argv), out, err) == 0,
                    "help failed");
            require(contains(out.str(), "verify"), "help text wrong");
            std::ostringstream out2, err2;
            const char *argv2[] = {"murmur-admin", "status", "/none"};
            require(admin_main(3, const_cast<char **>(argv2), out2, err2) == 1,
                    "missing key accepted");
            std::ostringstream out3, err3;
            const char *argv3[] = {"murmur-admin", "--key-hex=zzzz", "status",
                                   "/none"};
            require(admin_main(4, const_cast<char **>(argv3), out3, err3) == 1,
                    "bad key accepted");
            std::ostringstream out4, err4;
            const char *argv4[] = {"murmur-admin", "--key-hex", kKeyHex,
                                   "bogus", "/none"};
            require(admin_main(5, const_cast<char **>(argv4), out4, err4) == 1,
                    "unknown command accepted");
            AdminKey bad = admin_parse_key_hex("0123");
            require(!bad.ok, "short key accepted");
            unsigned char id[16];
            require(!admin_parse_id16("xyz", id), "bad id accepted");
        }

        // 2. Recursive copy: content, perms, dest-exists, symlink refusal.
        {
            const fs::path src = root / "copy-src";
            fs::create_directories(src / "sub");
            write_file(src / "a.txt", "A");
            write_file(src / "sub" / "b.bin", std::string("\0\1\2", 3));
            std::string msg;
            const std::string dest = (root / "copy-dest").string();
            require(admin_copy_tree(src.string(), dest, msg) == 0,
                    "copy failed: " + msg);
            require(fs::is_regular_file(fs::path(dest) / "a.txt"),
                    "copy lost file");
            require(fs::is_regular_file(fs::path(dest) / "sub" / "b.bin"),
                    "copy lost nested file");
            const auto perms =
                fs::status(fs::path(dest) / "a.txt").permissions() &
                fs::perms::all;
            require(perms ==
                        (fs::perms::owner_read | fs::perms::owner_write),
                    "copy not 0600");
            require(admin_copy_tree(src.string(), dest, msg) == 1,
                    "existing dest accepted");
            const fs::path link_src = root / "link-src";
            fs::create_directories(link_src);
            std::error_code ec;
            fs::create_symlink(src / "a.txt", link_src / "l", ec);
            require(!ec, "cannot stage symlink");
            require(admin_copy_tree(link_src.string(),
                                    (root / "link-dest").string(), msg) == 1,
                    "symlink accepted");
            require(!fs::exists(root / "link-dest"), "failed copy left dest");
        }

#ifndef _WIN32
        // 3. LOCK probe mechanics across processes (fcntl, not flock).
        {
            const fs::path dir = root / "probed";
            fs::create_directories(dir);
            std::string detail;
            require(!admin_store_locked(dir.string(), detail),
                    "free dir reported locked: " + detail);
            write_file(dir / "LOCK", "");
            require(!admin_store_locked(dir.string(), detail),
                    "unlocked file reported locked");
            int pipefd[2] = {-1, -1};
            require(pipe(pipefd) == 0, "cannot create pipe");
            const pid_t child = fork();
            require(child >= 0, "cannot fork");
            if (child == 0) {
                close(pipefd[0]);
                const int fd =
                    open((dir / "LOCK").c_str(), O_RDWR);
                if (fd >= 0) {
                    struct flock fl;
                    std::memset(&fl, 0, sizeof(fl));
                    fl.l_type = F_WRLCK;
                    if (fcntl(fd, F_SETLK, &fl) == 0) {
                        char ready = 'r';
                        const ssize_t wrote =
                            write(pipefd[1], &ready, 1);
                        (void)wrote;
                        sleep(30);
                    }
                }
                _exit(0);
            }
            close(pipefd[1]);
            char ready = 0;
            require(read(pipefd[0], &ready, 1) == 1, "child never locked");
            close(pipefd[0]);
            require(admin_store_locked(dir.string(), detail),
                    "live-held LOCK not detected: " + detail);
            kill(child, SIGKILL);
            int status = 0;
            waitpid(child, &status, 0);
            require(!admin_store_locked(dir.string(), detail),
                    "released LOCK still reported");
        }
#endif

        // 4. verify: live pass, garbage fail, manifest gates.
        {
            const std::string db = (root / "v.db").string();
            make_crr_db(db);
            std::ostringstream out, err;
            require(admin_cmd_verify(db, test_opts(), out, err) == 0,
                    "verify failed: " + err.str());
            require(contains(out.str(), "PASS"), "verify printed no PASS");
            std::ostringstream out2, err2;
            require(admin_cmd_verify((root / "missing").string(), test_opts(),
                                     out2, err2) == 1,
                    "missing dir verified");
            AdminOpts backup = test_opts();
            backup.is_backup = true;
            std::ostringstream out3, err3;
            require(admin_cmd_verify(db, backup, out3, err3) == 1,
                    "backup mode passed without manifest");
            write_file(fs::path(db) / "manifest.json",
                       "{\"format_version\":1,\"cipher\":\"aes256gcm\","
                       "\"group_id\":\"00\",\"node_id\":\"11\","
                       "\"rocksdb_seq\":7,\"created_unix_ms\":1}\n");
            std::ostringstream out4, err4;
            require(admin_cmd_verify(db, backup, out4, err4) == 0,
                    "backup mode failed: " + err4.str());
            write_file(fs::path(db) / "manifest.json",
                       "{\"format_version\":1,\"cipher\":\"rot13\","
                       "\"group_id\":\"00\",\"node_id\":\"11\","
                       "\"rocksdb_seq\":7,\"created_unix_ms\":1}\n");
            std::ostringstream out5, err5;
            require(admin_cmd_verify(db, backup, out5, err5) == 1,
                    "bad cipher manifest passed");
            // Restore the valid manifest for later tests reusing this dir.
            write_file(fs::path(db) / "manifest.json",
                       "{\"format_version\":1,\"cipher\":\"aes256gcm\","
                       "\"group_id\":\"00\",\"node_id\":\"11\","
                       "\"rocksdb_seq\":7,\"created_unix_ms\":1}\n");
        }

        // 5. restore round trip, then the MOVE rehome with the codex test:
        // history keeps the original UUID, post-reopen writes use the new.
        {
            const std::string src = (root / "r.db").string();
            const std::string old_site = make_crr_db(src);
            write_file(fs::path(src) / "manifest.json",
                       "{\"format_version\":1,\"cipher\":\"aes256gcm\","
                       "\"group_id\":\"00\",\"node_id\":\"11\","
                       "\"rocksdb_seq\":7,\"created_unix_ms\":1}\n");
            const std::string dest = (root / "r-restored").string();
            std::ostringstream out, err;
            require(admin_cmd_restore(src, dest, test_opts(), out, err) == 0,
                    "plain restore failed: " + err.str());
            require(query_one(dest, "SELECT COUNT(*) FROM t") == "2",
                    "restored data lost");

            const std::string dest2 = (root / "r-rehomed").string();
            AdminOpts rehome = test_opts();
            rehome.fresh_node = true;
            rehome.fresh_node_hex = "0102030405060708090a0b0c0d0e0f10";
            rehome.fresh_db = true;
            rehome.fresh_db_hex = "1112131415161718191a1b1c1d1e1f20";
            std::ostringstream out2, err2;
            require(admin_cmd_restore(src, dest2, rehome, out2, err2) == 0,
                    "rehome restore failed: " + err2.str());
            require(query_one(dest2,
                              "SELECT hex(site_id) FROM crsql_site_id "
                              "WHERE ordinal=0") ==
                        "0102030405060708090A0B0C0D0E0F10",
                    "new uuid not at ordinal 0");
            require(query_one(dest2,
                              "SELECT COUNT(*) FROM crsql_site_id WHERE "
                              "site_id=unhex('" +
                                  old_site + "') AND ordinal=1") == "1",
                    "old uuid not preserved at ordinal 1");
            require(query_one(dest2,
                              "SELECT COUNT(*) FROM \"t__crsql_clock\" WHERE "
                              "site_id=0") == "0",
                    "clocks still reference ordinal 0");
            require(query_one(dest2,
                              "SELECT COUNT(*) FROM \"t__crsql_clock\" WHERE "
                              "site_id=1") == "2",
                    "clocks not remapped to ordinal 1");
            require(query_one(dest2, "SELECT hex(node) FROM murmur_identity "
                                     "WHERE id=1") ==
                        "0102030405060708090A0B0C0D0E0F10",
                    "murmur_identity node wrong");
            require(query_one(dest2,
                              "SELECT value FROM murmur_config WHERE "
                              "name='group_id'") ==
                        "1112131415161718191a1b1c1d1e1f20",
                    "group not rehomed");

            // Post-reopen writes advertise the new UUID; history keeps old.
            {
                sqlite3 *raw = nullptr;
                sqlite_murmur_open_options opts{};
                opts.flags = SQLITE_MURMUR_OPEN_STORAGE_ONLY;
                require(sqlite_murmur_open_v2(
                            dest2.c_str(), &raw,
                            SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX,
                            &opts) == SQLITE_OK,
                        "cannot reopen rehomed db");
                char *cerr = nullptr;
                require(sqlite3_crsqlite_init(raw, &cerr, nullptr) == SQLITE_OK,
                        "cannot reinit crsql");
                exec_ok(raw, "INSERT INTO t VALUES(3,'three')");
                sqlite3_stmt *st = nullptr;
                require(sqlite3_prepare_v2(raw,
                                           "SELECT hex(site_id) FROM "
                                           "crsql_changes WHERE \"table\"='t' "
                                           "ORDER BY pk",
                                           -1, &st, nullptr) == SQLITE_OK,
                        "cannot read changes");
                std::vector<std::string> sites;
                while (sqlite3_step(st) == SQLITE_ROW) {
                    const auto *t =
                        sqlite3_column_text(st, 0);
                    sites.emplace_back(t ? reinterpret_cast<const char *>(t)
                                         : "");
                }
                sqlite3_finalize(st);
                exec_ok(raw, "SELECT crsql_finalize()");
                require(sqlite_murmur_close(raw) == SQLITE_OK,
                        "cannot close rehomed db");
                require(sites.size() == 3, "changes row count wrong");
                require(sites[0] == old_site && sites[1] == old_site,
                        "history uuid not preserved");
                require(sites[2] == "0102030405060708090A0B0C0D0E0F10",
                        "new write not under new uuid");
            }

            // Restore refuses an existing dest and cleans up failures.
            std::ostringstream out3, err3;
            require(admin_cmd_restore(src, dest, test_opts(), out3, err3) == 1,
                    "existing dest accepted");
            const std::string noman = (root / "noman.db").string();
            make_crr_db(noman);
            std::ostringstream out4, err4;
            require(admin_cmd_restore(noman, (root / "noman-out").string(),
                                      test_opts(), out4, err4) == 1,
                    "manifest-less restore passed dest verification");
            require(!fs::exists(root / "noman-out"),
                    "failed restore left dest behind");
        }

        // 6. status/inspect output, JSON shape, and secret redaction.
        {
            const std::string db = (root / "s.db").string();
            make_crr_db(db);
            {
                sqlite3 *raw = nullptr;
                sqlite_murmur_open_options opts{};
                opts.flags = SQLITE_MURMUR_OPEN_STORAGE_ONLY;
                require(sqlite_murmur_open_v2(
                            db.c_str(), &raw,
                            SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX,
                            &opts) == SQLITE_OK,
                        "cannot open for secret seed");
                exec_ok(raw,
                        "CREATE TABLE IF NOT EXISTS murmur_config(name TEXT "
                        "PRIMARY KEY,value TEXT NOT NULL)");
                exec_ok(raw,
                        "INSERT INTO murmur_config VALUES"
                        "('group_id','aa'),"
                        "('private_key','SUPERSECRET')");
                require(sqlite_murmur_close(raw) == SQLITE_OK,
                        "cannot close seeded db");
            }
            std::ostringstream out, err;
            require(admin_cmd_status(db, test_opts(), out, err) == 0,
                    "status failed: " + err.str());
            require(contains(out.str(), "Tables:"), "status shape wrong");
            AdminOpts json = test_opts();
            json.json = true;
            std::ostringstream out2, err2;
            require(admin_cmd_inspect(db, json, out2, err2) == 0,
                    "inspect json failed: " + err2.str());
            require(contains(out2.str(), "\"t\""), "inspect misses table");
            require(contains(out2.str(), "<redacted>"),
                    "inspect missed redaction");
            require(!contains(out2.str(), "SUPERSECRET"),
                    "inspect leaked secret");
        }

        // 7. Same-process open handle is not misread as a live lock: fcntl
        // locks merge within a process, so verify must still pass (the
        // cross-process refusal is covered by test 3 mechanics).
        {
            const std::string db = (root / "held.db").string();
            make_crr_db(db);
            sqlite3 *raw = nullptr;
            sqlite_murmur_open_options opts{};
            opts.flags = SQLITE_MURMUR_OPEN_STORAGE_ONLY;
            require(sqlite_murmur_open_v2(
                        db.c_str(), &raw,
                        SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX,
                        &opts) == SQLITE_OK,
                    "cannot hold db open");
            std::ostringstream out, err;
            require(admin_cmd_verify(db, test_opts(), out, err) == 0,
                    "verify falsely refused same-process handle: " +
                        err.str());
            require(sqlite_murmur_close(raw) == SQLITE_OK,
                    "cannot release db");
        }

        // 8. repair: healthy store recovers with data intact, missing
        // paths fail closed without creating anything, live handles are
        // refused, and --json reports the outcome.
        {
            const std::string db = (root / "repair.db").string();
            make_crr_db(db);
            std::ostringstream out, err;
            require(admin_cmd_repair(db, test_opts(), out, err) == 0,
                    "repair of healthy store failed: " + err.str());
            require(contains(out.str(), "PASS"), "repair printed no PASS");
            require(query_one(db, "SELECT COUNT(*) FROM t") == "2",
                    "repair lost data");
            AdminOpts json = test_opts();
            json.json = true;
            std::ostringstream out_json, err_json;
            require(admin_cmd_repair(db, json, out_json, err_json) == 0,
                    "repair --json failed");
            require(contains(out_json.str(), "\"repaired\":true"),
                    "repair json shape wrong");

            const std::string missing = (root / "repair-missing").string();
            std::ostringstream out2, err2;
            require(admin_cmd_repair(missing, test_opts(), out2, err2) == 1,
                    "repair of missing dir passed");
            require(!fs::exists(missing), "repair created missing dir");

            sqlite3 *raw = nullptr;
            sqlite_murmur_open_options opts{};
            opts.flags = SQLITE_MURMUR_OPEN_STORAGE_ONLY;
            require(sqlite_murmur_open_v2(
                        db.c_str(), &raw,
                        SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX,
                        &opts) == SQLITE_OK,
                    "cannot hold db open");
            std::ostringstream out3, err3;
            require(admin_cmd_repair(db, test_opts(), out3, err3) == 1,
                    "repair of live store passed");
            require(contains(err3.str(), "live target"),
                    "live refusal message wrong: " + err3.str());
            require(sqlite_murmur_close(raw) == SQLITE_OK,
                    "cannot release db");
        }

        // 9. F3: offline key inventory C API — sealed shape with an
        // exact-schema negative (no key bytes can smuggle through),
        // unsealed/missing/live mappings, null-arg misuse.
        {
            const std::string db = (root / "inv.db").string();
            make_crr_db(db);
            // Seal directly through the keyring FCTL (no Rust wiring
            // needed: seal-anytime on a STORAGE_ONLY handle).
            {
                sqlite3 *raw = nullptr;
                sqlite_murmur_open_options opts{};
                opts.flags = SQLITE_MURMUR_OPEN_STORAGE_ONLY;
                require(sqlite_murmur_open_v2(
                            db.c_str(), &raw,
                            SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX,
                            &opts) == SQLITE_OK,
                        "cannot open for seal");
                sqlite_murmur_keyring_args args{};
                args.op = SQLITE_MURMUR_KEYRING_SET;
                args.param = SQLITE_MURMUR_KEYRING_PARAM_KDF;
                args.value = 0; // direct
                require(sqlite3_file_control(raw, "main",
                                             SQLITE_MURMUR_FCNTL_KEYRING,
                                             &args) == SQLITE_OK,
                        "seal failed");
                require(sqlite_murmur_close(raw) == SQLITE_OK,
                        "cannot close sealed db");
            }
            char *json = nullptr;
            require(sqlite_murmur_key_inventory(db.c_str(), &json) ==
                        SQLITE_OK && json,
                    "inventory failed");
            const std::string body(json);
            sqlite3_free(json);
            const std::set<std::string> want = {
                "sealed", "seq",        "kdf",     "kdf_time",
                "kdf_memory_kib", "kdf_threads", "wrap_id", "current",
                "keys", "id", "created_nanos", "active"};
            require(json_keys(body) == want, "inventory schema drift: " + body);
            require(contains(body, "\"sealed\":true"), "sealed flag wrong");
            require(contains(body, "\"current\":1"), "current wrong");
            require(contains(body, "\"kdf\":\"direct\""), "kdf wrong");
            require(!contains(body, "\"key\":"), "key material in output");

            const std::string plain = (root / "inv-plain.db").string();
            make_crr_db(plain);
            char *json2 = nullptr;
            require(sqlite_murmur_key_inventory(plain.c_str(), &json2) ==
                        SQLITE_OK && json2,
                    "unsealed inventory failed");
            require(std::string(json2) == "{\"sealed\":false}",
                    "unsealed shape wrong");
            sqlite3_free(json2);

            char *json3 = reinterpret_cast<char *>(0x1);
            require(sqlite_murmur_key_inventory(
                        (root / "inv-missing").string().c_str(), &json3) ==
                        SQLITE_CANTOPEN && !json3,
                    "missing inventory wrong");
            require(!fs::exists(root / "inv-missing"),
                    "inventory created missing dir");
            require(sqlite_murmur_key_inventory(nullptr, &json3) ==
                        SQLITE_MISUSE,
                    "null path accepted");
            require(sqlite_murmur_key_inventory(db.c_str(), nullptr) ==
                        SQLITE_MISUSE,
                    "null out accepted");

            sqlite3 *raw = nullptr;
            sqlite_murmur_open_options opts{};
            opts.flags = SQLITE_MURMUR_OPEN_STORAGE_ONLY;
            require(sqlite_murmur_open_v2(
                        db.c_str(), &raw,
                        SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX,
                        &opts) == SQLITE_OK,
                    "cannot hold db open");
            char *json4 = nullptr;
            require(sqlite_murmur_key_inventory(db.c_str(), &json4) ==
                        SQLITE_BUSY && !json4,
                    "live inventory not refused");
            require(sqlite_murmur_close(raw) == SQLITE_OK,
                    "cannot release db");
        }

        // 10. D2b: rotation at the Store level. Format pins (EXP3
        // magic + u32le id bytes), round-trip read-back,
        // unknown/zero id + unknown magic fail closed, AAD tamper
        // breaks auth, prune cases, unsealed guards.
        {
            // Format + round-trip on a mixed-generation store.
            const std::string db = (root / "rot.db").string();
            make_crr_db(db);
            {
                sqlite3 *raw = open_storage(db);
                seal_direct(raw);
                exec_ok(raw, "INSERT INTO t VALUES(3,'three')");
                require(sqlite_murmur_close(raw) == SQLITE_OK,
                        "cannot close rot db");
            }
            const auto pages = scan_pages(db);
            std::size_t exp1 = 0, exp3 = 0;
            for (const auto &kv : pages) {
                const std::string &v = kv.second;
                if (v.size() >= 4 && v.compare(0, 4, "EXP1") == 0) ++exp1;
                if (v.size() >= 9 && v.compare(0, 4, "EXP3") == 0) {
                    ++exp3;
                    // u32le id 1: 01 00 00 00 at offset 5.
                    require(v[5] == '\x01' && v[6] == '\0' &&
                                v[7] == '\0' && v[8] == '\0',
                            "EXP3 id not u32le 1");
                }
            }
            require(exp1 > 0, "no pre-seal EXP1 pages");
            require(exp3 > 0, "no post-seal EXP3 pages");
            {
                // Round-trip: the sealed write reads back through a
                // fresh handle (exercises the key-id AAD path).
                sqlite3 *raw = open_storage(db);
                sqlite3_stmt *st = nullptr;
                require(sqlite3_prepare_v2(raw, "SELECT v FROM t WHERE a=3",
                                           -1, &st, nullptr) == SQLITE_OK,
                        "cannot query rotated row");
                require(sqlite3_step(st) == SQLITE_ROW, "rotated row lost");
                const auto *t = sqlite3_column_text(st, 0);
                require(t && std::string(reinterpret_cast<const char *>(t)) ==
                                    "three",
                        "rotated row wrong");
                sqlite3_finalize(st);
                // Rotation ids increase; F3 sees the new envelope.
                KeyringResult r1 =
                    keyring_op(raw, SQLITE_MURMUR_KEYRING_ROTATE);
                require(r1.rc == SQLITE_OK && r1.value == 2,
                        "rotate id wrong");
                KeyringResult r2 =
                    keyring_op(raw, SQLITE_MURMUR_KEYRING_ROTATE);
                require(r2.rc == SQLITE_OK && r2.value == 3,
                        "re-rotate id wrong");
                require(sqlite_murmur_close(raw) == SQLITE_OK,
                        "cannot close rotated db");
            }
            {
                char *json = nullptr;
                require(sqlite_murmur_key_inventory(db.c_str(), &json) ==
                            SQLITE_OK && json,
                        "rotated inventory failed");
                const std::string body(json);
                sqlite3_free(json);
                require(contains(body, "\"seq\":3"),
                        "rotated seq wrong: " + body);
                require(contains(body, "\"current\":3"),
                        "rotated current wrong: " + body);
            }
            // Prune blocked by the pre-seal EXP1 pages.
            {
                sqlite3 *raw = open_storage(db);
                KeyringResult p =
                    keyring_op(raw, SQLITE_MURMUR_KEYRING_PRUNE);
                require(p.rc == SQLITE_OK, "prune failed");
                require(contains(p.json, "\"pruned\":[]"),
                        "prune dropped live keys: " + p.json);
                require(contains(p.json, "\"retained\":[1,2,3]"),
                        "prune retained wrong: " + p.json);
                require(!contains(p.json, "\"blocked_by_unkeyed\":0"),
                        "prune missed unkeyed pages: " + p.json);
                require(sqlite_murmur_close(raw) == SQLITE_OK,
                        "cannot close pruned db");
            }
            // Prune on a pageless sealed store drops the old key.
            {
                const std::string empty = (root / "rot-empty.db").string();
                sqlite3 *raw = nullptr;
                sqlite_murmur_open_options opts{};
                opts.flags = SQLITE_MURMUR_OPEN_STORAGE_ONLY;
                require(sqlite_murmur_open_v2(
                            empty.c_str(), &raw,
                            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
                                SQLITE_OPEN_FULLMUTEX,
                            &opts) == SQLITE_OK,
                        "cannot create empty db");
                seal_direct(raw);
                KeyringResult r =
                    keyring_op(raw, SQLITE_MURMUR_KEYRING_ROTATE);
                require(r.rc == SQLITE_OK && r.value == 2,
                        "empty rotate wrong");
                KeyringResult p =
                    keyring_op(raw, SQLITE_MURMUR_KEYRING_PRUNE);
                require(p.rc == SQLITE_OK, "empty prune failed");
                require(p.json == "{\"pruned\":[1],"
                        "\"blocked_by_unkeyed\":0,\"retained\":[2],"
                        "\"unreadable\":0}",
                        "empty prune wrong: " + p.json);
                require(sqlite_murmur_close(raw) == SQLITE_OK,
                        "cannot close empty db");
            }
            // Unsealed guards: rotate refused, prune is an empty no-op.
            {
                const std::string plain = (root / "rot-plain.db").string();
                make_crr_db(plain);
                sqlite3 *raw = open_storage(plain);
                require(keyring_op(raw, SQLITE_MURMUR_KEYRING_ROTATE).rc ==
                            SQLITE_ERROR,
                        "unsealed rotate passed");
                KeyringResult p =
                    keyring_op(raw, SQLITE_MURMUR_KEYRING_PRUNE);
                require(p.rc == SQLITE_OK &&
                            p.json == "{\"pruned\":[],"
                            "\"blocked_by_unkeyed\":0,\"retained\":[],"
                            "\"unreadable\":0}",
                        "unsealed prune wrong: " + p.json);
                require(sqlite_murmur_close(raw) == SQLITE_OK,
                        "cannot close plain db");
            }
            // Control: an identity rewrite changes nothing and the
            // store stays fully readable (proves the corrupt cases
            // below fail on the byte change, not the harness).
            {
                const std::string cdb = (root / "rot-control.db").string();
                make_crr_db(cdb);
                sqlite3 *raw = open_storage(cdb);
                seal_direct(raw);
                exec_ok(raw, "CREATE TABLE c(a PRIMARY KEY, v)");
                exec_ok(raw, "INSERT INTO c VALUES(1,'x')");
                require(sqlite_murmur_close(raw) == SQLITE_OK,
                        "cannot close control db");
                const std::size_t n = rewrite_pages(
                    cdb, [](std::string &) { return false; });
                require(n == 0, "identity rewrite changed records");
                raw = open_storage(cdb);
                require(select_ok(raw, "SELECT * FROM c"),
                        "control db unreadable after raw pass");
                require(sqlite_murmur_close(raw) == SQLITE_OK,
                        "cannot close control db");
            }
            // Unknown key id fails closed.
            {
                const std::string udb = (root / "rot-unknown.db").string();
                make_crr_db(udb);
                sqlite3 *raw = open_storage(udb);
                seal_direct(raw);
                exec_ok(raw, "CREATE TABLE u(a PRIMARY KEY, v)");
                exec_ok(raw, "INSERT INTO u VALUES(1,'x')");
                require(sqlite_murmur_close(raw) == SQLITE_OK,
                        "cannot close unknown db");
                const std::size_t n = rewrite_pages(
                    udb, [](std::string &v) {
                        if (v.size() < 9 || v.compare(0, 4, "EXP3") != 0)
                            return false;
                        v[5] = static_cast<char>(0xe7);
                        v[6] = static_cast<char>(0x03);
                        v[7] = '\0';
                        v[8] = '\0'; // id 999 u32le
                        return true;
                    });
                require(n > 0, "no EXP3 records to corrupt");
                require_closed(open_storage_maybe(udb), "SELECT * FROM u",
                               "unknown-id page readable");
            }
            // Zero key id fails closed.
            {
                const std::string zdb = (root / "rot-zero.db").string();
                make_crr_db(zdb);
                sqlite3 *raw = open_storage(zdb);
                seal_direct(raw);
                exec_ok(raw, "CREATE TABLE z(a PRIMARY KEY, v)");
                exec_ok(raw, "INSERT INTO z VALUES(1,'x')");
                require(sqlite_murmur_close(raw) == SQLITE_OK,
                        "cannot close zero db");
                const std::size_t n = rewrite_pages(
                    zdb, [](std::string &v) {
                        if (v.size() < 9 || v.compare(0, 4, "EXP3") != 0)
                            return false;
                        v[5] = v[6] = v[7] = v[8] = '\0';
                        return true;
                    });
                require(n > 0, "no EXP3 records to zero");
                require_closed(open_storage_maybe(zdb), "SELECT * FROM z",
                               "zero-id page readable");
            }
            // Unknown magic fails closed (no forward compat).
            {
                const std::string mdb = (root / "rot-magic.db").string();
                make_crr_db(mdb);
                sqlite3 *raw = open_storage(mdb);
                seal_direct(raw);
                exec_ok(raw, "CREATE TABLE m(a PRIMARY KEY, v)");
                exec_ok(raw, "INSERT INTO m VALUES(1,'x')");
                require(sqlite_murmur_close(raw) == SQLITE_OK,
                        "cannot close magic db");
                const std::size_t n = rewrite_pages(
                    mdb, [](std::string &v) {
                        if (v.size() < 9 || v.compare(0, 4, "EXP3") != 0)
                            return false;
                        v[3] = '9';
                        return true;
                    });
                require(n > 0, "no EXP3 records to remagic");
                require_closed(open_storage_maybe(mdb), "SELECT * FROM m",
                               "unknown-magic page readable");
            }
            // AAD tamper: id bytes flipped 1 -> 2 (both keys known)
            // breaks auth on the gen-1 table only.
            {
                const std::string adb = (root / "rot-aad.db").string();
                make_crr_db(adb);
                sqlite3 *raw = open_storage(adb);
                seal_direct(raw);
                exec_ok(raw, "CREATE TABLE ta(a PRIMARY KEY, v)");
                exec_ok(raw, "INSERT INTO ta VALUES(1,'gen1')");
                // Close to flush ta's pages under id 1 (records take
                // the active key at flush time, not write time).
                require(sqlite_murmur_close(raw) == SQLITE_OK,
                        "cannot close aad gen-1 db");
                raw = open_storage(adb);
                require(keyring_op(raw, SQLITE_MURMUR_KEYRING_ROTATE).rc ==
                            SQLITE_OK,
                        "aad rotate failed");
                exec_ok(raw, "CREATE TABLE tb(a PRIMARY KEY, v)");
                exec_ok(raw, "INSERT INTO tb VALUES(1,'gen2')");
                require(sqlite_murmur_close(raw) == SQLITE_OK,
                        "cannot close aad db");
                const std::size_t n = rewrite_pages(
                    adb, [](std::string &v) {
                        if (v.size() < 9 || v.compare(0, 4, "EXP3") != 0)
                            return false;
                        if (v[5] != '\x01' || v[6] != '\0' ||
                            v[7] != '\0' || v[8] != '\0')
                            return false; // gen-2 record: keep
                        v[5] = '\x02';
                        return true;
                    });
                require(n > 0, "no gen-1 records to flip");
                raw = open_storage(adb);
                require(!select_ok(raw, "SELECT * FROM ta"),
                        "AAD-tampered page readable");
                require(select_ok(raw, "SELECT * FROM tb"),
                        "gen-2 table broke under tamper");
                require(sqlite_murmur_close(raw) == SQLITE_OK,
                        "cannot close tampered db");
            }
        }

        // 11. keys shell: sealed human + --json shapes, the
        // no-key-bytes negative, unsealed/missing/live mappings,
        // argv dispatch.
        {
            const std::string db = (root / "keys.db").string();
            make_crr_db(db);
            {
                sqlite3 *raw = open_storage(db);
                seal_direct(raw);
                require(keyring_op(raw, SQLITE_MURMUR_KEYRING_ROTATE).rc ==
                            SQLITE_OK,
                        "keys rotate failed");
                require(sqlite_murmur_close(raw) == SQLITE_OK,
                        "cannot close keys db");
            }
            std::ostringstream out, err;
            require(admin_cmd_keys(db, test_opts(), out, err) == 0,
                    "keys failed: " + err.str());
            require(contains(out.str(), "Key envelope at"),
                    "keys header wrong: " + out.str());
            require(contains(out.str(), "Sealed:   yes"),
                    "keys sealed wrong: " + out.str());
            require(contains(out.str(), "KDF:      direct"),
                    "keys kdf wrong: " + out.str());
            require(contains(out.str(), "Seq:      2"),
                    "keys seq wrong: " + out.str());
            require(contains(out.str(), "Current:  2"),
                    "keys current wrong: " + out.str());
            require(contains(out.str(), "Keys:     1, 2"),
                    "keys ids wrong: " + out.str());
            AdminOpts json = test_opts();
            json.json = true;
            std::ostringstream out_json, err_json;
            require(admin_cmd_keys(db, json, out_json, err_json) == 0,
                    "keys --json failed");
            const std::set<std::string> want = {
                "sealed", "seq",        "kdf",     "kdf_time",
                "kdf_memory_kib", "kdf_threads", "wrap_id", "current",
                "keys", "id", "created_nanos", "active"};
            require(json_keys(out_json.str()) == want,
                    "keys json schema drift: " + out_json.str());
            require(contains(out_json.str(), "\"seq\":2"),
                    "keys json seq wrong");
            const std::string key_frag =
                std::string(kKeyHex).substr(0, 32);
            require(!contains(out.str(), key_frag),
                    "key bytes in human output");
            require(!contains(out_json.str(), key_frag),
                    "key bytes in json output");
            require(!contains(out_json.str(), "\"key\":"),
                    "key field in json output");

            const std::string plain = (root / "keys-plain.db").string();
            make_crr_db(plain);
            std::ostringstream out2, err2;
            require(admin_cmd_keys(plain, test_opts(), out2, err2) == 0,
                    "keys of unsealed store failed");
            require(contains(out2.str(), "No key envelope"),
                    "unsealed keys wrong: " + out2.str());
            std::ostringstream out2j, err2j;
            require(admin_cmd_keys(plain, json, out2j, err2j) == 0,
                    "keys --json of unsealed store failed");
            require(out2j.str() == "{\"sealed\":false}\n",
                    "unsealed keys json wrong: " + out2j.str());

            const std::string missing = (root / "keys-missing").string();
            std::ostringstream out3, err3;
            require(admin_cmd_keys(missing, test_opts(), out3, err3) == 1,
                    "keys of missing dir passed");
            require(!fs::exists(missing), "keys created missing dir");

            sqlite3 *raw = open_storage(plain);
            std::ostringstream out4, err4;
            require(admin_cmd_keys(plain, test_opts(), out4, err4) == 1,
                    "keys of live store passed");
            require(contains(err4.str(), "live target"),
                    "live refusal message wrong: " + err4.str());
            require(sqlite_murmur_close(raw) == SQLITE_OK,
                    "cannot release db");

            std::ostringstream out5, err5;
            const char *argv[] = {"murmur-admin", "--key-hex", kKeyHex,
                                  "keys", db.c_str()};
            require(admin_main(5, const_cast<char **>(argv), out5, err5) ==
                        0,
                    "keys argv dispatch failed: " + err5.str());
            require(contains(out5.str(), "Key envelope at"),
                    "keys argv output wrong");
            std::ostringstream out6, err6;
            const char *argv6[] = {"murmur-admin", "--key-hex", kKeyHex,
                                   "keys"};
            require(admin_main(4, const_cast<char **>(argv6), out6, err6) ==
                        1,
                    "keys without dir accepted");
        }

        // 12. rekey CLI: scratch-store round trip, idempotence, simulated crash
        // recovery, unsealed/missing/live/wrong-old refusals, file indirection,
        // and argv dispatch.
        {
            const std::string db = (root / "rekey.db").string();
            make_crr_db(db);
            {
                sqlite3 *raw = open_storage(db);
                seal_direct(raw);
                require(sqlite_murmur_close(raw) == SQLITE_OK, "cannot close db");
            }

            // (a) Unsealed refusal
            const std::string plain = (root / "rekey-plain.db").string();
            make_crr_db(plain);
            AdminOpts rekey_opts = test_opts();
            rekey_opts.new_key_hex = kNewKeyHex;
            std::ostringstream out_unsealed, err_unsealed;
            require(admin_cmd_rekey(plain, rekey_opts, out_unsealed, err_unsealed) == 1,
                    "rekey of unsealed store passed");
            require(contains(err_unsealed.str(), "unsealed"),
                    "unsealed refusal message wrong: " + err_unsealed.str());

            // (b) Missing dir refusal
            const std::string missing = (root / "rekey-missing").string();
            std::ostringstream out_missing, err_missing;
            require(admin_cmd_rekey(missing, rekey_opts, out_missing, err_missing) == 1,
                    "rekey of missing dir passed");
            require(!fs::exists(missing), "rekey created missing dir");

            // (c) Live target refusal
            sqlite3 *raw = open_storage(db);
            std::ostringstream out_live, err_live;
            require(admin_cmd_rekey(db, rekey_opts, out_live, err_live) == 1,
                    "rekey of live store passed");
            require(contains(err_live.str(), "live target"),
                    "live refusal message wrong: " + err_live.str());
            require(sqlite_murmur_close(raw) == SQLITE_OK, "cannot close live db");

            // (d) Wrong old key refusal
            AdminOpts wrong_opts = test_opts();
            wrong_opts.key_hex = "1111111111111111111111111111111111111111111111111111111111111111";
            wrong_opts.new_key_hex = kNewKeyHex;
            std::ostringstream out_wrong, err_wrong;
            require(admin_cmd_rekey(db, wrong_opts, out_wrong, err_wrong) == 1,
                    "rekey with wrong old key passed");
            require(contains(err_wrong.str(), "cannot open with provided key"),
                    "wrong old key error message wrong: " + err_wrong.str());

            // (e) Simulated crash recovery (fault hook): write 1 page and fail
            sqlite_murmur_rekey_options fault_opts{};
            fault_opts.fault_after_pages = 1;
            AdminKey k_old = admin_parse_key_hex(kKeyHex);
            AdminKey k_new = admin_parse_key_hex(kNewKeyHex);
            int fault_rc = sqlite_murmur_rekey(db.c_str(), k_old.bytes, k_new.bytes, &fault_opts);
            require(fault_rc == SQLITE_IOERR_WRITE, "fault hook did not trigger IOERR");

            // (f) Staged-resume: retry with the same OLD+NEW pair completes successfully
            std::ostringstream out_resume, err_resume;
            rekey_opts.json = true;
            int r_res = admin_cmd_rekey(db, rekey_opts, out_resume, err_resume);
            require(r_res == 0,
                    "staged resume rekey failed (rc=" + std::to_string(r_res) + "): out=" + out_resume.str() + " err=" + err_resume.str());
            require(out_resume.str() == "{\"rekeyed\":true}\n",
                    "json rekey output wrong: " + out_resume.str());

            // (g) Idempotence: rekey-twice refusal (trying to rekey again with OLD fails with AUTH)
            std::ostringstream out_twice, err_twice;
            require(admin_cmd_rekey(db, rekey_opts, out_twice, err_twice) == 1,
                    "rekey-twice with old key passed");
            require(contains(err_twice.str(), "cannot open with provided key"),
                    "rekey-twice error message wrong: " + err_twice.str());

            // (h) Verify new key works & data intact
            std::string env_bytes;
            std::string store_id_str;
            {
                rocksdb::Options ropts;
                ropts.create_if_missing = false;
                rocksdb::DB *rdb = nullptr;
                require(rocksdb::DB::Open(ropts, db, &rdb).ok(), "cannot open rekeyed rocksdb");
                require(rdb->Get(rocksdb::ReadOptions(), "meta:keyring", &env_bytes).ok(),
                        "missing meta:keyring after rekey");
                require(rdb->Get(rocksdb::ReadOptions(), "meta:store_id", &store_id_str).ok(),
                        "missing meta:store_id after rekey");
                std::string dummy;
                require(!rdb->Get(rocksdb::ReadOptions(), "meta:keyring.REKEY", &dummy).ok(),
                        "meta:keyring.REKEY left behind after rekey");
                delete rdb;
            }
            sqlite_murmur_keyring ring{};
            const auto *binding = reinterpret_cast<const unsigned char *>(store_id_str.data());
            int env_rc = sqlite_murmur_keyring_open(
                reinterpret_cast<const unsigned char *>(env_bytes.data()), env_bytes.size(),
                k_new.bytes, nullptr, binding, binding, ring);
            require(env_rc == SQLITE_OK, "new master cannot open envelope");
            require(ring.current == 1, "rekeyed active key id is not 1");
            sqlite_murmur_keyring_destroy(ring);

            // (i) File indirection (--key-file and --new-key-file)
            const std::string old_kf = (root / "old.key").string();
            const std::string new_kf = (root / "new.key").string();
            {
                std::ofstream f(old_kf); f << kNewKeyHex << "\n";
            }
            {
                std::ofstream f(new_kf); f << kKeyHex << "\n";
            }
            AdminOpts file_opts{};
            file_opts.key_file = old_kf;
            file_opts.new_key_file = new_kf;
            std::ostringstream out_file, err_file;
            require(admin_cmd_rekey(db, file_opts, out_file, err_file) == 0,
                    "rekey with key files failed: " + err_file.str());

            // (j) Argv dispatch
            std::ostringstream out_argv, err_argv;
            const char *argv[] = {"murmur-admin", "--key-hex", kKeyHex,
                                  "--new-key-hex", kNewKeyHex, "rekey", db.c_str()};
            require(admin_main(7, const_cast<char **>(argv), out_argv, err_argv) == 0,
                    "rekey argv dispatch failed: " + err_argv.str());

            std::ostringstream out_no_new, err_no_new;
            const char *argv_no_new[] = {"murmur-admin", "--key-hex", kKeyHex,
                                         "rekey", db.c_str()};
            require(admin_main(5, const_cast<char **>(argv_no_new), out_no_new, err_no_new) == 1,
                    "rekey without new key accepted");

            admin_wipe(k_old);
            admin_wipe(k_new);
        }

        std::error_code ec;
        fs::remove_all(root, ec);
        std::cout << "murmur-admin: parsing, copy, probe, verify, restore, "
                     "rehome, status, inspect, redaction, repair, inventory, "
                     "rotation, keys, rekey passed\n";
        return 0;
    } catch (const std::exception &e) {
        std::cerr << e.what() << "\nscratch kept at " << tmpl << '\n';
        return 1;
    }
}
