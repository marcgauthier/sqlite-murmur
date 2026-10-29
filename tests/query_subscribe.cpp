// WP-E point 17 tests: live subscriptions over the proposed poll-based C
// API. The prototypes below are PROPOSED (see murmur-query
// INTEGRATION-PROPOSAL.md); the lead hosts final declarations in a public
// header at merge time, and this file adopts them then. Until wiring
// merges, this test fails to link/run — by design (owned files only).
//
// Usage: query_subscribe <db-path>
#include "sqlite_murmur.h"
#include "sqlite_murmur_crsqlite.h"
#include "sqlite_murmur_replication.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <string>

// ---- proposed subscription C API (pending lead header merge) ----
extern "C" {
typedef struct murmur_sub murmur_sub;
typedef struct {
    int buffer_size;   // default 32
    int emit_unchanged; // default 0
    const char *resume_cursor_json; // default NULL
} murmur_sub_opts;
murmur_sub *murmur_subscribe(sqlite3 *db, const char *sql, const char *tables_json,
                             const murmur_sub_opts *opts);
int murmur_sub_poll(murmur_sub *s, int timeout_ms, char **event_json); // 1 event, 0 timeout, <0 closed
void murmur_sub_free_event(char *event_json);
void murmur_sub_close(murmur_sub *s);
}

namespace {
void check(int rc, sqlite3 *db, const char *where) {
    if (rc != SQLITE_OK)
        throw std::runtime_error(std::string(where) + ": " + (db ? sqlite3_errmsg(db) : sqlite3_errstr(rc)));
}
void exec(sqlite3 *db, const std::string &text) {
    char *msg = nullptr;
    int rc = sqlite3_exec(db, text.c_str(), nullptr, nullptr, &msg);
    std::string error = msg ? msg : "";
    sqlite3_free(msg);
    if (rc != SQLITE_OK) throw std::runtime_error(text + ": " + error);
}
void require(bool ok, const std::string &what) {
    if (!ok) throw std::runtime_error("FAILED: " + what);
}
std::string poll(murmur_sub *s, int timeout_ms, const std::string &what) {
    char *event = nullptr;
    int rc = murmur_sub_poll(s, timeout_ms, &event);
    require(rc == 1 && event, what + " (poll rc=" + std::to_string(rc) + ")");
    std::string out = event;
    murmur_sub_free_event(event);
    return out;
}
} // namespace

int main(int argc, char **argv) {
    if (argc < 2) return 2;
    std::remove(argv[1]);
    sqlite3 *db = nullptr;
    murmur_sub *sub = nullptr;
    try {
        unsigned char key[32] = {42};
        sqlite_murmur_config cfg{key, 60000, 16384, 1};
        check(sqlite_murmur_register(&cfg), nullptr, "register VFS");
        check(sqlite3_open_v2(argv[1], &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "sqlite-murmur"), db, "open");
        sqlite3_busy_timeout(db, 2000);
        char *error = nullptr;
        int rc = sqlite3_crsqlite_init(db, &error, nullptr);
        if (rc) {
            std::string message = error ? error : "";
            sqlite3_free(error);
            throw std::runtime_error(message);
        }
        rc = sqlite3_murmurreplication_init(db, &error, nullptr);
        if (rc) {
            std::string message = error ? error : "";
            sqlite3_free(error);
            throw std::runtime_error(message);
        }
        exec(db, "CREATE TABLE docs(id INTEGER PRIMARY KEY NOT NULL, title TEXT);"
                 "SELECT crsql_as_crr('docs');"
                 "INSERT INTO docs VALUES(1,'one');");

        // Initial snapshot arrives immediately.
        murmur_sub_opts opts{32, 0, nullptr};
        sub = murmur_subscribe(db, "SELECT id, title FROM docs ORDER BY id", "[\"docs\"]", &opts);
        require(sub, "subscribe");
        std::string initial = poll(sub, 5000, "initial snapshot");
        require(initial.find("Initial") != std::string::npos && initial.find("one") != std::string::npos,
                "initial content: " + initial);

        // A commit affecting the watched table yields an Update diff.
        exec(db, "INSERT INTO docs VALUES(2,'two')");
        std::string update = poll(sub, 5000, "update");
        require(update.find("Update") != std::string::npos && update.find("Added") != std::string::npos,
                "update content: " + update);

        // Commits to unwatched tables stay silent.
        exec(db, "CREATE TABLE other(id INTEGER PRIMARY KEY NOT NULL);"
                 "SELECT crsql_as_crr('other');"
                 "INSERT INTO other VALUES(1)");
        char *event = nullptr;
        require(murmur_sub_poll(sub, 700, &event) == 0, "unrelated commit muted");
        murmur_sub_close(sub);
        sub = nullptr;

        // Resume cursors: extract the update cursor and resume from it.
        auto at = update.find("\"cursor\"");
        require(at != std::string::npos, "cursor present");
        auto end = update.find('}', at);
        std::string cursor = update.substr(at + 9, end - at - 8);
        murmur_sub_opts resume{32, 0, cursor.c_str()};
        sub = murmur_subscribe(db, "SELECT id, title FROM docs ORDER BY id", "[\"docs\"]", &resume);
        require(sub, "resume subscribe");
        std::string resumed = poll(sub, 5000, "resumed snapshot");
        require(resumed.find("Update") != std::string::npos, "resumed as update: " + resumed);
        murmur_sub_close(sub);
        sub = nullptr;

        check(sqlite_murmur_close(db), nullptr, "close");
        db = nullptr;
        std::printf("query_subscribe: all assertions passed\n");
        return 0;
    } catch (const std::exception &e) {
        std::fprintf(stderr, "query_subscribe FAILED: %s\n", e.what());
        if (sub) murmur_sub_close(sub);
        if (db) sqlite_murmur_close(db);
        return 1;
    }
}
