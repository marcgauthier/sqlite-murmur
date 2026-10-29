// WP-E query-plane tests: PRAGMAs, search (FTS5 + vec0), column declarations,
// migrations, batch exec, export/import (points 15, 16, 18). Point 17 lives
// in query_subscribe.cpp (needs the proposed C subscription API).
//
// Usage: query_search <db-path>  (fails loudly until lead merges wiring)
#include "sqlite_murmur.h"
#include "sqlite_murmur_crsqlite.h"
#include "sqlite_murmur_replication.h"
#include <cstdio>
#include <stdexcept>
#include <string>

namespace {
void check(int rc, sqlite3 *db, const char *where) {
    if (rc != SQLITE_OK)
        throw std::runtime_error(std::string(where) + ": " + (db ? sqlite3_errmsg(db) : sqlite3_errstr(rc)));
}
std::string sql(sqlite3 *db, const std::string &text) {
    const char *tail = text.c_str();
    std::string output;
    while (*tail) {
        sqlite3_stmt *stmt = nullptr;
        check(sqlite3_prepare_v2(db, tail, -1, &stmt, &tail), db, "prepare");
        if (!stmt) continue;
        int rc;
        while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
            if (!output.empty()) output += '|';
            for (int i = 0; i < sqlite3_column_count(stmt); ++i) {
                if (i) output += '~';
                auto *p = sqlite3_column_text(stmt, i);
                if (p) output.append(reinterpret_cast<const char *>(p), sqlite3_column_bytes(stmt, i));
            }
        }
        std::string error = sqlite3_errmsg(db);
        sqlite3_finalize(stmt);
        if (rc != SQLITE_DONE) throw std::runtime_error(error);
    }
    return output;
}
void require(bool ok, const std::string &what) {
    if (!ok) throw std::runtime_error("FAILED: " + what);
}
void requireFails(sqlite3 *db, const std::string &text, const std::string &what) {
    bool failed = false;
    try {
        sql(db, text);
    } catch (...) {
        failed = true;
    }
    require(failed, what);
}
} // namespace

int main(int argc, char **argv) {
    if (argc < 2) return 2;
    std::remove(argv[1]);
    sqlite3 *db = nullptr;
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

        // ---- PRAGMA defaults + read-back + ranges (constraint 8) ----
        require(sql(db, "PRAGMA murmur_search_queue_bytes") == "8388608", "search_queue_bytes default");
        require(sql(db, "PRAGMA murmur_search_batch_rows") == "256", "search_batch_rows default");
        require(sql(db, "PRAGMA murmur_search_interval_ms") == "100", "search_interval_ms default");
        require(sql(db, "PRAGMA murmur_sub_buffer_size") == "32", "sub_buffer_size default");
        require(sql(db, "PRAGMA murmur_sub_max_history") == "1024", "sub_max_history default");
        require(sql(db, "PRAGMA murmur_sub_emit_unchanged") == "0", "sub_emit_unchanged default");
        require(sql(db, "PRAGMA murmur_migrate_chunk_rows") == "2000", "migrate_chunk_rows default");
        require(sql(db, "PRAGMA murmur_migrate_chunk_bytes") == "2097152", "migrate_chunk_bytes default");
        require(sql(db, "PRAGMA murmur_exec_max_statements") == "1024", "exec_max_statements default");
        sql(db, "PRAGMA murmur_search_batch_rows='512'");
        require(sql(db, "PRAGMA murmur_search_batch_rows") == "512", "search_batch_rows read-back");
        requireFails(db, "PRAGMA murmur_search_batch_rows='0'", "batch_rows low rejected");
        requireFails(db, "PRAGMA murmur_search_batch_rows='70000'", "batch_rows high rejected");
        requireFails(db, "PRAGMA murmur_search_queue_bytes='10'", "queue_bytes low rejected");
        requireFails(db, "PRAGMA murmur_sub_emit_unchanged='2'", "emit_unchanged range rejected");
        requireFails(db, "PRAGMA murmur_nope='1'", "unknown pragma rejected");

        // ---- FTS5 search ----
        sql(db, "CREATE TABLE docs(id INTEGER PRIMARY KEY NOT NULL, title TEXT);"
                "SELECT crsql_as_crr('docs');"
                "INSERT INTO docs VALUES(1,'hello world');"
                "INSERT INTO docs VALUES(2,'goodbye world');");
        sql(db, "SELECT murmur_search_create_fts('docs_fts','docs','[\"title\"]','unicode61')");
        sql(db, "SELECT murmur_refresh_indexes()");
        require(sql(db, "SELECT count(*) FROM murmur_fts_docs_fts WHERE murmur_fts_docs_fts MATCH 'hello'") == "1", "fts match");
        require(sql(db, "SELECT count(*) FROM murmur_fts_docs_fts WHERE murmur_fts_docs_fts MATCH 'world'") == "2", "fts match 2");
        std::string status = sql(db, "SELECT murmur_search_status()");
        require(status.find("murmur_fts_docs_fts") != std::string::npos && status.find("\"ready\":true") != std::string::npos,
                "search status ready: " + status);
        requireFails(db, "SELECT murmur_search_create_fts('bad','docs','[\"title\"]','nope')", "bad tokenizer rejected");
        require(sql(db, "SELECT murmur_search_drop('docs_fts')") == "1", "fts drop");
        require(sql(db, "SELECT murmur_search_drop('docs_fts')") == "0", "fts drop missing");

        // ---- column declarations ----
        sql(db, "SELECT murmur_column_fts('docs','title',1)");
        sql(db, "SELECT murmur_refresh_indexes()");
        status = sql(db, "SELECT murmur_search_status()");
        require(status.find("murmur_fts_") != std::string::npos, "column fts declared: " + status);
        {
            // Index names embed the stable table id; extract and query it.
            auto at = status.find("murmur_fts_");
            auto end = status.find('"', at);
            std::string name = status.substr(at, end - at);
            require(sql(db, "SELECT count(*) FROM \"" + name + "\" WHERE \"" + name + "\" MATCH 'hello'") == "1",
                    "column fts match");
        }
        requireFails(db, "SELECT murmur_column_fts('docs','nope',1)", "unknown column rejected");
        sql(db, "SELECT murmur_column_fts('docs','title',0)");

        // ---- vector search ----
        sql(db, "CREATE TABLE vecs(id INTEGER PRIMARY KEY NOT NULL, embedding BLOB);"
                "SELECT crsql_as_crr('vecs');");
        {
            // Two float32[2] embeddings: (1,0) and (0,1).
            sqlite3_stmt *stmt = nullptr;
            check(sqlite3_prepare_v2(db, "INSERT INTO vecs VALUES(?,?)", -1, &stmt, nullptr), db, "prepare vec");
            float a[2] = {1.0f, 0.0f}, b[2] = {0.0f, 1.0f};
            sqlite3_bind_int(stmt, 1, 1);
            sqlite3_bind_blob(stmt, 2, a, sizeof a, SQLITE_TRANSIENT);
            require(sqlite3_step(stmt) == SQLITE_DONE, "step vec a");
            sqlite3_reset(stmt);
            sqlite3_bind_int(stmt, 1, 2);
            sqlite3_bind_blob(stmt, 2, b, sizeof b, SQLITE_TRANSIENT);
            require(sqlite3_step(stmt) == SQLITE_DONE, "step vec b");
            sqlite3_finalize(stmt);
        }
        sql(db, "SELECT murmur_search_create_vec('vecs_vec','vecs','embedding',2,'l2')");
        sql(db, "SELECT murmur_refresh_indexes()");
        require(sql(db, "SELECT rowid FROM murmur_vec_vecs_vec WHERE embedding MATCH '[1,0]' AND k=1") == "1", "knn");
        requireFails(db, "SELECT murmur_search_create_vec('v2','vecs','embedding',0,'l2')", "zero dims rejected");
        requireFails(db, "SELECT murmur_search_create_vec('v2','vecs','embedding',2,'nope')", "bad metric rejected");
        sql(db, "SELECT murmur_column_vector('vecs','embedding',2,'cosine')");
        sql(db, "SELECT murmur_refresh_indexes()");
        sql(db, "SELECT murmur_column_vector('vecs','embedding',NULL,NULL)");

        // ---- migrations ----
        std::string history = sql(db, "SELECT murmur_migrate('ALTER TABLE docs ADD COLUMN body TEXT','[]')");
        require(history.size() == 64, "migrate history hash");
        requireFails(db, "SELECT murmur_migrate('CREATE TABLE bad(a CHECK(a>0),PRIMARY KEY(a))','[]')",
                     "unsupported schema rejected");
        requireFails(db, "SELECT murmur_migrate('DROP TABLE docs','[]')", "drop replicated rejected");

        // ---- batch exec ----
        require(sql(db, "SELECT murmur_exec_json('[\"INSERT INTO docs(id,title) VALUES(3,''three'')\"]')") ==
                    "{\"statements\":1,\"rows_changed\":1}",
                "exec_json");
        sql(db, "PRAGMA murmur_exec_max_statements='1'");
        requireFails(db, "SELECT murmur_exec_json('[\"SELECT 1\",\"SELECT 2\"]')", "exec over cap");
        sql(db, "PRAGMA murmur_exec_max_statements='2'");
        require(sql(db, "SELECT murmur_exec_json('[\"SELECT 1\",\"SELECT 2\"]')") == "{\"statements\":2,\"rows_changed\":0}",
                "exec_json capped ok");
        sql(db, "PRAGMA murmur_exec_max_statements='1024'");
        require(sql(db, "SELECT count(*) FROM docs WHERE id=3") == "1", "exec persisted");
        requireFails(db, "SELECT murmur_exec_json('[\"INSERT INTO docs(id) VALUES(4)\",\"BOGUS\"]')", "exec rolls back");
        require(sql(db, "SELECT count(*) FROM docs WHERE id=4") == "0", "exec rollback");

        // ---- export/import ----
        std::string exported = sql(db, "SELECT murmur_export(0)");
        require(exported.find("\"db_version\"") != std::string::npos &&
                    exported.find("\"changes\"") != std::string::npos,
                "export shape: " + exported);
        requireFails(db, "SELECT murmur_import('{}')", "import unavailable until WP-A");

        // ---- table flags relax the migrate guard ----
        require(sql(db, "PRAGMA murmur_table_replication") == "{}", "flags default");
        sql(db, "PRAGMA murmur_table_replication='{\"docs\":false}'");
        require(sql(db, "PRAGMA murmur_table_replication") == "{\"docs\":false}", "flags read-back");
        require(sql(db, "SELECT murmur_migrate('DROP TABLE docs','[]')").size() > 0, "drop opted-out");
        require(sql(db, "SELECT count(*) FROM sqlite_schema WHERE name='docs'") == "0", "docs dropped");

        check(sqlite_murmur_close(db), nullptr, "close");
        db = nullptr;
        std::printf("query_search: all assertions passed\n");
        return 0;
    } catch (const std::exception &e) {
        std::fprintf(stderr, "query_search FAILED: %s\n", e.what());
        if (db) sqlite_murmur_close(db);
        return 1;
    }
}
