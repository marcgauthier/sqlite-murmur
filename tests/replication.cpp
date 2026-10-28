// Exercise actual two-peer change exchange, beyond benchmark tracking counts.
#define main sqlite_express_benchmark_main
#include "../benchmarks/benchmark.cpp"
#undef main

void transfer(sqlite3 *source, sqlite3 *destination) {
    Statement changes(source, "SELECT \"table\",pk,cid,val,col_version,db_version,site_id,cl,seq FROM crsql_changes");
    Statement apply(destination, "INSERT INTO crsql_changes(\"table\",pk,cid,val,col_version,db_version,site_id,cl,seq) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)");
    exec(destination, "BEGIN IMMEDIATE");
    int rc;
    while ((rc = sqlite3_step(changes.get())) == SQLITE_ROW) {
        apply.reset();
        for (int i = 0; i < 9; ++i)
            check(sqlite3_bind_value(apply.get(), i + 1, sqlite3_column_value(changes.get(), i)), destination, "bind replicated change");
        if (sqlite3_step(apply.get()) != SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(destination));
    }
    if (rc != SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(source));
    exec(destination, "COMMIT");
}
int main() {
    const auto root = std::filesystem::temp_directory_path() / ("sqlite-express-replication-" + std::to_string(Clock::now().time_since_epoch().count()));
    const auto aPath = (root / "a").string(), bPath = (root / "b").string();
    try {
        std::filesystem::create_directories(root);
        unsigned char key[32] = {42};
        sqlite_express_config config{key, 100, 16384, 1};
        check(sqlite_express_register(&config), nullptr, "register");
        {
            auto a = open(aPath, 1024), b = open(bPath, 1024);
            for (auto *db : {&a, &b}) {
                exec(db->get(), "PRAGMA cipher='aes256gcm'; PRAGMA compression='zstd3'; PRAGMA journal_mode=DELETE");
                enableReplication(*db, true);
                exec(db->get(), "CREATE TABLE records(id INTEGER PRIMARY KEY NOT NULL,body TEXT NOT NULL DEFAULT ''); SELECT crsql_as_crr('main','records')");
            }
            exec(a.get(), "INSERT INTO records VALUES(1,'from a')");
            exec(b.get(), "INSERT INTO records VALUES(2,'from b')");
            transfer(a.get(), b.get()); transfer(b.get(), a.get());
            transfer(a.get(), b.get()); // Replaying identical changes is idempotent.
            for (auto *db : {a.get(), b.get()}) {
                if (scalar(db, "SELECT count(*) FROM records WHERE (id=1 AND body='from a') OR (id=2 AND body='from b')") != 2)
                    throw std::runtime_error("replicas did not converge");
            }
            exec(a.get(), "UPDATE records SET body='updated' WHERE id=1");
            transfer(a.get(), b.get());
            if (scalar(b.get(), "SELECT count(*) FROM records WHERE id=1 AND body='updated'") != 1)
                throw std::runtime_error("replicated update missing");
            exec(b.get(), "DELETE FROM records WHERE id=2");
            transfer(b.get(), a.get());
            if (scalar(a.get(), "SELECT count(*) FROM records") != 1) throw std::runtime_error("replicated delete missing");
        }
        {
            auto b = open(bPath, 1024); enableReplication(b, true);
            if (scalar(b.get(), "SELECT count(*) FROM records WHERE id=1 AND body='updated'") != 1 ||
                scalar(b.get(), "SELECT crsql_db_version()") <= 0)
                throw std::runtime_error("replication state did not persist");
        }
        check(sqlite_express_unregister(), nullptr, "unregister");
        std::filesystem::remove_all(root);
        std::cout << "Encrypted two-peer replication, replay, update, delete and reopen passed.\n";
        return 0;
    } catch (const std::exception &e) { std::cerr << e.what() << '\n'; return 1; }
}
