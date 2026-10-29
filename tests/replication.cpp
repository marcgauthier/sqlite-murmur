// Exercise actual two-peer change exchange, beyond benchmark tracking counts.
#define main sqlite_murmur_benchmark_main
#include "../benchmarks/benchmark.cpp"
#undef main
#include <fstream>

int64_t transfer(sqlite3 *source, sqlite3 *destination) {
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
    auto impacted = scalar(destination, "SELECT crsql_rows_impacted()");
    exec(destination, "COMMIT");
    return impacted;
}
void transferBulk(sqlite3 *source, sqlite3 *destination) {
    Statement changes(source, "SELECT \"table\",pk,cid,val,col_version,db_version,site_id,cl,seq FROM crsql_changes");
    exec(destination, "BEGIN IMMEDIATE; SELECT crsql_ingest_begin()");
    Statement stage(destination, "INSERT INTO crsql_staging VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)");
    int rc;
    while ((rc = sqlite3_step(changes.get())) == SQLITE_ROW) {
        stage.reset();
        for (int i = 0; i < 9; ++i)
            check(sqlite3_bind_value(stage.get(), i+1, sqlite3_column_value(changes.get(), i)), destination, "bind staged change");
        if (sqlite3_step(stage.get()) != SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(destination));
    }
    if (rc != SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(source));
    exec(destination, "SELECT crsql_ingest_commit(); COMMIT");
}

replication_blob::Batch exportAll(sqlite3 *db) {
    Statement changes(db, "SELECT * FROM crsql_changes");
    return replication_blob::capture(changes.get());
}
void applyBlob(sqlite3 *db, const std::vector<unsigned char> &blob) {
    Statement apply(db, "SELECT crsql_ingest_blob(?1)");
    check(sqlite3_bind_blob64(apply.get(), 1, blob.data(), blob.size(), SQLITE_STATIC), db, "bind blob");
    if (sqlite3_step(apply.get()) != SQLITE_ROW) throw std::runtime_error(sqlite3_errmsg(db));
}
std::string snapshot(sqlite3 *db, const char *sql) {
    Statement query(db, sql);
    std::string result;
    int rc;
    while ((rc = sqlite3_step(query.get())) == SQLITE_ROW) {
        for (int i = 0; i < sqlite3_column_count(query.get()); ++i) {
            result += std::to_string(sqlite3_column_type(query.get(), i)) + ":";
            auto *text = sqlite3_column_text(query.get(), i);
            if (text) result.append(reinterpret_cast<const char *>(text), sqlite3_column_bytes(query.get(), i));
            result += "|";
        }
        result += "\n";
    }
    if (rc != SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(db));
    return result;
}
Database memoryReplica() {
    sqlite3 *raw = nullptr;
    check(sqlite3_open_v2(":memory:", &raw, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), raw, "memory replica");
    Database db(raw);
    enableReplication(db, true);
    return db;
}
void testBlobEquivalence() {
    auto source = memoryReplica(), row = memoryReplica(), blob = memoryReplica();
    for (auto *db : {source.get(), row.get(), blob.get()})
        exec(db, "CREATE TABLE typed(id INTEGER PRIMARY KEY NOT NULL,a,b); SELECT crsql_as_crr('typed')");
    const char *base = "SELECT id,quote(a),typeof(a),quote(b),typeof(b) FROM typed ORDER BY id";
    const char *clocks = "SELECT * FROM crsql_changes ORDER BY \"table\",pk,cid";
    for (const char *mutation : {
        "INSERT INTO typed VALUES(1,NULL,x''),(2,-9223372036854775808,1.25),(3,'1',1),(4,'',x'0001ff'),(5,'é' || char(0) || 'z',9223372036854775807)",
        "BEGIN; UPDATE typed SET a=x'00',b=NULL WHERE id=2; DELETE FROM typed WHERE id=3; COMMIT",
        "INSERT INTO typed VALUES(3,'resurrected',''); UPDATE typed SET a='changed' WHERE id=1"}) {
        exec(source.get(), mutation);
        auto batch = exportAll(source.get());
        exec(blob.get(), "BEGIN");
        auto truncated = batch.blob;
        truncated.pop_back();
        bool rejected = false;
        try { applyBlob(blob.get(), truncated); } catch (const std::exception &) { rejected = true; }
        if (!rejected) throw std::runtime_error("truncated blob accepted");
        exec(blob.get(), "ROLLBACK");
        for (int replay = 0; replay < 2; ++replay) {
            // transfer() supplies the same transaction boundary as blob ingestion.
            auto rowImpacted = transfer(source.get(), row.get());
            exec(blob.get(), "BEGIN IMMEDIATE");
            applyBlob(blob.get(), batch.blob);
            if (scalar(blob.get(), "SELECT crsql_rows_impacted()") != rowImpacted) throw std::runtime_error("blob rows_impacted mismatch");
            exec(blob.get(), "COMMIT");
            if (snapshot(row.get(), base) != snapshot(blob.get(), base) ||
                snapshot(row.get(), clocks) != snapshot(blob.get(), clocks) ||
                scalar(row.get(), "SELECT crsql_db_version()") != scalar(blob.get(), "SELECT crsql_db_version()"))
                throw std::runtime_error("blob and row ingestion differ");
        }
    }
    auto empty = memoryReplica();
    exec(empty.get(), "CREATE TABLE typed(id INTEGER PRIMARY KEY NOT NULL,a,b); SELECT crsql_as_crr('typed')");
    auto batch = exportAll(empty.get());
    applyBlob(empty.get(), batch.blob);
}
int main(int argc, char **argv) {
    if (argc == 3 && std::string(argv[1]) == "--pack-fixture") {
        auto db = memoryReplica();
        Statement fixture(db.get(), "SELECT 't',x'0001','c',NULL,NULL,NULL,NULL,NULL,NULL UNION ALL "
            "SELECT 't',x'ff','c',-9223372036854775808,1,2,x'00ff',3,4 UNION ALL "
            "SELECT 't',x'','c',1.25,1,2,NULL,3,4 UNION ALL "
            "SELECT 'é',x'','c','é' || char(0) || 'z',1,2,NULL,3,4 UNION ALL "
            "SELECT 't',x'','c',x'',1,2,NULL,3,4 UNION ALL "
            "SELECT 't',x'','c','',1,2,NULL,3,4 UNION ALL "
            "SELECT 't',x'','c',x'00ff',1,2,NULL,3,4");
        auto batch = replication_blob::capture(fixture.get());
        std::ofstream out(argv[2], std::ios::binary);
        out.write(reinterpret_cast<const char *>(batch.blob.data()), batch.blob.size());
        return out ? 0 : 1;
    }
    testBlobEquivalence();

    const auto root = std::filesystem::temp_directory_path() / ("sqlite-murmur-replication-" + std::to_string(Clock::now().time_since_epoch().count()));
    const auto aPath = (root / "a").string(), bPath = (root / "b").string(), cPath = (root / "c").string();
    try {
        std::filesystem::create_directories(root);
        unsigned char key[32] = {42};
        sqlite_murmur_config config{key, 100, 16384, 1};
        check(sqlite_murmur_register(&config), nullptr, "register");
        {
            auto a = open(aPath, 1024, true), b = open(bPath, 1024, true), c = open(cPath, 1024, true);
            for (auto *db : {&a, &b, &c}) {
                exec(db->get(), "PRAGMA cipher='aes256gcm'; PRAGMA compression='zstd3'; PRAGMA journal_mode=DELETE");
                enableReplication(*db, true);
                exec(db->get(), "CREATE TABLE records(id INTEGER PRIMARY KEY NOT NULL,body TEXT NOT NULL DEFAULT ''); SELECT crsql_as_crr('main','records')");
            }
            const auto shaQuery = "SELECT crsql_sha()='" + std::string(SQLITE_MURMUR_CRSQLITE_VERSION) + "'";
            if (scalar(a.get(), shaQuery.c_str()) != 1) throw std::runtime_error("compiled replication source identity mismatch");
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
            transferBulk(a.get(), c.get()); transferBulk(a.get(), c.get());
            if (scalar(c.get(), "SELECT count(*) FROM records WHERE id=1 AND body='updated'") != 1 ||
                scalar(c.get(), "SELECT count(*) FROM records") != 1)
                throw std::runtime_error("bulk ingestion did not converge or replay idempotently");
            auto peer = open(aPath, 1024, true); enableReplication(peer, true);
            const auto versionBefore = scalar(peer.get(), "SELECT crsql_db_version()");
            exec(a.get(), "BEGIN IMMEDIATE; INSERT INTO records VALUES(3,'rolled back'); UPDATE records SET body='rolled back' WHERE id=1; ROLLBACK");
            if (scalar(a.get(), "SELECT count(*) FROM records WHERE body='rolled back'") != 0 ||
                scalar(a.get(), "SELECT count(*) FROM crsql_changes WHERE cid='body' AND val='rolled back'") != 0 ||
                scalar(a.get(), "SELECT crsql_db_version()") != versionBefore)
                throw std::runtime_error("rollback leaked user data, clocks or a pending db version");
            exec(a.get(), "INSERT INTO records VALUES(3,'after rollback')");
            if (scalar(peer.get(), "SELECT crsql_db_version()") <= versionBefore ||
                scalar(peer.get(), "SELECT count(*) FROM records WHERE id=3 AND body='after rollback'") != 1)
                throw std::runtime_error("connection did not observe a peer's committed write/version");
            exec(peer.get(), "BEGIN IMMEDIATE; UPDATE records SET body='same final value' WHERE id=3; UPDATE records SET body='same final value' WHERE id=3; COMMIT");
            transfer(a.get(), b.get());
            auto finalBlob = exportAll(a.get());
            exec(c.get(), "BEGIN"); applyBlob(c.get(), finalBlob.blob); exec(c.get(), "COMMIT");
            for (auto *db : {b.get(), c.get()})
                if (scalar(db, "SELECT count(*) FROM records WHERE id=3 AND body='same final value'") != 1)
                    throw std::runtime_error("post-rollback/repeated-update tracking failed");
        }
        {
            auto c = open(cPath, 1024, true); enableReplication(c, true);
            if (scalar(c.get(), "SELECT count(*) FROM records WHERE id=3 AND body='same final value'") != 1) throw std::runtime_error("blob state did not persist");
            auto b = open(bPath, 1024, true); enableReplication(b, true);
            if (scalar(b.get(), "SELECT count(*) FROM records WHERE id=1 AND body='updated'") != 1 ||
                scalar(b.get(), "SELECT count(*) FROM records WHERE id=3 AND body='same final value'") != 1 ||
                scalar(b.get(), "SELECT crsql_db_version()") <= 0)
                throw std::runtime_error("replication state did not persist");
        }
        check(sqlite_murmur_unregister(), nullptr, "unregister");
        std::filesystem::remove_all(root);
        std::cout << "Encrypted replication, CRSB blob/row equivalence, staged bulk ingest, replay, rollback, peer version cache and reopen passed.\n";
        return 0;
    } catch (const std::exception &e) { std::cerr << e.what() << '\n'; return 1; }
}
