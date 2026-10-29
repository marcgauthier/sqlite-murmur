#include "sqlite_murmur.h"
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <chrono>
#include <iostream>

void require(bool condition,const char *message){if(!condition)throw std::runtime_error(message);}
void exec(sqlite3 *db,const char *sql){char *error=nullptr;int rc=sqlite3_exec(db,sql,nullptr,nullptr,&error);std::string message=error?error:"SQL failed";sqlite3_free(error);if(rc!=SQLITE_OK)throw std::runtime_error(message);}
void fails(sqlite3 *db,const char *sql,const char *message){try{exec(db,sql);}catch(...){return;}throw std::runtime_error(message);}
void fails_with(sqlite3 *db,const char *sql,const char *needle,const char *message){char *error=nullptr;int rc=sqlite3_exec(db,sql,nullptr,nullptr,&error);std::string text=error?error:"";sqlite3_free(error);if(rc==SQLITE_OK||text.find(needle)==std::string::npos)throw std::runtime_error(std::string(message)+": "+text);}
bool contains(const std::string &hay,const char *needle){return hay.find(needle)!=std::string::npos;}
long stat_count(const std::string &health,const char *klass){auto at=health.find(std::string("\"")+klass+"\":{\"acquisitions\":");require(at!=std::string::npos,"health class missing");return std::stol(health.substr(at+std::string(klass).size()+19));}
std::string value(sqlite3 *db,const char *sql){sqlite3_stmt *s=nullptr;require(sqlite3_prepare_v2(db,sql,-1,&s,nullptr)==SQLITE_OK,sqlite3_errmsg(db));require(sqlite3_step(s)==SQLITE_ROW,sqlite3_errmsg(db));auto p=sqlite3_column_text(s,0);std::string result=p?reinterpret_cast<const char*>(p):"";sqlite3_finalize(s);return result;}
int main(){
    auto root=std::filesystem::temp_directory_path()/std::filesystem::path("sqlite-murmur-application-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    try{
        std::filesystem::create_directories(root);
        unsigned char key[32]={42};sqlite_murmur_config cfg{key,100,16384,1};
        require(sqlite_murmur_register(&cfg)==SQLITE_OK,"register");
        unsigned char fkey[32]={7};
        require(sqlite_murmur_register_file_key(fkey,nullptr)==SQLITE_OK,"file key provision");
        sqlite3 *a=nullptr,*b=nullptr;
        auto path=(root/"integrated").string();
        sqlite_murmur_open_options options{0,
#ifdef SQLITE_MURMUR_TEST_AEGIS
            "aegis256",
#else
            "aes256gcm",
#endif
            "zstd3"};
        require(sqlite_murmur_open_v2(path.c_str(),&a,SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE,&options)==SQLITE_OK,"integrated open");
        require(value(a,"SELECT length(crsql_site_id())")=="16","built-in CRR missing");
        require(value(a,"SELECT murmur_status()") == "{\"running\":false}","replication unexpectedly started");
        require(value(a,"PRAGMA murmur_writer_local_share")=="90","writer_local_share default");
        require(value(a,"PRAGMA murmur_writer_remote_share")=="10","writer_remote_share default");
        require(value(a,"PRAGMA murmur_writer_max_debt_ms")=="1000","writer_max_debt_ms default");
        require(value(a,"PRAGMA murmur_writer_maintenance_reserve_every")=="100","writer_maintenance_reserve_every default");
        {std::string h=value(a,"SELECT murmur_health()");
        require(contains(h,"\"version\":1"),"health version");
        require(contains(h,"\"scope\":\"remote_and_maintenance\""),"health scope");
        require(contains(h,"\"local_admission_enforced\":false"),"health local scope");
        require(contains(h,"\"effective_config\":{\"local_share\":90"),"health effective default");
        require(contains(h,"\"pending_config\":{\"local_share\":90"),"health pending default");
        require(contains(h,"\"durable_cursor\":null"),"health durable N/A");
        require(contains(h,"\"worker_payload_bytes\":null"),"health payload N/A");}
        exec(a,"CREATE TABLE records(id INTEGER PRIMARY KEY NOT NULL,body DEFAULT ''); SELECT crsql_as_crr('main','records'); INSERT INTO records VALUES(1,'first connection')");
        exec(a,"PRAGMA murmur_poll_ms=123");
        exec(a,"PRAGMA murmur_writer_local_share=80");
        require(value(a,"PRAGMA murmur_writer_local_share")=="80","writer_local_share read-back");
        require(value(a,"PRAGMA murmur_writer_remote_share")=="20","writer_remote_share derived");
        fails(a,"PRAGMA murmur_writer_remote_share=30","derived share accepted a write");
        fails(a,"PRAGMA murmur_writer_local_share=0","local share 0 accepted");
        fails(a,"PRAGMA murmur_writer_local_share=100","local share 100 accepted");
        fails(a,"PRAGMA murmur_writer_max_debt_ms=0","debt 0 accepted");
        fails(a,"PRAGMA murmur_writer_maintenance_reserve_every=0","reserve 0 accepted");
        {std::string h=value(a,"SELECT murmur_health()");
        require(contains(h,"\"effective_config\":{\"local_share\":90"),"scheduler config applied live");
        require(contains(h,"\"pending_config\":{\"local_share\":80"),"pending config not persisted");}
        require(value(a,"PRAGMA murmur_retention_min_kept")=="1000","retention_min_kept default");
        require(value(a,"PRAGMA murmur_retention_max_offline_ms")=="604800000","retention_max_offline_ms default");
        exec(a,"PRAGMA murmur_retention_min_kept=10");
        require(value(a,"PRAGMA murmur_retention_min_kept")=="10","retention_min_kept read-back");
        fails(a,"PRAGMA murmur_retention_min_kept=0","min_kept 0 accepted");
        fails(a,"PRAGMA murmur_retention_max_offline_ms=1","max_offline 1 accepted");
        require(value(a,"SELECT murmur_retire_peer('00000000000000000000000000000003')")=="1","retire peer");
        require(value(a,"SELECT count(*) FROM murmur_excluded")=="1","retire did not exclude");
        {std::string h=value(a,"SELECT murmur_health()");
        require(contains(h,"\"retention\":null"),"retention status without identity");}
        exec(a,"SELECT murmur_refresh_indexes()");
        {std::string h=value(a,"SELECT murmur_health()");
        require(stat_count(h,"maintenance")>=1,"maintenance admission not counted");
        require(stat_count(h,"remote")==0,"unexpected remote admission");
        require(stat_count(h,"local")==0,"unexpected local admission");}
        require(sqlite_murmur_open(path.c_str(),&b)==SQLITE_OK,"second integrated connection");
        require(value(b,"PRAGMA murmur_poll_ms")=="123","shared settings");
        exec(b,"INSERT INTO records VALUES(2,'second connection')");
        require(value(a,"SELECT count(*) FROM records")=="2","second connection CRR initialization");
        require(value(a,"PRAGMA murmur_kdf")=="direct","kdf default");
        require(value(a,"PRAGMA murmur_kdf_time")=="3","kdf_time default");
        require(value(a,"PRAGMA murmur_kdf_memory_kib")=="65536","kdf_memory_kib default");
        require(value(a,"PRAGMA murmur_kdf_threads")=="4","kdf_threads default");
        require(contains(value(a,"SELECT murmur_key_inventory()"),"\"sealed\":false"),"inventory pre-seal");
        fails(a,"PRAGMA murmur_kdf=scrypt","bad kdf accepted");
        fails(a,"PRAGMA murmur_kdf_time=0","kdf_time 0 accepted");
        fails(a,"PRAGMA murmur_kdf_time=33","kdf_time 33 accepted");
        fails(a,"PRAGMA murmur_kdf_memory_kib=1023","kdf_memory_kib 1023 accepted");
        fails(a,"PRAGMA murmur_kdf_threads=0","kdf_threads 0 accepted");
        fails_with(a,"PRAGMA murmur_kdf='argon2id'","Argon2","argon2 without unit not loud");
        require(value(a,"PRAGMA murmur_kdf")=="direct","failed seal left staged trace");
        exec(a,"BEGIN");
        fails(a,"PRAGMA murmur_kdf_time=9","in-transaction KDF write accepted");
        exec(a,"ROLLBACK");
        exec(a,"PRAGMA murmur_kdf_time=5");
        require(value(a,"PRAGMA murmur_kdf_time")=="5","kdf_time staged");
        exec(a,"PRAGMA murmur_kdf='direct'");
        require(value(a,"PRAGMA murmur_kdf")=="direct","kdf sealed read-back");
        require(value(a,"PRAGMA murmur_kdf_time")=="0","direct seal must record zero cost params");
        fails_with(a,"PRAGMA murmur_kdf_time=7","write-once","sealed param accepted a write");
        fails_with(a,"PRAGMA murmur_kdf='direct'","write-once","reseal accepted");
        {std::string inv=value(a,"SELECT murmur_key_inventory()");
        require(contains(inv,"\"sealed\":true"),"inventory sealed");
        require(contains(inv,"\"seq\":1"),"inventory seq");
        require(contains(inv,"\"current\":1"),"inventory current");
        require(contains(inv,"\"kdf\":\"direct\""),"inventory kdf");
        require(contains(inv,"\"active\":true"),"inventory active");}
        exec(a,"INSERT INTO records VALUES(3,'post-seal write')");
        require(value(a,"SELECT count(*) FROM records")=="3","post-seal write");
        require(value(a,"SELECT count(*) FROM murmur_config WHERE name LIKE 'kdf%'")=="0","KDF leaked into murmur_config");
        require(value(a,"PRAGMA murmur_max_file_bytes")=="4294967296","max_file_bytes default");
        require(value(a,"PRAGMA murmur_fetch_bind")=="","fetch_bind default disabled");
        require(value(a,"PRAGMA murmur_bridge_role")=="","bridge_role default");
        exec(a,"PRAGMA murmur_fetch_concurrency=4");
        require(value(a,"PRAGMA murmur_fetch_concurrency")=="4","fetch_concurrency read-back");
        fails(a,"PRAGMA murmur_fetch_concurrency=65","fetch_concurrency 65 accepted");
        fails(a,"PRAGMA murmur_bridge_role=sideways","bad bridge_role accepted");
        exec(a,"PRAGMA murmur_files_dir='/tmp/legacy'");
        require(value(a,"PRAGMA murmur_files_dir")=="/tmp/legacy","files_dir inert read-back");
        fails(a,"PRAGMA murmur_bridge_staging_bytes=4096","staging under bundle accepted");
        require(value(a,"SELECT murmur_file_fetch()")=="0","fetch pre-split");
        require(value(a,"SELECT murmur_file_last_error()")=="","last_error initial");
        fails_with(a,"SELECT murmur_bridge_process()","not configured","bridge unexpectedly configured");
        {std::ofstream f((root/"stage.txt").string());f<<"hello files";}
        require(contains(value(a,("SELECT murmur_file_upload('doc1','"+(root/"stage.txt").string()+"')").c_str()),"\"doc1\""),"upload metadata");
        require(value(a,("SELECT murmur_file_download('doc1','"+(root/"out.txt").string()+"')").c_str())=="11","download bytes");
        {std::ifstream f((root/"out.txt").string());std::string body;std::getline(f,body);require(body=="hello files","download content");}
        require(contains(value(a,"SELECT murmur_file_list('')"),"doc1"),"list");
        require(contains(value(a,"SELECT murmur_file_metadata('doc1')"),"\"bytes\":11"),"metadata");
        require(value(a,"SELECT murmur_file_availability('doc1')")=="local","availability");
        require(value(a,"SELECT murmur_file_read('doc1',0,11)")=="hello files","read");
        fails(a,"SELECT murmur_file_read('doc1',0,0)","zero-limit read accepted");
        require(contains(value(a,"SELECT murmur_file_rotation_status()"),"current_generation"),"rotation status");
        require(value(a,"SELECT murmur_file_collect(0)")=="0","collect empty");
        require(value(a,"SELECT murmur_file_rotate(10)")=="0","rotate empty");
        exec(a,"BEGIN");
        fails(a,("SELECT murmur_file_upload('txn','"+(root/"stage.txt").string()+"')").c_str(),"in-transaction upload accepted");
        exec(a,"ROLLBACK");
        exec(a,"PRAGMA murmur_max_file_bytes=1048576");
        {std::ofstream f((root/"big.bin").string(),std::ios::binary);f<<std::string(2*1024*1024,'B');}
        require(contains(value(a,("SELECT murmur_file_upload('big1','"+(root/"big.bin").string()+"')").c_str()),"big1"),"pending cap not applied");
        require(sqlite_murmur_close(a)==SQLITE_OK,"close first");
        require(contains(value(b,"SELECT murmur_status()"),"\"running\":false"),"shared service lost");
        require(sqlite_murmur_close(b)==SQLITE_OK,"close final");
        require(sqlite_murmur_open(path.c_str(),&a)==SQLITE_OK,"reopen");
        require(value(a,"SELECT count(*) FROM records")=="3","persisted rows");
        require(value(a,"SELECT body FROM records WHERE id=1")=="first connection","pre-seal row readable post-reopen");
        require(contains(value(a,"SELECT murmur_key_inventory()"),"\"sealed\":true"),"envelope survived reopen");
        // D2b: rotate/prune UDFs. Handle c opens pre-rotate so its
        // snapshot is stale; its post-rotate write (keyed id 1) must
        // still land and stay readable everywhere (forward compat).
        sqlite3 *c=nullptr;
        require(sqlite_murmur_open(path.c_str(),&c)==SQLITE_OK,"race open");
        require(value(a,"SELECT murmur_rotate_key()")=="2","rotate did not return id 2");
        exec(a,"CREATE TABLE r2(id PRIMARY KEY, body); INSERT INTO r2 VALUES(1,'rotated-write')");
        require(value(a,"SELECT body FROM r2 WHERE id=1")=="rotated-write","rotated write unreadable");
        require(contains(value(a,"SELECT murmur_key_inventory()"),"\"seq\":2"),"inventory seq not bumped");
        require(contains(value(a,"SELECT murmur_key_inventory()"),"\"current\":2"),"inventory current not bumped");
        {std::string p=value(a,"SELECT murmur_prune_keys()");
        require(contains(p,"\"pruned\":[]"),"prune dropped live keys");
        require(contains(p,"\"retained\":[1,2]"),"prune retained wrong");
        require(!contains(p,"\"blocked_by_unkeyed\":0"),"prune missed pre-seal pages");}
        exec(a,"BEGIN");
        fails(a,"SELECT murmur_rotate_key()","rotate in txn not rejected");
        fails(a,"SELECT murmur_prune_keys()","prune in txn not rejected");
        exec(a,"ROLLBACK");
        exec(c,"INSERT INTO r2 VALUES(2,'stale-snapshot-write')");
        require(value(c,"SELECT body FROM r2 WHERE id=2")=="stale-snapshot-write","stale write unreadable on writer");
        require(value(a,"SELECT body FROM r2 WHERE id=2")=="stale-snapshot-write","stale write lost after rotate");
        require(sqlite_murmur_close(c)==SQLITE_OK,"race close");
        require(value(a,"PRAGMA murmur_kdf_time")=="0","sealed header read-back post-reopen");
        require(value(a,"PRAGMA murmur_fetch_concurrency")=="4","file pragma persisted");
        require(contains(value(a,"SELECT murmur_file_list('')"),"doc1"),"files persisted");
        require(value(a,("SELECT murmur_file_download('doc1','"+(root/"out2.txt").string()+"')").c_str())=="11","download post-reopen");
        fails(a,("SELECT murmur_file_upload('big2','"+(root/"big.bin").string()+"')").c_str(),"restart-applied cap not enforced");
        {std::string h=value(a,"SELECT murmur_health()");
        require(contains(h,"\"effective_config\":{\"local_share\":80"),"reopen did not apply pending config");
        require(contains(h,"\"pending_config\":{\"local_share\":80"),"pending config lost");}
        require(sqlite_murmur_close(a)==SQLITE_OK,"close reopened");
        auto storage=(root/"storage-only").string();
        options={SQLITE_MURMUR_OPEN_STORAGE_ONLY,nullptr,nullptr};
        require(sqlite_murmur_open_v2(storage.c_str(),&a,SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE,&options)==SQLITE_OK,"storage open");
        require(value(a,"SELECT count(*) FROM sqlite_schema WHERE name GLOB 'crsql*' OR name GLOB 'murmur_*'")=="0","storage-only created replication metadata");
        exec(a,"CREATE TABLE local(id PRIMARY KEY); INSERT INTO local VALUES(1)");
        require(sqlite_murmur_close(a)==SQLITE_OK,"storage close");
        require(sqlite_murmur_open_v2(storage.c_str(),&a,SQLITE_OPEN_READONLY,nullptr)==SQLITE_OK,"read-only open");
        require(value(a,"SELECT count(*) FROM local")=="1","read-only data");
        require(sqlite_murmur_close(a)==SQLITE_OK,"read-only close");
        // D2b: unsealed rotation guard. Placed last: a fresh-path full
        // open resets process-global pending accounting the earlier
        // cap asserts depend on.
        {const std::string upath=(root/"unsealed-rot").string();
        sqlite3 *u=nullptr;
        require(sqlite_murmur_open(upath.c_str(),&u)==SQLITE_OK,"unsealed open");
        fails(u,"SELECT murmur_rotate_key()","rotate on unsealed store not rejected");
        require(sqlite_murmur_close(u)==SQLITE_OK,"unsealed close");}
        require(sqlite_murmur_unregister()==SQLITE_OK,"unregister left live workers");
        std::filesystem::remove_all(root);return 0;
    }catch(const std::exception &e){std::cerr<<e.what()<<'\n';return 1;}
}
