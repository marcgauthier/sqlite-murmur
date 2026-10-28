#include "sqlite_murmur.h"
#include <filesystem>
#include <stdexcept>
#include <string>
#include <chrono>
#include <iostream>

void require(bool condition,const char *message){if(!condition)throw std::runtime_error(message);}
void exec(sqlite3 *db,const char *sql){char *error=nullptr;int rc=sqlite3_exec(db,sql,nullptr,nullptr,&error);std::string message=error?error:"SQL failed";sqlite3_free(error);if(rc!=SQLITE_OK)throw std::runtime_error(message);}
std::string value(sqlite3 *db,const char *sql){sqlite3_stmt *s=nullptr;require(sqlite3_prepare_v2(db,sql,-1,&s,nullptr)==SQLITE_OK,sqlite3_errmsg(db));require(sqlite3_step(s)==SQLITE_ROW,sqlite3_errmsg(db));auto p=sqlite3_column_text(s,0);std::string result=p?reinterpret_cast<const char*>(p):"";sqlite3_finalize(s);return result;}
int main(){
    auto root=std::filesystem::temp_directory_path()/std::filesystem::path("sqlite-murmur-application-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    try{
        std::filesystem::create_directories(root);
        unsigned char key[32]={42};sqlite_murmur_config cfg{key,100,16384,1};
        require(sqlite_murmur_register(&cfg)==SQLITE_OK,"register");
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
        exec(a,"CREATE TABLE records(id INTEGER PRIMARY KEY NOT NULL,body DEFAULT ''); SELECT crsql_as_crr('main','records'); INSERT INTO records VALUES(1,'first connection')");
        exec(a,"PRAGMA murmur_poll_ms=123");
        require(sqlite_murmur_open(path.c_str(),&b)==SQLITE_OK,"second integrated connection");
        require(value(b,"PRAGMA murmur_poll_ms")=="123","shared settings");
        exec(b,"INSERT INTO records VALUES(2,'second connection')");
        require(value(a,"SELECT count(*) FROM records")=="2","second connection CRR initialization");
        require(sqlite_murmur_close(a)==SQLITE_OK,"close first");
        require(value(b,"SELECT murmur_status()") == "{\"running\":false}","shared service lost");
        require(sqlite_murmur_close(b)==SQLITE_OK,"close final");
        require(sqlite_murmur_open(path.c_str(),&a)==SQLITE_OK,"reopen");
        require(value(a,"SELECT count(*) FROM records")=="2","persisted rows");
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
        require(sqlite_murmur_unregister()==SQLITE_OK,"unregister left live workers");
        std::filesystem::remove_all(root);return 0;
    }catch(const std::exception &e){std::cerr<<e.what()<<'\n';return 1;}
}
