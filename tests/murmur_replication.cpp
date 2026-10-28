// Stdio test driver only: applications use SQLite directly, never this protocol.
#include "sqlite_murmur.h"
#include "sqlite_murmur_crsqlite.h"
#include "sqlite_murmur_replication.h"
#include <cstdlib>
#include <atomic>
#include <map>
#include <mutex>
#include <iostream>
#include <stdexcept>
#include <string>

// Test-only VFS shim: fail persistence after a specific incoming row commits.
// This leaves production code free of fault injection controls.
namespace {
std::atomic<int> faultMode{0};
sqlite3 *inspection=nullptr;
std::mutex methodsMutex;
struct MethodEntry { const sqlite3_io_methods *original; sqlite3_io_methods wrapped; };
std::map<sqlite3_file *,MethodEntry> fileMethods;
int (*originalOpen)(sqlite3_vfs *,const char *,sqlite3_file *,int,int *)=nullptr;
int faultControl(sqlite3_file *file,int op,void *arg) {
    const sqlite3_io_methods *original;
    { std::lock_guard<std::mutex> lock(methodsMutex); original=fileMethods.at(file).original; }
    if(op==SQLITE_MURMUR_FCNTL_FLUSH && faultMode.load()) {
        bool fail=faultMode.load()==1;
        if(!fail && inspection) {
            sqlite3_stmt *stmt=nullptr;
            if(sqlite3_prepare_v2(inspection,"SELECT 1 FROM records WHERE id=7777",-1,&stmt,nullptr)==SQLITE_OK)
                fail=sqlite3_step(stmt)==SQLITE_ROW;
            sqlite3_finalize(stmt);
        }
        if(fail)return SQLITE_IOERR_WRITE;
    }
    return original->xFileControl(file,op,arg);
}
int faultClose(sqlite3_file *file) {
    const sqlite3_io_methods *original;
    { std::lock_guard<std::mutex> lock(methodsMutex); original=fileMethods.at(file).original;
      file->pMethods=original;fileMethods.erase(file); }
    return original->xClose(file);
}
int faultOpen(sqlite3_vfs *vfs,const char *path,sqlite3_file *file,int flags,int *out) {
    int rc=originalOpen(vfs,path,file,flags,out);
    if(rc==SQLITE_OK && (flags&SQLITE_OPEN_MAIN_DB)) {
        std::lock_guard<std::mutex> lock(methodsMutex);
        auto &entry=fileMethods[file];entry.original=file->pMethods;entry.wrapped=*file->pMethods;
        entry.wrapped.xFileControl=faultControl;entry.wrapped.xClose=faultClose;
        file->pMethods=&entry.wrapped;
    }
    return rc;
}
}

void check(int rc, sqlite3 *db, const char *where) {
    if (rc != SQLITE_OK) throw std::runtime_error(std::string(where)+": "+(db?sqlite3_errmsg(db):sqlite3_errstr(rc)));
}
std::string sql(sqlite3 *db,const std::string &text) {
    const char *tail=text.c_str();std::string output;
    while (*tail) {
        sqlite3_stmt *stmt=nullptr;
        check(sqlite3_prepare_v2(db,tail,-1,&stmt,&tail),db,"prepare");
        if (!stmt) continue;
        int rc;
        while ((rc=sqlite3_step(stmt))==SQLITE_ROW) {
            if(!output.empty()) output+='|';
            for(int i=0;i<sqlite3_column_count(stmt);++i){
                if(i)output+='~';auto *p=sqlite3_column_text(stmt,i);
                if(p)output.append(reinterpret_cast<const char *>(p),sqlite3_column_bytes(stmt,i));
            }
        }
        std::string error=sqlite3_errmsg(db);sqlite3_finalize(stmt);
        if(rc!=SQLITE_DONE)throw std::runtime_error(error);
    }
    return output;
}
void testFunctions(sqlite3 *db) {
    check(sqlite3_create_function_v2(db,"murmur_test_fault",1,SQLITE_UTF8|SQLITE_DIRECTONLY,nullptr,
      [](sqlite3_context *ctx,int,sqlite3_value **args){faultMode.store(sqlite3_value_int(args[0]));sqlite3_result_int(ctx,1);},nullptr,nullptr,nullptr),db,"test fault function");
    check(sqlite3_create_function_v2(db,"murmur_test_integrated",0,SQLITE_UTF8|SQLITE_DIRECTONLY,nullptr,
      [](sqlite3_context *ctx,int,sqlite3_value **){
#ifdef MURMUR_TEST_INTEGRATED
        sqlite3_result_int(ctx,1);
#else
        sqlite3_result_int(ctx,0);
#endif
      },nullptr,nullptr,nullptr),db,"test mode function");
}
int main(int argc,char **argv) {
    if(argc<2)return 2;
    sqlite3 *db=nullptr;
    try {
        unsigned char key[32]={42};sqlite_murmur_config cfg{key,60000,16384,1};
        check(sqlite_murmur_register(&cfg),nullptr,"register VFS");
        auto *vfs=sqlite3_vfs_find("sqlite-murmur");originalOpen=vfs->xOpen;vfs->xOpen=faultOpen;
#ifdef MURMUR_TEST_INTEGRATED
        check(sqlite_murmur_open(argv[1],&db),db,"integrated open");
#else
        check(sqlite3_open_v2(argv[1],&db,SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE,"sqlite-murmur"),db,"open");
#endif
        sqlite3_busy_timeout(db,2000);
#if defined(MURMUR_TEST_INTEGRATED)
        // SQL interfaces are built in: no extension init/load calls.
#elif defined(MURMUR_TEST_LOADABLE)
        if(argc!=4)throw std::runtime_error("requires murmur and crsqlite shared libraries");
        sqlite3_enable_load_extension(db,1);char *error=nullptr;
        int rc=sqlite3_load_extension(db,argv[3],"sqlite3_crsqlite_init",&error);
        std::string message=error?error:"";sqlite3_free(error);if(rc)throw std::runtime_error(message);
        rc=sqlite3_load_extension(db,argv[2],"sqlite3_murmurreplication_init",&error);
        message=error?error:"";sqlite3_free(error);if(rc)throw std::runtime_error(message);
        sqlite3_enable_load_extension(db,0);
        sql(db,"PRAGMA murmur_crsqlite_extension='"+std::string(argv[3])+"'");
#else
        char *error=nullptr;int rc=sqlite3_crsqlite_init(db,&error,nullptr);
        std::string message=error?error:"";sqlite3_free(error);if(rc)throw std::runtime_error(message);
        rc=sqlite3_murmurreplication_init(db,&error,nullptr);
        message=error?error:"";sqlite3_free(error);if(rc)throw std::runtime_error(message);
#endif
        check(sqlite3_open_v2(argv[1],&inspection,SQLITE_OPEN_READWRITE,"sqlite-murmur"),inspection,"open inspector");
        testFunctions(db);
        std::cout<<"READY"<<std::endl;
        std::string line;
        while(std::getline(std::cin,line)) {
            if(line=="EXIT")break;
#ifdef MURMUR_TEST_INTEGRATED
            if(line=="@REOPEN") {
                sqlite3 *next=nullptr;
                check(sqlite_murmur_open(argv[1],&next),next,"second application open");
                // Busy timeouts are application settings on each connection.
                sqlite3_busy_timeout(next,2000);
                check(sqlite_murmur_close(db),nullptr,"close original application connection");
                db=next;testFunctions(db);
                std::cout<<"OK:reopened"<<std::endl;
                continue;
            }
#endif
            try{std::string result=sql(db,line);std::cout<<"OK:"<<result<<std::endl;}
            catch(const std::exception &e){std::cout<<"ERR:"<<e.what()<<std::endl;}
        }
        faultMode.store(0);
        sql(db,"SELECT murmur_stop()");
#ifdef MURMUR_TEST_INTEGRATED
        check(sqlite_murmur_close(db),nullptr,"integrated close");db=nullptr;
#else
        sql(db,"SELECT crsql_finalize()");
        check(sqlite3_close(db),nullptr,"close");db=nullptr;
#endif
        check(sqlite3_close(inspection),nullptr,"close inspector");inspection=nullptr;
        check(sqlite_murmur_unregister(),nullptr,"unregister");
        return 0;
    }catch(const std::exception &e){std::cerr<<e.what()<<std::endl;if(db)sqlite3_close(db);return 1;}
}
