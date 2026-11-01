// Local mixed-workload benchmark for the official SQLite HC-tree branch.
// HCTree native replication is deliberately disabled: Murmur owns replication.
#include <sqlite3.h>
#include "hctree_format.h"
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>
using Clock=std::chrono::steady_clock;
constexpr unsigned long long SEED=50000;
const std::string BODY="sqlite-murmur-benchmark-payload-abcdefghijklmnopqrstuvwxyz-0123456789";
const std::string DDL="CREATE TABLE records(id INTEGER PRIMARY KEY NOT NULL, body TEXT NOT NULL DEFAULT '')";
void ck(sqlite3*d,int r,const char*s){if(r!=SQLITE_OK)throw std::runtime_error(std::string(s)+": "+sqlite3_errmsg(d));}
void sql(sqlite3*d,const std::string&s){char*e=nullptr;int r=sqlite3_exec(d,s.c_str(),nullptr,nullptr,&e);if(r!=SQLITE_OK){std::string m=e?e:sqlite3_errmsg(d);sqlite3_free(e);throw std::runtime_error(s.substr(0,60)+": "+m);}}
struct Statement{sqlite3*d;sqlite3_stmt*s=nullptr;
 Statement(sqlite3*db,const char*q):d(db){ck(d,sqlite3_prepare_v2(d,q,-1,&s,nullptr),"prepare");}
 ~Statement(){sqlite3_finalize(s);}
 void reset(){ck(d,sqlite3_reset(s),"reset");ck(d,sqlite3_clear_bindings(s),"clear");}
 void insert(long long id){reset();ck(d,sqlite3_bind_int64(s,1,id),"bind id");ck(d,sqlite3_bind_text(s,2,BODY.c_str(),int(BODY.size()),SQLITE_STATIC),"bind body");if(sqlite3_step(s)!=SQLITE_DONE)throw std::runtime_error(sqlite3_errmsg(d));}
 void read(long long id){reset();ck(d,sqlite3_bind_int64(s,1,id),"bind read");if(sqlite3_step(s)!=SQLITE_ROW)throw std::runtime_error(sqlite3_errmsg(d));const char*p=(const char*)sqlite3_column_text(s,0);if(!p||sqlite3_column_bytes(s,0)!=int(BODY.size())||memcmp(p,BODY.data(),BODY.size()))throw std::runtime_error("read mismatch");reset();}
};
long long scalar(sqlite3*d,const char*q){Statement s(d,q);if(sqlite3_step(s.s)!=SQLITE_ROW)throw std::runtime_error(sqlite3_errmsg(d));return sqlite3_column_int64(s.s,0);}
unsigned long long randomid(unsigned long long&x,unsigned long long n){x^=x>>12;x^=x<<25;x^=x>>27;return 1+(x*2685821657736338717ULL)%n;}
int main(int argc,char**argv){sqlite3*d=nullptr;try{
 std::string path;unsigned batch=1;double duration=60;
 for(int i=1;i<argc;++i){std::string a=argv[i];
  if(a.rfind("--database=",0)==0)path=a.substr(11);
  else if(a.rfind("--batch=",0)==0)batch=unsigned(std::stoul(a.substr(8)));
  else if(a.rfind("--seconds=",0)==0)duration=std::stod(a.substr(10));
  else if(a=="--replication=off"){} // Only storage-only HCTree is supported.
  else if(a=="--replication=on")throw std::runtime_error("HCTree native replication disabled; use Murmur for replication");
  else throw std::runtime_error("bad argument "+a);
 }
 if(path.empty()||(batch!=1&&batch!=100)||!(duration>0&&duration<=3600))throw std::runtime_error("invalid options");
 auto dbpath=std::filesystem::absolute(path);
 if(std::filesystem::exists(dbpath)||dbpath.string().find('?')!=std::string::npos)throw std::runtime_error("database already exists or invalid path");
 std::string uri="file:"+dbpath.string()+"?hctree=1";
 ck(d,sqlite3_open_v2(uri.c_str(),&d,SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_URI|SQLITE_OPEN_FULLMUTEX,nullptr),"open");
 sql(d,"PRAGMA page_size=4096");sql(d,"PRAGMA cache_size=-1048576");sql(d,"PRAGMA mmap_size=0");
 sql(d,DDL);
 {
  Statement ins(d,"INSERT INTO records(id,body) VALUES(?1,?2)");
  for(unsigned long long first=1;first<=SEED;first+=1000){
   sql(d,"BEGIN");
   for(auto id=first;id<first+1000&&id<=SEED;++id){ins.insert(id);}
   sql(d,"COMMIT");
  }
 }
 {std::ifstream f(dbpath,std::ios::binary);unsigned char header[hctree_format::kHeaderBytes]{};f.read(reinterpret_cast<char*>(header),sizeof(header));if(f.gcount()!=static_cast<std::streamsize>(sizeof(header)))throw std::runtime_error("HC-tree database header too short");if(!hctree_format::valid_header(header,sizeof(header),4096))throw std::runtime_error("not the expected HC-tree database header/page size");}
 if(scalar(d,"SELECT count(*) FROM records")!=static_cast<long long>(SEED))throw std::runtime_error("seed count wrong");
 unsigned long long writes=0,transactions=0,rng=1234*0x9e3779b97f4a7c15ULL;
 double txDuration=0,elapsed=0;
 {
 Statement ins(d,"INSERT INTO records(id,body) VALUES(?1,?2)");
 Statement sel(d,"SELECT body FROM records WHERE id=?1");
 auto start=Clock::now();
 do{
  auto txstart=Clock::now();if(batch>1)sql(d,"BEGIN");
  for(unsigned i=0;i<batch;++i){
   auto id=SEED+(++writes);ins.insert(id);
   // In the 1x case commit and time the insert *before* the four reads,
   // identical to the sqlite-murmur benchmark's transaction boundary.
   if(batch==1){
    txDuration+=std::chrono::duration<double>(Clock::now()-txstart).count();
   }
   for(int k=0;k<4;++k)sel.read(randomid(rng,SEED+writes));
  }
  if(batch>1)sql(d,"COMMIT");
  if(batch>1)txDuration+=std::chrono::duration<double>(Clock::now()-txstart).count();
  ++transactions;
 }while(std::chrono::duration<double>(Clock::now()-start).count()<duration);
 elapsed=std::chrono::duration<double>(Clock::now()-start).count();
 }
 if(scalar(d,"SELECT count(*) FROM records")!=static_cast<long long>(SEED+writes))throw std::runtime_error("row count wrong");
 auto journalRows=0LL;
 auto closing=Clock::now();ck(d,sqlite3_close(d),"close");d=nullptr;
 auto closeSeconds=std::chrono::duration<double>(Clock::now()-closing).count();
 ck(d,sqlite3_open_v2(uri.c_str(),&d,SQLITE_OPEN_READWRITE|SQLITE_OPEN_URI,nullptr),"reopen");
 if(scalar(d,"SELECT count(*) FROM records")!=static_cast<long long>(SEED+writes))throw std::runtime_error("reopen row count wrong");
 ck(d,sqlite3_close(d),"reclose");d=nullptr;
 unsigned long long diskbytes=0;
 for(const char*ext:{"","-pagemap","-journal","-wal","-shm"}){auto name=dbpath.string()+ext;if(std::filesystem::exists(name))diskbytes+=std::filesystem::file_size(name);}
 auto ops=5*writes;
 std::cout<<std::fixed<<std::setprecision(6)<<"backend,hctree\nreplication,"<<"off"
 <<"\nbatch_size,"<<batch<<"\nseconds,"<<elapsed<<"\nreads,"<<4*writes<<"\nwrites,"<<writes
 <<"\noperations,"<<ops<<"\ntransactions,"<<transactions<<"\nops_per_second,"<<ops/elapsed
 <<"\nwrite_ops_per_second,"<<writes/elapsed<<"\nmean_write_transaction_ms,"<<txDuration*1000/transactions
 <<"\nclose_seconds,"<<closeSeconds<<"\njournal_rows,"<<journalRows<<"\ndisk_bytes,"<<diskbytes
 <<"\nsource_id,"<<sqlite3_sourceid()<<"\nBENCH_DONE\n";return 0;
 }catch(const std::exception&e){if(d)sqlite3_close_v2(d);std::cerr<<"BENCH_ERROR,"<<e.what()<<"\n";return 1;}}
