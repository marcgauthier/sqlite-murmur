/* Consumer application: one public header, one CMake target, standard SQLite SQL.
 * Supply a 64-digit hexadecimal encryption key as argv[2]. No extension files. */
#include <sqlite_murmur.h>
#include <stdio.h>
#include <string.h>

static int digit(char c) {
    if (c >= '0' && c <= '9') return c-'0';
    if (c >= 'a' && c <= 'f') return c-'a'+10;
    if (c >= 'A' && c <= 'F') return c-'A'+10;
    return -1;
}
int main(int argc, char **argv) {
    unsigned char key[32];
    if (argc != 3 || strlen(argv[2]) != 64) {
        fprintf(stderr, "usage: %s <database-directory> <64-hex-digit-key>\n", argv[0]);
        return 2;
    }
    for (int i=0;i<32;++i) {
        int a=digit(argv[2][2*i]), b=digit(argv[2][2*i+1]);
        if (a<0 || b<0) return 2;
        key[i]=(unsigned char)((a<<4)|b);
    }
    sqlite_murmur_config config={key,250,16384,1};
    int rc=sqlite_murmur_register(&config);
    memset(key,0,sizeof(key));
    if (rc!=SQLITE_OK) return 1;
    sqlite3 *db=NULL;
    rc=sqlite_murmur_open(argv[1],&db);
    if (rc==SQLITE_OK)
        rc=sqlite3_exec(db,"CREATE TABLE IF NOT EXISTS example(id INTEGER PRIMARY KEY, value TEXT);"
                           "INSERT OR REPLACE INTO example VALUES(1,'encrypted and compressed');",
                        NULL,NULL,NULL);
    if (rc!=SQLITE_OK) fprintf(stderr,"%s\n",db?sqlite3_errmsg(db):sqlite3_errstr(rc));
    if (sqlite_murmur_close(db)!=SQLITE_OK) rc=SQLITE_ERROR;
    if (sqlite_murmur_unregister()!=SQLITE_OK) rc=SQLITE_ERROR;
    return rc==SQLITE_OK?0:1;
}
