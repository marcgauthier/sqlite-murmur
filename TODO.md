

sqlite-murmur codebase should be rust not c/cpp, please migrate the c/cpp code src  to rust.  This does not include third-party, third-party should stay as c, cpp or rust and tests-live should remain python, c, cpp, go or rust.

compare benchmark is good enough keep rust!

NO CR-SQLITE REPLICATION
| Backend | Inserts/tx | Ops/sec | Ops/sec incl. persistence | Mean tx (ms) | Logical size (MB) | Disk size (MB) |
|---|---:|---:|---:|---:|---:|---:|
| Murmur | 1 | 351,934 | 351,044 | 0.0064 | 344.1 | 37.6 |
| SQLite disk | 1 | 114,294 | 114,268 | 0.0267 | 114.4 | 114.4 |
| SQLite memory | 1 | 819,298 | 819,298 | 0.0013 | 795.8 | 0 |
| Murmur | 100 | 855,545 | 853,729 | 0.5844 | 830.8 | 90.7 |
| SQLite disk | 100 | 669,723 | 669,715 | 0.7465 | 652.2 | 652.2 |
| SQLite memory | 100 | 1,001,415 | 1,001,415 | 0.4992 | 971.8 | 0 |


WITH CR-SQLITE REPLICATION
| Backend | Inserts/tx | Ops/sec | Ops/sec incl. persistence | Mean tx (ms) | Logical size (MB) | Disk size (MB) |
|---|---:|---:|---:|---:|---:|---:|
| Murmur | 1 | 195,672 | 195,353 | 0.0184 | 356.6 | 80.0 |
| SQLite disk | 1 | 29,397 | 29,397 | 0.1523 | 59.5 | 59.5 |
| SQLite memory | 1 | 349,607 | 349,607 | 0.0094 | 633.4 | 0 |
| Murmur | 100 | 424,931 | 419,664 | 1.1766 | 767.0 | 160.8 |
| SQLite disk | 100 | 276,462 | 276,430 | 1.8085 | 498.0 | 498.0 |
| SQLite memory | 100 | 357,487 | 357,487 | 1.3986 | 644.8 | 0 |




need a good ./USAGE.md that show how to use the PRAGMA for all the features: encryption, replication, compression, etc.


reduce external dependencies that are not supported by corporate i.e.:
    replaced quinn rust crate with Cloudflare quiche

external create:
aes-gcm   
aws-lc-rs
bytes
chacha20poly1305
hkdf
ipnet
num-derive
num-traits
pem-rfc7468
postcard
quinn
rand
reqwest
rusqlite
rustls
rustls-pemfile
serde
serde_json
sha2
sqlite-vec
thiserror
tokio
uuid
x509-cert
zeroize
zstd


Create a golang driver internal base on crawshaw.
