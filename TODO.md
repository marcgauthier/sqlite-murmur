

sqlite-murmur (encrypt/compress/rockdb) + cr-sqlite + murmur-replication (QUIC/SWIM)
    1x at 183,693/s  and 100x at 372,226/s


after the tests-live works 



push to github.com before moving to rust.

sqlite-murmur codebase should be rust not c/cpp, please migrate the c/cpp code src  to rust.  This does not include third-party, third-party should stay as c, cpp or rust and tests-live should remain python, c, cpp, go or rust.

compare benchmark is good enough keep rust!


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