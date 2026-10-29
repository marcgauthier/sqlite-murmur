# Vendors: direct dependencies

Every external dependency this repository builds against, grouped by
who maintains it. **Direct dependencies only**: crates named in a
workspace member's `Cargo.toml`, and the `third-party/` libraries
linked by CMake. Transitive crates (visible via
`cargo tree --offline`) are intentionally excluded, as are the
first-party `murmur-*` crates and the locally maintained `ipnet` compatibility
subset (extracted upstream code; see notes below).

Category reflects **repository ownership** (the org or person holding
the upstream repo), not funding — several community projects below
receive corporate contributions. Versions are the pinned versions in
`crates/Cargo.lock` / `third-party/sources.json`; everything is
vendored, and all builds run offline.

## C and C++ external libraries

These include C/C++ engines packaged through Cargo. A Cargo wrapper listed
below refers to the same dependency, not a second copy of the engine.

| Library | Version | Language | Upstream / owner | Used for |
| --- | --- | --- | --- | --- |
| SQLite | 3.54.1 | C | sqlite.org (public domain) | SQL engine (vendored amalgamation) |
| RocksDB | 8.9.1 | C++ | [facebook/rocksdb](https://github.com/facebook/rocksdb) (Meta) | LSM storage engine |
| zstd | 1.5.7 | C | [facebook/zstd](https://github.com/facebook/zstd) (Meta) | page and transport compression |
| libaegis | 0.9.1 | C | [aegis-aead/libaegis](https://github.com/aegis-aead/libaegis) (independent) | AEGIS-256 cipher (optional build) |
| cr-sqlite | 2-43ab94ca | C + Rust | [vlcn-io/cr-sqlite](https://github.com/vlcn-io/cr-sqlite) (independent) | CRDT merge layer |
| sqlite-vec | 0.1.6 | C + Rust wrapper | [asg017/sqlite-vec](https://github.com/asg017/sqlite-vec) (independent) | C vector-search engine, compiled by the Cargo package; optional `vector` feature, on by default |
| AWS-LC | bundled by `aws-lc-rs` 1.18.2 / `aws-lc-sys` 0.46.0 | C + assembly, Rust wrappers | [aws/aws-lc](https://github.com/aws/aws-lc) (Amazon) | native crypto/TLS engine behind the direct `aws-lc-rs` package listed below; versions here identify its Cargo packaging |

## Rust libraries supported by the core team

Crates owned by the `rust-lang` organization.

| Crate | Version | Used by |
| --- | --- | --- |
| `libc` | 0.2.191 | murmur-storage (OS/syscall bindings) |

## Rust libraries supported by a corporation

Crates whose upstream repo is owned by a company organization.

| Crate | Version | Owner | Used by |
| --- | --- | --- | --- |
| `aws-lc-rs` | 1.18.2 | [aws/aws-lc-rs](https://github.com/aws/aws-lc-rs) (Amazon) | murmur-storage, murmur-files, murmur-query, murmur-replication, murmur-tx (AES-GCM, HKDF, SHA-256, TLS, CSPRNG, UUID-v5 hashing, origin signing and key agreement) |

## Rust libraries by independent authors or community teams

This section lists direct Cargo packages, including wrappers over C engines.

| Crate | Version | Maintainer (repo) | Used by |
| --- | --- | --- | --- |
| `chacha20poly1305` | 0.10.1 | [RustCrypto](https://github.com/RustCrypto/AEADs) | murmur-tx |
| `x509-cert` | 0.2.5 | [RustCrypto](https://github.com/RustCrypto/formats) | murmur-replication, murmur-tx |
| `zeroize` | 1.9.1 | [RustCrypto](https://github.com/RustCrypto/utils) | murmur-storage, murmur-files, murmur-replication, murmur-tx |
| `quinn` | 0.11.12 | [quinn-rs](https://github.com/quinn-rs/quinn) (community) | murmur-replication, murmur-tx (QUIC transport) |
| `rcgen` | 0.13.2 | [rustls](https://github.com/rustls/rcgen) (community) | murmur-replication, murmur-tx (tests only) |
| `reqwest` | 0.12.28 | [seanmonstar](https://github.com/seanmonstar/reqwest) (independent) | murmur-tx (HTTP fetch) |
| `rusqlite` | 0.40.2 | [rusqlite](https://github.com/rusqlite/rusqlite) (community) | murmur-query, murmur-replication |
| `rustls` | 0.23.46 | [rustls](https://github.com/rustls/rustls) (community) | murmur-replication, murmur-tx |
| `serde` | 1.0.229 | [serde-rs](https://github.com/serde-rs/serde) (independent) | murmur-replication, murmur-tx, murmur-query, murmur-policy, murmur-sched |
| `serde_json` | 1.0.154 | [serde-rs](https://github.com/serde-rs/json) (independent) | murmur-replication, murmur-query, murmur-files |
| `postcard` | 1.1.3 | [jamesmunns](https://github.com/jamesmunns/postcard) (independent) | murmur-replication, murmur-tx, murmur-query, murmur-policy |
| `thiserror` | 2.0.21 | [dtolnay](https://github.com/dtolnay/thiserror) (independent) | murmur-replication, murmur-tx, murmur-query, murmur-policy, murmur-sched |
| `tokio` | 1.54.1 | [tokio-rs](https://github.com/tokio-rs/tokio) (community) | murmur-replication, murmur-tx, murmur-files |
| `tempfile` | 3.27.0 | [Stebalien](https://github.com/Stebalien/tempfile) (independent) | tests only (all suites) |
| `sqlite-vec` | 0.1.6 | [asg017](https://github.com/asg017/sqlite-vec) (independent) | murmur-query (Rust registration wrapper for the C engine listed above; optional `vector` feature, on by default) |

## Notes

- `murmur-storage-api`, `murmur-admin`, and `murmur-admin-bin` have
  **zero** external dependencies (hand-written FFI only).
- `tempfile` and `rcgen` are `[dev-dependencies]` — they ship in no
  library or binary.
- No workspace member uses build-script dependencies
  (`murmur-storage/build.rs` and `murmur-tx/build.rs` are dependency-free).
- `cr-sqlite` is a hybrid: its merge core is Rust with a C API; it
  lives in `third-party/` and links like the C libraries.
- To re-check this list: `cargo tree --offline --workspace --depth 1`
  for Rust, `third-party/sources.json` for C/C++.

## Internal adapters and remaining transitive dependencies

- Active workspace crates use AWS-LC 1.18.2 for AES-256-GCM, HKDF-SHA256,
  and SHA-256, with shared source adapters in `crates/common/`. Existing
  encrypted records and protocol formats are preserved.
- Storage and transport share internal bindings to the vendored Meta zstd
  1.5.7 C archive. The Rust `zstd`, `zstd-safe`, and `zstd-sys` crates are
  no longer needed by the active workspace.
- PEM decoding uses `rustls::pki_types::pem::PemObject`. The redundant
  `rustls-pemfile` dependency is removed. `pem-rfc7468` remains transitive
  through `x509-cert` / `der`; `zeroize` also remains an AWS-LC dependency.
- `aes-gcm`, `hkdf`, and `sha2` are absent from the active workspace closure.
  XChaCha20-Poly1305 bridge encryption remains in use. TLS and test certificate
  generation now use AWS-LC; Ring is absent from the native build closure.
  Quinn still declares Ring unconditionally for `wasm32-unknown-unknown`, so
  Ring remains in the cross-platform lockfile and vendor tree. Check this with
  `cargo tree --offline --target all -i ring` from `crates/`.
- `crates/murmur-crdt` is a standalone reference workspace, not an active
  application dependency. It retains its original dependencies, including
  `sha2`; those are outside the direct-dependency tables above.
- Active crates use internal helpers backed by AWS-LC for random bytes, bounded
  sampling, shuffling, and gossip selection. `rand` 0.8 and `rand_chacha` 0.3
  are removed; Quinn still pulls in `rand` 0.10 transitively.
- Managed UUID-v5 primary keys use an internal helper with AWS-LC's legacy
  SHA-1 implementation. The URL namespace, canonical key encoding, version and
  variant bits, and formatted identifiers are byte-compatible. `uuid` and
  `sha1_smol` are removed from the active workspace closure.
- Vendored OpenSSL and its native build/link/install targets are removed because
  no runtime code uses them. The system `openssl` CLI remains a test tool for
  generating and inspecting certificates; it is not linked into the application.

- CIDR parsing and containment use the internal `crates/murmur-ipnet` package
  (Cargo name `ipnet`, version 2.12.2). The root Cargo patch redirects both
  Murmur allowlists and `hyper-util` HTTP proxy matching to this same subset.
  It retains upstream parser/Serde behavior and license notices; this is locally
  maintained extracted third-party code, not wholly original Murmur code.
  Aggregation, ranges, address arithmetic, and schema-generation features are
  omitted. The full registry `ipnet` package is removed from the vendor tree.
  Consumers building the Rust crates outside this workspace need the same root
  Cargo patch to select the subset; CMake builds apply it automatically.
