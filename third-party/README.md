# Vendored dependencies

All runtime library sources are stored here as ordinary repository files, not
submodules or downloaded build caches. A checkout builds offline;
CMake does not fetch code or discover installed copies of these libraries.
Compilers, CMake, GNU Make, Perl, platform system libraries and optional Python
for tests remain build prerequisites.

| Directory | Version | Purpose | License |
| --- | --- | --- | --- |
| `sqlite/` | 3.54.1 | SQLite amalgamation: `sqlite3.c`, `sqlite3.h`, extension header and shell source | Public domain; see source headers and `sqlite/LICENSE.md` |
| `rocksdb/` | 8.9.1 | RocksDB storage engine, kept on the project's tested API baseline | Apache-2.0 / BSD-3-Clause; see `rocksdb/LICENSE.Apache`, `rocksdb/LICENSE.leveldb` |
| `zstd/` | 1.5.7 | VFS compression before encryption | BSD-3-Clause / GPL-2.0 alternatives; see `zstd/LICENSE`, `zstd/COPYING` |
| `aegis/` | 0.9.1 | AEGIS-256 authenticated encryption | MIT; see `aegis/LICENSE` |
| `cr-sqlite/` | cr-sqlite-2 local snapshot | Optional CRR replication, pinned Rust submodule and locked Cargo crates | MIT; see `cr-sqlite/LICENSE` and individual crate licenses |
| `murmur-replication/` | 0.1.0 local adaptation | Optional Rust QUIC/SWIM/gossip replication extension with a SQLite-only client interface | See `murmur-replication/PROVENANCE.md` and vendored crate licenses |
| `openssl/` | 3.5.9 LTS | AES-256-GCM, cryptographic randomness and memory cleansing | Apache-2.0; see `openssl/LICENSE.txt` and upstream notices |

AES-256-GCM is supplied by OpenSSL; there is no separate handwritten AES library.
OpenSSL's normal static default provider is built in, so no external provider
modules are needed. RocksDB's optional external compression libraries, gflags,
allocators and liburing are disabled. RocksDB compression remains disabled:
the VFS compresses plaintext with Zstd before encrypting it.

`sources.json` records archive URLs/checksums for release archives and the exact
working-tree fingerprint/provenance for the local cr-sqlite-2 snapshot. SQLite's
amalgamation was additionally verified against its upstream SHA3-256 checksum.
Upstream trees are retained, including licenses, build files, tests and bundled
supporting code. Their `.gitignore` files are omitted because those patterns
would hide some vendored source/build files from this repository. The SQLite
license note above is added locally. Other release-library source files are unmodified; cr-sqlite-2 intentionally
includes the local fork changes recorded by its content fingerprint;
project-specific integration is in `../cmake/VendorDependencies.cmake` and
`../cmake/CrSqlite.cmake`.

cr-sqlite's optional build requires the installed Rust nightly-2023-10-05 toolchain
and libclang compiler tooling. `sources.json` records upstream base commit
43ab94cac537d080aac7cb7628a435c02ee9e268 plus the local working-tree fingerprint,
and the unchanged sqlite-rs-embedded submodule archive. The sibling checkout's
uncommitted source changes, including bulk_ingest.rs, are ordinary vendored files.
Source-control metadata, local .env files, upstream .gitignore files and ignored
build outputs are excluded. The generated sqlite-express-revision.txt ties the
Rust build and benchmark metadata to that snapshot. Cargo packages are locked by
`cr-sqlite/core/rs/bundle_static/Cargo.lock` and retained under `cr-sqlite/vendor`,
including package checksums, licenses, upstream Windows import archives and test
fixtures. These are upstream package contents, not project build outputs. A local
vendor-root `.gitignore` allows those files through this repository's ignore
rules. Cargo verifies crate checksums and builds offline with `--frozen`.

## Integrity and updates

Run `python3 third-party/verify.py` to check the vendored file inventory and
SHA-256 hashes against `source-files.sha256`. This needs no network access.
Generated build outputs belong in the CMake binary directory, not these trees.

To update a dependency, obtain its official release archive, verify the release
checksum/signature, replace that directory, omit upstream `.gitignore` files,
and retain its licenses/notices. Update its entry in `sources.json`, review the
source changes, regenerate `source-files.sha256`, then run an offline clean build,
CTest and the benchmark. Dependency updates are explicit source changes; no
branch is followed automatically during configure or build.

## Murmur replication

`murmur-replication/` adapts the user-provided Murmur archive at commit
`e5a19c25cceaf93375872e3a7f01f05ffc3659ac`. It retains the transport and membership
logic and replaces Spool synchronization with cr-sqlite batches and ordinary
SQLite metadata tables. See its PROVENANCE.md for source attribution and its
USAGE.md for the SQL-only API. The optional CMake build requires modern stable
Rust in addition to cr-sqlite's existing nightly toolchain. Cargo dependencies
are pinned and vendored; both static and loadable builds run offline.
