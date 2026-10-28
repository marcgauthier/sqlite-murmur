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
| `cr-sqlite/` | 0.16.3 | Optional CRR replication, pinned Rust submodule and locked Cargo crates | MIT; see `cr-sqlite/LICENSE` and individual crate licenses |
| `openssl/` | 3.5.9 LTS | AES-256-GCM, cryptographic randomness and memory cleansing | Apache-2.0; see `openssl/LICENSE.txt` and upstream notices |

AES-256-GCM is supplied by OpenSSL; there is no separate handwritten AES library.
OpenSSL's normal static default provider is built in, so no external provider
modules are needed. RocksDB's optional external compression libraries, gflags,
allocators and liburing are disabled. RocksDB compression remains disabled:
the VFS compresses plaintext with Zstd before encrypting it.

`sources.json` records the exact archive URLs and SHA-256 values. SQLite's
amalgamation was additionally verified against its upstream SHA3-256 checksum.
Upstream trees are retained, including licenses, build files, tests and bundled
supporting code. Their `.gitignore` files are omitted because those patterns
would hide some vendored source/build files from this repository. The SQLite
license note above is added locally. Other upstream source files are unmodified;
project-specific integration is in `../cmake/VendorDependencies.cmake` and
`../cmake/CrSqlite.cmake`.

cr-sqlite's optional build requires the installed Rust nightly-2023-10-05 toolchain
and libclang compiler tooling. `sources.json` records its exact commit and the
sqlite-rs-embedded submodule archive. Cargo packages are locked by
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
