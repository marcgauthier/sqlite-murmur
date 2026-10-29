# Source provenance

`murmur-storage` is authored in this repository as a Rust port of the
sqlite-murmur storage core (`src/sqlite_murmur_*.cpp`). It has no archive
source: behavior is defined by the C++ implementation it replaces and
pinned by the existing suites (`tests/errors.cpp`, `tests/keyring.cpp`,
`tests/backup.cpp`, `tests/storage_concurrency.cpp`, `tests/integration.cpp`,
`tests/concurrent.cpp`, the benchmark, and the 79 live mesh scenarios).

Ported modules keep their `extern "C"` symbol names from `include/` so
C/C++ test drivers and consumer applications link unchanged. Port order
and per-module parity gates follow the migration plan: errors, keyring,
backup, then the Store/VFS core. The open API lives in the sibling
`murmur-storage-api` crate (own staticlib): minimal consumers must never
reference the extension initializers, whose transitive chain reaches C++
symbols that only store-linking binaries provide, and only crate
(archive) boundaries guarantee that separation.

`src/error.rs` ports `src/sqlite_murmur_errors.cpp` (error taxonomy
projection). The 15 extended codes keep discriminator `0x5301..=0x530f`
with the SQLite primary in the low byte; canonical values stay declared
in `include/sqlite_murmur_errors.h`.

The current crate uses the workspace's pinned, vendored AWS-LC for AES-GCM,
HKDF-SHA256, and SHA-256 through the internal `crates/common/` adapters.
Compression uses shared hand-written bindings to the vendored C zstd archive;
RocksDB and AEGIS likewise use hand-written `extern "C"` bindings. `zeroize`
and `libc` remain direct dependencies; random bytes come from AWS-LC. Provider
compatibility fixtures and the existing C++ compatibility suite preserve the
original record formats.

Dependency consolidation uses AWS-LC for randomness and storage cryptography.
Original encrypted record formats are covered by compatibility fixtures.
