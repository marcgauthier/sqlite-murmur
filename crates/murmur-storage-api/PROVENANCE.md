# Source provenance

`murmur-storage-api` is authored in this repository as a Rust port of
`src/sqlite_murmur_api.cpp` (integrated open/close: VFS open, cipher /
compression options, CRR + Murmur extension initialization, CRR-aware
close). It has no archive source: behavior is defined by the C++
implementation it replaces and pinned by the existing suites
(`tests/integration.cpp`, `tests/application.cpp`,
`tests/concurrent.cpp`, the benchmark, and the 79 live mesh scenarios).

The port keeps the `extern "C"` symbol names from `include/sqlite_murmur.h`
(`sqlite_murmur_open`, `sqlite_murmur_open_v2`, `sqlite_murmur_close`) so
C/C++ test drivers and consumer applications link unchanged.

The crate is standalone by design (no path dependencies, no registry
dependencies): it builds its own staticlib, and sharing Rust items with
`murmur-storage` would compile them into two staticlibs with colliding
symbols. SQLite itself is used through hand-declared FFI that resolves
from the vendored amalgamation at final link, exactly like the C++
objects. Cargo features mirror the CMake options (`crsqlite`, `murmur`);
without them the extension symbols are never referenced, so option-off
builds link cleanly.
