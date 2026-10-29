# murmur-admin provenance

Rust port of `tools/murmur-admin/admin.cpp` (in-repo C++ port; no archive
source): the offline-only operational CLI (verify, restore, status,
inspect, repair, keys, rekey) behind the stable C ABI declared in
`tools/murmur-admin/admin.h`.

Port discipline: byte-exact stdout/stderr/exit codes (pinned by the
in-process C++ suite `tools/murmur-admin/tests/admin.cpp`), hand-written
FFI only (SQLite C API subset + stable murmur C API + fcntl probe), zero
crate dependencies. Key material is wiped after registration and never
printed; `--json` output carries identity markers only.
