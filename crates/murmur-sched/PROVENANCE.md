# Source provenance

`src/scheduler.rs` is a byte-identical copy of
`/media/marc/2TB/Archives/MURMUR-RUST/src/scheduler.rs` (594 lines), archive
Git commit `e5a19c25cceaf93375872e3a7f01f05ffc3659ac`.

SHA-256:
`0ad684b5f92df6fa205671c2c1c1a4334e36c278e92f43322c1d468eb8c4e86d`.

All admission, weighted-service, maintenance, timeout, closure and statistics
logic, including the four archived unit tests, remains unchanged. No
`invalidErr!` macro is used by this module. Its only crate-local dependencies
are `Error` and `Result`; external code uses `serde` and the standard library.

`src/lib.rs` is a downstream seam: public re-exports plus the three required
archived `src/lib.rs` error variants (`Invalid(String)`, `Busy`, `Closed`),
with their original display messages and a `Result<T>` alias. The remaining
database/storage error variants are intentionally excluded. No SQLite,
Spool, network or database crate is imported.

`Cargo.toml` pins serde `1.0.229` and thiserror `2.0.21` to the archive lock.
Initial isolated verification used the minimal reachable subset of the
archive lock, with only the root package entry created and the
now-unambiguous `syn 3.0.9` dependency label shortened to `syn`. That
temporary standalone lock was removed after verification per coordinator
policy: the consuming murmur-replication/Cargo.lock governs path dependencies.
All verified registry versions and checksums matched the archive. The registry
dependency closure already exists in `../murmur-replication/vendor/`:
serde, serde_core, serde_derive, thiserror, thiserror-impl, proc-macro2,
quote, syn 3.0.9, unicode-ident. No vendor delta is needed.

`tests/admission.rs` contains downstream regression tests for timeout cleanup,
idempotent release, shutdown behavior and configuration rejection. The
integration proposal and usage document describe downstream seams; they do
not change the imported scheduler or claim production integration.

The archive supplies no top-level LICENSE or license field in its root
manifest. This document records provenance and does not assign a new
license to the imported material.
