# Phase 0 — Scaffold + linkage spike (done)

## Result

A Rust `cdylib` loads into stock session-enabled SQLite hosts, creates a
`sqlite3_session` on the host connection, captures real writes, and walks the
changeset — all through Sessions entry points resolved from the host library.
Verified in three hosts: a hand-rolled Rust host (`tests/spike.rs`), the stock
`sqlite3` CLI, and CPython's `sqlite3` module.

## Key decisions

1. **Framework: `rusqlite` `loadable_extension`, not `sqlite-loadable-rs`.**
   Same SQLite dependency the Murmur engine already uses (0.40.2), canonical
   `extension_init2` pattern, no extra crate. `functions`, `hooks`, and
   `trace` all compile under `loadable_extension`.
2. **Session FFI declared manually (`src/session_sys.rs`).** `libsqlite3-sys`
   hard-errors on `loadable_extension` + `preupdate_hook`/`session` because
   session entry points are not in the `sqlite3_api_routines` dispatch table.
   The extension therefore calls core APIs through the dispatch table and the
   9 session functions it needs as direct `extern "C"` symbols. Signatures
   verified against the SQLite 3.45.1 amalgamation `sqlite3.h` (note:
   `sqlite3changeset_op` takes `int *pbIndirect` in 3.45).
3. **Explicit `DT_NEEDED` on `libsqlite3.so.0` (build.rs).** `rusqlite` emits
   no link directives for `loadable_extension` builds, leaving symbols to
   resolve from the host's global namespace. That fails in hosts like CPython
   that keep `libsqlite3` out of the global scope (`RTLD_LOCAL`). Linking by
   SONAME guarantees the same library instance the host uses. No sudo needed:
   the build script symlinks the system's `libsqlite3.so.0` into `OUT_DIR`.
4. **Foreign-host integration tests, not a `rusqlite` host.** Cargo feature
   unification makes it impossible to build `rusqlite` as both
   `loadable_extension` (the extension) and `bundled`/`session` (a test host)
   in one compilation — both crates `compile_error!`. `tests/spike.rs` instead
   links system SQLite through ~20 hand-declared functions, which is also a
   more faithful production simulation. (Quirk found: the lib's build.rs
   `rustc-link-lib` is not propagated to integration-test links, though its
   `rustc-link-search` is — so the test declares `#[link(name = "sqlite3")]`
   itself.)
5. **Static-link variant deferred, not needed yet.** The `.so` covers shared-
   library hosts; statically linked hosts remain a documented non-goal until
   Phase 5.

## Verified semantics (shape later phases)

- **Per-PK netting:** a session records each primary key once per lifetime —
  INSERT+UPDATE nets to one op, INSERT+DELETE to nothing
  ([`sqlite3session_changeset`](https://www.sqlite.org/session/sqlite3session_changeset.html)).
  Consequence for the CRDT layer: each row appears at most once per collected
  batch, already netted. The spike test initially asserted 4 ops for
  overlapping writes, got 1, and was corrected to use disjoint keys.
- **Host floor:** system SQLite 3.45.1 with `ENABLE_SESSION`,
  `ENABLE_PREUPDATE_HOOK`, `ENABLE_LOAD_EXTENSION` confirmed via
  `PRAGMA compile_options` + `nm -D`.

## Evidence

- `cargo test -p murmur-sqlite-ext` — 3/3 pass (`spike.rs`).
- `sqlite3 :memory:` + `.load` — hello/begin/capture/count/end all correct.
- CPython `load_extension` — 2-row insert yields 2 ops + 34-byte blob.
- `readelf -d` — cdylib carries `NEEDED libsqlite3.so.0`.
