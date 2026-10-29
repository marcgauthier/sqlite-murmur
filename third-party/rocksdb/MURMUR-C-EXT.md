# Murmur C-API extension for vendored RocksDB

`db/c_murmur_ext.cc` is sqlite-murmur-owned code (C++, like the rest of
`third-party/`) that extends the vendored RocksDB C API with the two
functions the storage layer needs and upstream `db/c.cc` does not provide:

- `murmur_rocksdb_set_db_option(db, name, value, errptr)` — forwards to
  `DB::SetDBOptions` (needed for dynamic `max_background_jobs`).
- `murmur_rocksdb_compact_range_full(db, errptr)` — forwards to
  `DB::CompactRange` over the full key range with status reporting
  (upstream `rocksdb_compact_range` is `void` and swallows failures).
- `murmur_rocksdb_checkpoint_create_with_seq(db, dir, seq, errptr)` —
  `Checkpoint::Create` + `CreateCheckpoint(dir, 0, seq)` in one call
  (upstream `rocksdb_checkpoint_create` cannot report the checkpoint
  sequence number, which the backup manifest records).
- `murmur_rocksdb_get_live_option(db, name, out, errptr)` — live readback
  of `write_buffer_size`, `max_write_buffer_number`, `max_background_jobs`.

Both follow upstream `errptr` conventions (`strdup` of `Status::ToString`,
freed with `rocksdb_free`; `Invalid argument: ...` for misuse).

## Build wiring

One line in the vendored `CMakeLists.txt` SOURCES list (next to `db/c.cc`):

```
        db/c_murmur_ext.cc
```

No upstream file content is modified; the extension is additive only.

## Upstream-sync checklist

1. Re-apply the one-line `CMakeLists.txt` addition after syncing upstream.
2. Confirm `struct rocksdb_t { DB* rep; }` in upstream `db/c.cc` still
   matches the ODR-identical re-declaration in `c_murmur_ext.cc` (the
   `static_assert`s pin size/offset; a layout change fails the build).
3. If upstream ever adds `rocksdb_set_db_options` / live option getters,
   delete this file and switch the Rust FFI in `crates/murmur-storage`
   to the upstream symbols.
4. Regenerate `third-party/source-files.sha256` via `third-party/verify.py`.
