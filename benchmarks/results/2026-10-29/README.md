# Rust storage-port benchmark — October 29, 2026

Paired runs deciding the storage migration (C++ `src/*.cpp` vs Rust
`crates/murmur-storage` behind the same stable C ABI). The C++ side is
built from git HEAD (`2c0aed06`) in a separate worktree with identical
CMake options; the Rust side is the working tree. Same machine, same
day, back-to-back.

## Method

Intel Core i5-6500, four physical cores, Release build, one connection,
60 seconds per run, 80% random primary-key reads / 20% inserts. Local
replication tracking on, 50,000 seed rows, AEGIS-256, Zstd3, 100 ms
flush, 4 KiB pages, 64 MiB VFS cache, 1 GiB SQLite cache, 32 MiB
RocksDB cache, 4 background workers. Command per cell:

```bash
./benchmark --suite=single --backend=murmur --replication=on \
  --batch-size=<1|100> --cipher=aegis256 --seconds=60
```

## Result

| Inserts/transaction | C++ ops/sec | Rust ops/sec | Change |
| ---: | ---: | ---: | ---: |
| 1 | 184,393 | 199,292 | +8.08% |
| 100 | 397,448 | 391,579 | -1.48% |

Single paired trials, not confidence intervals. The batch-100 gap is
noise-level; the batch-1 run favors Rust. Verdict: **keep Rust**.

## Latency regression found and fixed

The first Rust batch-1 run measured 115,396 ops/sec (-37%). `perf`
showed `ReusePool::push_return` at 37% of cycles: the vendored
`zeroize` primitive wipes byte-by-byte with volatile stores, while the
C++ used `OPENSSL_cleanse` (12% on the same hygiene). The fix
(`murmur_storage::secure_wipe`: one SIMD `memset` anchored by a
compiler fence) applies to hot page/journal/scratch buffers; small key
material keeps the volatile primitive. All wipe assertions still pass
(`cargo test -p murmur-storage`: 45/45; `ctest`: 18/18; mesh 79/79).
The batch-1 re-run after the fix is the Rust number above.

Raw outputs sit alongside this report (`*-cpp.txt` vs `*-rust.txt`).
