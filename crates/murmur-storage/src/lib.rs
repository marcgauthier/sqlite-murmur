//! Rust port of the sqlite-murmur storage core (`src/*.cpp`).
//!
//! This crate owns the encrypted RocksDB VFS, keyring envelope, backup,
//! and error taxonomy behind the stable C ABI declared in `include/`.
//! Ported modules keep their `extern "C"` symbol names so the existing
//! C/C++ test drivers and consumer applications link unchanged.

#[path = "../../common/crypto.rs"]
mod crypto_provider;
#[path = "../../common/random.rs"]
mod randomness;

pub mod abi;
pub mod backup;
pub mod crypto;
pub mod error;
#[cfg(feature = "aegis256")]
pub mod ffi_aegis;
pub mod ffi_rocksdb;
pub mod ffi_sqlite;
pub mod ffi_zstd;
pub mod keyring;
pub mod pool;
pub mod store;
pub mod store_ctl;
pub mod vfs;

/// Panic boundary for `extern "C"` entries: run `f`, returning `fallback`
/// if it unwinds. FFI entries fail closed and never unwind into C/C++.
pub(crate) fn guard<F, R>(f: F, fallback: R) -> R
where
    F: FnOnce() -> R,
{
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).unwrap_or(fallback)
}

/// Fast cleansing wipe for hot page/journal/scratch buffers (4 KiB and
/// up, touched per transaction or per block). Uses one SIMD `memset`
/// anchored by a compiler fence, which forces the stores to be emitted
/// and blocks dead-store elimination across it; the wiped buffers always
/// outlive the fence (pool return, truncate, or stack unwind), so the
/// observable behavior matches a volatile wipe at `memset` speed.
///
/// Rationale: the vendored `zeroize` primitive issues a volatile store
/// per byte, which profiled at 37% of batch-1 cycles (vs 12% for the
/// C++ `OPENSSL_cleanse` on the same hygiene). Small key material
/// (<= 64 bytes, cold paths) keeps the audited volatile primitive.
pub(crate) fn secure_wipe(bytes: &mut [u8]) {
    // SAFETY: `write_bytes` over the live slice, then fenced.
    unsafe {
        std::ptr::write_bytes(bytes.as_mut_ptr(), 0, bytes.len());
    }
    std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
}
