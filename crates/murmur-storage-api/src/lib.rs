//! Integrated open/close behind the stable C ABI (`sqlite_murmur_open*`).
//!
//! Deliberately standalone (no workspace path dependencies): this crate
//! builds its own staticlib, and any shared Rust item would be compiled
//! into two staticlibs with colliding symbols. The SQLite result codes
//! and panic guard are therefore defined here rather than reused.
//!
//! Split from `murmur-storage` for link granularity: minimal consumers
//! (error/keyring/backup tests) must never reference the extension
//! initializers, whose transitive chain reaches C++ symbols that only
//! store-linking binaries provide. Rust codegen units group modules
//! unpredictably, so only crate (archive) boundaries guarantee this.

pub mod api;
