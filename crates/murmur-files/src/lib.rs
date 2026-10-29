//! sqlite-murmur file and bundle transport (WP-C).
//!
//! This crate owns the sqlite-murmur seams around the canonical murmur-tx
//! transport modules: the `murmur_files_*` / `murmur_bridge_*` / `murmur_fetch_*`
//! PRAGMA spec, pragma-to-config mapping, SQL-action handler logic, and the
//! fetch-driver / bridge host lifecycles. The wire formats (SPFO objects,
//! FF/1 frames, bridge bundle v1) and the QUIC/TLS endpoint stack stay
//! canonical in murmur-tx and are reused, never copied. See PROVENANCE.md.

pub mod adapt;
pub mod hosts;
pub mod objects;
pub mod store;

mod crypto;
mod fetch_sqlite;

#[cfg(test)]
mod stub;

pub use murmur_tx::{Backend, Error, NodeId, Record, Result, bridge, filefetch, files, ftp};
