//! Column-policy kernels reused from the Murmur archive, hosted by CRR.
//!
//! This crate owns no rows, clocks, SQLite connections, persistence or wire
//! protocol. It validates/reduces carriers in memory; SQLite installation and
//! the native ingest/projection adapter remain lead-owned behind the A4 gate.
pub mod carrier;
pub mod causal;
pub mod compare;
pub mod high_low;
pub mod model;
pub mod ownership;

// Canonical imported implementation; do not compile another copy.
pub use murmur_tx::policies;

/// The archived database Error subset required by these pure kernels.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid configuration or operation: {0}")]
    Invalid(String),
    #[error("corrupt or unsupported storage: {0}")]
    Corrupt(String),
    #[error("resource limit: {0}")]
    Limit(String),
    #[error("database identity or schema history mismatch")]
    SchemaMismatch,
}

pub type Result<T> = std::result::Result<T, Error>;
