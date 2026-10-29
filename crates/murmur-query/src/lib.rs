//! Query plane for sqlite-murmur (WP-E, points 15-18): disposable search
//! indexes (FTS5 + vec0), live subscriptions, migrations, and managed keys.
//!
//! Archive modules (`search`, `subscription`, `schema`, `primary_key`) are
//! ported from MURMUR-RUST `src/` with sqlite-murmur seams isolated in
//! [`adapt`]. See `PROVENANCE.md` for sources and deviations.

pub mod adapt;
pub mod crdt_compat;
pub mod primary_key;
pub mod schema;
pub mod search;
pub mod subscription;

/// Compatibility re-exports so ported `model::` paths keep working.
/// Backed by [`crdt_compat`]; replaced by a `murmur-crdt` dependency when
/// WP-B lands (see the removal note in `crdt_compat.rs`).
pub mod model {
    pub use crate::crdt_compat::*;
}

/// Compatibility re-export so ported `crate::digest::hash` keeps working.
pub mod digest {
    pub use crate::crdt_compat::{hash, hash_hex};
}

pub use crdt_compat::{
    ApplicationKey, Cell, Column, Migration, RowState, Schema, Table, TableMapping, Value,
    Version,
};
pub use search::{
    DistanceMetric, Fts5Index, SearchIndex, SearchIndexStatus, SearchOptions, VectorColumnConfig,
    VectorIndex,
};
pub use subscription::{
    ChangeType, RowChange, Subscription, SubscriptionCursor, SubscriptionEvent, SubscriptionOptions,
};

/// Archive error taxonomy (`sqlite-session` src/lib.rs), minus `WrongKey`
/// (no crypto in this crate), plus `Unavailable` for the WP-A merge-engine
/// seam (maps to WP-F `SQLITE_MURMUR_UNAVAILABLE`).
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("SQLite: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("storage: {0}")]
    Storage(String),
    #[error("invalid configuration or operation: {0}")]
    Invalid(String),
    #[error("corrupt or unsupported storage: {0}")]
    Corrupt(String),
    #[error("resource limit: {0}")]
    Limit(String),
    #[error("database identity or schema history mismatch")]
    SchemaMismatch,
    #[error("checkpoint queue full or merge baseline changed")]
    Busy,
    #[error("database is closed")]
    Closed,
    #[error("SQL callback panicked; transaction rolled back")]
    CallbackPanicked,
    #[error("query service unavailable: {0}")]
    Unavailable(String),
}

pub type Result<T> = std::result::Result<T, Error>;
