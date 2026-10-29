//! Shared Murmur CRDT core, extracted from `sqlite-session`.
//!
//! Pure conflict machinery with no database, worker, or transport attached:
//! HLC versions ([`model::Version`]), cell/row/tombstone types, the LWW row
//! merge ([`merge`]), Spool metadata key layout ([`meta`]), table opt-in
//! flags ([`table_replication`]), stable schema identities ([`schema`]), and
//! the managed-UUID key scheme ([`managed`]).
//!
//! Both the in-memory engine (`sqlite-session`) and the loadable SQLite
//! extension consume this crate so conflict semantics cannot diverge.

pub mod compare;
pub mod digest;
pub mod managed;
pub mod merge;
pub mod meta;
pub mod model;
pub mod schema;
pub mod schema_codec;
pub mod table_replication;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Invalid(String),
    Corrupt(String),
    SchemaMismatch,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(message) => write!(f, "invalid configuration or operation: {message}"),
            Self::Corrupt(message) => write!(f, "corrupt or unsupported storage: {message}"),
            Self::SchemaMismatch => write!(f, "database identity or schema history mismatch"),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;
