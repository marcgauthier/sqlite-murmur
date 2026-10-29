//! Archived Murmur writer scheduler with a minimal error-type seam.
//!
//! SQLite integration and PRAGMA configuration belong to the consuming crate.
//! See USAGE.md and INTEGRATION-PROPOSAL.md for the downstream contract.

pub mod scheduler;
pub use scheduler::{
    SchedulerClassStats, SchedulerConfig, SchedulerSnapshot, Ticket, WriterClass, WriterScheduler,
};

/// The three archived database Error variants used by scheduler.rs.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid configuration or operation: {0}")]
    Invalid(String),
    #[error("checkpoint queue full or merge baseline changed")]
    Busy,
    #[error("database is closed")]
    Closed,
}

pub type Result<T> = std::result::Result<T, Error>;
