//! Shared safe wrappers over the vendored libzstd C API.
#[path = "../../common/zstd.rs"]
mod shared;
pub use shared::*;
