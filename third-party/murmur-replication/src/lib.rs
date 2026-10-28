//! Embedded SQLite replication. SQL is the client interface; QUIC is peer-only.
mod config;
mod engine;
mod membership;
mod protocol;
mod sql;
mod storage;
mod transport;
pub(crate) use config::Config;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
pub(crate) type NodeId = [u8; 16];
pub(crate) type Result<T> = std::result::Result<T, Error>;
#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("invalid replication configuration: {0}")]
    Invalid(String),
    #[error("replication protocol: {0}")]
    Protocol(String),
    #[error("replication authentication: {0}")]
    Auth(String),
    #[error("replication unavailable: {0}")]
    Unavailable(String),
    #[error("replication capacity exceeded")]
    Overloaded,
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
pub(crate) fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>> {
    postcard::to_allocvec(v).map_err(|e| Error::Protocol(e.to_string()))
}
pub(crate) fn decode<T: serde::de::DeserializeOwned>(v: &[u8]) -> Result<T> {
    postcard::from_bytes(v).map_err(|e| Error::Protocol(e.to_string()))
}
pub(crate) fn hex(v: &[u8]) -> String {
    v.iter().map(|b| format!("{b:02x}")).collect()
}
pub(crate) fn parse_id(v: &str) -> Result<NodeId> {
    let v = v.replace('-', "");
    if v.len() != 32 || !v.is_ascii() {
        return Err(Error::Invalid(
            "expected 128-bit hexadecimal identity".into(),
        ));
    }
    let mut id = [0; 16];
    for (i, b) in id.iter_mut().enumerate() {
        *b = u8::from_str_radix(&v[i * 2..i * 2 + 2], 16)
            .map_err(|_| Error::Invalid("invalid identity".into()))?;
    }
    if id == [0; 16] {
        return Err(Error::Invalid("zero identity".into()));
    }
    Ok(id)
}

/// Register SQL functions and the connection-scoped PRAGMA handler.
/// # Safety
/// `db` must be an open, writable sqlite-express connection in autocommit mode.
/// Loadable builds additionally require the host's SQLite API table.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_murmurreplication_init(
    db: *mut rusqlite::ffi::sqlite3,
    error: *mut *mut std::ffi::c_char,
    api: *mut rusqlite::ffi::sqlite3_api_routines,
) -> std::ffi::c_int {
    #[cfg(feature = "loadable")]
    if unsafe { rusqlite::ffi::rusqlite_extension_init2(api) }.is_err() {
        return rusqlite::ffi::SQLITE_ERROR;
    }
    #[cfg(not(feature = "loadable"))]
    let _ = api;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        sql::register(db)
    }));
    match result {
        Ok(Ok(())) => {
            // VFS callbacks must remain executable until every worker and file closes.
            #[cfg(feature = "loadable")]
            {
                rusqlite::ffi::SQLITE_OK_LOAD_PERMANENTLY
            }
            #[cfg(not(feature = "loadable"))]
            {
                rusqlite::ffi::SQLITE_OK
            }
        }
        other => {
            let msg = match other {
                Ok(Err(e)) => e.to_string(),
                _ => "murmur initialization panic".into(),
            };
            if !error.is_null() {
                unsafe { *error = sql::sqlite_string(&msg) }
            }
            rusqlite::ffi::SQLITE_ERROR
        }
    }
}
