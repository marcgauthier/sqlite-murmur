//! Policy input values only; no row model, schema or clock.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

pub fn encode<T: Serialize>(t: &T) -> Result<Vec<u8>> {
    postcard::to_allocvec(t).map_err(|e| Error::Corrupt(e.to_string()))
}
pub fn decode<T: serde::de::DeserializeOwned>(b: &[u8]) -> Result<T> {
    postcard::from_bytes(b).map_err(|e| Error::Corrupt(e.to_string()))
}
