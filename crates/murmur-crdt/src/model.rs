//! CRDT data model: versions, values, cells, rows, tombstones, and schemas.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};

/// Total order: physical milliseconds, logical counter, node identity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Version {
    pub millis: u64,
    pub logical: u32,
    pub node: [u8; 16],
}
impl Version {
    pub fn next(self, node: [u8; 16]) -> Result<Self> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        if now > self.millis {
            Ok(Self {
                millis: now,
                logical: 0,
                node,
            })
        } else if let Some(logical) = self.logical.checked_add(1) {
            Ok(Self {
                millis: self.millis,
                logical,
                node,
            })
        } else {
            Ok(Self {
                millis: self
                    .millis
                    .checked_add(1)
                    .ok_or_else(|| Error::Invalid("clock exhausted".into()))?,
                logical: 0,
                node,
            })
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}
impl rusqlite::types::ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        use rusqlite::types::{ToSqlOutput, ValueRef};
        Ok(ToSqlOutput::Borrowed(match self {
            Self::Null => ValueRef::Null,
            Self::Integer(n) => ValueRef::Integer(*n),
            Self::Real(n) => ValueRef::Real(*n),
            Self::Text(s) => ValueRef::Text(s.as_bytes()),
            Self::Blob(b) => ValueRef::Blob(b),
        }))
    }
}
impl Value {
    pub fn read(v: rusqlite::types::ValueRef<'_>) -> rusqlite::Result<Self> {
        use rusqlite::types::ValueRef;
        Ok(match v {
            ValueRef::Null => Self::Null,
            ValueRef::Integer(n) => Self::Integer(n),
            ValueRef::Real(n) => Self::Real(n),
            ValueRef::Text(s) => Self::Text(
                std::str::from_utf8(s)
                    .map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            s.len(),
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    })?
                    .into(),
            ),
            ValueRef::Blob(b) => Self::Blob(b.to_vec()),
        })
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergePolicy {#[default] Lww, Counter, Set, Min, Max}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PolicyOperation {Counter(i64),Add(Value),Remove(Value),Min(Value),Max(Value)}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyIntent {pub table:String,pub column:String,pub key:Vec<Value>,pub before:Value,pub operation:PolicyOperation,pub id:[u8;16]}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    /// Persistent vector-search declaration; index contents are disposable.
    #[serde(default)]
    pub vector: Option<VectorColumnConfig>,
    /// Include this TEXT column in the table's managed FTS5 index.
    #[serde(default)]
    pub fts: bool,
    pub policy: MergePolicy,
    pub id: String,
    pub name: String,
    pub primary_key: usize,
    pub declared_type: String,
    pub not_null: bool,
}
/// Original application key retained while SQLite uses the managed UUID key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicationKey {
    pub columns: Vec<String>,
    pub collations: Vec<String>,
    pub descending: Vec<bool>,
    pub without_rowid: bool,
    pub strict: bool,
    pub conflict: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Table {
    pub application_key: Option<ApplicationKey>,
    pub id: String,
    pub name: String,
    pub sql: String,
    pub columns: Vec<Column>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Schema {
    /// Local table replication settings, keyed by stable table ID. Missing means
    /// local-only. Persisted alongside the schema in Spool, excluded from wire data.
    pub table_replication: std::collections::BTreeMap<String, bool>,
    pub tables: Vec<Table>,
    pub indexes: Vec<String>,
    pub history: [u8; 32],
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cell {
    pub column: String,
    pub value: Value,
    pub version: Version,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowState {
    pub table: String,
    pub key: Vec<Value>,
    pub generation: Version,
    pub cells: Vec<Cell>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tombstone {
    pub table: String,
    pub key: Vec<u8>,
    pub generation: Version,
    pub version: Version,
}
/// Portable logical state. Local node identity and persistence cursors are not merged.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplicaState {
    pub database_id: [u8; 16],
    pub schema: Schema,
    pub rows: Vec<RowState>,
    pub tombstones: Vec<Tombstone>,
    pub clock: Version,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DistanceMetric {
    #[default]
    L2,
    Cosine,
    L1,
}
/// Persistent configuration for one application's BLOB vector column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorColumnConfig {
    pub dimensions: usize,
    #[serde(default)]
    pub distance: DistanceMetric,
}
pub fn encode<T: Serialize>(t: &T) -> Result<Vec<u8>> {
    postcard::to_allocvec(t).map_err(|e| Error::Corrupt(e.to_string()))
}
pub fn decode<T: serde::de::DeserializeOwned>(b: &[u8]) -> Result<T> {
    postcard::from_bytes(b).map_err(|e| Error::Corrupt(e.to_string()))
}
pub fn id(seed: &[u8]) -> String {
    crate::digest::hash_hex(seed)
}
pub fn quote(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}
