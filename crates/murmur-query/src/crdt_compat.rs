//! Minimal `murmur-crdt` compatibility copies for WP-E.
//!
//! REMOVAL NOTE (lead-approved temporary duplication): replaced by the
//! WP-B KEEP-set import when it lands (per the scope ruling there may be
//! no standalone `murmur-crdt` crate; adopt whatever WP-B delivers).
//! Every item below is a verbatim copy from the stated archive file; only the
//! `use` paths were adjusted to this crate. Items the WP-E modules never touch
//! (`PolicyOperation`, `PolicyIntent`, `Tombstone`, `ReplicaState`, `meta`,
//! `merge`, `logical`, `table_replication`) were omitted on purpose.
//! SCOPE-RULING (7b) NOTE: this file holds inert data types + key helpers
//! only — no merge engine (merge.rs never imported), no clock behavior
//! (`Version::next` never called; only `Version::default()` stamps for
//! local row reads), no wire codecs (`encode`/`decode` never called).
//! Verified by grep over this crate; safe under the ruling.
//!
//! Sources: MURMUR-RUST `crdt/src/{digest,model,managed,schema}.rs` and
//! `src/model.rs` (`Migration`, `TableMapping`).

use crate::{Error, Result};
use serde::{Deserialize, Serialize};

// === From murmur-crdt/src/digest.rs (verbatim) ===
// SHA-256 for managed persistence, logical keys and stable schema identities.
use crate::crypto_provider::Sha256;

pub fn hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(text, "{byte:02x}").unwrap();
    }
    text
}

pub fn hash_hex(bytes: &[u8]) -> String {
    hex(&hash(bytes))
}

// === From murmur-crdt/src/model.rs (verbatim, minus PolicyOperation,
// === PolicyIntent, Tombstone, ReplicaState which WP-E never touches) ===
// CRDT data model: versions, values, cells, rows, tombstones, and schemas.

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
    hash_hex(seed)
}
pub fn quote(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

// === From murmur-crdt/src/managed.rs (verbatim) ===
// Managed-UUID primary-key scheme: the engine replaces application keys with
// a stable deterministic UUID column.

pub const COLUMN: &str = "id_murmur_primary_key";

/// Stable, length-prefixed encoding; numeric values use SQLite equality, and
/// built-in text collations are normalized before hashing.
pub fn uuid(table: &str, collations: &[String], values: &[Value]) -> Result<String> {
    if values.len() != collations.len() {
        return Err(Error::Invalid("application key width mismatch".into()));
    }
    let mut bytes = b"murmur/replication-primary-key/v1\0".to_vec();
    fn part(out: &mut Vec<u8>, value: &[u8]) {
        out.extend_from_slice(&(value.len() as u64).to_be_bytes());
        out.extend_from_slice(value);
    }
    part(&mut bytes, table.to_ascii_lowercase().as_bytes());
    bytes.extend_from_slice(&(values.len() as u64).to_be_bytes());
    for (value, collation) in values.iter().zip(collations) {
        match value {
            Value::Null => {
                return Err(Error::Invalid(
                    "application primary key must be supplied and non-null".into(),
                ));
            }
            Value::Integer(n) => {
                bytes.push(1);
                bytes.extend_from_slice(&n.to_be_bytes());
            }
            Value::Real(n)
                if n.is_finite()
                    && *n >= i64::MIN as f64
                    && *n < -(i64::MIN as f64)
                    && (*n as i64) as f64 == *n =>
            {
                bytes.push(1);
                bytes.extend_from_slice(&(*n as i64).to_be_bytes());
            }
            Value::Real(n) => {
                bytes.push(2);
                bytes.extend_from_slice(&n.to_bits().to_be_bytes());
            }
            Value::Text(s) => {
                bytes.push(3);
                let s = if collation.eq_ignore_ascii_case("NOCASE") {
                    let mut b = s.as_bytes().to_ascii_lowercase();
                    if let Some(n) = b.iter().position(|c| *c == 0) {
                        b[n..].fill(0);
                    }
                    String::from_utf8(b).map_err(|_| Error::Invalid("invalid NOCASE key".into()))?
                } else if collation.eq_ignore_ascii_case("RTRIM") {
                    s.trim_end_matches(' ').into()
                } else if collation.eq_ignore_ascii_case("BINARY") {
                    s.clone()
                } else {
                    return Err(Error::Invalid(
                        "unsupported application key collation".into(),
                    ));
                };
                part(&mut bytes, s.as_bytes());
            }
            Value::Blob(b) => {
                bytes.push(4);
                part(&mut bytes, b);
            }
        }
    }
    Ok(crate::uuid_provider::v5_url(&bytes))
}

pub fn validate_row(t: &Table, row: &RowState) -> Result<()> {
    if let Some(key) = &t.application_key {
        let values = key
            .columns
            .iter()
            .map(|name| {
                let col = t
                    .columns
                    .iter()
                    .find(|c| c.name == *name)
                    .ok_or(Error::SchemaMismatch)?;
                row.cells
                    .iter()
                    .find(|c| c.column == col.id)
                    .map(|c| c.value.clone())
                    .ok_or(Error::SchemaMismatch)
            })
            .collect::<Result<Vec<_>>>()?;
        let expected = Value::Text(uuid(&t.name, &key.collations, &values)?);
        let col = t
            .columns
            .iter()
            .find(|c| c.name == COLUMN)
            .ok_or(Error::SchemaMismatch)?;
        if row.key != [expected.clone()]
            || row
                .cells
                .iter()
                .find(|c| c.column == col.id)
                .map(|c| &c.value)
                != Some(&expected)
        {
            return Err(Error::Invalid(
                "invalid managed replication primary key".into(),
            ));
        }
    }
    Ok(())
}

// === From murmur-crdt/src/schema.rs (verbatim) ===
// Stable schema helpers shared by the engine and the extension.

pub fn pk_columns(t: &Table) -> Vec<&Column> {
    let mut pk: Vec<_> = t.columns.iter().filter(|c| c.primary_key > 0).collect();
    pk.sort_by_key(|c| c.primary_key);
    pk
}

/// Table named by a plain `CREATE [UNIQUE] INDEX [IF NOT EXISTS] <name> ON
/// [<schema>.]<table> (...)` statement. Quoted identifiers and comments are
/// honored. Returns None when the shape is unrecognized; callers keep such
/// indexes and let SQLite validate them on adopt.
pub fn index_table(sql: &str) -> Option<String> {
    enum T {
        Bare(String),
        Name(String),
        Dot,
    }
    let b = sql.as_bytes();
    let mut toks: Vec<T> = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c == b'\'' {
            i += 1;
            while i < b.len() {
                if b[i] == b'\'' {
                    i += 1;
                    if i < b.len() && b[i] == b'\'' {
                        i += 1;
                    } else {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
        } else if c == b'"' || c == b'`' {
            i += 1;
            let mut raw: Vec<u8> = Vec::new();
            while i < b.len() {
                if b[i] == c {
                    if i + 1 < b.len() && b[i + 1] == c {
                        raw.push(c);
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    raw.push(b[i]);
                    i += 1;
                }
            }
            toks.push(T::Name(String::from_utf8_lossy(&raw).into_owned()));
        } else if c == b'[' {
            i += 1;
            let start = i;
            while i < b.len() && b[i] != b']' {
                i += 1;
            }
            toks.push(T::Name(
                String::from_utf8_lossy(&b[start..i]).into_owned(),
            ));
            i = (i + 1).min(b.len());
        } else if c == b'-' && b[i..].starts_with(b"--") {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
        } else if c == b'/' && i + 1 < b.len() && b[i + 1] == b'*' {
            i += 2;
            while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(b.len());
        } else if c.is_ascii_alphabetic() || c == b'_' {
            let start = i;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'$') {
                i += 1;
            }
            toks.push(T::Bare(sql[start..i].to_ascii_uppercase()));
        } else if c == b'.' {
            toks.push(T::Dot);
            i += 1;
        } else {
            i += 1;
        }
    }
    let mut k = 0;
    let eat = |toks: &[T], k: &mut usize, word: &str| -> bool {
        if matches!(&toks.get(*k), Some(T::Bare(w)) if w == word) {
            *k += 1;
            true
        } else {
            false
        }
    };
    if !eat(&toks, &mut k, "CREATE") {
        return None;
    }
    eat(&toks, &mut k, "UNIQUE");
    if !eat(&toks, &mut k, "INDEX") {
        return None;
    }
    if eat(&toks, &mut k, "IF") {
        if !eat(&toks, &mut k, "NOT") || !eat(&toks, &mut k, "EXISTS") {
            return None;
        }
    }
    match toks.get(k) {
        Some(T::Bare(_)) | Some(T::Name(_)) => k += 1,
        _ => return None,
    }
    if !eat(&toks, &mut k, "ON") {
        return None;
    }
    let first = match toks.get(k) {
        Some(T::Bare(n)) | Some(T::Name(n)) => n.clone(),
        _ => return None,
    };
    k += 1;
    if matches!(toks.get(k), Some(T::Dot)) {
        k += 1;
        return match toks.get(k) {
            Some(T::Bare(n)) | Some(T::Name(n)) => Some(n.clone()),
            _ => None,
        };
    }
    // Bare tokens were uppercased for keyword matching; table names compare
    // case-insensitively at use sites.
    Some(first)
}

// === From sqlite-session src/model.rs (verbatim: migration types) ===

#[derive(Debug, Clone, Default)]
pub struct TableMapping {
    pub old_name: String,
    pub new_name: String,
    pub columns: Vec<(String, String)>,
}
#[derive(Debug, Clone)]
pub struct Migration {
    pub sql: String,
    pub mappings: Vec<TableMapping>,
}
impl Migration {
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            mappings: Vec::new(),
        }
    }
}
