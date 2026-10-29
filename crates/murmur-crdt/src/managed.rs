//! Managed-UUID primary-key scheme: the engine replaces application keys with
//! a stable deterministic UUID column. The loadable extension uses natural
//! keys instead and does not call this; it moves with the merge so engine
//! behavior stays identical.
use crate::{
    Error, Result,
    model::{RowState, Table, Value},
};

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
    Ok(::uuid::Uuid::new_v5(&::uuid::Uuid::NAMESPACE_URL, &bytes)
        .hyphenated()
        .to_string())
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
