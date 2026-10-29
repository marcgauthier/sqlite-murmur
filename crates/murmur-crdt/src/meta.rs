//! Conflict-version record layout. The authoritative store stays in Spool
//! (owned by the engine); this module shares the key formats and the transient
//! row-metadata payload so both hosts address identical keys.
use crate::{
    Result,
    model::{Cell, Table, Tombstone, Value, Version, encode},
};

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct RowMeta {
    pub key: Vec<u8>,
    pub generation: Version,
    pub cells: Vec<Cell>,
}

impl RowMeta {
    /// Schema adoption can add nullable columns without rewriting every Spool
    /// row. Interpret an absent cell as SQLite's implicit NULL, with the oldest
    /// version so a real incoming value wins. Mutations persist the expanded row.
    pub fn extend_nullable(&mut self, table: &Table) -> Result<()> {
        for column in &table.columns {
            if self.cells.iter().any(|cell| cell.column == column.id) {
                continue;
            }
            if column.primary_key > 0 || column.not_null {
                return Err(crate::Error::SchemaMismatch);
            }
            self.cells.push(Cell {
                column: column.id.clone(),
                value: Value::Null,
                version: Version::default(),
            });
        }
        Ok(())
    }
}

pub fn row_key(table: &str, key: &[u8]) -> Vec<u8> {
    format!("04/meta/row/{table}/{}", crate::digest::hash_hex(key)).into_bytes()
}

pub fn tomb_key(t: &Tombstone) -> Result<Vec<u8>> {
    Ok(format!(
        "05/meta/tomb/{}/{}",
        t.table,
        crate::digest::hash_hex(&encode(&(t.key.clone(), t.generation))?)
    )
    .into_bytes())
}
