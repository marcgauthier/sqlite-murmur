//! Changeset-to-mutation translation for the drain path.
//!
//! Mirrors the engine's `metadata::delta` net-effect rules over our manual
//! changeset iterator: coalesce to one op per row, tombstone deletes of known
//! rows (generation preserved), stamp touched cells on update, mint fresh
//! generations on insert. Divergences from engine behavior here would fork
//! conflict semantics, so the rules stay line-for-line parallel.

use crate::session_sys;
use murmur_crdt::model::{Cell, RowState, Schema, Tombstone, Value, Version};
use std::collections::HashMap;
use std::ffi::CStr;
use std::os::raw::c_int;

/// One batch's net effect in two forms: Spool mutations for the local
/// commit, and row/tombstone states for the replication log payload. Both
/// describe the same effect; the caller commits them atomically so local
/// state and the log can never diverge.
pub struct Delta {
    pub mutations: Vec<spool::Mutation>,
    pub rows: Vec<RowState>,
    pub tombstones: Vec<Tombstone>,
}

impl Delta {
    fn empty() -> Self {
        Self {
            mutations: Vec::new(),
            rows: Vec::new(),
            tombstones: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty() && self.tombstones.is_empty()
    }
}

struct Coalesced {
    table: usize,
    key: Vec<u8>,
    first_insert: bool,
    last_delete: bool,
    last_new: Vec<Option<Value>>,
}

fn value_from_sqlite(value: *mut rusqlite::ffi::sqlite3_value) -> Result<Value, String> {
    use rusqlite::ffi;
    if value.is_null() {
        return Err("murmur: null sqlite3_value".into());
    }
    // SAFETY: `value` borrows from a live changeset iterator; bytes are
    // copied immediately.
    unsafe {
        match ffi::sqlite3_value_type(value) {
            ffi::SQLITE_INTEGER => Ok(Value::Integer(ffi::sqlite3_value_int64(value))),
            ffi::SQLITE_FLOAT => Ok(Value::Real(ffi::sqlite3_value_double(value))),
            ffi::SQLITE_TEXT => {
                let n = ffi::sqlite3_value_bytes(value) as usize;
                let p = ffi::sqlite3_value_text(value);
                if p.is_null() {
                    return Err("murmur: null text pointer".into());
                }
                let bytes = std::slice::from_raw_parts(p as *const u8, n);
                let text = std::str::from_utf8(bytes)
                    .map_err(|_| "murmur: invalid UTF-8 in changeset text")?;
                Ok(Value::Text(text.to_string()))
            }
            ffi::SQLITE_BLOB => {
                let n = ffi::sqlite3_value_bytes(value) as usize;
                let p = ffi::sqlite3_value_blob(value);
                if n == 0 || p.is_null() {
                    Ok(Value::Blob(Vec::new()))
                } else {
                    Ok(Value::Blob(
                        std::slice::from_raw_parts(p as *const u8, n).to_vec(),
                    ))
                }
            }
            ffi::SQLITE_NULL => Ok(Value::Null),
            t => Err(format!("murmur: unknown sqlite value type {t}")),
        }
    }
}

fn check(rc: c_int, what: &str) -> Result<(), String> {
    if rc == session_sys::SQLITE_OK {
        Ok(())
    } else {
        Err(format!("murmur: {what} failed rc={rc}"))
    }
}

/// Build one atomic mutation group for a collected batch. Reads existing row
/// metadata through `store`; the caller commits the group together with the
/// clock bump, the replication log entry, and acknowledges the outbox row
/// only after.
pub fn build(
    store: &murmur_crdt::logical::LogicalStore,
    schema: &Schema,
    bytes: &[u8],
    version: Version,
) -> Result<Delta, String> {
    if bytes.is_empty() {
        return Ok(Delta::empty());
    }
    // First pass: validate every op and coalesce to one net effect per row.
    let mut buf = bytes.to_vec();
    let mut iter: *mut session_sys::ChangesetIter = std::ptr::null_mut();
    // SAFETY: `buf` is a live heap copy for the iterator's lifetime.
    check(
        unsafe {
            session_sys::sqlite3changeset_start(
                &mut iter,
                buf.len() as c_int,
                buf.as_mut_ptr() as *mut std::ffi::c_void,
            )
        },
        "changeset_start",
    )?;
    struct Guard(*mut session_sys::ChangesetIter);
    impl Drop for Guard {
        fn drop(&mut self) {
            unsafe { session_sys::sqlite3changeset_finalize(self.0) };
        }
    }
    let _guard = Guard(iter);

    let mut order: Vec<Coalesced> = Vec::new();
    let mut index: HashMap<(String, Vec<u8>), usize> = HashMap::new();
    loop {
        // SAFETY: `iter` is live until the guard drops.
        let rc = unsafe { session_sys::sqlite3changeset_next(iter) };
        if rc == session_sys::SQLITE_DONE {
            break;
        }
        if rc != session_sys::SQLITE_ROW {
            return Err(format!("murmur: changeset_next failed rc={rc}"));
        }
        let (mut tab, mut ncol, mut op, mut _indirect) = (std::ptr::null(), 0, 0, 0);
        // SAFETY: out-pointers are valid locals; `iter` is live.
        check(
            unsafe {
                session_sys::sqlite3changeset_op(iter, &mut tab, &mut ncol, &mut op, &mut _indirect)
            },
            "changeset_op",
        )?;
        if tab.is_null() {
            return Err("murmur: changeset op without table".into());
        }
        let table_name = unsafe { CStr::from_ptr(tab) }
            .to_str()
            .map_err(|_| "murmur: invalid table name in changeset")?;
        let (table_idx, table) = schema
            .tables
            .iter()
            .enumerate()
            .find(|(_, t)| t.name == table_name)
            .ok_or_else(|| format!("murmur: unknown changeset table '{table_name}'"))?;
        if ncol as usize != table.columns.len() {
            return Err("murmur: changeset/schema column mismatch".into());
        }
        // Primary-key shape must match the adopted schema exactly.
        let (mut pk_ptr, mut pk_n) = (std::ptr::null_mut(), 0);
        // SAFETY: out-pointers valid; the pk array borrows from the iterator
        // and is copied before the next iterator call.
        check(
            unsafe { session_sys::sqlite3changeset_pk(iter, &mut pk_ptr, &mut pk_n) },
            "changeset_pk",
        )?;
        if pk_ptr.is_null() || pk_n as usize != table.columns.len() {
            return Err("murmur: changeset pk shape mismatch".into());
        }
        let pk = unsafe { std::slice::from_raw_parts(pk_ptr as *const u8, pk_n as usize) };
        if pk
            .iter()
            .zip(&table.columns)
            .any(|(flag, c)| (*flag != 0) != (c.primary_key > 0))
        {
            return Err("murmur: changeset primary-key mismatch".into());
        }
        let value_at = |i: usize, is_insert: bool| -> Result<Value, String> {
            let mut out: *mut rusqlite::ffi::sqlite3_value = std::ptr::null_mut();
            // SAFETY: `iter` live; `out` a valid local.
            let rc = unsafe {
                if is_insert {
                    session_sys::sqlite3changeset_new(iter, i as c_int, &mut out)
                } else {
                    session_sys::sqlite3changeset_old(iter, i as c_int, &mut out)
                }
            };
            check(rc, "changeset value")?;
            if out.is_null() {
                return Err("murmur: missing key value in changeset".into());
            }
            value_from_sqlite(out)
        };
        let is_insert = op == session_sys::SQLITE_INSERT_OP;
        let key: Vec<Value> = murmur_crdt::schema::pk_columns(table)
            .iter()
            .map(|col| {
                let i = table.columns.iter().position(|c| c.id == col.id).unwrap();
                value_at(i, is_insert)
            })
            .collect::<Result<_, _>>()?;
        if key.iter().any(|v| matches!(v, Value::Null)) {
            return Err("murmur: null changeset primary key".into());
        }
        let key = murmur_crdt::model::encode(&key).map_err(|e| e.to_string())?;
        let slot = match index.get(&(table.id.clone(), key.clone())) {
            Some(&slot) => slot,
            None => {
                let slot = order.len();
                index.insert((table.id.clone(), key.clone()), slot);
                order.push(Coalesced {
                    table: table_idx,
                    key,
                    first_insert: is_insert,
                    last_delete: false,
                    last_new: vec![None; table.columns.len()],
                });
                slot
            }
        };
        let entry = &mut order[slot];
        if op == session_sys::SQLITE_INSERT_OP {
            for (i, cell) in entry.last_new.iter_mut().enumerate() {
                *cell = Some(value_at(i, true)?);
            }
            entry.last_delete = false;
        } else if op == session_sys::SQLITE_UPDATE_OP {
            for (i, cell) in entry.last_new.iter_mut().enumerate() {
                let mut out: *mut rusqlite::ffi::sqlite3_value = std::ptr::null_mut();
                // SAFETY: `iter` live; `out` a valid local.
                let rc = unsafe {
                    session_sys::sqlite3changeset_new(iter, i as c_int, &mut out)
                };
                check(rc, "changeset new value")?;
                // Unchanged columns come back NULL (not an error).
                if !out.is_null() {
                    *cell = Some(value_from_sqlite(out)?);
                }
            }
            entry.last_delete = false;
        } else if op == session_sys::SQLITE_DELETE_OP {
            entry.last_delete = true;
        } else {
            return Err(format!("murmur: unsupported changeset action {op}"));
        }
    }

    // Second pass: emit one net-effect op per row, in first-seen order.
    let mut delta = Delta::empty();
    for entry in &order {
        let table = &schema.tables[entry.table];
        let row_key = murmur_crdt::meta::row_key(&table.id, &entry.key);
        let mut old: Option<murmur_crdt::meta::RowMeta> = store
            .get(&row_key)
            .map_err(|e| e.to_string())?
            .map(|b| murmur_crdt::model::decode(&b))
            .transpose()
            .map_err(|e| e.to_string())?;
        if let Some(meta) = old.as_mut() {
            meta.extend_nullable(table).map_err(|e| e.to_string())?;
        }
        let decoded_key: Vec<Value> = murmur_crdt::model::decode(&entry.key)
            .map_err(|e| format!("murmur: corrupt changeset key: {e}"))?;
        if entry.last_delete {
            // Deleted by batch end: a row that never existed at batch start
            // (INSERT then DELETE) has no net effect.
            if let Some(old) = old {
                let tomb = Tombstone {
                    table: table.id.clone(),
                    key: entry.key.clone(),
                    generation: old.generation,
                    version,
                };
                let tomb_key =
                    murmur_crdt::meta::tomb_key(&tomb).map_err(|e| e.to_string())?;
                let bytes = murmur_crdt::model::encode(&tomb).map_err(|e| e.to_string())?;
                delta.mutations.push(spool::Mutation::put(tomb_key, bytes));
                delta.mutations.push(spool::Mutation::delete(row_key));
                delta.tombstones.push(tomb);
            }
            continue;
        }
        if let Some(mut meta) = old {
            // Net UPDATE: keep the generation, stamp touched cells.
            for (i, c) in table.columns.iter().enumerate() {
                if let Some(value) = entry.last_new[i].clone() {
                    let cell = meta
                        .cells
                        .iter_mut()
                        .find(|cell| cell.column == c.id)
                        .ok_or("murmur: changeset/schema column mismatch")?;
                    cell.version = version;
                    cell.value = value;
                }
            }
            let bytes = murmur_crdt::model::encode(&meta).map_err(|e| e.to_string())?;
            delta.mutations.push(spool::Mutation::put(row_key, bytes));
            delta.rows.push(RowState {
                table: table.id.clone(),
                key: decoded_key,
                generation: meta.generation,
                cells: meta.cells.clone(),
            });
            continue;
        }
        // New to this batch: the first op must have been an INSERT carrying
        // every column. Anything else means metadata is missing (a table
        // enabled without backfill, or a reordered batch).
        if !entry.first_insert {
            return Err("murmur: update has no row metadata (re-enable the table)".into());
        }
        let cells: Vec<Cell> = table
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                Ok(Cell {
                    column: c.id.clone(),
                    value: entry.last_new[i]
                        .clone()
                        .ok_or("murmur: insert misses column values")?,
                    version,
                })
            })
            .collect::<Result<_, String>>()?;
        let meta = murmur_crdt::meta::RowMeta {
            key: entry.key.clone(),
            generation: version,
            cells,
        };
        let bytes = murmur_crdt::model::encode(&meta).map_err(|e| e.to_string())?;
        delta.mutations.push(spool::Mutation::put(row_key, bytes));
        delta.rows.push(RowState {
            table: table.id.clone(),
            key: decoded_key,
            generation: meta.generation,
            cells: meta.cells.clone(),
        });
    }
    Ok(delta)
}
