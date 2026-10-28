//! Natural-key schema mapping: user tables become stable CRDT identities.
//!
//! Unlike the in-memory engine (managed UUID keys + rewritten tables), the
//! extension maps each table's *existing* primary key. Stable IDs derive
//! deterministically from names, and the adopted schema persists in Spool so
//! reopens reuse identical IDs. Consequences, by design:
//! - Renaming a table or column orphan its old ID: the renamed object
//!   replicates as new. Disable sync around DDL.
//! - Enabling a table backfills metadata for all current rows (one version,
//!   one atomic Spool commit), so later UPDATEs always find row metadata.
//! - Tables without a primary key and virtual tables are rejected: sessions
//!   cannot capture them into usable changesets.

use crate::session_sys;
use murmur_crdt::model::{Column, MergePolicy, RowState, Schema, Table, Value, Version};
use std::ffi::CString;

pub const SCHEMA_KEY: &[u8] = b"00/schema/ext-v1";
/// Local opt-in flags, stored apart like the engine's table-flags record:
/// the `Schema` wire codec intentionally excludes them.
pub const FLAGS_KEY: &[u8] = b"00/schema/ext-flags-v1";

pub fn table_id(name: &str) -> String {
    let digest = murmur_crdt::digest::hash_hex(
        format!("murmur/ext/table/v1\0{}", name.to_ascii_lowercase()).as_bytes(),
    );
    format!("ext-{:.16}", digest)
}

fn column_id(table: &str, column: &str) -> String {
    format!("{table}/{}", column.to_ascii_lowercase())
}

/// Adoptable user table or a precise rejection reason.
pub fn inspect_table(conn: &rusqlite::Connection, name: &str) -> Result<Table, String> {
    if name == crate::outbox::OUTBOX_TABLE || name.to_ascii_lowercase().starts_with("sqlite_") {
        return Err(format!("murmur: cannot sync internal table '{name}'"));
    }
    let (kind, sql): (String, Option<String>) = conn
        .query_row(
            "SELECT type, sql FROM main.sqlite_master WHERE name = ?1",
            rusqlite::params![name],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(|_| {
            format!("murmur: '{name}' is not a table in the main schema (missing, TEMP, or attached)")
        })?;
    if kind != "table" {
        return Err(format!("murmur: '{name}' is not a table ({kind})"));
    }
    let sql = sql.unwrap_or_default();
    let head: Vec<String> = sql
        .split_whitespace()
        .take(3)
        .map(|w| w.to_ascii_uppercase())
        .collect();
    if head == ["CREATE", "VIRTUAL", "TABLE"] {
        return Err(format!(
            "murmur: cannot sync virtual table '{name}' (sessions cannot capture virtual tables)"
        ));
    }
    let pragma = format!("PRAGMA table_info({})", murmur_crdt::model::quote(name));
    let mut stmt = conn.prepare(&pragma).map_err(|e| e.to_string())?;
    let info = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, bool>(3)?,
                r.get::<_, i64>(5)?,
            ))
        })
        .map_err(|e| e.to_string())?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|e| e.to_string())?;
    if info.is_empty() {
        return Err(format!("murmur: table '{name}' has no columns"));
    }
    if info.iter().all(|(_, _, _, pk)| *pk == 0) {
        return Err(format!(
            "murmur: table '{name}' requires a PRIMARY KEY for session capture"
        ));
    }
    let id = table_id(name);
    Ok(Table {
        application_key: None,
        id: id.clone(),
        name: name.to_string(),
        sql,
        columns: info
            .into_iter()
            .map(|(col_name, declared_type, not_null, pk)| Column {
                vector: None,
                fts: false,
                policy: MergePolicy::Lww,
                id: column_id(&id, &col_name),
                name: col_name,
                primary_key: pk as usize,
                declared_type,
                not_null,
            })
            .collect(),
    })
}

pub fn load_schema(store: &murmur_crdt::logical::LogicalStore) -> Result<Schema, String> {
    let mut schema: Schema =
        match store.get(SCHEMA_KEY).map_err(|e| e.to_string())? {
            Some(bytes) => murmur_crdt::model::decode(&bytes).map_err(|e| e.to_string())?,
            None => Schema::default(),
        };
    schema.table_replication =
        match store.get(FLAGS_KEY).map_err(|e| e.to_string())? {
            Some(bytes) => murmur_crdt::model::decode(&bytes).map_err(|e| e.to_string())?,
            None => std::collections::BTreeMap::new(),
        };
    Ok(schema)
}

pub fn save_schema(
    store: &murmur_crdt::logical::LogicalStore,
    schema: &mut Schema,
) -> Result<(), String> {
    schema.history = murmur_crdt::digest::hash(
        &murmur_crdt::model::encode(&schema.tables).map_err(|e| e.to_string())?,
    );
    let schema_bytes = murmur_crdt::model::encode(&schema).map_err(|e| e.to_string())?;
    let flag_bytes = murmur_crdt::model::encode(&schema.table_replication)
        .map_err(|e| e.to_string())?;
    store
        .commit(
            &[
                spool::Mutation::put(SCHEMA_KEY.to_vec(), schema_bytes),
                spool::Mutation::put(FLAGS_KEY.to_vec(), flag_bytes),
            ],
            spool::Durability::Sync,
        )
        .map_err(|e| e.to_string())
}

/// Scan every current row into row metadata: generation zero (the shared
/// pre-adoption incarnation — deterministic across nodes so tombstone
/// fencing compares incarnations, not backfill timing), cells stamped with
/// the fresh `version` (adoption recency wins value conflicts, so enabling
/// never silently discards local data to older synced state).
/// Rows with NULL in any primary-key column are skipped: sessions never
/// record them either, so capture and backfill agree.
pub fn scan_metas(
    conn: &rusqlite::Connection,
    table: &Table,
    version: Version,
) -> Result<Vec<murmur_crdt::meta::RowMeta>, String> {
    let pk: Vec<&Column> = murmur_crdt::schema::pk_columns(table);
    let select = format!(
        "SELECT {} FROM {}",
        table
            .columns
            .iter()
            .map(|c| murmur_crdt::model::quote(&c.name))
            .collect::<Vec<_>>()
            .join(","),
        murmur_crdt::model::quote(&table.name),
    );
    let mut stmt = conn.prepare(&select).map_err(|e| e.to_string())?;
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
    let mut metas = Vec::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        let values: Vec<Value> = (0..table.columns.len())
            .map(|i| {
                Value::read(row.get_ref(i).map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())
            })
            .collect::<Result<_, _>>()?;
        let key: Vec<Value> = pk
            .iter()
            .map(|c| {
                let i = table.columns.iter().position(|t| t.id == c.id).unwrap();
                values[i].clone()
            })
            .collect();
        if key.iter().any(|v| matches!(v, Value::Null)) {
            continue;
        }
        metas.push(murmur_crdt::meta::RowMeta {
            key: murmur_crdt::model::encode(&key).map_err(|e| e.to_string())?,
            generation: Version::default(),
            cells: table
                .columns
                .iter()
                .zip(values)
                .map(|(c, value)| murmur_crdt::model::Cell {
                    column: c.id.clone(),
                    value,
                    version,
                })
                .collect(),
        });
    }
    Ok(metas)
}

/// Build the backfill commit (row metas + clock bump) plus the delta rows
/// for the replication log. The caller adds log puts and commits once, so
/// adopted state and its log entry are atomic.
pub fn build_backfill(
    store: &murmur_crdt::logical::LogicalStore,
    table: &Table,
    version: Version,
    metas: Vec<murmur_crdt::meta::RowMeta>,
) -> Result<(Vec<spool::Mutation>, Vec<RowState>, Version), String> {
    if metas.is_empty() {
        return Ok((Vec::new(), Vec::new(), version));
    }
    let mut puts = Vec::with_capacity(metas.len() + 1);
    let mut rows = Vec::with_capacity(metas.len());
    for meta in &metas {
        let row_key = murmur_crdt::meta::row_key(&table.id, &meta.key);
        let bytes = murmur_crdt::model::encode(meta).map_err(|e| e.to_string())?;
        puts.push(spool::Mutation::put(row_key, bytes));
        rows.push(RowState {
            table: table.id.clone(),
            key: murmur_crdt::model::decode(&meta.key).map_err(|e| e.to_string())?,
            generation: meta.generation,
            cells: meta.cells.clone(),
        });
    }
    // The backfill version must survive restarts exactly like drained batch
    // versions: same atomic group, or a restart could re-mint it for new
    // data and poison merges with same-version-different-value cells.
    let clock = crate::outbox::read_clock(store)?.max(version);
    puts.push(spool::Mutation::put(
        crate::outbox::CLOCK_KEY.to_vec(),
        murmur_crdt::model::encode(&clock).map_err(|e| e.to_string())?,
    ));
    Ok((puts, rows, clock))
}

/// Enable sync for a table: drain pending work, attach session capture,
/// backfill current rows under an EXCLUSIVE lock, persist the adoption.
/// Idempotent: re-enabling re-backfills (newer version, current values) and
/// returns the existing stable ID. Must run outside a transaction.
pub fn enable(
    conn: &rusqlite::Connection,
    core: &mut crate::state::Core,
    db_key: usize,
    table_name: &str,
) -> Result<String, String> {
    if !conn.is_autocommit() {
        return Err("murmur: murmur_enable_sync must run outside a transaction".into());
    }
    let table = inspect_table(conn, table_name)?;
    let (store, db_path, spool_path, vfs, already) = {
        let state = core
            .conns
            .get(&db_key)
            .ok_or("murmur: call murmur_open first")?;
        (
            state.spool.clone(),
            state.db_path.clone(),
            state.spool_path.clone(),
            state.vfs.clone(),
            state.attached.iter().any(|t| t == &table.name),
        )
    };
    crate::outbox::drain_all(core, &store, &db_path, vfs.as_deref())?;
    let version = {
        let entry = core
            .spools
            .get_mut(&spool_path)
            .ok_or("murmur: spool released")?;
        crate::outbox::tick(&entry.store, &mut entry.last_tick)?
    };
    if !already {
        // SAFETY: under CORE on the calling thread; the session belongs to
        // this live connection.
        let state = core.conns.get_mut(&db_key).unwrap();
        let name =
            CString::new(table.name.clone()).map_err(|_| "murmur: NUL in table name")?;
        let rc = unsafe { session_sys::sqlite3session_attach(state.session.as_ptr(), name.as_ptr()) };
        if rc != session_sys::SQLITE_OK {
            return Err(format!("murmur: session attach failed rc={rc}"));
        }
        state.attached.push(table.name.clone());
    }
    conn.execute_batch("BEGIN EXCLUSIVE")
        .map_err(|e| format!("murmur: exclusive lock failed: {e}"))?;
    let scanned = scan_metas(conn, &table, version);
    let release = conn.execute_batch("COMMIT");
    let metas = scanned?;
    release.map_err(|e| format!("murmur: backfill scan release failed: {e}"))?;
    let mut schema = load_schema(&store)?;
    if !schema.tables.iter().any(|t| t.id == table.id) {
        schema.tables.push(table.clone());
    }
    schema
        .table_replication
        .insert(table.id.clone(), true);
    let (mut puts, rows, clock) = build_backfill(&store, &table, version, metas)?;
    if !puts.is_empty() {
        let norm = crate::repl::normalize_schema(&schema);
        puts.extend(crate::repl::sign_log(
            &store,
            &norm,
            crate::sync::database_id(&store)?,
            crate::outbox::node_id(&store)?,
            &crate::repl::read_signkey(&store)?,
            clock,
            rows,
            Vec::new(),
        )?);
        store
            .commit(&puts, spool::Durability::Sync)
            .map_err(|e| e.to_string())?;
    }
    save_schema(&store, &mut schema)?;
    Ok(table.id)
}

/// Disable sync: flag off, persist, and rebuild the session without the
/// table (sessions cannot detach one table).
pub fn disable(
    core: &mut crate::state::Core,
    db_key: usize,
    table_name: &str,
) -> Result<(), String> {
    let state = core
        .conns
        .get_mut(&db_key)
        .ok_or("murmur: call murmur_open first")?;
    let mut schema = load_schema(&state.spool)?;
    let id = schema
        .tables
        .iter()
        .find(|t| t.name == table_name)
        .map(|t| t.id.clone())
        .ok_or_else(|| format!("murmur: table '{table_name}' is not synced"))?;
    schema.table_replication.insert(id, false);
    let store = state.spool.clone();
    save_schema(&store, &mut schema)?;
    state.attached.retain(|t| t != table_name);
    let remaining = state.attached.clone();
    // SAFETY: under CORE on the calling thread; the old session is dropped
    // (deleted) before the replacement is created on the same connection.
    let db = state.db;
    let fresh =
        unsafe { session_sys::Session::create_attach_list(db, remaining.iter().map(String::as_str)) }
            .map_err(|rc| format!("murmur: session rebuild failed rc={rc}"))?;
    state.session = fresh;
    Ok(())
}
