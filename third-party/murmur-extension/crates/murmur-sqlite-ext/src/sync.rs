//! Node-to-node merge: export local state, apply remote batches.
//!
//! Transport-agnostic by design: [`export_state`] serializes a Murmur
//! [`ReplicaState`](murmur_crdt::model::ReplicaState) (engine-identical wire
//! format) and [`apply_batch`] merges one back in. Phase 4 carries these
//! bytes over QUIC; until then tests exchange them directly.
//!
//! Receive path mirrors the engine (`network.rs`): validate compatibility,
//! filter to locally-enabled tables, normalize the incoming schema onto the
//! local one, [`merge::prepare`](murmur_crdt::merge::prepare) the patch, then
//! commit. Two deliberate divergences, both forced by independent adoption
//! (the engine converges table sets through managed DDL; extension nodes
//! adopt tables on their own):
//! - Tables unknown locally are *dropped* from the incoming batch (scope
//!   intersection), where the engine errors. Genuine divergence (same ID,
//!   different shape) still errors loudly.
//! - No `preserve_local` step: the local baseline reconstructs *all* adopted
//!   tables, so disabled rows ride through the join untouched.
//!
//! Crash order is Spool-first with a write-ahead journal: the metadata
//! commit carries the pending SQLite write-set, because re-merging after
//! the commit yields an empty patch (the baseline already has the rows) and
//! could never heal SQLite on its own. [`replay_journal`] runs at open and
//! at the top of every apply, completing any stranded write-set
//! idempotently. Merged data never re-ticks the clock; versions are
//! preserved verbatim, which is what makes re-application a no-op.

use murmur_crdt::logical::LogicalStore;
use murmur_crdt::model::{ReplicaState, RowState, Schema, Value, Version};

pub const DBID_KEY: &[u8] = b"00/database-id/ext-v1";
/// Pending SQLite write-set of the latest apply: `(patch.rows,
/// patch.deleted)` in [`RowState`](murmur_crdt::model::RowState) encoding.
/// Committed atomically with the apply's metadata, cleared once SQLite
/// confirms. Present only in the crash window (or a failed apply).
pub const JOURNAL_KEY: &[u8] = b"00/apply/journal/ext-v1";

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(text: &str) -> Result<Vec<u8>, String> {
    if text.len() % 2 != 0 {
        return Err("murmur: malformed hex".into());
    }
    (0..text.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&text[i..i + 2], 16).map_err(|_| "murmur: malformed hex".to_string())
        })
        .collect()
}

/// This spool's database identity, minted once. Nodes merge only with the
/// same identity; link nodes with [`set_database_id`].
pub fn database_id(store: &LogicalStore) -> Result<[u8; 16], String> {
    if let Some(bytes) = store.get(DBID_KEY).map_err(|e| e.to_string())? {
        return bytes
            .as_slice()
            .try_into()
            .map_err(|_| "murmur: corrupt database identity".to_string());
    }
    let id: [u8; 16] = rand::random();
    store
        .commit(
            &[spool::Mutation::put(DBID_KEY.to_vec(), id.to_vec())],
            spool::Durability::Sync,
        )
        .map_err(|e| e.to_string())?;
    Ok(id)
}

pub fn database_id_hex(store: &LogicalStore) -> Result<String, String> {
    Ok(hex(&database_id(store)?))
}

pub fn set_database_id(store: &LogicalStore, text: &str) -> Result<(), String> {
    let bytes = unhex(text)?;
    if bytes.len() != 16 {
        return Err("murmur: database id must be 32 hex chars".into());
    }
    let current = database_id(store)?;
    if bytes == current.to_vec() {
        return Ok(());
    }
    // Log entries bind the database id at sign time and are immutable: a
    // node with logged history cannot move databases without orphaning it.
    // Link nodes (adopt a shared id) before the first write.
    let node = crate::outbox::node_id(store)?;
    let logged = store
        .get(&murmur_replication::protocol::sequence_key(node))
        .map_err(|e| e.to_string())?
        .is_some();
    if logged {
        return Err("murmur: database id is locked once history is logged".into());
    }
    store
        .commit(
            &[spool::Mutation::put(DBID_KEY.to_vec(), bytes)],
            spool::Durability::Sync,
        )
        .map_err(|e| e.to_string())
}

/// Full local state: every adopted table, enabled or not. Disabled rows must
/// be present so the join carries them through untouched.
fn reconstruct(store: &LogicalStore, schema: &Schema) -> Result<ReplicaState, String> {
    let mut rows = Vec::new();
    for table in &schema.tables {
        let prefix = format!("04/meta/row/{}/", table.id);
        for key in store.keys_with_prefix(prefix.as_bytes()) {
            let bytes = store
                .get(&key)
                .map_err(|e| e.to_string())?
                .ok_or("murmur: metadata vanished mid-export")?;
            let meta: murmur_crdt::meta::RowMeta =
                murmur_crdt::model::decode(&bytes).map_err(|e| e.to_string())?;
            rows.push(RowState {
                table: table.id.clone(),
                key: murmur_crdt::model::decode(&meta.key).map_err(|e| e.to_string())?,
                generation: meta.generation,
                cells: meta.cells,
            });
        }
    }
    let mut tombstones = Vec::new();
    for key in store.keys_with_prefix(b"05/meta/tomb/") {
        let bytes = store
            .get(&key)
            .map_err(|e| e.to_string())?
            .ok_or("murmur: tombstone vanished mid-export")?;
        tombstones.push(murmur_crdt::model::decode(&bytes).map_err(|e| e.to_string())?);
    }
    Ok(ReplicaState {
        database_id: database_id(store)?,
        schema: schema.clone(),
        rows,
        tombstones,
        clock: crate::outbox::read_clock(store)?,
    })
}

/// Reconstruct + scope-filter without draining: the shared core of exports
/// and replication snapshots (both drain first through their own paths).
pub fn export_state_inner(
    store: &LogicalStore,
    schema: &Schema,
) -> Result<ReplicaState, String> {
    let mut state = reconstruct(store, schema)?;
    murmur_crdt::table_replication::filter(&mut state, schema);
    Ok(state)
}

/// Export enabled state as portable batch bytes. Drains first: export reads
/// Spool, so undrained outbox rows would otherwise be silently stale.
pub fn export_state(
    core: &crate::state::Core,
    store: &LogicalStore,
    db_path: &str,
    vfs: Option<&str>,
) -> Result<Vec<u8>, String> {
    crate::outbox::drain_all(core, store, db_path, vfs)?;
    let schema = crate::schema_map::load_schema(store)?;
    let state = export_state_inner(store, &schema)?;
    murmur_crdt::model::encode(&state).map_err(|e| e.to_string())
}

/// Complete a previously crashed or failed apply: if the journal holds a
/// pending SQLite write-set, run it and clear it. Idempotent (upserts and
/// deletes by key), so a second crash mid-replay simply replays again.
pub fn replay_journal(
    store: &LogicalStore,
    db_path: &str,
    vfs: Option<&str>,
) -> Result<(), String> {
    let Some(bytes) = store.get(JOURNAL_KEY).map_err(|e| e.to_string())? else {
        return Ok(());
    };
    let (rows, deleted): (Vec<RowState>, Vec<RowState>) = murmur_crdt::model::decode(&bytes)
        .map_err(|e| format!("murmur: bad apply journal: {e}"))?;
    let schema = crate::schema_map::load_schema(store)?;
    apply_sqlite(db_path, &schema, &rows, &deleted, vfs)?;
    store
        .commit(
            &[spool::Mutation::delete(JOURNAL_KEY.to_vec())],
            spool::Durability::Sync,
        )
        .map_err(|e| format!("murmur: apply journal clear failed: {e}"))?;
    Ok(())
}

/// Apply a remote batch. Returns `(rows_upserted, rows_deleted, tombstones)`.
pub fn apply_batch(
    store: &LogicalStore,
    db_path: &str,
    vfs: Option<&str>,
    bytes: &[u8],
) -> Result<(usize, usize, usize), String> {
    replay_journal(store, db_path, vfs)?;
    let mut remote: ReplicaState =
        murmur_crdt::model::decode(bytes).map_err(|e| format!("murmur: bad batch: {e}"))?;
    let schema = crate::schema_map::load_schema(store)?;
    let local = reconstruct(store, &schema)?;
    if remote.database_id != local.database_id {
        return Err(
            "murmur: database mismatch (link nodes with murmur_set_database_id)".into(),
        );
    }
    // Scope to locally-known tables; validate the rest strictly.
    for table in &remote.schema.tables {
        let Some(local_table) = schema.tables.iter().find(|t| t.id == table.id) else {
            continue;
        };
        if !murmur_crdt::table_replication::enabled(&schema, &table.id) {
            continue;
        }
        if local_table.name != table.name
            || table.columns.iter().any(|c| {
                !local_table.columns.iter().any(|l| {
                    let mut historical = c.clone();
                    historical.fts = l.fts;
                    historical.vector = l.vector.clone();
                    *l == historical
                })
            })
        {
            return Err(format!(
                "murmur: schema divergence on table '{}'",
                local_table.name
            ));
        }
    }
    let known = |id: &str| schema.tables.iter().any(|t| t.id == id);
    remote.rows.retain(|row| {
        known(&row.table) && murmur_crdt::table_replication::enabled(&schema, &row.table)
    });
    remote.tombstones.retain(|tomb| {
        known(&tomb.table) && murmur_crdt::table_replication::enabled(&schema, &tomb.table)
    });
    for row in &mut remote.rows {
        let table = schema
            .tables
            .iter()
            .find(|t| t.id == row.table)
            .ok_or("murmur: internal scope error")?;
        for column in &table.columns {
            if !row.cells.iter().any(|c| c.column == column.id) {
                if column.not_null || column.primary_key > 0 {
                    return Err(format!(
                        "murmur: remote row misses non-null column '{}'",
                        column.name
                    ));
                }
                row.cells.push(murmur_crdt::model::Cell {
                    column: column.id.clone(),
                    value: Value::Null,
                    version: Version::default(),
                });
            }
        }
    }
    remote.schema = schema.clone();
    let patch =
        murmur_crdt::merge::prepare(&local, &remote, 0).map_err(|e| e.to_string())?;
    if patch.rows.is_empty() && patch.deleted.is_empty() && patch.tombstones.is_empty() {
        if remote.clock <= local.clock {
            return Ok((0, 0, 0));
        }
        let bytes = murmur_crdt::model::encode(&remote.clock).map_err(|e| e.to_string())?;
        store
            .commit(
                &[spool::Mutation::put(
                    crate::outbox::CLOCK_KEY.to_vec(),
                    bytes,
                )],
                spool::Durability::Sync,
            )
            .map_err(|e| e.to_string())?;
        return Ok((0, 0, 0));
    }
    // Metadata first, carrying the crash journal: re-merging after this
    // commit yields an empty patch, so without the journal a crash before
    // SQLite application would strand versioned rows SQLite never sees.
    let mut mutations = Vec::new();
    for row in &patch.rows {
        let key = murmur_crdt::model::encode(&row.key).map_err(|e| e.to_string())?;
        let meta = murmur_crdt::meta::RowMeta {
            key,
            generation: row.generation,
            cells: row.cells.clone(),
        };
        let row_key = murmur_crdt::meta::row_key(&row.table, &meta.key);
        let bytes = murmur_crdt::model::encode(&meta).map_err(|e| e.to_string())?;
        mutations.push(spool::Mutation::put(row_key, bytes));
    }
    for row in &patch.deleted {
        let key = murmur_crdt::model::encode(&row.key).map_err(|e| e.to_string())?;
        let table = schema
            .tables
            .iter()
            .find(|t| t.id == row.table)
            .ok_or("murmur: internal scope error")?;
        mutations.push(spool::Mutation::delete(
            murmur_crdt::meta::row_key(&table.id, &key),
        ));
    }
    for tomb in &patch.tombstones {
        let key = murmur_crdt::meta::tomb_key(tomb).map_err(|e| e.to_string())?;
        let bytes = murmur_crdt::model::encode(tomb).map_err(|e| e.to_string())?;
        mutations.push(spool::Mutation::put(key, bytes));
    }
    let clock = local.clock.max(remote.clock).max(patch.clock);
    mutations.push(spool::Mutation::put(
        crate::outbox::CLOCK_KEY.to_vec(),
        murmur_crdt::model::encode(&clock).map_err(|e| e.to_string())?,
    ));
    let journal = murmur_crdt::model::encode(&(&patch.rows, &patch.deleted))
        .map_err(|e| e.to_string())?;
    mutations.push(spool::Mutation::put(JOURNAL_KEY.to_vec(), journal));
    store
        .commit(&mutations, spool::Durability::Sync)
        .map_err(|e| format!("murmur: apply metadata commit failed: {e}"))?;
    apply_sqlite(db_path, &schema, &patch.rows, &patch.deleted, vfs)?;
    store
        .commit(
            &[spool::Mutation::delete(JOURNAL_KEY.to_vec())],
            spool::Durability::Sync,
        )
        .map_err(|e| format!("murmur: apply journal clear failed: {e}"))?;
    Ok((patch.rows.len(), patch.deleted.len(), patch.tombstones.len()))
}

/// Apply patch rows to user tables on a dedicated connection: applied writes
/// never pass through any app connection's session, so they cannot loop back
/// into the outbox. One transaction, deletes before upserts.
///
/// Restriction: triggers firing on this connection whose side effects land
/// on other synced tables are NOT captured (no session here). Keep synced
/// tables trigger-free, or confined to side effects outside synced tables.
/// (Same-connection app writes *are* captured with their cascades.)
fn apply_sqlite(
    db_path: &str,
    schema: &Schema,
    rows: &[RowState],
    deleted: &[RowState],
    vfs: Option<&str>,
) -> Result<(), String> {
    let conn = crate::db::open_helper(db_path, vfs)?;
    conn.busy_timeout(std::time::Duration::from_secs(10))
        .map_err(|e| e.to_string())?;
    conn.execute_batch("BEGIN IMMEDIATE")
        .map_err(|e| format!("murmur: apply begin failed: {e}"))?;
    let result = (|| -> Result<(), String> {
        for row in deleted {
            let table = schema
                .tables
                .iter()
                .find(|t| t.id == row.table)
                .ok_or("murmur: internal scope error")?;
            let sql = format!(
                "DELETE FROM {} WHERE {}",
                murmur_crdt::model::quote(&table.name),
                murmur_crdt::schema::pk_columns(table)
                    .iter()
                    .map(|c| format!("{} IS ?", murmur_crdt::model::quote(&c.name)))
                    .collect::<Vec<_>>()
                    .join(" AND "),
            );
            conn.execute(&sql, rusqlite::params_from_iter(&row.key))
                .map_err(|e| e.to_string())?;
        }
        for row in rows {
            upsert_row(&conn, schema, row)?;
        }
        Ok(())
    })();
    match result {
        Ok(()) => conn
            .execute_batch("COMMIT")
            .map_err(|e| format!("murmur: apply commit failed: {e}")),
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}

fn upsert_row(
    conn: &rusqlite::Connection,
    schema: &Schema,
    row: &RowState,
) -> Result<(), String> {
    let table = schema
        .tables
        .iter()
        .find(|t| t.id == row.table)
        .ok_or("murmur: internal scope error")?;
    let pk = murmur_crdt::schema::pk_columns(table);
    // Patch cells map by ID onto schema order; merge guarantees completeness.
    let mut values: Vec<&Value> = Vec::with_capacity(table.columns.len());
    for column in &table.columns {
        values.push(
            row.cells
                .iter()
                .find(|c| c.column == column.id)
                .map(|c| &c.value)
                .ok_or("murmur: patch row misses column values")?,
        );
    }
    let names: Vec<String> = table
        .columns
        .iter()
        .map(|c| murmur_crdt::model::quote(&c.name))
        .collect();
    let placeholders = vec!["?"; names.len()].join(",");
    let conflict: Vec<String> = pk
        .iter()
        .map(|c| murmur_crdt::model::quote(&c.name))
        .collect();
    let updates: Vec<String> = table
        .columns
        .iter()
        .filter(|c| c.primary_key == 0)
        .map(|c| {
            format!(
                "{0}=excluded.{0}",
                murmur_crdt::model::quote(&c.name)
            )
        })
        .collect();
    let sql = if updates.is_empty() {
        format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT({}) DO NOTHING",
            murmur_crdt::model::quote(&table.name),
            names.join(","),
            placeholders,
            conflict.join(","),
        )
    } else {
        format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT({}) DO UPDATE SET {}",
            murmur_crdt::model::quote(&table.name),
            names.join(","),
            placeholders,
            conflict.join(","),
            updates.join(","),
        )
    };
    conn.execute(&sql, rusqlite::params_from_iter(values))
        .map_err(|e| format!("murmur: apply upsert failed on '{}': {e}", table.name))?;
    Ok(())
}

pub fn clock_json(store: &LogicalStore) -> Result<String, String> {
    let clock = crate::outbox::read_clock(store)?;
    Ok(serde_json::json!({
        "millis": clock.millis,
        "logical": clock.logical,
        "node": hex(&clock.node),
    })
    .to_string())
}
