//! Outbox durability: every collected batch lands in a SQLite outbox row in
//! the *same* transaction as the user writes, then drains into Spool.
//!
//! Protocol invariants:
//! - `collect` drains previously committed rows *before* ticking, so batch
//!   versions stay monotonic even with concurrent connections.
//! - Drain reads through a dedicated connection, which only ever sees
//!   committed rows; uncommitted work can never reach Spool.
//! - Each outbox row applies as one atomic Spool group (metadata + clock
//!   bump) and is acknowledged (deleted) only after. Re-drain after a crash
//!   between commit and ack rewrites identical bytes: idempotent.
//! - The HLC is assigned at collection and stored on the outbox row, so
//!   retries and restarts preserve the original version (Murmur rule).

use murmur_crdt::logical::LogicalStore;
use murmur_crdt::model::Version;

pub const OUTBOX_TABLE: &str = "_murmur_outbox";
const OUTBOX_DDL: &str = "CREATE TABLE IF NOT EXISTS _murmur_outbox(\
    seq INTEGER PRIMARY KEY,\
    millis INTEGER NOT NULL,\
    logical INTEGER NOT NULL,\
    node BLOB NOT NULL,\
    changeset BLOB NOT NULL\
    )";
pub const CLOCK_KEY: &[u8] = b"00/clock/ext-v1";
pub const NODE_KEY: &[u8] = b"00/node-id/ext-v1";
const DRAIN_BATCH: i64 = 50;

pub fn ensure(conn: &rusqlite::Connection) -> Result<(), String> {
    conn.execute_batch(OUTBOX_DDL).map_err(|e| e.to_string())
}

pub fn insert(
    conn: &rusqlite::Connection,
    version: &Version,
    changeset: &[u8],
) -> Result<i64, String> {
    conn.execute(
        "INSERT INTO _murmur_outbox(millis, logical, node, changeset) VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![version.millis as i64, version.logical, version.node.as_slice(), changeset],
    )
    .map_err(|e| e.to_string())?;
    Ok(conn.last_insert_rowid())
}

/// This spool's node identity, created once and stable thereafter.
pub fn node_id(store: &LogicalStore) -> Result<[u8; 16], String> {
    if let Some(bytes) = store.get(NODE_KEY).map_err(|e| e.to_string())? {
        return bytes
            .try_into()
            .map_err(|_| "murmur: corrupt node identity".to_string());
    }
    let id: [u8; 16] = rand::random();
    store
        .commit(
            &[spool::Mutation::put(NODE_KEY.to_vec(), id.to_vec())],
            spool::Durability::Sync,
        )
        .map_err(|e| e.to_string())?;
    Ok(id)
}

pub fn read_clock(store: &LogicalStore) -> Result<Version, String> {
    match store.get(CLOCK_KEY).map_err(|e| e.to_string())? {
        Some(bytes) => murmur_crdt::model::decode(&bytes).map_err(|e| e.to_string()),
        None => Ok(Version::default()),
    }
}

/// Tick the clock above both the persisted clock and every version minted
/// since (committed or not). The tick itself is *not* persisted here: drain
/// commits persist `max(clock, batch)` atomically with each batch's metadata.
/// Must run under CORE: the `last_tick` update and the tick are one unit.
pub fn tick(store: &LogicalStore, last_tick: &mut Version) -> Result<Version, String> {
    let node = node_id(store)?;
    let version = read_clock(store)?
        .max(*last_tick)
        .next(node)
        .map_err(|e| e.to_string())?;
    *last_tick = version;
    Ok(version)
}

/// Drain every committed outbox row, oldest first. Safe to call anytime:
/// idempotent, and a no-op when no rows await.
///
/// The ack `DELETE` runs fail-fast: when the caller holds the write lock
/// (collect inside a transaction after earlier undrained commits), acks
/// defer to a later drain while processing continues, so ticks and exports
/// stay fresh. Deferred rows are reprocessed idempotently and acked once
/// the lock is free.
///
/// Each drained batch also signs into the replication log in the same
/// atomic commit (log-from-birth: transport may never run, but history is
/// never lost). The `&Core` borrow proves the caller holds `CORE`: sequence
/// assignment is read-modify-write and concurrent drains would fork it.
pub fn drain_all(
    _core: &crate::state::Core,
    store: &LogicalStore,
    db_path: &str,
    vfs: Option<&str>,
) -> Result<u64, String> {
    let drain = crate::db::open_helper(db_path, vfs)?;
    drain
        .busy_timeout(std::time::Duration::from_secs(10))
        .map_err(|e| e.to_string())?;
    // The outbox may not exist yet (fresh database, nothing enabled).
    let has_outbox: bool = drain
        .query_row(
            "SELECT COUNT(*) FROM main.sqlite_master WHERE type = 'table' AND name = '_murmur_outbox'",
            [],
            |r| r.get::<_, i64>(0),
        )
        .map_err(|e| e.to_string())?
        > 0;
    if !has_outbox {
        return Ok(0);
    }
    let schema = crate::schema_map::load_schema(store)?;
    let norm_schema = crate::repl::normalize_schema(&schema);
    // Signing identity, loaded lazily so empty drains stay cheap.
    let mut signer: Option<([u8; 16], [u8; 16], [u8; 32])> = None;
    let mut last_seq: i64 = 0;
    let mut drained = 0u64;
    loop {
        let mut stmt = drain
            .prepare(
                "SELECT seq, millis, logical, node, changeset FROM _murmur_outbox \
                 WHERE seq > ?1 ORDER BY seq LIMIT 50",
            )
            .map_err(|e| e.to_string())?;
        let batch: Vec<(i64, Version, Vec<u8>)> = stmt
            .query_map([last_seq], |r| {
                let node: Vec<u8> = r.get(3)?;
                let node: [u8; 16] = node.try_into().map_err(|_| {
                    rusqlite::Error::FromSqlConversionFailure(
                        16,
                        rusqlite::types::Type::Blob,
                        "outbox node identity must be 16 bytes".into(),
                    )
                })?;
                Ok((
                    r.get::<_, i64>(0)?,
                    Version {
                        millis: r.get::<_, i64>(1)? as u64,
                        logical: r.get::<_, u32>(2)?,
                        node,
                    },
                    r.get::<_, Vec<u8>>(4)?,
                ))
            })
            .map_err(|e| e.to_string())?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|e| e.to_string())?;
        if batch.is_empty() {
            break;
        }
        // Once an ack defers, the remaining acks in this drain would fail
        // the same way; skip straight to processing so Spool stays complete.
        let mut ack_live = true;
        for (seq, version, changeset) in &batch {
            let delta = crate::delta::build(store, &schema, changeset, *version)?;
            let empty = delta.is_empty();
            let crate::delta::Delta {
                mut mutations,
                rows,
                tombstones,
            } = delta;
            let clock = read_clock(store)?.max(*version);
            mutations.push(spool::Mutation::put(
                CLOCK_KEY.to_vec(),
                murmur_crdt::model::encode(&clock).map_err(|e| e.to_string())?,
            ));
            if !empty {
                if signer.is_none() {
                    signer = Some((
                        crate::sync::database_id(store)?,
                        node_id(store)?,
                        crate::repl::read_signkey(store)?,
                    ));
                }
                let (db, node, signkey) = *signer.as_ref().unwrap();
                mutations.extend(crate::repl::sign_log(
                    store,
                    &norm_schema,
                    db,
                    node,
                    &signkey,
                    clock,
                    rows,
                    tombstones,
                )?);
            }
            store
                .commit(&mutations, spool::Durability::Sync)
                .map_err(|e| format!("murmur: spool commit failed for outbox seq {seq}: {e}"))?;
            if ack_live {
                drain
                    .busy_timeout(std::time::Duration::ZERO)
                    .map_err(|e| e.to_string())?;
                let ack = drain.execute("DELETE FROM _murmur_outbox WHERE seq = ?1", [seq]);
                drain
                    .busy_timeout(std::time::Duration::from_secs(10))
                    .map_err(|e| e.to_string())?;
                match ack {
                    Ok(_) => {}
                    Err(rusqlite::Error::SqliteFailure(err, _))
                        if err.code == rusqlite::ErrorCode::DatabaseBusy =>
                    {
                        ack_live = false;
                    }
                    Err(e) => return Err(e.to_string()),
                }
            }
            last_seq = *seq;
            drained += 1;
        }
        if (batch.len() as i64) < DRAIN_BATCH {
            break;
        }
    }
    Ok(drained)
}

/// Collect the connection's pending session changes into the caller's
/// transaction. Returns the outbox seq, or `None` when nothing changed.
pub fn collect(
    conn: &rusqlite::Connection,
    core: &mut crate::state::Core,
    db_key: usize,
) -> Result<Option<i64>, String> {
    let (store, db_path, spool_path, vfs) = {
        let state = core
            .conns
            .get(&db_key)
            .ok_or("murmur: call murmur_open first")?;
        (
            state.spool.clone(),
            state.db_path.clone(),
            state.spool_path.clone(),
            state.vfs.clone(),
        )
    };
    // Catch up first: the new tick must exceed every committed version.
    drain_all(core, &store, &db_path, vfs.as_deref())?;
    let version = {
        let entry = core
            .spools
            .get_mut(&spool_path)
            .ok_or("murmur: spool released")?;
        tick(&entry.store, &mut entry.last_tick)?
    };
    let bytes = {
        let state = core.conns.get_mut(&db_key).unwrap();
        // SAFETY: under CORE on the calling thread; the session belongs to
        // this live connection.
        unsafe { state.session.changeset_bytes() }
            .map_err(|rc| format!("murmur: changeset extraction failed rc={rc}"))?
    };
    if bytes.is_empty() {
        return Ok(None);
    }
    let seq = insert(conn, &version, &bytes)?;
    // Reset capture immediately: the batch is now the outbox's
    // responsibility. If session recreation fails, the old session (still
    // holding these changes) stays, and the next collect re-inserts them
    // under a fresh version — same values, harmless.
    let state = core.conns.get_mut(&db_key).unwrap();
    let attached = state.attached.clone();
    // SAFETY: same-thread session replacement on a live connection.
    let fresh = unsafe {
        crate::session_sys::Session::create_attach_list(
            state.db,
            attached.iter().map(String::as_str),
        )
    }
    .map_err(|rc| format!("murmur: session reset failed rc={rc}"))?;
    state.session = fresh;
    Ok(Some(seq))
}
