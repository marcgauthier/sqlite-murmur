//! Murmur SQLite CRDT extension.
//!
//! Sessions capture changes, an in-database outbox makes collection crash-safe,
//! and Spool holds CRDT metadata (column versions, tombstones, clocks).
//!
//! SQL API:
//! - `murmur_open(spool_path)` / `murmur_open_keyed(spool_path, key32)` —
//!   attach replication state to this connection (file-backed DBs only).
//! - `murmur_enable_sync(table)` → stable table ID (backfills current rows).
//! - `murmur_disable_sync(table)`.
//! - `murmur_collect()` → outbox seq, or NULL when nothing changed. Call in
//!   the same transaction as the writes it must capture.
//! - `murmur_drain()` → rows applied to Spool.
//! - `murmur_sync_tables()` → JSON adoption list.
//! - `murmur_export()` → portable batch blob of enabled state.
//! - `murmur_apply(batch)` → JSON `{rows, deleted, tombstones}` summary.
//! - `murmur_database_id()` / `murmur_set_database_id(hex)` — node linking.
//! - `murmur_clock()` → JSON HLC position.
//! - `murmur_close()` — detach (also runs automatically at connection close).
//! - `murmur_debug_sessions()` → live session count (tests/diagnostics).

mod db;
mod delta;
mod outbox;
mod repl;
mod schema_map;
mod session_sys;
mod state;
mod sync;

use rusqlite::functions::{Context, FunctionFlags};
use rusqlite::types::{ToSqlOutput, Value};
use rusqlite::{Connection, Result};
use std::os::raw::{c_char, c_int, c_void};

/// Entry point for SQLite to load the extension.
#[cfg(feature = "loadable")]
#[no_mangle]
pub unsafe extern "C" fn sqlite3_extension_init(
    db: *mut rusqlite::ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut rusqlite::ffi::sqlite3_api_routines,
) -> c_int {
    Connection::extension_init2(db, pz_err_msg, p_api, extension_init)
}

/// Filename-derived entry point (`libmurmur_sqlite_ext.so` →
/// `sqlite3_murmur_sqlite_ext_init`), for hosts that pass an explicit `zProc`.
#[cfg(feature = "loadable")]
#[no_mangle]
pub unsafe extern "C" fn sqlite3_murmur_sqlite_ext_init(
    db: *mut rusqlite::ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut rusqlite::ffi::sqlite3_api_routines,
) -> c_int {
    Connection::extension_init2(db, pz_err_msg, p_api, extension_init)
}

/// Static-link entry point for hosts that link the archive directly
/// instead of loading the shared object. `db` must be a live connection;
/// `p_api` is used only by `loadable` builds (static builds call SQLite
/// directly). Errors are allocated with `sqlite3_malloc` for `sqlite3_free`.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_murmur_ext_init(
    db: *mut rusqlite::ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut rusqlite::ffi::sqlite3_api_routines,
) -> c_int {
    #[cfg(feature = "loadable")]
    if unsafe { rusqlite::ffi::rusqlite_extension_init2(p_api) }.is_err() {
        return rusqlite::ffi::SQLITE_ERROR;
    }
    #[cfg(not(feature = "loadable"))]
    let _ = p_api;
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // SAFETY: the host passes a live connection; registration borrows it.
        let conn = unsafe { rusqlite::Connection::from_handle(db) }?;
        extension_init(conn).map(|_| ())
    }));
    match outcome {
        Ok(Ok(())) => rusqlite::ffi::SQLITE_OK,
        failed => {
            let msg = match failed {
                Ok(Err(e)) => e.to_string(),
                _ => "murmur: initialization panic".to_string(),
            };
            if !pz_err_msg.is_null() {
                unsafe { *pz_err_msg = sqlite_error_string(&msg) };
            }
            rusqlite::ffi::SQLITE_ERROR
        }
    }
}

fn sqlite_error_string(msg: &str) -> *mut c_char {
    let bytes = msg.as_bytes();
    // SAFETY: host allocation, NUL-terminated below, freed with sqlite3_free.
    let ptr = unsafe { rusqlite::ffi::sqlite3_malloc(bytes.len() as c_int + 1) as *mut u8 };
    if ptr.is_null() {
        return std::ptr::null_mut();
    }
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr, bytes.len());
        *ptr.add(bytes.len()) = 0;
    }
    ptr as *mut c_char
}

/// Per-connection pin: SQLite runs `x_destroy` when the connection closes,
/// which drops murmur state even if the host never called `murmur_close`.
struct Pin {
    db: usize,
}

unsafe extern "C" fn x_pin(
    ctx: *mut rusqlite::ffi::sqlite3_context,
    _n: c_int,
    _args: *mut *mut rusqlite::ffi::sqlite3_value,
) {
    // Never called by users; exists only to carry the pin box.
    unsafe { rusqlite::ffi::sqlite3_result_null(ctx) };
}

unsafe extern "C" fn x_destroy(app: *mut c_void) {
    if app.is_null() {
        return;
    }
    let pin = unsafe { Box::from_raw(app as *mut Pin) };
    // Backstop: nothing to report a close error to; state is still evicted.
    let _ = state::remove_conn(&mut state::lock(), pin.db);
}

fn extension_init(db: Connection) -> Result<bool> {
    db.create_scalar_function(
        c"murmur_open",
        1,
        FunctionFlags::empty(),
        murmur_open,
    )?;
    db.create_scalar_function(
        c"murmur_open_keyed",
        2,
        FunctionFlags::empty(),
        murmur_open_keyed,
    )?;
    db.create_scalar_function(
        c"murmur_enable_sync",
        1,
        FunctionFlags::empty(),
        murmur_enable_sync,
    )?;
    db.create_scalar_function(
        c"murmur_disable_sync",
        1,
        FunctionFlags::empty(),
        murmur_disable_sync,
    )?;
    db.create_scalar_function(
        c"murmur_collect",
        0,
        FunctionFlags::empty(),
        murmur_collect,
    )?;
    db.create_scalar_function(c"murmur_drain", 0, FunctionFlags::empty(), murmur_drain)?;
    db.create_scalar_function(
        c"murmur_sync_tables",
        0,
        FunctionFlags::empty(),
        murmur_sync_tables,
    )?;
    db.create_scalar_function(c"murmur_export", 0, FunctionFlags::empty(), murmur_export)?;
    db.create_scalar_function(c"murmur_apply", 1, FunctionFlags::empty(), murmur_apply)?;
    db.create_scalar_function(
        c"murmur_log_export",
        2,
        FunctionFlags::empty(),
        murmur_log_export,
    )?;
    db.create_scalar_function(c"murmur_log_seq", 1, FunctionFlags::empty(), murmur_log_seq)?;
    db.create_scalar_function(
        c"murmur_log_rows",
        3,
        FunctionFlags::empty(),
        murmur_log_rows,
    )?;
    db.create_scalar_function(
        c"murmur_database_id",
        0,
        FunctionFlags::empty(),
        murmur_database_id,
    )?;
    db.create_scalar_function(
        c"murmur_set_database_id",
        1,
        FunctionFlags::empty(),
        murmur_set_database_id,
    )?;
    db.create_scalar_function(c"murmur_clock", 0, FunctionFlags::empty(), murmur_clock)?;
    db.create_scalar_function(c"murmur_close", 0, FunctionFlags::empty(), murmur_close)?;
    db.create_scalar_function(
        c"murmur_debug_sessions",
        0,
        FunctionFlags::empty(),
        murmur_debug_sessions,
    )?;
    db.create_scalar_function(
        c"murmur_repl_start",
        -1,
        FunctionFlags::empty(),
        murmur_repl_start,
    )?;
    db.create_scalar_function(
        c"murmur_repl_stop",
        0,
        FunctionFlags::empty(),
        murmur_repl_stop,
    )?;
    db.create_scalar_function(
        c"murmur_repl_add_peer",
        1,
        FunctionFlags::empty(),
        murmur_repl_add_peer,
    )?;
    db.create_scalar_function(
        c"murmur_repl_status",
        0,
        FunctionFlags::empty(),
        murmur_repl_status,
    )?;
    db.create_scalar_function(
        c"murmur_repl_sync",
        0,
        FunctionFlags::empty(),
        murmur_repl_sync,
    )?;
    db.create_scalar_function(
        c"murmur_repl_pubkey",
        0,
        FunctionFlags::empty(),
        murmur_repl_pubkey,
    )?;
    db.create_scalar_function(
        c"murmur_node_id",
        0,
        FunctionFlags::empty(),
        murmur_node_id,
    )?;
    db.create_scalar_function(
        c"murmur_repl_progress",
        0,
        FunctionFlags::empty(),
        murmur_repl_progress,
    )?;
    // Pin cleanup to connection close. Raw registration: rusqlite's builder
    // has no destructor slot.
    // SAFETY: `db` is the live loading connection; the pin box is adopted by
    // SQLite on success and reclaimed below on failure.
    let pin = Box::new(Pin {
        db: unsafe { db.handle() } as usize,
    });
    let raw = Box::into_raw(pin);
    let rc = unsafe {
        rusqlite::ffi::sqlite3_create_function_v2(
            db.handle(),
            c"_murmur_conn_pin".as_ptr(),
            0,
            rusqlite::ffi::SQLITE_UTF8,
            raw as *mut c_void,
            Some(x_pin),
            None,
            None,
            Some(x_destroy),
        )
    };
    if rc != rusqlite::ffi::SQLITE_OK {
        drop(unsafe { Box::from_raw(raw) });
        return Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rc),
            Some("murmur: connection pin registration failed".into()),
        ));
    }
    rusqlite::trace::log(rusqlite::ffi::SQLITE_WARNING, "murmur-sqlite-ext initialized");
    Ok(false)
}

fn fn_error(msg: String) -> rusqlite::Error {
    rusqlite::Error::UserFunctionError(Box::new(std::io::Error::other(msg)))
}

/// Named VFS of the calling connection, or `None` for the default VFS.
/// Only custom VFSes need naming: helper connections reach the default VFS
/// by opening the path plainly.
fn detect_vfs(conn: &Connection) -> Result<Option<String>, String> {
    let mut vfs: *mut rusqlite::ffi::sqlite3_vfs = std::ptr::null_mut();
    // SAFETY: `conn` is the live calling connection; the control only reads.
    let rc = unsafe {
        rusqlite::ffi::sqlite3_file_control(
            conn.handle(),
            c"main".as_ptr(),
            rusqlite::ffi::SQLITE_FCNTL_VFS_POINTER,
            (&mut vfs as *mut *mut rusqlite::ffi::sqlite3_vfs).cast(),
        )
    };
    if rc != rusqlite::ffi::SQLITE_OK || vfs.is_null() {
        return Err("murmur: cannot inspect database VFS".into());
    }
    // SAFETY: the VFS outlives the connection; `zName` is a valid C string.
    let name = unsafe { std::ffi::CStr::from_ptr((*vfs).zName) }
        .to_string_lossy()
        .into_owned();
    if name == "sqlite-murmur" {
        Ok(Some(name))
    } else {
        Ok(None)
    }
}

/// Connection-handle key for the calling context.
fn db_key(ctx: &Context) -> Result<usize> {
    // SAFETY: the handle identifies the calling connection; state transitions
    // happen under CORE on this thread.
    let conn = unsafe { ctx.get_connection()? };
    Ok(unsafe { conn.handle() } as usize)
}

fn murmur_open(ctx: &Context) -> Result<ToSqlOutput<'static>> {
    let path: String = ctx.get(0)?;
    open_impl(ctx, path, None).map_err(fn_error)?;
    Ok(ToSqlOutput::Owned(Value::Integer(0)))
}

fn murmur_open_keyed(ctx: &Context) -> Result<ToSqlOutput<'static>> {
    let path: String = ctx.get(0)?;
    let key: Vec<u8> = ctx.get(1)?;
    if key.len() != 32 {
        return Err(fn_error("murmur: key must be exactly 32 bytes".into()));
    }
    let mut key32 = [0u8; 32];
    key32.copy_from_slice(&key);
    open_impl(ctx, path, Some(key32)).map_err(fn_error)?;
    Ok(ToSqlOutput::Owned(Value::Integer(0)))
}

fn open_impl(ctx: &Context, path: String, key: Option<[u8; 32]>) -> Result<(), String> {
    let mut core = state::lock();
    let key_now = db_key(ctx).map_err(|e| e.to_string())?;
    if core.conns.contains_key(&key_now) {
        return Err("murmur: already open on this connection".into());
    }
    // SAFETY: borrowed handle of the calling connection for direct queries.
    let conn = unsafe { ctx.get_connection().map_err(|e| e.to_string())? };
    let db_handle = unsafe { conn.handle() };
    let db_path: String = conn
        .query_row(
            "SELECT file FROM pragma_database_list WHERE seq = 0",
            [],
            |r| r.get(0),
        )
        .map_err(|e| format!("murmur: database path lookup failed: {e}"))?;
    if db_path.is_empty() {
        return Err("murmur: file-backed databases only (:memory: has no stable path)".into());
    }
    // Named-VFS detection comes before the journal check: the
    // sqlite-murmur VFS is DELETE-only by design, so WAL is neither
    // requested nor required there. Elsewhere WAL is a hard requirement,
    // enforced before any state changes (so a refusal leaves zero trace)
    // and persistent once set: in rollback mode a large uncommitted write
    // transaction blocks second-connection reads for its whole duration
    // (observed at ~1MB+ on stock SQLite 3.45.1), which would wedge
    // drains, exports, and applies. Under WAL readers snapshot and never
    // block. The pragma is a silent no-op inside a transaction, hence the
    // read-back check with an actionable error.
    let vfs = detect_vfs(&conn)?;
    if vfs.as_deref() != Some("sqlite-murmur") {
        let mode: String = conn
            .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
            .map_err(|e| format!("murmur: WAL mode check failed: {e}"))?;
        if mode.to_lowercase() != "wal" {
            return Err(
                "murmur requires WAL mode (call murmur_open outside a transaction on first use)"
                    .into(),
            );
        }
    }
    let spool_path = if std::path::Path::new(&path).is_absolute() {
        std::path::PathBuf::from(&path)
    } else {
        std::env::current_dir()
            .map_err(|e| e.to_string())?
            .join(&path)
    };
    if let Some(existing) = core.db_spool.get(&db_path) {
        if *existing != spool_path {
            return Err("murmur: database already open with a different spool directory".into());
        }
    }
    // Strict 1:1: metadata keys carry no database qualifier, so two
    // databases sharing one spool would merge same-named tables.
    if let Some(other) = core
        .db_spool
        .iter()
        .find(|(db, sp)| db.as_str() != db_path.as_str() && sp.as_os_str() == spool_path.as_os_str())
        .map(|(db, _)| db.clone())
    {
        return Err(format!(
            "murmur: spool already open with a different database ({other})"
        ));
    }
    let fingerprint = key.map(|k| murmur_crdt::digest::hash(&k));
    let (store, fresh) = match core.spools.get_mut(&spool_path) {
        Some(entry) => {
            if entry.key_fingerprint != fingerprint {
                return Err("murmur: spool already open with different key material".into());
            }
            entry.refs += 1;
            (entry.store.clone(), false)
        }
        None => {
            let mut opts = spool::default_options(spool_path.to_string_lossy().to_string());
            opts.reclaim_interval = None;
            match key {
                Some(k) => opts.master_key = k.to_vec(),
                None => opts.encryption = spool::Encryption::None,
            }
            let store = murmur_crdt::logical::LogicalStore::new(
                spool::open(&opts).map_err(|e| format!("murmur: spool open failed: {e}"))?,
            );
            // Fail fast on unreadable state, and mint the node identities
            // (replication log signing starts at first drain, so the key
            // must exist before any drain runs).
            outbox::node_id(&store)?;
            repl::ensure_signkey(&store)?;
            core.spools.insert(
                spool_path.clone(),
                state::SpoolEntry {
                    store: store.clone(),
                    key_fingerprint: fingerprint,
                    refs: 1,
                    last_tick: murmur_crdt::model::Version::default(),
                    repl: None,
                },
            );
            core.db_spool.insert(db_path.clone(), spool_path.clone());
            (store, true)
        }
    };
    // Recovery plus session setup, rolled back as a unit on failure: a
    // failed open must not pin a spool reference or a db binding.
    let tail: Result<_, String> = (|| {
        // Recovery: complete any stranded apply first, then replay anything
        // committed but never drained.
        crate::sync::replay_journal(&store, &db_path, vfs.as_deref())?;
        outbox::drain_all(&core, &store, &db_path, vfs.as_deref())?;
        outbox::ensure(&conn)?;
        // Re-attach tables adopted before this process (reopen path).
        let schema = crate::schema_map::load_schema(&store)?;
        let attached: Vec<String> = schema
            .tables
            .iter()
            .filter(|t| {
                murmur_crdt::table_replication::enabled(&schema, &t.id)
            })
            .map(|t| t.name.clone())
            .collect();
        // SAFETY: under CORE on the calling thread; fresh session on a live
        // connection, deleted (Drop) before close via murmur_close/x_destroy.
        let session = unsafe {
            session_sys::Session::create_attach_list(db_handle, attached.iter().map(String::as_str))
        }
        .map_err(|rc| format!("murmur: session create failed rc={rc}"))?;
        Ok((attached, session))
    })();
    match tail {
        Ok((attached, session)) => {
            core.conns.insert(
                key_now,
                state::ConnState {
                    db: db_handle,
                    session,
                    attached,
                    spool: store,
                    spool_path,
                    db_path,
                    vfs,
                },
            );
            Ok(())
        }
        Err(e) => {
            if fresh {
                core.db_spool.remove(&db_path);
            }
            if let Some(entry) = core.spools.get_mut(&spool_path) {
                entry.refs = entry.refs.saturating_sub(1);
            }
            let _ = state::evict_if_orphan(&mut core, &spool_path);
            Err(e)
        }
    }
}

fn murmur_enable_sync(ctx: &Context) -> Result<ToSqlOutput<'static>> {
    let table: String = ctx.get(0)?;
    let mut core = state::lock();
    let key = db_key(ctx)?;
    // SAFETY: borrowed handle of the calling connection for direct queries.
    let conn = unsafe { ctx.get_connection()? };
    let id =
        crate::schema_map::enable(&conn, &mut core, key, &table).map_err(fn_error)?;
    Ok(ToSqlOutput::Owned(Value::Text(id)))
}

fn murmur_disable_sync(ctx: &Context) -> Result<ToSqlOutput<'static>> {
    let table: String = ctx.get(0)?;
    let mut core = state::lock();
    let key = db_key(ctx)?;
    crate::schema_map::disable(&mut core, key, &table).map_err(fn_error)?;
    Ok(ToSqlOutput::Owned(Value::Integer(0)))
}

fn murmur_collect(ctx: &Context) -> Result<ToSqlOutput<'static>> {
    let mut core = state::lock();
    let key = db_key(ctx)?;
    // SAFETY: borrowed handle of the calling connection for direct queries.
    let conn = unsafe { ctx.get_connection()? };
    let seq = crate::outbox::collect(&conn, &mut core, key).map_err(fn_error)?;
    Ok(match seq {
        Some(seq) => ToSqlOutput::Owned(Value::Integer(seq)),
        None => ToSqlOutput::Owned(Value::Null),
    })
}

fn murmur_drain(ctx: &Context) -> Result<ToSqlOutput<'static>> {
    let core = state::lock();
    let key = db_key(ctx)?;
    let (store, db_path, vfs) = {
        let state = core
            .conns
            .get(&key)
            .ok_or_else(|| fn_error("murmur: call murmur_open first".into()))?;
        (
            state.spool.clone(),
            state.db_path.clone(),
            state.vfs.clone(),
        )
    };
    let drained =
        crate::outbox::drain_all(&core, &store, &db_path, vfs.as_deref()).map_err(fn_error)?;
    Ok(ToSqlOutput::Owned(Value::Integer(drained as i64)))
}

fn murmur_sync_tables(ctx: &Context) -> Result<ToSqlOutput<'static>> {
    let core = state::lock();
    let key = db_key(ctx)?;
    let store = core
        .conns
        .get(&key)
        .ok_or_else(|| fn_error("murmur: call murmur_open first".into()))?
        .spool
        .clone();
    let schema = crate::schema_map::load_schema(&store).map_err(fn_error)?;
    let list: Vec<serde_json::Value> = schema
        .tables
        .iter()
        .map(|t| {
            serde_json::json!({
                "name": t.name,
                "id": t.id,
                "enabled": murmur_crdt::table_replication::enabled(&schema, &t.id),
            })
        })
        .collect();
    Ok(ToSqlOutput::Owned(Value::Text(
        serde_json::Value::Array(list).to_string(),
    )))
}

/// Borrow the calling connection's store. The caller must hold the CORE
/// guard across the whole operation (ticks, drains, and applies serialize
/// on it); the guard stays alive in the caller's frame.
fn lookup<'c>(
    core: &'c state::Core,
    ctx: &Context,
) -> Result<
    (
        murmur_crdt::logical::LogicalStore,
        String,
        Option<String>,
    ),
    rusqlite::Error,
> {
    let key = db_key(ctx)?;
    let state = core
        .conns
        .get(&key)
        .ok_or_else(|| fn_error("murmur: call murmur_open first".into()))?;
    Ok((
        state.spool.clone(),
        state.db_path.clone(),
        state.vfs.clone(),
    ))
}

fn murmur_export(ctx: &Context) -> Result<ToSqlOutput<'static>> {
    let core = state::lock();
    let (store, db_path, vfs) = lookup(&core, ctx)?;
    let bytes =
        crate::sync::export_state(&core, &store, &db_path, vfs.as_deref()).map_err(fn_error)?;
    Ok(ToSqlOutput::Owned(Value::Blob(bytes)))
}

fn murmur_apply(ctx: &Context) -> Result<ToSqlOutput<'static>> {
    let batch: Vec<u8> = ctx.get(0)?;
    let core = state::lock();
    let (store, db_path, vfs) = lookup(&core, ctx)?;
    let (rows, deleted, tombstones) =
        crate::sync::apply_batch(&store, &db_path, vfs.as_deref(), &batch).map_err(fn_error)?;
    Ok(ToSqlOutput::Owned(Value::Text(
        serde_json::json!({"rows": rows, "deleted": deleted, "tombstones": tombstones})
            .to_string(),
    )))
}

fn murmur_log_export(ctx: &Context) -> Result<ToSqlOutput<'static>> {
    let origin: String = ctx.get(0)?;
    let from: i64 = ctx.get(1)?;
    let core = state::lock();
    let (store, db_path, vfs) = lookup(&core, ctx)?;
    let bytes = crate::repl::log_export(&core, &store, &db_path, vfs.as_deref(), &origin, from)
        .map_err(fn_error)?;
    Ok(ToSqlOutput::Owned(Value::Blob(bytes)))
}

fn murmur_log_seq(ctx: &Context) -> Result<ToSqlOutput<'static>> {
    let origin: String = ctx.get(0)?;
    let core = state::lock();
    let (store, _, _) = lookup(&core, ctx)?;
    let seq = crate::repl::log_seq(&store, &origin).map_err(fn_error)?;
    let seq: i64 = seq
        .try_into()
        .map_err(|_| fn_error("murmur: log sequence exceeds int64".into()))?;
    Ok(ToSqlOutput::Owned(Value::Integer(seq)))
}

fn murmur_log_rows(ctx: &Context) -> Result<ToSqlOutput<'static>> {
    let origin: String = ctx.get(0)?;
    let from: i64 = ctx.get(1)?;
    let to: i64 = ctx.get(2)?;
    let core = state::lock();
    let (store, _, _) = lookup(&core, ctx)?;
    let rows = crate::repl::log_rows(&store, &origin, from, to).map_err(fn_error)?;
    let rows: i64 = rows
        .try_into()
        .map_err(|_| fn_error("murmur: logged row count exceeds int64".into()))?;
    Ok(ToSqlOutput::Owned(Value::Integer(rows)))
}

fn murmur_database_id(ctx: &Context) -> Result<ToSqlOutput<'static>> {
    let core = state::lock();
    let (store, _, _) = lookup(&core, ctx)?;
    let id = crate::sync::database_id_hex(&store).map_err(fn_error)?;
    Ok(ToSqlOutput::Owned(Value::Text(id)))
}

fn murmur_set_database_id(ctx: &Context) -> Result<ToSqlOutput<'static>> {
    let hex: String = ctx.get(0)?;
    let core = state::lock();
    let (store, _, _) = lookup(&core, ctx)?;
    crate::sync::set_database_id(&store, &hex).map_err(fn_error)?;
    Ok(ToSqlOutput::Owned(Value::Integer(0)))
}

fn murmur_clock(ctx: &Context) -> Result<ToSqlOutput<'static>> {
    let core = state::lock();
    let (store, _, _) = lookup(&core, ctx)?;
    let clock = crate::sync::clock_json(&store).map_err(fn_error)?;
    Ok(ToSqlOutput::Owned(Value::Text(clock)))
}

fn murmur_close(ctx: &Context) -> Result<ToSqlOutput<'static>> {
    let mut core = state::lock();
    let key = db_key(ctx)?;
    state::remove_conn(&mut core, key).map_err(fn_error)?;
    Ok(ToSqlOutput::Owned(Value::Integer(0)))
}

fn murmur_debug_sessions(_ctx: &Context) -> Result<ToSqlOutput<'static>> {
    let core = state::lock();
    Ok(ToSqlOutput::Owned(Value::Integer(
        core.conns.len() as i64
    )))
}

fn murmur_repl_start(ctx: &Context) -> Result<ToSqlOutput<'static>> {
    let mut core = state::lock();
    let key = db_key(ctx)?;
    let argc = ctx.len();
    if argc != 3 && argc != 4 {
        return Err(fn_error(
            "murmur: murmur_repl_start(bind, credentials, peers[, intervals])".into(),
        ));
    }
    let bind: String = ctx.get(0)?;
    let creds: String = ctx.get(1)?;
    let peers: String = ctx.get(2)?;
    let intervals: Option<String> = if argc == 4 { Some(ctx.get(3)?) } else { None };
    let addr =
        crate::repl::repl_start(&mut core, key, &bind, &creds, &peers, intervals.as_deref())
            .map_err(fn_error)?;
    Ok(ToSqlOutput::Owned(Value::Text(addr)))
}

fn murmur_repl_stop(ctx: &Context) -> Result<ToSqlOutput<'static>> {
    let key = db_key(ctx)?;
    let (handle, spool_path) = {
        let mut core = state::lock();
        crate::repl::take_repl(&mut core, key).map_err(fn_error)?
    };
    // Unguarded: close() joins the replication thread, which may be inside
    // a backend call holding CORE.
    if let Some(handle) = handle {
        handle
            .close()
            .map_err(|e| fn_error(format!("murmur: repl stop failed: {e}")))?;
    }
    let mut core = state::lock();
    state::evict_if_orphan(&mut core, &spool_path).map_err(fn_error)?;
    Ok(ToSqlOutput::Owned(Value::Integer(0)))
}

fn murmur_repl_add_peer(ctx: &Context) -> Result<ToSqlOutput<'static>> {
    let core = state::lock();
    let key = db_key(ctx)?;
    let addr: String = ctx.get(0)?;
    crate::repl::repl_add_peer(&core, key, &addr).map_err(fn_error)?;
    Ok(ToSqlOutput::Owned(Value::Integer(0)))
}

fn murmur_repl_status(ctx: &Context) -> Result<ToSqlOutput<'static>> {
    let core = state::lock();
    let key = db_key(ctx)?;
    let status = crate::repl::repl_status(&core, key).map_err(fn_error)?;
    Ok(ToSqlOutput::Owned(Value::Text(status)))
}

fn murmur_repl_sync(ctx: &Context) -> Result<ToSqlOutput<'static>> {
    let core = state::lock();
    let key = db_key(ctx)?;
    crate::repl::repl_sync(&core, key).map_err(fn_error)?;
    Ok(ToSqlOutput::Owned(Value::Integer(0)))
}

fn murmur_repl_pubkey(ctx: &Context) -> Result<ToSqlOutput<'static>> {
    let core = state::lock();
    let key = db_key(ctx)?;
    let pubkey = crate::repl::repl_pubkey(&core, key).map_err(fn_error)?;
    Ok(ToSqlOutput::Owned(Value::Text(pubkey)))
}

fn murmur_node_id(ctx: &Context) -> Result<ToSqlOutput<'static>> {
    let core = state::lock();
    let (store, _, _) = lookup(&core, ctx)?;
    let id = crate::outbox::node_id(&store).map_err(fn_error)?;
    Ok(ToSqlOutput::Owned(Value::Text(
        id.iter().map(|b| format!("{b:02x}")).collect(),
    )))
}

fn murmur_repl_progress(ctx: &Context) -> Result<ToSqlOutput<'static>> {
    let core = state::lock();
    let key = db_key(ctx)?;
    let progress = crate::repl::repl_progress(&core, key).map_err(fn_error)?;
    Ok(ToSqlOutput::Owned(Value::Text(progress)))
}
