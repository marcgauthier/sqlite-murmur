//! Extension-wide state: one global core behind one lock.
//!
//! Deliberate Phase 2 simplification: every SQL function takes `CORE` for its
//! whole body, so there is no lock ordering to get wrong between session
//! handles, Spool access, and drain coordination. This serializes extension
//! operations process-wide; fine-grained locking is future work once the
//! protocol is proven.
//!
//! Layout:
//! - `conns`: live murmur sessions keyed by connection handle. Sessions are
//!   connection-affine; all access happens under `CORE` on the calling thread.
//! - `spools`: one shared [`LogicalStore`] per spool directory per process, so
//!   two connections on the same database share replication state instead of
//!   double-opening Spool (which file-locks). Entries live for the process
//!   lifetime; Spool tolerates idle opens.
//! - `db_spool`: guards against opening one database with two spool
//!   directories (split-brain metadata).

use crate::session_sys;
use murmur_crdt::logical::LogicalStore;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{LazyLock, Mutex, MutexGuard};

pub struct ConnState {
    /// Raw handle (map key); owned by the host, never touched after close.
    pub db: *mut rusqlite::ffi::sqlite3,
    pub session: session_sys::Session,
    /// Tables currently attached to the session, by user name.
    pub attached: Vec<String>,
    pub spool: LogicalStore,
    pub spool_path: std::path::PathBuf,
    pub db_path: String,
    /// Named VFS of the database, if it is not the default one. Helper
    /// connections (drain, apply) must open the same VFS: a custom-VFS path
    /// is meaningless to the default VFS.
    pub vfs: Option<String>,
}

// SAFETY: `db` is an opaque key; the session and store are used only under
// `CORE` on calling threads, and cleanup runs before close returns.
unsafe impl Send for ConnState {}

/// One shared store plus the key fingerprint it was opened with, so a
/// second connection cannot silently share a spool opened with other keys.
/// Evicted (closed, lock released) when the last connection detaches, so a
/// database can be fully handed off and its spool reopened elsewhere.
pub struct SpoolEntry {
    pub store: LogicalStore,
    pub key_fingerprint: Option<[u8; 32]>,
    pub refs: usize,
    /// Live QUIC replication. Outlives connections: a last-close with a
    /// running handle leaves the entry (refs zero) instead of evicting, so
    /// the node keeps relaying headless and a reopen finds its store.
    pub repl: Option<murmur_replication::Handle>,
    /// Highest version minted in this process. Ticks take `max(persisted
    /// clock, last_tick)`. Drains always process every committed row (acks
    /// defer on BUSY but processing continues), so the persisted clock
    /// covers all of them; last_tick additionally covers versions minted
    /// but not yet committed by live peers, including writers SQLite may
    /// admit beside the caller (`BEGIN CONCURRENT`). A fresh entry implies
    /// no live peers (evicted at refs zero), so the recovery drain alone
    /// makes it safe.
    pub last_tick: murmur_crdt::model::Version,
}

pub struct Core {
    pub conns: HashMap<usize, ConnState>,
    pub spools: HashMap<PathBuf, SpoolEntry>,
    pub db_spool: HashMap<String, PathBuf>,
}

impl Core {
    fn new() -> Self {
        Self {
            conns: HashMap::new(),
            spools: HashMap::new(),
            db_spool: HashMap::new(),
        }
    }
}

static CORE: LazyLock<Mutex<Core>> = LazyLock::new(|| Mutex::new(Core::new()));

pub fn lock() -> MutexGuard<'static, Core> {
    CORE.lock().expect("murmur core lock poisoned")
}

/// Evict a spool entry and close its store, but only once no connection
/// references it AND replication is stopped. Idempotent.
pub fn evict_if_orphan(core: &mut Core, spool_path: &std::path::Path) -> Result<(), String> {
    let orphan = core
        .spools
        .get(spool_path)
        .is_some_and(|e| e.refs == 0 && e.repl.is_none());
    if !orphan {
        return Ok(());
    }
    let entry = core.spools.remove(spool_path).unwrap();
    entry
        .store
        .close()
        .map_err(|e| format!("murmur: spool close failed: {e}"))
}

/// Remove a connection's state, deleting its session, and release its
/// spool reference (evicting at zero, unless replication is running — see
/// `SpoolEntry::repl`). Idempotent: safe to call from both `murmur_close`
/// and the connection-close backstop.
pub fn remove_conn(core: &mut Core, db: usize) -> Result<(), String> {
    let Some(state) = core.conns.remove(&db) else {
        return Ok(());
    };
    if let Some(entry) = core.spools.get_mut(&state.spool_path) {
        entry.refs = entry.refs.saturating_sub(1);
    }
    evict_if_orphan(core, &state.spool_path)
}
