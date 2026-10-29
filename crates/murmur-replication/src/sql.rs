use crate::{engine::Handle, storage::Worker, *};
use murmur_tx::Backend as _;
use rusqlite::{Connection, ffi, functions::FunctionFlags, vtab};
use std::{
    ffi::{CStr, c_char, c_int, c_uint, c_void},
    path::PathBuf,
    sync::{
        Mutex, OnceLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
};
const REGISTER: i32 = 0x535801;
const BACKUP: i32 = 0x535803;
const COMPACT: i32 = 0x535804;
const KEYRING: i32 = 0x535805;
const KEYRING_GET: c_int = 1;
const KEYRING_SET: c_int = 2;
const KEYRING_INVENTORY: c_int = 3;
const KEYRING_ROTATE: c_int = 4;
const KEYRING_PRUNE: c_int = 5;
const KEYRING_PARAM_KDF: c_int = 1;
const KEYRING_PARAM_TIME: c_int = 2;
const KEYRING_PARAM_MEMORY: c_int = 3;
const KEYRING_PARAM_THREADS: c_int = 4;
/// Mirrors `sqlite_murmur_keyring_args` (include/sqlite_murmur.h): field
/// order is ABI; keep the two in sync.
#[repr(C)]
struct KeyringArgs {
    op: c_int,
    param: c_int,
    value: u64,
    sealed: c_int,
    kdf: c_uint,
    time: c_uint,
    memory_kib: c_uint,
    threads: c_uint,
    json: *mut c_char,
}
impl KeyringArgs {
    fn get() -> Self {
        Self { op: KEYRING_GET, param: 0, value: 0, sealed: 0, kdf: 0, time: 0, memory_kib: 0, threads: 0, json: std::ptr::null_mut() }
    }
    fn set(param: c_int, value: u64) -> Self {
        Self { op: KEYRING_SET, param, value, sealed: 0, kdf: 0, time: 0, memory_kib: 0, threads: 0, json: std::ptr::null_mut() }
    }
    fn inventory() -> Self {
        Self { op: KEYRING_INVENTORY, param: 0, value: 0, sealed: 0, kdf: 0, time: 0, memory_kib: 0, threads: 0, json: std::ptr::null_mut() }
    }
    fn rotate() -> Self {
        Self { op: KEYRING_ROTATE, param: 0, value: 0, sealed: 0, kdf: 0, time: 0, memory_kib: 0, threads: 0, json: std::ptr::null_mut() }
    }
    fn prune() -> Self {
        Self { op: KEYRING_PRUNE, param: 0, value: 0, sealed: 0, kdf: 0, time: 0, memory_kib: 0, threads: 0, json: std::ptr::null_mut() }
    }
}
/// WP-D slice D2a: the four envelope-header params bypass murmur_config.
fn kdf_param_id(name: &str) -> Option<c_int> {
    match name {
        "kdf" => Some(KEYRING_PARAM_KDF),
        "kdf_time" => Some(KEYRING_PARAM_TIME),
        "kdf_memory_kib" => Some(KEYRING_PARAM_MEMORY),
        "kdf_threads" => Some(KEYRING_PARAM_THREADS),
        _ => None,
    }
}
fn kdf_format(name: &str, args: &KeyringArgs) -> String {
    match name {
        "kdf" => {
            if args.kdf == 1 {
                "argon2id".to_string()
            } else {
                "direct".to_string()
            }
        }
        "kdf_time" => args.time.to_string(),
        "kdf_memory_kib" => args.memory_kib.to_string(),
        _ => args.threads.to_string(),
    }
}

/// WP-C: side-effecting file/bridge actions require autocommit (pure
/// reads do not). Mirrors the replication peer-control guard.
fn file_autocommit(raw: usize, c: &Arc<Control>) -> rusqlite::Result<()> {
    if unsafe { ffi::sqlite3_get_autocommit(raw as *mut ffi::sqlite3) } == 0 {
        return Err(c.fail(Error::Invalid("file actions require autocommit".into())));
    }
    Ok(())
}

/// WP-C: map a murmur-files (murmur_tx) error onto the SQL surface.
fn file_err(c: &Arc<Control>, e: murmur_tx::Error) -> rusqlite::Error {
    c.fail(crate::wait::backend_err(e))
}

/// WP-C: serialize action metadata as JSON text.
fn file_json<T: serde::Serialize>(c: &Arc<Control>, value: &T) -> rusqlite::Result<String> {
    serde_json::to_string(value).map_err(|e| c.fail(Error::Protocol(format!("file json: {e}"))))
}

/// WP-C: non-negative SQL integers for counts/byte sizes (fail loud on
/// wrap rather than truncating).
fn file_int(c: &Arc<Control>, value: u64) -> rusqlite::Result<i64> {
    i64::try_from(value).map_err(|_| c.fail(Error::Overloaded))
}

/// WP-C: checked SQL INTEGER to u64.
fn file_u64(c: &Arc<Control>, name: &str, value: i64) -> rusqlite::Result<u64> {
    u64::try_from(value)
        .map_err(|_| c.fail(Error::Invalid(format!("{name} must be non-negative"))))
}
/// Mirrors `sqlite_murmur_backup_args` (include/sqlite_murmur_backup.h):
/// NUL-terminated dest path plus manifest identity (zeros when the node
/// never initialized replication).
#[repr(C)]
struct BackupArgs {
    dest: *const c_char,
    group: [u8; 16],
    node: [u8; 16],
}
/// Lenient 32-hex parse for manifest identity (zeros allowed: identity
/// here is descriptive, never authentication).
fn parse_hex_id(v: &str) -> Result<NodeId> {
    if v.len() != 32 || !v.is_ascii() {
        return Err(Error::Invalid("expected 128-bit hexadecimal identity".into()));
    }
    let mut id = [0; 16];
    for (i, b) in id.iter_mut().enumerate() {
        *b = u8::from_str_radix(&v[i * 2..i * 2 + 2], 16)
            .map_err(|_| Error::Invalid("invalid identity".into()))?;
    }
    Ok(id)
}
#[repr(C)]
struct Bridge {
    context: *mut c_void,
    pragma:
        unsafe extern "C" fn(*mut c_void, *const c_char, *const c_char, *mut *mut c_char) -> i32,
    destroy: unsafe extern "C" fn(*mut c_void),
}
static SERVICES: OnceLock<Mutex<BTreeMap<String, Weak<Control>>>> = OnceLock::new();
pub struct Control {
    worker: Arc<Worker>,
    handle: Mutex<Option<Handle>>,
    last_error: Mutex<Option<RecordedError>>,
    /// WP-C file/bridge hosts + restart-applied knobs + DB parent dir
    /// (bridge dir resolution). Built once at register; files act on the
    /// worker-routed stores with or without the replication engine.
    files: Arc<murmur_files::hosts::Hosts>,
    files_backend: Arc<crate::file_store::FileBackend>,
    file_knobs: murmur_files::adapt::Knobs,
    file_db_parent: PathBuf,
}

// WP-C §7a file-key slot readers (sqlite_murmur.h). SQLITE_OK with 32
// bytes when provisioned, SQLITE_NOTFOUND otherwise.
unsafe extern "C" {
    fn sqlite_murmur_file_key_current(out: *mut u8) -> c_int;
    fn sqlite_murmur_file_key_previous(out: *mut u8) -> c_int;
}

/// Read the provisioned group file key slot. `(None, None)` while
/// unprovisioned — file actions fail closed until then.
fn file_key_slot() -> (Option<[u8; 32]>, Option<[u8; 32]>) {
    let mut current = [0u8; 32];
    let mut previous = [0u8; 32];
    let have_current =
        unsafe { sqlite_murmur_file_key_current(current.as_mut_ptr()) } == ffi::SQLITE_OK;
    if !have_current {
        return (None, None);
    }
    let have_previous =
        unsafe { sqlite_murmur_file_key_previous(previous.as_mut_ptr()) } == ffi::SQLITE_OK;
    (
        Some(current),
        if have_previous { Some(previous) } else { None },
    )
}
struct RecordedError {
    code: i32,
    kind: String,
    message: String,
}
impl Control {
    /// Record a SQL-surface failure for `murmur_status()` and convert it
    /// to a rusqlite error carrying the murmur extended code + message.
    fn fail(&self, e: Error) -> rusqlite::Error {
        self.record_raw(
            crate::errors::murmur_code(&e),
            crate::errors::murmur_kind(&e),
            e.to_string(),
        );
        sql_error(e)
    }
    fn record_raw(&self, code: i32, kind: &str, message: String) {
        *self.last_error.lock().unwrap() = Some(RecordedError {
            code,
            kind: kind.into(),
            message,
        });
    }
    pub(crate) fn worker(&self) -> &Arc<Worker> {
        &self.worker
    }
    fn start(&self) -> Result<i64> {
        let mut h = self.handle.lock().unwrap();
        if h.is_some() {
            return Ok(0);
        }
        let cfg = self.worker.call(|s| s.initialize())?;
        *h = Some(Handle::start(cfg.clone(), self.worker.clone())?);
        // Online file backend now that identity exists (bridge snapshots).
        // Group comes from config; node reads live (initialize just set it).
        let (node, _site) = self.worker.call(|s| Ok(s.file_identity()))?;
        // Convert before the move: upgrade takes cfg by value.
        let tx_cfg = cfg.to_fetch_transport();
        self.files_backend.upgrade(cfg.group, node, cfg);
        // Phase-3: best-effort fetch start — THE IGNORE IS REQUIRED
        // (fetch defaults disabled: empty fetch_bind must not fail
        // murmur_start; failures stay silent by design, ticks idle,
        // next start retries; idempotent).
        let _ = self.files.start_fetch(&self.file_knobs, &tx_cfg);
        Ok(1)
    }
    fn stop(&self) -> i64 {
        // Phase-3: endpoint pairing — quiesce file transfers before
        // engine teardown (no-op when never started).
        self.files.stop_fetch();
        let mut h = self.handle.lock().unwrap();
        if h.take().is_some() { 1 } else { 0 }
    }
    fn members(&self) -> Vec<serde_json::Value> {
        self.handle
            .lock()
            .unwrap()
            .as_ref()
            .map_or_else(Vec::new, |h| h.live.members())
    }
    fn fetch_sources(&self) -> Vec<(NodeId, SocketAddr)> {
        let Ok(bind) = self.file_knobs.fetch_bind.parse::<SocketAddr>() else {
            return Vec::new();
        };
        self.handle
            .lock()
            .unwrap()
            .as_ref()
            .map_or_else(Vec::new, |h| h.live.fetch_sources(bind.port()))
    }
    fn status(&self) -> String {
        let mut value = self.handle.lock().unwrap().as_ref().map_or_else(
            || serde_json::json!({"running": false}),
            |h| h.live.status(),
        );
        if let Some(recorded) = self.last_error.lock().unwrap().as_ref() {
            value["last_error_code"] = recorded.code.into();
            value["last_error_kind"] = recorded.kind.clone().into();
            value["last_error_message"] = recorded.message.clone().into();
        }
        value.to_string()
    }
    /// Read-only health projection (point 14). Additive versioned JSON;
    /// unmeasurable metrics stay null. Short reads only: no flush, no
    /// network activity, no cr-sqlite initialization.
    fn health(&self) -> String {
        let null = serde_json::Value::Null;
        let live = self.handle.lock().unwrap().as_ref().map(|h| {
            let s = h.live.status();
            (
                s["applied_batches"].clone(),
                s["sent_batches"].clone(),
                s["connected_peers"].clone(),
                s["last_error"].clone(),
                h.live.serving_in_use(),
                h.live.receiving_in_use(),
            )
        });
        let (running, applied, sent, peers, engine_error, serving, receiving) =
            live.map_or_else(
                || (false, null.clone(), null.clone(), null.clone(), null.clone(), 0, 0),
                |(a, s, p, e, sv, rc)| (true, a, s, p, e, sv, rc),
            );
        // One short worker round trip: pending scheduler config, staged
        // bytes, and the local logical cursor. Any failure nulls the
        // whole block rather than reporting partial reads.
        let store: Option<(u64, u64, u64, i64, Option<i64>)> = self
            .worker
            .call(|s| {
                let num = |name: &str| -> Result<u64> {
                    config::setting(&s.db, name)?
                        .parse()
                        .map_err(|_| Error::Invalid(format!("invalid {name}")))
                };
                let stage: Option<i64> = s
                    .db
                    .query_row("SELECT SUM(LENGTH(data)) FROM murmur_stage", [], |r| {
                        r.get(0)
                    })?;
                let cursor: Option<i64> =
                    s.db.query_row("SELECT crsql_db_version()", [], |r| r.get(0)).ok();
                Ok((
                    num("writer_local_share")?,
                    num("writer_max_debt_ms")?,
                    num("writer_maintenance_reserve_every")?,
                    stage.unwrap_or(0),
                    cursor,
                ))
            })
            .ok();
        let (pending, stage_bytes, cursor) = match store {
            Some((local, debt, reserve, stage, cursor)) if (1..=99).contains(&local) => (
                serde_json::json!({
                    "local_share": local,
                    "remote_share": 100 - local,
                    "max_debt_ms": debt,
                    "maintenance_reserve_every": reserve,
                }),
                serde_json::json!(stage),
                cursor.map_or(null.clone(), serde_json::Value::from),
            ),
            _ => (null.clone(), null.clone(), null.clone()),
        };
        let effective = self.worker.scheduler_effective();
        let stats = serde_json::to_value(self.worker.scheduler_snapshot()).unwrap_or(null.clone());
        let retention = match self.backend_opt() {
            Ok(Some(b)) => murmur_tx::retention::status(&b)
                .map(|s| {
                    serde_json::json!({
                        "peers": s.peers.len(),
                        "retired": s.peers.iter().filter(|p| p.retired).count(),
                        "collected_transactions": s.collected_transactions,
                        "last_collection_millis": s.last_collection_millis,
                    })
                })
                .unwrap_or(null.clone()),
            _ => null.clone(),
        };
        let mut value = serde_json::json!({
            "version": 1,
            "replication": {
                "running": running,
                "applied_batches": applied,
                "sent_batches": sent,
                "connected_peers": peers,
                "last_error": engine_error,
            },
            "scheduler": {
                "local_admission_enforced": false,
                "scope": "remote_and_maintenance",
                "effective_config": {
                    "local_share": effective.local_share,
                    "remote_share": effective.remote_share,
                    "max_debt_ms": effective.max_debt.as_millis() as u64,
                    "maintenance_reserve_every": effective.maintenance_reserve_every,
                },
                "pending_config": pending,
                "stats": stats,
            },
            "storage": {
                "committed_cursor": cursor,
                "durable_cursor": null,
                "counters": null,
            },
            "budgets": {
                "serving_in_use_bytes": serving,
                "receiving_in_use_bytes": receiving,
                "stage_bytes": stage_bytes,
                "worker_payload_bytes": null,
            },
            "retention": retention,
        });
        if let Some(recorded) = self.last_error.lock().unwrap().as_ref() {
            value["replication"]["last_error_code"] = recorded.code.into();
            value["replication"]["last_error_kind"] = recorded.kind.clone().into();
            value["replication"]["last_error_message"] = recorded.message.clone().into();
        }
        value.to_string()
    }
    /// Backend for progress/waits: live config while running, stored
    /// identities while stopped. `None` when replication never started.
    /// Locks the handle mutex: never call while holding it (self-deadlock).
    fn backend_opt(&self) -> Result<Option<CrrBackend>> {
        if let Some(h) = self.handle.lock().unwrap().as_ref() {
            let cfg = h.live.cfg.clone();
            return Ok(Some(CrrBackend::new(
                self.worker.clone(),
                cfg.group,
                cfg.node,
                cfg,
            )));
        }
        let (group, node): (String, Option<Vec<u8>>) =
            self.worker.call(|s| {
                let group = config::setting(&s.db, "group_id")?;
                let node: Option<Vec<u8>> = s.db.query_row(
                    "SELECT node FROM murmur_identity WHERE id=1",
                    [],
                    |r| r.get(0),
                )?;
                Ok((group, node))
            })?;
        let (Some(node), Ok(group)) = (node, parse_id(&group)) else {
            return Ok(None);
        };
        if node.len() != 16 {
            return Ok(None);
        }
        let mut id = [0u8; 16];
        id.copy_from_slice(&node);
        Ok(Some(CrrBackend::new_offline(
            self.worker.clone(),
            group,
            id,
        )))
    }
}
impl Drop for Control {
    fn drop(&mut self) {
        self.handle.get_mut().unwrap().take();
    }
}

/// One retention pass: advance floors (needs a backend, skipped before
/// first start), prune acked tombstones, and reap abandoned staging.
/// Best-effort; the tick loop retries next interval on any error.
/// Returns (floors moved, tombstones pruned) for the on-demand UDF.
fn retention_pass(control: &Arc<Control>, worker: &Arc<Worker>) -> (u64, u64) {
    let online: BTreeSet<NodeId> = control
        .members()
        .iter()
        .filter_map(|m| {
            if m.get("connected").and_then(|v| v.as_bool()).unwrap_or(false) {
                m.get("node_id")
                    .and_then(|v| v.as_str())
                    .and_then(|s| parse_id(s).ok())
            } else {
                None
            }
        })
        .collect();
    let params = worker.call(|s| {
        let num = |name: &str| -> Result<u64> {
            config::setting(&s.db, name)?
                .parse()
                .map_err(|_| Error::Invalid(format!("invalid {name}")))
        };
        let bootstrap = config::setting(&s.db, "bootstrap")?;
        Ok(crate::retention::RetentionParams {
            minimum_retained: num("retention_min_kept")?,
            maximum_offline_ms: num("retention_max_offline_ms")?,
            bootstrap_nonempty: bootstrap != "[]",
        })
    });
    let mut floors = 0u64;
    let mut pruned = 0u64;
    if let (Ok(params), Ok(Some(backend))) = (params, control.backend_opt()) {
        floors = crate::retention::collect_floors(
            &backend,
            &params,
            &online,
            murmur_tx::retention::now_millis(),
            worker.boot_millis(),
        )
        .unwrap_or(0);
        // Snapshot hygiene rides the same pass: expire pins, chunks,
        // and receipts, and release pins for forgotten recipients.
        let now = murmur_tx::retention::now_millis();
        let _ = murmur_tx::snapshot::maintain(&backend, now);
        let _ = murmur_tx::snapshot::reap_forgotten_pins(
            &backend,
            now,
            std::time::Duration::from_millis(params.maximum_offline_ms),
            &online,
        );
        // A2b physical GC: prune tombstones at or below the my-stream
        // floor. Disabled by retention_gc=0; before first start there
        // is no backend and the pass only reaps staging.
        let gc_on = worker
            .call(|s| {
                Ok::<_, Error>(config::setting(&s.db, "retention_gc")? == "1")
            })
            .unwrap_or(false);
        if gc_on {
            let own = backend.identity().1;
            let own_applied = worker
                .call(|s| {
                    Ok(s.db
                        .query_row("SELECT crsql_db_version()", [], |r| {
                            r.get::<_, i64>(0)
                        })?
                        .max(0) as u64)
                })
                .unwrap_or(0);
            if let Ok(floor) = crate::retention::my_stream_floor(
                &backend,
                &params,
                &online,
                own,
                own_applied,
                now,
                worker.boot_millis(),
            )
            .map_err(|e| crate::wait::backend_err(e))
            {
                pruned = worker
                    .call_admitted(
                        murmur_sched::WriterClass::Maintenance,
                        crate::storage::ADMISSION_TIMEOUT,
                        move |s| s.gc_tombstones(floor),
                    )
                    .unwrap_or(0);
            }
        }
    }
    let _ = worker.call_admitted(
        murmur_sched::WriterClass::Maintenance,
        crate::storage::ADMISSION_TIMEOUT,
        |s| s.reap_staging(),
    );
    (floors, pruned)
}
struct PragmaContext {
    control: Arc<Control>,
    db: usize,
    maintenance_stop: Arc<AtomicBool>,
    maintenance_thread: Option<std::thread::JoinHandle<()>>,
}
/// Resolve the registered [`Control`] for a database handle (used by the
/// subscription C bridge). `None` when murmur never registered there.
pub(crate) fn control_for(raw: *mut ffi::sqlite3) -> Option<Arc<Control>> {
    if raw.is_null() {
        return None;
    }
    let path = unsafe { ffi::sqlite3_db_filename(raw, c"main".as_ptr()) };
    if path.is_null() {
        return None;
    }
    let path = unsafe { CStr::from_ptr(path) }
        .to_string_lossy()
        .into_owned();
    SERVICES
        .get()?
        .lock()
        .unwrap()
        .get(&path)
        .and_then(Weak::upgrade)
}
pub unsafe fn sqlite_string(s: &str) -> *mut c_char {
    let p = unsafe { ffi::sqlite3_malloc64((s.len() + 1) as u64) }.cast::<u8>();
    if !p.is_null() {
        unsafe {
            std::ptr::copy_nonoverlapping(s.as_ptr(), p, s.len());
            *p.add(s.len()) = 0;
        }
    }
    p.cast()
}
unsafe extern "C" fn destroy(p: *mut c_void) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut ctx = unsafe { Box::from_raw(p.cast::<PragmaContext>()) };
        ctx.maintenance_stop.store(true, Ordering::Release);
        if let Some(t) = ctx.maintenance_thread.take() {
            let _ = t.join();
        }
        drop(ctx);
    }));
}
unsafe extern "C" fn pragma(
    p: *mut c_void,
    name: *const c_char,
    value: *const c_char,
    out: *mut *mut c_char,
) -> i32 {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<String> {
        let ctx = unsafe { &*p.cast::<PragmaContext>() };
        let name = unsafe { CStr::from_ptr(name) }
            .to_string_lossy()
            .to_ascii_lowercase();
        let name = name.strip_prefix("murmur_").unwrap().to_string();
        // Settings use a separate worker, never SQL on this connection inside a VFS callback.
        if value.is_null() {
            if kdf_param_id(&name).is_some() {
                // Envelope-header params (exception J): forward to the
                // Store FCTL; no worker, no SQL, no murmur_config.
                let mut args = KeyringArgs::get();
                let rc = unsafe {
                    ffi::sqlite3_file_control(
                        ctx.db as *mut ffi::sqlite3,
                        c"main".as_ptr(),
                        KEYRING,
                        (&mut args as *mut KeyringArgs).cast(),
                    )
                };
                if rc != ffi::SQLITE_OK {
                    return Err(Error::Unavailable(format!("keyring unavailable (rc {rc})")));
                }
                return Ok(kdf_format(&name, &args));
            }
            return ctx
                .control
                .worker
                .call(move |s| config::setting(&s.db, &name));
        }
        if unsafe { ffi::sqlite3_get_autocommit(ctx.db as *mut ffi::sqlite3) } == 0 {
            return Err(Error::Invalid(
                "configure replication outside a transaction".into(),
            ));
        }
        let value = unsafe { CStr::from_ptr(value) }
            .to_str()
            .map_err(|_| Error::Invalid("setting must be UTF-8".into()))?
            .to_string();
        config::validate(&name, &value)?;
        let h = ctx.control.handle.lock().unwrap();
        if h.is_some() {
            return Err(Error::Invalid(
                "stop replication before changing settings".into(),
            ));
        }
        if let Some(param) = kdf_param_id(&name) {
            // Envelope-header write: the Store owns write-once, the
            // empty-database guard, and the seal itself.
            let number = if param == KEYRING_PARAM_KDF {
                u64::from(value == "argon2id")
            } else {
                // validate() range-checked already; unreachable on Err.
                value.parse::<u64>().map_err(|_| Error::Invalid(format!("{name} must be numeric")))?
            };
            let mut args = KeyringArgs::set(param, number);
            let rc = unsafe {
                ffi::sqlite3_file_control(
                    ctx.db as *mut ffi::sqlite3,
                    c"main".as_ptr(),
                    KEYRING,
                    (&mut args as *mut KeyringArgs).cast(),
                )
            };
            match rc {
                ffi::SQLITE_OK => return Ok(value),
                ffi::SQLITE_BUSY => {
                    // The Store seals on any live store; BUSY means the
                    // envelope already exists (write-once).
                    return Err(Error::Invalid(
                        "KDF envelope is sealed; parameters are write-once".into(),
                    ));
                }
                ffi::SQLITE_ERROR if param == KEYRING_PARAM_KDF && number == 1 => {
                    return Err(Error::Invalid(
                        "argon2id requires a build with a vendored Argon2 unit".into(),
                    ));
                }
                ffi::SQLITE_READONLY => {
                    return Err(Error::Invalid(
                        "cannot set KDF parameters on a read-only connection".into(),
                    ));
                }
                _ => {
                    return Err(Error::Unavailable(format!("keyring rejected the write (rc {rc})")));
                }
            }
        }
        ctx.control.worker.call(move|s|{
            if name=="group_id"{let node:Option<Vec<u8>>=s.db.query_row("SELECT node FROM murmur_identity WHERE id=1",[],|r|r.get(0))?;
                if node.is_some()&&config::setting(&s.db,&name)?!=value{return Err(Error::Invalid("group identity cannot change after first start; use a new database".into()));}}
            if name=="bridge_staging_bytes"||name=="bridge_max_bundle_bytes"{
                // Coupled file rule over effective values: persisted rows
                // with the incoming write applied (adapt keys full names).
                let pending_name=name.clone();let pending_value=value.clone();
                let get=|full:&str|->Option<String>{
                    let stripped=full.strip_prefix("murmur_").unwrap_or(full);
                    if stripped==pending_name{return Some(pending_value.clone());}
                    s.db.query_row("SELECT value FROM murmur_config WHERE name=?1",[stripped],|r|r.get::<_,String>(0)).ok()
                };
                murmur_files::adapt::validate_coupled(&get).map_err(crate::wait::backend_err)?;
            }
            s.db.execute("INSERT INTO murmur_config VALUES(?1,?2) ON CONFLICT(name) DO UPDATE SET value=excluded.value",rusqlite::params![name,value])?;s.flush()?;Ok(value)
        })
    }));
    let control = unsafe { &*p.cast::<PragmaContext>() }.control.clone();
    let (rc, msg) = match result {
        Ok(Ok(s)) => (ffi::SQLITE_OK, s),
        Ok(Err(e)) => {
            let message = e.to_string();
            control.record_raw(
                crate::errors::murmur_code(&e),
                crate::errors::murmur_kind(&e),
                message.clone(),
            );
            (crate::errors::murmur_code(&e), message)
        }
        Err(_) => {
            control.record_raw(
                crate::errors::CALLBACK_PANICKED,
                "CallbackPanicked",
                "murmur PRAGMA panic".into(),
            );
            (
                crate::errors::CALLBACK_PANICKED,
                "murmur PRAGMA panic".into(),
            )
        }
    };
    unsafe {
        *out = sqlite_string(&msg);
        if (*out).is_null() {
            ffi::SQLITE_NOMEM
        } else {
            rc
        }
    }
}
fn sql_error(e: Error) -> rusqlite::Error {
    rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error::new(crate::errors::murmur_code(&e)),
        Some(e.to_string()),
    )
}
/// Wait timeout in milliseconds: 0 polls once, otherwise up to one day.
fn wait_timeout_ms(ms: i64) -> Result<Duration> {
    if !(0..=86400000).contains(&ms) {
        return Err(Error::Invalid("timeout outside 0..86400000 ms".into()));
    }
    Ok(Duration::from_millis(ms as u64))
}
pub unsafe fn register(raw: *mut ffi::sqlite3, shared: bool) -> Result<()> {
    if raw.is_null() {
        return Err(Error::Invalid("null database".into()));
    }
    let db = unsafe { Connection::from_handle(raw) }?;
    if !db.is_autocommit() || unsafe { ffi::sqlite3_db_readonly(raw, c"main".as_ptr()) } != 0 {
        return Err(Error::Invalid(
            "load on a writable connection outside a transaction".into(),
        ));
    }
    let path = unsafe { ffi::sqlite3_db_filename(raw, c"main".as_ptr()) };
    if path.is_null() {
        return Err(Error::Invalid(
            "requires a named sqlite-murmur database".into(),
        ));
    }
    let path = unsafe { CStr::from_ptr(path) }
        .to_string_lossy()
        .into_owned();
    let mut vfs: *mut ffi::sqlite3_vfs = std::ptr::null_mut();
    if unsafe {
        ffi::sqlite3_file_control(
            raw,
            c"main".as_ptr(),
            ffi::SQLITE_FCNTL_VFS_POINTER,
            (&mut vfs as *mut *mut ffi::sqlite3_vfs).cast(),
        )
    } != 0
        || vfs.is_null()
    {
        return Err(Error::Invalid("cannot inspect VFS".into()));
    }
    let vfs_name = unsafe { CStr::from_ptr((*vfs).zName) }
        .to_string_lossy()
        .into_owned();
    if vfs_name != "sqlite-murmur" {
        return Err(Error::Invalid(
            "murmur-replication requires the sqlite-murmur VFS".into(),
        ));
    }
    let mut services = SERVICES.get_or_init(Default::default).lock().unwrap();
    services.retain(|_, v| v.strong_count() > 0);
    let existing = services.get(&path).and_then(Weak::upgrade);
    if existing.is_some() && !shared {
        return Err(Error::Invalid(
            "murmur already registered on a control connection for this database".into(),
        ));
    }
    let fresh = existing.is_none();
    let control = match existing {
        Some(control) => control,
        None => {
            let worker = Worker::open(path.clone(), vfs_name)?;
            // WP-C hosts: live-identity metadata backend + worker-routed
            // chunk store. Restart-applied knobs build from the persisted
            // config; file crypto configures iff the group key is
            // provisioned, else actions fail closed until it is.
            let files_backend = Arc::new(crate::file_store::FileBackend::new_offline(
                worker.clone(),
            ));
            let chunk_store = Arc::new(crate::file_store::WorkerChunkStore::new(
                worker.clone(),
            ));
            let files = Arc::new(murmur_files::hosts::Hosts::open(
                files_backend.clone(),
                chunk_store,
            ));
            let file_knobs = worker.call(|s| {
                let mut map = std::collections::BTreeMap::new();
                for (full, _) in murmur_files::adapt::PRAGMAS {
                    let stripped = full.strip_prefix("murmur_").unwrap_or(full);
                    map.insert(full.to_string(), config::setting(&s.db, stripped)?);
                }
                murmur_files::adapt::Knobs::from_map(&|name| map.get(name).cloned())
                    .map_err(crate::wait::backend_err)
            })?;
            if let (Some(key), previous) = file_key_slot() {
                let objects_config =
                    murmur_files::adapt::objects_config(&file_knobs, key, previous)
                        .map_err(crate::wait::backend_err)?;
                // Loud: rotation rules (previous-required, one-step) fail
                // the open rather than mis-decrypting.
                files
                    .configure_files(objects_config)
                    .map_err(crate::wait::backend_err)?;
            }
            let file_db_parent = std::path::Path::new(&path)
                .parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_default();
            Arc::new(Control {
                worker,
                handle: Mutex::new(None),
                last_error: Mutex::new(None),
                files,
                files_backend,
                file_knobs,
                file_db_parent,
            })
        }
    };
    // Query plane is per-connection state: managed-key UDFs and the vec0
    // module must exist on every registering connection (app + worker),
    // since shadow tables live in main schema and are read everywhere.
    murmur_query::primary_key::register(&db).map_err(config::query_err)?;
    murmur_query::search::ensure_vec_module(&db).map_err(config::query_err)?;
    let (maintenance_stop, maintenance_thread) = if fresh {
        // Maintenance thread: drives search indexing + subscription
        // commits on the worker with or without the replication engine.
        // G7: the tick upgrades weak refs to strong Arcs for the whole
        // iteration, so destroy() must stop+join it — a detached thread
        // keeps Control/Worker/Store alive past close and trips
        // unregister BUSY. PragmaContext owns the stop flag + handle.
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = stop.clone();
        let weak = Arc::downgrade(&control);
        let weak_worker = Arc::downgrade(&control.worker);
        let thread = std::thread::Builder::new()
            .name("murmur-query-tick".into())
            .spawn(move || {
                let mut failures = 0u32;
                let mut last_retention = std::time::Instant::now();
                let mut last_fetch = std::time::Instant::now();
                loop {
                    std::thread::sleep(std::time::Duration::from_millis(50));
                    if stop_clone.load(Ordering::Acquire) {
                        break;
                    }
                    let (Some(control), Some(worker)) =
                        (weak.upgrade(), weak_worker.upgrade())
                    else {
                        break;
                    };
                    match worker.call(|s| s.query_tick_due()) {
                        Err(_) => break, // worker gone
                        // The whole tick is one bounded maintenance unit
                        // (index work is iteration-capped inside).
                        Ok(true) => match worker.call_admitted(
                            murmur_sched::WriterClass::Maintenance,
                            crate::storage::ADMISSION_TIMEOUT,
                            |s| s.query_tick(),
                        ) {
                            Ok(()) => failures = 0,
                            Err(_) => {
                                failures += 1;
                                if failures >= 20 {
                                    break;
                                }
                            }
                        },
                        Ok(false) => failures = 0,
                    }
                    // Retention floors + staging reaper at the archived
                    // 60s cadence. Best-effort: errors retry next
                    // interval (staleness is visible via the retention
                    // status) and never kill the query ticks.
                    if last_retention.elapsed() >= std::time::Duration::from_secs(60) {
                        last_retention = std::time::Instant::now();
                        retention_pass(&control, &worker);
                    }
                    // File fetch tick, throttled by the restart-applied
                    // fetch interval. Reports idle until start_fetch;
                    // best-effort like retention (errors retry next
                    // interval).
                    if last_fetch.elapsed() >= control.file_knobs.fetch_interval {
                        last_fetch = std::time::Instant::now();
                        // Phase-3: membership sources (uniform-port
                        // derivation; empty pre-start/disabled).
                        let sources = control.fetch_sources();
                        let _ = control.files.fetch_tick(sources);
                    }
                }
            })
            .map_err(Error::Io)?;
        (stop, Some(thread))
    } else {
        (Arc::new(AtomicBool::new(false)), None)
    };
    let flags = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DIRECTONLY;
    for name in ["murmur_start", "murmur_stop", "murmur_sync"] {
        let c = control.clone();
        let raw = raw as usize;
        db.create_scalar_function(name, 0, flags, move |_| {
            if unsafe { ffi::sqlite3_get_autocommit(raw as *mut ffi::sqlite3) } == 0 {
                return Err(c.fail(Error::Invalid(
                    "replication controls require autocommit".into(),
                )));
            }
            match name {
                "murmur_start" => c.start().map_err(|e| c.fail(e)),
                "murmur_stop" => Ok(c.stop()),
                _ => {
                    let h = c.handle.lock().unwrap();
                    let h = h.as_ref().ok_or_else(|| {
                        c.fail(Error::Unavailable("replication is stopped".into()))
                    })?;
                    h.live.sync.notify_waiters();
                    Ok(1)
                }
            }
        })?;
    }
    let c = control.clone();
    db.create_scalar_function("murmur_gc", 0, flags, move |_| {
        // On-demand retention pass (floors + tombstone GC + staging
        // reap). Returns tombstones pruned; the 60s tick runs the same
        // pass and ignores the counts.
        Ok(retention_pass(&c, &c.worker).1 as i64)
    })?;
    let c = control.clone();
    let raw_addr = raw as usize;
    db.create_scalar_function("murmur_backup", 1, flags, move |ctx| {
        if unsafe { ffi::sqlite3_get_autocommit(raw_addr as *mut ffi::sqlite3) } == 0 {
            return Err(c.fail(Error::Invalid(
                "backup requires autocommit".into(),
            )));
        }
        let dest: String = ctx.get(0)?;
        let dest = std::ffi::CString::new(dest)
            .map_err(|_| c.fail(Error::Invalid("backup path contains NUL".into())))?;
        // Online backup runs on the worker handle (same Store by path);
        // committed state only — the dispatch flushes completed murmur
        // transactions before the checkpoint observes them.
        c.worker
            .call(move |s| {
                let group = config::setting(&s.db, "group_id")?;
                let group = parse_hex_id(&group)?;
                let node: Option<Vec<u8>> = s
                    .db
                    .query_row("SELECT node FROM murmur_identity WHERE id=1", [], |r| {
                        r.get(0)
                    })?;
                let mut node_id = [0u8; 16];
                if let Some(n) = node
                    && n.len() == 16
                {
                    node_id.copy_from_slice(&n);
                }
                let mut args = BackupArgs {
                    dest: dest.as_ptr(),
                    group,
                    node: node_id,
                };
                let rc = unsafe {
                    ffi::sqlite3_file_control(
                        s.db.handle(),
                        c"main".as_ptr(),
                        BACKUP,
                        &mut args as *mut BackupArgs as *mut std::ffi::c_void,
                    )
                };
                match rc {
                    ffi::SQLITE_OK => Ok(1),
                    ffi::SQLITE_ERROR | ffi::SQLITE_MISUSE => Err(Error::Invalid(format!(
                        "backup rejected (rc {rc}): destination must not exist"
                    ))),
                    _ => Err(Error::Unavailable(format!("backup failed (rc {rc})"))),
                }
            })
            .map_err(|e| c.fail(e))
    })?;
    let c = control.clone();
    let raw_addr = raw as usize;
    db.create_scalar_function("murmur_compact", 0, flags, move |_| {
        if unsafe { ffi::sqlite3_get_autocommit(raw_addr as *mut ffi::sqlite3) } == 0 {
            return Err(c.fail(Error::Invalid(
                "compact requires autocommit".into(),
            )));
        }
        // Full-range compaction on the worker handle. Blocking: large
        // stores stall the calling statement until the merge finishes.
        c.worker
            .call(|s| {
                let rc = unsafe {
                    ffi::sqlite3_file_control(
                        s.db.handle(),
                        c"main".as_ptr(),
                        COMPACT,
                        std::ptr::null_mut(),
                    )
                };
                match rc {
                    ffi::SQLITE_OK => Ok(1),
                    _ => Err(Error::Unavailable(format!("compact failed (rc {rc})"))),
                }
            })
            .map_err(|e| c.fail(e))
    })?;
    let c = control.clone();
    db.create_scalar_function("murmur_key_inventory", 0, flags, move |_| {
        // Read-only over immutable post-seal state: no autocommit guard,
        // so operators can inspect the envelope inside a transaction.
        c.worker
            .call(|s| {
                let mut args = KeyringArgs::inventory();
                let rc = unsafe {
                    ffi::sqlite3_file_control(
                        s.db.handle(),
                        c"main".as_ptr(),
                        KEYRING,
                        (&mut args as *mut KeyringArgs).cast(),
                    )
                };
                if rc != ffi::SQLITE_OK || args.json.is_null() {
                    return Err(Error::Unavailable(format!(
                        "key inventory unavailable (rc {rc})"
                    )));
                }
                let out = unsafe { CStr::from_ptr(args.json) }
                    .to_string_lossy()
                    .into_owned();
                unsafe { ffi::sqlite3_free(args.json.cast()) };
                Ok(out)
            })
            .map_err(|e| c.fail(e))
    })?;
    // WP-D slice D2b: online rotation + conservative prune. Autocommit
    // guards (mutating actions); no replication guards — page crypto is
    // local and rotation changes no logical content (ruling T).
    let c = control.clone();
    let raw_addr = raw as usize;
    db.create_scalar_function("murmur_rotate_key", 0, flags, move |_| {
        if unsafe { ffi::sqlite3_get_autocommit(raw_addr as *mut ffi::sqlite3) } == 0 {
            return Err(c.fail(Error::Invalid(
                "rotation requires autocommit".into(),
            )));
        }
        c.worker
            .call(|s| {
                let mut args = KeyringArgs::rotate();
                let rc = unsafe {
                    ffi::sqlite3_file_control(
                        s.db.handle(),
                        c"main".as_ptr(),
                        KEYRING,
                        (&mut args as *mut KeyringArgs).cast(),
                    )
                };
                match rc {
                    ffi::SQLITE_OK => Ok(args.value as i64),
                    ffi::SQLITE_ERROR => Err(Error::Invalid(
                        "rotation requires a sealed direct envelope".into(),
                    )),
                    _ => Err(Error::Unavailable(format!(
                        "rotation failed (rc {rc})"
                    ))),
                }
            })
            .map_err(|e| c.fail(e))
    })?;
    let c = control.clone();
    let raw_addr = raw as usize;
    db.create_scalar_function("murmur_prune_keys", 0, flags, move |_| {
        if unsafe { ffi::sqlite3_get_autocommit(raw_addr as *mut ffi::sqlite3) } == 0 {
            return Err(c.fail(Error::Invalid(
                "prune requires autocommit".into(),
            )));
        }
        // Blocking full key-space scan on large stores; the Store
        // reports {pruned, blocked_by_unkeyed, retained, unreadable}.
        c.worker
            .call(|s| {
                let mut args = KeyringArgs::prune();
                let rc = unsafe {
                    ffi::sqlite3_file_control(
                        s.db.handle(),
                        c"main".as_ptr(),
                        KEYRING,
                        (&mut args as *mut KeyringArgs).cast(),
                    )
                };
                if rc == ffi::SQLITE_ERROR {
                    return Err(Error::Invalid(
                        "prune requires a direct envelope".into(),
                    ));
                }
                if rc != ffi::SQLITE_OK || args.json.is_null() {
                    return Err(Error::Unavailable(format!(
                        "prune failed (rc {rc})"
                    )));
                }
                let out = unsafe { CStr::from_ptr(args.json) }
                    .to_string_lossy()
                    .into_owned();
                unsafe { ffi::sqlite3_free(args.json.cast()) };
                Ok(out)
            })
            .map_err(|e| c.fail(e))
    })?;
    // WP-C file actions (12). Metadata shapes return JSON text; pure
    // reads skip the autocommit guard, side effects require it.
    let c = control.clone();
    let raw_addr = raw as usize;
    db.create_scalar_function("murmur_file_upload", 2, flags, move |ctx| {
        file_autocommit(raw_addr, &c)?;
        let name: String = ctx.get(0)?;
        let path: String = ctx.get(1)?;
        let meta = c
            .files
            .upload(&name, std::path::Path::new(&path))
            .map_err(|e| file_err(&c, e))?;
        file_json(&c, &meta)
    })?;
    let c = control.clone();
    let raw_addr = raw as usize;
    db.create_scalar_function("murmur_file_download", 2, flags, move |ctx| {
        file_autocommit(raw_addr, &c)?;
        let name: String = ctx.get(0)?;
        let dest: String = ctx.get(1)?;
        let bytes = c
            .files
            .download(&name, std::path::Path::new(&dest))
            .map_err(|e| file_err(&c, e))?;
        file_int(&c, bytes)
    })?;
    let c = control.clone();
    db.create_scalar_function("murmur_file_read", 3, flags, move |ctx| {
        let name: String = ctx.get(0)?;
        let offset: i64 = ctx.get(1)?;
        let limit: i64 = ctx.get(2)?;
        let offset = file_u64(&c, "offset", offset)?;
        let limit = usize::try_from(limit)
            .map_err(|_| c.fail(Error::Invalid("limit must be non-negative".into())))?;
        c.files
            .read_chunk(&name, offset, limit)
            .map_err(|e| file_err(&c, e))
    })?;
    let c = control.clone();
    let raw_addr = raw as usize;
    db.create_scalar_function("murmur_file_delete", 1, flags, move |ctx| {
        file_autocommit(raw_addr, &c)?;
        let name: String = ctx.get(0)?;
        c.files.delete(&name).map_err(|e| file_err(&c, e))?;
        Ok(1)
    })?;
    let c = control.clone();
    db.create_scalar_function("murmur_file_list", 1, flags, move |ctx| {
        let pattern: String = ctx.get(0)?;
        let pattern = if pattern.is_empty() { None } else { Some(pattern.as_str()) };
        let metas = c.files.list(pattern).map_err(|e| file_err(&c, e))?;
        file_json(&c, &metas)
    })?;
    let c = control.clone();
    db.create_scalar_function("murmur_file_metadata", 1, flags, move |ctx| {
        let name: String = ctx.get(0)?;
        match c.files.metadata(&name).map_err(|e| file_err(&c, e))? {
            Some(meta) => Ok(Some(file_json(&c, &meta)?)),
            None => Ok(None),
        }
    })?;
    let c = control.clone();
    db.create_scalar_function("murmur_file_availability", 1, flags, move |ctx| {
        let name: String = ctx.get(0)?;
        c.files.availability(&name).map_err(|e| file_err(&c, e))
    })?;
    let c = control.clone();
    let raw_addr = raw as usize;
    db.create_scalar_function("murmur_file_fetch", 0, flags, move |_| {
        file_autocommit(raw_addr, &c)?;
        // §6 split landed (Phase-2): one manual fetch round over the
        // SQLite object store. 1 ran / 0 idle (idle until start_fetch,
        // which Phase-3 wires into Control::start with engine creds).
        let ran = c
            .files
            .fetch_tick(c.fetch_sources())
            .map_err(|e| file_err(&c, e))?;
        file_int(&c, u64::from(ran))
    })?;
    let c = control.clone();
    db.create_scalar_function("murmur_file_last_error", 0, flags, move |_| {
        c.files.last_error().map_err(|e| file_err(&c, e))
    })?;
    let c = control.clone();
    let raw_addr = raw as usize;
    db.create_scalar_function("murmur_file_collect", 1, flags, move |ctx| {
        file_autocommit(raw_addr, &c)?;
        let grace_ms: i64 = ctx.get(0)?;
        let removed = c
            .files
            .collect(file_u64(&c, "grace_ms", grace_ms)?)
            .map_err(|e| file_err(&c, e))?;
        file_int(&c, u64::try_from(removed).map_err(|_| c.fail(Error::Overloaded))?)
    })?;
    let c = control.clone();
    let raw_addr = raw as usize;
    db.create_scalar_function("murmur_file_rotate", 1, flags, move |ctx| {
        file_autocommit(raw_addr, &c)?;
        let limit: i64 = ctx.get(0)?;
        let limit = usize::try_from(limit)
            .map_err(|_| c.fail(Error::Invalid("limit must be non-negative".into())))?;
        let done = c.files.rotate(limit).map_err(|e| file_err(&c, e))?;
        file_int(&c, u64::try_from(done).map_err(|_| c.fail(Error::Overloaded))?)
    })?;
    let c = control.clone();
    db.create_scalar_function("murmur_file_rotation_status", 0, flags, move |_| {
        let status = c.files.rotation_status().map_err(|e| file_err(&c, e))?;
        file_json(&c, &status)
    })?;
    // Point 7 policy ops (2). All side-effecting: autocommit required.
    let c = control.clone();
    let raw_addr = raw as usize;
    db.create_scalar_function("murmur_policy_op", 5, flags, move |ctx| {
        if unsafe { ffi::sqlite3_get_autocommit(raw_addr as *mut ffi::sqlite3) } == 0 {
            return Err(c.fail(Error::Invalid("policy ops require autocommit".into())));
        }
        let table: String = ctx.get(0)?;
        let column: String = ctx.get(1)?;
        let pk: rusqlite::types::Value = ctx.get(2)?;
        let op: String = ctx.get(3)?;
        let value: rusqlite::types::Value = ctx.get(4)?;
        let out = c
            .worker
            .call(move |s| s.policy_op(&table, &column, &pk, &op, &value))
            .map_err(|e| c.fail(e))?;
        Ok(out.unwrap_or(rusqlite::types::Value::Null))
    })?;
    let c = control.clone();
    let raw_addr = raw as usize;
    db.create_scalar_function("murmur_policy_recompute", 3, flags, move |ctx| {
        if unsafe { ffi::sqlite3_get_autocommit(raw_addr as *mut ffi::sqlite3) } == 0 {
            return Err(c.fail(Error::Invalid("policy ops require autocommit".into())));
        }
        let table: String = ctx.get(0)?;
        let column: String = ctx.get(1)?;
        let pk: rusqlite::types::Value = ctx.get(2)?;
        let out = c
            .worker
            .call(move |s| s.policy_recompute(&table, &column, &pk))
            .map_err(|e| c.fail(e))?;
        Ok(out.unwrap_or(rusqlite::types::Value::Null))
    })?;
    // Point 8 High/Low ownership (4). Side-effecting: autocommit required.
    for (name, nargs) in [
        ("murmur_hl_grant", 3),
        ("murmur_hl_release", 3),
        ("murmur_hl_projection", 3),
    ] {
        let c = control.clone();
        let raw_addr = raw as usize;
        db.create_scalar_function(name, nargs, flags, move |ctx| {
            if unsafe { ffi::sqlite3_get_autocommit(raw_addr as *mut ffi::sqlite3) } == 0 {
                return Err(c.fail(Error::Invalid("ownership actions require autocommit".into())));
            }
            let table: String = ctx.get(0)?;
            let pk: rusqlite::types::Value = ctx.get(1)?;
            let column: String = ctx.get(2)?;
            c.worker
                .call(move |s| match name {
                    "murmur_hl_grant" => s.hl_grant(&table, &pk, &column),
                    "murmur_hl_release" => s.hl_release(&table, &pk, &column),
                    _ => s.hl_projection(&table, &pk, &column),
                })
                .map_err(|e| c.fail(e))
        })?;
    }
    let c = control.clone();
    let raw_addr = raw as usize;
    db.create_scalar_function("murmur_hl_reduce", 0, flags, move |_| {
        if unsafe { ffi::sqlite3_get_autocommit(raw_addr as *mut ffi::sqlite3) } == 0 {
            return Err(c.fail(Error::Invalid("ownership actions require autocommit".into())));
        }
        c.worker
            .call(move |s| s.hl_reduce())
            .map_err(|e| c.fail(e))
            .map(|n| n as i64)
    })?;
    // WP-C bridge actions (10). All side-effecting: autocommit required.
    let c = control.clone();
    let raw_addr = raw as usize;
    db.create_scalar_function("murmur_bridge_export", 1, flags, move |ctx| {
        file_autocommit(raw_addr, &c)?;
        let payload_hex: String = ctx.get(0)?;
        let path = c.files.bridge_export(&payload_hex).map_err(|e| file_err(&c, e))?;
        Ok(path.to_string_lossy().into_owned())
    })?;
    let c = control.clone();
    let raw_addr = raw as usize;
    db.create_scalar_function("murmur_bridge_process", 0, flags, move |_| {
        file_autocommit(raw_addr, &c)?;
        let applied = c.files.bridge_process().map_err(|e| file_err(&c, e))?;
        file_int(&c, u64::try_from(applied).map_err(|_| c.fail(Error::Overloaded))?)
    })?;
    let c = control.clone();
    let raw_addr = raw as usize;
    db.create_scalar_function("murmur_bridge_capture", 0, flags, move |_| {
        file_autocommit(raw_addr, &c)?;
        let exported = c.files.bridge_capture().map_err(|e| file_err(&c, e))?;
        file_int(&c, u64::try_from(exported).map_err(|_| c.fail(Error::Overloaded))?)
    })?;
    let c = control.clone();
    let raw_addr = raw as usize;
    db.create_scalar_function("murmur_bridge_stage", 1, flags, move |ctx| {
        file_autocommit(raw_addr, &c)?;
        let path: String = ctx.get(0)?;
        let header = c
            .files
            .bridge_stage(std::path::Path::new(&path))
            .map_err(|e| file_err(&c, e))?;
        file_json(&c, &header)
    })?;
    let c = control.clone();
    let raw_addr = raw as usize;
    db.create_scalar_function("murmur_bridge_export_file", 1, flags, move |ctx| {
        file_autocommit(raw_addr, &c)?;
        let name: String = ctx.get(0)?;
        // Fails closed until the §6 store split (canonical error).
        c.files.bridge_export_file(&name).map_err(|e| file_err(&c, e))?;
        Ok(1)
    })?;
    let c = control.clone();
    let raw_addr = raw as usize;
    db.create_scalar_function("murmur_bridge_export_snapshot", 0, flags, move |_| {
        file_autocommit(raw_addr, &c)?;
        let path = c.files.bridge_export_snapshot().map_err(|e| file_err(&c, e))?;
        Ok(path.to_string_lossy().into_owned())
    })?;
    let c = control.clone();
    let raw_addr = raw as usize;
    db.create_scalar_function("murmur_bridge_release", 3, flags, move |ctx| {
        file_autocommit(raw_addr, &c)?;
        let table: String = ctx.get(0)?;
        let key_hex: String = ctx.get(1)?;
        let column: Option<String> = ctx.get(2)?;
        let applied = c
            .files
            .bridge_release(&table, &key_hex, column.as_deref())
            .map_err(|e| file_err(&c, e))?;
        Ok(i64::from(applied))
    })?;
    let c = control.clone();
    let raw_addr = raw as usize;
    db.create_scalar_function("murmur_bridge_configure", 1, flags, move |ctx| {
        file_autocommit(raw_addr, &c)?;
        let json: String = ctx.get(0)?;
        c.files
            .configure_bridge_json(&c.file_knobs, &c.file_db_parent, &json)
            .map_err(|e| file_err(&c, e))?;
        Ok(1)
    })?;
    let c = control.clone();
    let raw_addr = raw as usize;
    db.create_scalar_function("murmur_bridge_publish_ftp", 4, flags, move |ctx| {
        file_autocommit(raw_addr, &c)?;
        let path: String = ctx.get(0)?;
        let url: String = ctx.get(1)?;
        let roots_pem: Vec<u8> = ctx.get(2)?;
        let timeout_ms: i64 = ctx.get(3)?;
        let timeout_ms = file_u64(&c, "timeout_ms", timeout_ms)?;
        let timeout = std::time::Duration::from_millis(if timeout_ms == 0 { 30_000 } else { timeout_ms });
        c.files
            .publish_ftp(std::path::Path::new(&path), &url, roots_pem, timeout)
            .map_err(|e| file_err(&c, e))?;
        Ok(1)
    })?;
    let c = control.clone();
    let raw_addr = raw as usize;
    db.create_scalar_function("murmur_bridge_publish_http", 2, flags, move |ctx| {
        file_autocommit(raw_addr, &c)?;
        let path: String = ctx.get(0)?;
        let url: String = ctx.get(1)?;
        c.files
            .publish_http(std::path::Path::new(&path), &url)
            .map_err(|e| file_err(&c, e))?;
        Ok(1)
    })?;
    let c = control.clone();
    db.create_scalar_function("murmur_status", 0, flags, move |_| Ok(c.status()))?;
    let c = control.clone();
    db.create_scalar_function("murmur_health", 0, flags, move |_| Ok(c.health()))?;
    let c = control.clone();
    let raw_addr = raw as usize;
    db.create_scalar_function("murmur_add_peer",1,flags,move|ctx|{
        if unsafe{ffi::sqlite3_get_autocommit(raw_addr as *mut ffi::sqlite3)}==0{return Err(c.fail(Error::Invalid("peer controls require autocommit".into())));}
        let address:String=ctx.get(0)?;let addr=address.parse::<SocketAddr>().map_err(|_|c.fail(Error::Invalid("expected IP:port".into())))?;
        let h=c.handle.lock().unwrap();
        c.worker.call(move|s|{let mut seeds:Vec<SocketAddr>=serde_json::from_str(&config::setting(&s.db,"bootstrap")?).map_err(|e|Error::Invalid(e.to_string()))?;if !seeds.contains(&addr){if seeds.len()>=4096{return Err(Error::Overloaded);}seeds.push(addr);}
            s.db.execute("INSERT INTO murmur_config VALUES('bootstrap',?1) ON CONFLICT(name) DO UPDATE SET value=excluded.value",[serde_json::to_string(&seeds).unwrap()])?;s.flush()}).map_err(|e| c.fail(e))?;
        if let Some(h)=h.as_ref(){h.live.add(addr).map_err(|e| c.fail(e))?;}Ok(1)
    })?;
    let c = control.clone();
    db.create_scalar_function("murmur_remove_peer", 1, flags, move |ctx| {
        if unsafe { ffi::sqlite3_get_autocommit(raw_addr as *mut ffi::sqlite3) } == 0 {
            return Err(c.fail(Error::Invalid(
                "peer controls require autocommit".into(),
            )));
        }
        let id = parse_id(&ctx.get::<String>(0)?).map_err(|e| c.fail(e))?;
        let h = c.handle.lock().unwrap();
        c.worker
            .call(move |s| {
                s.db.execute(
                    "INSERT OR IGNORE INTO murmur_excluded VALUES(?)",
                    [id.as_slice()],
                )?;
                s.flush()
            })
            .map_err(|e| c.fail(e))?;
        if let Some(h) = h.as_ref() {
            h.live.remove(id);
        }
        Ok(1)
    })?;
    let c = control.clone();
    db.create_scalar_function("murmur_retire_peer", 1, flags, move |ctx| {
        if unsafe { ffi::sqlite3_get_autocommit(raw_addr as *mut ffi::sqlite3) } == 0 {
            return Err(c.fail(Error::Invalid(
                "peer controls require autocommit".into(),
            )));
        }
        let id = parse_id(&ctx.get::<String>(0)?).map_err(|e| c.fail(e))?;
        // Exclusion first (transport safety), then the retention
        // obligation ends. Either crash window self-heals: a retired
        // but connected peer is ignored by floors, an excluded but
        // unretired peer ages out via the offline window. The handle
        // lock is never held across backend_opt (it locks the same
        // mutex and would self-deadlock).
        c.worker
            .call(move |s| {
                s.db.execute(
                    "INSERT OR IGNORE INTO murmur_excluded VALUES(?)",
                    [id.as_slice()],
                )?;
                s.flush()
            })
            .map_err(|e| c.fail(e))?;
        if let Some(b) = c.backend_opt().map_err(|e| c.fail(e))? {
            murmur_tx::retention::retire(&b, id)
                .map_err(|e| c.fail(crate::wait::backend_err(e)))?;
        }
        if let Some(h) = c.handle.lock().unwrap().as_ref() {
            h.live.remove(id);
        }
        Ok(1)
    })?;
    let c = control.clone();
    db.create_scalar_function("murmur_snapshot_request", 1, flags, move |ctx| {
        if unsafe { ffi::sqlite3_get_autocommit(raw_addr as *mut ffi::sqlite3) } == 0 {
            return Err(c.fail(Error::Invalid(
                "peer controls require autocommit".into(),
            )));
        }
        let id = parse_id(&ctx.get::<String>(0)?).map_err(|e| c.fail(e))?;
        // Clone the Arc under the lock, then queue without holding it.
        let live = c.handle.lock().unwrap().as_ref().map(|h| h.live.clone());
        let Some(live) = live else {
            return Err(c.fail(Error::Unavailable("replication is not running".into())));
        };
        crate::engine::Live::snapshot_request(&live, id).map_err(|e| c.fail(e))?;
        Ok(1)
    })?;
    let c = control.clone();
    db.create_scalar_function("murmur_wait_applied", 3, flags, move |ctx| {
        if unsafe { ffi::sqlite3_get_autocommit(raw_addr as *mut ffi::sqlite3) } == 0 {
            return Err(c.fail(Error::Invalid(
                "waits require autocommit".into(),
            )));
        }
        let origin = parse_id(&ctx.get::<String>(0)?).map_err(|e| c.fail(e))?;
        let seq = u64::try_from(ctx.get::<i64>(1)?)
            .map_err(|_| c.fail(Error::Invalid("sequence must be >= 0".into())))?;
        let timeout = wait_timeout_ms(ctx.get(2)?).map_err(|e| c.fail(e))?;
        match c.backend_opt().map_err(|e| c.fail(e))? {
            Some(b) => Ok(wait::wait_for_applied(&b, origin, seq, timeout).map_err(|e| c.fail(e))?
                as i64),
            None => Ok(0),
        }
    })?;
    let c = control.clone();
    db.create_scalar_function("murmur_wait_lag", 2, flags, move |ctx| {
        if unsafe { ffi::sqlite3_get_autocommit(raw_addr as *mut ffi::sqlite3) } == 0 {
            return Err(c.fail(Error::Invalid(
                "waits require autocommit".into(),
            )));
        }
        let bound = u64::try_from(ctx.get::<i64>(0)?)
            .map_err(|_| c.fail(Error::Invalid("bound must be >= 0".into())))?;
        let timeout = wait_timeout_ms(ctx.get(1)?).map_err(|e| c.fail(e))?;
        match c.backend_opt().map_err(|e| c.fail(e))? {
            Some(b) => {
                Ok(wait::wait_for_lag(&b, bound, timeout).map_err(|e| c.fail(e))? as i64)
            }
            None => Ok(0),
        }
    })?;
    let c = control.clone();
    db.create_scalar_function("murmur_progress", 0, flags, move |_| {
        let heads = match c.backend_opt().map_err(|e| c.fail(e))? {
            Some(b) => b.progress().map_err(|e| c.fail(wait::backend_err(e)))?,
            None => Vec::new(),
        };
        let rows: Vec<_> = heads
            .iter()
            .map(|p| {
                serde_json::json!({"origin":hex(&p.origin),"applied":p.applied,"observed":p.observed,"retained_from":p.retained_from,"retained_through":p.retained_through})
            })
            .collect();
        Ok(serde_json::Value::Array(rows).to_string())
    })?;
    // Query-plane actions (WP-E): search, migrations, batch exec,
    // export/import. Mutating actions require autocommit; reads do not.
    macro_rules! query_scalar {
        ($name:literal, $nargs:literal, requires_autocommit, |$c:ident, $ctx:ident| $body:block) => {{
            let $c = control.clone();
            let raw_addr = raw as usize;
            db.create_scalar_function($name, $nargs, flags, move |$ctx| {
                if unsafe { ffi::sqlite3_get_autocommit(raw_addr as *mut ffi::sqlite3) } == 0 {
                    return Err($c.fail(Error::Invalid(
                        "query actions require autocommit".into(),
                    )));
                }
                $body
            })?;
        }};
        ($name:literal, $nargs:literal, readonly, |$c:ident, $ctx:ident| $body:block) => {{
            let $c = control.clone();
            db.create_scalar_function($name, $nargs, flags, move |$ctx| $body)?;
        }};
    }
    query_scalar!("murmur_search_create_fts", 4, requires_autocommit, |c, ctx| {
        let name: String = ctx.get(0)?;
        let table: String = ctx.get(1)?;
        let columns: String = ctx.get(2)?;
        let tokenizer: String = ctx.get(3)?;
        c.worker
            .call_admitted(
                murmur_sched::WriterClass::Maintenance,
                crate::storage::ADMISSION_TIMEOUT,
                move |s| s.query_create_fts(&name, &table, &columns, &tokenizer),
            )
            .map_err(|e| c.fail(e))?;
        Ok(1)
    });
    query_scalar!("murmur_search_create_vec", 5, requires_autocommit, |c, ctx| {
        let name: String = ctx.get(0)?;
        let table: String = ctx.get(1)?;
        let column: String = ctx.get(2)?;
        let dimensions = ctx.get::<i64>(3)?;
        if dimensions < 1 {
            return Err(c.fail(Error::Invalid("dimensions must be >= 1".into())));
        }
        let metric: String = ctx.get(4)?;
        c.worker
            .call_admitted(
                murmur_sched::WriterClass::Maintenance,
                crate::storage::ADMISSION_TIMEOUT,
                move |s| {
                    s.query_create_vec(&name, &table, &column, dimensions as usize, &metric)
                },
            )
            .map_err(|e| c.fail(e))?;
        Ok(1)
    });
    query_scalar!("murmur_search_drop", 1, requires_autocommit, |c, ctx| {
        let name: String = ctx.get(0)?;
        c.worker
            .call_admitted(
                murmur_sched::WriterClass::Maintenance,
                crate::storage::ADMISSION_TIMEOUT,
                move |s| s.query_drop_index(&name),
            )
            .map_err(|e| c.fail(e))
    });
    query_scalar!("murmur_refresh_indexes", 0, requires_autocommit, |c, _ctx| {
        c.worker
            .call_admitted(
                murmur_sched::WriterClass::Maintenance,
                crate::storage::ADMISSION_TIMEOUT,
                |s| s.query_refresh(),
            )
            .map_err(|e| c.fail(e))
    });
    query_scalar!("murmur_search_status", 0, readonly, |c, _ctx| {
        c.worker
            .call(|s| Ok(s.query_status()))
            .map_err(|e| c.fail(e))
    });
    query_scalar!("murmur_column_fts", 3, requires_autocommit, |c, ctx| {
        let table: String = ctx.get(0)?;
        let column: String = ctx.get(1)?;
        let enabled: i64 = ctx.get(2)?;
        c.worker
            .call_admitted(
                murmur_sched::WriterClass::Maintenance,
                crate::storage::ADMISSION_TIMEOUT,
                move |s| s.query_set_column_fts(&table, &column, enabled != 0),
            )
            .map_err(|e| c.fail(e))?;
        Ok(1)
    });
    query_scalar!("murmur_column_vector", 4, requires_autocommit, |c, ctx| {
        let table: String = ctx.get(0)?;
        let column: String = ctx.get(1)?;
        let dimensions: Option<i64> = ctx.get(2)?;
        let metric: Option<String> = ctx.get(3)?;
        let (dimensions, metric) = match (dimensions, metric) {
            (None, None) => (None, None),
            (Some(d), Some(m)) if d >= 1 => (Some(d as usize), Some(m)),
            _ => {
                return Err(c.fail(Error::Invalid(
                    "dimensions and metric must both be set or both NULL".into(),
                )));
            }
        };
        c.worker
            .call_admitted(
                murmur_sched::WriterClass::Maintenance,
                crate::storage::ADMISSION_TIMEOUT,
                move |s| s.query_set_column_vector(&table, &column, dimensions, metric),
            )
            .map_err(|e| c.fail(e))?;
        Ok(1)
    });
    query_scalar!("murmur_migrate", 2, requires_autocommit, |c, ctx| {
        let sql: String = ctx.get(0)?;
        let mappings: String = ctx.get(1)?;
        c.worker
            .call_admitted(
                murmur_sched::WriterClass::Maintenance,
                crate::storage::ADMISSION_TIMEOUT,
                move |s| s.query_migrate(&sql, &mappings),
            )
            .map_err(|e| c.fail(e))
    });
    query_scalar!("murmur_exec_json", 1, requires_autocommit, |c, ctx| {
        let statements: String = ctx.get(0)?;
        c.worker
            .call(move |s| s.query_exec_json(&statements))
            .map_err(|e| c.fail(e))
    });
    query_scalar!("murmur_export", 1, readonly, |c, ctx| {
        let since: i64 = ctx.get(0)?;
        c.worker
            .call(move |s| s.query_export(since))
            .map_err(|e| c.fail(e))
    });
    query_scalar!("murmur_import", 1, requires_autocommit, |c, ctx| {
        let state: String = ctx.get(0)?;
        c.worker
            .call(move |s| s.query_import(&state))
            .map_err(|e| c.fail(e))?;
        Ok(1)
    });
    const MODULE: vtab::Module<PeerTable> = vtab::Module::eponymous_only_module();
    db.create_module(c"murmur_peers", &MODULE, Some(control.clone()))?;
    let ctx = Box::into_raw(Box::new(PragmaContext {
        control: control.clone(),
        db: raw as usize,
        maintenance_stop,
        maintenance_thread,
    }));
    let mut bridge = Bridge {
        context: ctx.cast(),
        pragma,
        destroy,
    };
    let rc = unsafe {
        ffi::sqlite3_file_control(
            raw,
            c"main".as_ptr(),
            REGISTER,
            (&mut bridge as *mut Bridge).cast(),
        )
    };
    if rc != 0 {
        unsafe {
            destroy(ctx.cast());
        }
        return Err(Error::Invalid(format!(
            "VFS extension registration failed ({rc})"
        )));
    }
    services.insert(path, Arc::downgrade(&control));
    Ok(())
}
#[repr(C)]
struct PeerTable {
    base: ffi::sqlite3_vtab,
    control: Arc<Control>,
}
#[repr(C)]
struct PeerCursor {
    base: ffi::sqlite3_vtab_cursor,
    control: Arc<Control>,
    rows: Vec<serde_json::Value>,
    index: usize,
}
unsafe impl<'v> vtab::VTab<'v> for PeerTable {
    type Aux = Arc<Control>;
    type Cursor = PeerCursor;
    fn connect(
        db: &mut vtab::VTabConnection,
        aux: Option<&Self::Aux>,
        _: &[u8],
        _: &[u8],
        _: &[u8],
        _: &[&[u8]],
    ) -> rusqlite::Result<(std::borrow::Cow<'static, CStr>, Self)> {
        db.config(vtab::VTabConfig::DirectOnly)?;
        Ok((std::borrow::Cow::Borrowed(c"CREATE TABLE x(node_id TEXT,address TEXT,state TEXT,connected INTEGER,incarnation INTEGER)"),Self{base:Default::default(),control:aux.unwrap().clone()}))
    }
    fn best_index(&self, info: &mut vtab::IndexInfo) -> rusqlite::Result<bool> {
        info.set_estimated_cost(4096.);
        info.set_estimated_rows(4096);
        Ok(true)
    }
    fn open(&'v mut self) -> rusqlite::Result<Self::Cursor> {
        Ok(PeerCursor {
            base: Default::default(),
            control: self.control.clone(),
            rows: Vec::new(),
            index: 0,
        })
    }
}
unsafe impl vtab::VTabCursor for PeerCursor {
    fn filter(&mut self, _: i32, _: Option<&str>, _: &vtab::Filters<'_>) -> rusqlite::Result<()> {
        self.rows = self.control.members();
        self.index = 0;
        Ok(())
    }
    fn next(&mut self) -> rusqlite::Result<()> {
        self.index += 1;
        Ok(())
    }
    fn eof(&self) -> bool {
        self.index >= self.rows.len()
    }
    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(self.index as i64)
    }
    fn column(&self, ctx: &mut vtab::Context, i: i32) -> rusqlite::Result<()> {
        let r = &self.rows[self.index];
        match i {
            0 => ctx.set_result(&r["node_id"].as_str().unwrap_or("")),
            1 => ctx.set_result(&r["address"].as_str().unwrap_or("")),
            2 => ctx.set_result(&r["state"].as_str().unwrap_or("")),
            3 => ctx.set_result(&(r["connected"].as_bool().unwrap_or(false) as i64)),
            _ => ctx.set_result(&(r["incarnation"].as_u64().unwrap_or(0) as i64)),
        }
    }
}
