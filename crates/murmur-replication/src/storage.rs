use crate::{
    protocol::{self, Cursor, Key},
    *,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params, types::ValueRef};
use std::sync::mpsc;
type Job = Box<dyn FnOnce(&mut Store) + Send>;
const FLUSH: i32 = 0x535802;
/// Downstream-owned hybrid-clock key (no archived equivalent; the archived
/// clock lives in the Spool-backed Core). Value is postcard `(u64, u32)`.
pub(crate) const CLOCK_KEY: &[u8] = b"07/replication/clock/hlc";
/// Admission wait bound for scheduled writer jobs. No per-operation
/// deadline exists yet, so engine and maintenance paths share this
/// constant; hitting it surfaces Overloaded, never a silent stall.
pub(crate) const ADMISSION_TIMEOUT: Duration = Duration::from_secs(30);
/// Unit tests link without the cr-sqlite staticlib; the init entry
/// resolves to a stub that reports failure, so `ensure_crsql` degrades
/// to `Unavailable` instead of failing the link. CRR behavior stays
/// covered by the C++ integration tests.
#[cfg(test)]
#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_crsqlite_init(
    _db: *mut rusqlite::ffi::sqlite3,
    _error: *mut *mut std::ffi::c_char,
    _api: *const std::ffi::c_void,
) -> i32 {
    1
}

/// Effective writer-scheduler config: persisted `writer_*` settings, or
/// archive defaults when the store is fresh, unreadable, or invalid.
/// Generations never replace live ones: a SET persists into
/// `murmur_config` and takes effect on next open (health reports the
/// effective config beside the pending persisted values).
fn load_sched_config(path: &str, vfs: &str) -> murmur_sched::SchedulerConfig {
    let default = murmur_sched::SchedulerConfig::default();
    let db = match Connection::open_with_flags_and_vfs(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        vfs,
    ) {
        Ok(db) => db,
        Err(_) => return default,
    };
    let num = |name: &str| -> Option<u64> {
        config::setting(&db, name).ok()?.parse().ok()
    };
    let (Some(local), Some(debt_ms), Some(reserve)) = (
        num("writer_local_share"),
        num("writer_max_debt_ms"),
        num("writer_maintenance_reserve_every"),
    ) else {
        return default;
    };
    sched_config_from(local, debt_ms, reserve)
}

/// Build an effective config from persisted values, falling back to
/// archive defaults on anything outside the validated ranges.
/// Ranges mirror config::validate; u32 casts are safe below 2^32.
fn sched_config_from(local: u64, debt_ms: u64, reserve: u64) -> murmur_sched::SchedulerConfig {
    let default = murmur_sched::SchedulerConfig::default();
    if !(1..=99).contains(&local)
        || !(1..=60000).contains(&debt_ms)
        || !(1..=1000000).contains(&reserve)
    {
        return default;
    }
    let cfg = murmur_sched::SchedulerConfig {
        local_share: local as u32,
        remote_share: (100 - local) as u32,
        max_debt: Duration::from_millis(debt_ms),
        maintenance_reserve_every: reserve,
    };
    // The archived constructor does not validate; check before use.
    if cfg.validate().is_err() {
        return default;
    }
    cfg
}

pub struct Worker {
    sender: Option<mpsc::Sender<Job>>,
    thread: Option<std::thread::JoinHandle<()>>,
    scheduler: Arc<murmur_sched::WriterScheduler>,
    sched_effective: murmur_sched::SchedulerConfig,
    boot_millis: u64,
}
impl Worker {
    pub fn open(path: String, vfs: String) -> Result<Arc<Self>> {
        let boot_millis = murmur_tx::retention::now_millis();
        let sched_effective = load_sched_config(&path, &vfs);
        let scheduler = Arc::new(murmur_sched::WriterScheduler::new(
            sched_effective.clone(),
        ));
        let (tx, rx) = mpsc::channel::<Job>();
        let (ready, wait) = mpsc::sync_channel(1);
        let thread = std::thread::Builder::new()
            .name("murmur-sqlite".into())
            .spawn(move || match Store::open(&path, &vfs) {
                Ok(mut store) => {
                    let _ = ready.send(Ok(()));
                    while let Ok(job) = rx.recv() {
                        job(&mut store);
                    }
                }
                Err(e) => {
                    let _ = ready.send(Err(e.to_string()));
                }
            })?;
        wait.recv()
            .map_err(|_| Error::Unavailable("SQLite worker startup".into()))?
            .map_err(Error::Unavailable)?;
        Ok(Arc::new(Self {
            sender: Some(tx),
            thread: Some(thread),
            scheduler,
            sched_effective,
            boot_millis,
        }))
    }
    /// Test worker over a bare store (default VFS, no persistence barrier).
    #[cfg(test)]
    pub fn open_test(path: String) -> Result<Arc<Self>> {
        let boot_millis = murmur_tx::retention::now_millis();
        let sched_effective = murmur_sched::SchedulerConfig::default();
        let scheduler = Arc::new(murmur_sched::WriterScheduler::new(
            sched_effective.clone(),
        ));
        let (tx, rx) = mpsc::channel::<Job>();
        let thread = std::thread::Builder::new()
            .name("murmur-sqlite-test".into())
            .spawn(move || {
                if let Ok(mut store) = Store::open_bare(&path) {
                    while let Ok(job) = rx.recv() {
                        job(&mut store);
                    }
                }
            })?;
        Ok(Arc::new(Self {
            sender: Some(tx),
            thread: Some(thread),
            scheduler,
            sched_effective,
            boot_millis,
        }))
    }
    pub fn boot_millis(&self) -> u64 {
        self.boot_millis
    }
    pub fn call<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Store) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.sender
            .as_ref()
            .unwrap()
            .send(Box::new(move |s| {
                let r = s.check_schema_cookie().and_then(|_| f(s));
                let _ = tx.send(r);
            }))
            .map_err(|_| Error::Unavailable("SQLite worker stopped".into()))?;
        rx.recv()
            .map_err(|_| Error::Unavailable("SQLite worker stopped".into()))?
    }
    pub async fn run<T: Send + 'static>(
        self: &Arc<Self>,
        f: impl FnOnce(&mut Store) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let worker = self.clone();
        tokio::task::spawn_blocking(move || worker.call(f))
            .await
            .map_err(|e| Error::Unavailable(e.to_string()))?
    }
    /// Admitted worker call: a scheduler ticket is granted on the caller
    /// and MOVED into the job, so admission covers queue wait plus the
    /// whole transaction and barrier (holding it on the caller stack
    /// would release early on cancellation). Reads stay unscheduled.
    /// Never call from inside a worker job: the ticket owner would be
    /// queued behind itself. Scoped to Remote/Maintenance; Local
    /// admission is not enforced (no SQLite-core hook).
    pub fn call_admitted<T: Send + 'static>(
        &self,
        class: murmur_sched::WriterClass,
        timeout: Duration,
        f: impl FnOnce(&mut Store) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let ticket = self
            .scheduler
            .admit_timeout(class, timeout)
            .map_err(crate::errors::sched_err)?;
        self.call(move |s| {
            let _ticket = ticket;
            f(s)
        })
    }
    pub async fn run_admitted<T: Send + 'static>(
        self: &Arc<Self>,
        class: murmur_sched::WriterClass,
        timeout: Duration,
        f: impl FnOnce(&mut Store) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let worker = self.clone();
        tokio::task::spawn_blocking(move || worker.call_admitted(class, timeout, f))
            .await
            .map_err(|e| Error::Unavailable(e.to_string()))?
    }
    pub fn scheduler_snapshot(&self) -> murmur_sched::SchedulerSnapshot {
        self.scheduler.snapshot()
    }
    pub fn scheduler_effective(&self) -> murmur_sched::SchedulerConfig {
        self.sched_effective.clone()
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

pub struct Store {
    pub db: Connection,
    initialized: bool,
    /// Test-only bypass for the VFS persistence barrier (bare connections
    /// have no sqlite-murmur VFS, so the FLUSH file-control is meaningless).
    no_flush: bool,
    // Query plane (WP-E): search indexes, inspected schema, subscriptions.
    search: murmur_query::search::Manager,
    query_schema: murmur_query::Schema,
    sub_mgr: Arc<murmur_query::subscription::SubscriptionManager>,
    /// Last crsql db version consumed by the query tick (0 = full catch-up).
    last_tick_version: u64,
    last_tick_at: std::time::Instant,
    /// Last observed SQLite schema cookie. Bumped by any DDL on any
    /// connection; a change triggers the orphan-clock janitor before
    /// the job runs (a DROP leaves cr-sqlite side tables behind that
    /// abort every crsql_changes read on every connection).
    last_schema_cookie: i64,
}
impl Drop for Store {
    fn drop(&mut self) {
        if self.initialized {
            let _ = self.db.query_row("SELECT crsql_finalize()", [], |_| Ok(()));
        }
    }
}
impl Store {
    fn open(path: &str, vfs: &str) -> Result<Self> {
        let db = Connection::open_with_flags_and_vfs(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
            vfs,
        )?;
        db.busy_timeout(Duration::from_millis(200))?;
        Self::create_tables(&db)?;
        let mut s = Self {
            db,
            initialized: false,
            no_flush: false,
            search: murmur_query::search::Manager::new(
                &murmur_query::search::SearchOptions::default(),
            ),
            query_schema: murmur_query::Schema::default(),
            // Replaced below once settings are readable; node id unknown
            // before first start, so zero is the honest pre-start value.
            sub_mgr: Arc::new(murmur_query::subscription::SubscriptionManager::new([0; 16], 1024)),
            last_tick_version: 0,
            last_tick_at: std::time::Instant::now(),
            last_schema_cookie: 0,
        };
        s.init_query_state()?;
        // Heal orphans predating this worker (created before the
        // janitor existed, or while replication was stopped).
        s.reap_orphan_clocks()?;
        s.last_schema_cookie = Self::schema_cookie(&s.db)?;
        s.flush()?;
        Ok(s)
    }
    /// Query-plane open sequence (WP-E §4): declarations, schema snapshot,
    /// vec module, manager + adopted shadows, key UDFs, subscriptions.
    /// Unsupported DDL never fails the open: actions re-inspect strictly
    /// and surface precise errors, while the tick keeps the last good
    /// schema. Follows the worker recipe (per-connection vec module,
    /// adopt-on-sync).
    fn init_query_state(&mut self) -> Result<()> {
        use murmur_query::adapt;
        adapt::ensure_search_tables(&self.db).map_err(config::query_err)?;
        let num = |name| -> Result<usize> {
            config::setting(&self.db, name)?
                .parse()
                .map_err(|_| Error::Invalid(format!("invalid {name}")))
        };
        let schema = murmur_query::schema::inspect(
            &self.db,
            &murmur_query::Schema::default(),
            &murmur_query::Migration::new(""),
            &|t| adapt::is_crr(&self.db, t).unwrap_or(false),
        )
        .map_err(config::query_err);
        // adopt-on-sync: keep going with an empty snapshot when the
        // database holds DDL outside the supported subset.
        self.query_schema = schema.unwrap_or_default();
        let definitions = adapt::declared_indexes(&self.db).map_err(config::query_err)?;
        let options = adapt::search_options(
            definitions.clone(),
            num("search_queue_bytes")?,
            num("search_batch_rows")?,
            num("search_interval_ms")? as u64,
        )
        .map_err(config::query_err)?;
        murmur_query::search::Manager::register(&self.db, &definitions)
            .map_err(config::query_err)?;
        murmur_query::search::ensure_vec_module(&self.db).map_err(config::query_err)?;
        let mut search = murmur_query::search::Manager::new(&options);
        search
            .sync_schema(&self.db, &self.query_schema, true)
            .map_err(config::query_err)?;
        self.search = search;
        murmur_query::primary_key::register(&self.db).map_err(config::query_err)?;
        let node: Option<Vec<u8>> = self
            .db
            .query_row("SELECT node FROM murmur_identity WHERE id=1", [], |r| {
                r.get(0)
            })?;
        let mut id = [0u8; 16];
        if let Some(n) = node
            && n.len() == 16
        {
            id.copy_from_slice(&n);
        }
        self.sub_mgr = Arc::new(murmur_query::subscription::SubscriptionManager::new(
            id,
            num("sub_max_history")?,
        ));
        Ok(())
    }
    /// Tables shared by the VFS-backed store and the bare test store.
    fn create_tables(db: &Connection) -> Result<()> {
        db.execute_batch("CREATE TABLE IF NOT EXISTS murmur_config(name TEXT PRIMARY KEY,value TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS murmur_identity(id INTEGER PRIMARY KEY CHECK(id=1),node BLOB,site BLOB,incarnation INTEGER NOT NULL DEFAULT 0);
          INSERT OR IGNORE INTO murmur_identity(id) VALUES(1);
          CREATE TABLE IF NOT EXISTS murmur_progress(node BLOB PRIMARY KEY,site BLOB NOT NULL,cursor BLOB NOT NULL,sent BLOB);
          CREATE TABLE IF NOT EXISTS murmur_excluded(node BLOB PRIMARY KEY);
          CREATE TABLE IF NOT EXISTS murmur_stage(node BLOB NOT NULL,idx INTEGER NOT NULL,data BLOB NOT NULL,created INTEGER NOT NULL,PRIMARY KEY(node,idx));
          CREATE TABLE IF NOT EXISTS murmur_kv(key BLOB PRIMARY KEY,value BLOB NOT NULL);
          CREATE TABLE IF NOT EXISTS murmur_file_chunks(digest BLOB NOT NULL,generation INTEGER NOT NULL,idx INTEGER NOT NULL,bytes BLOB NOT NULL,created_ms INTEGER NOT NULL,PRIMARY KEY(digest,generation,idx));
          CREATE TABLE IF NOT EXISTS murmur_file_transfers(transfer BLOB PRIMARY KEY,digest BLOB,generation INTEGER,total_bytes INTEGER,identity BLOB,created_ms INTEGER NOT NULL,updated_ms INTEGER NOT NULL);
          CREATE TABLE IF NOT EXISTS murmur_file_staging(transfer BLOB NOT NULL,idx INTEGER NOT NULL,bytes BLOB NOT NULL,created_ms INTEGER NOT NULL,PRIMARY KEY(transfer,idx));")?;
        // Point 7: merge-policy carrier table (plain DDL here so the
        // bare test store matches; CRR-ified in initialize()).
        crate::policy_ingest::install(db)?;
        // Point 8: ownership event carriers (same install pattern).
        crate::ownership::install(db)?;
        Ok(())
    }
    /// Bare test store on the default VFS: same tables, no persistence
    /// barrier, no cr-sqlite. Only kv/clock/progress paths are exercised.
    #[cfg(test)]
    fn open_bare(path: &str) -> Result<Self> {
        let db = Connection::open(path)?;
        db.busy_timeout(Duration::from_millis(200))?;
        Self::create_tables(&db)?;
        Ok(Self {
            db,
            initialized: false,
            no_flush: true,
            // Bare stores never run the query tick (no cr-sqlite);
            // fields exist so worker jobs keep one shape.
            search: murmur_query::search::Manager::new(
                &murmur_query::search::SearchOptions::default(),
            ),
            query_schema: murmur_query::Schema::default(),
            sub_mgr: Arc::new(murmur_query::subscription::SubscriptionManager::new([0; 16], 1024)),
            last_tick_version: 0,
            last_tick_at: std::time::Instant::now(),
            last_schema_cookie: 0,
        })
    }
    pub fn flush(&self) -> Result<()> {
        if self.no_flush {
            return Ok(());
        }
        let rc = unsafe {
            rusqlite::ffi::sqlite3_file_control(
                self.db.handle(),
                c"main".as_ptr(),
                FLUSH,
                std::ptr::null_mut(),
            )
        };
        if rc != rusqlite::ffi::SQLITE_OK {
            return Err(Error::Unavailable(format!(
                "sqlite-murmur persistence barrier failed ({rc})"
            )));
        }
        Ok(())
    }
    /// Load cr-sqlite on the worker connection without requiring
    /// replication credentials. Query actions and the query tick call
    /// this lazily so search/subscriptions work without murmur_start.
    pub fn ensure_crsql(&mut self) -> Result<()> {
        if self.initialized {
            return Ok(());
        }
            #[cfg(not(feature = "loadable"))]
            unsafe {
                unsafe extern "C" {
                    fn sqlite3_crsqlite_init(
                        db: *mut rusqlite::ffi::sqlite3,
                        error: *mut *mut std::ffi::c_char,
                        api: *const std::ffi::c_void,
                    ) -> i32;
                }
                let mut error = std::ptr::null_mut();
                let rc = sqlite3_crsqlite_init(self.db.handle(), &mut error, std::ptr::null());
                if rc != 0 {
                    let msg = if error.is_null() {
                        format!("cr-sqlite init {rc}")
                    } else {
                        std::ffi::CStr::from_ptr(error)
                            .to_string_lossy()
                            .into_owned()
                    };
                    rusqlite::ffi::sqlite3_free(error.cast());
                    return Err(Error::Unavailable(msg));
                }
            }
            #[cfg(feature = "loadable")]
            unsafe {
                let path = std::ffi::CString::new(config::setting(&self.db, "crsqlite_extension")?)
                    .map_err(|_| Error::Invalid("crsqlite_extension".into()))?;
                if path.as_bytes().is_empty() {
                    return Err(Error::Invalid(
                        "set PRAGMA murmur_crsqlite_extension to the companion crsqlite library"
                            .into(),
                    ));
                }
                let db = self.db.handle();
                let mut error = std::ptr::null_mut();
                // Enable only the C loader, leaving SQL load_extension disabled.
                let rc = rusqlite::ffi::sqlite3_db_config(
                    db,
                    rusqlite::ffi::SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION,
                    1,
                    std::ptr::null_mut::<i32>(),
                );
                if rc != 0 {
                    return Err(Error::Unavailable("extension loading unavailable".into()));
                }
                let rc = rusqlite::ffi::sqlite3_load_extension(
                    db,
                    path.as_ptr(),
                    c"sqlite3_crsqlite_init".as_ptr(),
                    &mut error,
                );
                rusqlite::ffi::sqlite3_db_config(
                    db,
                    rusqlite::ffi::SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION,
                    0,
                    std::ptr::null_mut::<i32>(),
                );
                if rc != 0 {
                    let msg = if error.is_null() {
                        format!("load crsqlite {rc}")
                    } else {
                        std::ffi::CStr::from_ptr(error)
                            .to_string_lossy()
                            .into_owned()
                    };
                    rusqlite::ffi::sqlite3_free(error.cast());
                    return Err(Error::Unavailable(msg));
                }
            }
            self.initialized = true;
        Ok(())
    }
    pub fn initialize(&mut self) -> Result<Config> {
        self.ensure_crsql()?;
        // Point 7: carriers replicate, so the carrier table is a CRR.
        crate::policy_ingest::ensure_carrier_crr(&self.db)?;
        // Point 8: ownership events replicate too.
        crate::ownership::ensure_owner_crr(&self.db)?;
        let site: Vec<u8> = self
            .db
            .query_row("SELECT crsql_site_id()", [], |r| r.get(0))?;
        let site: NodeId = site
            .try_into()
            .map_err(|_| Error::Protocol("cr-sqlite site identity".into()))?;
        let old: Option<Vec<u8>> =
            self.db
                .query_row("SELECT site FROM murmur_identity WHERE id=1", [], |r| {
                    r.get(0)
                })?;
        if old.as_deref().is_some_and(|v| v != site) {
            return Err(Error::Auth("cr-sqlite site identity changed".into()));
        }
        self.db.execute(
            "UPDATE murmur_identity SET site=? WHERE id=1",
            [site.as_slice()],
        )?;
        let cfg = Config::load(&self.db, site)?;
        self.db.prepare("SELECT crsql_ingest_blob(?1)")?;
        self.schema()?;
        self.flush()?;
        Ok(cfg)
    }
    pub fn schema(&self) -> Result<[u8; 32]> {
        Ok(protocol::digest(&self.schema_bytes()?))
    }
    /// Canonical replicated-subset bytes: ordered (table, DDL) pairs.
    /// Clock tables identify CRRs; only their application table DDL counts.
    pub fn schema_bytes(&self) -> Result<Vec<u8>> {
        let mut q=self.db.prepare("SELECT name,sql FROM sqlite_schema WHERE type='table' AND name IN (SELECT substr(name,1,length(name)-13) FROM sqlite_schema WHERE type='table' AND substr(name,-13)='__crsql_clock') ORDER BY name")?;
        let rows = q.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
        let mut schema = Vec::new();
        for row in rows {
            let (name, ddl) = row?;
            // Point 7 strict exception: the policy carrier table is the
            // one murmur_ table allowed as a CRR (components replicate).
            // Exact name + exact DDL only, never a prefix exemption.
            if name == murmur_policy::carrier::TABLE {
                let strip =
                    |s: &str| s.trim_end().trim_end_matches(';').trim_end().to_string();
                if strip(&ddl) != strip(murmur_policy::carrier::CREATE_SQL) {
                    return Err(Error::Invalid("carrier table diverges".into()));
                }
                continue;
            }
            // Point 8 strict exception: ownership events replicate.
            if name == murmur_policy::ownership::EVENT_TABLE {
                let strip =
                    |s: &str| s.trim_end().trim_end_matches(';').trim_end().to_string();
                if strip(&ddl) != strip(murmur_policy::ownership::CREATE_EVENT_SQL) {
                    return Err(Error::Invalid("owner event table diverges".into()));
                }
                continue;
            }
            if name.starts_with("murmur_") {
                return Err(Error::Invalid("murmur tables must not be CRRs".into()));
            }
            schema.push((name, ddl));
        }
        if schema.is_empty() {
            return Err(Error::Invalid("no CRR tables configured".into()));
        }
        Ok(encode(&schema)?)
    }
    /// Manifest bytes: canonical pairs minus local-only tables. The
    /// archive excludes local-only tables before signing; opt-outs ride
    /// the A4 flag map here. An all-opted-out node signs an empty set.
    pub fn manifest_schema_bytes(&self) -> Result<Vec<u8>> {
        let pairs: Vec<(String, String)> = decode(&self.schema_bytes()?)?;
        let kept = pairs
            .into_iter()
            .filter(|(table, _)| self.table_enabled(table).unwrap_or(true))
            .collect::<Vec<_>>();
        Ok(encode(&kept)?)
    }
    /// Apply an additive remote-schema delta: create + CRR-enable new
    /// tables, add validated columns. Every statement is validated
    /// before execution (remote DDL is untrusted text) and verified
    /// afterwards; the query schema snapshot and subscriptions reset
    /// like a local migration. Idempotent: a concurrent adopt of the
    /// same manifest (push racing pull-mismatch) skips what is already
    /// present, but only when the existing object matches the expected
    /// DDL — real divergence still fails loudly.
    pub fn apply_schema_additive(&mut self, additive: &schema::Additive) -> Result<()> {
        self.ensure_crsql()?;
        for (table, ddl) in &additive.new_tables {
            schema::check_create_table(table, ddl)?;
            let prior: Option<String> = self
                .db
                .query_row(
                    "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?",
                    [table],
                    |r| r.get(0),
                )
                .optional()?;
            if let Some(existing) = prior {
                let strip = |s: &str| s.trim_end().trim_end_matches(';').trim_end().to_string();
                if strip(&existing) != strip(ddl) {
                    return Err(Error::Protocol("adopted table diverges".into()));
                }
            } else {
                self.db.execute_batch(ddl)?;
            }
            let clock = format!("{table}__crsql_clock");
            let crr: i64 = self.db.query_row(
                "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name=?",
                [&clock],
                |r| r.get(0),
            )?;
            if crr != 1 {
                self.db
                    .execute_batch(&format!("SELECT crsql_as_crr('main', \"{table}\")"))?;
                let crr: i64 = self.db.query_row(
                    "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name=?",
                    [&clock],
                    |r| r.get(0),
                )?;
                if crr != 1 {
                    return Err(Error::Protocol("adopted table is not a CRR".into()));
                }
            }
        }
        for (table, coldef) in &additive.add_columns {
            if !schema::valid_ident(table) {
                return Err(Error::Invalid("rejected schema table".into()));
            }
            let column = schema::check_add_column(coldef)?;
            let present: Option<String> = self
                .db
                .query_row(
                    &format!("SELECT type FROM pragma_table_info(\"{table}\") WHERE name=?"),
                    [&column],
                    |r| r.get(0),
                )
                .optional()?;
            if let Some(affinity) = present {
                let declared = coldef
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or_default()
                    .trim_matches('"');
                if !affinity.eq_ignore_ascii_case(declared) {
                    return Err(Error::Protocol("adopted column diverges".into()));
                }
            } else {
                self.db.execute_batch(&format!(
                    "ALTER TABLE \"{table}\" ADD COLUMN {coldef}"
                ))?;
                let found: i64 = self.db.query_row(
                    &format!("SELECT count(*) FROM pragma_table_info(\"{table}\") WHERE name=?"),
                    [&column],
                    |r| r.get(0),
                )?;
                if found != 1 {
                    return Err(Error::Protocol("adopted column missing".into()));
                }
            }
        }
        self.refresh_query_schema()?;
        self.sub_mgr.notify_reset("schema adopted");
        self.flush()
    }
    /// Name-keyed table replication flags (lowercased). Missing or true
    /// follows CRR state; explicit false keeps the table local in both
    /// wire directions. Unknown tables are inert.
    fn replication_flags(db: &Connection) -> Result<BTreeMap<String, bool>> {
        let raw = config::setting(db, "table_replication")?;
        let map: BTreeMap<String, bool> = serde_json::from_str(&raw)
            .map_err(|e| Error::Invalid(format!("table_replication map: {e}")))?;
        Ok(map
            .into_iter()
            .map(|(k, v)| (k.to_ascii_lowercase(), v))
            .collect())
    }

    /// True unless the table is explicitly opted out. Case-insensitive.
    pub fn table_enabled(&self, table: &str) -> Result<bool> {
        Ok(Self::replication_flags(&self.db)?
            .get(&table.to_ascii_lowercase())
            != Some(&false))
    }
    /// Highest pruned local db_version. Pulls from older cursors fail
    /// loud (`cursor pruned; snapshot required`) instead of serving
    /// silent gaps. Snapshots bypass it (frontier state by design).
    pub fn gc_watermark(&self) -> Result<u64> {
        // Internal state, not a pragma: read the table directly
        // (config::setting rejects names without defaults).
        let raw: Option<String> = self
            .db
            .query_row(
                "SELECT value FROM murmur_config WHERE name='gc_watermark'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        raw.unwrap_or_else(|| "0".to_string())
            .parse()
            .map_err(|_| Error::Invalid("invalid gc_watermark".into()))
    }
    fn schema_cookie(db: &Connection) -> Result<i64> {
        Ok(db.query_row("PRAGMA schema_version", [], |r| r.get(0))?)
    }
    /// Run the orphan janitor when DDL landed since the last job (any
    /// connection). The stored cookie is the pre-janitor read, so our
    /// own DROPs re-scan once (no-op) while a concurrent DROP can never
    /// be missed.
    pub fn check_schema_cookie(&mut self) -> Result<()> {
        let cookie = Self::schema_cookie(&self.db)?;
        if cookie != self.last_schema_cookie {
            self.reap_orphan_clocks()?;
            self.last_schema_cookie = cookie;
        }
        Ok(())
    }
    /// Drop cr-sqlite side tables (`__crsql_clock`, `__crsql_pks` plus
    /// their indexes) whose application table is gone. `DROP TABLE`
    /// removes the app table and its triggers but NOT these side
    /// tables, and cr-sqlite's changes vtab enumerates every clock
    /// table: a clock with no app table builds a zero-column TableInfo
    /// whose empty PK list aborts the vtab read (`query aborted`),
    /// breaking export — and all replication reads — for the whole
    /// database until the orphans go. Drop means drop: re-creating the
    /// table starts fresh rather than resurrecting clock history.
    /// Before forgetting versions, the max db_version across dropped
    /// clocks is pinned into `crsql_master.pre_compact_dbversion` (the
    /// same key compaction uses): db_version is max-derived, so
    /// dropping the max-holding clock without a pin would rewind the
    /// counter and strand pull cursors past the reused versions.
    /// Returns tables dropped.
    pub fn reap_orphan_clocks(&mut self) -> Result<u64> {
        let mut sides: Vec<String> = self
            .db
            .prepare("SELECT name FROM sqlite_schema WHERE type='table' AND (name LIKE '%__crsql_clock' ESCAPE '\\' OR name LIKE '%__crsql_pks' ESCAPE '\\')")?
            .query_map([], |r| r.get(0))?
            .collect::<std::result::Result<_, _>>()?;
        sides.retain(|name: &String| {
            let app = name
                .strip_suffix("__crsql_clock")
                .or_else(|| name.strip_suffix("__crsql_pks"));
            match app {
                Some(prefix) if !prefix.is_empty() => prefix
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$'),
                _ => false,
            }
        });
        let mut orphans = Vec::new();
        for side in &sides {
            let app = side
                .strip_suffix("__crsql_clock")
                .or_else(|| side.strip_suffix("__crsql_pks"))
                .unwrap_or("");
            let exists: bool = self.db.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)",
                [app],
                |r| r.get(0),
            )?;
            if !exists {
                orphans.push(side.clone());
            }
        }
        self.pin_version_floor(&orphans)?;
        let mut dropped = 0u64;
        for side in &orphans {
            self.db.execute(&format!("DROP TABLE \"{side}\""), [])?;
            dropped += 1;
        }
        Ok(dropped)
    }
    /// Pin `max(db_version)` over about-to-drop orphan clocks into the
    /// compaction floor key (monotonic: never retreats an existing
    /// pin). Skipped when `crsql_master` is absent (bare test stores)
    /// or no orphan clock carries versions.
    fn pin_version_floor(&self, orphans: &[String]) -> Result<()> {
        let master: bool = self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='crsql_master')",
            [],
            |r| r.get(0),
        )?;
        if !master {
            return Ok(());
        }
        let mut peak: Option<i64> = None;
        for side in orphans {
            if !side.ends_with("__crsql_clock") {
                continue;
            }
            let has_col: bool = self.db.query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_table_info(?1) WHERE name='db_version')",
                [side],
                |r| r.get(0),
            )?;
            if !has_col {
                continue;
            }
            let max: Option<i64> = self
                .db
                .query_row(&format!("SELECT max(db_version) FROM \"{side}\""), [], |r| {
                    r.get(0)
                })?;
            peak = peak.max(max);
        }
        let Some(peak) = peak else {
            return Ok(());
        };
        let floor: Option<i64> = self.db.query_row(
            "SELECT CAST(value AS INTEGER) FROM crsql_master WHERE key='pre_compact_dbversion'",
            [],
            |r| r.get(0),
        ).optional()?;
        if peak > floor.unwrap_or(0) {
            self.db.execute(
                "INSERT OR REPLACE INTO crsql_master (key, value) VALUES ('pre_compact_dbversion', ?1)",
                [peak],
            )?;
        }
        Ok(())
    }
    /// Delete tombstone (`'-1'`) clock rows at or below `floor` across
    /// every CRR clock table, then advance the watermark. Live cells
    /// keep their single clock row each; returns rows deleted. Table
    /// names come from `sqlite_schema` (suffix-matched) and are
    /// identifier-checked before interpolation.
    pub fn gc_tombstones(&mut self, floor: u64) -> Result<u64> {
        let mut clocks: Vec<String> = self
            .db
            .prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name LIKE '%__crsql_clock' ESCAPE '\\'")?
            .query_map([], |r| r.get(0))?
            .collect::<std::result::Result<_, _>>()?;
        clocks.retain(|name: &String| {
            name.len() > 13
                && name.ends_with("__crsql_clock")
                && name[..name.len() - 13]
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
        });
        let mut deleted = 0u64;
        for clock in &clocks {
            deleted += self.db.execute(
                &format!(
                    "DELETE FROM \"{clock}\" WHERE col_name = '-1' AND db_version <= ?1"
                ),
                [floor as i64],
            )? as u64;
        }
        let watermark = self.gc_watermark()?.max(floor);
        self.db.execute(
            "INSERT INTO murmur_config VALUES('gc_watermark',?1) ON CONFLICT(name) DO UPDATE SET value=excluded.value",
            [watermark.to_string()],
        )?;
        self.flush()?;
        Ok(deleted)
    }

    pub fn progress(&self, node: NodeId) -> Result<(Option<NodeId>, Cursor)> {
        let row: Option<(Vec<u8>, Vec<u8>)> = self
            .db
            .query_row(
                "SELECT site,cursor FROM murmur_progress WHERE node=?",
                [node.as_slice()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        match row {
            None => Ok((None, Cursor::default())),
            Some((site, cursor)) => Ok((
                Some(
                    site.try_into()
                        .map_err(|_| Error::Protocol("stored site".into()))?,
                ),
                decode(&cursor)?,
            )),
        }
    }
    pub fn reset(&self, node: NodeId, site: NodeId) -> Result<()> {
        self.db.execute("INSERT INTO murmur_progress(node,site,cursor) VALUES(?1,?2,?3) ON CONFLICT(node) DO UPDATE SET site=excluded.site,cursor=excluded.cursor,sent=NULL",params![node.as_slice(),site.as_slice(),encode(&Cursor::default())?])?;
        self.flush()
    }
    pub fn export(&mut self, cursor: Cursor, cfg: &Config) -> Result<(Cursor, Vec<u8>)> {
        if cursor.from < 0
            || cursor.through.is_some_and(|h| h < cursor.from)
            || cursor
                .after
                .as_ref()
                .is_some_and(|k| k.version <= cursor.from || Some(k.version) > cursor.through)
        {
            return Err(Error::Protocol("invalid export cursor".into()));
        }
        let tx = self.db.transaction()?;
        let current: i64 = tx.query_row("SELECT crsql_db_version()", [], |r| r.get(0))?;
        let through = cursor.through.unwrap_or(current);
        if through > current || cursor.from > current {
            return Err(Error::Protocol("source version regressed".into()));
        }
        let mut blob = b"CRSB\x01\0\0\0\0\0\0\0".to_vec();
        let mut count = 0u32;
        let mut last = cursor.after.clone();
        let mut done = true;
        // Outgoing opt-out: skip disabled tables before any budget
        // accounting (a huge row in a local-only table must not fail
        // the export). No false flags means no behavior change.
        // Skipped rows still advance the cursor: rows written while a
        // table is disabled stay local permanently (no backfill on
        // re-enable; snapshots cover peers that need them).
        let flags = Self::replication_flags(&tx)?;
        let filtering = flags.values().any(|v| !v);
        {
            let mut q=tx.prepare("SELECT \"table\",pk,cid,val,col_version,db_version,site_id,cl,seq FROM crsql_changes WHERE db_version>?1 AND db_version<=?2 AND (?3 IS NULL OR (db_version,seq,\"table\",pk,cid)>(?3,?4,?5,?6,?7)) ORDER BY db_version,seq,\"table\",pk,cid")?;
            let a = cursor.after.as_ref();
            let mut rows = q.query(params![
                cursor.from,
                through,
                a.map(|k| k.version),
                a.map(|k| k.seq),
                a.map(|k| k.table.as_str()),
                a.map(|k| k.pk.as_slice()),
                a.map(|k| k.cid.as_str())
            ])?;
            while let Some(row) = rows.next()? {
                let key = Key {
                    table: row.get(0)?,
                    pk: row.get(1)?,
                    cid: row.get(2)?,
                    version: row.get(5)?,
                    seq: row.get(8)?,
                };
                if cursor
                    .after
                    .as_ref()
                    .is_some_and(|a| compare_key(&key, a) != std::cmp::Ordering::Greater)
                {
                    continue;
                }
                if filtering
                    && flags.get(&key.table.to_ascii_lowercase()) == Some(&false)
                {
                    continue;
                }
                // Avoid copying an over-budget value before enforcing the limit.
                let bytes = (0..9)
                    .map(|i| match row.get_ref(i) {
                        Ok(ValueRef::Text(b) | ValueRef::Blob(b)) => b.len(),
                        _ => 8,
                    })
                    .sum::<usize>()
                    + 64;
                if bytes > cfg.max_batch_bytes - 12 {
                    return Err(Error::Overloaded);
                }
                if count > 0 && blob.len() + bytes > cfg.batch_bytes {
                    done = false;
                    break;
                }
                let mut fields = Vec::with_capacity(bytes);
                for i in [0, 1, 2] {
                    let v = row.get_ref(i)?;
                    match v {
                        ValueRef::Text(b) | ValueRef::Blob(b) => field(&mut fields, b),
                        _ => return Err(Error::Protocol("CRR key storage class".into())),
                    }
                }
                match row.get_ref(3)? {
                    ValueRef::Null => fields.push(0),
                    ValueRef::Integer(v) => {
                        fields.push(1);
                        fields.extend(v.to_le_bytes());
                    }
                    ValueRef::Real(v) => {
                        fields.push(2);
                        fields.extend(v.to_le_bytes());
                    }
                    ValueRef::Text(v) => {
                        fields.push(3);
                        field(&mut fields, v)
                    }
                    ValueRef::Blob(v) => {
                        fields.push(4);
                        field(&mut fields, v)
                    }
                }
                for i in [4, 5] {
                    fields.extend(row.get::<_, i64>(i)?.to_le_bytes());
                }
                field(
                    &mut fields,
                    row.get_ref(6)?
                        .as_blob()
                        .map_err(|e| Error::Protocol(e.to_string()))?,
                );
                for i in [7, 8] {
                    fields.extend(row.get::<_, i64>(i)?.to_le_bytes());
                }
                if blob.len() + fields.len() > cfg.max_batch_bytes {
                    return Err(Error::Overloaded);
                }
                blob.extend(fields);
                count += 1;
                last = Some(key);
                if count >= 1024 {
                    done = false;
                    break;
                }
            }
        }
        blob[8..12].copy_from_slice(&count.to_le_bytes());
        tx.commit()?;
        self.flush()?;
        let next = if done {
            Cursor {
                from: through,
                through: None,
                after: None,
            }
        } else {
            Cursor {
                from: cursor.from,
                through: Some(through),
                after: last,
            }
        };
        Ok((next, blob))
    }
    pub fn stage_begin(&self, node: NodeId, cfg: &Config) -> Result<()> {
        self.db.execute(
            "DELETE FROM murmur_stage WHERE node=?1 OR created < unixepoch()*1000-?2",
            params![node.as_slice(), cfg.staging_timeout_ms as i64],
        )?;
        Ok(())
    }
    /// Delete abandoned staging rows past `staging_timeout_ms`
    /// (transfers whose peer never sent another chunk get no fresh
    /// `stage_begin` to reap them). Returns rows removed.
    pub fn reap_staging(&mut self) -> Result<u64> {
        let timeout: u64 = config::setting(&self.db, "staging_timeout_ms")?
            .parse()
            .map_err(|_| Error::Invalid("invalid staging_timeout_ms".into()))?;
        let removed = self.db.execute(
            "DELETE FROM murmur_stage WHERE created < unixepoch()*1000 - ?1",
            [timeout as i64],
        )?;
        Ok(removed as u64)
    }

    pub fn stage(&self, node: NodeId, index: u32, data: Vec<u8>, budget: usize) -> Result<()> {
        let total: i64 = self.db.query_row(
            "SELECT coalesce(sum(length(data)),0) FROM murmur_stage",
            [],
            |r| r.get(0),
        )?;
        if total as usize + data.len() > budget {
            return Err(Error::Overloaded);
        }
        self.db.execute(
            "INSERT INTO murmur_stage VALUES(?1,?2,?3,unixepoch()*1000)",
            params![node.as_slice(), index, data],
        )?;
        Ok(())
    }
    pub fn apply(
        &mut self,
        node: NodeId,
        site: NodeId,
        previous: Cursor,
        next: Cursor,
        bytes: usize,
        digest: [u8; 32],
    ) -> Result<()> {
        let (stored_site, stored) = self.progress(node)?;
        if stored_site != Some(site) || stored != previous {
            return Err(Error::Protocol(
                "receive progress changed during transfer".into(),
            ));
        }
        let mut blob = Vec::with_capacity(bytes);
        let mut index = 0;
        {
            let mut q = self
                .db
                .prepare("SELECT idx,data FROM murmur_stage WHERE node=? ORDER BY idx")?;
            let mut rows = q.query([node.as_slice()])?;
            while let Some(r) = rows.next()? {
                if r.get::<_, u32>(0)? != index {
                    return Err(Error::Protocol("missing chunk".into()));
                }
                let b: Vec<u8> = r.get(1)?;
                if blob.len() + b.len() > bytes {
                    return Err(Error::Protocol("excess payload".into()));
                }
                blob.extend(b);
                index += 1;
            }
        }
        if blob.len() != bytes || protocol::digest(&blob) != digest {
            return Err(Error::Protocol("batch digest or length".into()));
        }
        // Incoming opt-out: drop disabled-table cells before ingest.
        // Integrity was checked on the wire bytes above; the cursor
        // still advances past the whole batch (sender stream position),
        // so dropped cells are never revisited (no backfill).
        let flags = Self::replication_flags(&self.db)?;
        let blob = if flags.values().any(|v| !v) {
            let mut cells = protocol::decode_cells(&blob)?;
            cells.retain(|c| flags.get(&c.table.to_ascii_lowercase()) != Some(&false));
            if cells.is_empty() {
                None
            } else {
                Some(protocol::encode_cells(&cells))
            }
        } else {
            Some(blob)
        };
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        if let Some(blob) = blob {
            tx.query_row("SELECT crsql_ingest_blob(?)", [blob], |_| Ok(()))?;
        }
        // Point 7: carriers just ingested ride the blob above; reduce +
        // project every configured target in the same atomic commit.
        crate::policy_ingest::recompute_configured(&tx)?;
        tx.execute(
            "UPDATE murmur_progress SET cursor=?1 WHERE node=?2",
            params![encode(&next)?, node.as_slice()],
        )?;
        tx.execute("DELETE FROM murmur_stage WHERE node=?", [node.as_slice()])?;
        // Publish per-origin heads for waits/retention in the same atomic
        // commit: applied is the contiguous version, observed is the
        // sender's latest (through when the batch is partial). Heads are
        // sender stream positions by design, not guesses: forwarding does
        // not preserve origin versions in CRSB rows (db_version is the
        // sender's local version), so a batch from P advances P's head and
        // no one else's. Other origins advance on direct contact; waits
        // and floors stay conservative rather than exact across relays.
        let applied = next.from.max(0) as u64;
        let observed = next.through.unwrap_or(next.from).max(next.from).max(0) as u64;
        let enc = |v: u64| murmur_tx::encode(&v).map_err(|e| Error::Protocol(e.to_string()));
        Self::kv_apply_tx(
            &tx,
            &[
                murmur_tx::Record::put(murmur_tx::protocol::progress_key(node), enc(applied)?),
                murmur_tx::Record::put(murmur_tx::protocol::observed_key(node), enc(observed)?),
            ],
        )?;
        tx.commit()?;
        self.flush()
    }
    /// Max-merge an observed version for an origin (e.g. what a puller
    /// acked of our data). No flush: hints become durable at the next
    /// barrier; retention re-flushes before pruning decisions (A2).
    pub fn observed_max(&mut self, origin: NodeId, seq: u64) -> Result<()> {
        let key = murmur_tx::protocol::observed_key(origin);
        let current: u64 = match self.kv_get(&key)? {
            Some(b) => murmur_tx::decode(&b).map_err(|e| Error::Protocol(e.to_string()))?,
            None => 0,
        };
        if seq > current {
            let value =
                murmur_tx::encode(&seq).map_err(|e| Error::Protocol(e.to_string()))?;
            self.kv_commit(vec![murmur_tx::Record::put(key, value)])?;
        }
        Ok(())
    }
    // -- Generic kv record store backing the murmur-tx Backend adapter. --
    pub fn kv_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self
            .db
            .query_row("SELECT value FROM murmur_kv WHERE key=?", [key], |r| {
                r.get(0)
            })
            .optional()?)
    }

    /// Live (group, node) identity for file metadata stamping. Zeros
    /// when uninitialized or malformed — the honest pre-start value.
    pub fn file_identity(&self) -> ([u8; 16], [u8; 16]) {
        let group = crate::config::setting(&self.db, "group_id")
            .ok()
            .and_then(|s| crate::parse_id(&s).ok())
            .unwrap_or([0u8; 16]);
        let node: Option<Vec<u8>> = self
            .db
            .query_row("SELECT node FROM murmur_identity WHERE id=1", [], |r| r.get(0))
            .ok()
            .flatten();
        let node = node
            .and_then(|b| <[u8; 16]>::try_from(b).ok())
            .unwrap_or([0u8; 16]);
        (group, node)
    }
    pub fn kv_get_many(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>> {
        let mut q = self.db.prepare("SELECT value FROM murmur_kv WHERE key=?")?;
        keys.iter()
            .map(|k| Ok(q.query_row([k], |r| r.get(0)).optional()?))
            .collect()
    }
    /// Point 7 local policy op: component write + reduce + projection
    /// in one immediate transaction on the worker connection. UDF
    /// callers must be in autocommit (no caller-held locks).
    pub fn policy_op(
        &mut self,
        table: &str,
        column: &str,
        pk: &rusqlite::types::Value,
        op: &str,
        value: &rusqlite::types::Value,
    ) -> Result<Option<rusqlite::types::Value>> {
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let out = crate::policy_ingest::apply_op(&tx, table, column, pk, op, value)?;
        tx.commit()?;
        self.flush()
            .map(|_| out)
    }
    /// Point 8 High/Low grant: store event + reduce in one tx.
    pub fn hl_grant(
        &mut self,
        table: &str,
        pk: &rusqlite::types::Value,
        column: &str,
    ) -> Result<String> {
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let out = crate::ownership::grant(&tx, table, pk, column)?;
        tx.commit()?;
        self.flush().map(|_| out.to_string())
    }
    /// Point 8 High/Low release.
    pub fn hl_release(
        &mut self,
        table: &str,
        pk: &rusqlite::types::Value,
        column: &str,
    ) -> Result<String> {
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let out = crate::ownership::release(&tx, table, pk, column)?;
        tx.commit()?;
        self.flush().map(|_| out.to_string())
    }
    /// Point 8 authority projection (lazy reduce on read).
    pub fn hl_projection(
        &mut self,
        table: &str,
        pk: &rusqlite::types::Value,
        column: &str,
    ) -> Result<String> {
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let out = crate::ownership::projection(&tx, table, pk, column)?;
        tx.commit()?;
        self.flush().map(|_| out)
    }
    /// Point 8 eager reduce of every event-bearing target.
    pub fn hl_reduce(&mut self) -> Result<u64> {
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let out = crate::ownership::reduce_all(&tx)?;
        tx.commit()?;
        self.flush().map(|_| out)
    }
    /// Point 7 explicit recompute of one configured target.
    pub fn policy_recompute(
        &mut self,
        table: &str,
        column: &str,
        pk: &rusqlite::types::Value,
    ) -> Result<Option<rusqlite::types::Value>> {
        let map = crate::policy_ingest::policies(&self.db)?;
        let policy = map
            .get(&(table.to_string(), column.to_string()))
            .copied()
            .ok_or_else(|| Error::Invalid("column has no merge policy".into()))?;
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let out = crate::policy_ingest::recompute(&tx, table, column, pk, policy)?;
        tx.commit()?;
        self.flush()
            .map(|_| out)
    }
    pub fn kv_keys(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        match prefix_end(prefix) {
            Some(end) => {
                let mut q = self.db.prepare(
                    "SELECT key FROM murmur_kv WHERE key>=?1 AND key<?2 ORDER BY key",
                )?;
                let mut rows = q.query(params![prefix, end])?;
                while let Some(r) = rows.next()? {
                    out.push(r.get(0)?);
                }
            }
            None => {
                let mut q = self
                    .db
                    .prepare("SELECT key FROM murmur_kv WHERE key>=?1 ORDER BY key")?;
                let mut rows = q.query([prefix])?;
                while let Some(r) = rows.next()? {
                    out.push(r.get(0)?);
                }
            }
        }
        Ok(out)
    }
    pub fn kv_keys_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        let end = prefix_end(prefix);
        let mut q = match (&end, after) {
            (Some(_), Some(_)) => self.db.prepare(
                "SELECT key FROM murmur_kv WHERE key>=?1 AND key<?2 AND key>?3 ORDER BY key LIMIT ?4",
            )?,
            (Some(_), None) => self.db.prepare(
                "SELECT key FROM murmur_kv WHERE key>=?1 AND key<?2 ORDER BY key LIMIT ?3",
            )?,
            (None, Some(_)) => self.db.prepare(
                "SELECT key FROM murmur_kv WHERE key>=?1 AND key>?2 ORDER BY key LIMIT ?3",
            )?,
            (None, None) => self.db.prepare(
                "SELECT key FROM murmur_kv WHERE key>=?1 ORDER BY key LIMIT ?2",
            )?,
        };
        // Bind positionally per shape; the query text above fixes the arity.
        let mut rows = match (&end, after) {
            (Some(e), Some(a)) => q.query(params![prefix, e, a, limit as i64])?,
            (Some(e), None) => q.query(params![prefix, e, limit as i64])?,
            (None, Some(a)) => q.query(params![prefix, a, limit as i64])?,
            (None, None) => q.query(params![prefix, limit as i64])?,
        };
        while let Some(r) = rows.next()? {
            out.push(r.get(0)?);
        }
        Ok(out)
    }
    /// Atomically apply puts/deletes without flushing; callers add the
    /// persistence barrier (commit/publish flush, apply flushes once).
    pub fn kv_commit(&mut self, records: Vec<murmur_tx::Record>) -> Result<()> {
        let tx = self.db.transaction()?;
        Self::kv_apply_tx(&tx, &records)?;
        Ok(tx.commit()?)
    }
    /// Puts/deletes inside a caller-owned transaction (lets `apply` commit
    /// ingested payloads and control records atomically).
    pub fn kv_apply_tx(tx: &Transaction, records: &[murmur_tx::Record]) -> Result<()> {
        let mut put = tx.prepare(
            "INSERT INTO murmur_kv(key,value) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        )?;
        let mut del = tx.prepare("DELETE FROM murmur_kv WHERE key=?1")?;
        for r in records {
            match &r.value {
                Some(v) => {
                    put.execute(params![r.key, v])?;
                }
                None => {
                    del.execute([&r.key])?;
                }
            }
        }
        Ok(())
    }
    /// Max-merge an observed hybrid clock; returns the stored clock.
    pub fn clock_observe(&mut self, millis: u64, logical: u32) -> Result<(u64, u32)> {
        let current: Option<Vec<u8>> = self.kv_get(CLOCK_KEY)?;
        let mut clock: (u64, u32) = match current {
            Some(b) => decode(&b)?,
            None => (0, 0),
        };
        clock = clock.max((millis, logical));
        self.kv_commit(vec![murmur_tx::Record::put(CLOCK_KEY, encode(&clock)?)])?;
        Ok(clock)
    }
    /// Single-round-trip durable per-origin progress. Mirrors the archived
    /// `Backend::progress` default over `07/replication/*` keys.
    pub fn tx_progress(&self) -> Result<Vec<murmur_tx::protocol::Progress>> {
        use murmur_tx::protocol::Progress;
        let mut origins = BTreeMap::<murmur_tx::NodeId, Progress>::new();
        for key in self.kv_keys(b"07/replication/retained/")? {
            if let Some(b) = self.kv_get(&key)? {
                let p: Progress = murmur_tx::decode(&b)
                    .map_err(|e| Error::Protocol(e.to_string()))?;
                origins.insert(p.origin, p);
            }
        }
        for key in self.kv_keys(b"07/replication/applied/")? {
            let Some(b) = self.kv_get(&key)? else {
                continue;
            };
            let suffix = std::str::from_utf8(&key)
                .map_err(|_| Error::Protocol("progress key".into()))?
                .rsplit('/')
                .next()
                .unwrap();
            let origin = murmur_tx::parse_id(suffix)
                .map_err(|e| Error::Protocol(e.to_string()))?;
            origins
                .entry(origin)
                .or_insert(Progress {
                    origin,
                    applied: 0,
                    observed: 0,
                    retained_from: 0,
                    retained_through: 0,
                })
                .applied = murmur_tx::decode(&b).map_err(|e| Error::Protocol(e.to_string()))?;
        }
        for key in self.kv_keys(b"07/replication/observed/")? {
            if let Some(b) = self.kv_get(&key)? {
                let origin = murmur_tx::parse_id(
                    std::str::from_utf8(&key)
                        .map_err(|_| Error::Protocol("observed key".into()))?
                        .rsplit('/')
                        .next()
                        .unwrap(),
                )
                .map_err(|e| Error::Protocol(e.to_string()))?;
                let p = origins.entry(origin).or_insert(Progress {
                    origin,
                    applied: 0,
                    observed: 0,
                    retained_from: 0,
                    retained_through: 0,
                });
                p.observed =
                    murmur_tx::decode(&b).map_err(|e| Error::Protocol(e.to_string()))?;
            }
        }
        for p in origins.values_mut() {
            p.observed = p.observed.max(p.applied);
        }
        Ok(origins.into_values().collect())
    }
    pub fn sub_manager(&self) -> Arc<murmur_query::subscription::SubscriptionManager> {
        self.sub_mgr.clone()
    }
    /// Strict schema refresh for query actions: precise errors surface
    /// (unlike open/tick, which keep the last good snapshot).
    fn refresh_query_schema(&mut self) -> Result<()> {
        let next = murmur_query::schema::inspect(
            &self.db,
            &self.query_schema,
            &murmur_query::Migration::new(""),
            &|t| murmur_query::adapt::is_crr(&self.db, t).unwrap_or(false),
        )
        .map_err(config::query_err)?;
        self.query_schema = next;
        Ok(())
    }
    fn query_action<T>(
        &mut self,
        run: impl FnOnce(&Connection, &mut murmur_query::search::Manager, &murmur_query::Schema) -> std::result::Result<T, murmur_query::Error>,
        reset_subs: bool,
    ) -> Result<T> {
        self.ensure_crsql()?;
        self.refresh_query_schema()?;
        let out = run(&self.db, &mut self.search, &self.query_schema).map_err(config::query_err)?;
        if reset_subs {
            self.sub_mgr.notify_reset("search indexes reconfigured");
        }
        self.flush()?;
        Ok(out)
    }
    pub fn query_create_fts(
        &mut self,
        name: &str,
        table: &str,
        columns_json: &str,
        tokenizer: &str,
    ) -> Result<()> {
        self.query_action(
            |c, m, s| {
                murmur_query::adapt::actions::create_fts(c, m, s, name, table, columns_json, tokenizer)
            },
            true,
        )
    }
    pub fn query_create_vec(
        &mut self,
        name: &str,
        table: &str,
        column: &str,
        dimensions: usize,
        metric: &str,
    ) -> Result<()> {
        self.query_action(
            |c, m, s| {
                murmur_query::adapt::actions::create_vec(
                    c, m, s, name, table, column, dimensions, metric,
                )
            },
            true,
        )
    }
    pub fn query_drop_index(&mut self, name: &str) -> Result<i64> {
        self.query_action(
            |c, m, s| murmur_query::adapt::actions::drop_index(c, m, s, name),
            true,
        )
    }
    pub fn query_refresh(&mut self) -> Result<String> {
        self.query_action(
            |c, m, s| murmur_query::adapt::actions::refresh(c, m, s),
            false,
        )
    }
    pub fn query_status(&self) -> String {
        murmur_query::adapt::actions::status(&self.search)
    }
    pub fn query_set_column_fts(
        &mut self,
        table: &str,
        column: &str,
        enabled: bool,
    ) -> Result<()> {
        self.query_action(
            |c, m, s| {
                murmur_query::adapt::actions::set_column_fts(c, m, s, table, column, enabled)
            },
            true,
        )
    }
    pub fn query_set_column_vector(
        &mut self,
        table: &str,
        column: &str,
        dimensions: Option<usize>,
        metric: Option<String>,
    ) -> Result<()> {
        self.query_action(
            |c, m, s| {
                murmur_query::adapt::actions::set_column_vector(
                    c,
                    m,
                    s,
                    table,
                    column,
                    dimensions,
                    metric.as_deref(),
                )
            },
            true,
        )
    }
    pub fn query_migrate(&mut self, sql: &str, mappings_json: &str) -> Result<String> {
        self.ensure_crsql()?;
        self.refresh_query_schema()?;
        // Snapshot replication status BEFORE executing, keyed by stable
        // table id (the inspect/FlagStore contract): afterwards a
        // dropped table's clock is gone and a live probe would lie.
        // WP-A's flag map replaces this CRR fallback.
        let mut guarded = BTreeSet::new();
        for t in &self.query_schema.tables {
            // Map-backed FlagStore: a CRR table the operator disabled is
            // local-only, so destructive DDL needs no peer protection.
            // Unreadable flags fail closed (guarded).
            if murmur_query::adapt::is_crr(&self.db, &t.name).unwrap_or(false)
                && self.table_enabled(&t.name).unwrap_or(true)
            {
                guarded.insert(t.id.clone());
            }
        }
        let out = murmur_query::adapt::actions::migrate(
            &self.db,
            &self.query_schema,
            sql,
            mappings_json,
            &|id| guarded.contains(id),
        )
        .map_err(config::query_err)?;
        // DDL changed the world: snapshot the new schema for ticks.
        self.refresh_query_schema()?;
        self.sub_mgr.notify_reset("schema migrated");
        self.flush()?;
        Ok(out)
    }
    pub fn query_exec_json(&mut self, statements_json: &str) -> Result<String> {
        self.ensure_crsql()?;
        self.refresh_query_schema()?;
        let max: usize = config::setting(&self.db, "exec_max_statements")?
            .parse()
            .map_err(|_| Error::Invalid("invalid exec_max_statements".into()))?;
        let out = murmur_query::adapt::actions::exec_json(
            &self.db,
            &self.query_schema,
            statements_json,
            max,
        )
        .map_err(config::query_err)?;
        self.flush()?;
        Ok(out)
    }
    pub fn query_export(&mut self, since_version: i64) -> Result<String> {
        self.ensure_crsql()?;
        murmur_query::adapt::actions::export_changes(&self.db, since_version)
            .map_err(config::query_err)
    }
    pub fn query_import(&mut self, state_json: &str) -> Result<()> {
        self.ensure_crsql()?;
        murmur_query::adapt::actions::merge_state(&self.db, state_json)
            .map_err(config::query_err)
    }
    /// True when a query tick is due. Reads the interval live so
    /// stopped-time pragma changes apply without restart; a corrupt
    /// value falls back to 100ms rather than killing the background
    /// thread (actions validate strictly).
    pub fn query_tick_due(&mut self) -> Result<bool> {
        let interval = config::setting(&self.db, "search_interval_ms")?
            .parse::<u64>()
            .unwrap_or(100);
        if self.last_tick_at.elapsed()
            >= std::time::Duration::from_millis(interval.max(1))
        {
            self.last_tick_at = std::time::Instant::now();
            return Ok(true);
        }
        Ok(false)
    }
    /// One background maintenance pass: refresh the schema snapshot
    /// (keeping the last good one on unsupported DDL), feed exact
    /// changed keys to the search manager, drain bounded index work,
    /// and notify subscriptions. Non-CRR tables never appear in
    /// crsql_changes and need explicit murmur_refresh_indexes().
    /// Composite/text-pk changes and key floods fall back to a full
    /// rebuild; tick iterations are capped so a close never waits on
    /// an unbounded reindex.
    pub fn query_tick(&mut self) -> Result<()> {
        self.ensure_crsql()?;
        let next = murmur_query::schema::inspect(
            &self.db,
            &self.query_schema,
            &murmur_query::Migration::new(""),
            &|t| murmur_query::adapt::is_crr(&self.db, t).unwrap_or(false),
        )
        .map_err(config::query_err);
        if let Ok(s) = next {
            self.query_schema = s;
        }
        let version: i64 = self
            .db
            .query_row("SELECT crsql_db_version()", [], |r| r.get(0))?;
        let mut keys_by_table: BTreeMap<String, Vec<i64>> = BTreeMap::new();
        let mut exotic = false;
        let mut total = 0usize;
        {
            let mut q = self.db.prepare(
                "SELECT DISTINCT \"table\", pk FROM crsql_changes WHERE db_version > ?1",
            )?;
            let mut rows = q.query([self.last_tick_version as i64])?;
            while let Some(r) = rows.next()? {
                // Cap collection: floods rebuild instead of queueing.
                if total >= 1 << 20 {
                    exotic = true;
                    break;
                }
                let table: String = r.get(0)?;
                let pk: Vec<u8> = r.get(1)?;
                match decode_single_int_pk(&pk) {
                    Some(k) => {
                        keys_by_table.entry(table).or_default().push(k);
                        total += 1;
                    }
                    None => {
                        exotic = true;
                        keys_by_table.entry(table).or_default();
                    }
                }
            }
        }
        let tables: Vec<String> = keys_by_table.keys().cloned().collect();
        // Subscriptions advance even if indexing below fails.
        self.sub_mgr
            .notify_commit(version.max(0) as u64, &tables);
        self.last_tick_version = version.max(0) as u64;
        if exotic {
            let defs =
                murmur_query::adapt::declared_indexes(&self.db).map_err(config::query_err)?;
            self.search
                .set_explicit(&self.db, &self.query_schema, defs)
                .map_err(config::query_err)?;
            self.sub_mgr.notify_reset("search indexes reconfigured");
        } else {
            for (table, keys) in &keys_by_table {
                self.search.observe(table, keys);
            }
            let mut n = 0;
            while self.search.pending() && n < 64 {
                n += 1;
                self.search
                    .tick(&self.db, &self.query_schema)
                    .map_err(config::query_err)?;
            }
        }
        let _ = self.search.take_publications();
        Ok(())
    }
}
/// Decode a single-INTEGER cr-sqlite packed pk to its rowid key.
/// Packing (pack_columns.rs): one column, type byte `intlen<<3|1`
/// (Integer = 1), then big-endian signed bytes. Composite, text, and
/// blob pks return None and the caller rebuilds instead.
fn decode_single_int_pk(pk: &[u8]) -> Option<i64> {
    let [1, tb, rest @ ..] = pk else {
        return None;
    };
    if tb & 0x07 != 1 {
        return None;
    }
    let len = (tb >> 3) as usize;
    if !(1..=8).contains(&len) || rest.len() != len {
        return None;
    }
    let mut buf = if rest[0] & 0x80 == 0 {
        [0u8; 8]
    } else {
        [0xffu8; 8]
    };
    buf[8 - len..].copy_from_slice(rest);
    Some(i64::from_be_bytes(buf))
}
fn field(out: &mut Vec<u8>, value: &[u8]) {
    out.extend((value.len() as u32).to_le_bytes());
    out.extend(value);
}
/// Exclusive BLOB range end for a prefix scan: increment the last
/// non-0xff byte and truncate. `None` (all-0xff prefix) means unbounded.
fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while end.last() == Some(&0xff) {
        end.pop();
    }
    let last = end.last_mut()?;
    *last = last.wrapping_add(1);
    Some(end)
}
fn compare_key(a: &Key, b: &Key) -> std::cmp::Ordering {
    (a.version, a.seq, &a.table, &a.pk, &a.cid).cmp(&(b.version, b.seq, &b.table, &b.pk, &b.cid))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (tempfile::TempDir, Arc<Worker>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db").to_str().unwrap().to_owned();
        let worker = Worker::open_test(path).unwrap();
        (dir, worker)
    }

    #[test]
    fn orphan_clock_janitor_reaps_on_schema_change() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db").to_str().unwrap().to_owned();
        let worker = Worker::open_test(path.clone()).unwrap();
        // Outside connection simulates the driver: a DROP leaves
        // cr-sqlite side tables behind (plain tables here; no
        // cr-sqlite in unit tests, but the orphan shape is identical).
        let outside = rusqlite::Connection::open(&path).unwrap();
        outside
            .execute_batch(
                "CREATE TABLE crsql_master(key TEXT PRIMARY KEY, value ANY);
                 CREATE TABLE kept(id INTEGER PRIMARY KEY);
                 CREATE TABLE kept__crsql_clock(key BLOB, db_version INTEGER);
                 INSERT INTO kept__crsql_clock VALUES(1, 2);
                 CREATE TABLE gone__crsql_clock(key BLOB, db_version INTEGER);
                 INSERT INTO gone__crsql_clock VALUES(1, 7);
                 CREATE TABLE gone__crsql_pks(key BLOB);",
            )
            .unwrap();
        let side_tables = || -> Vec<String> {
            outside
                .prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name LIKE '%__crsql%' ORDER BY name")
                .unwrap()
                .query_map([], |r| r.get(0))
                .unwrap()
                .collect::<std::result::Result<_, _>>()
                .unwrap()
        };
        // Any worker job triggers the cookie check, which reaps orphans
        // but keeps side tables whose app table exists.
        worker.call(|_| Ok(())).unwrap();
        assert_eq!(side_tables(), vec!["kept__crsql_clock".to_string()]);
        // The dropped clock's max version pins the floor (db_version is
        // max-derived; without the pin the counter would rewind).
        let floor: i64 = outside
            .query_row(
                "SELECT CAST(value AS INTEGER) FROM crsql_master WHERE key='pre_compact_dbversion'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(floor, 7);
        // A later DROP re-arms the trigger: the newly orphaned clock goes.
        outside.execute_batch("DROP TABLE kept").unwrap();
        worker.call(|_| Ok(())).unwrap();
        assert!(side_tables().is_empty());
        // Pins never retreat: the smaller orphan max does not move the 7.
        let floor: i64 = outside
            .query_row(
                "SELECT CAST(value AS INTEGER) FROM crsql_master WHERE key='pre_compact_dbversion'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(floor, 7);
        // Idempotent once clean.
        assert_eq!(worker.call(|s| s.reap_orphan_clocks()).unwrap(), 0);
    }

    #[test]
    fn admitted_call_grants_and_releases() {
        let (_dir, worker) = setup();
        let out = worker
            .call_admitted(
                murmur_sched::WriterClass::Maintenance,
                Duration::from_secs(5),
                |_| Ok(7),
            )
            .unwrap();
        assert_eq!(out, 7);
        let snap = worker.scheduler_snapshot();
        assert_eq!(snap.maintenance.acquisitions, 1);
        assert_eq!(snap.remote.acquisitions, 0);
        assert!(!worker.scheduler.active());
    }

    #[test]
    fn admitted_call_timeout_maps_overloaded() {
        let (_dir, worker) = setup();
        let held = worker
            .scheduler
            .admit(murmur_sched::WriterClass::Remote)
            .unwrap();
        let err = worker
            .call_admitted(
                murmur_sched::WriterClass::Remote,
                Duration::from_millis(1),
                |_| Ok(()),
            )
            .unwrap_err();
        assert!(matches!(err, Error::Overloaded), "{err:?}");
        drop(held);
    }

    #[test]
    fn sched_config_from_valid_and_invalid() {
        let cfg = sched_config_from(80, 500, 50);
        assert_eq!(cfg.local_share, 80);
        assert_eq!(cfg.remote_share, 20);
        assert_eq!(cfg.max_debt, Duration::from_millis(500));
        assert_eq!(cfg.maintenance_reserve_every, 50);
        let default = murmur_sched::SchedulerConfig::default();
        for (local, debt, reserve) in [(0, 1000, 100), (100, 1000, 100), (90, 0, 100), (90, 60001, 100), (90, 1000, 0), (90, 1000, 1000001)] {
            assert_eq!(sched_config_from(local, debt, reserve), default);
        }
    }

    #[test]
    fn load_sched_config_missing_db_returns_default() {
        let cfg = load_sched_config("/nonexistent-dir-xyz/t.db", "no-such-vfs");
        assert_eq!(cfg, murmur_sched::SchedulerConfig::default());
    }

    #[test]
    fn table_enabled_follows_opt_out_map() {
        let (_dir, worker) = setup();
        worker
            .call(|s| {
                assert!(s.table_enabled("docs")?);
                assert!(s.table_enabled("anything")?);
                s.db.execute(
                    "INSERT INTO murmur_config VALUES('table_replication','{\"Scratch\":false,\"docs\":true}') \
                     ON CONFLICT(name) DO UPDATE SET value=excluded.value",
                    [],
                )?;
                assert!(!s.table_enabled("scratch")?);
                assert!(!s.table_enabled("SCRATCH")?);
                assert!(s.table_enabled("docs")?);
                assert!(s.table_enabled("other")?);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn reap_staging_removes_only_expired_rows() {
        let (_dir, worker) = setup();
        worker
            .call(|s| {
                s.db.execute(
                    "INSERT INTO murmur_config VALUES('staging_timeout_ms','1000') \
                     ON CONFLICT(name) DO UPDATE SET value=excluded.value",
                    [],
                )?;
                s.db.execute(
                    "INSERT INTO murmur_stage VALUES(?1,0,x'00',unixepoch()*1000-60000)",
                    [[1u8; 16].as_slice()],
                )?;
                s.db.execute(
                    "INSERT INTO murmur_stage VALUES(?1,0,x'00',unixepoch()*1000)",
                    [[2u8; 16].as_slice()],
                )?;
                Ok(())
            })
            .unwrap();
        let removed = worker.call(|s| s.reap_staging()).unwrap();
        assert_eq!(removed, 1);
        let left: i64 = worker
            .call(|s| {
                Ok(s.db.query_row("SELECT count(*) FROM murmur_stage", [], |r| {
                    r.get(0)
                })?)
            })
            .unwrap();
        assert_eq!(left, 1);
    }

    #[test]
    fn gc_tombstones_prunes_markers_only_and_advances_watermark() {
        let (_dir, worker) = setup();
        // Fake clock table (same shape cr-sqlite creates); the pruner
        // discovers by suffix, so no CRRs are needed. The app table
        // must exist or the orphan janitor reaps the clock first.
        worker
            .call(|s| {
                s.db.execute_batch(
                    "CREATE TABLE t(id INTEGER PRIMARY KEY);
                     CREATE TABLE t__crsql_clock(key INTEGER, col_name TEXT, col_version INTEGER, db_version INTEGER, site_id INTEGER, seq INTEGER);
                     INSERT INTO t__crsql_clock VALUES(1,'-1',2,3,0,0),(2,'-1',1,100,0,0),(2,'body',1,100,0,0),(3,'body',1,4,0,0);",
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(worker.call(|s| s.gc_watermark()).unwrap(), 0);
        // Floor 50 prunes only the old tombstone; the live cell at
        // version 4 and everything above the floor stays.
        let deleted = worker.call(|s| s.gc_tombstones(50)).unwrap();
        assert_eq!(deleted, 1);
        let left: i64 = worker
            .call(|s| {
                Ok(s.db.query_row("SELECT count(*) FROM t__crsql_clock", [], |r| {
                    r.get(0)
                })?)
            })
            .unwrap();
        assert_eq!(left, 3);
        assert_eq!(worker.call(|s| s.gc_watermark()).unwrap(), 50);
        // Watermarks never retreat, even on an empty prune.
        let deleted = worker.call(|s| s.gc_tombstones(10)).unwrap();
        assert_eq!(deleted, 0);
        assert_eq!(worker.call(|s| s.gc_watermark()).unwrap(), 50);
    }
}
