use crate::{
    protocol::{self, Cursor, Key},
    *,
};
use rusqlite::{Connection, OptionalExtension, params, types::ValueRef};
use std::sync::mpsc;
type Job = Box<dyn FnOnce(&mut Store) + Send>;
const FLUSH: i32 = 0x535802;
pub struct Worker {
    sender: Option<mpsc::Sender<Job>>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Worker {
    pub fn open(path: String, vfs: String) -> Result<Arc<Self>> {
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
        }))
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
                let _ = tx.send(f(s));
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
        db.execute_batch("CREATE TABLE IF NOT EXISTS murmur_config(name TEXT PRIMARY KEY,value TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS murmur_identity(id INTEGER PRIMARY KEY CHECK(id=1),node BLOB,site BLOB,incarnation INTEGER NOT NULL DEFAULT 0);
          INSERT OR IGNORE INTO murmur_identity(id) VALUES(1);
          CREATE TABLE IF NOT EXISTS murmur_progress(node BLOB PRIMARY KEY,site BLOB NOT NULL,cursor BLOB NOT NULL,sent BLOB);
          CREATE TABLE IF NOT EXISTS murmur_excluded(node BLOB PRIMARY KEY);
          CREATE TABLE IF NOT EXISTS murmur_stage(node BLOB NOT NULL,idx INTEGER NOT NULL,data BLOB NOT NULL,created INTEGER NOT NULL,PRIMARY KEY(node,idx));")?;
        let s = Self {
            db,
            initialized: false,
        };
        s.flush()?;
        Ok(s)
    }
    pub fn flush(&self) -> Result<()> {
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
    pub fn initialize(&mut self) -> Result<Config> {
        if !self.initialized {
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
        }
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
        // Clock tables identify CRRs; compare only their application table DDL.
        let mut q=self.db.prepare("SELECT name,sql FROM sqlite_schema WHERE type='table' AND name IN (SELECT substr(name,1,length(name)-13) FROM sqlite_schema WHERE type='table' AND substr(name,-13)='__crsql_clock') ORDER BY name")?;
        let rows = q.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
        let mut schema = Vec::new();
        for row in rows {
            let (name, ddl) = row?;
            if name.starts_with("murmur_") {
                return Err(Error::Invalid("murmur tables must not be CRRs".into()));
            }
            schema.push((name, ddl));
        }
        if schema.is_empty() {
            return Err(Error::Invalid("no CRR tables configured".into()));
        }
        Ok(protocol::digest(&encode(&schema)?))
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
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.query_row("SELECT crsql_ingest_blob(?)", [blob], |_| Ok(()))?;
        tx.execute(
            "UPDATE murmur_progress SET cursor=?1 WHERE node=?2",
            params![encode(&next)?, node.as_slice()],
        )?;
        tx.execute("DELETE FROM murmur_stage WHERE node=?", [node.as_slice()])?;
        tx.commit()?;
        self.flush()
    }
}
fn field(out: &mut Vec<u8>, value: &[u8]) {
    out.extend((value.len() as u32).to_le_bytes());
    out.extend(value);
}
fn compare_key(a: &Key, b: &Key) -> std::cmp::Ordering {
    (a.version, a.seq, &a.table, &a.pk, &a.cid).cmp(&(b.version, b.seq, &b.table, &b.pk, &b.cid))
}
