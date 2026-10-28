use crate::{engine::Handle, storage::Worker, *};
use rusqlite::{Connection, ffi, functions::FunctionFlags, vtab};
use std::{
    ffi::{CStr, c_char, c_void},
    sync::{Mutex, OnceLock, Weak},
};
const REGISTER: i32 = 0x535801;
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
}
impl Control {
    fn start(&self) -> Result<i64> {
        let mut h = self.handle.lock().unwrap();
        if h.is_some() {
            return Ok(0);
        }
        let cfg = self.worker.call(|s| s.initialize())?;
        *h = Some(Handle::start(cfg, self.worker.clone())?);
        Ok(1)
    }
    fn stop(&self) -> i64 {
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
    fn status(&self) -> String {
        self.handle.lock().unwrap().as_ref().map_or_else(
            || "{\"running\":false}".into(),
            |h| h.live.status().to_string(),
        )
    }
}
impl Drop for Control {
    fn drop(&mut self) {
        self.handle.get_mut().unwrap().take();
    }
}
struct PragmaContext {
    control: Arc<Control>,
    db: usize,
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
        drop(unsafe { Box::from_raw(p.cast::<PragmaContext>()) });
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
        ctx.control.worker.call(move|s|{
            if name=="group_id"{let node:Option<Vec<u8>>=s.db.query_row("SELECT node FROM murmur_identity WHERE id=1",[],|r|r.get(0))?;
                if node.is_some()&&config::setting(&s.db,&name)?!=value{return Err(Error::Invalid("group identity cannot change after first start; use a new database".into()));}}
            s.db.execute("INSERT INTO murmur_config VALUES(?1,?2) ON CONFLICT(name) DO UPDATE SET value=excluded.value",rusqlite::params![name,value])?;s.flush()?;Ok(value)
        })
    }));
    let (rc, msg) = match result {
        Ok(Ok(s)) => (ffi::SQLITE_OK, s),
        Ok(Err(e)) => (ffi::SQLITE_ERROR, e.to_string()),
        Err(_) => (ffi::SQLITE_ERROR, "murmur PRAGMA panic".into()),
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
    rusqlite::Error::UserFunctionError(Box::new(e))
}
pub unsafe fn register(raw: *mut ffi::sqlite3) -> Result<()> {
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
            "requires a named sqlite-express database".into(),
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
    if vfs_name != "sqlite-express" {
        return Err(Error::Invalid(
            "murmur-replication requires the sqlite-express VFS".into(),
        ));
    }
    let mut services = SERVICES.get_or_init(Default::default).lock().unwrap();
    services.retain(|_, v| v.strong_count() > 0);
    if services.get(&path).and_then(Weak::upgrade).is_some() {
        return Err(Error::Invalid(
            "murmur already registered on a control connection for this database".into(),
        ));
    }
    let control = Arc::new(Control {
        worker: Worker::open(path.clone(), vfs_name)?,
        handle: Mutex::new(None),
    });
    let flags = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DIRECTONLY;
    for name in ["murmur_start", "murmur_stop", "murmur_sync"] {
        let c = control.clone();
        let raw = raw as usize;
        db.create_scalar_function(name, 0, flags, move |_| {
            if unsafe { ffi::sqlite3_get_autocommit(raw as *mut ffi::sqlite3) } == 0 {
                return Err(sql_error(Error::Invalid(
                    "replication controls require autocommit".into(),
                )));
            }
            match name {
                "murmur_start" => c.start().map_err(sql_error),
                "murmur_stop" => Ok(c.stop()),
                _ => {
                    let h = c.handle.lock().unwrap();
                    let h = h.as_ref().ok_or_else(|| {
                        sql_error(Error::Unavailable("replication is stopped".into()))
                    })?;
                    h.live.sync.notify_waiters();
                    Ok(1)
                }
            }
        })?;
    }
    let c = control.clone();
    db.create_scalar_function("murmur_status", 0, flags, move |_| Ok(c.status()))?;
    let c = control.clone();
    let raw_addr = raw as usize;
    db.create_scalar_function("murmur_add_peer",1,flags,move|ctx|{
        if unsafe{ffi::sqlite3_get_autocommit(raw_addr as *mut ffi::sqlite3)}==0{return Err(sql_error(Error::Invalid("peer controls require autocommit".into())));}
        let address:String=ctx.get(0)?;let addr=address.parse::<SocketAddr>().map_err(|_|sql_error(Error::Invalid("expected IP:port".into())))?;
        let h=c.handle.lock().unwrap();
        c.worker.call(move|s|{let mut seeds:Vec<SocketAddr>=serde_json::from_str(&config::setting(&s.db,"bootstrap")?).map_err(|e|Error::Invalid(e.to_string()))?;if !seeds.contains(&addr){if seeds.len()>=4096{return Err(Error::Overloaded);}seeds.push(addr);}
            s.db.execute("INSERT INTO murmur_config VALUES('bootstrap',?1) ON CONFLICT(name) DO UPDATE SET value=excluded.value",[serde_json::to_string(&seeds).unwrap()])?;s.flush()}).map_err(sql_error)?;
        if let Some(h)=h.as_ref(){h.live.add(addr).map_err(sql_error)?;}Ok(1)
    })?;
    let c = control.clone();
    db.create_scalar_function("murmur_remove_peer", 1, flags, move |ctx| {
        if unsafe { ffi::sqlite3_get_autocommit(raw_addr as *mut ffi::sqlite3) } == 0 {
            return Err(sql_error(Error::Invalid(
                "peer controls require autocommit".into(),
            )));
        }
        let id = parse_id(&ctx.get::<String>(0)?).map_err(sql_error)?;
        let h = c.handle.lock().unwrap();
        c.worker
            .call(move |s| {
                s.db.execute(
                    "INSERT OR IGNORE INTO murmur_excluded VALUES(?)",
                    [id.as_slice()],
                )?;
                s.flush()
            })
            .map_err(sql_error)?;
        if let Some(h) = h.as_ref() {
            h.live.remove(id);
        }
        Ok(1)
    })?;
    const MODULE: vtab::Module<PeerTable> = vtab::Module::eponymous_only_module();
    db.create_module(c"murmur_peers", &MODULE, Some(control.clone()))?;
    let ctx = Box::into_raw(Box::new(PragmaContext {
        control: control.clone(),
        db: raw as usize,
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
