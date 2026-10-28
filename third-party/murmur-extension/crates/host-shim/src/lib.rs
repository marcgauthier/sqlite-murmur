//! Minimal foreign SQLite host for integration tests.
//!
//! Links the system `libsqlite3` directly through hand-declared FFI instead of
//! `rusqlite`: the extension under test must survive arbitrary hosts, and
//! `rusqlite` cannot mix its `loadable_extension` build with a
//! `bundled`/`session` host build in one compilation. Shared by the
//! extension's integration tests and the `crash-host` helper binary.

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_void};

mod sys {
    use std::os::raw::{c_char, c_int, c_uchar, c_void};

    pub enum Db {}
    pub enum Stmt {}

    pub const SQLITE_OK: c_int = 0;
    pub const SQLITE_ROW: c_int = 100;
    pub const SQLITE_DONE: c_int = 101;
    pub const SQLITE_INTEGER: c_int = 1;
    pub const SQLITE_TEXT: c_int = 3;
    pub const SQLITE_BLOB: c_int = 4;
    pub const SQLITE_NULL: c_int = 5;

    #[link(name = "sqlite3")]
    extern "C" {
        pub fn sqlite3_open(filename: *const c_char, pp_db: *mut *mut Db) -> c_int;
        pub fn sqlite3_close(db: *mut Db) -> c_int;
        pub fn sqlite3_busy_timeout(db: *mut Db, ms: c_int) -> c_int;
        pub fn sqlite3_enable_load_extension(db: *mut Db, onoff: c_int) -> c_int;
        pub fn sqlite3_load_extension(
            db: *mut Db,
            z_file: *const c_char,
            z_proc: *const c_char,
            pz_err_msg: *mut *mut c_char,
        ) -> c_int;
        pub fn sqlite3_exec(
            db: *mut Db,
            sql: *const c_char,
            callback: *mut c_void,
            arg: *mut c_void,
            errmsg: *mut *mut c_char,
        ) -> c_int;
        pub fn sqlite3_prepare_v2(
            db: *mut Db,
            z_sql: *const c_char,
            n_byte: c_int,
            pp_stmt: *mut *mut Stmt,
            pz_tail: *mut *const c_char,
        ) -> c_int;
        pub fn sqlite3_step(stmt: *mut Stmt) -> c_int;
        pub fn sqlite3_finalize(stmt: *mut Stmt) -> c_int;
        pub fn sqlite3_column_count(stmt: *mut Stmt) -> c_int;
        pub fn sqlite3_column_type(stmt: *mut Stmt, col: c_int) -> c_int;
        pub fn sqlite3_column_int64(stmt: *mut Stmt, col: c_int) -> i64;
        pub fn sqlite3_column_bytes(stmt: *mut Stmt, col: c_int) -> c_int;
        pub fn sqlite3_column_blob(stmt: *mut Stmt, col: c_int) -> *const c_void;
        pub fn sqlite3_column_text(stmt: *mut Stmt, col: c_int) -> *const c_uchar;
        pub fn sqlite3_errmsg(db: *mut Db) -> *const c_char;
        pub fn sqlite3_free(p: *mut c_void);
    }
}

use sys::*;

/// An open SQLite connection. Not `Sync`: drive each connection from one
/// thread, like any SQLite host.
pub struct Conn {
    db: *mut Db,
}

impl Conn {
    pub fn open(path: &str) -> Result<Self, String> {
        let path = CString::new(path).map_err(|e| e.to_string())?;
        let mut db: *mut Db = std::ptr::null_mut();
        let rc = unsafe { sqlite3_open(path.as_ptr(), &mut db) };
        if rc != SQLITE_OK || db.is_null() {
            return Err(format!("sqlite3_open failed rc={rc}"));
        }
        unsafe { sqlite3_busy_timeout(db, 10_000) };
        Ok(Self { db })
    }

    pub fn open_memory() -> Result<Self, String> {
        Self::open(":memory:")
    }

    pub fn last_error(&self) -> String {
        let msg = unsafe { CStr::from_ptr(sqlite3_errmsg(self.db)) };
        msg.to_string_lossy().into_owned()
    }

    fn take_errmsg(&self, err: *mut c_char) -> String {
        if err.is_null() {
            self.last_error()
        } else {
            let m = unsafe { CStr::from_ptr(err) }.to_string_lossy().into_owned();
            unsafe { sqlite3_free(err as *mut c_void) };
            m
        }
    }

    pub fn exec(&self, sql: &str) -> Result<(), String> {
        let sql = CString::new(sql).map_err(|e| e.to_string())?;
        let mut err: *mut c_char = std::ptr::null_mut();
        let rc = unsafe {
            sqlite3_exec(
                self.db,
                sql.as_ptr(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut err,
            )
        };
        if rc != SQLITE_OK {
            return Err(format!("exec({sql:?}) rc={rc}: {}", self.take_errmsg(err)));
        }
        Ok(())
    }

    pub fn load_extension(&self, path: &str, proc: Option<&str>) -> Result<(), String> {
        if unsafe { sqlite3_enable_load_extension(self.db, 1) } != SQLITE_OK {
            return Err("enable_load_extension failed".into());
        }
        let path = CString::new(path).map_err(|e| e.to_string())?;
        let proc = proc
            .map(CString::new)
            .transpose()
            .map_err(|e| e.to_string())?;
        let mut err: *mut c_char = std::ptr::null_mut();
        let rc = unsafe {
            sqlite3_load_extension(
                self.db,
                path.as_ptr(),
                proc.as_ref().map_or(std::ptr::null(), |p| p.as_ptr()),
                &mut err,
            )
        };
        if rc != SQLITE_OK {
            return Err(format!("load_extension rc={rc}: {}", self.take_errmsg(err)));
        }
        Ok(())
    }

    /// Run a query expected to return exactly one row; hand the raw statement
    /// to `read` for column extraction.
    fn query_one<T>(
        &self,
        sql: &str,
        read: impl FnOnce(*mut Stmt) -> Result<T, String>,
    ) -> Result<T, String> {
        let sql = CString::new(sql).map_err(|e| e.to_string())?;
        let mut stmt: *mut Stmt = std::ptr::null_mut();
        let rc = unsafe {
            sqlite3_prepare_v2(self.db, sql.as_ptr(), -1, &mut stmt, std::ptr::null_mut())
        };
        if rc != SQLITE_OK {
            return Err(format!("prepare({sql:?}) rc={rc}: {}", self.last_error()));
        }
        let rc = unsafe { sqlite3_step(stmt) };
        if rc != SQLITE_ROW {
            let msg = format!("step({sql:?}) rc={rc}: {}", self.last_error());
            unsafe { sqlite3_finalize(stmt) };
            return Err(msg);
        }
        let out = read(stmt);
        unsafe { sqlite3_finalize(stmt) };
        out
    }

    pub fn query_text(&self, sql: &str) -> Result<String, String> {
        self.query_one(sql, |stmt| {
            if unsafe { sqlite3_column_type(stmt, 0) } != SQLITE_TEXT {
                return Err(format!("{sql:?} did not return TEXT"));
            }
            let p = unsafe { sqlite3_column_text(stmt, 0) };
            if p.is_null() {
                return Err(format!("{sql:?} returned NULL text"));
            }
            Ok(unsafe { CStr::from_ptr(p as *const c_char) }
                .to_string_lossy()
                .into_owned())
        })
    }

    pub fn query_int(&self, sql: &str) -> Result<i64, String> {
        self.query_one(sql, |stmt| {
            if unsafe { sqlite3_column_type(stmt, 0) } != SQLITE_INTEGER {
                return Err(format!("{sql:?} did not return INTEGER"));
            }
            Ok(unsafe { sqlite3_column_int64(stmt, 0) })
        })
    }

    pub fn query_int_nullable(&self, sql: &str) -> Result<Option<i64>, String> {
        self.query_one(sql, |stmt| {
            match unsafe { sqlite3_column_type(stmt, 0) } {
                SQLITE_INTEGER => Ok(Some(unsafe { sqlite3_column_int64(stmt, 0) })),
                SQLITE_NULL => Ok(None),
                t => Err(format!("{sql:?} returned unexpected type {t}")),
            }
        })
    }

    /// Dump all rows with each cell rendered by type (integers, text,
    /// hex blobs, NULLs). For content-equality assertions between nodes.
    pub fn query_dump(&self, sql: &str) -> Result<Vec<Vec<String>>, String> {
        let sql_c = CString::new(sql).map_err(|e| e.to_string())?;
        let mut stmt: *mut Stmt = std::ptr::null_mut();
        let rc = unsafe {
            sqlite3_prepare_v2(self.db, sql_c.as_ptr(), -1, &mut stmt, std::ptr::null_mut())
        };
        if rc != SQLITE_OK {
            return Err(format!("prepare({sql:?}) rc={rc}: {}", self.last_error()));
        }
        let mut out = Vec::new();
        loop {
            let rc = unsafe { sqlite3_step(stmt) };
            if rc == SQLITE_DONE {
                break;
            }
            if rc != SQLITE_ROW {
                let msg = format!("step({sql:?}) rc={rc}: {}", self.last_error());
                unsafe { sqlite3_finalize(stmt) };
                return Err(msg);
            }
            let mut row = Vec::new();
            let ncol = unsafe { sqlite3_column_count(stmt) };
            for col in 0..ncol {
                row.push(match unsafe { sqlite3_column_type(stmt, col) } {
                    SQLITE_INTEGER => unsafe { sqlite3_column_int64(stmt, col) }.to_string(),
                    SQLITE_TEXT => {
                        let p = unsafe { sqlite3_column_text(stmt, col) };
                        if p.is_null() {
                            "NULL".into()
                        } else {
                            unsafe { CStr::from_ptr(p as *const c_char) }
                                .to_string_lossy()
                                .into_owned()
                        }
                    }
                    SQLITE_BLOB => {
                        let n = unsafe { sqlite3_column_bytes(stmt, col) } as usize;
                        let p = unsafe { sqlite3_column_blob(stmt, col) };
                        if n == 0 || p.is_null() {
                            "blob:".into()
                        } else {
                            let b = unsafe { std::slice::from_raw_parts(p as *const u8, n) };
                            format!(
                                "blob:{}",
                                b.iter().map(|x| format!("{x:02x}")).collect::<String>()
                            )
                        }
                    }
                    SQLITE_NULL => "NULL".into(),
                    t => format!("<type {t}>"),
                });
            }
            out.push(row);
        }
        unsafe { sqlite3_finalize(stmt) };
        Ok(out)
    }

    pub fn query_blob(&self, sql: &str) -> Result<Vec<u8>, String> {
        self.query_one(sql, |stmt| {
            if unsafe { sqlite3_column_type(stmt, 0) } != SQLITE_BLOB {
                return Err(format!("{sql:?} did not return BLOB"));
            }
            let n = unsafe { sqlite3_column_bytes(stmt, 0) } as usize;
            let p = unsafe { sqlite3_column_blob(stmt, 0) };
            if n == 0 || p.is_null() {
                Ok(Vec::new())
            } else {
                Ok(unsafe { std::slice::from_raw_parts(p as *const u8, n) }.to_vec())
            }
        })
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        unsafe { sqlite3_close(self.db) };
    }
}
