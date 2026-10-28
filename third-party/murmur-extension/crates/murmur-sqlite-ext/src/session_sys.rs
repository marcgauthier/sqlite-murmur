//! Manually declared FFI for the subset of the SQLite Sessions API the
//! extension needs.
//!
//! `rusqlite` cannot provide these: `libsqlite3-sys` refuses to combine the
//! `loadable_extension` and `preupdate_hook`/`session` features because the
//! session entry points are not part of the `sqlite3_api_routines` dispatch
//! table. Instead we declare them here as plain `extern "C"` symbols.
//! Dynamic builds resolve them from the host's `libsqlite3` at load time
//! (our `build.rs` records a `DT_NEEDED` on it), so the session calls
//! always hit the same SQLite instance that owns the connection. Static
//! builds (`static-sqlite`) resolve them against the vendored amalgamation
//! at link time instead — same declarations, no host dependency, at the
//! cost of a same-version requirement (see `vendor/README.md`).
//!
//! Signatures verified against the SQLite 3.45.1 amalgamation `sqlite3.h`.
//! All functions are `unsafe`: session objects must be created, used, and
//! deleted on the owning connection's thread, and before the connection
//! closes.

use rusqlite::ffi::sqlite3;
use std::os::raw::{c_char, c_int, c_uchar, c_void};

pub enum SessionHandle {}
pub enum ChangesetIter {}

pub const SQLITE_OK: c_int = 0;
pub const SQLITE_ROW: c_int = 100;
pub const SQLITE_DONE: c_int = 101;
pub const SQLITE_MISUSE: c_int = 21;

extern "C" {
    pub fn sqlite3session_create(
        db: *mut sqlite3,
        z_db: *const c_char,
        pp_session: *mut *mut SessionHandle,
    ) -> c_int;
    pub fn sqlite3session_attach(p_session: *mut SessionHandle, z_tab: *const c_char) -> c_int;
    pub fn sqlite3session_delete(p_session: *mut SessionHandle);
    pub fn sqlite3session_changeset(
        p_session: *mut SessionHandle,
        pn_changeset: *mut c_int,
        pp_changeset: *mut *mut c_void,
    ) -> c_int;
    pub fn sqlite3changeset_start(
        pp: *mut *mut ChangesetIter,
        n_changeset: c_int,
        p_changeset: *mut c_void,
    ) -> c_int;
    pub fn sqlite3changeset_next(p_iter: *mut ChangesetIter) -> c_int;
    pub fn sqlite3changeset_op(
        p_iter: *mut ChangesetIter,
        pz_tab: *mut *const c_char,
        pn_col: *mut c_int,
        p_op: *mut c_int,
        pb_indirect: *mut c_int,
    ) -> c_int;
    pub fn sqlite3changeset_finalize(p_iter: *mut ChangesetIter) -> c_int;
    pub fn sqlite3changeset_pk(
        p_iter: *mut ChangesetIter,
        pp_pk: *mut *mut c_uchar,
        pn_col: *mut c_int,
    ) -> c_int;
    pub fn sqlite3changeset_old(
        p_iter: *mut ChangesetIter,
        i_val: c_int,
        pp_value: *mut *mut rusqlite::ffi::sqlite3_value,
    ) -> c_int;
    pub fn sqlite3changeset_new(
        p_iter: *mut ChangesetIter,
        i_val: c_int,
        pp_value: *mut *mut rusqlite::ffi::sqlite3_value,
    ) -> c_int;
    pub fn sqlite3_free(p: *mut c_void);
}

/// Changeset operation codes (`SQLITE_INSERT/UPDATE/DELETE`).
pub const SQLITE_INSERT_OP: c_int = 18;
pub const SQLITE_UPDATE_OP: c_int = 23;
pub const SQLITE_DELETE_OP: c_int = 9;

/// Owned session object. `Send` so it can live in a global registry, but every
/// method is `unsafe` and must be called on the owning connection's thread.
pub struct Session {
    ptr: *mut SessionHandle,
}

// SAFETY: see struct docs; the spike test is single-threaded per connection.
unsafe impl Send for Session {}

impl Session {
    /// Create a session on `db` (`"main"`) and attach all tables (`NULL`).
    ///
    /// # Safety
    /// `db` must be a live connection on the calling thread. The session must
    /// be deleted (`Drop`) on the same thread before `db` closes, and no
    /// preupdate hook may be registered on `db` while it lives.
    /// Create a session attaching exactly `tables` (never the outbox or
    /// anything else). Same safety contract as [`Session::create_attach_list`].
    pub unsafe fn create_attach_list<'a>(
        db: *mut sqlite3,
        tables: impl Iterator<Item = &'a str>,
    ) -> Result<Self, c_int> {
        let mut ptr: *mut SessionHandle = std::ptr::null_mut();
        let rc = sqlite3session_create(db, c"main".as_ptr(), &mut ptr);
        if rc != SQLITE_OK {
            return Err(rc);
        }
        for table in tables {
            let name = match std::ffi::CString::new(table) {
                Ok(name) => name,
                Err(_) => {
                    sqlite3session_delete(ptr);
                    return Err(SQLITE_MISUSE);
                }
            };
            let rc = sqlite3session_attach(ptr, name.as_ptr());
            if rc != SQLITE_OK {
                sqlite3session_delete(ptr);
                return Err(rc);
            }
        }
        Ok(Self { ptr })
    }

    pub fn as_ptr(&mut self) -> *mut SessionHandle {
        self.ptr
    }

    /// Extract the current changeset bytes. Does not reset the session.
    ///
    /// # Safety
    /// Same thread affinity as [`Session::create_attach_list`].
    pub unsafe fn changeset_bytes(&mut self) -> Result<Vec<u8>, c_int> {
        let mut n: c_int = 0;
        let mut p: *mut c_void = std::ptr::null_mut();
        let rc = sqlite3session_changeset(self.ptr, &mut n, &mut p);
        if rc != SQLITE_OK {
            return Err(rc);
        }
        let out = if n == 0 || p.is_null() {
            Vec::new()
        } else {
            std::slice::from_raw_parts(p as *const u8, n as usize).to_vec()
        };
        if !p.is_null() {
            sqlite3_free(p);
        }
        Ok(out)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        // SAFETY: by construction `ptr` came from `sqlite3session_create` and
        // every owner upholds the thread/drop-before-close contract.
        unsafe { sqlite3session_delete(self.ptr) };
    }
}
