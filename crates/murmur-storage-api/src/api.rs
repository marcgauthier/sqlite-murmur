//! Integrated open/close. Rust port of `src/sqlite_murmur_api.cpp`.
//!
//! Opens databases through the `sqlite-murmur` VFS, applies the cipher /
//! compression options before extension metadata exists, then initializes
//! the built-in CRR + Murmur SQL interfaces unless the handle is
//! storage-only or read-only. Close finalizes CRR state before SQLite.
//!
//! SQLite itself is used through hand-declared FFI (no `rusqlite`
//! dependency): in the CMake staticlib the symbols resolve from the
//! vendored amalgamation at final link, exactly like the C++ objects.
//! Under `cfg(test)` the same declarations link the system SQLite so the
//! open/close flow runs for real (default VFS — the murmur VFS lives in
//! the C++ store until Phase 4).
//!
//! Cargo features mirror the CMake options: `crsqlite` and `murmur`
//! compile in the extension initializers; without them the symbols are
//! never referenced, so option-off builds link cleanly.

/// SQLite result codes used here (`third-party/sqlite/sqlite3.h`).
/// Duplicated from `murmur-storage` (this crate is standalone; see lib.rs).
const SQLITE_OK: i32 = 0;
const SQLITE_MISUSE: i32 = 21;
// Step-result codes: CRR finalizing plus unit-test assertions.
#[cfg(any(test, feature = "crsqlite"))]
const SQLITE_ROW: i32 = 100;
#[cfg(feature = "crsqlite")]
const SQLITE_DONE: i32 = 101;

/// Panic boundary for `extern "C"` entries (duplicated from
/// `murmur-storage`; see lib.rs). FFI entries fail closed and never
/// unwind into C/C++.
fn guard<F, R>(f: F, fallback: R) -> R
where
    F: FnOnce() -> R,
{
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).unwrap_or(fallback)
}

const SQLITE_ERROR: i32 = 1;
use std::ffi::{CStr, c_char, c_int, c_void};
use std::ptr;

// ---------------------------------------------------------------------------
// SQLite FFI (subset used by api.cpp, declared from sqlite3.h).

#[allow(non_camel_case_types)]
#[repr(C)]
pub struct sqlite3 {
    _private: [u8; 0],
}

#[allow(non_camel_case_types)]
#[repr(C)]
pub struct sqlite3_stmt {
    _private: [u8; 0],
}

pub type ExecCallback =
    Option<unsafe extern "C" fn(*mut c_void, c_int, *mut *mut c_char, *mut *mut c_char) -> c_int>;

#[cfg_attr(test, link(name = "sqlite3"))]
unsafe extern "C" {
    unsafe fn sqlite3_open_v2(
        filename: *const c_char,
        pp_db: *mut *mut sqlite3,
        flags: c_int,
        vfs: *const c_char,
    ) -> c_int;
    unsafe fn sqlite3_exec(
        db: *mut sqlite3,
        sql: *const c_char,
        callback: ExecCallback,
        arg: *mut c_void,
        errmsg: *mut *mut c_char,
    ) -> c_int;
    unsafe fn sqlite3_close(db: *mut sqlite3) -> c_int;
    unsafe fn sqlite3_db_readonly(db: *mut sqlite3, dbname: *const c_char) -> c_int;
}

// Statement API: CRR finalizing (feature) plus unit-test assertions.
#[cfg(any(test, feature = "crsqlite"))]
#[cfg_attr(test, link(name = "sqlite3"))]
unsafe extern "C" {
    unsafe fn sqlite3_prepare_v2(
        db: *mut sqlite3,
        sql: *const c_char,
        nbyte: c_int,
        stmt: *mut *mut sqlite3_stmt,
        tail: *const *const c_char,
    ) -> c_int;
    unsafe fn sqlite3_step(stmt: *mut sqlite3_stmt) -> c_int;
    unsafe fn sqlite3_finalize(stmt: *mut sqlite3_stmt) -> c_int;
}

// Init-failure reporting: only extension builds can fail this way.
#[cfg(all(not(test), any(feature = "crsqlite", feature = "murmur")))]
unsafe extern "C" {
    unsafe fn sqlite3_free(ptr: *mut c_void);
    unsafe fn sqlite3_log(code: c_int, fmt: *const c_char, ...);
}

// ---------------------------------------------------------------------------
// C ABI types (mirrors include/sqlite_murmur.h).

/// `SQLITE_MURMUR_OPEN_STORAGE_ONLY`: skip replication initialization.
pub const OPEN_STORAGE_ONLY: u32 = 1;

/// `struct sqlite_murmur_open_options` (`unsigned` is 32-bit LP64/LLP64).
#[repr(C)]
pub struct OpenOptions {
    pub flags: u32,
    pub cipher: *const c_char,
    pub compression: *const c_char,
}

// ---------------------------------------------------------------------------
// Extension initializers (feature-gated like the C++ #ifdefs).

/// Log an init failure like the C++ `extensionResult`, free the message,
/// and return the code. Only compiled when an extension can fail.
#[cfg(all(not(test), any(feature = "crsqlite", feature = "murmur")))]
fn extension_result(rc: c_int, error: *mut c_char) -> c_int {
    if rc != SQLITE_OK && !error.is_null() {
        unsafe {
            sqlite3_log(
                rc,
                c"sqlite-murmur initialization: %s".as_ptr(),
                error as *const c_char,
            )
        };
    }
    unsafe { sqlite3_free(error as *mut c_void) };
    rc
}

#[cfg(all(not(test), feature = "crsqlite"))]
fn init_crsqlite(db: *mut sqlite3) -> c_int {
    unsafe extern "C" {
        unsafe fn sqlite3_crsqlite_init(
            db: *mut sqlite3,
            error: *mut *mut c_char,
            api: *const c_void,
        ) -> c_int;
    }
    let mut error: *mut c_char = ptr::null_mut();
    let rc = unsafe { sqlite3_crsqlite_init(db, &mut error, ptr::null()) };
    extension_result(rc, error)
}

/// Test double: observable marker proving the init path ran on a live
/// handle (the real initializer lives outside unit-test linkage).
#[cfg(all(test, feature = "crsqlite"))]
fn init_crsqlite(db: *mut sqlite3) -> c_int {
    unsafe {
        sqlite3_exec(
            db,
            c"CREATE TABLE _rs_test_crsql_init(x)".as_ptr(),
            None,
            ptr::null_mut(),
            ptr::null_mut(),
        )
    }
}

#[cfg(not(feature = "crsqlite"))]
fn init_crsqlite(_db: *mut sqlite3) -> c_int {
    SQLITE_OK
}

#[cfg(all(not(test), feature = "murmur"))]
fn init_murmur(db: *mut sqlite3) -> c_int {
    unsafe extern "C" {
        unsafe fn sqlite3_murmurreplication_attach(
            db: *mut sqlite3,
            error: *mut *mut c_char,
            api: *const c_void,
        ) -> c_int;
    }
    let mut error: *mut c_char = ptr::null_mut();
    let rc = unsafe { sqlite3_murmurreplication_attach(db, &mut error, ptr::null()) };
    extension_result(rc, error)
}

/// Test double: observable marker proving the init path ran on a live
/// handle (the real initializer lives outside unit-test linkage).
#[cfg(all(test, feature = "murmur"))]
fn init_murmur(db: *mut sqlite3) -> c_int {
    unsafe {
        sqlite3_exec(
            db,
            c"CREATE TABLE _rs_test_murmur_attach(x)".as_ptr(),
            None,
            ptr::null_mut(),
            ptr::null_mut(),
        )
    }
}

#[cfg(not(feature = "murmur"))]
fn init_murmur(_db: *mut sqlite3) -> c_int {
    SQLITE_OK
}

// ---------------------------------------------------------------------------
// Open/close logic.

/// Build `PRAGMA <name>=<value>` with `%Q` quoting (single quotes doubled).
/// `None` skips, matching the C++ `configure` NULL check.
fn configure_sql(name: &[u8], value: Option<&[u8]>) -> Option<Vec<u8>> {
    let value = value?;
    let mut sql = Vec::with_capacity(name.len() + value.len() + 12);
    sql.extend_from_slice(b"PRAGMA ");
    sql.extend_from_slice(name);
    sql.extend_from_slice(b"='");
    for &b in value {
        if b == b'\'' {
            sql.extend_from_slice(b"''");
        } else {
            sql.push(b);
        }
    }
    sql.extend_from_slice(b"'\0");
    Some(sql)
}

fn configure(db: *mut sqlite3, name: &[u8], value: Option<&[u8]>) -> c_int {
    let Some(sql) = configure_sql(name, value) else {
        return SQLITE_OK;
    };
    unsafe {
        sqlite3_exec(
            db,
            sql.as_ptr() as *const c_char,
            None,
            ptr::null_mut(),
            ptr::null_mut(),
        )
    }
}

fn opt_bytes<'a>(options: Option<&'a OpenOptions>, pick: fn(&'a OpenOptions) -> *const c_char) -> Option<&'a [u8]> {
    let options = options?;
    let ptr = pick(options);
    if ptr.is_null() {
        return None;
    }
    // SAFETY: NUL-terminated per the C contract.
    Some(unsafe { CStr::from_ptr(ptr) }.to_bytes())
}

fn open_inner(
    path: *const c_char,
    db: *mut *mut sqlite3,
    flags: c_int,
    options: Option<&OpenOptions>,
    vfs: *const c_char,
) -> c_int {
    // SAFETY: `db` was null-checked by the caller.
    unsafe { *db = ptr::null_mut() };
    if path.is_null() {
        return SQLITE_MISUSE;
    }
    // SAFETY: NUL-terminated per the C contract.
    let path = unsafe { CStr::from_ptr(path) };
    if path.to_bytes().is_empty() {
        return SQLITE_MISUSE;
    }
    if let Some(o) = options {
        if o.flags & !OPEN_STORAGE_ONLY != 0 {
            return SQLITE_MISUSE;
        }
    }
    // SAFETY: `db` points at caller storage; SQLite takes ownership of
    // the handle result exactly as in the C++ (on failure *db may be
    // non-NULL and must be closed).
    let mut rc = unsafe { sqlite3_open_v2(path.as_ptr(), db, flags, vfs) };
    if rc != SQLITE_OK {
        return rc;
    }
    // SAFETY: successful open yields a live handle in *db.
    let handle = unsafe { *db };
    rc = configure(handle, b"cipher", opt_bytes(options, |o| o.cipher));
    if rc == SQLITE_OK {
        rc = configure(handle, b"compression", opt_bytes(options, |o| o.compression));
    }
    if rc != SQLITE_OK {
        return rc;
    }
    let storage_only = options
        .map(|o| o.flags & OPEN_STORAGE_ONLY != 0)
        .unwrap_or(false);
    if storage_only || unsafe { sqlite3_db_readonly(handle, c"main".as_ptr()) } != 0 {
        return SQLITE_OK;
    }
    rc = init_crsqlite(handle);
    if rc != SQLITE_OK {
        return rc;
    }
    init_murmur(handle)
}

/// Default open: READWRITE | CREATE | FULLMUTEX.
#[unsafe(no_mangle)]
pub extern "C" fn sqlite_murmur_open(path: *const c_char, db: *mut *mut sqlite3) -> c_int {
    const SQLITE_OPEN_READWRITE: c_int = 0x2;
    const SQLITE_OPEN_CREATE: c_int = 0x4;
    const SQLITE_OPEN_FULLMUTEX: c_int = 0x10000;
    // No extra guard: the callee owns the panic boundary.
    sqlite_murmur_open_v2(
        path,
        db,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
        ptr::null(),
    )
}

/// Full open. See `include/sqlite_murmur.h` for the contract.
#[unsafe(no_mangle)]
pub extern "C" fn sqlite_murmur_open_v2(
    path: *const c_char,
    db: *mut *mut sqlite3,
    flags: c_int,
    options: *const OpenOptions,
) -> c_int {
    guard(
        || {
            if db.is_null() {
                return SQLITE_MISUSE;
            }
            // SAFETY: options is null or a valid shared borrow for the call.
            let options = unsafe { options.as_ref() };
            open_inner(path, db, flags, options, c"sqlite-murmur".as_ptr())
        },
        SQLITE_ERROR,
    )
}

/// Finalize CRR state when present, then close SQLite. Null is a no-op OK.
#[unsafe(no_mangle)]
pub extern "C" fn sqlite_murmur_close(db: *mut sqlite3) -> c_int {
    guard(
        || {
            if db.is_null() {
                return SQLITE_OK;
            }
            #[cfg(feature = "crsqlite")]
            {
                // Storage-only and read-only handles may not have CRR
                // functions registered: a failed prepare skips finalizing.
                let mut finalize: *mut sqlite3_stmt = ptr::null_mut();
                if unsafe {
                    sqlite3_prepare_v2(
                        db,
                        c"SELECT crsql_finalize()".as_ptr(),
                        -1,
                        &mut finalize,
                        ptr::null(),
                    )
                } == SQLITE_OK
                {
                    let rc = unsafe { sqlite3_step(finalize) };
                    unsafe { sqlite3_finalize(finalize) };
                    if rc != SQLITE_ROW && rc != SQLITE_DONE {
                        return rc;
                    }
                }
            }
            unsafe { sqlite3_close(db) }
        },
        SQLITE_ERROR,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use std::sync::atomic::{AtomicU64, Ordering};

    const SQLITE_OPEN_READONLY: c_int = 0x1;
    const SQLITE_OPEN_READWRITE: c_int = 0x2;
    const SQLITE_OPEN_CREATE: c_int = 0x4;
    const SQLITE_OPEN_FULLMUTEX: c_int = 0x10000;

    fn scratch(name: &str) -> std::path::PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "murmur-storage-api-{}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed),
            name
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn has_table(db: *mut sqlite3, name: &str) -> bool {
        let sql = CString::new(format!(
            "SELECT name FROM sqlite_master WHERE name='{name}'"
        ))
        .unwrap();
        let mut stmt: *mut sqlite3_stmt = ptr::null_mut();
        let rc = unsafe {
            sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null())
        };
        assert_eq!(rc, SQLITE_OK);
        let found = unsafe { sqlite3_step(stmt) } == SQLITE_ROW;
        unsafe { sqlite3_finalize(stmt) };
        found
    }

    #[test]
    fn pragma_quoting_matches_percent_q() {
        assert_eq!(configure_sql(b"cipher", None), None);
        assert_eq!(
            configure_sql(b"cipher", Some(b"aes256gcm")).unwrap(),
            b"PRAGMA cipher='aes256gcm'\0"
        );
        assert_eq!(
            configure_sql(b"compression", Some(b"a'b")).unwrap(),
            b"PRAGMA compression='a''b'\0"
        );
        assert_eq!(
            configure_sql(b"cipher", Some(b"")).unwrap(),
            b"PRAGMA cipher=''\0"
        );
    }

    #[test]
    fn validation_fails_misuse_without_touching_sqlite() {
        // Null db: rejected before *db is written (nothing to write to).
        assert_eq!(
            sqlite_murmur_open_v2(ptr::null(), ptr::null_mut(), 0, ptr::null()),
            SQLITE_MISUSE
        );
        // Null/empty path and unknown option flags: *db is nulled first,
        // exactly like the C++ (no SQLite call happens).
        for (path, flags) in [
            (None, None),
            (Some(c""), None),
            (Some(c":memory:"), Some(0x2u32)),
            (Some(c":memory:"), Some(u32::MAX)),
        ] {
            let mut slot: *mut sqlite3 = ptr::without_provenance_mut(0x1);
            let opts = flags.map(|flags| OpenOptions {
                flags,
                cipher: ptr::null(),
                compression: ptr::null(),
            });
            let rc = sqlite_murmur_open_v2(
                path.map(|p| p.as_ptr()).unwrap_or(ptr::null()),
                &mut slot,
                0,
                opts.as_ref()
                    .map(|o| o as *const OpenOptions)
                    .unwrap_or(ptr::null()),
            );
            assert_eq!(rc, SQLITE_MISUSE);
            assert!(slot.is_null());
        }
    }

    #[test]
    fn memory_round_trip_and_close() {
        let mut db: *mut sqlite3 = ptr::null_mut();
        let rc = open_inner(
            c":memory:".as_ptr(),
            &mut db,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
            None,
            ptr::null(),
        );
        assert_eq!(rc, SQLITE_OK);
        assert!(!db.is_null());
        // Full (non-storage-only) open runs the initializers.
        #[cfg(feature = "crsqlite")]
        assert!(has_table(db, "_rs_test_crsql_init"));
        #[cfg(feature = "murmur")]
        assert!(has_table(db, "_rs_test_murmur_attach"));
        #[cfg(not(any(feature = "crsqlite", feature = "murmur")))]
        {
            assert!(!has_table(db, "_rs_test_crsql_init"));
            assert!(!has_table(db, "_rs_test_murmur_attach"));
        }
        assert_eq!(sqlite_murmur_close(db), SQLITE_OK);
        assert_eq!(sqlite_murmur_close(ptr::null_mut()), SQLITE_OK);
    }

    #[test]
    fn storage_only_skips_initializers() {
        let opts = OpenOptions {
            flags: OPEN_STORAGE_ONLY,
            cipher: c"aes256gcm".as_ptr(),
            compression: c"zstd3".as_ptr(),
        };
        let mut db: *mut sqlite3 = ptr::null_mut();
        // Unknown PRAGMAs on the default VFS are silent no-ops, so the
        // cipher/compression options apply vacuously and succeed.
        let rc = open_inner(
            c":memory:".as_ptr(),
            &mut db,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
            Some(&opts),
            ptr::null(),
        );
        assert_eq!(rc, SQLITE_OK);
        #[cfg(feature = "crsqlite")]
        assert!(!has_table(db, "_rs_test_crsql_init"));
        #[cfg(feature = "murmur")]
        assert!(!has_table(db, "_rs_test_murmur_attach"));
        assert_eq!(sqlite_murmur_close(db), SQLITE_OK);
    }

    #[test]
    fn readonly_skips_initializers() {
        let root = scratch("readonly");
        let file = root.join("r.db");
        let file_c = CString::new(file.to_string_lossy().as_bytes()).unwrap();
        // Seed the file through a storage-only open (no initializer
        // markers) and materialize it with a write.
        let seed_opts = OpenOptions {
            flags: OPEN_STORAGE_ONLY,
            cipher: ptr::null(),
            compression: ptr::null(),
        };
        let mut db: *mut sqlite3 = ptr::null_mut();
        assert_eq!(
            open_inner(
                file_c.as_ptr(),
                &mut db,
                SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
                Some(&seed_opts),
                ptr::null(),
            ),
            SQLITE_OK
        );
        assert_eq!(
            unsafe {
                sqlite3_exec(
                    db,
                    c"CREATE TABLE seed(x)".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut(),
                )
            },
            SQLITE_OK
        );
        assert_eq!(sqlite_murmur_close(db), SQLITE_OK);
        // Read-only reopen of the populated file.
        let mut ro: *mut sqlite3 = ptr::null_mut();
        assert_eq!(
            open_inner(
                file_c.as_ptr(),
                &mut ro,
                SQLITE_OPEN_READONLY,
                None,
                ptr::null(),
            ),
            SQLITE_OK
        );
        #[cfg(feature = "crsqlite")]
        assert!(!has_table(ro, "_rs_test_crsql_init"));
        #[cfg(feature = "murmur")]
        assert!(!has_table(ro, "_rs_test_murmur_attach"));
        assert_eq!(sqlite_murmur_close(ro), SQLITE_OK);
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn open_failure_propagates() {
        // Missing parent without CREATE: SQLite reports CANTOPEN; the
        // handle result follows SQLite semantics (closed if produced).
        let mut db: *mut sqlite3 = ptr::null_mut();
        let rc = open_inner(
            c"/no/such/dir/x.db".as_ptr(),
            &mut db,
            SQLITE_OPEN_READWRITE,
            None,
            ptr::null(),
        );
        assert_ne!(rc, SQLITE_OK);
        if !db.is_null() {
            sqlite_murmur_close(db);
        }
    }
}
