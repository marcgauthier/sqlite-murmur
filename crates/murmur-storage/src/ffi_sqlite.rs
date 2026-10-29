//! SQLite C ABI subset for the VFS port, transcribed from
//! `third-party/sqlite/sqlite3.h`: result codes, open/lock/fcntl
//! constants, the `sqlite3_file` / `sqlite3_io_methods` / `sqlite3_vfs`
//! layouts, and the handful of functions the store calls. Symbol
//! resolution matches the api crate: the vendored amalgamation at final
//! link, the system SQLite under `cfg(test)`.

use std::ffi::{c_char, c_double, c_int, c_void};
#[cfg(test)]
use std::ffi::c_uchar;

pub const SQLITE_OK: c_int = 0;
pub const SQLITE_ERROR: c_int = 1;
pub const SQLITE_BUSY: c_int = 5;
pub const SQLITE_LOCKED: c_int = 6;
pub const SQLITE_NOMEM: c_int = 7;
pub const SQLITE_READONLY: c_int = 8;
pub const SQLITE_IOERR: c_int = 10;
pub const SQLITE_CORRUPT: c_int = 11;
pub const SQLITE_NOTFOUND: c_int = 12;
pub const SQLITE_FULL: c_int = 13;
pub const SQLITE_CANTOPEN: c_int = 14;
pub const SQLITE_MISUSE: c_int = 21;
pub const SQLITE_IOERR_READ: c_int = SQLITE_IOERR | (1 << 8);
pub const SQLITE_IOERR_SHORT_READ: c_int = SQLITE_IOERR | (2 << 8);
pub const SQLITE_IOERR_WRITE: c_int = SQLITE_IOERR | (3 << 8);
pub const SQLITE_IOERR_TRUNCATE: c_int = SQLITE_IOERR | (6 << 8);
pub const SQLITE_IOERR_DELETE: c_int = SQLITE_IOERR | (10 << 8);

pub const SQLITE_OPEN_READONLY: c_int = 0x0000_0001;
pub const SQLITE_OPEN_READWRITE: c_int = 0x0000_0002;
pub const SQLITE_OPEN_CREATE: c_int = 0x0000_0004;
pub const SQLITE_OPEN_DELETEONCLOSE: c_int = 0x0000_0008;
pub const SQLITE_OPEN_MAIN_DB: c_int = 0x0000_0100;
pub const SQLITE_OPEN_MAIN_JOURNAL: c_int = 0x0000_0800;
pub const SQLITE_OPEN_FULLMUTEX: c_int = 0x0001_0000;

pub const SQLITE_LOCK_NONE: c_int = 0;
pub const SQLITE_LOCK_SHARED: c_int = 1;
pub const SQLITE_LOCK_RESERVED: c_int = 2;
pub const SQLITE_LOCK_PENDING: c_int = 3;
pub const SQLITE_LOCK_EXCLUSIVE: c_int = 4;

pub const SQLITE_FCNTL_PRAGMA: c_int = 14;
pub const SQLITE_FCNTL_MMAP_SIZE: c_int = 18;
pub const SQLITE_FCNTL_COMMIT_PHASETWO: c_int = 22;

pub const SQLITE_ACCESS_EXISTS: c_int = 0;
pub const SQLITE_ACCESS_READWRITE: c_int = 1;
#[allow(dead_code)]
pub const SQLITE_ACCESS_READ: c_int = 2;

#[allow(non_camel_case_types)]
#[repr(C)]
pub struct sqlite3 {
    _private: [u8; 0],
}

#[cfg(test)]
#[allow(non_camel_case_types)]
#[repr(C)]
pub struct sqlite3_stmt {
    _private: [u8; 0],
}

#[cfg(test)]
pub const SQLITE_ROW: c_int = 100;
#[cfg(test)]
pub const SQLITE_DONE: c_int = 101;

#[allow(non_camel_case_types)]
#[repr(C)]
pub struct sqlite3_file {
    pub p_methods: *const sqlite3_io_methods,
}

#[allow(non_camel_case_types)]
#[repr(C)]
pub struct sqlite3_io_methods {
    pub i_version: c_int,
    pub x_close: Option<unsafe extern "C" fn(*mut sqlite3_file) -> c_int>,
    pub x_read: Option<unsafe extern "C" fn(*mut sqlite3_file, *mut c_void, c_int, i64) -> c_int>,
    pub x_write: Option<unsafe extern "C" fn(*mut sqlite3_file, *const c_void, c_int, i64) -> c_int>,
    pub x_truncate: Option<unsafe extern "C" fn(*mut sqlite3_file, i64) -> c_int>,
    pub x_sync: Option<unsafe extern "C" fn(*mut sqlite3_file, c_int) -> c_int>,
    pub x_file_size: Option<unsafe extern "C" fn(*mut sqlite3_file, *mut i64) -> c_int>,
    pub x_lock: Option<unsafe extern "C" fn(*mut sqlite3_file, c_int) -> c_int>,
    pub x_unlock: Option<unsafe extern "C" fn(*mut sqlite3_file, c_int) -> c_int>,
    pub x_check_reserved_lock: Option<unsafe extern "C" fn(*mut sqlite3_file, *mut c_int) -> c_int>,
    pub x_file_control: Option<unsafe extern "C" fn(*mut sqlite3_file, c_int, *mut c_void) -> c_int>,
    pub x_sector_size: Option<unsafe extern "C" fn(*mut sqlite3_file) -> c_int>,
    pub x_device_characteristics: Option<unsafe extern "C" fn(*mut sqlite3_file) -> c_int>,
    // v2 (SHM): unused, like the C++ (null).
    pub x_shm_map: Option<unsafe extern "C" fn(*mut sqlite3_file, c_int, c_int, c_int, *mut *mut c_void) -> c_int>,
    pub x_shm_lock: Option<unsafe extern "C" fn(*mut sqlite3_file, c_int, c_int, c_int) -> c_int>,
    pub x_shm_barrier: Option<unsafe extern "C" fn(*mut sqlite3_file)>,
    pub x_shm_unmap: Option<unsafe extern "C" fn(*mut sqlite3_file, c_int) -> c_int>,
    // v3 (mmap fetch): unused, like the C++ (null).
    pub x_fetch: Option<unsafe extern "C" fn(*mut sqlite3_file, i64, c_int, *mut *mut c_void) -> c_int>,
    pub x_unfetch: Option<unsafe extern "C" fn(*mut sqlite3_file, i64, *mut c_void) -> c_int>,
}

#[allow(non_camel_case_types)]
pub type sqlite3_syscall_ptr = Option<unsafe extern "C" fn()>;

#[allow(non_camel_case_types)]
#[repr(C)]
pub struct sqlite3_vfs {
    pub i_version: c_int,
    pub sz_os_file: c_int,
    pub mx_pathname: c_int,
    pub p_next: *mut sqlite3_vfs,
    pub z_name: *const c_char,
    pub p_app_data: *mut c_void,
    pub x_open: Option<unsafe extern "C" fn(*mut sqlite3_vfs, *const c_char, *mut sqlite3_file, c_int, *mut c_int) -> c_int>,
    pub x_delete: Option<unsafe extern "C" fn(*mut sqlite3_vfs, *const c_char, c_int) -> c_int>,
    pub x_access: Option<unsafe extern "C" fn(*mut sqlite3_vfs, *const c_char, c_int, *mut c_int) -> c_int>,
    pub x_full_pathname: Option<unsafe extern "C" fn(*mut sqlite3_vfs, *const c_char, c_int, *mut c_char) -> c_int>,
    pub x_dl_open: Option<unsafe extern "C" fn(*mut sqlite3_vfs, *const c_char) -> *mut c_void>,
    pub x_dl_error: Option<unsafe extern "C" fn(*mut sqlite3_vfs, c_int, *mut c_char)>,
    pub x_dl_sym: Option<unsafe extern "C" fn(*mut sqlite3_vfs, *mut c_void, *const c_char) -> sqlite3_syscall_ptr>,
    pub x_dl_close: Option<unsafe extern "C" fn(*mut sqlite3_vfs, *mut c_void)>,
    pub x_randomness: Option<unsafe extern "C" fn(*mut sqlite3_vfs, c_int, *mut c_char) -> c_int>,
    pub x_sleep: Option<unsafe extern "C" fn(*mut sqlite3_vfs, c_int) -> c_int>,
    pub x_current_time: Option<unsafe extern "C" fn(*mut sqlite3_vfs, *mut c_double) -> c_int>,
    pub x_get_last_error: Option<unsafe extern "C" fn(*mut sqlite3_vfs, c_int, *mut c_char) -> c_int>,
    // v2/v3: unused, like the C++ (null).
    pub x_current_time_int64: Option<unsafe extern "C" fn(*mut sqlite3_vfs, *mut i64) -> c_int>,
    pub x_set_system_call: Option<unsafe extern "C" fn(*mut sqlite3_vfs, *const c_char, sqlite3_syscall_ptr) -> c_int>,
    pub x_get_system_call: Option<unsafe extern "C" fn(*mut sqlite3_vfs, *const c_char) -> sqlite3_syscall_ptr>,
    pub x_next_system_call: Option<unsafe extern "C" fn(*mut sqlite3_vfs, *const c_char) -> *const c_char>,
}

unsafe extern "C" {
    pub(crate) unsafe fn sqlite3_vfs_find(vfs_name: *const c_char) -> *mut sqlite3_vfs;
    pub(crate) unsafe fn sqlite3_vfs_register(vfs: *mut sqlite3_vfs, make_dflt: c_int) -> c_int;
    pub(crate) unsafe fn sqlite3_vfs_unregister(vfs: *mut sqlite3_vfs) -> c_int;
    pub(crate) unsafe fn sqlite3_free(ptr: *mut c_void);
    pub(crate) unsafe fn sqlite3_malloc64(n: u64) -> *mut c_void;
    pub(crate) unsafe fn sqlite3_file_control(
        db: *mut sqlite3,
        db_name: *const c_char,
        op: c_int,
        arg: *mut c_void,
    ) -> c_int;
    #[cfg(test)]
    pub(crate) unsafe fn sqlite3_open_v2(
        filename: *const c_char,
        pp_db: *mut *mut sqlite3,
        flags: c_int,
        vfs: *const c_char,
    ) -> c_int;
    #[cfg(test)]
    pub(crate) unsafe fn sqlite3_close(db: *mut sqlite3) -> c_int;
    #[cfg(test)]
    pub(crate) unsafe fn sqlite3_exec(
        db: *mut sqlite3,
        sql: *const c_char,
        callback: Option<
            unsafe extern "C" fn(*mut c_void, c_int, *mut *mut c_char, *mut *mut c_char) -> c_int,
        >,
        arg: *mut c_void,
        errmsg: *mut *mut c_char,
    ) -> c_int;
    #[cfg(test)]
    pub(crate) unsafe fn sqlite3_prepare_v2(
        db: *mut sqlite3,
        sql: *const c_char,
        n: c_int,
        stmt: *mut *mut sqlite3_stmt,
        tail: *mut *const c_char,
    ) -> c_int;
    #[cfg(test)]
    pub(crate) unsafe fn sqlite3_step(stmt: *mut sqlite3_stmt) -> c_int;
    #[cfg(test)]
    pub(crate) unsafe fn sqlite3_column_text(stmt: *mut sqlite3_stmt, col: c_int) -> *const c_uchar;
    #[cfg(test)]
    pub(crate) unsafe fn sqlite3_finalize(stmt: *mut sqlite3_stmt) -> c_int;
    #[cfg(test)]
    pub(crate) unsafe fn sqlite3_errmsg(db: *mut sqlite3) -> *const c_char;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consts_match_sqlite3_h() {
        assert_eq!(SQLITE_IOERR_READ, 266);
        assert_eq!(SQLITE_IOERR_SHORT_READ, 522);
        assert_eq!(SQLITE_IOERR_WRITE, 778);
        assert_eq!(SQLITE_IOERR_TRUNCATE, 1546);
        assert_eq!(SQLITE_IOERR_DELETE, 2570);
        // Ground truth from a C probe over third-party/sqlite/sqlite3.h.
        assert_eq!(std::mem::size_of::<sqlite3_vfs>(), 168);
        assert_eq!(std::mem::size_of::<sqlite3_io_methods>(), 152);
        assert_eq!(std::mem::offset_of!(sqlite3_vfs, x_next_system_call), 160);
        assert_eq!(std::mem::offset_of!(sqlite3_io_methods, x_unfetch), 144);
        assert_eq!(std::mem::offset_of!(sqlite3_file, p_methods), 0);
    }

    #[test]
    fn smoke_malloc64_free() {
        // Linkage + allocator proof on the used surface (`malloc_string`
        // builds on these two).
        let out = unsafe { sqlite3_malloc64(6) } as *mut u8;
        assert!(!out.is_null());
        unsafe {
            std::ptr::copy_nonoverlapping(b"hi\0".as_ptr(), out, 3);
            let text = std::ffi::CStr::from_ptr(out as *const c_char)
                .to_string_lossy()
                .into_owned();
            sqlite3_free(out as *mut c_void);
            assert_eq!(text, "hi");
        }
    }
}
