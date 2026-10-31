//! murmur-admin: offline-only operational CLI (WP-D slice D3a).
//!
//! Rust port of `tools/murmur-admin/admin.cpp`. All commands refuse live
//! targets (fcntl probe on the store LOCK plus open as backstop) and never
//! open a live database read-write. Key material travels from `--key-hex`
//! into `sqlite_murmur_register` memory only; it is wiped after
//! registration and never printed, logged, or embedded in JSON output.
//!
//! The C ABI at the bottom mirrors the old C++ `admin.h` surface with
//! capture buffers (`char **` out/err) instead of injected ostreams; the
//! in-process C++ suite (`tools/murmur-admin/tests/admin.cpp`) drives it.
//! Provenance notes live in `tools/murmur-admin/PROVENANCE.md`.

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_uchar, c_void};

// ---------------------------------------------------------------------------
// FFI: SQLite C API subset (hand-written, no rusqlite — same discipline as
// murmur-storage). Resolved from the vendored amalgamation at final link.
// ---------------------------------------------------------------------------

mod ffi_sqlite {
    use super::*;

    pub enum Sqlite3 {}
    pub enum Sqlite3Stmt {}

    pub const SQLITE_OK: c_int = 0;
    pub const SQLITE_ERROR: c_int = 1;
    pub const SQLITE_BUSY: c_int = 5;
    pub const SQLITE_CANTOPEN: c_int = 14;
    pub const SQLITE_AUTH: c_int = 23;
    pub const SQLITE_ROW: c_int = 100;
    pub const SQLITE_DONE: c_int = 101;
    pub const SQLITE_NULL: c_int = 5;

    pub const SQLITE_OPEN_READONLY: c_int = 0x00000001;
    pub const SQLITE_OPEN_READWRITE: c_int = 0x00000002;
    pub const SQLITE_OPEN_FULLMUTEX: c_int = 0x00010000;

    /// SQLITE_TRANSIENT: tell bind to copy the value (-1 as a destructor).
    /// A function because const-transmute is not stable.
    pub fn sqlite_transient() -> Option<unsafe extern "C" fn(*mut c_void)> {
        Some(unsafe { std::mem::transmute::<isize, unsafe extern "C" fn(*mut c_void)>(-1) })
    }

    unsafe extern "C" {
        pub fn sqlite3_exec(
            db: *mut Sqlite3,
            sql: *const c_char,
            callback: Option<unsafe extern "C" fn()>,
            arg: *mut c_void,
            errmsg: *mut *mut c_char,
        ) -> c_int;
        pub fn sqlite3_prepare_v2(
            db: *mut Sqlite3,
            sql: *const c_char,
            nbyte: c_int,
            stmt: *mut *mut Sqlite3Stmt,
            tail: *mut *const c_char,
        ) -> c_int;
        pub fn sqlite3_step(stmt: *mut Sqlite3Stmt) -> c_int;
        pub fn sqlite3_finalize(stmt: *mut Sqlite3Stmt) -> c_int;
        pub fn sqlite3_column_text(stmt: *mut Sqlite3Stmt, i: c_int) -> *const c_uchar;
        pub fn sqlite3_column_blob(stmt: *mut Sqlite3Stmt, i: c_int) -> *const c_void;
        pub fn sqlite3_column_bytes(stmt: *mut Sqlite3Stmt, i: c_int) -> c_int;
        pub fn sqlite3_column_type(stmt: *mut Sqlite3Stmt, i: c_int) -> c_int;
        pub fn sqlite3_column_int64(stmt: *mut Sqlite3Stmt, i: c_int) -> i64;
        pub fn sqlite3_bind_text(
            stmt: *mut Sqlite3Stmt,
            i: c_int,
            text: *const c_char,
            n: c_int,
            destructor: Option<unsafe extern "C" fn(*mut c_void)>,
        ) -> c_int;
        pub fn sqlite3_bind_blob(
            stmt: *mut Sqlite3Stmt,
            i: c_int,
            data: *const c_void,
            n: c_int,
            destructor: Option<unsafe extern "C" fn(*mut c_void)>,
        ) -> c_int;
        pub fn sqlite3_errmsg(db: *mut Sqlite3) -> *const c_char;
        pub fn sqlite3_errstr(rc: c_int) -> *const c_char;
        pub fn sqlite3_free(p: *mut c_void);
        pub fn sqlite3_close(db: *mut Sqlite3) -> c_int;
    }
}

// ---------------------------------------------------------------------------
// FFI: stable murmur C API (implemented by murmur-storage, resolved at
// final link). Layouts mirror include/sqlite_murmur.h exactly.
// ---------------------------------------------------------------------------

mod ffi_murmur {
    use super::*;
    use crate::ffi_sqlite::Sqlite3;

    pub const SQLITE_MURMUR_OPEN_STORAGE_ONLY: u32 = 1;

    #[repr(C)]
    pub struct MurmurConfig {
        pub key: *const u8,
        pub flush_interval_ms: u32,
        pub cache_pages: usize,
        pub rocksdb_sync: c_int,
    }

    #[repr(C)]
    pub struct MurmurOpenOptions {
        pub flags: u32,
        pub cipher: *const c_char,
        pub compression: *const c_char,
    }

    unsafe extern "C" {
        pub fn sqlite_murmur_register(config: *const MurmurConfig) -> c_int;
        pub fn sqlite_murmur_open_v2(
            path: *const c_char,
            db: *mut *mut Sqlite3,
            sqlite_flags: c_int,
            options: *const MurmurOpenOptions,
        ) -> c_int;
        pub fn sqlite_murmur_close(db: *mut Sqlite3) -> c_int;
        pub fn sqlite_murmur_repair(path: *const c_char) -> c_int;
        pub fn sqlite_murmur_key_inventory(path: *const c_char, json_out: *mut *mut c_char) -> c_int;
        pub fn sqlite_murmur_rekey(
            path: *const c_char,
            old_master: *const u8,
            new_master: *const u8,
            opts: *const c_void,
        ) -> c_int;
    }
}

// ---------------------------------------------------------------------------
// FFI: fcntl LOCK probe (Unix). RocksDB holds LOCK via fcntl, so the probe
// takes a read lock through the same lock space (flock would miss it).
// ---------------------------------------------------------------------------

#[cfg(unix)]
mod ffi_lock {
    use super::*;

    pub const O_RDONLY: c_int = 0;
    pub const F_SETLK: c_int = 6;
    pub const F_RDLCK: i16 = 0;
    pub const SEEK_SET: c_int = 0;
    pub const EACCES: c_int = 13;
    pub const EAGAIN: c_int = 11;

    #[repr(C)]
    pub struct Flock {
        pub l_type: i16,
        pub l_whence: i16,
        pub l_start: i64,
        pub l_len: i64,
        pub l_pid: i32,
        // Trailing C padding is implicit in repr(C).
    }

    unsafe extern "C" {
        pub fn open(path: *const c_char, flags: c_int, ...) -> c_int;
        pub fn fcntl(fd: c_int, cmd: c_int, arg: *mut Flock) -> c_int;
        pub fn close(fd: c_int) -> c_int;
        pub fn __errno_location() -> *mut c_int;
        pub fn strerror(err: c_int) -> *const c_char;
    }
}

/// Panic boundary for `extern "C"` entries (mirrors murmur-storage).
fn guard<F, R>(f: F, fallback: R) -> R
where
    F: FnOnce() -> R,
{
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).unwrap_or(fallback)
}

const VERSION: &str = "0.1.0";

// ---------------------------------------------------------------------------
// Pure helpers (unit-tested; no FFI below this line until the Db layer).
// ---------------------------------------------------------------------------

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

fn hex_to_bytes(hex: &[u8], out: &mut [u8]) -> bool {
    if hex.len() != out.len() * 2 {
        return false;
    }
    for (i, slot) in out.iter_mut().enumerate() {
        let (Some(hi), Some(lo)) = (hex_val(hex[2 * i]), hex_val(hex[2 * i + 1])) else {
            return false;
        };
        *slot = (hi << 4) | lo;
    }
    true
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 0xf) as usize] as char);
    }
    out
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.bytes() {
        match c {
            b'"' => out.push_str("\\\""),
            b'\\' => out.push_str("\\\\"),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            _ => {
                if c < 0x20 {
                    out.push_str("\\u00");
                    out.push_str(&bytes_to_hex(&[c]));
                } else {
                    out.push(c as char);
                }
            }
        }
    }
    out
}

fn quote_ident(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 2);
    out.push('"');
    for c in name.chars() {
        if c == '"' {
            out.push_str("\"\"");
        } else {
            out.push(c);
        }
    }
    out.push('"');
    out
}

fn secret_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.contains("private_key")
        || lower.contains("secret")
        || lower.contains("password")
        || lower.contains("certificate")
}

/// Parsed key bytes. Wipe with [`wipe_key`] once registered.
pub struct AdminKey {
    pub bytes: [u8; 32],
    pub ok: bool,
}

/// Parse 64 hex chars into key bytes (mirrors `admin_parse_key_hex`).
pub fn parse_key_hex(hex: &[u8]) -> AdminKey {
    let mut key = AdminKey { bytes: [0u8; 32], ok: false };
    if hex_to_bytes(hex, &mut key.bytes) {
        key.ok = true;
    }
    key
}

/// Volatile wipe of key material (32 bytes; cold path, max paranoia).
pub fn wipe_key(key: &mut AdminKey) {
    for slot in key.bytes.iter_mut() {
        // SAFETY: volatile store to live stack memory.
        unsafe { std::ptr::write_volatile(slot, 0) };
    }
    std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
    key.ok = false;
}

pub fn parse_id16(hex: &[u8], out: &mut [u8; 16]) -> bool {
    hex_to_bytes(hex, out)
}

// Register the VFS key for this process. Mirrors `register_key_impl`:
// the VFS registers once per process; reuse is allowed only when the
// key matches the first registration.
static REGISTERED: std::sync::Mutex<Option<[u8; 32]>> =
    std::sync::Mutex::new(None);

/// Register `key_hex` (64 hex chars). Appends the failure line to `err`.
pub fn ensure_registered(key_hex: &str, err: &mut String) -> bool {
    let mut key = parse_key_hex(key_hex.as_bytes());
    if !key.ok {
        err.push_str("Error: --key-hex must be 64 hex characters (32 bytes)\n");
        return false;
    }
    let mut slot = REGISTERED.lock().expect("registered mutex poisoned");
    if let Some(first) = slot.as_ref() {
        let same = first == &key.bytes;
        wipe_key(&mut key);
        if !same {
            err.push_str("Error: a different store key is already registered\n");
            return false;
        }
        return true;
    }
    let cfg = ffi_murmur::MurmurConfig {
        key: key.bytes.as_ptr(),
        flush_interval_ms: 0, // default 250ms
        cache_pages: 0,       // default 16384
        rocksdb_sync: 1,      // maintenance tool: durable batches
    };
    // SAFETY: config borrows live key bytes; register copies 32 bytes.
    let rc = unsafe { ffi_murmur::sqlite_murmur_register(&cfg) };
    if rc == ffi_sqlite::SQLITE_OK {
        *slot = Some(key.bytes);
    }
    wipe_key(&mut key);
    if rc != ffi_sqlite::SQLITE_OK {
        err.push_str(&format!("Error: cannot register store key ({rc})\n"));
        return false;
    }
    true
}

fn random_bytes(out: &mut [u8]) -> bool {
    #[cfg(unix)]
    {
        use std::io::Read;
        let Ok(mut f) = std::fs::File::open("/dev/urandom") else {
            return false;
        };
        f.read_exact(out).is_ok()
    }
    #[cfg(not(unix))]
    {
        let _ = out;
        false
    }
}

/// Recursive size + file count (follows nothing; LOCK counted).
fn dir_usage(dir: &std::path::Path, bytes: &mut u64, files: &mut usize) {
    let mut stack = vec![dir.to_path_buf()];
    while let Some(top) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&top) else {
            break;
        };
        for entry in rd.flatten() {
            let Ok(ft) = entry.file_type() else {
                continue;
            };
            if ft.is_dir() {
                stack.push(entry.path());
            } else if ft.is_file() {
                *files += 1;
                *bytes += entry.metadata().map(|m| m.len()).unwrap_or(0);
            }
        }
    }
}

/// Recursive copy with 0600/0700 discipline; dest must not exist; any
/// failure removes dest. Returns 0 on success, 1 on failure (msg set).
/// Mirrors `admin_copy_tree` including message texts.
pub fn copy_tree(src: &std::path::Path, dest: &std::path::Path, msg: &mut String) -> c_int {
    if !src.is_dir() {
        *msg = format!("source is not a directory: {}", src.display());
        return 1;
    }
    if std::fs::symlink_metadata(dest).is_ok() {
        *msg = format!("destination already exists: {}", dest.display());
        return 1;
    }
    if let Err(e) = std::fs::create_dir_all(dest) {
        *msg = format!("cannot create destination: {}: {e}", dest.display());
        return 1;
    }
    let cleanup = || {
        let _ = std::fs::remove_dir_all(dest);
    };
    // Walk with explicit relative paths (mirrors recursive_directory_iterator).
    let mut stack = vec![src.to_path_buf()];
    while let Some(top) = stack.pop() {
        let rd = match std::fs::read_dir(&top) {
            Ok(rd) => rd,
            Err(e) => {
                *msg = format!("cannot scan source: {e}");
                cleanup();
                return 1;
            }
        };
        for entry in rd {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    *msg = format!("cannot scan source: {e}");
                    cleanup();
                    return 1;
                }
            };
            let rel = match entry.path().strip_prefix(src) {
                Ok(r) if !r.as_os_str().is_empty() => r.to_path_buf(),
                _ => {
                    *msg = "cannot relativize source entry".to_string();
                    cleanup();
                    return 1;
                }
            };
            let out = dest.join(&rel);
            let st = match std::fs::symlink_metadata(entry.path()) {
                Ok(s) => s,
                Err(e) => {
                    *msg = format!("cannot stat source entry: {e}");
                    cleanup();
                    return 1;
                }
            };
            if st.is_dir() {
                if let Err(e) = std::fs::create_dir_all(&out) {
                    *msg = format!("cannot create directory: {e}");
                    cleanup();
                    return 1;
                }
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let _ = std::fs::set_permissions(
                        &out,
                        std::fs::Permissions::from_mode(0o700),
                    );
                }
                stack.push(entry.path());
            } else if st.is_file() {
                if let Err(e) = std::fs::copy(entry.path(), &out) {
                    *msg = format!("cannot copy file: {e}");
                    cleanup();
                    return 1;
                }
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let _ = std::fs::set_permissions(
                        &out,
                        std::fs::Permissions::from_mode(0o600),
                    );
                }
            } else {
                *msg = format!(
                    "refusing non-regular source entry: {}",
                    entry.path().display()
                );
                cleanup();
                return 1;
            }
        }
    }
    0
}

/// True when another process holds the store lock (diagnostic probe).
/// Mirrors `admin_store_locked` including detail texts.
pub fn store_locked(dir: &std::path::Path, detail: &mut String) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let lock = dir.join("LOCK");
        let bytes = lock.as_os_str().as_bytes();
        let Ok(c) = CString::new(bytes) else {
            *detail = "LOCK probe failed: bad path".to_string();
            return false;
        };
        // SAFETY: NUL-terminated path; fcntl struct is a valid out-arg.
        unsafe {
            let fd = ffi_lock::open(c.as_ptr(), ffi_lock::O_RDONLY);
            if fd < 0 {
                *detail = "no LOCK file (not a live store or not yet created)".to_string();
                return false;
            }
            let mut fl = ffi_lock::Flock {
                l_type: ffi_lock::F_RDLCK,
                l_whence: ffi_lock::SEEK_SET as i16,
                l_start: 0,
                l_len: 0,
                l_pid: 0,
            };
            let rc = ffi_lock::fcntl(fd, ffi_lock::F_SETLK, &mut fl);
            let err = *ffi_lock::__errno_location();
            ffi_lock::close(fd);
            if rc == 0 {
                *detail = "LOCK is free".to_string();
                return false;
            }
            if err == ffi_lock::EACCES || err == ffi_lock::EAGAIN {
                *detail = "LOCK is held by a live process".to_string();
                return true;
            }
            let what = CStr::from_ptr(ffi_lock::strerror(err)).to_bytes();
            *detail = format!(
                "LOCK probe failed: {}",
                String::from_utf8_lossy(what)
            );
            return false;
        }
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        *detail = "LOCK probe unsupported on this platform (open is the backstop)".to_string();
        false
    }
}

/// Command options (mirrors `AdminOpts`).
#[derive(Default, Clone)]
pub struct Opts {
    pub json: bool,
    pub is_backup: bool,
    pub key_hex: String,
    pub key_file: String,
    pub new_key_hex: String,
    pub new_key_file: String,
    pub fresh_node: bool,
    pub fresh_db: bool,
    pub fresh_node_hex: String,
    pub fresh_db_hex: String,
}

/// Path as a NUL-terminated C string, truncating at the first interior
/// NUL exactly like C++ `c_str()` passthrough.
fn c_path(path: &std::path::Path) -> CString {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let bytes = path.as_os_str().as_bytes();
        let end = bytes.iter().position(|b| *b == 0).unwrap_or(bytes.len());
        CString::new(&bytes[..end]).expect("truncated path has no NUL")
    }
    #[cfg(not(unix))]
    {
        CString::new(path.to_string_lossy().as_bytes().to_vec())
            .unwrap_or_default()
    }
}

fn read_key_file(path: &str, hex_out: &mut String, err: &mut String) -> bool {
    let content = match std::fs::read(path) {
        Ok(c) => c,
        Err(_) => {
            err.push_str(&format!("Error: cannot open key file {path}\n"));
            return false;
        }
    };
    let mut end = content.len();
    while end > 0
        && matches!(content[end - 1], b'\n' | b'\r' | b' ' | b'\t')
    {
        end -= 1;
    }
    let content = &content[..end];
    if content.len() == 64 {
        *hex_out = String::from_utf8_lossy(content).into_owned();
        return true;
    }
    if content.len() == 32 {
        *hex_out = bytes_to_hex(content);
        return true;
    }
    err.push_str(&format!(
        "Error: key file {path} must contain 64 hex characters or 32 raw bytes\n"
    ));
    false
}

fn register_key(opts: &Opts, err: &mut String) -> bool {
    if opts.key_hex.is_empty() && !opts.key_file.is_empty() {
        let mut hex = String::new();
        if !read_key_file(&opts.key_file, &mut hex, err) {
            return false;
        }
        return ensure_registered(&hex, err);
    }
    ensure_registered(&opts.key_hex, err)
}

/// RAII handle closed with `sqlite_murmur_close` (mirrors `DbPtr`).
struct Db(*mut ffi_sqlite::Sqlite3);

impl Drop for Db {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: live handle from open; close once via RAII.
            unsafe {
                ffi_murmur::sqlite_murmur_close(self.0);
            }
        }
    }
}

fn open_readonly(dir: &std::path::Path, err: &mut String) -> Option<Db> {
    let c = c_path(dir);
    let mut raw: *mut ffi_sqlite::Sqlite3 = std::ptr::null_mut();
    // SAFETY: valid out-pointer; options NULL like the C++.
    let rc = unsafe {
        ffi_murmur::sqlite_murmur_open_v2(
            c.as_ptr(),
            &mut raw,
            ffi_sqlite::SQLITE_OPEN_READONLY | ffi_sqlite::SQLITE_OPEN_FULLMUTEX,
            std::ptr::null(),
        )
    };
    if rc != ffi_sqlite::SQLITE_OK || raw.is_null() {
        err.push_str(&format!(
            "Error: cannot open store read-only ({rc}); is it locked by a live process or not a database?\n"
        ));
        if !raw.is_null() {
            // SAFETY: failed-open handle; the C++ closes with sqlite3_close.
            unsafe {
                ffi_sqlite::sqlite3_close(raw);
            }
        }
        return None;
    }
    Some(Db(raw))
}

fn open_storage_rw(dir: &std::path::Path) -> Option<Db> {
    let c = c_path(dir);
    let mut raw: *mut ffi_sqlite::Sqlite3 = std::ptr::null_mut();
    let opts = ffi_murmur::MurmurOpenOptions {
        flags: ffi_murmur::SQLITE_MURMUR_OPEN_STORAGE_ONLY,
        cipher: std::ptr::null(),
        compression: std::ptr::null(),
    };
    // SAFETY: valid out-pointer and options.
    let rc = unsafe {
        ffi_murmur::sqlite_murmur_open_v2(
            c.as_ptr(),
            &mut raw,
            ffi_sqlite::SQLITE_OPEN_READWRITE | ffi_sqlite::SQLITE_OPEN_FULLMUTEX,
            &opts,
        )
    };
    if rc != ffi_sqlite::SQLITE_OK || raw.is_null() {
        if !raw.is_null() {
            // SAFETY: failed-open handle.
            unsafe {
                ffi_sqlite::sqlite3_close(raw);
            }
        }
        return None;
    }
    Some(Db(raw))
}

fn errmsg(db: *mut ffi_sqlite::Sqlite3) -> String {
    // SAFETY: sqlite3_errmsg never returns NULL.
    unsafe {
        CStr::from_ptr(ffi_sqlite::sqlite3_errmsg(db))
            .to_string_lossy()
            .into_owned()
    }
}

fn exec_all(db: *mut ffi_sqlite::Sqlite3, sql: &CStr, msg: &mut String) -> bool {
    let mut err: *mut c_char = std::ptr::null_mut();
    // SAFETY: valid handle/SQL; errmsg freed below.
    let rc = unsafe {
        ffi_sqlite::sqlite3_exec(db, sql.as_ptr(), None, std::ptr::null_mut(), &mut err)
    };
    if rc != ffi_sqlite::SQLITE_OK {
        if !err.is_null() {
            // SAFETY: sqlite-allocated message.
            unsafe {
                *msg = CStr::from_ptr(err).to_string_lossy().into_owned();
                ffi_sqlite::sqlite3_free(err as *mut c_void);
            }
        } else {
            // SAFETY: errstr never returns NULL.
            unsafe {
                *msg = CStr::from_ptr(ffi_sqlite::sqlite3_errstr(rc))
                    .to_string_lossy()
                    .into_owned();
            }
        }
        return false;
    }
    true
}

/// First column of the first row as text (empty when no rows).
fn query_text(db: *mut ffi_sqlite::Sqlite3, sql: &str, out: &mut String, msg: &mut String) -> bool {
    let c = CString::new(sql).expect("query has no NUL");
    let mut st: *mut ffi_sqlite::Sqlite3Stmt = std::ptr::null_mut();
    // SAFETY: valid handle/SQL; statement finalized below.
    unsafe {
        if ffi_sqlite::sqlite3_prepare_v2(db, c.as_ptr(), -1, &mut st, std::ptr::null_mut())
            != ffi_sqlite::SQLITE_OK
        {
            *msg = errmsg(db);
            return false;
        }
        let rc = ffi_sqlite::sqlite3_step(st);
        let mut ok = true;
        if rc == ffi_sqlite::SQLITE_ROW {
            let t = ffi_sqlite::sqlite3_column_text(st, 0);
            *out = if t.is_null() {
                String::new()
            } else {
                CStr::from_ptr(t as *const c_char).to_string_lossy().into_owned()
            };
        } else if rc != ffi_sqlite::SQLITE_DONE {
            *msg = errmsg(db);
            ok = false;
        }
        ffi_sqlite::sqlite3_finalize(st);
        ok
    }
}

fn query_blob(
    db: *mut ffi_sqlite::Sqlite3,
    sql: &str,
    out: &mut Vec<u8>,
    is_null: &mut bool,
    msg: &mut String,
) -> bool {
    let c = CString::new(sql).expect("query has no NUL");
    let mut st: *mut ffi_sqlite::Sqlite3Stmt = std::ptr::null_mut();
    // SAFETY: per query_text.
    unsafe {
        if ffi_sqlite::sqlite3_prepare_v2(db, c.as_ptr(), -1, &mut st, std::ptr::null_mut())
            != ffi_sqlite::SQLITE_OK
        {
            *msg = errmsg(db);
            return false;
        }
        let rc = ffi_sqlite::sqlite3_step(st);
        let mut ok = true;
        if rc == ffi_sqlite::SQLITE_ROW {
            if ffi_sqlite::sqlite3_column_type(st, 0) == ffi_sqlite::SQLITE_NULL {
                *is_null = true;
            } else {
                *is_null = false;
                let p = ffi_sqlite::sqlite3_column_blob(st, 0) as *const u8;
                let n = ffi_sqlite::sqlite3_column_bytes(st, 0);
                let n = if n > 0 { n as usize } else { 0 };
                out.clear();
                if n > 0 {
                    out.extend_from_slice(std::slice::from_raw_parts(p, n));
                }
            }
        } else if rc != ffi_sqlite::SQLITE_DONE {
            *msg = errmsg(db);
            ok = false;
        } else {
            *is_null = true;
        }
        ffi_sqlite::sqlite3_finalize(st);
        ok
    }
}

fn query_int(
    db: *mut ffi_sqlite::Sqlite3,
    sql: &str,
    out: &mut i64,
    has_row: &mut bool,
    msg: &mut String,
) -> bool {
    let c = CString::new(sql).expect("query has no NUL");
    let mut st: *mut ffi_sqlite::Sqlite3Stmt = std::ptr::null_mut();
    // SAFETY: per query_text.
    unsafe {
        if ffi_sqlite::sqlite3_prepare_v2(db, c.as_ptr(), -1, &mut st, std::ptr::null_mut())
            != ffi_sqlite::SQLITE_OK
        {
            *msg = errmsg(db);
            return false;
        }
        let rc = ffi_sqlite::sqlite3_step(st);
        let mut ok = true;
        if rc == ffi_sqlite::SQLITE_ROW {
            *has_row = true;
            *out = ffi_sqlite::sqlite3_column_int64(st, 0);
        } else if rc == ffi_sqlite::SQLITE_DONE {
            *has_row = false;
        } else {
            *msg = errmsg(db);
            ok = false;
        }
        ffi_sqlite::sqlite3_finalize(st);
        ok
    }
}

fn has_table(db: *mut ffi_sqlite::Sqlite3, name: &str) -> bool {
    let sql = c"SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1";
    let cname = CString::new(name).expect("table name has no NUL");
    let mut st: *mut ffi_sqlite::Sqlite3Stmt = std::ptr::null_mut();
    // SAFETY: per query_text; TRANSIENT copies the bound name.
    unsafe {
        if ffi_sqlite::sqlite3_prepare_v2(
            db,
            sql.as_ptr(),
            -1,
            &mut st,
            std::ptr::null_mut(),
        ) == ffi_sqlite::SQLITE_OK
        {
            ffi_sqlite::sqlite3_bind_text(st, 1, cname.as_ptr(), -1, ffi_sqlite::sqlite_transient());
            let found = ffi_sqlite::sqlite3_step(st) == ffi_sqlite::SQLITE_ROW;
            ffi_sqlite::sqlite3_finalize(st);
            return found;
        }
        ffi_sqlite::sqlite3_finalize(st);
        false
    }
}

fn refuse_if_locked(dir: &std::path::Path, err: &mut String) -> bool {
    let mut detail = String::new();
    if store_locked(dir, &mut detail) {
        err.push_str(&format!(
            "Error: refusing a live target: {} ({}). Stop the database first; this tool is offline-only.\n",
            dir.display(),
            detail
        ));
        return true;
    }
    false
}

/// Verify the D1 artifact manifest (ruling A): exact schema, supported
/// version, known cipher. Byte-level mirror of `check_manifest`.
fn check_manifest(
    dir: &std::path::Path,
    cipher: &mut Vec<u8>,
    group: &mut Vec<u8>,
    node: &mut Vec<u8>,
    seq: &mut Vec<u8>,
    msg: &mut String,
) -> bool {
    let body = match std::fs::read(dir.join("manifest.json")) {
        Ok(b) => b,
        Err(_) => {
            *msg = "manifest.json missing (not a murmur_backup artifact?)".to_string();
            return false;
        }
    };
    let mut keys = std::collections::BTreeSet::new();
    let mut i = 0usize;
    while i + 2 < body.len() {
        if body[i] != b'"' {
            i += 1;
            continue;
        }
        let Some(rel) = body[i + 1..].iter().position(|b| *b == b'"') else {
            break;
        };
        let end = i + 1 + rel;
        let mut j = end + 1;
        while j < body.len() && body[j] == b' ' {
            j += 1;
        }
        if j < body.len() && body[j] == b':' {
            keys.insert(body[i + 1..end].to_vec());
        }
        i = end + 1;
    }
    let want: std::collections::BTreeSet<Vec<u8>> = [
        b"format_version".as_slice(),
        b"cipher",
        b"group_id",
        b"node_id",
        b"rocksdb_seq",
        b"created_unix_ms",
    ]
    .into_iter()
    .map(|k| k.to_vec())
    .collect();
    if keys != want {
        *msg = "manifest.json has an unexpected schema".to_string();
        return false;
    }
    fn field(body: &[u8], name: &[u8], out: &mut Vec<u8>) -> bool {
        let mut key = Vec::with_capacity(name.len() + 4);
        key.push(b'"');
        key.extend_from_slice(name);
        key.extend_from_slice(b"\":\"");
        let Some(at) = find_sub(body, &key) else {
            return false;
        };
        let begin = at + key.len();
        let Some(rel) = body[begin..].iter().position(|b| *b == b'"') else {
            return false;
        };
        *out = body[begin..begin + rel].to_vec();
        true
    }
    fn number(body: &[u8], name: &[u8], out: &mut Vec<u8>) -> bool {
        let mut key = Vec::with_capacity(name.len() + 4);
        key.push(b'"');
        key.extend_from_slice(name);
        key.extend_from_slice(b"\":");
        let Some(at) = find_sub(body, &key) else {
            return false;
        };
        let mut end = at + key.len();
        while end < body.len() && body[end].is_ascii_digit() {
            end += 1;
        }
        if end == at + key.len() {
            return false;
        }
        *out = body[at + key.len()..end].to_vec();
        true
    }
    let mut version = Vec::new();
    if !number(&body, b"format_version", &mut version) || version != b"1" {
        *msg = "manifest.json has an unsupported format_version".to_string();
        return false;
    }
    if !field(&body, b"cipher", cipher) || (*cipher != b"aes256gcm" && *cipher != b"aegis256") {
        *msg = "manifest.json has an unknown cipher".to_string();
        return false;
    }
    if !field(&body, b"group_id", group)
        || !field(&body, b"node_id", node)
        || !number(&body, b"rocksdb_seq", seq)
    {
        *msg = "manifest.json is missing identity fields".to_string();
        return false;
    }
    true
}

fn find_sub(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || hay.len() < needle.len() {
        return None;
    }
    (0..=hay.len() - needle.len()).find(|i| &hay[*i..*i + needle.len()] == needle)
}

/// integrity_check must return exactly one row containing "ok".
fn check_integrity(db: *mut ffi_sqlite::Sqlite3, rows: &mut Vec<String>, msg: &mut String) -> bool {
    let sql = c"PRAGMA integrity_check";
    let mut st: *mut ffi_sqlite::Sqlite3Stmt = std::ptr::null_mut();
    // SAFETY: per query_text.
    unsafe {
        if ffi_sqlite::sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut st, std::ptr::null_mut())
            != ffi_sqlite::SQLITE_OK
        {
            *msg = errmsg(db);
            return false;
        }
        let mut ok = true;
        loop {
            let rc = ffi_sqlite::sqlite3_step(st);
            if rc == ffi_sqlite::SQLITE_ROW {
                let t = ffi_sqlite::sqlite3_column_text(st, 0);
                rows.push(if t.is_null() {
                    String::new()
                } else {
                    CStr::from_ptr(t as *const c_char).to_string_lossy().into_owned()
                });
            } else if rc == ffi_sqlite::SQLITE_DONE {
                break;
            } else {
                *msg = errmsg(db);
                ok = false;
                break;
            }
        }
        ffi_sqlite::sqlite3_finalize(st);
        ok
    }
}

struct VerifyResult {
    valid: bool,
    mode: String,
    database_id: Vec<u8>,
    node_id: Vec<u8>,
    cipher: Vec<u8>,
    seq: Vec<u8>,
    tables_checked: usize,
    errors: Vec<String>,
}

fn run_verify_core(
    target: &std::path::Path,
    backup_mode: bool,
    out: &mut VerifyResult,
    err: &mut String,
) -> bool {
    out.mode = if backup_mode { "backup" } else { "database" }.to_string();
    let Some(db) = open_readonly(target, err) else {
        out.errors.push("cannot open target read-only".to_string());
        return false;
    };
    let mut rows = Vec::new();
    let mut msg = String::new();
    if !check_integrity(db.0, &mut rows, &mut msg) {
        out.errors.push(format!("integrity_check failed: {msg}"));
        return false;
    }
    if rows.len() != 1 || rows[0] != "ok" {
        for r in &rows {
            out.errors.push(format!("integrity: {r}"));
        }
        return false;
    }
    let mut tables = 0i64;
    let mut has_row = false;
    if query_text_count(db.0, &mut tables, &mut has_row, &mut msg) && has_row {
        out.tables_checked = tables as usize;
    }
    if backup_mode {
        let mut cipher = Vec::new();
        let mut group = Vec::new();
        let mut node = Vec::new();
        let mut seq = Vec::new();
        if !check_manifest(target, &mut cipher, &mut group, &mut node, &mut seq, &mut msg) {
            out.errors.push(msg);
            return false;
        }
        out.cipher = cipher;
        out.database_id = group;
        out.node_id = node;
        out.seq = seq;
    } else if has_table(db.0, "murmur_identity") {
        let mut node = Vec::new();
        let mut is_null = true;
        if query_blob(
            db.0,
            "SELECT node FROM murmur_identity WHERE id=1",
            &mut node,
            &mut is_null,
            &mut msg,
        ) && !is_null
            && node.len() == 16
        {
            out.node_id = bytes_to_hex(&node).into_bytes();
        }
    }
    out.valid = true;
    true
}

fn query_text_count(
    db: *mut ffi_sqlite::Sqlite3,
    tables: &mut i64,
    has_row: &mut bool,
    msg: &mut String,
) -> bool {
    query_int(
        db,
        "SELECT COUNT(*) FROM sqlite_schema WHERE type='table'",
        tables,
        has_row,
        msg,
    )
}

fn print_verify(r: &VerifyResult, target: &str, json: bool, out: &mut String) {
    if json {
        out.push_str(&format!(
            "{{\"target\":\"{}\",\"valid\":{},\"mode\":\"{}\"",
            json_escape(target),
            if r.valid { "true" } else { "false" },
            r.mode
        ));
        if !r.database_id.is_empty() {
            out.push_str(&format!(
                ",\"database_id\":\"{}\"",
                String::from_utf8_lossy(&r.database_id)
            ));
        }
        if !r.node_id.is_empty() {
            out.push_str(&format!(",\"node_id\":\"{}\"", String::from_utf8_lossy(&r.node_id)));
        }
        if !r.cipher.is_empty() {
            out.push_str(&format!(",\"cipher\":\"{}\"", String::from_utf8_lossy(&r.cipher)));
        }
        if !r.seq.is_empty() {
            out.push_str(&format!(",\"rocksdb_seq\":{}", String::from_utf8_lossy(&r.seq)));
        }
        out.push_str(&format!(",\"tables_checked\":{},\"errors\":[", r.tables_checked));
        for (i, e) in r.errors.iter().enumerate() {
            out.push_str(&format!("{}\"{}\"", if i > 0 { "," } else { "" }, json_escape(e)));
        }
        out.push_str("]}\n");
        return;
    }
    if r.valid {
        out.push_str(&format!(
            "PASS: {} storage integrity verified at {target}\n",
            if r.mode == "backup" { "Backup" } else { "Database" }
        ));
        if !r.node_id.is_empty() {
            out.push_str(&format!("Node ID:         {}\n", String::from_utf8_lossy(&r.node_id)));
        }
        if !r.database_id.is_empty() {
            out.push_str(&format!(
                "Database ID:     {}\n",
                String::from_utf8_lossy(&r.database_id)
            ));
        }
        if !r.cipher.is_empty() {
            out.push_str(&format!("Cipher:          {}\n", String::from_utf8_lossy(&r.cipher)));
        }
        out.push_str(&format!("Tables Checked:  {}\n", r.tables_checked));
    } else {
        out.push_str(&format!("FAIL: verification failed at {target}:\n"));
        for e in &r.errors {
            out.push_str(&format!("  - {e}\n"));
        }
    }
}

/// Minimal extractors over the F3 inventory schema (single-sourced with
/// murmur_key_inventory(); the schema is pinned by the admin suite).
/// Missing/unparseable fields yield empty; callers omit those lines.
fn inv_string(body: &[u8], key: &[u8]) -> Vec<u8> {
    let mut needle = Vec::with_capacity(key.len() + 4);
    needle.push(b'"');
    needle.extend_from_slice(key);
    needle.extend_from_slice(b"\":\"");
    let Some(at) = find_sub(body, &needle) else {
        return Vec::new();
    };
    let begin = at + needle.len();
    let Some(rel) = body[begin..].iter().position(|b| *b == b'"') else {
        return Vec::new();
    };
    body[begin..begin + rel].to_vec()
}

fn inv_number(body: &[u8], key: &[u8]) -> Vec<u8> {
    let mut needle = Vec::with_capacity(key.len() + 4);
    needle.push(b'"');
    needle.extend_from_slice(key);
    needle.extend_from_slice(b"\":");
    let Some(at) = find_sub(body, &needle) else {
        return Vec::new();
    };
    let begin = at + needle.len();
    let mut end = begin;
    while end < body.len() && (body[end].is_ascii_digit() || body[end] == b'-') {
        end += 1;
    }
    if end == begin {
        return Vec::new();
    }
    body[begin..end].to_vec()
}

fn inv_key_ids(body: &[u8]) -> Vec<Vec<u8>> {
    let mut ids = Vec::new();
    let mut from = 0usize;
    loop {
        let Some(rel) = find_sub(&body[from..], b"\"id\":") else {
            break;
        };
        let begin = from + rel + 5;
        let mut end = begin;
        while end < body.len() && body[end].is_ascii_digit() {
            end += 1;
        }
        if end == begin {
            break;
        }
        ids.push(body[begin..end].to_vec());
        from = end;
    }
    ids
}

fn print_keys(body: &[u8], dir: &str, out: &mut String) {
    if find_sub(body, b"\"sealed\":true").is_none() {
        out.push_str(&format!("No key envelope at {dir} (legacy unsealed store)\n"));
        return;
    }
    out.push_str(&format!("Key envelope at {dir}:\n"));
    out.push_str("Sealed:   yes\n");
    let kdf = inv_string(body, b"kdf");
    if !kdf.is_empty() {
        out.push_str(&format!("KDF:      {}\n", String::from_utf8_lossy(&kdf)));
    }
    let seq = inv_number(body, b"seq");
    if !seq.is_empty() {
        out.push_str(&format!("Seq:      {}\n", String::from_utf8_lossy(&seq)));
    }
    let current = inv_number(body, b"current");
    if !current.is_empty() {
        out.push_str(&format!("Current:  {}\n", String::from_utf8_lossy(&current)));
    }
    let ids = inv_key_ids(body);
    if !ids.is_empty() {
        out.push_str("Keys:     ");
        for (i, id) in ids.iter().enumerate() {
            out.push_str(&format!(
                "{}{}",
                if i > 0 { ", " } else { "" },
                String::from_utf8_lossy(id)
            ));
        }
        out.push('\n');
    }
}

/// Identity rewrite on the destination COPY (temp STORAGE_ONLY handle).
/// Approved procedure (lead D3a ruling, codex-corrected): single-transaction
/// MOVE of ordinal 0 -> N, remap of every __crsql_clock site_id 0 -> N,
/// install of the new UUID at ordinal 0, murmur_identity update, commit.
/// Clocks are the only ordinal-bearing metadata (tracked_peers uses UUID
/// blobs; the changes vtab exposes resolved UUIDs).
fn rewrite_identity(
    db: *mut ffi_sqlite::Sqlite3,
    new_node: Option<&[u8; 16]>,
    new_group_hex: &str,
    msg: &mut String,
) -> bool {
    let move_site = new_node.is_some();
    let move_group = !new_group_hex.is_empty();
    let begin = c"BEGIN IMMEDIATE";
    if !exec_all(db, begin, msg) {
        return false;
    }
    let mut ok = false;
    loop {
        if move_site && has_table(db, "crsql_site_id") {
            let node = new_node.expect("move_site implies new_node");
            let mut old = Vec::new();
            let mut is_null = true;
            if !query_blob(
                db,
                "SELECT site_id FROM crsql_site_id WHERE ordinal=0",
                &mut old,
                &mut is_null,
                msg,
            ) || is_null
                || old.len() != 16
            {
                *msg = "no ordinal-0 site to rehome".to_string();
                break;
            }
            // Precheck: fresh UUID must be absent.
            let mut precheck = false;
            {
                let sql = c"SELECT COUNT(*) FROM crsql_site_id WHERE site_id=?1";
                let mut st: *mut ffi_sqlite::Sqlite3Stmt = std::ptr::null_mut();
                // SAFETY: per query_text.
                unsafe {
                    if ffi_sqlite::sqlite3_prepare_v2(
                        db,
                        sql.as_ptr(),
                        -1,
                        &mut st,
                        std::ptr::null_mut(),
                    ) != ffi_sqlite::SQLITE_OK
                    {
                        *msg = errmsg(db);
                        break;
                    }
                    ffi_sqlite::sqlite3_bind_blob(
                        st,
                        1,
                        node.as_ptr() as *const c_void,
                        16,
                        ffi_sqlite::sqlite_transient(),
                    );
                    if ffi_sqlite::sqlite3_step(st) == ffi_sqlite::SQLITE_ROW {
                        precheck = ffi_sqlite::sqlite3_column_int64(st, 0) == 0;
                    }
                    ffi_sqlite::sqlite3_finalize(st);
                }
            }
            if !precheck {
                *msg = "fresh site UUID already present".to_string();
                break;
            }
            let mut mx = 0i64;
            let mut has_row = false;
            if !query_int(db, "SELECT max(ordinal) FROM crsql_site_id", &mut mx, &mut has_row, msg)
                || !has_row
            {
                *msg = "cannot allocate site ordinal".to_string();
                break;
            }
            let fresh = mx + 1;
            let mv = CString::new(format!("UPDATE crsql_site_id SET ordinal={fresh} WHERE ordinal=0"))
                .expect("ordinal has no NUL");
            if !exec_all(db, &mv, msg) {
                break;
            }
            // Remap every clock table (suffix match; LIKE '_' would
            // overmatch, and identifiers are quoted defensively).
            let mut remap_ok = true;
            {
                let sql = c"SELECT name FROM sqlite_schema WHERE type='table'";
                let mut tables: *mut ffi_sqlite::Sqlite3Stmt = std::ptr::null_mut();
                // SAFETY: per query_text.
                unsafe {
                    if ffi_sqlite::sqlite3_prepare_v2(
                        db,
                        sql.as_ptr(),
                        -1,
                        &mut tables,
                        std::ptr::null_mut(),
                    ) != ffi_sqlite::SQLITE_OK
                    {
                        *msg = errmsg(db);
                        break;
                    }
                    let suffix = "__crsql_clock";
                    loop {
                        if ffi_sqlite::sqlite3_step(tables) != ffi_sqlite::SQLITE_ROW {
                            break;
                        }
                        let t = ffi_sqlite::sqlite3_column_text(tables, 0);
                        let name = if t.is_null() {
                            String::new()
                        } else {
                            CStr::from_ptr(t as *const c_char).to_string_lossy().into_owned()
                        };
                        if !name.ends_with(suffix) {
                            continue;
                        }
                        let upd = CString::new(format!(
                            "UPDATE {} SET site_id={fresh} WHERE site_id=0",
                            quote_ident(&name)
                        ))
                        .expect("identifier has no NUL");
                        if !exec_all(db, &upd, msg) {
                            remap_ok = false;
                            break;
                        }
                    }
                    ffi_sqlite::sqlite3_finalize(tables);
                }
            }
            if !remap_ok {
                break;
            }
            {
                let sql = c"INSERT INTO crsql_site_id(site_id, ordinal) VALUES(?1, 0)";
                let mut ins: *mut ffi_sqlite::Sqlite3Stmt = std::ptr::null_mut();
                // SAFETY: per query_text.
                unsafe {
                    if ffi_sqlite::sqlite3_prepare_v2(
                        db,
                        sql.as_ptr(),
                        -1,
                        &mut ins,
                        std::ptr::null_mut(),
                    ) != ffi_sqlite::SQLITE_OK
                    {
                        *msg = errmsg(db);
                        break;
                    }
                    ffi_sqlite::sqlite3_bind_blob(
                        ins,
                        1,
                        node.as_ptr() as *const c_void,
                        16,
                        ffi_sqlite::sqlite_transient(),
                    );
                    let irc = ffi_sqlite::sqlite3_step(ins);
                    ffi_sqlite::sqlite3_finalize(ins);
                    if irc != ffi_sqlite::SQLITE_DONE {
                        *msg = errmsg(db);
                        break;
                    }
                }
            }
        }
        let create_ident = c"CREATE TABLE IF NOT EXISTS murmur_identity(id INTEGER PRIMARY KEY CHECK(id=1),node BLOB,site BLOB,incarnation INTEGER NOT NULL DEFAULT 0)";
        if !exec_all(db, create_ident, msg) {
            break;
        }
        let seed_ident = c"INSERT OR IGNORE INTO murmur_identity(id) VALUES(1)";
        if !exec_all(db, seed_ident, msg) {
            break;
        }
        if move_site {
            let node = new_node.expect("move_site implies new_node");
            let has_site_table = has_table(db, "crsql_site_id");
            let sql = if has_site_table {
                c"UPDATE murmur_identity SET node=?1,site=?1,incarnation=incarnation+1 WHERE id=1"
            } else {
                c"UPDATE murmur_identity SET node=?1,site=NULL,incarnation=incarnation+1 WHERE id=1"
            };
            let mut up: *mut ffi_sqlite::Sqlite3Stmt = std::ptr::null_mut();
            // SAFETY: per query_text.
            unsafe {
                if ffi_sqlite::sqlite3_prepare_v2(
                    db,
                    sql.as_ptr(),
                    -1,
                    &mut up,
                    std::ptr::null_mut(),
                ) != ffi_sqlite::SQLITE_OK
                {
                    *msg = errmsg(db);
                    break;
                }
                ffi_sqlite::sqlite3_bind_blob(
                    up,
                    1,
                    node.as_ptr() as *const c_void,
                    16,
                    ffi_sqlite::sqlite_transient(),
                );
                let urc = ffi_sqlite::sqlite3_step(up);
                ffi_sqlite::sqlite3_finalize(up);
                if urc != ffi_sqlite::SQLITE_DONE {
                    *msg = errmsg(db);
                    break;
                }
            }
        }
        if move_group {
            let create_cfg = c"CREATE TABLE IF NOT EXISTS murmur_config(name TEXT PRIMARY KEY,value TEXT NOT NULL)";
            if !exec_all(db, create_cfg, msg) {
                break;
            }
            let sql = c"INSERT INTO murmur_config(name,value) VALUES('group_id',?1) ON CONFLICT(name) DO UPDATE SET value=excluded.value";
            let cgroup = CString::new(new_group_hex).expect("group hex has no NUL");
            let mut up: *mut ffi_sqlite::Sqlite3Stmt = std::ptr::null_mut();
            // SAFETY: per query_text.
            unsafe {
                if ffi_sqlite::sqlite3_prepare_v2(
                    db,
                    sql.as_ptr(),
                    -1,
                    &mut up,
                    std::ptr::null_mut(),
                ) != ffi_sqlite::SQLITE_OK
                {
                    *msg = errmsg(db);
                    break;
                }
                ffi_sqlite::sqlite3_bind_text(
                    up,
                    1,
                    cgroup.as_ptr(),
                    -1,
                    ffi_sqlite::sqlite_transient(),
                );
                let urc = ffi_sqlite::sqlite3_step(up);
                ffi_sqlite::sqlite3_finalize(up);
                if urc != ffi_sqlite::SQLITE_DONE {
                    *msg = errmsg(db);
                    break;
                }
            }
        }
        ok = true;
        break;
    }
    let end = if ok { c"COMMIT" } else { c"ROLLBACK" };
    if !exec_all(db, end, msg) {
        return false;
    }
    ok
}

fn resolve_fresh_id(hex: &str, flag: &str, out: &mut [u8; 16], err: &mut String) -> bool {
    if !hex.is_empty() {
        if !parse_id16(hex.as_bytes(), out) {
            err.push_str(&format!("Error: {flag} must be 32 hex characters\n"));
            return false;
        }
        return true;
    }
    for _ in 0..4 {
        if !random_bytes(out) {
            err.push_str("Error: cannot draw random identity\n");
            return false;
        }
        if out.iter().any(|b| *b != 0) {
            return true;
        }
    }
    err.push_str("Error: cannot draw non-zero random identity\n");
    false
}

fn print_restore(
    backup: &str,
    dest: &str,
    new_node: &str,
    new_group: &str,
    json: bool,
    out: &mut String,
) {
    if json {
        out.push_str(&format!(
            "{{\"backup\":\"{}\",\"destination\":\"{}\",\"success\":true",
            json_escape(backup),
            json_escape(dest)
        ));
        if !new_node.is_empty() {
            out.push_str(&format!(",\"new_node_id\":\"{new_node}\""));
        }
        if !new_group.is_empty() {
            out.push_str(&format!(",\"new_database_id\":\"{new_group}\""));
        }
        out.push_str("}\n");
        return;
    }
    out.push_str("PASS: Database restored successfully.\n");
    out.push_str(&format!("Backup Source:    {backup}\n"));
    out.push_str(&format!("Destination:      {dest}\n"));
    if !new_node.is_empty() {
        out.push_str(&format!("New Node ID:      {new_node}\n"));
    }
    if !new_group.is_empty() {
        out.push_str(&format!("New Database ID:  {new_group}\n"));
    }
}

/// Each command returns a process exit code (0 ok, 1 failure) and appends
/// human or --json output to `out`. Mirrors `admin_cmd_*` exactly.
pub fn cmd_verify(target: &std::path::Path, opts: &Opts, out: &mut String, err: &mut String) -> c_int {
    if refuse_if_locked(target, err) {
        return 1;
    }
    if !register_key(opts, err) {
        return 1;
    }
    let mut r = VerifyResult {
        valid: false,
        mode: String::new(),
        database_id: Vec::new(),
        node_id: Vec::new(),
        cipher: Vec::new(),
        seq: Vec::new(),
        tables_checked: 0,
        errors: Vec::new(),
    };
    run_verify_core(target, opts.is_backup, &mut r, err);
    print_verify(&r, &target.to_string_lossy(), opts.json, out);
    if r.valid { 0 } else { 1 }
}

pub fn cmd_restore(
    backup: &std::path::Path,
    dest: &std::path::Path,
    opts: &Opts,
    out: &mut String,
    err: &mut String,
) -> c_int {
    if refuse_if_locked(backup, err) {
        return 1;
    }
    let mut new_node = [0u8; 16];
    let mut new_group = [0u8; 16];
    if opts.fresh_node && !resolve_fresh_id(&opts.fresh_node_hex, "--fresh-node-id", &mut new_node, err) {
        return 1;
    }
    if opts.fresh_db && !resolve_fresh_id(&opts.fresh_db_hex, "--fresh-db-id", &mut new_group, err) {
        return 1;
    }
    if !register_key(opts, err) {
        return 1;
    }
    let mut msg = String::new();
    if copy_tree(backup, dest, &mut msg) != 0 {
        err.push_str(&format!("Error: restore copy failed: {msg}\n"));
        return 1;
    }
    let fail_remove = |why: &str, err: &mut String| {
        let _ = std::fs::remove_dir_all(dest);
        err.push_str(&format!("Error: restore failed: {why}\n"));
        1
    };
    let mut new_node_hex = String::new();
    let mut new_group_hex = String::new();
    if opts.fresh_node || opts.fresh_db {
        let Some(db) = open_storage_rw(dest) else {
            return fail_remove("cannot open destination copy", err);
        };
        new_node_hex = if opts.fresh_node { bytes_to_hex(&new_node) } else { String::new() };
        new_group_hex = if opts.fresh_db { bytes_to_hex(&new_group) } else { String::new() };
        let node_opt = if opts.fresh_node { Some(&new_node) } else { None };
        if !rewrite_identity(db.0, node_opt, &new_group_hex, &mut msg) {
            let why = if msg.is_empty() { "identity rewrite failed".to_string() } else { std::mem::take(&mut msg) };
            drop(db);
            return fail_remove(&why, err);
        }
        drop(db);
    }
    let mut r = VerifyResult {
        valid: false,
        mode: String::new(),
        database_id: Vec::new(),
        node_id: Vec::new(),
        cipher: Vec::new(),
        seq: Vec::new(),
        tables_checked: 0,
        errors: Vec::new(),
    };
    let mut quiet = String::new();
    if !run_verify_core(dest, true, &mut r, &mut quiet) || !r.valid {
        let mut why = std::mem::take(&mut msg);
        if let Some(first) = r.errors.first() {
            why = first.clone();
        }
        if why.is_empty() {
            why = "restored copy failed verification".to_string();
        }
        return fail_remove(&why, err);
    }
    print_restore(
        &backup.to_string_lossy(),
        &dest.to_string_lossy(),
        &new_node_hex,
        &new_group_hex,
        opts.json,
        out,
    );
    0
}
pub fn cmd_status(dir: &std::path::Path, opts: &Opts, out: &mut String, err: &mut String) -> c_int {
    if refuse_if_locked(dir, err) {
        return 1;
    }
    if !register_key(opts, err) {
        return 1;
    }
    let Some(db) = open_readonly(dir, err) else {
        return 1;
    };
    let mut msg = String::new();
    let mut node = String::new();
    let mut site = String::new();
    let mut group = String::new();
    let mut cipher = "unknown".to_string();
    let mut incarnation = 0i64;
    let mut tables = 0i64;
    let mut has_row = false;
    if has_table(db.0, "murmur_identity") {
        let mut blob = Vec::new();
        let mut is_null = true;
        if query_blob(db.0, "SELECT node FROM murmur_identity WHERE id=1", &mut blob, &mut is_null, &mut msg)
            && !is_null
            && blob.len() == 16
        {
            node = bytes_to_hex(&blob);
        }
        if query_blob(db.0, "SELECT site FROM murmur_identity WHERE id=1", &mut blob, &mut is_null, &mut msg)
            && !is_null
            && blob.len() == 16
        {
            site = bytes_to_hex(&blob);
        }
        let mut inc = 0i64;
        if query_int(db.0, "SELECT incarnation FROM murmur_identity WHERE id=1", &mut inc, &mut has_row, &mut msg)
            && has_row
        {
            incarnation = inc;
        }
    }
    if has_table(db.0, "murmur_config") {
        let mut value = String::new();
        if query_text(db.0, "SELECT value FROM murmur_config WHERE name='group_id'", &mut value, &mut msg) {
            group = value;
        }
    }
    {
        let mut value = String::new();
        if query_text(db.0, "PRAGMA cipher", &mut value, &mut msg) && !value.is_empty() {
            cipher = value;
        }
    }
    if query_int(
        db.0,
        "SELECT COUNT(*) FROM sqlite_schema WHERE type='table'",
        &mut tables,
        &mut has_row,
        &mut msg,
    ) && !has_row
    {
        tables = 0;
    }
    let mut disk_bytes = 0u64;
    let mut files = 0usize;
    dir_usage(dir, &mut disk_bytes, &mut files);
    if opts.json {
        out.push_str(&format!("{{\"data_dir\":\"{}\"", json_escape(&dir.to_string_lossy())));
        if !node.is_empty() {
            out.push_str(&format!(",\"node_id\":\"{node}\""));
        }
        if !site.is_empty() {
            out.push_str(&format!(",\"site_id\":\"{site}\""));
        }
        if !group.is_empty() {
            out.push_str(&format!(",\"group_id\":\"{}\"", json_escape(&group)));
        }
        out.push_str(&format!(
            ",\"incarnation\":{incarnation},\"cipher\":\"{}\",\"tables\":{tables},\"disk_bytes\":{disk_bytes},\"files\":{files}}}\n",
            json_escape(&cipher)
        ));
        return 0;
    }
    out.push_str("=== Murmur Database Status ===\n");
    out.push_str(&format!("Data Directory:      {}\n", dir.display()));
    if !node.is_empty() {
        out.push_str(&format!("Node ID:             {node}\n"));
    }
    if !site.is_empty() {
        out.push_str(&format!("Site ID:             {site}\n"));
    }
    if !group.is_empty() {
        out.push_str(&format!("Group ID:            {group}\n"));
    }
    out.push_str(&format!("Incarnation:         {incarnation}\n"));
    out.push_str(&format!("Cipher:              {cipher}\n"));
    out.push_str(&format!("Tables:              {tables}\n"));
    out.push_str(&format!("Files On Disk:       {files} files ({disk_bytes} bytes)\n"));
    0
}

pub fn cmd_inspect(dir: &std::path::Path, opts: &Opts, out: &mut String, err: &mut String) -> c_int {
    if refuse_if_locked(dir, err) {
        return 1;
    }
    if !register_key(opts, err) {
        return 1;
    }
    let Some(db) = open_readonly(dir, err) else {
        return 1;
    };
    struct Table {
        name: String,
        columns: i64,
        crr: bool,
    }
    let mut tables: Vec<Table> = Vec::new();
    {
        let sql = c"SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name";
        let mut st: *mut ffi_sqlite::Sqlite3Stmt = std::ptr::null_mut();
        // SAFETY: per query_text.
        unsafe {
            if ffi_sqlite::sqlite3_prepare_v2(db.0, sql.as_ptr(), -1, &mut st, std::ptr::null_mut())
                == ffi_sqlite::SQLITE_OK
            {
                while ffi_sqlite::sqlite3_step(st) == ffi_sqlite::SQLITE_ROW {
                    let t = ffi_sqlite::sqlite3_column_text(st, 0);
                    tables.push(Table {
                        name: if t.is_null() {
                            String::new()
                        } else {
                            CStr::from_ptr(t as *const c_char).to_string_lossy().into_owned()
                        },
                        columns: 0,
                        crr: false,
                    });
                }
            }
            ffi_sqlite::sqlite3_finalize(st);
        }
    }
    for t in &mut tables {
        let mut msg = String::new();
        let mut ncols = 0i64;
        let mut has_row = false;
        if query_int(
            db.0,
            &format!("SELECT COUNT(*) FROM pragma_table_info({})", quote_ident(&t.name)),
            &mut ncols,
            &mut has_row,
            &mut msg,
        ) && has_row
        {
            t.columns = ncols;
        }
        t.crr = has_table(db.0, &(t.name.clone() + "__crsql_clock"));
    }
    let mut config: Vec<(String, String)> = Vec::new();
    if has_table(db.0, "murmur_config") {
        let sql = c"SELECT name,value FROM murmur_config ORDER BY name";
        let mut st: *mut ffi_sqlite::Sqlite3Stmt = std::ptr::null_mut();
        // SAFETY: per query_text.
        unsafe {
            if ffi_sqlite::sqlite3_prepare_v2(db.0, sql.as_ptr(), -1, &mut st, std::ptr::null_mut())
                == ffi_sqlite::SQLITE_OK
            {
                while ffi_sqlite::sqlite3_step(st) == ffi_sqlite::SQLITE_ROW {
                    let n = ffi_sqlite::sqlite3_column_text(st, 0);
                    let v = ffi_sqlite::sqlite3_column_text(st, 1);
                    let name = if n.is_null() {
                        String::new()
                    } else {
                        CStr::from_ptr(n as *const c_char).to_string_lossy().into_owned()
                    };
                    let mut value = if v.is_null() {
                        String::new()
                    } else {
                        CStr::from_ptr(v as *const c_char).to_string_lossy().into_owned()
                    };
                    // Never emit key material: config rows can carry private
                    // keys and certificates. Fail closed on any secret-like
                    // name rather than maintaining an allowlist.
                    if secret_name(&name) {
                        value = "<redacted>".to_string();
                    }
                    config.push((name, value));
                }
            }
            ffi_sqlite::sqlite3_finalize(st);
        }
    }
    let mut disk_bytes = 0u64;
    let mut files = 0usize;
    dir_usage(dir, &mut disk_bytes, &mut files);
    if opts.json {
        out.push_str(&format!("{{\"data_dir\":\"{}\",\"tables\":[", json_escape(&dir.to_string_lossy())));
        for (i, t) in tables.iter().enumerate() {
            out.push_str(&format!(
                "{}{{\"name\":\"{}\",\"columns\":{},\"crr\":{}}}",
                if i > 0 { "," } else { "" },
                json_escape(&t.name),
                t.columns,
                if t.crr { "true" } else { "false" }
            ));
        }
        out.push_str("],\"config\":{");
        for (i, (k, v)) in config.iter().enumerate() {
            out.push_str(&format!(
                "{}\"{}\":\"{}\"",
                if i > 0 { "," } else { "" },
                json_escape(k),
                json_escape(v)
            ));
        }
        out.push_str(&format!("}},\"disk_bytes\":{disk_bytes},\"files\":{files}}}\n"));
        return 0;
    }
    out.push_str("=== Murmur Storage Inspection ===\n");
    out.push_str(&format!("Data Directory:      {}\n", dir.display()));
    out.push_str(&format!("Files On Disk:       {files} files ({disk_bytes} bytes)\n"));
    out.push_str(&format!("Tables Registered:   {}\n", tables.len()));
    for t in &tables {
        out.push_str(&format!(
            "  - {} (columns: {}{})\n",
            t.name,
            t.columns,
            if t.crr { ", crr" } else { "" }
        ));
    }
    if !config.is_empty() {
        out.push_str("--- murmur_config ---\n");
        for (k, v) in &config {
            out.push_str(&format!("  {k} = {v}\n"));
        }
    }
    0
}
pub fn cmd_repair(dir: &std::path::Path, opts: &Opts, out: &mut String, err: &mut String) -> c_int {
    if refuse_if_locked(dir, err) {
        return 1;
    }
    if !register_key(opts, err) {
        return 1;
    }
    // Repair works at the LSM level (records stay opaque), so no key use
    // beyond registration; the key stays uniform across commands.
    let c = c_path(dir);
    // SAFETY: NUL-terminated path.
    let rc = unsafe { ffi_murmur::sqlite_murmur_repair(c.as_ptr()) };
    if rc == ffi_sqlite::SQLITE_OK {
        if opts.json {
            out.push_str(&format!(
                "{{\"data_dir\":\"{}\",\"repaired\":true}}\n",
                json_escape(&dir.to_string_lossy())
            ));
        } else {
            out.push_str(&format!("PASS: Store recovered at {}\n", dir.display()));
        }
        return 0;
    }
    if rc == ffi_sqlite::SQLITE_BUSY {
        err.push_str(&format!(
            "Error: refusing a live target: {} (store is open; stop the database first)\n",
            dir.display()
        ));
        return 1;
    }
    if rc == ffi_sqlite::SQLITE_CANTOPEN {
        err.push_str(&format!(
            "Error: cannot repair {} (missing or not a database; nothing was created)\n",
            dir.display()
        ));
        return 1;
    }
    err.push_str(&format!("Error: repair failed for {} ({rc})\n", dir.display()));
    1
}

pub fn cmd_keys(dir: &std::path::Path, opts: &Opts, out: &mut String, err: &mut String) -> c_int {
    if refuse_if_locked(dir, err) {
        return 1;
    }
    if !register_key(opts, err) {
        return 1;
    }
    let c = c_path(dir);
    let mut json: *mut c_char = std::ptr::null_mut();
    // SAFETY: NUL-terminated path, valid out-pointer; json freed below.
    let rc = unsafe { ffi_murmur::sqlite_murmur_key_inventory(c.as_ptr(), &mut json) };
    if rc == ffi_sqlite::SQLITE_BUSY {
        err.push_str(&format!(
            "Error: refusing a live target: {} (store is open; stop the database first)\n",
            dir.display()
        ));
        return 1;
    }
    if rc == ffi_sqlite::SQLITE_CANTOPEN {
        err.push_str(&format!(
            "Error: cannot read keys from {} (missing or not a database; nothing was created)\n",
            dir.display()
        ));
        return 1;
    }
    if rc != ffi_sqlite::SQLITE_OK || json.is_null() {
        // SAFETY: sqlite-allocated (or null).
        unsafe {
            ffi_sqlite::sqlite3_free(json as *mut c_void);
        }
        err.push_str(&format!("Error: key inventory failed for {} ({rc})\n", dir.display()));
        return 1;
    }
    // SAFETY: non-null NUL-terminated JSON from the inventory call.
    let body = unsafe { CStr::from_ptr(json).to_bytes().to_vec() };
    // SAFETY: sqlite-allocated.
    unsafe {
        ffi_sqlite::sqlite3_free(json as *mut c_void);
    }
    if opts.json {
        out.push_str(&String::from_utf8_lossy(&body));
        out.push('\n');
    } else {
        print_keys(&body, &dir.to_string_lossy(), out);
    }
    0
}

pub fn cmd_rekey(dir: &std::path::Path, opts: &Opts, out: &mut String, err: &mut String) -> c_int {
    if refuse_if_locked(dir, err) {
        return 1;
    }
    if !dir.is_dir() {
        err.push_str(&format!(
            "Error: cannot rekey {} (missing or not a database; nothing was created)\n",
            dir.display()
        ));
        return 1;
    }
    let mut old_hex = opts.key_hex.clone();
    if old_hex.is_empty() && !opts.key_file.is_empty() {
        if !read_key_file(&opts.key_file, &mut old_hex, err) {
            return 1;
        }
    }
    let mut old_key = parse_key_hex(old_hex.as_bytes());
    if !old_key.ok {
        err.push_str("Error: --key-hex or --key-file must provide 32 key bytes (64 hex characters)\n");
        return 1;
    }
    let mut new_hex = opts.new_key_hex.clone();
    if new_hex.is_empty() && !opts.new_key_file.is_empty() {
        if !read_key_file(&opts.new_key_file, &mut new_hex, err) {
            wipe_key(&mut old_key);
            return 1;
        }
    }
    let mut new_key = parse_key_hex(new_hex.as_bytes());
    if !new_key.ok {
        wipe_key(&mut old_key);
        err.push_str("Error: --new-key-hex or --new-key-file must provide 32 key bytes (64 hex characters)\n");
        return 1;
    }
    let c = c_path(dir);
    // SAFETY: NUL path, live 32-byte keys, null opts (normal run).
    let rc = unsafe {
        ffi_murmur::sqlite_murmur_rekey(c.as_ptr(), old_key.bytes.as_ptr(), new_key.bytes.as_ptr(), std::ptr::null())
    };
    wipe_key(&mut old_key);
    wipe_key(&mut new_key);
    if rc == ffi_sqlite::SQLITE_OK {
        if opts.json {
            out.push_str("{\"rekeyed\":true}\n");
        } else {
            out.push_str(&format!(
                "PASS: Database at {} rekeyed successfully under fresh envelope.\n",
                dir.display()
            ));
        }
        return 0;
    }
    if rc == ffi_sqlite::SQLITE_BUSY {
        err.push_str(&format!(
            "Error: refusing a live target: {} (store is open; stop the database first)\n",
            dir.display()
        ));
        return 1;
    }
    if rc == ffi_sqlite::SQLITE_CANTOPEN {
        err.push_str(&format!(
            "Error: cannot rekey {} (missing or not a database; nothing was created)\n",
            dir.display()
        ));
        return 1;
    }
    if rc == ffi_sqlite::SQLITE_AUTH {
        err.push_str("Error: cannot open with provided key\n");
        return 1;
    }
    if rc == ffi_sqlite::SQLITE_ERROR {
        err.push_str(&format!(
            "Error: cannot rekey {} (unsealed store, or sealed store with unsupported cipher/KDF)\n",
            dir.display()
        ));
        return 1;
    }
    err.push_str(&format!("Error: rekey failed ({rc}); retry rekey with the same keys to complete\n"));
    1
}
fn print_usage(out: &mut String) {
    out.push_str(&format!("Murmur Operational Admin CLI (v{VERSION})\n"));
    out.push_str("\nUsage:\n");
    out.push_str("  murmur-admin <command> [options] [arguments]\n");
    out.push_str("\nCommands (all offline-only; live targets are refused):\n");
    out.push_str("  status   <data-dir>                Identity and storage summary\n");
    out.push_str("  inspect  <data-dir>                Schema and config dump\n");
    out.push_str("  verify   <data-dir> [--backup]     Open, integrity_check, manifest\n");
    out.push_str("  restore  <backup-dir> <dest-dir>   Copy + optional fresh identity\n");
    out.push_str("  repair   <data-dir>                Recover a damaged store\n");
    out.push_str("  keys     <data-dir>                Show the key envelope\n");
    out.push_str("  rekey    <data-dir>                Full offline rekey to new envelope/master\n");
    out.push_str("\nOptions:\n");
    out.push_str("  --json                            Structured JSON output\n");
    out.push_str("  --key-hex=<hex>                   32-byte key (64 hex chars)\n");
    out.push_str("  --key-file=<path>                 File with 64 hex chars or 32 raw bytes\n");
    out.push_str("  --new-key-hex=<hex>               New 32-byte master key for rekey\n");
    out.push_str("  --new-key-file=<path>             File with new master key for rekey\n");
    out.push_str("  --backup                          Expect a backup artifact\n");
    out.push_str("  --fresh-node-id[=<hex>]            Rehome to a new node id\n");
    out.push_str("  --fresh-db-id[=<hex>]              Rehome to a new database id\n");
    out.push_str("  -v, --version                     Print version\n");
    out.push_str("  -h, --help                        Print help\n");
}

/// Parsed command line: either an early exit (help/version/error/no-op)
/// or a command dispatch. Pure (no FFI): unit-tested.
enum Parsed {
    Exit(c_int, String, String),
    Run { cmd: String, args: Vec<String>, opts: Opts },
}

fn parse_argv(argv: &[String]) -> Parsed {
    let mut opts = Opts::default();
    let mut positional: Vec<String> = Vec::new();
    let mut i = 1usize;
    while i < argv.len() {
        let arg = &argv[i];
        if arg == "-h" || arg == "--help" {
            let mut out = String::new();
            print_usage(&mut out);
            return Parsed::Exit(0, out, String::new());
        } else if arg == "-v" || arg == "--version" {
            return Parsed::Exit(
                0,
                format!("murmur-admin version {VERSION}\n"),
                String::new(),
            );
        } else if arg == "--json" {
            opts.json = true;
        } else if arg == "--backup" {
            opts.is_backup = true;
        } else if let Some(v) = arg.strip_prefix("--key-hex=") {
            opts.key_hex = v.to_string();
        } else if arg == "--key-hex" && i + 1 < argv.len() {
            i += 1;
            opts.key_hex = argv[i].clone();
        } else if let Some(v) = arg.strip_prefix("--key-file=") {
            opts.key_file = v.to_string();
        } else if arg == "--key-file" && i + 1 < argv.len() {
            i += 1;
            opts.key_file = argv[i].clone();
        } else if let Some(v) = arg.strip_prefix("--new-key-hex=") {
            opts.new_key_hex = v.to_string();
        } else if arg == "--new-key-hex" && i + 1 < argv.len() {
            i += 1;
            opts.new_key_hex = argv[i].clone();
        } else if let Some(v) = arg.strip_prefix("--new-key-file=") {
            opts.new_key_file = v.to_string();
        } else if arg == "--new-key-file" && i + 1 < argv.len() {
            i += 1;
            opts.new_key_file = argv[i].clone();
        } else if arg == "--fresh-node-id" {
            opts.fresh_node = true;
        } else if let Some(v) = arg.strip_prefix("--fresh-node-id=") {
            opts.fresh_node = true;
            opts.fresh_node_hex = v.to_string();
        } else if arg == "--fresh-db-id" {
            opts.fresh_db = true;
        } else if let Some(v) = arg.strip_prefix("--fresh-db-id=") {
            opts.fresh_db = true;
            opts.fresh_db_hex = v.to_string();
        } else if arg.starts_with('-') && !arg.is_empty() {
            return Parsed::Exit(
                1,
                String::new(),
                format!("Error: unknown option: {arg}\n"),
            );
        } else {
            positional.push(arg.clone());
        }
        i += 1;
    }
    if positional.is_empty() {
        let mut out = String::new();
        print_usage(&mut out);
        return Parsed::Exit(0, out, String::new());
    }
    // NOTE: key-file/ng-key-file reads and key validation happen in
    // dispatch (they emit to err); parsing stays pure.
    Parsed::Run {
        cmd: positional[0].clone(),
        args: positional[1..].to_vec(),
        opts,
    }
}
/// argv dispatch (mirrors `admin_main`). Returns the process exit code.
pub fn admin_main(argv: &[String], out: &mut String, err: &mut String) -> c_int {
    let (cmd, args, mut opts) = match parse_argv(argv) {
        Parsed::Exit(code, o, e) => {
            out.push_str(&o);
            err.push_str(&e);
            return code;
        }
        Parsed::Run { cmd, args, opts } => (cmd, args, opts),
    };
    if opts.key_hex.is_empty() && !opts.key_file.is_empty() {
        let mut hex = String::new();
        if !read_key_file(&opts.key_file, &mut hex, err) {
            return 1;
        }
        opts.key_hex = hex;
    }
    if !parse_key_hex(opts.key_hex.as_bytes()).ok {
        err.push_str("Error: --key-hex=<64 hex chars> or --key-file=<path> is required\n");
        return 1;
    }
    if !opts.fresh_node_hex.is_empty() {
        let mut tmp = [0u8; 16];
        if !parse_id16(opts.fresh_node_hex.as_bytes(), &mut tmp) {
            err.push_str("Error: --fresh-node-id must be 32 hex characters\n");
            return 1;
        }
    }
    if !opts.fresh_db_hex.is_empty() {
        let mut tmp = [0u8; 16];
        if !parse_id16(opts.fresh_db_hex.as_bytes(), &mut tmp) {
            err.push_str("Error: --fresh-db-id must be 32 hex characters\n");
            return 1;
        }
    }
    if cmd == "verify" {
        if args.len() != 1 {
            err.push_str("Error: usage: murmur-admin verify <data-dir> [--backup] --key-hex=<hex>\n");
            return 1;
        }
        return cmd_verify(std::path::Path::new(&args[0]), &opts, out, err);
    }
    if cmd == "restore" {
        if args.len() != 2 {
            err.push_str("Error: usage: murmur-admin restore <backup-dir> <dest-dir> [--fresh-node-id[=hex]] [--fresh-db-id[=hex]] --key-hex=<hex>\n");
            return 1;
        }
        return cmd_restore(
            std::path::Path::new(&args[0]),
            std::path::Path::new(&args[1]),
            &opts,
            out,
            err,
        );
    }
    if cmd == "status" {
        if args.len() != 1 {
            err.push_str("Error: usage: murmur-admin status <data-dir> --key-hex=<hex>\n");
            return 1;
        }
        return cmd_status(std::path::Path::new(&args[0]), &opts, out, err);
    }
    if cmd == "inspect" {
        if args.len() != 1 {
            err.push_str("Error: usage: murmur-admin inspect <data-dir> --key-hex=<hex>\n");
            return 1;
        }
        return cmd_inspect(std::path::Path::new(&args[0]), &opts, out, err);
    }
    if cmd == "repair" {
        if args.len() != 1 {
            err.push_str("Error: usage: murmur-admin repair <data-dir> --key-hex=<hex>\n");
            return 1;
        }
        return cmd_repair(std::path::Path::new(&args[0]), &opts, out, err);
    }
    if cmd == "keys" {
        if args.len() != 1 {
            err.push_str("Error: usage: murmur-admin keys <data-dir> --key-hex=<hex>\n");
            return 1;
        }
        return cmd_keys(std::path::Path::new(&args[0]), &opts, out, err);
    }
    if cmd == "rekey" {
        if args.len() != 1 {
            err.push_str("Error: usage: murmur-admin rekey <data-dir> (--key-hex=<hex>|--key-file=<path>) (--new-key-hex=<hex>|--new-key-file=<path>) [--json]\n");
            return 1;
        }
        if opts.new_key_hex.is_empty() && !opts.new_key_file.is_empty() {
            let mut hex = String::new();
            if !read_key_file(&opts.new_key_file, &mut hex, err) {
                return 1;
            }
            opts.new_key_hex = hex;
        }
        if !parse_key_hex(opts.new_key_hex.as_bytes()).ok {
            err.push_str("Error: --new-key-hex=<64 hex chars> or --new-key-file=<path> is required for rekey\n");
            return 1;
        }
        return cmd_rekey(std::path::Path::new(&args[0]), &opts, out, err);
    }
    err.push_str(&format!(
        "Error: unknown command: \"{cmd}\". Run 'murmur-admin --help' for usage.\n"
    ));
    1
}
// ---------------------------------------------------------------------------
// C ABI: declared in tools/murmur-admin/admin.h. Capture buffers (`char **`)
// replace the injected ostreams; the caller releases them with
// `murmur_admin_free_str`. A null capture pointer discards that stream.
// ---------------------------------------------------------------------------

/// C mirror of [`Opts`]. All strings are NUL-terminated; NULL means empty.
#[repr(C)]
pub struct AdminOptsC {
    pub json: c_int,
    pub is_backup: c_int,
    pub key_hex: *const c_char,
    pub key_file: *const c_char,
    pub new_key_hex: *const c_char,
    pub new_key_file: *const c_char,
    pub fresh_node: c_int,
    pub fresh_db: c_int,
    pub fresh_node_hex: *const c_char,
    pub fresh_db_hex: *const c_char,
}

/// C mirror of [`AdminKey`].
#[repr(C)]
pub struct AdminKeyC {
    pub bytes: [u8; 32],
    pub ok: c_int,
}

fn c_str_or_empty(p: *const c_char) -> String {
    if p.is_null() {
        return String::new();
    }
    // SAFETY: NUL-terminated per contract.
    unsafe { CStr::from_ptr(p).to_string_lossy().into_owned() }
}

fn opts_from_c(o: &AdminOptsC) -> Opts {
    Opts {
        json: o.json != 0,
        is_backup: o.is_backup != 0,
        key_hex: c_str_or_empty(o.key_hex),
        key_file: c_str_or_empty(o.key_file),
        new_key_hex: c_str_or_empty(o.new_key_hex),
        new_key_file: c_str_or_empty(o.new_key_file),
        fresh_node: o.fresh_node != 0,
        fresh_db: o.fresh_db != 0,
        fresh_node_hex: c_str_or_empty(o.fresh_node_hex),
        fresh_db_hex: c_str_or_empty(o.fresh_db_hex),
    }
}

fn emit_capture(slot: *mut *mut c_char, text: &str) {
    if slot.is_null() {
        return;
    }
    // Output never contains NUL (validated messages + escaped data);
    // degrade to empty rather than panic on the FFI boundary.
    let c = CString::new(text).unwrap_or_default();
    // SAFETY: caller-provided slot; ownership transfers to the caller.
    unsafe {
        *slot = c.into_raw();
    }
}

fn path_from_c(p: *const c_char) -> Option<std::path::PathBuf> {
    if p.is_null() {
        return None;
    }
    // SAFETY: NUL-terminated per contract.
    let bytes = unsafe { CStr::from_ptr(p) }.to_bytes();
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        Some(std::ffi::OsStr::from_bytes(bytes).into())
    }
    #[cfg(not(unix))]
    {
        Some(std::path::PathBuf::from(String::from_utf8_lossy(bytes).into_owned()))
    }
}

/// Release a capture buffer from any `murmur_admin_*` call. Null-safe.
#[unsafe(no_mangle)]
pub extern "C" fn murmur_admin_free_str(s: *mut c_char) {
    if s.is_null() {
        return;
    }
    // SAFETY: buffer previously transferred by `emit_capture`.
    unsafe {
        drop(CString::from_raw(s));
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn murmur_admin_parse_key_hex(hex: *const c_char, out: *mut AdminKeyC) -> c_int {
    guard(
        || {
            if hex.is_null() || out.is_null() {
                return 0;
            }
            // SAFETY: NUL-terminated per contract; out is live.
            let bytes = unsafe { CStr::from_ptr(hex) }.to_bytes();
            let key = parse_key_hex(bytes);
            unsafe {
                (*out).bytes = key.bytes;
                (*out).ok = c_int::from(key.ok);
            }
            c_int::from(key.ok)
        },
        0,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn murmur_admin_wipe_key(key: *mut AdminKeyC) {
    guard(
        || {
            if key.is_null() {
                return;
            }
            let mut k = AdminKey {
                // SAFETY: live struct per contract.
                bytes: unsafe { (*key).bytes },
                ok: true,
            };
            wipe_key(&mut k);
            // SAFETY: live struct per contract.
            unsafe {
                (*key).bytes = k.bytes;
                (*key).ok = 0;
            }
        },
        (),
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn murmur_admin_parse_id16(hex: *const c_char, out: *mut u8) -> c_int {
    guard(
        || {
            if hex.is_null() || out.is_null() {
                return 0;
            }
            // SAFETY: NUL-terminated per contract; out holds 16 bytes.
            let bytes = unsafe { CStr::from_ptr(hex) }.to_bytes();
            let mut id = [0u8; 16];
            if !parse_id16(bytes, &mut id) {
                return 0;
            }
            unsafe {
                std::ptr::copy_nonoverlapping(id.as_ptr(), out, 16);
            }
            1
        },
        0,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn murmur_admin_ensure_registered(key_hex: *const c_char, err: *mut *mut c_char) -> c_int {
    guard(
        || {
            let mut es = String::new();
            let ok = ensure_registered(&c_str_or_empty(key_hex), &mut es);
            emit_capture(err, &es);
            c_int::from(ok)
        },
        0,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn murmur_admin_store_locked(dir: *const c_char, detail: *mut *mut c_char) -> c_int {
    guard(
        || {
            let mut ds = String::new();
            let Some(path) = path_from_c(dir) else {
                ds = "null path".to_string();
                emit_capture(detail, &ds);
                return 0;
            };
            let locked = store_locked(&path, &mut ds);
            emit_capture(detail, &ds);
            c_int::from(locked)
        },
        0,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn murmur_admin_copy_tree(
    src: *const c_char,
    dest: *const c_char,
    msg: *mut *mut c_char,
) -> c_int {
    guard(
        || {
            let mut ms = String::new();
            let (Some(s), Some(d)) = (path_from_c(src), path_from_c(dest)) else {
                ms = "null path".to_string();
                emit_capture(msg, &ms);
                return 1;
            };
            let rc = copy_tree(&s, &d, &mut ms);
            emit_capture(msg, &ms);
            rc
        },
        1,
    )
}

macro_rules! admin_cmd {
    ($name:ident, $cmd:ident, 1) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $name(
            target: *const c_char,
            opts: *const AdminOptsC,
            out: *mut *mut c_char,
            err: *mut *mut c_char,
        ) -> c_int {
            guard(
                || {
                    let mut os = String::new();
                    let mut es = String::new();
                    let rc = match (path_from_c(target), unsafe { opts.as_ref() }) {
                        (Some(t), Some(o)) => $cmd(&t, &opts_from_c(o), &mut os, &mut es),
                        _ => {
                            es = "Error: null argument\n".to_string();
                            1
                        }
                    };
                    emit_capture(out, &os);
                    emit_capture(err, &es);
                    rc
                },
                1,
            )
        }
    };
    ($name:ident, $cmd:ident, 2) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $name(
            first: *const c_char,
            second: *const c_char,
            opts: *const AdminOptsC,
            out: *mut *mut c_char,
            err: *mut *mut c_char,
        ) -> c_int {
            guard(
                || {
                    let mut os = String::new();
                    let mut es = String::new();
                    let rc = match (path_from_c(first), path_from_c(second), unsafe {
                        opts.as_ref()
                    }) {
                        (Some(a), Some(b), Some(o)) => $cmd(&a, &b, &opts_from_c(o), &mut os, &mut es),
                        _ => {
                            es = "Error: null argument\n".to_string();
                            1
                        }
                    };
                    emit_capture(out, &os);
                    emit_capture(err, &es);
                    rc
                },
                1,
            )
        }
    };
}

admin_cmd!(murmur_admin_cmd_verify, cmd_verify, 1);
admin_cmd!(murmur_admin_cmd_restore, cmd_restore, 2);
admin_cmd!(murmur_admin_cmd_status, cmd_status, 1);
admin_cmd!(murmur_admin_cmd_inspect, cmd_inspect, 1);
admin_cmd!(murmur_admin_cmd_repair, cmd_repair, 1);
admin_cmd!(murmur_admin_cmd_keys, cmd_keys, 1);
admin_cmd!(murmur_admin_cmd_rekey, cmd_rekey, 1);

#[unsafe(no_mangle)]
pub extern "C" fn murmur_admin_main(
    argc: c_int,
    argv: *const *const c_char,
    out: *mut *mut c_char,
    err: *mut *mut c_char,
) -> c_int {
    guard(
        || {
            let mut os = String::new();
            let mut es = String::new();
            let rc = if argc < 0 || argv.is_null() {
                es = "Error: null argument\n".to_string();
                1
            } else {
                // SAFETY: argc NUL-terminated entries per contract.
                let args: Vec<String> = unsafe {
                    std::slice::from_raw_parts(argv, argc as usize)
                        .iter()
                        .map(|p| {
                            if p.is_null() {
                                String::new()
                            } else {
                                CStr::from_ptr(*p).to_string_lossy().into_owned()
                            }
                        })
                        .collect()
                };
                admin_main(&args, &mut os, &mut es)
            };
            emit_capture(out, &os);
            emit_capture(err, &es);
            rc
        },
        1,
    )
}
#[cfg(test)]
mod more_tests {
    use super::*;
    use std::io::Write;

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "murmur-admin-ut-{}-{}",
            name,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("scratch");
        dir
    }

    #[test]
    fn manifest_gates() {
        let dir = scratch("manifest");
        let good = "{\"format_version\":1,\"cipher\":\"aes256gcm\",\
            \"group_id\":\"00\",\"node_id\":\"11\",\
            \"rocksdb_seq\":7,\"created_unix_ms\":1}\n";
        std::fs::write(dir.join("manifest.json"), good).expect("write");
        let (mut c, mut g, mut n, mut s) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let mut msg = String::new();
        assert!(check_manifest(&dir, &mut c, &mut g, &mut n, &mut s, &mut msg));
        assert_eq!(c, b"aes256gcm");
        assert_eq!(s, b"7");
        // Bad cipher, bad version, bad schema, missing file.
        for (body, want) in [
            (good.replace("aes256gcm", "rot13"), "unknown cipher"),
            (good.replace(":1,", ":2,"), "unsupported format_version"),
            ("{\"format_version\":1}\n".to_string(), "unexpected schema"),
        ] {
            std::fs::write(dir.join("manifest.json"), body).expect("write");
            let mut msg = String::new();
            assert!(!check_manifest(&dir, &mut c, &mut g, &mut n, &mut s, &mut msg));
            assert!(msg.contains(want), "msg was: {msg}");
        }
        std::fs::remove_file(dir.join("manifest.json")).expect("rm");
        assert!(!check_manifest(&dir, &mut c, &mut g, &mut n, &mut s, &mut msg));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn inventory_extractors() {
        let body = br#"{"sealed":true,"seq":2,"kdf":"direct","kdf_time":0,"kdf_memory_kib":0,"kdf_threads":0,"wrap_id":"","current":2,"keys":[{"id":1,"created_nanos":5,"active":false},{"id":2,"created_nanos":6,"active":true}]}"#;
        assert_eq!(inv_string(body, b"kdf"), b"direct");
        assert_eq!(inv_number(body, b"seq"), b"2");
        assert_eq!(inv_number(body, b"current"), b"2");
        assert_eq!(inv_key_ids(body), vec![b"1".to_vec(), b"2".to_vec()]);
        assert!(inv_string(body, b"nope").is_empty());
        assert!(inv_number(body, b"nope").is_empty());
        let mut out = String::new();
        print_keys(body, "/d", &mut out);
        assert!(out.contains("Sealed:   yes\n"), "{out}");
        assert!(out.contains("KDF:      direct\n"), "{out}");
        assert!(out.contains("Seq:      2\n"), "{out}");
        assert!(out.contains("Current:  2\n"), "{out}");
        assert!(out.contains("Keys:     1, 2\n"), "{out}");
        let mut out2 = String::new();
        print_keys(br#"{"sealed":false}"#, "/d", &mut out2);
        assert!(out2.contains("No key envelope"), "{out2}");
    }

    #[test]
    fn verify_printer_shapes() {
        let r = VerifyResult {
            valid: true,
            mode: "database".to_string(),
            database_id: Vec::new(),
            node_id: b"aa".to_vec(),
            cipher: b"aes256gcm".to_vec(),
            seq: Vec::new(),
            tables_checked: 3,
            errors: Vec::new(),
        };
        let mut out = String::new();
        print_verify(&r, "/d", false, &mut out);
        assert!(out.contains("PASS: Database storage integrity verified at /d\n"), "{out}");
        assert!(out.contains("Node ID:         aa\n"), "{out}");
        assert!(out.contains("Cipher:          aes256gcm\n"), "{out}");
        assert!(out.contains("Tables Checked:  3\n"), "{out}");
        let mut js = String::new();
        print_verify(&r, "/d", true, &mut js);
        assert_eq!(
            js,
            "{\"target\":\"/d\",\"valid\":true,\"mode\":\"database\",\
            \"node_id\":\"aa\",\"cipher\":\"aes256gcm\",\
            \"tables_checked\":3,\"errors\":[]}\n"
                .replace("            ", "")
        );
        let bad = VerifyResult {
            valid: false,
            mode: "backup".to_string(),
            database_id: Vec::new(),
            node_id: Vec::new(),
            cipher: Vec::new(),
            seq: Vec::new(),
            tables_checked: 0,
            errors: vec!["boom".to_string()],
        };
        let mut fo = String::new();
        print_verify(&bad, "/d", false, &mut fo);
        assert!(fo.contains("FAIL: verification failed at /d:\n  - boom\n"), "{fo}");
    }

    #[test]
    fn argv_parses() {
        // --help / --version / empty.
        for argv in [
            vec!["murmur-admin".to_string(), "--help".to_string()],
            vec!["murmur-admin".to_string()],
        ] {
            match parse_argv(&argv) {
                Parsed::Exit(0, o, e) => {
                    assert!(o.contains("verify"), "{o}");
                    assert!(e.is_empty());
                }
                _ => panic!("help must early-exit 0"),
            }
        }
        match parse_argv(&["murmur-admin".to_string(), "-v".to_string()]) {
            Parsed::Exit(0, o, _) => assert!(o.contains("0.1.0"), "{o}"),
            _ => panic!("version must early-exit 0"),
        }
        // Unknown option / key forms / fresh ids.
        match parse_argv(&["murmur-admin".to_string(), "--bogus".to_string()]) {
            Parsed::Exit(1, _, e) => assert!(e.contains("unknown option"), "{e}"),
            _ => panic!("bogus must exit 1"),
        }
        match parse_argv(&[
            "murmur-admin".to_string(),
            "--key-hex".to_string(),
            "k".to_string(),
            "keys".to_string(),
            "/d".to_string(),
        ]) {
            Parsed::Run { cmd, args, opts } => {
                assert_eq!(cmd, "keys");
                assert_eq!(args, vec!["/d".to_string()]);
                assert_eq!(opts.key_hex, "k");
            }
            _ => panic!("keys must parse"),
        }
        // Trailing bare --key-hex is an unknown option (C++ fall-through).
        match parse_argv(&["murmur-admin".to_string(), "--key-hex".to_string()]) {
            Parsed::Exit(1, _, e) => assert!(e.contains("unknown option"), "{e}"),
            _ => panic!("dangling flag must exit 1"),
        }
    }

    #[test]
    fn copy_and_probe() {
        let root = scratch("copy");
        let src = root.join("src");
        std::fs::create_dir_all(src.join("sub")).expect("mkdir");
        std::fs::write(src.join("a.txt"), "A").expect("write");
        std::fs::write(src.join("sub").join("b.bin"), b"\0\x01\x02").expect("write");
        let mut msg = String::new();
        let dest = root.join("dest");
        assert_eq!(copy_tree(&src, &dest, &mut msg), 0, "{msg}");
        assert_eq!(std::fs::read(dest.join("a.txt")).expect("read"), b"A");
        assert_eq!(
            std::fs::read(dest.join("sub").join("b.bin")).expect("read"),
            b"\0\x01\x02"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dest.join("a.txt")).expect("meta").permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "{mode:o}");
        }
        assert_eq!(copy_tree(&src, &dest, &mut msg), 1);
        // Symlink refusal + cleanup.
        let lsrc = root.join("lsrc");
        std::fs::create_dir_all(&lsrc).expect("mkdir");
        #[cfg(unix)]
        std::os::unix::fs::symlink(src.join("a.txt"), lsrc.join("l")).expect("link");
        #[cfg(unix)]
        {
            assert_eq!(copy_tree(&lsrc, &root.join("ldest"), &mut msg), 1);
            assert!(!root.join("ldest").exists());
        }
        // LOCK probe states.
        let mut detail = String::new();
        let probed = root.join("probed");
        std::fs::create_dir_all(&probed).expect("mkdir");
        assert!(!store_locked(&probed, &mut detail));
        let mut f = std::fs::File::create(probed.join("LOCK")).expect("lock");
        f.write_all(b"").ok();
        drop(f);
        assert!(!store_locked(&probed, &mut detail), "{detail}");
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn key_cycle() {
        let k = parse_key_hex(b"00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff");
        assert!(k.ok);
        assert_eq!(k.bytes[0], 0x00);
        assert_eq!(k.bytes[31], 0xff);
        assert!(!parse_key_hex(b"0123").ok);
        let mut id = [0u8; 16];
        assert!(parse_id16(b"0102030405060708090a0b0c0d0e0f10", &mut id));
        assert_eq!(id[15], 0x10);
        assert!(!parse_id16(b"xyz", &mut id));
        let mut k2 = parse_key_hex(b"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
        wipe_key(&mut k2);
        assert!(!k2.ok);
        assert!(k2.bytes.iter().all(|b| *b == 0));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_round_trip() {
        let mut out = [0u8; 32];
        assert!(hex_to_bytes(
            b"00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            &mut out
        ));
        assert_eq!(out[0], 0x00);
        assert_eq!(out[31], 0xff);
        assert_eq!(
            bytes_to_hex(&out),
            "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
        );
        // Uppercase accepted, odd/short/bad rejected.
        let mut w = [0u8; 2];
        assert!(hex_to_bytes(b"aB", &mut w[..1]));
        assert!(!hex_to_bytes(b"0123", &mut [0u8; 3]));
        assert!(!hex_to_bytes(b"zz", &mut w[..1]));
        assert!(!hex_to_bytes(b"0", &mut w[..1]));
    }

    #[test]
    fn escape_shapes() {
        assert_eq!(json_escape("plain"), "plain");
        assert_eq!(json_escape("a\"b\\c"), "a\\\"b\\\\c");
        assert_eq!(json_escape("a\nb\rc\td"), "a\\nb\\rc\\td");
        assert_eq!(json_escape("\x01\x1f"), "\\u0001\\u001f");
    }

    #[test]
    fn ident_and_secrets() {
        assert_eq!(quote_ident("t"), "\"t\"");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
        assert!(secret_name("private_key"));
        assert!(secret_name("TLS Secret"));
        assert!(secret_name("db_password"));
        assert!(secret_name("CertificatePem"));
        assert!(!secret_name("group_id"));
        assert!(!secret_name("node"));
    }
}
