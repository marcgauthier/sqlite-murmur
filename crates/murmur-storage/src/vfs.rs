//! SQLite VFS (`sqlite-murmur`), main-DB/memory file dispatch, and the
//! remaining stable C API. Ported from the second half of
//! `src/sqlite_murmur.cpp` (`MurmurFile`, `io*`, `vfs*`, `sqlite_murmur_*`).
//!
//! Every `extern "C"` entry is panic-guarded (fail closed, never unwind
//! into SQLite). `MurmurFile` is placement-constructed in SQLite's
//! `szOsFile` buffer exactly like the C++.

use std::collections::HashMap;
use std::ffi::{CStr, c_char, c_double, c_int, c_uchar, c_void};
use std::os::unix::ffi::OsStrExt;
use std::sync::{Arc, LazyLock, Mutex, OnceLock, Weak, atomic::{AtomicPtr, AtomicU64, Ordering}};
use zeroize::Zeroize;

use crate::abi::{
    OPEN_STORAGE_ONLY, sqlite_murmur_backup_args, sqlite_murmur_config, sqlite_murmur_extension,
    sqlite_murmur_keyring_args, sqlite_murmur_open_options, sqlite_murmur_rekey_options,
    sqlite_murmur_stats,
};
use crate::crypto::{BLOCK, Cipher};
use crate::ffi_rocksdb::{ReadOptions, StoreOptions, WriteBatch, WriteOptions, is_busy_or_retry, repair_db};
use crate::ffi_sqlite::{
    SQLITE_ACCESS_READWRITE, SQLITE_BUSY, SQLITE_CANTOPEN, SQLITE_CORRUPT, SQLITE_ERROR,
    SQLITE_FCNTL_COMMIT_PHASETWO, SQLITE_FCNTL_MMAP_SIZE, SQLITE_FCNTL_PRAGMA, SQLITE_FULL,
    SQLITE_IOERR, SQLITE_IOERR_DELETE, SQLITE_IOERR_READ, SQLITE_IOERR_SHORT_READ,
    SQLITE_IOERR_TRUNCATE, SQLITE_IOERR_WRITE, SQLITE_LOCK_EXCLUSIVE, SQLITE_LOCK_NONE,
    SQLITE_LOCK_PENDING, SQLITE_LOCK_RESERVED, SQLITE_LOCK_SHARED, SQLITE_MISUSE,
    SQLITE_NOMEM, SQLITE_NOTFOUND, SQLITE_OK, SQLITE_OPEN_CREATE, SQLITE_OPEN_DELETEONCLOSE,
    SQLITE_OPEN_FULLMUTEX, SQLITE_OPEN_MAIN_DB, SQLITE_OPEN_MAIN_JOURNAL, SQLITE_OPEN_READONLY,
    SQLITE_READONLY, sqlite3, sqlite3_file, sqlite3_io_methods,
    sqlite3_vfs,
};
use crate::pool::{MemFile, Pooled};
use crate::store::{META_CIPHER, META_KEYRING, META_KEYRING_REKEY, META_STORE_ID, Settings, Store, lock};

const VFS_NAME: &[u8] = b"sqlite-murmur\0";

struct Globals {
    settings: Settings,
    stores: HashMap<Vec<u8>, Weak<Store>>,
    mem_files: HashMap<Vec<u8>, Pooled<MemFile>>,
    registered: bool,
    mx_pathname: c_int,
    file_key: [u8; 32],
    file_key_previous: [u8; 32],
    file_key_set: bool,
    file_key_previous_set: bool,
}

static GLOBALS: LazyLock<Mutex<Globals>> = LazyLock::new(|| {
    Mutex::new(Globals {
        settings: Settings::new([0u8; 32], 250, 16384, false),
        stores: HashMap::new(),
        mem_files: HashMap::new(),
        registered: false,
        mx_pathname: 0,
        file_key: [0u8; 32],
        file_key_previous: [0u8; 32],
        file_key_set: false,
        file_key_previous_set: false,
    })
});

/// Stable, leaked VFS struct (the C++ `gVfs` static). Initialized once;
/// fields rewritten on every register (like the C++ assignment).
struct VfsPtr(*mut sqlite3_vfs);
// SAFETY: the referent is a stable leaked allocation, mutated only under
// `GLOBALS` in `register` (serialized) and read by SQLite after
// registration — the same discipline as the C++ static.
unsafe impl Send for VfsPtr {}
unsafe impl Sync for VfsPtr {}
static GVFS: OnceLock<VfsPtr> = OnceLock::new();
/// Parent VFS (lock-free by design: delegates never take `GLOBALS`).
static GPARENT: AtomicPtr<sqlite3_vfs> = AtomicPtr::new(std::ptr::null_mut());
static TEMP_ID: AtomicU64 = AtomicU64::new(0);

fn gvfs() -> *mut sqlite3_vfs {
    GVFS.get_or_init(|| VfsPtr(Box::into_raw(Box::new(empty_vfs())))).0
}

fn empty_vfs() -> sqlite3_vfs {
    sqlite3_vfs {
        i_version: 0,
        sz_os_file: 0,
        mx_pathname: 0,
        p_next: std::ptr::null_mut(),
        z_name: std::ptr::null(),
        p_app_data: std::ptr::null_mut(),
        x_open: None,
        x_delete: None,
        x_access: None,
        x_full_pathname: None,
        x_dl_open: None,
        x_dl_error: None,
        x_dl_sym: None,
        x_dl_close: None,
        x_randomness: None,
        x_sleep: None,
        x_current_time: None,
        x_get_last_error: None,
        x_current_time_int64: None,
        x_set_system_call: None,
        x_get_system_call: None,
        x_next_system_call: None,
    }
}

#[repr(C)]
struct MurmurFile {
    base: sqlite3_file,
    store: Option<Arc<Store>>,
    mem: Option<Pooled<MemFile>>,
    name: Vec<u8>,
    lock_level: c_int,
    main_db: bool,
    read_only: bool,
    delete_on_close: bool,
    extension: sqlite_murmur_extension,
}

impl MurmurFile {
    fn new() -> Self {
        Self {
            base: sqlite3_file { p_methods: std::ptr::null() },
            store: None,
            mem: None,
            name: Vec::new(),
            lock_level: SQLITE_LOCK_NONE,
            main_db: false,
            read_only: false,
            delete_on_close: false,
            extension: sqlite_murmur_extension { context: std::ptr::null_mut(), pragma: None, destroy: None },
        }
    }
}

fn is_journal(name: &[u8]) -> bool {
    name.len() >= 8 && &name[name.len() - 8..] == b"-journal"
}

fn find_store_for_journal_locked(globals: &Globals, name: &[u8]) -> Option<Arc<Store>> {
    if !is_journal(name) {
        return None;
    }
    let base = &name[..name.len() - 8];
    globals.stores.get(base).and_then(Weak::upgrade)
}

unsafe extern "C" fn io_close(p: *mut sqlite3_file) -> c_int {
    crate::guard(
        || {
            // SAFETY: SQLite passes a live `MurmurFile` it allocated with
            // `szOsFile` bytes (single close per open by contract).
            let file = unsafe { &mut *(p as *mut MurmurFile) };
            let addr = file as *mut MurmurFile as usize;
            if file.main_db {
                if let Some(store) = file.store.as_ref() {
                    lock(&store.mu).locks.remove(&addr);
                }
            }
            if file.delete_on_close && !file.name.is_empty() {
                lock(&GLOBALS).mem_files.remove(&file.name);
            }
            if let Some(destroy) = file.extension.destroy {
                // SAFETY: installed by the extension owner via FCTL with
                // the documented destroy contract; called once here.
                unsafe { destroy(file.extension.context) };
            }
            // SAFETY: placement-constructed in `vfs_open`; balanced here.
            unsafe { std::ptr::drop_in_place(file) };
            SQLITE_OK
        },
        SQLITE_IOERR_DELETE,
    )
}

unsafe extern "C" fn io_read(p: *mut sqlite3_file, data: *mut c_void, n: c_int, off: i64) -> c_int {
    crate::guard(
        || {
            // SAFETY: `p` is a live file; `data` spans `n` bytes per contract.
            let file = unsafe { &mut *(p as *mut MurmurFile) };
            if file.main_db {
                let store = file.store.as_ref().expect("main db owns a store");
                if off < 0 || n < 0 {
                    return SQLITE_IOERR_READ;
                }
                let dst = unsafe { std::slice::from_raw_parts_mut(data as *mut u8, n as usize) };
                return store.read(dst, off);
            }
            if off < 0 || n < 0 {
                return SQLITE_IOERR_READ;
            }
            let mem = file.mem.as_ref().expect("mem file owns a buffer");
            let inner = mem.lock();
            let pos = off as u64;
            let avail = if pos < inner.size as u64 {
                (n as u64).min(inner.size as u64 - pos) as usize
            } else {
                0
            };
            if avail > 0 {
                unsafe { std::ptr::copy_nonoverlapping(inner.bytes.as_ptr().add(pos as usize), data as *mut u8, avail) };
            }
            if avail < n as usize {
                unsafe { std::ptr::write_bytes((data as *mut u8).add(avail), 0, n as usize - avail) };
                return SQLITE_IOERR_SHORT_READ;
            }
            SQLITE_OK
        },
        SQLITE_IOERR_READ,
    )
}

unsafe extern "C" fn io_write(
    p: *mut sqlite3_file,
    data: *const c_void,
    n: c_int,
    off: i64,
) -> c_int {
    crate::guard(
        || {
            // SAFETY: same contract as `io_read`.
            let file = unsafe { &mut *(p as *mut MurmurFile) };
            if file.main_db {
                let store = file.store.as_ref().expect("main db owns a store");
                if off < 0 || n < 0 {
                    return SQLITE_IOERR_WRITE;
                }
                let src = unsafe { std::slice::from_raw_parts(data as *const u8, n as usize) };
                return store.write(src, off);
            }
            if off < 0 || n < 0 {
                return SQLITE_IOERR_WRITE;
            }
            let end = off as u64 + n as u64;
            if end > usize::MAX as u64 {
                return SQLITE_FULL;
            }
            let mem = file.mem.as_ref().expect("mem file owns a buffer");
            let mut inner = mem.lock();
            if end as usize > inner.bytes.len() {
                inner.bytes.resize(end as usize, 0);
            }
            if off as u64 > inner.size as u64 {
                let from = inner.size;
                let to = off as usize;
                inner.bytes[from..to].fill(0);
            }
            if n != 0 {
                unsafe {
                    std::ptr::copy_nonoverlapping(data as *const u8, inner.bytes.as_mut_ptr().add(off as usize), n as usize)
                };
                inner.dirty_end = inner.dirty_end.max(end as usize);
            }
            inner.size = inner.size.max(end as usize);
            SQLITE_OK
        },
        SQLITE_IOERR_WRITE,
    )
}

unsafe extern "C" fn io_truncate(p: *mut sqlite3_file, n: i64) -> c_int {
    crate::guard(
        || {
            // SAFETY: `p` is a live file.
            let file = unsafe { &mut *(p as *mut MurmurFile) };
            if file.main_db {
                let store = file.store.as_ref().expect("main db owns a store");
                return store.truncate(n);
            }
            if n < 0 || n as u64 > usize::MAX as u64 {
                return SQLITE_IOERR_TRUNCATE;
            }
            let mem = file.mem.as_ref().expect("mem file owns a buffer");
            let mut inner = mem.lock();
            let target = n as usize;
            if target > inner.bytes.len() {
                inner.bytes.resize(target, 0);
            }
            let dirty_end = inner.dirty_end;
            if target < dirty_end {
                crate::secure_wipe(&mut inner.bytes[target..dirty_end]);
                inner.dirty_end = target;
            }
            let size = inner.size;
            if target > size {
                inner.bytes[size..target].fill(0);
            }
            inner.size = target;
            SQLITE_OK
        },
        SQLITE_IOERR_TRUNCATE,
    )
}

unsafe extern "C" fn io_sync(_p: *mut sqlite3_file, _flags: c_int) -> c_int {
    // Deliberately deferred; commit completion is tracked separately.
    SQLITE_OK
}

unsafe extern "C" fn io_file_size(p: *mut sqlite3_file, out: *mut i64) -> c_int {
    crate::guard(
        || {
            // SAFETY: `p`/`out` are live per contract.
            let file = unsafe { &mut *(p as *mut MurmurFile) };
            if file.main_db {
                let store = file.store.as_ref().expect("main db owns a store");
                unsafe { *out = lock(&store.mu).size as i64 };
            } else {
                let mem = file.mem.as_ref().expect("mem file owns a buffer");
                unsafe { *out = mem.lock().size as i64 };
            }
            SQLITE_OK
        },
        SQLITE_IOERR,
    )
}

unsafe extern "C" fn io_lock(p: *mut sqlite3_file, level: c_int) -> c_int {
    crate::guard(
        || {
            // SAFETY: `p` is a live file.
            let file = unsafe { &mut *(p as *mut MurmurFile) };
            if !file.main_db {
                return SQLITE_OK;
            }
            let me = file as *mut MurmurFile as usize;
            let store = file.store.clone().expect("main db owns a store");
            let mut guarded = lock(&store.mu);
            if level <= file.lock_level {
                return SQLITE_OK;
            }
            for (other_file, other) in guarded.locks.iter() {
                if *other_file == me {
                    continue;
                }
                if (level == SQLITE_LOCK_SHARED && *other >= SQLITE_LOCK_PENDING)
                    || (level >= SQLITE_LOCK_RESERVED
                        && level <= SQLITE_LOCK_PENDING
                        && *other >= SQLITE_LOCK_RESERVED)
                    || (level == SQLITE_LOCK_EXCLUSIVE && *other > SQLITE_LOCK_NONE)
                {
                    return SQLITE_BUSY;
                }
            }
            file.lock_level = level;
            guarded.locks.insert(me, level);
            SQLITE_OK
        },
        SQLITE_BUSY,
    )
}

unsafe extern "C" fn io_unlock(p: *mut sqlite3_file, level: c_int) -> c_int {
    crate::guard(
        || {
            // SAFETY: `p` is a live file.
            let file = unsafe { &mut *(p as *mut MurmurFile) };
            if !file.main_db {
                return SQLITE_OK;
            }
            let me = file as *mut MurmurFile as usize;
            let store = file.store.clone().expect("main db owns a store");
            file.lock_level = level;
            lock(&store.mu).locks.insert(me, level);
            SQLITE_OK
        },
        SQLITE_IOERR,
    )
}

unsafe extern "C" fn io_file_control(p: *mut sqlite3_file, op: c_int, arg: *mut c_void) -> c_int {
    crate::guard(
        || {
            // SAFETY: `p` is a live file; `arg` per-opcode by contract.
            let file = unsafe { &mut *(p as *mut MurmurFile) };
            if file.main_db && op == crate::abi::FCTL_EXTENSION && !arg.is_null() {
                if !file.extension.context.is_null() {
                    return SQLITE_MISUSE;
                }
                file.extension = unsafe { *(arg as *const sqlite_murmur_extension) };
                return SQLITE_OK;
            }
            if file.main_db && op == crate::abi::FCTL_FLUSH {
                let store = file.store.as_ref().expect("main db owns a store");
                return store.flush();
            }
            if file.main_db && op == crate::abi::FCTL_BACKUP && !arg.is_null() {
                let store = file.store.as_ref().expect("main db owns a store");
                let rc = store.flush();
                if rc != SQLITE_OK {
                    return rc;
                }
                let _backup = lock(&store.backup_mu);
                let args = unsafe { &*(arg as *const sqlite_murmur_backup_args) };
                return run_backup_on_store(store, args);
            }
            if file.main_db && op == crate::abi::FCTL_COMPACT {
                let store = file.store.as_ref().expect("main db owns a store");
                let rc = store.flush();
                if rc != SQLITE_OK {
                    return rc;
                }
                return run_compact_on_store(store);
            }
            if file.main_db && op == crate::abi::FCTL_KEYRING && !arg.is_null() {
                let store = file.store.as_ref().expect("main db owns a store");
                let args = unsafe { &mut *(arg as *mut sqlite_murmur_keyring_args) };
                return store.keyring_control(args, file.read_only);
            }
            if file.main_db && op == SQLITE_FCNTL_PRAGMA && !arg.is_null() {
                // SAFETY: PRAGMA args are `char *[4]` per SQLite contract.
                let arguments = unsafe { std::slice::from_raw_parts_mut(arg as *mut *mut c_char, 4) };
                let name = if arguments[1].is_null() {
                    None
                } else {
                    Some(unsafe { CStr::from_ptr(arguments[1]) }.to_bytes())
                };
                let value = if arguments[2].is_null() {
                    None
                } else {
                    Some(unsafe { CStr::from_ptr(arguments[2]) }.to_bytes())
                };
                let store = file.store.as_ref().expect("main db owns a store");
                if let Some(name) = name {
                    if name.len() >= 7 && name[..7].eq_ignore_ascii_case(b"murmur_") {
                        if let Some(pragma) = file.extension.pragma {
                            // SAFETY: installed by the owner via FCTL with
                            // the documented call contract.
                            return unsafe {
                                pragma(
                                    file.extension.context,
                                    arguments[1],
                                    arguments[2],
                                    arguments.as_mut_ptr(),
                                )
                            };
                        }
                        arguments[0] = crate::store_ctl::malloc_string(
                            "load murmur-replication before using murmur PRAGMAs",
                        );
                        return SQLITE_ERROR;
                    }
                    if name.eq_ignore_ascii_case(b"background_threads") {
                        let (rc, out) =
                            store.background_threads_pragma(value, file.read_only);
                        arguments[0] = out;
                        return rc;
                    }
                    if name.eq_ignore_ascii_case(b"compression") {
                        return pragma_compression(store, file.read_only, value, arguments);
                    }
                }
                if let Some(name) = name {
                    let (rc, out) = store.rocksdb_pragma(name, value, file.read_only);
                    if rc != SQLITE_NOTFOUND {
                        arguments[0] = out;
                        return rc;
                    }
                }
                if let Some(name) = name {
                    if name.eq_ignore_ascii_case(b"flush_interval_ms") {
                        return pragma_flush_interval(store, value, arguments);
                    }
                    if name.eq_ignore_ascii_case(b"cipher") {
                        return pragma_cipher(store, file.read_only, value, arguments);
                    }
                    if name.eq_ignore_ascii_case(b"journal_mode") {
                        if let Some(value) = value {
                            if !value.eq_ignore_ascii_case(b"delete") {
                                arguments[0] = crate::store_ctl::malloc_string(
                                    "sqlite-murmur requires journal_mode=DELETE",
                                );
                                return SQLITE_ERROR;
                            }
                        }
                    }
                    if name.eq_ignore_ascii_case(b"mmap_size") {
                        arguments[0] = crate::store_ctl::malloc_string("0");
                        return SQLITE_OK;
                    }
                }
            }
            if file.main_db && op == SQLITE_FCNTL_COMMIT_PHASETWO {
                let store = file.store.as_ref().expect("main db owns a store");
                return store.complete_transaction();
            }
            if op == SQLITE_FCNTL_MMAP_SIZE && !arg.is_null() {
                unsafe { *(arg as *mut i64) = 0 };
                return SQLITE_OK;
            }
            SQLITE_NOTFOUND
        },
        SQLITE_ERROR,
    )
}

/// `PRAGMA compression[=none|zstdN]`.
fn pragma_compression(
    store: &Store,
    read_only: bool,
    value: Option<&[u8]>,
    arguments: &mut [*mut c_char],
) -> c_int {
    use crate::store_ctl::malloc_string;
    let mut level = -1i32;
    if let Some(text) = value {
        if read_only {
            arguments[0] = malloc_string("cannot set compression on a read-only connection");
            return if arguments[0].is_null() { SQLITE_NOMEM } else { SQLITE_READONLY };
        }
        if text.eq_ignore_ascii_case(b"none") {
            level = 0;
        } else if text.len() >= 4 && text[..4].eq_ignore_ascii_case(b"zstd") {
            let max = crate::ffi_zstd::max_c_level() as u64;
            if let Some(parsed) = crate::store::parse_unsigned(&text[4..], max) {
                if parsed > 0 {
                    level = parsed as i32;
                }
            }
        }
        if level < 0 {
            let text = format!(
                "compression must be none or zstd1 through zstd{}",
                crate::ffi_zstd::max_c_level()
            );
            arguments[0] = malloc_string(&text);
            return if arguments[0].is_null() { SQLITE_NOMEM } else { SQLITE_ERROR };
        }
    }
    let level = store.compression_level(if value.is_some() { Some(level) } else { None });
    let text;
    arguments[0] = malloc_string(if level != 0 {
        text = format!("zstd{level}");
        &text
    } else {
        "none"
    });
    if arguments[0].is_null() { SQLITE_NOMEM } else { SQLITE_OK }
}

/// `PRAGMA flush_interval_ms[=N]` (0 normalizes to 250).
fn pragma_flush_interval(
    store: &Store,
    value: Option<&[u8]>,
    arguments: &mut [*mut c_char],
) -> c_int {
    use crate::store_ctl::malloc_string;
    let mut interval = 0u32;
    if let Some(text) = value {
        let mut valid = !text.is_empty();
        for &c in text {
            if !c.is_ascii_digit()
                || interval > (u32::MAX - u32::from(c - b'0')) / 10
            {
                valid = false;
                break;
            }
            interval = interval * 10 + u32::from(c - b'0');
        }
        if !valid {
            arguments[0] = malloc_string("flush_interval_ms requires an unsigned integer");
            return if arguments[0].is_null() { SQLITE_NOMEM } else { SQLITE_ERROR };
        }
        if interval == 0 {
            interval = 250;
        }
    }
    let current = store.flush_interval(if value.is_some() { Some(interval) } else { None });
    arguments[0] = malloc_string(&current.to_string());
    if arguments[0].is_null() { SQLITE_NOMEM } else { SQLITE_OK }
}

/// `PRAGMA cipher[=aes256gcm|aegis256]`.
fn pragma_cipher(
    store: &Store,
    read_only: bool,
    value: Option<&[u8]>,
    arguments: &mut [*mut c_char],
) -> c_int {
    use crate::store_ctl::malloc_string;
    if let Some(text) = value {
        if read_only {
            arguments[0] = malloc_string("cannot set cipher on a read-only connection");
            return if arguments[0].is_null() { SQLITE_NOMEM } else { SQLITE_READONLY };
        }
        let cipher = if text.eq_ignore_ascii_case(b"aes256gcm") {
            Cipher::Aes256Gcm
        } else if text.eq_ignore_ascii_case(b"aegis256") {
            #[cfg(feature = "aegis256")]
            {
                Cipher::Aegis256
            }
            #[cfg(not(feature = "aegis256"))]
            {
                arguments[0] = malloc_string(
                    "aegis256 requires a build with SQLITE_MURMUR_ENABLE_AEGIS256=ON",
                );
                return if arguments[0].is_null() { SQLITE_NOMEM } else { SQLITE_ERROR };
            }
        } else {
            arguments[0] = malloc_string("cipher must be aes256gcm or aegis256");
            return if arguments[0].is_null() { SQLITE_NOMEM } else { SQLITE_ERROR };
        };
        let rc = store.set_cipher(cipher);
        if rc != SQLITE_OK {
            arguments[0] = malloc_string(if rc == SQLITE_BUSY {
                "cipher can only be changed on an empty database outside a transaction"
            } else {
                "could not persist cipher setting"
            });
            return if arguments[0].is_null() { SQLITE_NOMEM } else { rc };
        }
    }
    arguments[0] = malloc_string(store.cipher_name());
    if arguments[0].is_null() { SQLITE_NOMEM } else { SQLITE_OK }
}

unsafe extern "C" fn io_check_reserved(p: *mut sqlite3_file, out: *mut c_int) -> c_int {
    crate::guard(
        || {
            // SAFETY: `p`/`out` are live per contract.
            let file = unsafe { &mut *(p as *mut MurmurFile) };
            unsafe { *out = 0 };
            if !file.main_db {
                return SQLITE_OK;
            }
            let store = file.store.as_ref().expect("main db owns a store");
            for level in lock(&store.mu).locks.values() {
                if *level >= SQLITE_LOCK_RESERVED {
                    unsafe { *out = 1 };
                    break;
                }
            }
            SQLITE_OK
        },
        SQLITE_IOERR,
    )
}

unsafe extern "C" fn io_sector_size(_p: *mut sqlite3_file) -> c_int {
    BLOCK as c_int
}

unsafe extern "C" fn io_device(_p: *mut sqlite3_file) -> c_int {
    0
}

static METHODS: sqlite3_io_methods = sqlite3_io_methods {
    i_version: 1,
    x_close: Some(io_close),
    x_read: Some(io_read),
    x_write: Some(io_write),
    x_truncate: Some(io_truncate),
    x_sync: Some(io_sync),
    x_file_size: Some(io_file_size),
    x_lock: Some(io_lock),
    x_unlock: Some(io_unlock),
    x_check_reserved_lock: Some(io_check_reserved),
    x_file_control: Some(io_file_control),
    x_sector_size: Some(io_sector_size),
    x_device_characteristics: Some(io_device),
    x_shm_map: None,
    x_shm_lock: None,
    x_shm_barrier: None,
    x_shm_unmap: None,
    x_fetch: None,
    x_unfetch: None,
};

unsafe extern "C" fn vfs_open(
    _vfs: *mut sqlite3_vfs,
    z_name: *const c_char,
    out: *mut sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    crate::guard(
        || {
            // SAFETY: SQLite passes `szOsFile` bytes at `out` (single
            // open per slot) and a borrowed-or-null `z_name`.
            let file = unsafe { &mut *(out as *mut MurmurFile) };
            unsafe { std::ptr::write(file, MurmurFile::new()) };
            let done = (|| {
                file.main_db = flags & SQLITE_OPEN_MAIN_DB != 0;
                file.delete_on_close = flags & SQLITE_OPEN_DELETEONCLOSE != 0;
                file.name = if z_name.is_null() {
                    let id = TEMP_ID.fetch_add(1, Ordering::Relaxed) + 1;
                    format!(":sqlite-murmur-temp:{id}").into_bytes()
                } else {
                    unsafe { CStr::from_ptr(z_name) }.to_bytes().to_vec()
                };
                if file.main_db {
                    file.read_only = flags & SQLITE_OPEN_READONLY != 0;
                    let mut globals = lock(&GLOBALS);
                    // Vacant entry first (mirrors the C++ `auto &weak =
                    // gStores[name]`): on start failure an expired weak
                    // remains in the map, handled by every lookup.
                    globals.stores.entry(file.name.clone()).or_insert_with(Weak::new);
                    file.store = globals
                        .stores
                        .get(&file.name)
                        .and_then(Weak::upgrade);
                    if file.store.is_none() {
                        let settings = globals.settings.clone();
                        let store = Arc::new(Store::new(settings));
                        let dir = std::path::Path::new(std::ffi::OsStr::from_bytes(&file.name));
                        if !store.start(dir, flags & SQLITE_OPEN_CREATE != 0) {
                            return SQLITE_CANTOPEN;
                        }
                        globals.stores.insert(file.name.clone(), Arc::downgrade(&store));
                        file.store = Some(store);
                    }
                } else {
                    if file.name.len() >= 4 && &file.name[file.name.len() - 4..] == b"-wal" {
                        // WAL requires a different recovery design.
                        return SQLITE_CANTOPEN;
                    }
                    let mut globals = lock(&GLOBALS);
                    let mem = match globals.mem_files.get(&file.name).cloned() {
                        Some(existing) => existing,
                        None => {
                            let owner = if flags & SQLITE_OPEN_MAIN_JOURNAL != 0 {
                                find_store_for_journal_locked(&globals, &file.name)
                            } else {
                                None
                            };
                            let fresh = match owner {
                                Some(store) => store.journal_pool.acquire_shared(),
                                None => {
                                    // Standalone buffer (never pooled):
                                    // build outside the map dance via a
                                    // one-off pool whose last release
                                    // simply drops the buffer.
                                    use crate::pool::{JournalPool, ReusePool};
                                    let solo: Arc<JournalPool> =
                                        Arc::new(ReusePool::new(0));
                                    solo.acquire_shared()
                                }
                            };
                            globals.mem_files.insert(file.name.clone(), fresh.clone());
                            fresh
                        }
                    };
                    file.mem = Some(mem);
                }
                file.base.p_methods = &METHODS;
                if !out_flags.is_null() {
                    unsafe { *out_flags = flags };
                }
                SQLITE_OK
            })();
            if done != SQLITE_OK {
                // SAFETY: placement-constructed above; balanced here.
                unsafe { std::ptr::drop_in_place(file) };
            }
            done
        },
        SQLITE_NOMEM,
    )
}

unsafe extern "C" fn vfs_delete(_vfs: *mut sqlite3_vfs, z_name: *const c_char, _sync: c_int) -> c_int {
    crate::guard(
        || {
            if z_name.is_null() {
                return SQLITE_IOERR_DELETE;
            }
            // SAFETY: borrowed NUL-terminated name per contract.
            let name = unsafe { CStr::from_ptr(z_name) }.to_bytes();
            let (removed, commit) = {
                let mut globals = lock(&GLOBALS);
                let removed = globals.mem_files.remove(name);
                let commit = find_store_for_journal_locked(&globals, name);
                (removed, commit)
            };
            // Last-reference cleansing/pooling must not hold the VFS mutex.
            drop(removed);
            if let Some(store) = commit {
                return store.complete_transaction();
            }
            // Never delete an open RocksDB directory via SQLite xDelete.
            SQLITE_OK
        },
        SQLITE_IOERR_DELETE,
    )
}

unsafe extern "C" fn vfs_access(
    _vfs: *mut sqlite3_vfs,
    z_name: *const c_char,
    flags: c_int,
    out: *mut c_int,
) -> c_int {
    crate::guard(
        || {
            // SAFETY: `out` is live; `z_name` borrowed-or-null.
            if z_name.is_null() {
                unsafe { *out = 0 };
                return SQLITE_OK;
            }
            let name = unsafe { CStr::from_ptr(z_name) }.to_bytes();
            {
                let globals = lock(&GLOBALS);
                if globals.mem_files.contains_key(name) {
                    unsafe { *out = 1 };
                    return SQLITE_OK;
                }
                let suffix = if is_journal(name) {
                    8
                } else if name.len() >= 4 && &name[name.len() - 4..] == b"-wal" {
                    4
                } else {
                    0
                };
                if suffix != 0 {
                    // A registered main database may itself end in
                    // -journal or -wal.
                    let main_live = globals
                        .stores
                        .get(name)
                        .is_some_and(|weak| weak.strong_count() > 0);
                    if !main_live {
                        let base = &name[..name.len() - suffix];
                        let owner_live = globals
                            .stores
                            .get(base)
                            .is_some_and(|weak| weak.strong_count() > 0);
                        if owner_live {
                            // DELETE journals exist only in RAM, and WAL
                            // is unsupported.
                            unsafe { *out = 0 };
                            return SQLITE_OK;
                        }
                    }
                }
            }
            // Real filesystem checks for main databases and unowned
            // paths, outside the global mutex. (`Path::exists` is false
            // on any error, matching `!ec && present`.)
            let present = std::path::Path::new(std::ffi::OsStr::from_bytes(name)).exists();
            let mut result = c_int::from(present);
            if flags == SQLITE_ACCESS_READWRITE {
                result = result & c_int::from(present);
            }
            unsafe { *out = result };
            SQLITE_OK
        },
        SQLITE_IOERR,
    )
}

unsafe extern "C" fn vfs_full_pathname(
    _vfs: *mut sqlite3_vfs,
    name: *const c_char,
    n: c_int,
    out: *mut c_char,
) -> c_int {
    crate::guard(
        || {
            let parent = GPARENT.load(Ordering::Relaxed);
            if parent.is_null() {
                return SQLITE_MISUSE;
            }
            // SAFETY: parent VFS is live while registered; args per contract.
            unsafe {
                match (*parent).x_full_pathname {
                    Some(f) => f(parent, name, n, out),
                    None => SQLITE_MISUSE,
                }
            }
        },
        SQLITE_CANTOPEN,
    )
}

macro_rules! delegate {
    ($name:ident ($($arg:ident : $ty:ty),*) -> $ret:ty, $field:ident, $fallback:expr) => {
        unsafe extern "C" fn $name(_vfs: *mut sqlite3_vfs, $($arg: $ty),*) -> $ret {
            crate::guard(
                || {
                    let parent = GPARENT.load(Ordering::Relaxed);
                    if parent.is_null() {
                        return $fallback;
                    }
                    // SAFETY: see `vfs_full_pathname`.
                    unsafe {
                        match (*parent).$field {
                            Some(f) => f(parent, $($arg),*),
                            None => $fallback,
                        }
                    }
                },
                $fallback,
            )
        }
    };
}

delegate!(vfs_dl_open(z: *const c_char) -> *mut c_void, x_dl_open, std::ptr::null_mut());
delegate!(vfs_randomness(n: c_int, out: *mut c_char) -> c_int, x_randomness, SQLITE_ERROR);
delegate!(vfs_sleep(n: c_int) -> c_int, x_sleep, SQLITE_ERROR);
delegate!(vfs_current_time(out: *mut c_double) -> c_int, x_current_time, SQLITE_ERROR);

unsafe extern "C" fn vfs_dl_error(_vfs: *mut sqlite3_vfs, n: c_int, z: *mut c_char) {
    let parent = GPARENT.load(Ordering::Relaxed);
    if parent.is_null() {
        return;
    }
    // SAFETY: see `vfs_full_pathname`.
    unsafe {
        if let Some(f) = (*parent).x_dl_error {
            f(parent, n, z);
        }
    }
}

unsafe extern "C" fn vfs_dl_sym(
    _vfs: *mut sqlite3_vfs,
    handle: *mut c_void,
    z: *const c_char,
) -> crate::ffi_sqlite::sqlite3_syscall_ptr {
    crate::guard(
        || {
            let parent = GPARENT.load(Ordering::Relaxed);
            if parent.is_null() {
                return None;
            }
            // SAFETY: see `vfs_full_pathname`.
            unsafe {
                match (*parent).x_dl_sym {
                    Some(f) => f(parent, handle, z),
                    None => None,
                }
            }
        },
        None,
    )
}

unsafe extern "C" fn vfs_dl_close(_vfs: *mut sqlite3_vfs, handle: *mut c_void) {
    let parent = GPARENT.load(Ordering::Relaxed);
    if parent.is_null() {
        return;
    }
    // SAFETY: see `vfs_full_pathname`.
    unsafe {
        if let Some(f) = (*parent).x_dl_close {
            f(parent, handle);
        }
    }
}

unsafe extern "C" fn vfs_get_last_error(_vfs: *mut sqlite3_vfs, n: c_int, out: *mut c_char) -> c_int {
    crate::guard(
        || {
            let parent = GPARENT.load(Ordering::Relaxed);
            if parent.is_null() {
                return 0;
            }
            // SAFETY: see `vfs_full_pathname`.
            unsafe {
                match (*parent).x_get_last_error {
                    Some(f) => f(parent, n, out),
                    None => 0,
                }
            }
        },
        0,
    )
}

// The open/close pair lives in `murmur-storage-api` (separate staticlib
// for link granularity); resolved at final link like any C symbol.
unsafe extern "C" {
    fn sqlite_murmur_open_v2(
        path: *const c_char,
        db: *mut *mut sqlite3,
        flags: c_int,
        opts: *const sqlite_murmur_open_options,
    ) -> c_int;
    fn sqlite_murmur_close(db: *mut sqlite3) -> c_int;
}

fn lookup_store(path: *const c_char) -> Result<Arc<Store>, c_int> {
    if path.is_null() {
        return Err(SQLITE_MISUSE);
    }
    let globals = lock(&GLOBALS);
    if !globals.registered {
        return Err(SQLITE_MISUSE);
    }
    let mut full = vec![0u8; globals.mx_pathname as usize + 1];
    let parent = GPARENT.load(Ordering::Relaxed);
    // SAFETY: parent is live while registered; buffer sized per contract.
    let rc = unsafe {
        match (*parent).x_full_pathname {
            // SAFETY: `full` has `mxPathname + 1` bytes.
            Some(f) => f(
                parent,
                path,
                full.len() as c_int,
                full.as_mut_ptr() as *mut c_char,
            ),
            None => SQLITE_MISUSE,
        }
    };
    if rc != SQLITE_OK {
        return Err(SQLITE_CANTOPEN);
    }
    let key = unsafe { CStr::from_ptr(full.as_ptr() as *const c_char) }.to_bytes();
    match globals.stores.get(key).and_then(Weak::upgrade) {
        Some(store) => Ok(store),
        None => Err(SQLITE_NOTFOUND),
    }
}

unsafe extern "C" fn store_checkpoint(
    ctx: *mut c_void,
    dest: *const c_char,
    seq: *mut u64,
) -> c_int {
    // SAFETY: `ctx` is the store's live `Db`; `dest`/`seq` per the
    // backup contract (non-null, borrowed for the call).
    let db = unsafe { &*(ctx as *const crate::ffi_rocksdb::Db) };
    let dir = unsafe { CStr::from_ptr(dest) };
    let path = std::path::Path::new(std::ffi::OsStr::from_bytes(dir.to_bytes()));
    match db.checkpoint_to(path) {
        Ok(s) => {
            unsafe { *seq = s };
            SQLITE_OK
        }
        Err(_) => SQLITE_IOERR_WRITE,
    }
}

unsafe extern "C" fn store_compact(ctx: *mut c_void) -> c_int {
    // SAFETY: `ctx` is the store's live `Db`.
    let db = unsafe { &*(ctx as *const crate::ffi_rocksdb::Db) };
    match db.compact_full_range() {
        Ok(()) => SQLITE_OK,
        Err(_) => SQLITE_IOERR_WRITE,
    }
}

fn run_backup_on_store(store: &Store, args: &sqlite_murmur_backup_args) -> c_int {
    if args.dest.is_null() {
        return SQLITE_MISUSE;
    }
    // SAFETY: `dest` is borrowed for the call; the `Db` outlives it.
    let dest = unsafe { CStr::from_ptr(args.dest) };
    let dest_path = std::path::Path::new(std::ffi::OsStr::from_bytes(dest.to_bytes()));
    let ctx = store.db() as *const crate::ffi_rocksdb::Db as *mut c_void;
    crate::backup::backup_run(
        dest_path,
        args.dest,
        store.cipher_name().as_bytes(),
        &args.group,
        &args.node,
        None,
        ctx,
        store_checkpoint,
    )
}

fn run_compact_on_store(store: &Store) -> c_int {
    // SAFETY: the `Db` outlives the call.
    let ctx = store.db() as *const crate::ffi_rocksdb::Db as *mut c_void;
    crate::backup::compact_run(ctx, store_compact)
}

/// Register the `sqlite-murmur` VFS (non-default) and global settings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite_murmur_register(config: *const sqlite_murmur_config) -> c_int {
    crate::guard(
        || {
            if config.is_null() {
                return SQLITE_MISUSE;
            }
            // SAFETY: borrowed config per contract.
            let config = unsafe { &*config };
            if config.key.is_null() {
                return SQLITE_MISUSE;
            }
            let mut globals = lock(&GLOBALS);
            if globals.registered {
                return SQLITE_MISUSE;
            }
            #[cfg(feature = "aegis256")]
            {
                static AEGIS_ONCE: std::sync::Once = std::sync::Once::new();
                static AEGIS_STATUS: std::sync::atomic::AtomicI32 =
                    std::sync::atomic::AtomicI32::new(-1);
                AEGIS_ONCE.call_once(|| {
                    // SAFETY: libaegis init, once per process.
                    let status = unsafe { crate::ffi_aegis::aegis_init() };
                    AEGIS_STATUS.store(status, Ordering::Relaxed);
                });
                if AEGIS_STATUS.load(Ordering::Relaxed) < 0 {
                    return SQLITE_ERROR;
                }
            }
            // SAFETY: `vfs_find(NULL)` returns the default VFS or null.
            let parent = unsafe { crate::ffi_sqlite::sqlite3_vfs_find(std::ptr::null()) };
            if parent.is_null() {
                return SQLITE_ERROR;
            }
            // SAFETY: 32 borrowed key bytes per contract.
            let key = unsafe { *(config.key as *const [u8; 32]) };
            globals.settings = Settings::new(
                key,
                config.flush_interval_ms,
                config.cache_pages,
                config.rocksdb_sync != 0,
            );
            GPARENT.store(parent, Ordering::Relaxed);
            // SAFETY: stable leaked struct, exclusively rewritten here
            // under the global mutex (register is serialized; SQLite
            // only reads it after `vfs_register` below returns).
            let vfs = unsafe { &mut *gvfs() };
            *vfs = empty_vfs();
            vfs.i_version = 1;
            vfs.sz_os_file = std::mem::size_of::<MurmurFile>() as c_int;
            // SAFETY: parent is live (just found).
            vfs.mx_pathname = unsafe { (*parent).mx_pathname };
            vfs.z_name = VFS_NAME.as_ptr() as *const c_char;
            vfs.x_open = Some(vfs_open);
            vfs.x_delete = Some(vfs_delete);
            vfs.x_access = Some(vfs_access);
            vfs.x_full_pathname = Some(vfs_full_pathname);
            vfs.x_dl_open = Some(vfs_dl_open);
            vfs.x_dl_error = Some(vfs_dl_error);
            vfs.x_dl_sym = Some(vfs_dl_sym);
            vfs.x_dl_close = Some(vfs_dl_close);
            vfs.x_randomness = Some(vfs_randomness);
            vfs.x_sleep = Some(vfs_sleep);
            vfs.x_current_time = Some(vfs_current_time);
            vfs.x_get_last_error = Some(vfs_get_last_error);
            globals.mx_pathname = vfs.mx_pathname;
            // SAFETY: stable pointer, valid VFS struct.
            let rc = unsafe { crate::ffi_sqlite::sqlite3_vfs_register(gvfs(), 0) };
            globals.registered = rc == SQLITE_OK;
            rc
        },
        SQLITE_ERROR,
    )
}

/// Flush one open store by path.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite_murmur_flush(path: *const c_char) -> c_int {
    crate::guard(
        || match lookup_store(path) {
            Ok(store) => store.flush(),
            Err(rc) => rc,
        },
        SQLITE_ERROR,
    )
}

/// Snapshot one open store's counters by path.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite_murmur_get_stats(
    path: *const c_char,
    out: *mut sqlite_murmur_stats,
) -> c_int {
    crate::guard(
        || {
            if out.is_null() {
                return SQLITE_MISUSE;
            }
            match lookup_store(path) {
                // SAFETY: `out` is a live struct per contract.
                Ok(store) => store.get_stats(unsafe { &mut *out }),
                Err(rc) => rc,
            }
        },
        SQLITE_ERROR,
    )
}

/// Flush + memtable flush + 30 s compaction wait for one open store.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite_murmur_checkpoint(path: *const c_char) -> c_int {
    crate::guard(
        || match lookup_store(path) {
            Ok(store) => store.checkpoint(),
            Err(rc) => rc,
        },
        SQLITE_ERROR,
    )
}

/// Offline repair (refuses missing dirs and live same-process stores).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite_murmur_repair(path: *const c_char) -> c_int {
    crate::guard(
        || {
            if path.is_null() {
                return SQLITE_MISUSE;
            }
            // SAFETY: borrowed NUL-terminated path per contract.
            let bytes = unsafe { CStr::from_ptr(path) }.to_bytes();
            if bytes.is_empty() {
                return SQLITE_MISUSE;
            }
            let dir = std::path::Path::new(std::ffi::OsStr::from_bytes(bytes));
            if !dir.is_dir() {
                return SQLITE_CANTOPEN;
            }
            if lookup_store(path).is_ok() {
                return SQLITE_BUSY;
            }
            // Same options recipe as `Store::start`, minus
            // create_if_missing and statistics: repair never initializes
            // a missing store and reads no tickers.
            let (options, _cache) = StoreOptions::for_store(
                false,
                false,
                crate::store::ROCKS_BLOCK_CACHE_BYTES,
            );
            match repair_db(dir, &options) {
                Ok(()) => SQLITE_OK,
                Err(_) => SQLITE_IOERR,
            }
        },
        SQLITE_IOERR,
    )
}

/// Offline key inventory: temp-open STORAGE_ONLY read-only and reuse the
/// INVENTORY op, so the JSON stays single-sourced.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite_murmur_key_inventory(
    path: *const c_char,
    json_out: *mut *mut c_char,
) -> c_int {
    crate::guard(
        || {
            if path.is_null() || json_out.is_null() {
                return SQLITE_MISUSE;
            }
            // SAFETY: borrowed path per contract; `json_out` is live.
            unsafe { *json_out = std::ptr::null_mut() };
            let bytes = unsafe { CStr::from_ptr(path) }.to_bytes();
            if bytes.is_empty() {
                return SQLITE_MISUSE;
            }
            if !std::path::Path::new(std::ffi::OsStr::from_bytes(bytes)).is_dir() {
                return SQLITE_CANTOPEN;
            }
            if lookup_store(path).is_ok() {
                return SQLITE_BUSY;
            }
            let opts = sqlite_murmur_open_options {
                flags: OPEN_STORAGE_ONLY,
                cipher: std::ptr::null(),
                compression: std::ptr::null(),
            };
            let mut db: *mut sqlite3 = std::ptr::null_mut();
            // SAFETY: API-crate contract; `db` is a live out-param.
            let rc = unsafe {
                sqlite_murmur_open_v2(
                    path,
                    &mut db,
                    SQLITE_OPEN_READONLY | SQLITE_OPEN_FULLMUTEX,
                    &opts,
                )
            };
            if rc != SQLITE_OK || db.is_null() {
                if !db.is_null() {
                    // SAFETY: live handle per contract.
                    unsafe { sqlite_murmur_close(db) };
                }
                return SQLITE_CANTOPEN;
            }
            let mut args = sqlite_murmur_keyring_args {
                op: crate::abi::KEYRING_INVENTORY,
                param: 0,
                value: 0,
                sealed: 0,
                kdf: 0,
                time: 0,
                memory_kib: 0,
                threads: 0,
                json: std::ptr::null_mut(),
            };
            // SAFETY: live handle; `"main"` is static.
            let frc = unsafe {
                crate::ffi_sqlite::sqlite3_file_control(
                    db,
                    c"main".as_ptr(),
                    crate::abi::FCTL_KEYRING,
                    &mut args as *mut _ as *mut c_void,
                )
            };
            // SAFETY: live handle per contract.
            unsafe { sqlite_murmur_close(db) };
            if frc != SQLITE_OK {
                if !args.json.is_null() {
                    // SAFETY: malloc'd by the control call.
                    unsafe { crate::ffi_sqlite::sqlite3_free(args.json as *mut c_void) };
                }
                return frc;
            }
            if args.json.is_null() {
                return SQLITE_CORRUPT;
            }
            unsafe { *json_out = args.json };
            SQLITE_OK
        },
        SQLITE_ERROR,
    )
}

/// Offline full-store rekey (staged envelope + chunked rewrite + flip).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite_murmur_rekey(
    path: *const c_char,
    old_master: *const c_uchar,
    new_master: *const c_uchar,
    opts: *const sqlite_murmur_rekey_options,
) -> c_int {
    crate::guard(
        || {
            if path.is_null() || old_master.is_null() || new_master.is_null() {
                return SQLITE_MISUSE;
            }
            // SAFETY: borrowed args per contract.
            let bytes = unsafe { CStr::from_ptr(path) }.to_bytes();
            if bytes.is_empty() {
                return SQLITE_MISUSE;
            }
            let dir = std::path::Path::new(std::ffi::OsStr::from_bytes(bytes));
            if !dir.is_dir() {
                return SQLITE_CANTOPEN;
            }
            if lookup_store(path).is_ok() {
                return SQLITE_BUSY;
            }
            let (options, _cache) =
                StoreOptions::for_store(false, false, crate::store::ROCKS_BLOCK_CACHE_BYTES);
            let db = match crate::ffi_rocksdb::Db::open(dir, &options) {
                Ok(db) => db,
                Err(msg) if is_busy_or_retry(&msg) => return SQLITE_BUSY,
                Err(_) => return SQLITE_CANTOPEN,
            };
            let old_master = unsafe { &*(old_master as *const [u8; 32]) };
            let new_master = unsafe { &*(new_master as *const [u8; 32]) };
            let ro = ReadOptions::new();
            let mut cipher = Cipher::Aes256Gcm;
            // A failed meta read (missing key included) keeps the AES
            // default, exactly like the C++.
            if let Ok(Some(cipher_bytes)) = db.get(&ro, META_CIPHER) {
                if cipher_bytes == b"aegis256" {
                    #[cfg(feature = "aegis256")]
                    {
                        cipher = Cipher::Aegis256;
                    }
                    #[cfg(not(feature = "aegis256"))]
                    {
                        return SQLITE_ERROR;
                    }
                } else if cipher_bytes != b"aes256gcm" {
                    return SQLITE_CORRUPT;
                }
            }
            // R2: clean refusal for unsealed stores.
            let current_env = match db.get(&ro, META_KEYRING) {
                Ok(Some(env)) => env,
                _ => return SQLITE_ERROR,
            };
            let store_id_bytes = match db.get(&ro, META_STORE_ID) {
                Ok(Some(id)) if id.len() == 16 => id,
                _ => return SQLITE_CORRUPT,
            };
            let mut binding = [0u8; 16];
            binding.copy_from_slice(&store_id_bytes);
            let old_ring = match crate::keyring::open(
                &current_env,
                Some(old_master),
                None,
                &binding,
                &binding,
            ) {
                Ok(ring) => ring,
                Err(rc) => return rc,
            };
            if old_ring.kdf != crate::abi::KDF_DIRECT_HKDF {
                return SQLITE_ERROR;
            }
            let (old_current_id, old_current) = match crate::store::keyring_current(&old_ring) {
                Some(pair) => pair,
                None => return SQLITE_CORRUPT,
            };
            if old_current_id == 0 {
                return SQLITE_CORRUPT;
            }
            let mut old_keys = crate::store::PageKeys {
                page: [0u8; 32],
                master: [0u8; 32],
                active: [0u8; 32],
                current: old_current_id,
                sealed: true,
                retired: Vec::new(),
            };
            old_keys.page.copy_from_slice(old_current);
            old_keys.active.copy_from_slice(old_current);
            old_keys.master.copy_from_slice(old_master);
            for entry in old_ring.keys.iter() {
                if !entry.active {
                    old_keys.retired.push((entry.id, entry.key));
                }
            }
            let write_opts = WriteOptions::with_sync(true);
            let staged_env = match db.get(&ro, META_KEYRING_REKEY) {
                Ok(Some(env)) if !env.is_empty() => env,
                _ => {
                    let env = match crate::keyring::create(
                        old_ring.kdf,
                        old_ring.argon_time,
                        old_ring.argon_memory_kib,
                        old_ring.argon_threads,
                        &old_ring.wrap_id,
                        &binding,
                        &binding,
                        Some(new_master),
                        None,
                    ) {
                        Ok(env) => env,
                        Err(rc) => return rc,
                    };
                    if db.put(&write_opts, META_KEYRING_REKEY, &env).is_err() {
                        return SQLITE_IOERR_WRITE;
                    }
                    env
                }
            };
            let new_ring = match crate::keyring::open(
                &staged_env,
                Some(new_master),
                None,
                &binding,
                &binding,
            ) {
                Ok(ring) => ring,
                Err(_) => return SQLITE_CORRUPT,
            };
            let (new_active_id, new_active) = match crate::store::keyring_current(&new_ring) {
                Some(pair) => pair,
                None => return SQLITE_CORRUPT,
            };
            if new_active_id == 0 {
                return SQLITE_CORRUPT;
            }
            let mut new_active_key = zeroize::Zeroizing::new([0u8; 32]);
            new_active_key.copy_from_slice(new_active);
            let mut workspace = crate::crypto::DecodeWorkspace::new();
            let mut batch = WriteBatch::new();
            let mut chunk_count = 0;
            let mut total_written = 0;
            let mut cctx = crate::ffi_zstd::Cctx::create();
            let mut scratch = [0u8; crate::crypto::SCRATCH_SIZE];
            let fault_after = if opts.is_null() {
                0
            } else {
                // SAFETY: borrowed options per contract.
                unsafe { (*opts).fault_after_pages }
            };
            let mut it = db.iterator(&ro);
            it.seek(&crate::crypto::page_key(0));
            while it.valid() {
                let key = it.key();
                if key.len() != 9 || key[0] != b'p' {
                    break;
                }
                let mut page = 0u64;
                for byte in key[1..9].iter() {
                    page = (page << 8) | u64::from(*byte);
                }
                let record = it.value();
                let mut block = [0u8; BLOCK];
                // New-key trial second: already-rewritten records (from
                // an interrupted rekey) decrypt under the new key, so
                // reruns converge instead of failing CORRUPT.
                let mut decrypted = crate::store::decrypt_page(
                    cipher,
                    page,
                    &record,
                    &mut block,
                    &mut workspace,
                    &old_keys,
                );
                if !decrypted {
                    decrypted = crate::crypto::decrypt(
                        cipher,
                        &new_active_key,
                        page,
                        &record,
                        &mut block,
                        &mut workspace,
                    );
                }
                if !decrypted {
                    return SQLITE_CORRUPT;
                }
                let was_keyed = crate::crypto::record_key_id(&record).is_some();
                let mut n = BLOCK;
                if let Some(cctx) = cctx.as_mut() {
                    match cctx.compress(&mut scratch, &block, 3) {
                        Ok(size) => n = size,
                        Err(_) => n = BLOCK,
                    }
                }
                let compressed = cctx.is_some() && n < BLOCK;
                let payload = if compressed { &scratch[..n] } else { &block[..] };
                let target_key_id = if was_keyed { new_active_id } else { 0 };
                let new_record = match crate::crypto::encrypt(
                    cipher,
                    &new_active_key,
                    page,
                    payload,
                    compressed,
                    target_key_id,
                ) {
                    Some(record) => record,
                    None => return SQLITE_IOERR_WRITE,
                };
                batch.put(key, &new_record);
                // The batch holds only the re-encrypted record: cleanse
                // plaintext before the next page.
                crate::secure_wipe(&mut block);
                crate::secure_wipe(&mut scratch);
                chunk_count += 1;
                total_written += 1;
                if fault_after > 0 && total_written >= fault_after {
                    let _ = db.write(&write_opts, &mut batch);
                    return SQLITE_IOERR_WRITE;
                }
                if chunk_count >= 100 {
                    if db.write(&write_opts, &mut batch).is_err() {
                        return SQLITE_IOERR_WRITE;
                    }
                    batch.clear();
                    chunk_count = 0;
                }
                it.next();
            }
            // NOTE: the C++ never checks the iterator status here either
            // (a mid-iteration IO error ends the loop silently). Mirrored
            // as-is; see the module docs on deliberate divergences.
            if chunk_count > 0 {
                if db.write(&write_opts, &mut batch).is_err() {
                    return SQLITE_IOERR_WRITE;
                }
                batch.clear();
            }
            let mut flip = WriteBatch::new();
            flip.put(META_KEYRING, &staged_env);
            flip.delete(META_KEYRING_REKEY);
            if db.write(&write_opts, &mut flip).is_err() {
                return SQLITE_IOERR_WRITE;
            }
            SQLITE_OK
        },
        SQLITE_IOERR_WRITE,
    )
}

/// Install the group file key (+ optional rotation previous). Once only.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite_murmur_register_file_key(
    current: *const c_uchar,
    previous: *const c_uchar,
) -> c_int {
    crate::guard(
        || {
            if current.is_null() {
                return SQLITE_MISUSE;
            }
            let mut globals = lock(&GLOBALS);
            if globals.file_key_set {
                return SQLITE_MISUSE;
            }
            // SAFETY: 32 borrowed bytes per contract.
            unsafe { std::ptr::copy_nonoverlapping(current, globals.file_key.as_mut_ptr(), 32) };
            if !previous.is_null() {
                // SAFETY: same contract.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        previous,
                        globals.file_key_previous.as_mut_ptr(),
                        32,
                    )
                };
                globals.file_key_previous_set = true;
            }
            globals.file_key_set = true;
            SQLITE_OK
        },
        SQLITE_ERROR,
    )
}

/// Read back the current group file key (replication layer, once).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite_murmur_file_key_current(out: *mut c_uchar) -> c_int {
    crate::guard(
        || {
            if out.is_null() {
                return SQLITE_MISUSE;
            }
            let globals = lock(&GLOBALS);
            if !globals.file_key_set {
                return SQLITE_NOTFOUND;
            }
            // SAFETY: 32-byte out-param per contract.
            unsafe { std::ptr::copy_nonoverlapping(globals.file_key.as_ptr(), out, 32) };
            SQLITE_OK
        },
        SQLITE_ERROR,
    )
}

/// Read back the previous group file key (replication layer, once).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite_murmur_file_key_previous(out: *mut c_uchar) -> c_int {
    crate::guard(
        || {
            if out.is_null() {
                return SQLITE_MISUSE;
            }
            let globals = lock(&GLOBALS);
            if !globals.file_key_previous_set {
                return SQLITE_NOTFOUND;
            }
            // SAFETY: 32-byte out-param per contract.
            unsafe { std::ptr::copy_nonoverlapping(globals.file_key_previous.as_ptr(), out, 32) };
            SQLITE_OK
        },
        SQLITE_ERROR,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi_sqlite::{
        SQLITE_DONE, SQLITE_OPEN_READWRITE, SQLITE_ROW, sqlite3_close, sqlite3_column_text,
        sqlite3_errmsg, sqlite3_exec, sqlite3_file_control, sqlite3_finalize, sqlite3_free,
        sqlite3_open_v2, sqlite3_prepare_v2, sqlite3_step, sqlite3_stmt,
    };
    use std::ffi::CString;

    fn exec(db: *mut sqlite3, sql: &str) {
        let sql = CString::new(sql).unwrap();
        let mut errmsg: *mut c_char = std::ptr::null_mut();
        // SAFETY: live handle; borrowed SQL; out-param for the message.
        let rc = unsafe { sqlite3_exec(db, sql.as_ptr(), None, std::ptr::null_mut(), &mut errmsg) };
        if rc != SQLITE_OK {
            let msg = if errmsg.is_null() {
                String::new()
            } else {
                let text = unsafe { CStr::from_ptr(errmsg) }.to_string_lossy().into_owned();
                unsafe { sqlite3_free(errmsg as *mut c_void) };
                text
            };
            panic!("exec {sql:?} failed: {msg}");
        }
    }

    fn query1(db: *mut sqlite3, sql: &str) -> String {
        let sql = CString::new(sql).unwrap();
        let mut stmt: *mut sqlite3_stmt = std::ptr::null_mut();
        // SAFETY: live handle; borrowed SQL; out-param for the statement.
        let rc = unsafe { sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, std::ptr::null_mut()) };
        assert_eq!(rc, SQLITE_OK, "prepare {sql:?}: {}", unsafe {
            CStr::from_ptr(sqlite3_errmsg(db)).to_string_lossy()
        });
        // SAFETY: live statement.
        let rc = unsafe { sqlite3_step(stmt) };
        assert_eq!(rc, SQLITE_ROW);
        // SAFETY: row is live; column 0 of these queries is non-null text.
        let text = unsafe { CStr::from_ptr(sqlite3_column_text(stmt, 0) as *const c_char) }
            .to_string_lossy()
            .into_owned();
        assert_eq!(unsafe { sqlite3_step(stmt) }, SQLITE_DONE);
        assert_eq!(unsafe { sqlite3_finalize(stmt) }, SQLITE_OK);
        text
    }

    /// Full VFS loop through real SQLite: register, open with the
    /// `sqlite-murmur` VFS, SQL writes/reads, PRAGMAs, FCTL flush,
    /// close, reopen for persistence, unregister. (The only test that
    /// touches the process-global registry; parallel-safe by
    /// construction since no other test registers.)
    #[test]
    fn vfs_end_to_end_through_sqlite() {
        let dir = std::env::temp_dir().join(format!("murmur-vfs-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let path = CString::new(dir.as_os_str().as_encoded_bytes()).unwrap();
        let mut key = [0u8; 32];
        for (i, b) in key.iter_mut().enumerate() {
            *b = i as u8;
        }
        let config = sqlite_murmur_config {
            key: key.as_ptr(),
            flush_interval_ms: 50,
            cache_pages: 64,
            rocksdb_sync: 1,
        };
        // SAFETY: borrowed config per contract.
        assert_eq!(unsafe { sqlite_murmur_register(&config) }, SQLITE_OK);
        // Second register refuses.
        assert_eq!(unsafe { sqlite_murmur_register(&config) }, SQLITE_MISUSE);

        let vfs_name = CString::new("sqlite-murmur").unwrap();
        let mut db: *mut sqlite3 = std::ptr::null_mut();
        // SAFETY: borrowed path; live out-param; known VFS name.
        let rc = unsafe {
            sqlite3_open_v2(
                path.as_ptr(),
                &mut db,
                SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
                vfs_name.as_ptr(),
            )
        };
        assert_eq!(rc, SQLITE_OK);
        exec(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
        exec(
            db,
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<200) \
             INSERT INTO t SELECT x, 'v-' || x FROM c",
        );
        assert_eq!(query1(db, "SELECT count(*) FROM t"), "200");
        assert_eq!(query1(db, "SELECT sum(id) FROM t"), "20100");
        assert_eq!(query1(db, "SELECT v FROM t WHERE id=7"), "v-7");
        // PRAGMA round-trips through file control.
        assert_eq!(query1(db, "PRAGMA cipher"), "aes256gcm");
        assert_eq!(query1(db, "PRAGMA compression"), "zstd3");
        assert_eq!(query1(db, "PRAGMA flush_interval_ms"), "50");
        // Flush + stats by path.
        assert_eq!(unsafe { sqlite_murmur_flush(path.as_ptr()) }, SQLITE_OK);
        // FCTL flush on the live handle.
        assert_eq!(
            unsafe {
                sqlite3_file_control(
                    db,
                    c"main".as_ptr(),
                    crate::abi::FCTL_FLUSH,
                    std::ptr::null_mut(),
                )
            },
            SQLITE_OK
        );
        let mut stats = sqlite_murmur_stats::default();
        assert_eq!(unsafe { sqlite_murmur_get_stats(path.as_ptr(), &mut stats) }, SQLITE_OK);
        assert!(stats.persisted_blocks > 0);
        assert_eq!(unsafe { sqlite3_close(db) }, SQLITE_OK);

        // Reopen: persisted records decrypt.
        let mut db: *mut sqlite3 = std::ptr::null_mut();
        let rc = unsafe {
            sqlite3_open_v2(
                path.as_ptr(),
                &mut db,
                SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX,
                vfs_name.as_ptr(),
            )
        };
        assert_eq!(rc, SQLITE_OK);
        assert_eq!(query1(db, "SELECT count(*) FROM t"), "200");
        assert_eq!(query1(db, "SELECT v FROM t WHERE id=7"), "v-7");
        assert_eq!(unsafe { sqlite3_close(db) }, SQLITE_OK);

        // Unregister refuses while a store is open.
        let mut db: *mut sqlite3 = std::ptr::null_mut();
        let rc = unsafe {
            sqlite3_open_v2(
                path.as_ptr(),
                &mut db,
                SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX,
                vfs_name.as_ptr(),
            )
        };
        assert_eq!(rc, SQLITE_OK);
        assert_eq!(unsafe { sqlite_murmur_unregister() }, SQLITE_BUSY);
        assert_eq!(unsafe { sqlite3_close(db) }, SQLITE_OK);
        assert_eq!(unsafe { sqlite_murmur_unregister() }, SQLITE_OK);
        assert_eq!(unsafe { sqlite_murmur_unregister() }, SQLITE_MISUSE);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Memory-file io paths without VFS registration.
    #[test]
    fn mem_file_io_direct() {
        use crate::pool::{MemFile, ReusePool};
        let pool: Arc<ReusePool<MemFile>> = Arc::new(ReusePool::new(1));
        let mut slot = Box::new(MurmurFile::new());
        slot.mem = Some(pool.acquire_shared());
        let p = (&mut *slot as *mut MurmurFile) as *mut sqlite3_file;
        let data = b"hello-journal";
        // SAFETY: live file + borrowed buffers per io contract.
        assert_eq!(unsafe { io_write(p, data.as_ptr() as *const c_void, data.len() as c_int, 0) }, SQLITE_OK);
        let mut size = 0i64;
        assert_eq!(unsafe { io_file_size(p, &mut size) }, SQLITE_OK);
        assert_eq!(size, data.len() as i64);
        let mut out = vec![0u8; data.len()];
        assert_eq!(
            unsafe { io_read(p, out.as_mut_ptr() as *mut c_void, data.len() as c_int, 0) },
            SQLITE_OK
        );
        assert_eq!(&out, data);
        // Short read zero-fills.
        let mut out = vec![0x7fu8; data.len() + 4];
        assert_eq!(
            unsafe { io_read(p, out.as_mut_ptr() as *mut c_void, (data.len() + 4) as c_int, 0) },
            SQLITE_IOERR_SHORT_READ
        );
        assert_eq!(&out[data.len()..], &[0u8; 4]);
        assert_eq!(unsafe { io_truncate(p, 4) }, SQLITE_OK);
        assert_eq!(unsafe { io_file_size(p, &mut size) }, SQLITE_OK);
        assert_eq!(size, 4);
        assert_eq!(unsafe { io_sync(p, 0) }, SQLITE_OK);
        assert_eq!(unsafe { io_sector_size(p) }, BLOCK as c_int);
        // Locks are no-ops for mem files.
        assert_eq!(unsafe { io_lock(p, SQLITE_LOCK_EXCLUSIVE) }, SQLITE_OK);
        let mut reserved = 0;
        assert_eq!(unsafe { io_check_reserved(p, &mut reserved) }, SQLITE_OK);
        assert_eq!(reserved, 0);
        assert_eq!(unsafe { io_unlock(p, SQLITE_LOCK_NONE) }, SQLITE_OK);
        assert_eq!(unsafe { io_close(p) }, SQLITE_OK);
        std::mem::forget(slot);
    }
}

/// Unregister the VFS (refuses while stores are open).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite_murmur_unregister() -> c_int {
    crate::guard(
        || {
            let mut globals = lock(&GLOBALS);
            for weak in globals.stores.values() {
                if weak.strong_count() > 0 {
                    return SQLITE_BUSY;
                }
            }
            if !globals.registered {
                return SQLITE_MISUSE;
            }
            // SAFETY: stable pointer, valid registered VFS struct.
            let rc = unsafe { crate::ffi_sqlite::sqlite3_vfs_unregister(gvfs()) };
            if rc == SQLITE_OK {
                globals.settings.key.zeroize();
                globals.file_key.zeroize();
                globals.file_key_previous.zeroize();
                globals.file_key_set = false;
                globals.file_key_previous_set = false;
                globals.mem_files.clear();
                globals.stores.clear();
                globals.registered = false;
            }
            rc
        },
        SQLITE_ERROR,
    )
}
