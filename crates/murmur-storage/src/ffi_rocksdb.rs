//! Safe wrappers over the vendored RocksDB C API
//! (`third-party/rocksdb/include/rocksdb/c.h`) plus the Murmur extension
//! (`third-party/rocksdb/db/c_murmur_ext.cc`, `MURMUR-C-EXT.md`).
//!
//! Every declaration here is transcribed from those headers; the wrappers
//! map `errptr` failures to `Err(String)` and classify `Status::ToString`
//! prefixes back into the status kinds the C++ store branched on
//! (`IsNotFound` / `IsIOError` / `IsTimedOut` / `IsBusy` / `IsTryAgain`).
//! `rocksdb_get` returning NULL with no error means not-found, exactly as
//! the C API documents.

use std::ffi::{CStr, CString, c_char, c_uchar, c_void};
use std::ptr;

macro_rules! opaque {
    ($name:ident) => {
        #[allow(non_camel_case_types)]
        #[repr(C)]
        pub struct $name {
            _private: [u8; 0],
        }
    };
}

opaque!(rocksdb_t);
opaque!(rocksdb_options_t);
opaque!(rocksdb_readoptions_t);
opaque!(rocksdb_writeoptions_t);
opaque!(rocksdb_writebatch_t);
opaque!(rocksdb_iterator_t);
opaque!(rocksdb_cache_t);
opaque!(rocksdb_block_based_table_options_t);
opaque!(rocksdb_flushoptions_t);
opaque!(rocksdb_wait_for_compact_options_t);

/// Ticker ids from `third-party/rocksdb/include/rocksdb/statistics.h`
/// (verified empirically: WAL=63, COMPACT=68, FLUSH=69).
pub const TICKER_WAL_FILE_BYTES: u32 = 63;
pub const TICKER_COMPACT_WRITE_BYTES: u32 = 68;
pub const TICKER_FLUSH_WRITE_BYTES: u32 = 69;

/// `rocksdb_statistics_level_except_histogram_or_timers` (c.h): the level
/// the C++ store selected via `set_stats_level`.
pub const STATS_LEVEL_EXCEPT_HISTOGRAM_OR_TIMERS: i32 = 1;
/// `rocksdb_no_compression` (c.h): the store compresses pages itself.
pub const NO_COMPRESSION: i32 = 0;
/// `rocksdb_wait_for_compact_options_set_timeout` takes microseconds;
/// the C++ checkpoint waits 30 s.
pub const CHECKPOINT_COMPACT_TIMEOUT_US: u64 = 30_000_000;

unsafe extern "C" {
    pub(crate) unsafe fn rocksdb_open(
        options: *const rocksdb_options_t,
        name: *const c_char,
        errptr: *mut *mut c_char,
    ) -> *mut rocksdb_t;
    pub(crate) unsafe fn rocksdb_close(db: *mut rocksdb_t);
    pub(crate) unsafe fn rocksdb_put(
        db: *mut rocksdb_t,
        options: *const rocksdb_writeoptions_t,
        key: *const c_char,
        keylen: usize,
        val: *const c_char,
        vallen: usize,
        errptr: *mut *mut c_char,
    );
    pub(crate) unsafe fn rocksdb_get(
        db: *mut rocksdb_t,
        options: *const rocksdb_readoptions_t,
        key: *const c_char,
        keylen: usize,
        vallen: *mut usize,
        errptr: *mut *mut c_char,
    ) -> *mut c_char;
    pub(crate) unsafe fn rocksdb_write(
        db: *mut rocksdb_t,
        options: *const rocksdb_writeoptions_t,
        batch: *mut rocksdb_writebatch_t,
        errptr: *mut *mut c_char,
    );
    pub(crate) unsafe fn rocksdb_create_iterator(
        db: *mut rocksdb_t,
        options: *const rocksdb_readoptions_t,
    ) -> *mut rocksdb_iterator_t;
    pub(crate) unsafe fn rocksdb_iter_destroy(it: *mut rocksdb_iterator_t);
    pub(crate) unsafe fn rocksdb_iter_valid(it: *const rocksdb_iterator_t) -> c_uchar;
    pub(crate) unsafe fn rocksdb_iter_seek(it: *mut rocksdb_iterator_t, k: *const c_char, klen: usize);
    pub(crate) unsafe fn rocksdb_iter_next(it: *mut rocksdb_iterator_t);
    pub(crate) unsafe fn rocksdb_iter_key(it: *const rocksdb_iterator_t, klen: *mut usize) -> *const c_char;
    pub(crate) unsafe fn rocksdb_iter_value(
        it: *const rocksdb_iterator_t,
        vlen: *mut usize,
    ) -> *const c_char;
    pub(crate) unsafe fn rocksdb_iter_get_error(
        it: *const rocksdb_iterator_t,
        errptr: *mut *mut c_char,
    );
    pub(crate) unsafe fn rocksdb_writebatch_create() -> *mut rocksdb_writebatch_t;
    pub(crate) unsafe fn rocksdb_writebatch_destroy(b: *mut rocksdb_writebatch_t);
    pub(crate) unsafe fn rocksdb_writebatch_clear(b: *mut rocksdb_writebatch_t);
    pub(crate) unsafe fn rocksdb_writebatch_put(
        b: *mut rocksdb_writebatch_t,
        key: *const c_char,
        klen: usize,
        val: *const c_char,
        vlen: usize,
    );
    pub(crate) unsafe fn rocksdb_writebatch_delete(
        b: *mut rocksdb_writebatch_t,
        key: *const c_char,
        klen: usize,
    );
    pub(crate) unsafe fn rocksdb_writebatch_delete_range(
        b: *mut rocksdb_writebatch_t,
        start_key: *const c_char,
        start_key_len: usize,
        end_key: *const c_char,
        end_key_len: usize,
    );
    pub(crate) unsafe fn rocksdb_options_create() -> *mut rocksdb_options_t;
    pub(crate) unsafe fn rocksdb_options_destroy(opt: *mut rocksdb_options_t);
    pub(crate) unsafe fn rocksdb_options_set_create_if_missing(opt: *mut rocksdb_options_t, v: c_uchar);
    pub(crate) unsafe fn rocksdb_options_set_compression(opt: *mut rocksdb_options_t, v: i32);
    pub(crate) unsafe fn rocksdb_options_enable_statistics(opt: *mut rocksdb_options_t);
    pub(crate) unsafe fn rocksdb_options_set_statistics_level(opt: *mut rocksdb_options_t, level: i32);
    pub(crate) unsafe fn rocksdb_options_statistics_get_ticker_count(
        opt: *mut rocksdb_options_t,
        ticker_type: u32,
    ) -> u64;
    pub(crate) unsafe fn rocksdb_options_set_block_based_table_factory(
        opt: *mut rocksdb_options_t,
        table_options: *mut rocksdb_block_based_table_options_t,
    );
    pub(crate) unsafe fn rocksdb_block_based_options_create() -> *mut rocksdb_block_based_table_options_t;
    pub(crate) unsafe fn rocksdb_block_based_options_destroy(
        opt: *mut rocksdb_block_based_table_options_t,
    );
    pub(crate) unsafe fn rocksdb_block_based_options_set_block_cache(
        opt: *mut rocksdb_block_based_table_options_t,
        block_cache: *mut rocksdb_cache_t,
    );
    pub(crate) unsafe fn rocksdb_cache_create_lru(capacity: usize) -> *mut rocksdb_cache_t;
    pub(crate) unsafe fn rocksdb_cache_destroy(cache: *mut rocksdb_cache_t);
    pub(crate) unsafe fn rocksdb_cache_set_capacity(cache: *mut rocksdb_cache_t, capacity: usize);
    pub(crate) unsafe fn rocksdb_cache_get_capacity(cache: *const rocksdb_cache_t) -> usize;
    pub(crate) unsafe fn rocksdb_readoptions_create() -> *mut rocksdb_readoptions_t;
    pub(crate) unsafe fn rocksdb_readoptions_destroy(opt: *mut rocksdb_readoptions_t);
    pub(crate) unsafe fn rocksdb_writeoptions_create() -> *mut rocksdb_writeoptions_t;
    pub(crate) unsafe fn rocksdb_writeoptions_destroy(opt: *mut rocksdb_writeoptions_t);
    pub(crate) unsafe fn rocksdb_writeoptions_set_sync(opt: *mut rocksdb_writeoptions_t, v: c_uchar);
    pub(crate) unsafe fn rocksdb_writeoptions_disable_WAL(opt: *mut rocksdb_writeoptions_t, disable: i32);
    pub(crate) unsafe fn rocksdb_flushoptions_create() -> *mut rocksdb_flushoptions_t;
    pub(crate) unsafe fn rocksdb_flushoptions_destroy(opt: *mut rocksdb_flushoptions_t);
    pub(crate) unsafe fn rocksdb_flushoptions_set_wait(opt: *mut rocksdb_flushoptions_t, v: c_uchar);
    pub(crate) unsafe fn rocksdb_flush(
        db: *mut rocksdb_t,
        options: *const rocksdb_flushoptions_t,
        errptr: *mut *mut c_char,
    );
    pub(crate) unsafe fn rocksdb_wait_for_compact_options_create() -> *mut rocksdb_wait_for_compact_options_t;
    pub(crate) unsafe fn rocksdb_wait_for_compact_options_destroy(
        opt: *mut rocksdb_wait_for_compact_options_t,
    );
    pub(crate) unsafe fn rocksdb_wait_for_compact_options_set_timeout(
        opt: *mut rocksdb_wait_for_compact_options_t,
        microseconds: u64,
    );
    pub(crate) unsafe fn rocksdb_wait_for_compact(
        db: *mut rocksdb_t,
        options: *mut rocksdb_wait_for_compact_options_t,
        errptr: *mut *mut c_char,
    );
    pub(crate) unsafe fn rocksdb_set_options(
        db: *mut rocksdb_t,
        count: i32,
        keys: *const *const c_char,
        values: *const *const c_char,
        errptr: *mut *mut c_char,
    );
    pub(crate) unsafe fn rocksdb_repair_db(
        options: *const rocksdb_options_t,
        name: *const c_char,
        errptr: *mut *mut c_char,
    );
    pub(crate) unsafe fn rocksdb_free(ptr: *mut c_void);
    // Murmur extension (db/c_murmur_ext.cc): 0 = ok, 1 = error in errptr.
    pub(crate) unsafe fn murmur_rocksdb_set_db_option(
        db: *mut rocksdb_t,
        name: *const c_char,
        value: *const c_char,
        errptr: *mut *mut c_char,
    ) -> i32;
    pub(crate) unsafe fn murmur_rocksdb_get_live_option(
        db: *mut rocksdb_t,
        name: *const c_char,
        out: *mut u64,
        errptr: *mut *mut c_char,
    ) -> i32;
    pub(crate) unsafe fn murmur_rocksdb_compact_range_full(
        db: *mut rocksdb_t,
        errptr: *mut *mut c_char,
    ) -> i32;
    pub(crate) unsafe fn murmur_rocksdb_checkpoint_create_with_seq(
        db: *mut rocksdb_t,
        checkpoint_dir: *const c_char,
        seq: *mut u64,
        errptr: *mut *mut c_char,
    ) -> i32;
}

/// Take an `errptr` outcome: `None` when the call succeeded, the owned
/// message (freed with `rocksdb_free`) otherwise.
pub(crate) unsafe fn take_err(err: *mut c_char) -> Option<String> {
    if err.is_null() {
        return None;
    }
    let msg = unsafe { CStr::from_ptr(err) }
        .to_string_lossy()
        .into_owned();
    unsafe { rocksdb_free(err as *mut c_void) };
    Some(msg)
}

/// Status kinds the C++ store branched on, recovered from the
/// `Status::ToString` text the C API stores in `errptr` (`db/c.cc`
/// `SaveError` keeps the `"<Code>: <details>"` shape).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StatusKind {
    NotFound,
    IoError,
    TimedOut,
    Busy,
    TryAgain,
    Other,
}

pub fn classify(message: &str) -> StatusKind {
    if message.starts_with("NotFound") {
        StatusKind::NotFound
    } else if message.starts_with("IO error") {
        StatusKind::IoError
    } else if message.starts_with("TimedOut") {
        StatusKind::TimedOut
    } else if message.starts_with("Busy") {
        StatusKind::Busy
    } else if message.starts_with("Try again") {
        StatusKind::TryAgain
    } else {
        StatusKind::Other
    }
}

pub fn is_timed_out(message: &str) -> bool {
    classify(message) == StatusKind::TimedOut
}

pub fn is_busy_or_retry(message: &str) -> bool {
    matches!(classify(message), StatusKind::Busy | StatusKind::TryAgain)
}

pub fn is_io_error(message: &str) -> bool {
    classify(message) == StatusKind::IoError
}

macro_rules! wrapper {
    ($name:ident, $raw:ty, $destroy:path) => {
        pub struct $name {
            raw: *mut $raw,
        }

        impl Drop for $name {
            fn drop(&mut self) {
                if !self.raw.is_null() {
                    unsafe { $destroy(self.raw) };
                }
            }
        }
    };
}

wrapper!(ReadOptions, rocksdb_readoptions_t, rocksdb_readoptions_destroy);
wrapper!(WriteOptions, rocksdb_writeoptions_t, rocksdb_writeoptions_destroy);
wrapper!(WriteBatch, rocksdb_writebatch_t, rocksdb_writebatch_destroy);
wrapper!(Cache, rocksdb_cache_t, rocksdb_cache_destroy);
wrapper!(
    BlockBasedTableOptions,
    rocksdb_block_based_table_options_t,
    rocksdb_block_based_options_destroy
);
wrapper!(FlushOptions, rocksdb_flushoptions_t, rocksdb_flushoptions_destroy);
wrapper!(
    WaitForCompactOptions,
    rocksdb_wait_for_compact_options_t,
    rocksdb_wait_for_compact_options_destroy
);

/// `rocksdb::DB` handle. `Send + Sync`: RocksDB snapshots concurrent
/// access, and the C++ store already shared one `DB` across the VFS
/// threads, the flush worker, and the encoder pool.
pub struct Db {
    raw: *mut rocksdb_t,
}

// SAFETY: `rocksdb_t` methods are thread-safe; the raw pointer is only
// dereferenced through those methods.
unsafe impl Send for Db {}
unsafe impl Sync for Db {}

impl Drop for Db {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe { rocksdb_close(self.raw) };
        }
    }
}

/// `rocksdb::Options` kept alive for the store lifetime: the C API reads
/// tickers back through the options object, and the block cache must
/// outlive the table factory that references it.
pub struct StoreOptions {
    raw: *mut rocksdb_options_t,
}

// SAFETY: configured once before `Db::open`, then only read (ticker
// queries); same sharing shape as the C++ `statistics` handle.
unsafe impl Send for StoreOptions {}
unsafe impl Sync for StoreOptions {}

impl Drop for StoreOptions {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe { rocksdb_options_destroy(self.raw) };
        }
    }
}

// SAFETY: `Cache::SetCapacity` / `GetCapacity` are thread-safe; the C++
// store shared one cache across connections the same way.
unsafe impl Send for Cache {}
unsafe impl Sync for Cache {}

impl StoreOptions {
    /// Mirror of the C++ `Store::start` / repair / rekey options recipe:
    /// optional create, no RocksDB compression (pages compress before
    /// encryption), optional statistics, LRU block cache behind a
    /// block-based table factory. The cache is returned alongside so the
    /// caller keeps it alive and can resize it via PRAGMA.
    pub fn for_store(create_if_missing: bool, with_statistics: bool, block_cache_bytes: usize) -> (Self, Cache) {
        let raw = unsafe { rocksdb_options_create() };
        assert!(!raw.is_null(), "rocksdb_options_create returned NULL");
        unsafe {
            rocksdb_options_set_create_if_missing(raw, c_uchar::from(create_if_missing));
            rocksdb_options_set_compression(raw, NO_COMPRESSION);
            if with_statistics {
                rocksdb_options_enable_statistics(raw);
                rocksdb_options_set_statistics_level(raw, STATS_LEVEL_EXCEPT_HISTOGRAM_OR_TIMERS);
            }
        }
        let cache_raw = unsafe { rocksdb_cache_create_lru(block_cache_bytes) };
        assert!(!cache_raw.is_null(), "rocksdb_cache_create_lru returned NULL");
        let table = unsafe { rocksdb_block_based_options_create() };
        assert!(!table.is_null(), "rocksdb_block_based_options_create returned NULL");
        unsafe {
            rocksdb_block_based_options_set_block_cache(table, cache_raw);
            rocksdb_options_set_block_based_table_factory(raw, table);
            // The factory copies what it needs; the C++ temporary dies
            // here too (end of the enclosing scope in Store::start).
            rocksdb_block_based_options_destroy(table);
        }
        (Self { raw }, Cache { raw: cache_raw })
    }

    pub fn ticker(&self, ticker_type: u32) -> u64 {
        unsafe { rocksdb_options_statistics_get_ticker_count(self.raw, ticker_type) }
    }
}

impl Cache {
    pub fn set_capacity(&self, bytes: usize) {
        unsafe { rocksdb_cache_set_capacity(self.raw, bytes) }
    }

    pub fn capacity(&self) -> usize {
        unsafe { rocksdb_cache_get_capacity(self.raw) }
    }
}

impl ReadOptions {
    pub fn new() -> Self {
        let raw = unsafe { rocksdb_readoptions_create() };
        assert!(!raw.is_null(), "rocksdb_readoptions_create returned NULL");
        Self { raw }
    }
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteOptions {
    pub fn new() -> Self {
        let raw = unsafe { rocksdb_writeoptions_create() };
        assert!(!raw.is_null(), "rocksdb_writeoptions_create returned NULL");
        Self { raw }
    }

    pub fn with_sync(sync: bool) -> Self {
        let this = Self::new();
        unsafe { rocksdb_writeoptions_set_sync(this.raw, c_uchar::from(sync)) };
        this
    }

    pub fn set_no_wal(&self) {
        unsafe { rocksdb_writeoptions_disable_WAL(self.raw, 1) };
    }
}

impl WriteBatch {
    pub fn new() -> Self {
        let raw = unsafe { rocksdb_writebatch_create() };
        assert!(!raw.is_null(), "rocksdb_writebatch_create returned NULL");
        Self { raw }
    }

    pub fn put(&mut self, key: &[u8], val: &[u8]) {
        unsafe {
            rocksdb_writebatch_put(
                self.raw,
                key.as_ptr() as *const c_char,
                key.len(),
                val.as_ptr() as *const c_char,
                val.len(),
            )
        };
    }

    pub fn delete(&mut self, key: &[u8]) {
        unsafe { rocksdb_writebatch_delete(self.raw, key.as_ptr() as *const c_char, key.len()) };
    }

    pub fn delete_range(&mut self, start: &[u8], end: &[u8]) {
        unsafe {
            rocksdb_writebatch_delete_range(
                self.raw,
                start.as_ptr() as *const c_char,
                start.len(),
                end.as_ptr() as *const c_char,
                end.len(),
            )
        };
    }

    pub fn clear(&mut self) {
        unsafe { rocksdb_writebatch_clear(self.raw) };
    }
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

impl Db {
    pub fn open(path: &std::path::Path, options: &StoreOptions) -> Result<Self, String> {
        let path = CString::new(path.as_os_str().as_encoded_bytes())
            .map_err(|_| "path contains NUL".to_string())?;
        let mut err: *mut c_char = ptr::null_mut();
        let raw = unsafe { rocksdb_open(options.raw, path.as_ptr(), &mut err) };
        if let Some(msg) = unsafe { take_err(err) } {
            return Err(msg);
        }
        if raw.is_null() {
            return Err("rocksdb_open returned NULL without an error".to_string());
        }
        Ok(Self { raw })
    }

    /// Get into a caller buffer (hot read path; mirrors the C++ `Get`
    /// into a pooled string). Returns whether the key was found.
    pub fn get_into(
        &self,
        options: &ReadOptions,
        key: &[u8],
        buf: &mut Vec<u8>,
    ) -> Result<bool, String> {
        let mut vallen: usize = 0;
        let mut err: *mut c_char = ptr::null_mut();
        let val = unsafe {
            rocksdb_get(
                self.raw,
                options.raw,
                key.as_ptr() as *const c_char,
                key.len(),
                &mut vallen,
                &mut err,
            )
        };
        if let Some(msg) = unsafe { take_err(err) } {
            return Err(msg);
        }
        if val.is_null() {
            return Ok(false);
        }
        buf.clear();
        buf.reserve(vallen);
        unsafe {
            ptr::copy_nonoverlapping(val as *const u8, buf.as_mut_ptr(), vallen);
            buf.set_len(vallen);
            rocksdb_free(val as *mut c_void);
        }
        Ok(true)
    }

    /// `None` value with no error means not-found (C API convention).
    pub fn get(&self, options: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let mut vallen: usize = 0;
        let mut err: *mut c_char = ptr::null_mut();
        let val = unsafe {
            rocksdb_get(
                self.raw,
                options.raw,
                key.as_ptr() as *const c_char,
                key.len(),
                &mut vallen,
                &mut err,
            )
        };
        if let Some(msg) = unsafe { take_err(err) } {
            return Err(msg);
        }
        if val.is_null() {
            return Ok(None);
        }
        let out = unsafe { std::slice::from_raw_parts(val as *const u8, vallen) }.to_vec();
        unsafe { rocksdb_free(val as *mut c_void) };
        Ok(Some(out))
    }

    pub fn put(&self, options: &WriteOptions, key: &[u8], val: &[u8]) -> Result<(), String> {
        let mut err: *mut c_char = ptr::null_mut();
        unsafe {
            rocksdb_put(
                self.raw,
                options.raw,
                key.as_ptr() as *const c_char,
                key.len(),
                val.as_ptr() as *const c_char,
                val.len(),
                &mut err,
            )
        };
        if let Some(msg) = unsafe { take_err(err) } {
            return Err(msg);
        }
        Ok(())
    }

    pub fn write(&self, options: &WriteOptions, batch: &mut WriteBatch) -> Result<(), String> {
        let mut err: *mut c_char = ptr::null_mut();
        unsafe { rocksdb_write(self.raw, options.raw, batch.raw, &mut err) };
        if let Some(msg) = unsafe { take_err(err) } {
            return Err(msg);
        }
        Ok(())
    }

    pub fn iterator(&self, options: &ReadOptions) -> DbIterator {
        let raw = unsafe { rocksdb_create_iterator(self.raw, options.raw) };
        assert!(!raw.is_null(), "rocksdb_create_iterator returned NULL");
        DbIterator { raw }
    }

    pub fn set_options(&self, pairs: &[(&CStr, &CStr)]) -> Result<(), String> {
        let keys: Vec<*const c_char> = pairs.iter().map(|(k, _)| k.as_ptr()).collect();
        let values: Vec<*const c_char> = pairs.iter().map(|(_, v)| v.as_ptr()).collect();
        let mut err: *mut c_char = ptr::null_mut();
        unsafe {
            rocksdb_set_options(
                self.raw,
                pairs.len() as i32,
                keys.as_ptr(),
                values.as_ptr(),
                &mut err,
            )
        };
        if let Some(msg) = unsafe { take_err(err) } {
            return Err(msg);
        }
        Ok(())
    }

    pub fn set_db_option(&self, name: &CStr, value: &CStr) -> Result<(), String> {
        let mut err: *mut c_char = ptr::null_mut();
        let rc = unsafe { murmur_rocksdb_set_db_option(self.raw, name.as_ptr(), value.as_ptr(), &mut err) };
        if let Some(msg) = unsafe { take_err(err) } {
            debug_assert_ne!(rc, 0);
            return Err(msg);
        }
        debug_assert_eq!(rc, 0);
        Ok(())
    }

    pub fn live_option(&self, name: &CStr) -> Result<u64, String> {
        let mut out: u64 = 0;
        let mut err: *mut c_char = ptr::null_mut();
        let rc = unsafe { murmur_rocksdb_get_live_option(self.raw, name.as_ptr(), &mut out, &mut err) };
        if let Some(msg) = unsafe { take_err(err) } {
            debug_assert_ne!(rc, 0);
            return Err(msg);
        }
        debug_assert_eq!(rc, 0);
        Ok(out)
    }

    pub fn flush_wait(&self) -> Result<(), String> {
        let opts = unsafe { rocksdb_flushoptions_create() };
        assert!(!opts.is_null(), "rocksdb_flushoptions_create returned NULL");
        unsafe { rocksdb_flushoptions_set_wait(opts, 1) };
        let mut err: *mut c_char = ptr::null_mut();
        unsafe { rocksdb_flush(self.raw, opts, &mut err) };
        unsafe { rocksdb_flushoptions_destroy(opts) };
        if let Some(msg) = unsafe { take_err(err) } {
            return Err(msg);
        }
        Ok(())
    }

    pub fn wait_for_compact_30s(&self) -> Result<(), String> {
        let opts = unsafe { rocksdb_wait_for_compact_options_create() };
        assert!(!opts.is_null(), "wait_for_compact_options_create returned NULL");
        unsafe { rocksdb_wait_for_compact_options_set_timeout(opts, CHECKPOINT_COMPACT_TIMEOUT_US) };
        let mut err: *mut c_char = ptr::null_mut();
        unsafe { rocksdb_wait_for_compact(self.raw, opts, &mut err) };
        unsafe { rocksdb_wait_for_compact_options_destroy(opts) };
        if let Some(msg) = unsafe { take_err(err) } {
            return Err(msg);
        }
        Ok(())
    }

    pub fn compact_full_range(&self) -> Result<(), String> {
        let mut err: *mut c_char = ptr::null_mut();
        let rc = unsafe { murmur_rocksdb_compact_range_full(self.raw, &mut err) };
        if let Some(msg) = unsafe { take_err(err) } {
            debug_assert_ne!(rc, 0);
            return Err(msg);
        }
        debug_assert_eq!(rc, 0);
        Ok(())
    }

    /// Checkpoint + sequence number in one call (Murmur extension):
    /// the sequence is captured atomically with the checkpoint, exactly
    /// like the C++ backup shim's `CreateCheckpoint(dest, 0, seq)`.
    pub fn checkpoint_to(&self, dir: &std::path::Path) -> Result<u64, String> {
        let dir = CString::new(dir.as_os_str().as_encoded_bytes())
            .map_err(|_| "checkpoint path contains NUL".to_string())?;
        let mut seq: u64 = 0;
        let mut err: *mut c_char = ptr::null_mut();
        let rc = unsafe {
            murmur_rocksdb_checkpoint_create_with_seq(self.raw, dir.as_ptr(), &mut seq, &mut err)
        };
        if let Some(msg) = unsafe { take_err(err) } {
            debug_assert_ne!(rc, 0);
            return Err(msg);
        }
        debug_assert_eq!(rc, 0);
        Ok(seq)
    }
}

pub fn repair_db(path: &std::path::Path, options: &StoreOptions) -> Result<(), String> {
    let path = CString::new(path.as_os_str().as_encoded_bytes())
        .map_err(|_| "path contains NUL".to_string())?;
    let mut err: *mut c_char = ptr::null_mut();
    unsafe { rocksdb_repair_db(options.raw, path.as_ptr(), &mut err) };
    if let Some(msg) = unsafe { take_err(err) } {
        return Err(msg);
    }
    Ok(())
}

/// Single-threaded iterator (created, consumed, and destroyed on one
/// thread at every call site, as in the C++).
pub struct DbIterator {
    raw: *mut rocksdb_iterator_t,
}

impl Drop for DbIterator {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe { rocksdb_iter_destroy(self.raw) };
        }
    }
}

impl DbIterator {
    pub fn seek(&mut self, key: &[u8]) {
        unsafe { rocksdb_iter_seek(self.raw, key.as_ptr() as *const c_char, key.len()) };
    }

    pub fn next(&mut self) {
        unsafe { rocksdb_iter_next(self.raw) };
    }

    pub fn valid(&self) -> bool {
        let valid = unsafe { rocksdb_iter_valid(self.raw) };
        valid != 0
    }

    pub fn key(&self) -> &[u8] {
        let mut klen: usize = 0;
        let k = unsafe { rocksdb_iter_key(self.raw, &mut klen) };
        if k.is_null() {
            return &[];
        }
        unsafe { std::slice::from_raw_parts(k as *const u8, klen) }
    }

    pub fn value(&self) -> Vec<u8> {
        let mut vlen: usize = 0;
        let v = unsafe { rocksdb_iter_value(self.raw, &mut vlen) };
        if v.is_null() {
            return Vec::new();
        }
        unsafe { std::slice::from_raw_parts(v as *const u8, vlen) }.to_vec()
    }

    pub fn status(&self) -> Result<(), String> {
        let mut err: *mut c_char = ptr::null_mut();
        unsafe { rocksdb_iter_get_error(self.raw, &mut err) };
        if let Some(msg) = unsafe { take_err(err) } {
            return Err(msg);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("murmur-ffi-rdb-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn smoke_open_put_get_iterate() {
        let dir = scratch("smoke");
        let (opts, cache) = StoreOptions::for_store(true, true, 8 << 20);
        assert_eq!(cache.capacity(), 8 << 20);
        cache.set_capacity(4 << 20);
        assert_eq!(cache.capacity(), 4 << 20);
        let db = Db::open(&dir, &opts).expect("open");
        let ro = ReadOptions::new();
        let wo = WriteOptions::with_sync(true);
        assert_eq!(db.get(&ro, b"missing").expect("get"), None);
        db.put(&wo, b"k1", b"v1").expect("put");
        assert_eq!(db.get(&ro, b"k1").expect("get"), Some(b"v1".to_vec()));
        let mut batch = WriteBatch::new();
        batch.put(b"k2", b"v2");
        db.write(&wo, &mut batch).expect("write");
        assert_eq!(db.get(&ro, b"k2").expect("get"), Some(b"v2".to_vec()));
        let mut it = db.iterator(&ro);
        it.seek(b"k");
        let mut seen = Vec::new();
        while it.valid() {
            seen.push(it.key().to_vec());
            it.next();
        }
        it.status().expect("iter status");
        assert_eq!(seen, vec![b"k1".to_vec(), b"k2".to_vec()]);
        assert!(opts.ticker(TICKER_WAL_FILE_BYTES) > 0);
        db.flush_wait().expect("flush");
        db.wait_for_compact_30s().expect("wait compact");
        db.compact_full_range().expect("compact");
        let c1 = CString::new("max_background_jobs").unwrap();
        db.set_db_option(&c1, &CString::new("3").unwrap()).expect("set db opt");
        assert_eq!(db.live_option(&c1).expect("live opt"), 3);
        let cpt = scratch("cpt");
        let seq = db.checkpoint_to(&cpt).expect("checkpoint");
        assert!(seq >= 2, "checkpoint seq {seq}");
        assert!(cpt.join("CURRENT").exists());
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&cpt);
    }

    #[test]
    fn classify_known_prefixes() {
        assert_eq!(classify("NotFound: x"), StatusKind::NotFound);
        assert_eq!(classify("IO error: lock y"), StatusKind::IoError);
        assert_eq!(classify("TimedOut: z"), StatusKind::TimedOut);
        assert_eq!(classify("Busy: w"), StatusKind::Busy);
        assert_eq!(classify("Try again: v"), StatusKind::TryAgain);
        assert_eq!(classify("Corruption: u"), StatusKind::Other);
        assert!(is_busy_or_retry("Busy: w"));
        assert!(is_timed_out("TimedOut: z"));
        assert!(is_io_error("IO error: lock y"));
    }
}
