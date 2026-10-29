//! The encrypted page store: RocksDB-backed page cache, keyring state,
//! background persistence worker, and PRAGMA/keyring control. Ported from
//! the `Store` struct in `src/sqlite_murmur.cpp`.
//!
//! Lock order (unchanged): `mutation_mu` -> `flush_mu` -> `mu`. Pool
//! locks never remain held while acquiring these or doing crypto/IO.
//! `backup_mu` is a leaf serializing online backups. Rust consolidates
//! every `mu`-guarded field into [`Guarded`]; a few paths the C++ reads
//! lock-free under `flushMu` take `mu` briefly instead (same order, no
//! deadlock; the critical sections are single field copies).
//!
//! Deliberate divergences from the C++ (all forced by the language):
//!
//! - `catch (bad_alloc) -> SQLITE_NOMEM` becomes process abort: safe Rust
//!   cannot recover from allocation failure. Only reachable on actual OOM.
//! - `meta:size` parses strict ASCII digits (`stoull` also skips leading
//!   whitespace and takes a sign). The writer only emits canonical digits,
//!   so reachable databases parse identically; garbage fails either way.
//! - A failed zstd context creation fails the decrypt closed instead of
//!   throwing `bad_alloc` through the read path (which would abort).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::c_int;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread::JoinHandle;
use zeroize::Zeroize;

use crate::abi::sqlite_murmur_stats;
use crate::crypto::{self, BLOCK, Cipher, DecodeWorkspace, page_key};
use crate::ffi_rocksdb::{Cache, Db, ReadOptions, StoreOptions, WriteBatch, WriteOptions};
use crate::ffi_sqlite::{
    SQLITE_BUSY, SQLITE_IOERR_READ, SQLITE_IOERR_TRUNCATE, SQLITE_IOERR_WRITE, SQLITE_LOCK_NONE,
    SQLITE_OK,
};
use crate::keyring::{self, Keyring};
use crate::pool::{
    EncodingPool, JournalPool, PagePool, PoolLease, Record, RecordPool, WorkspacePool,
    physical_core_count,
};

pub const META_SIZE: &[u8] = b"meta:size";
pub const META_CIPHER: &[u8] = b"meta:cipher";
pub const META_KEYRING: &[u8] = b"meta:keyring";
pub const META_KEYRING_REKEY: &[u8] = b"meta:keyring.REKEY";
pub const META_STORE_ID: &[u8] = b"meta:store_id";
pub const END_KEY: &[u8] = b"q";
pub const ROCKS_BLOCK_CACHE_BYTES: usize = 32 * 1024 * 1024;
pub const RECORD_POOL_LIMIT: usize = 128;
pub const WORKSPACE_POOL_LIMIT: usize = 8;
pub const JOURNAL_POOL_LIMIT: usize = 1;

pub(crate) fn lock<T>(mu: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mu.lock().expect("store mutex poisoned")
}

/// Poison-tolerant lock for the worker thread (loop + `persist`): a
/// panicked holder's operation already failed closed via the FFI guard;
/// the worker salvages and carries on instead of wedging the store.
fn lock_salvage<T>(mu: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mu.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Strict `stoull` for canonical writer output (see module docs).
fn parse_meta_size(text: &[u8]) -> Option<u64> {
    if text.is_empty() || !text.iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let mut value: u64 = 0;
    for c in text {
        value = value.checked_mul(10)?.checked_add(u64::from(c - b'0'))?;
    }
    Some(value)
}

pub fn parse_unsigned(text: &[u8], maximum: u64) -> Option<u64> {
    if text.is_empty() {
        return None;
    }
    let mut value: u64 = 0;
    for c in text {
        if !c.is_ascii_digit() {
            return None;
        }
        let digit = u64::from(c - b'0');
        if value > maximum / 10 || (value == maximum / 10 && digit > maximum % 10) {
            return None;
        }
        value = value * 10 + digit;
    }
    Some(value)
}

#[derive(Clone)]
pub struct Settings {
    pub key: [u8; 32],
    pub interval_ms: u32,
    pub cipher: Cipher,
    pub compression_level: i32,
    pub cache_pages: usize,
    pub rocksdb_sync: bool,
    pub background_threads: u32,
}

impl Settings {
    pub fn new(key: [u8; 32], interval_ms: u32, cache_pages: usize, rocksdb_sync: bool) -> Self {
        Self {
            key,
            interval_ms: if interval_ms == 0 { 250 } else { interval_ms },
            cipher: Cipher::Aes256Gcm,
            compression_level: 3,
            cache_pages: if cache_pages == 0 { 16384 } else { cache_pages },
            rocksdb_sync,
            background_threads: physical_core_count(),
        }
    }
}

pub struct CachedPage {
    pub record: Record,
    pub dirty: bool,
    pub lru_key: (u64, u64),
}

pub struct FlushBatch {
    pub pages: HashMap<u64, Record>,
    pub trim: bool,
    pub have_size: bool,
    pub trim_from: u64,
    pub new_size: u64,
}

/// State under `flush_mu` (C++ `flushMu` + the fields it guards).
pub(crate) struct FlushState {
    pub(crate) encoders: Option<EncodingPool>,
    pub(crate) failed_batch: Option<FlushBatch>,
}

/// State under `mu` (the bulk of the C++ `Store` fields).
pub struct Guarded {
    pub settings: Settings,
    pub counters: sqlite_murmur_stats,
    pub generation: u64,
    pub keyring: Option<Keyring>,
    pub active_key: [u8; 32],
    pub keyring_store_id: [u8; 16],
    pub staged_kdf: u8,
    pub staged_time: u32,
    pub staged_memory_kib: u32,
    pub staged_threads: u8,
    pub clean_count: usize,
    pub requested_flush: u64,
    pub completed_flush: u64,
    pub flush_result: c_int,
    pub in_flight_pages: usize,
    pub stopping: bool,
    pub kick: bool,
    pub interval_changed: bool,
    pub background_error: bool,
    pub size: u64,
    pub last_committed_size: u64,
    pub size_touched: bool,
    pub pending_size_valid: bool,
    pub pending_size: u64,
    pub txn_trim: bool,
    pub txn_trim_at: u64,
    pub pending_trim: bool,
    pub pending_trim_at: u64,
    pub touched: HashSet<u64>,
    pub pending: HashMap<u64, Record>,
    pub cache: HashMap<u64, CachedPage>,
    pub lru: BTreeMap<(u64, u64), ()>,
    pub lru_counter: u64,
    pub locks: HashMap<usize, c_int>,
}

impl Guarded {
    fn touch(&mut self, id: u64) {
        let front = self.lru.last_key_value().map(|(k, _)| *k);
        if let Some(page) = self.cache.get_mut(&id) {
            if Some(page.lru_key) == front {
                return;
            }
            self.lru.remove(&page.lru_key);
            self.lru_counter += 1;
            page.lru_key = (self.lru_counter, id);
            self.lru.insert(page.lru_key, ());
        }
    }

    fn insert(&mut self, id: u64, record: Record, dirty: bool) {
        if let Some(page) = self.cache.get_mut(&id) {
            if !page.dirty {
                self.clean_count -= 1;
            }
            page.record = record;
            page.dirty = dirty;
            if !dirty {
                self.clean_count += 1;
            }
            self.touch(id);
        } else {
            self.lru_counter += 1;
            let key = (self.lru_counter, id);
            self.lru.insert(key, ());
            self.cache.insert(id, CachedPage { record, dirty, lru_key: key });
            if !dirty {
                self.clean_count += 1;
            }
        }
        self.evict();
    }

    fn evict(&mut self) {
        // Mirror the C++ back-to-front scan: evict clean entries oldest
        // first, skipping dirty ones, until the cache fits or no clean
        // entry remains.
        if self.clean_count == 0 || self.cache.len() <= self.settings.cache_pages {
            return;
        }
        let mut victims = Vec::new();
        for (key, _) in self.lru.iter() {
            if self.cache.len() - victims.len() <= self.settings.cache_pages {
                break;
            }
            if matches!(self.cache.get(&key.1), Some(page) if !page.dirty) {
                victims.push(*key);
            }
        }
        for key in victims {
            self.lru.remove(&key);
            self.cache.remove(&key.1);
            self.clean_count -= 1;
        }
    }
}

/// Point-in-time key snapshot for one decrypt/encode. RAII-wiped.
pub struct PageKeys {
    pub page: [u8; 32],
    pub master: [u8; 32],
    pub active: [u8; 32],
    pub current: u32,
    pub sealed: bool,
    pub retired: Vec<(u32, [u8; 32])>,
}

impl Drop for PageKeys {
    fn drop(&mut self) {
        self.page.zeroize();
        self.master.zeroize();
        self.active.zeroize();
        for (_, key) in self.retired.iter_mut() {
            key.zeroize();
        }
    }
}

impl Guarded {
    pub fn snapshot_page_keys(&self) -> PageKeys {
        let sealed = self.keyring.is_some();
        let mut snap = PageKeys {
            page: [0u8; 32],
            master: self.settings.key,
            active: [0u8; 32],
            current: 0,
            sealed,
            retired: Vec::new(),
        };
        if sealed {
            let ring = self.keyring.as_ref().expect("sealed means a ring");
            snap.active = self.active_key;
            snap.page = self.active_key;
            snap.current = ring.current;
            for entry in ring.keys.iter() {
                if entry.active {
                    continue;
                }
                snap.retired.push((entry.id, entry.key));
            }
        } else {
            snap.page = self.settings.key;
        }
        snap
    }
}

/// Look up unlocked material (mirrors `sqlite_murmur_keyring_current`:
/// linear scan for `id == ring.current`, no active-flag check).
pub fn keyring_current(ring: &Keyring) -> Option<(u32, &[u8; 32])> {
    ring.keys.iter().find(|k| k.id == ring.current).map(|k| (k.id, &k.key))
}

pub fn keyring_by_id(ring: &Keyring, id: u32) -> Option<&[u8; 32]> {
    ring.keys.iter().find(|k| k.id == id).map(|k| &k.key)
}

/// Keyed (EXP3/EXP4) records take the direct-id path — unknown or zero
/// id fails closed with no trial backstop. Legacy records keep the trial
/// chain (page key, then retired ring keys, then the master).
pub fn decrypt_page(
    cipher: Cipher,
    page: u64,
    record: &[u8],
    out: &mut [u8; BLOCK],
    workspace: &mut DecodeWorkspace,
    keys: &PageKeys,
) -> bool {
    if let Some(id) = crypto::record_key_id(record) {
        if id == 0 {
            return false;
        }
        let mut direct: Option<&[u8; 32]> = None;
        if keys.sealed && id == keys.current {
            direct = Some(&keys.active);
        }
        if direct.is_none() {
            direct = keys.retired.iter().find(|(rid, _)| *rid == id).map(|(_, key)| key);
        }
        let Some(key) = direct else {
            return false;
        };
        return crypto::decrypt(cipher, key, page, record, out, workspace);
    }
    if crypto::decrypt(cipher, &keys.page, page, record, out, workspace) {
        return true;
    }
    if !keys.sealed {
        return false;
    }
    for (_, key) in keys.retired.iter() {
        if crypto::decrypt(cipher, key, page, record, out, workspace) {
            return true;
        }
    }
    crypto::decrypt(cipher, &keys.master, page, record, out, workspace)
}

pub fn json_escape(input: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::new();
    for &c in input {
        match c {
            b'"' => out.push_str("\\\""),
            b'\\' => out.push_str("\\\\"),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            _ if c < 0x20 => {
                out.push_str("\\u00");
                out.push(HEX[(c >> 4) as usize] as char);
                out.push(HEX[(c & 15) as usize] as char);
            }
            _ => out.push(c as char),
        }
    }
    out
}

pub struct Store {
    pub(crate) mutation_mu: Mutex<()>,
    pub(crate) mu: Mutex<Guarded>,
    pub(crate) flush_mu: Mutex<FlushState>,
    pub(crate) backup_mu: Mutex<()>,
    pub(crate) flush_request_mu: Mutex<()>,
    pub(crate) record_pool: Arc<RecordPool>,
    pub(crate) workspace_pool: Arc<WorkspacePool>,
    pub(crate) page_pool: Arc<PagePool>,
    pub journal_pool: Arc<JournalPool>,
    pub(crate) db: OnceLock<Db>,
    pub(crate) db_options: OnceLock<StoreOptions>,
    pub(crate) block_cache: OnceLock<Cache>,
    pub(crate) cv: Condvar,
    pub(crate) flush_done: Condvar,
    pub(crate) write_space: Condvar,
    pub(crate) worker: Mutex<Option<JoinHandle<()>>>,
}

impl Store {
    pub fn new(cfg: Settings) -> Self {
        let page_retention = cfg.cache_pages;
        Self {
            mutation_mu: Mutex::new(()),
            mu: Mutex::new(Guarded {
                settings: cfg,
                counters: sqlite_murmur_stats::default(),
                generation: 0,
                keyring: None,
                active_key: [0u8; 32],
                keyring_store_id: [0u8; 16],
                staged_kdf: crate::abi::KDF_DIRECT_HKDF,
                staged_time: crate::abi::ARGON_TIME_DEFAULT,
                staged_memory_kib: crate::abi::ARGON_MEMORY_KIB_DEFAULT,
                staged_threads: crate::abi::ARGON_THREADS_DEFAULT,
                clean_count: 0,
                requested_flush: 0,
                completed_flush: 0,
                flush_result: SQLITE_OK,
                in_flight_pages: 0,
                stopping: false,
                kick: false,
                interval_changed: false,
                background_error: false,
                size: 0,
                last_committed_size: 0,
                size_touched: false,
                pending_size_valid: false,
                pending_size: 0,
                txn_trim: false,
                txn_trim_at: 0,
                pending_trim: false,
                pending_trim_at: 0,
                touched: HashSet::new(),
                pending: HashMap::new(),
                cache: HashMap::new(),
                lru: BTreeMap::new(),
                lru_counter: 0,
                locks: HashMap::new(),
            }),
            flush_mu: Mutex::new(FlushState { encoders: None, failed_batch: None }),
            backup_mu: Mutex::new(()),
            flush_request_mu: Mutex::new(()),
            record_pool: Arc::new(RecordPool::new(RECORD_POOL_LIMIT)),
            workspace_pool: Arc::new(WorkspacePool::new(WORKSPACE_POOL_LIMIT)),
            page_pool: Arc::new(PagePool::new(page_retention)),
            journal_pool: Arc::new(JournalPool::new(JOURNAL_POOL_LIMIT)),
            db: OnceLock::new(),
            db_options: OnceLock::new(),
            block_cache: OnceLock::new(),
            cv: Condvar::new(),
            flush_done: Condvar::new(),
            write_space: Condvar::new(),
            worker: Mutex::new(None),
        }
    }

    pub(crate) fn db(&self) -> &Db {
        self.db.get().expect("store started")
    }

    fn new_record(&self) -> PoolLease<Vec<u8>> {
        let mut buffer = self.record_pool.acquire_unique();
        buffer.reserve(BLOCK + 69);
        buffer
    }

    /// Open (or create) the RocksDB backend and start the worker. Runs
    /// before the store is published; all fallible steps precede the
    /// thread spawn. Returns false on any failure (caller maps to
    /// `SQLITE_CANTOPEN`, like a cipher mismatch).
    pub fn start(self: &Arc<Self>, filename: &std::path::Path, create: bool) -> bool {
        let (options, cache) = StoreOptions::for_store(create, true, ROCKS_BLOCK_CACHE_BYTES);
        let db = match Db::open(filename, &options) {
            Ok(db) => db,
            Err(_) => return false,
        };
        let ro = ReadOptions::new();
        match db.get(&ro, META_SIZE) {
            Ok(Some(value)) => match parse_meta_size(&value) {
                Some(size) => lock(&self.mu).size = size,
                None => return false,
            },
            Ok(None) => {}
            Err(_) => return false,
        }
        match db.get(&ro, META_CIPHER) {
            Ok(Some(value)) => {
                let cipher = match value.as_slice() {
                    b"aes256gcm" => Cipher::Aes256Gcm,
                    #[cfg(feature = "aegis256")]
                    b"aegis256" => Cipher::Aegis256,
                    _ => return false,
                };
                lock(&self.mu).settings.cipher = cipher;
            }
            Ok(None) => {}
            Err(_) => return false,
        }
        match db.get(&ro, META_KEYRING) {
            Ok(Some(envelope)) => {
                let store_id = match db.get(&ro, META_STORE_ID) {
                    Ok(Some(id)) if id.len() == 16 => id,
                    _ => return false,
                };
                let mut binding = [0u8; 16];
                binding.copy_from_slice(&store_id);
                // Pre-publish: uncontended, so hold the guard and pass the
                // master by reference (no stack copy of key material).
                let mut guarded = lock(&self.mu);
                let ring = match keyring::open(
                    &envelope,
                    Some(&guarded.settings.key),
                    None,
                    &binding,
                    &binding,
                ) {
                    Ok(ring) => ring,
                    Err(_) => return false,
                };
                let Some((_, active)) = keyring_current(&ring) else {
                    return false;
                };
                let mut fresh = [0u8; 32];
                fresh.copy_from_slice(active);
                guarded.keyring_store_id = binding;
                guarded.keyring = Some(ring);
                guarded.active_key = fresh;
                fresh.zeroize();
            }
            Ok(None) => {}
            Err(_) => return false,
        }
        {
            let mut guarded = lock(&self.mu);
            guarded.last_committed_size = guarded.size;
        }
        let _ = self.db.set(db);
        let _ = self.db_options.set(options);
        let _ = self.block_cache.set(cache);
        let threads = lock(&self.mu).settings.background_threads;
        let encoders = match EncodingPool::new(threads) {
            Ok(pool) => pool,
            Err(_) => return false,
        };
        lock(&self.flush_mu).encoders = Some(encoders);
        // The worker borrows the store like the C++ `this` capture: it
        // must NOT hold a strong `Arc` (that would cycle and `Drop`
        // would never run). SAFETY: the handle is stored below before
        // `start` returns, `Drop` always stops and joins it before any
        // field drops, and `start` runs at most once per store (second
        // call fails closed; the C++ would `terminate` on reassignment).
        if lock(&self.worker).is_some() {
            return false;
        }
        // Address as `usize` (plainly `Send`); reconstructed inside.
        let this = Arc::as_ptr(self) as usize;
        // SAFETY: see above; the closure only dereferences while joined.
        let worker = std::thread::Builder::new().spawn(move || {
            let this = unsafe { &*(this as *const Store) };
            let mut guarded = lock_salvage(&this.mu);
            loop {
                let interval = guarded.settings.interval_ms;
                let (guard, _) = this
                    .cv
                    .wait_timeout_while(
                        guarded,
                        std::time::Duration::from_millis(u64::from(interval)),
                        |g| !g.kick && !g.stopping && !g.interval_changed,
                    )
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                guarded = guard;
                if guarded.interval_changed && !guarded.kick && !guarded.stopping {
                    guarded.interval_changed = false;
                    continue;
                }
                guarded.interval_changed = false;
                guarded.kick = false;
                let goal = guarded.requested_flush;
                let finish = guarded.stopping;
                drop(guarded);
                let rc = this.persist();
                guarded = lock_salvage(&this.mu);
                guarded.background_error = rc != SQLITE_OK;
                if goal > guarded.completed_flush {
                    guarded.completed_flush = goal;
                    guarded.flush_result = rc;
                    this.flush_done.notify_all();
                }
                this.write_space.notify_all();
                if finish {
                    break;
                }
            }
        });
        match worker {
            Ok(handle) => *lock(&self.worker) = Some(handle),
            Err(_) => return false,
        }
        true
    }

    pub(crate) fn cipher(&self) -> Cipher {
        lock(&self.mu).settings.cipher
    }

    /// Set-then-readback under one lock hold (like the C++ PRAGMA
    /// bodies, which serialize the pair against concurrent setters).
    pub(crate) fn compression_level(&self, set: Option<i32>) -> i32 {
        let _serial = lock(&self.flush_mu);
        let mut guarded = lock(&self.mu);
        if let Some(level) = set {
            guarded.settings.compression_level = level;
        }
        guarded.settings.compression_level
    }

    pub(crate) fn flush_interval(&self, set: Option<u32>) -> u32 {
        let mut guarded = lock(&self.mu);
        if let Some(interval_ms) = set {
            guarded.settings.interval_ms = interval_ms;
            guarded.interval_changed = true;
            self.cv.notify_one();
        }
        guarded.settings.interval_ms
    }

    pub(crate) fn cipher_name(&self) -> &'static str {
        crypto::cipher_name(self.cipher())
    }

    pub fn set_cipher(&self, cipher: Cipher) -> c_int {
        let _mutation = lock(&self.mutation_mu);
        let _serial = lock(&self.flush_mu);
        let sync;
        {
            let guarded = lock(&self.mu);
            if guarded.settings.cipher == cipher {
                return SQLITE_OK;
            }
            if guarded.size != 0
                || !guarded.cache.is_empty()
                || !guarded.pending.is_empty()
                || guarded.locks.values().any(|&level| level > SQLITE_LOCK_NONE)
            {
                return SQLITE_BUSY;
            }
            sync = guarded.settings.rocksdb_sync;
        }
        let opts = WriteOptions::with_sync(sync);
        if self.db().put(&opts, META_CIPHER, crypto::cipher_name(cipher).as_bytes()).is_err() {
            return SQLITE_IOERR_WRITE;
        }
        let mut guarded = lock(&self.mu);
        guarded.settings.cipher = cipher;
        guarded.generation += 1;
        SQLITE_OK
    }
}

struct BlockVersion {
    id: u64,
    record: Record,
}

/// Small-buffer versions: 16 inline like the C++ (covers normal SQLite
/// reads/writes without a heap allocation), spilling to a `Vec`.
struct BlockVersions {
    small: [BlockVersion; 16],
    large: Vec<BlockVersion>,
    count: usize,
}

impl BlockVersions {
    fn new() -> Self {
        Self {
            small: std::array::from_fn(|_| BlockVersion { id: 0, record: Record::Vacant }),
            large: Vec::new(),
            count: 0,
        }
    }

    fn resize(&mut self, n: usize) {
        if n > self.small.len() {
            self.large.clear();
            self.large.reserve(n);
            for _ in 0..n {
                self.large.push(BlockVersion { id: 0, record: Record::Vacant });
            }
        }
        self.count = n;
    }

    fn slot(&mut self, n: usize) -> &mut BlockVersion {
        if self.count > self.small.len() { &mut self.large[n] } else { &mut self.small[n] }
    }

    fn reset_all(&mut self) {
        for i in 0..self.count {
            self.slot(i).record = Record::Vacant;
        }
    }
}

impl Store {
    // `mutation_mu` is held by the caller; crypto and IO run without `mu`.
    fn load_for_mutation(&self, id: u64, workspace: &mut DecodeWorkspace, cipher: Cipher) -> c_int {
        let (record, current_size, keys) = {
            let guarded = lock(&self.mu);
            let record = guarded.cache.get(&id).map(|page| page.record.clone()).unwrap_or_default();
            (record, guarded.size, guarded.snapshot_page_keys())
        };
        if id.checked_mul(BLOCK as u64).map_or(true, |start| start >= current_size) {
            workspace.block_mut().fill(0);
            return SQLITE_OK;
        }
        let record = match record {
            Record::Vacant => {
                let mut buffer = self.new_record();
                match self.db().get_into(&ReadOptions::new(), &page_key(id), &mut buffer) {
                    Err(_) => return SQLITE_IOERR_READ,
                    Ok(false) => {
                        workspace.block_mut().fill(0);
                        return SQLITE_OK;
                    }
                    Ok(true) => Record::Encrypted(buffer.share()),
                }
            }
            other => other,
        };
        if let Some(plain) = record.plain() {
            *workspace.block_mut() = plain.bytes;
            workspace.mark_block_dirty();
        } else {
            // Decrypt via a local: `out` and `workspace` overlap in the
            // C++ (`workspace.block` + `workspace`), which the borrow
            // checker cannot express. One extra 4 KB copy on the
            // partial-block path; the local wipes before returning.
            let mut block = [0u8; BLOCK];
            let ok = decrypt_page(cipher, id, record.encrypted(), &mut block, workspace, &keys);
            if ok {
                *workspace.block_mut() = block;
            }
            crate::secure_wipe(&mut block);
            if !ok {
                return SQLITE_IOERR_READ;
            }
        }
        if (current_size - id * BLOCK as u64) < BLOCK as u64 {
            let from = (current_size % BLOCK as u64) as usize;
            workspace.block_mut()[from..].fill(0);
        }
        SQLITE_OK
    }

    pub fn read(&self, dst: &mut [u8], offset: i64) -> c_int {
        if offset < 0 || dst.len() > i32::MAX as usize {
            return SQLITE_IOERR_READ;
        }
        let count = dst.len() as u64;
        if count == 0 {
            return SQLITE_OK;
        }
        let begin = offset as u64;
        let maximum_blocks = ((begin % BLOCK as u64) + count + BLOCK as u64 - 1) / BLOCK as u64;
        let mut versions = BlockVersions::new();
        versions.resize(maximum_blocks as usize);
        let mut workspace: Option<PoolLease<DecodeWorkspace>> = None;
        loop {
            // Release stale snapshots outside the metadata lock.
            versions.reset_all();
            let (epoch, avail, blocks, cipher, keys, ids) = {
                let guarded = lock(&self.mu);
                let avail = if begin < guarded.size {
                    count.min(guarded.size - begin)
                } else {
                    0
                };
                let blocks = if avail > 0 {
                    ((begin % BLOCK as u64) + avail + BLOCK as u64 - 1) / BLOCK as u64
                } else {
                    0
                };
                let mut ids = Vec::with_capacity(blocks as usize);
                for i in 0..blocks {
                    let id = begin / BLOCK as u64 + i;
                    ids.push(id);
                    let version = versions.slot(i as usize);
                    version.id = id;
                    if let Some(page) = guarded.cache.get(&id) {
                        version.record = page.record.clone();
                    }
                }
                (
                    guarded.generation,
                    avail,
                    blocks,
                    guarded.settings.cipher,
                    guarded.snapshot_page_keys(),
                    ids,
                )
            };
            let mut rc = SQLITE_OK;
            let mut cursor = begin;
            let mut remaining = avail;
            let mut out_at = 0usize;
            for (i, id) in ids.iter().enumerate() {
                let version = versions.slot(i);
                if !version.record.is_some() {
                    let mut buffer = self.new_record();
                    match self.db().get_into(&ReadOptions::new(), &page_key(*id), &mut buffer) {
                        Err(_) => {
                            rc = SQLITE_IOERR_READ;
                            break;
                        }
                        Ok(true) => version.record = Record::Encrypted(buffer.share()),
                        Ok(false) => {}
                    }
                }
                let within = (cursor % BLOCK as u64) as usize;
                let n = (remaining.min(BLOCK as u64 - within as u64)) as usize;
                if let Some(plain) = version.record.plain() {
                    dst[out_at..out_at + n].copy_from_slice(&plain.bytes[within..within + n]);
                } else if version.record.is_some() {
                    if workspace.is_none() {
                        workspace = Some(self.workspace_pool.acquire_unique());
                    }
                    let ws = workspace.as_mut().expect("just acquired");
                    // Borrow split: decrypt into a local, then copy (the
                    // C++ decrypts into the workspace block and copies).
                    let mut block = [0u8; BLOCK];
                    if !decrypt_page(cipher, *id, version.record.encrypted(), &mut block, ws, &keys) {
                        crate::secure_wipe(&mut block);
                        rc = SQLITE_IOERR_READ;
                        break;
                    }
                    dst[out_at..out_at + n].copy_from_slice(&block[within..within + n]);
                    crate::secure_wipe(&mut block);
                } else {
                    dst[out_at..out_at + n].fill(0);
                }
                out_at += n;
                cursor += n as u64;
                remaining -= n as u64;
            }
            {
                let mut guarded = lock(&self.mu);
                if guarded.generation != epoch {
                    continue; // Also retries IO/auth failures from a raced version.
                }
                if rc != SQLITE_OK {
                    return rc;
                }
                // Admit only authenticated current-generation results.
                for i in 0..blocks as usize {
                    let version = versions.slot(i);
                    if !version.record.is_some() {
                        continue;
                    }
                    if guarded.cache.contains_key(&version.id) {
                        let id = version.id;
                        guarded.touch(id);
                    } else if version.record.plain().is_none() {
                        let id = version.id;
                        let record = std::mem::replace(&mut version.record, Record::Vacant);
                        guarded.insert(id, record, false);
                    }
                }
            }
            if avail < count {
                dst[avail as usize..count as usize].fill(0);
                return crate::ffi_sqlite::SQLITE_IOERR_SHORT_READ;
            }
            return SQLITE_OK;
        }
    }

    pub fn write(&self, src: &[u8], offset: i64) -> c_int {
        if offset < 0 || src.len() > i32::MAX as usize || offset as u64 > u64::MAX - src.len() as u64
        {
            return SQLITE_IOERR_WRITE;
        }
        let count = src.len() as u64;
        if count == 0 {
            return SQLITE_OK;
        }
        let _mutation = lock(&self.mutation_mu);
        let cipher = {
            let mut guarded = lock(&self.mu);
            if !guarded.stopping
                && guarded.pending.len() + guarded.in_flight_pages >= guarded.settings.cache_pages
            {
                guarded.kick = true;
                self.cv.notify_one();
                guarded = self
                    .write_space
                    .wait_while(guarded, |g| {
                        !g.stopping
                            && !g.background_error
                            && g.pending.len() + g.in_flight_pages >= g.settings.cache_pages
                    })
                    .expect("store mutex poisoned");
            }
            if guarded.background_error {
                return SQLITE_IOERR_WRITE;
            }
            guarded.settings.cipher
        };
        let mut cursor = offset as u64;
        let mut left = count as usize;
        let mut input_at = 0usize;
        let blocks = ((cursor % BLOCK as u64) + count + BLOCK as u64 - 1) / BLOCK as u64;
        let mut replacements = BlockVersions::new();
        replacements.resize(blocks as usize);
        let mut workspace = self.workspace_pool.acquire_unique();
        for i in 0..blocks as usize {
            let id = cursor / BLOCK as u64;
            let within = (cursor % BLOCK as u64) as usize;
            let n = left.min(BLOCK - within);
            let mut buffer = self.page_pool.acquire_unique();
            if within != 0 || n != BLOCK {
                let rc = self.load_for_mutation(id, &mut workspace, cipher);
                if rc != SQLITE_OK {
                    return rc;
                }
                buffer.bytes = *workspace.block();
                workspace.wipe_block();
            }
            buffer.bytes[within..within + n].copy_from_slice(&src[input_at..input_at + n]);
            let version = replacements.slot(i);
            version.id = id;
            version.record = Record::Plain(buffer.share());
            input_at += n;
            cursor += n as u64;
            left -= n;
        }
        let mut guarded = lock(&self.mu);
        if guarded.background_error {
            return SQLITE_IOERR_WRITE;
        }
        guarded.generation += 1;
        for i in 0..blocks as usize {
            let version = replacements.slot(i);
            let id = version.id;
            let record = std::mem::replace(&mut version.record, Record::Vacant);
            guarded.touched.insert(id);
            guarded.insert(id, record, true);
            guarded.counters.vfs_block_writes += 1;
        }
        guarded.size = guarded.size.max(cursor);
        guarded.counters.vfs_write_bytes += count;
        guarded.size_touched = true;
        if guarded.touched.len() > guarded.settings.cache_pages / 2 {
            guarded.kick = true;
            self.cv.notify_one();
        }
        SQLITE_OK
    }

    pub fn truncate(&self, length: i64) -> c_int {
        if length < 0 {
            return SQLITE_IOERR_TRUNCATE;
        }
        let _mutation = lock(&self.mutation_mu);
        let target = length as u64;
        let (old_size, cipher) = {
            let guarded = lock(&self.mu);
            if guarded.background_error {
                return SQLITE_IOERR_TRUNCATE;
            }
            (guarded.size, guarded.settings.cipher)
        };
        let mut tail = Record::Vacant;
        if target < old_size && target % BLOCK as u64 != 0 {
            let mut workspace = self.workspace_pool.acquire_unique();
            let rc = self.load_for_mutation(target / BLOCK as u64, &mut workspace, cipher);
            if rc != SQLITE_OK {
                return rc;
            }
            let from = (target % BLOCK as u64) as usize;
            workspace.block_mut()[from..].fill(0);
            let mut buffer = self.page_pool.acquire_unique();
            buffer.bytes = *workspace.block();
            workspace.wipe_block();
            tail = Record::Plain(buffer.share());
        }
        let mut guarded = lock(&self.mu);
        if guarded.background_error {
            return SQLITE_IOERR_TRUNCATE;
        }
        guarded.generation += 1;
        if tail.is_some() {
            guarded.touched.insert(target / BLOCK as u64);
            guarded.insert(target / BLOCK as u64, tail, true);
        }
        if target < old_size {
            let first_gone = (target + BLOCK as u64 - 1) / BLOCK as u64;
            if !guarded.txn_trim || first_gone < guarded.txn_trim_at {
                guarded.txn_trim_at = first_gone;
            }
            guarded.txn_trim = true;
            guarded.touched.retain(|id| *id < first_gone);
            let goners: Vec<u64> = guarded.cache.keys().filter(|id| **id >= first_gone).copied().collect();
            for id in goners {
                if let Some(page) = guarded.cache.remove(&id) {
                    if !page.dirty {
                        guarded.clean_count -= 1;
                    }
                    guarded.lru.remove(&page.lru_key);
                }
            }
        }
        guarded.size = target;
        guarded.size_touched = true;
        SQLITE_OK
    }

    pub fn complete_transaction(&self) -> c_int {
        let _mutation = lock(&self.mutation_mu);
        let mut guarded = lock(&self.mu);
        // Allocate missing map nodes before publishing any part of this commit.
        let mut additions: HashMap<u64, Record> = HashMap::new();
        for id in guarded.touched.iter() {
            if let Some(page) = guarded.cache.get(id) {
                if !guarded.pending.contains_key(id)
                    || (guarded.txn_trim && *id >= guarded.txn_trim_at)
                {
                    additions.insert(*id, page.record.clone());
                }
            }
        }
        guarded.pending.reserve(additions.len());
        if guarded.txn_trim {
            let at = guarded.txn_trim_at;
            guarded.pending.retain(|id, _| *id < at);
        }
        let touched: Vec<u64> = guarded.touched.iter().copied().collect();
        for id in touched {
            let Some(page) = guarded.cache.get(&id) else {
                continue;
            };
            let record = page.record.clone();
            if let Some(slot) = guarded.pending.get_mut(&id) {
                *slot = record;
            } else {
                let moved = additions.remove(&id).expect("addition preallocated");
                guarded.pending.insert(id, moved);
            }
        }
        guarded.touched.clear();
        if guarded.size_touched || guarded.txn_trim {
            guarded.pending_size = guarded.size;
            guarded.pending_size_valid = true;
            guarded.last_committed_size = guarded.size;
        }
        if guarded.txn_trim {
            if !guarded.pending_trim || guarded.txn_trim_at < guarded.pending_trim_at {
                guarded.pending_trim_at = guarded.txn_trim_at;
            }
            guarded.pending_trim = true;
        }
        guarded.txn_trim = false;
        guarded.size_touched = false;
        if (!guarded.pending.is_empty() || guarded.pending_size_valid)
            && guarded.pending.len() + guarded.in_flight_pages >= guarded.settings.cache_pages
        {
            guarded.kick = true;
            self.cv.notify_one();
        }
        SQLITE_OK
    }

    /// Public persistence barrier: dispatch to the background coordinator
    /// and wait for the goal.
    pub fn flush(&self) -> c_int {
        let _request = lock(&self.flush_request_mu);
        let mut guarded = lock(&self.mu);
        guarded.requested_flush += 1;
        let goal = guarded.requested_flush;
        guarded.kick = true;
        self.cv.notify_one();
        guarded = self
            .flush_done
            .wait_while(guarded, |g| g.completed_flush < goal)
            .expect("store mutex poisoned");
        guarded.flush_result
    }

    pub fn get_stats(&self, out: &mut sqlite_murmur_stats) -> c_int {
        let _serial = lock(&self.flush_mu);
        let guarded = lock(&self.mu);
        *out = guarded.counters;
        out.logical_size_bytes = guarded.last_committed_size;
        let options = self.db_options.get().expect("store started");
        out.rocksdb_wal_bytes = options.ticker(crate::ffi_rocksdb::TICKER_WAL_FILE_BYTES);
        out.rocksdb_flush_bytes = options.ticker(crate::ffi_rocksdb::TICKER_FLUSH_WRITE_BYTES);
        out.rocksdb_compaction_bytes =
            options.ticker(crate::ffi_rocksdb::TICKER_COMPACT_WRITE_BYTES);
        SQLITE_OK
    }

    pub fn checkpoint(&self) -> c_int {
        let rc = self.flush();
        if rc != SQLITE_OK {
            return rc;
        }
        let _serial = lock(&self.flush_mu);
        if self.db().flush_wait().is_err() {
            return SQLITE_IOERR_WRITE;
        }
        match self.db().wait_for_compact_30s() {
            Ok(()) => SQLITE_OK,
            Err(msg) if crate::ffi_rocksdb::is_timed_out(&msg) => SQLITE_BUSY,
            Err(_) => SQLITE_IOERR_WRITE,
        }
    }

    /// Drain committed pages into RocksDB. Holds `flush_mu` throughout
    /// (like the C++); `mu` is taken briefly to drain/reconcile.
    pub(crate) fn persist(&self) -> c_int {
        let mut serial = lock_salvage(&self.flush_mu);
        let passes = if serial.failed_batch.is_some() { 2 } else { 1 };
        for _ in 0..passes {
            let work = match serial.failed_batch.take() {
                Some(batch) => batch,
                None => {
                    let mut batch = FlushBatch {
                        pages: HashMap::new(),
                        trim: false,
                        have_size: false,
                        trim_from: 0,
                        new_size: 0,
                    };
                    {
                        let mut guarded = lock_salvage(&self.mu);
                        if guarded.pending.is_empty() && !guarded.pending_size_valid {
                            return SQLITE_OK;
                        }
                        std::mem::swap(&mut batch.pages, &mut guarded.pending);
                        guarded.in_flight_pages = batch.pages.len();
                        batch.trim = guarded.pending_trim;
                        batch.trim_from = guarded.pending_trim_at;
                        guarded.pending_trim = false;
                        batch.have_size = guarded.pending_size_valid;
                        batch.new_size = guarded.pending_size;
                        guarded.pending_size_valid = false;
                    }
                    batch
                }
            };
            let (cipher, level, persist_keys, sync) = {
                // Brief `mu` where the C++ reads lock-free under
                // `flushMu` (same order; seal/rotate/cipher/compression
                // changes all exclude `flushMu` holders).
                let guarded = lock_salvage(&self.mu);
                let keys = guarded.snapshot_page_keys();
                (guarded.settings.cipher, guarded.settings.compression_level, keys, guarded.settings.rocksdb_sync)
            };
            let job = Arc::new(EncodeJob {
                inputs: work.pages.iter().map(|(id, record)| (*id, record.clone())).collect(),
                outputs: (0..work.pages.len()).map(|_| Mutex::new(None)).collect(),
                cipher,
                level,
                keys: persist_keys,
                record_pool: Arc::clone(&self.record_pool),
                failed: std::sync::atomic::AtomicBool::new(false),
            });
            let encoders = serial.encoders.as_ref().expect("encoders installed in start");
            encoders.run(job.inputs.len(), Arc::new({
                let job = Arc::clone(&job);
                move |index: usize, workspace: &mut crate::pool::EncoderWorkspace| {
                    job.encode_one(index, workspace);
                }
            }));
            let mut rc = SQLITE_OK;
            let mut cleaned: Vec<(u64, Record)> = Vec::new();
            let mut payload_bytes: u64 = 0;
            let mut record_bytes: u64 = 0;
            let mut batch = WriteBatch::new();
            if job.failed.load(std::sync::atomic::Ordering::Relaxed) {
                rc = SQLITE_IOERR_WRITE;
            }
            if work.trim {
                batch.delete_range(&page_key(work.trim_from), END_KEY);
            }
            cleaned.reserve(job.inputs.len());
            if rc == SQLITE_OK {
                for (index, (id, _)) in job.inputs.iter().enumerate() {
                    let slot = job.outputs[index].lock().expect("encode mutex poisoned").take();
                    let Some((encoded, payload)) = slot else {
                        rc = SQLITE_IOERR_WRITE;
                        break;
                    };
                    payload_bytes += payload as u64;
                    record_bytes += encoded.encrypted().len() as u64;
                    batch.put(&page_key(*id), encoded.encrypted());
                    cleaned.push((*id, encoded));
                }
            }
            if rc == SQLITE_OK {
                if work.have_size {
                    batch.put(META_SIZE, work.new_size.to_string().as_bytes());
                }
                let options = WriteOptions::with_sync(sync);
                if self.db().write(&options, &mut batch).is_err() {
                    rc = SQLITE_IOERR_WRITE;
                }
            }
            if rc != SQLITE_OK {
                serial.failed_batch = Some(work);
                let mut guarded = lock_salvage(&self.mu);
                guarded.background_error = true;
                self.write_space.notify_all();
                return rc;
            }
            {
                let mut guarded = lock_salvage(&self.mu);
                guarded.counters.flush_batches += 1;
                guarded.counters.persisted_blocks += work.pages.len() as u64;
                guarded.counters.uncompressed_bytes += work.pages.len() as u64 * BLOCK as u64;
                guarded.counters.compressed_payload_bytes += payload_bytes;
                guarded.counters.encrypted_record_bytes += record_bytes;
                for (id, encoded) in cleaned {
                    let matches = match (guarded.cache.get(&id), work.pages.get(&id)) {
                        (Some(cached), Some(original)) => cached.record == *original,
                        _ => false,
                    };
                    if matches {
                        let was_dirty =
                            guarded.cache.get(&id).map(|cached| cached.dirty).unwrap_or(false);
                        if let Some(cached) = guarded.cache.get_mut(&id) {
                            cached.record = encoded;
                            cached.dirty = false;
                        }
                        if was_dirty {
                            guarded.clean_count += 1;
                        }
                    }
                }
                guarded.evict();
                guarded.background_error = false;
                guarded.in_flight_pages = 0;
                self.write_space.notify_all();
            }
        }
        SQLITE_OK
    }
}

/// Owned per-persist encode job (see `EncodingPool::run`): inputs are
/// shared read-only; each index writes its own output slot exactly once.
struct EncodeJob {
    inputs: Vec<(u64, Record)>,
    outputs: Vec<Mutex<Option<(Record, usize)>>>,
    cipher: Cipher,
    level: i32,
    keys: PageKeys,
    record_pool: Arc<RecordPool>,
    failed: std::sync::atomic::AtomicBool,
}

impl EncodeJob {
    fn encode_one(&self, index: usize, workspace: &mut crate::pool::EncoderWorkspace) {
        let (id, original) = &self.inputs[index];
        // Lazy compressor first (the C++ inits it after the decrypt;
        // worker-local disjoint state, so the order is unobservable).
        // Failing here leaves no plaintext behind to wipe.
        if self.level != 0 && workspace.compression.is_none() {
            workspace.compression = crate::ffi_zstd::Cctx::create();
            if workspace.compression.is_none() {
                // C++ throws bad_alloc here (OOM abort in Rust is
                // unreachable-and-fatal; treat as a write failure).
                self.failed.store(true, std::sync::atomic::Ordering::Relaxed);
                return;
            }
        }
        // Decoded-plaintext scratch when the input is still encrypted
        // (the C++ reuses the worker decode block; a stack block wiped
        // at every exit has the shorter, strictly safer lifetime).
        let mut decoded = [0u8; BLOCK];
        let block: &[u8; BLOCK] = if let Some(plain) = original.plain() {
            &plain.bytes
        } else {
            if !decrypt_page(
                self.cipher,
                *id,
                original.encrypted(),
                &mut decoded,
                &mut workspace.decode,
                &self.keys,
            ) {
                crate::secure_wipe(&mut decoded);
                self.failed.store(true, std::sync::atomic::Ordering::Relaxed);
                return;
            }
            &decoded
        };
        let mut n = BLOCK;
        if self.level != 0 {
            let cctx = workspace.compression.as_mut().expect("initialized above");
            match cctx.compress(&mut workspace.scratch, block, self.level) {
                Ok(size) => n = size,
                Err(_) => n = BLOCK,
            }
        }
        // The C++ checks `!ZSTD_isError(n)` on the raw return. The
        // safe wrapper converts errors to `Err`, mapped above to
        // `n = BLOCK` (never `< BLOCK`), so `n < BLOCK` implies success.
        let compressed = self.level != 0 && n < BLOCK;
        let (payload, length) = if compressed {
            (&workspace.scratch[..n], n)
        } else {
            (&block[..], BLOCK)
        };
        let mut encoded = self.record_pool.acquire_unique();
        encoded.reserve(BLOCK + 69);
        let record = match crypto::encrypt(
            self.cipher,
            &self.keys.page,
            *id,
            payload,
            compressed,
            if self.keys.sealed { self.keys.current } else { 0 },
        ) {
            Some(bytes) => {
                encoded.extend_from_slice(&bytes);
                Record::Encrypted(encoded.share())
            }
            None => {
                crate::secure_wipe(&mut decoded);
                self.failed.store(true, std::sync::atomic::Ordering::Relaxed);
                return;
            }
        };
        crate::secure_wipe(&mut decoded);
        *self.outputs[index].lock().expect("encode mutex poisoned") = Some((record, length));
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        {
            let mut guarded = self.mu.lock().unwrap_or_else(|p| p.into_inner());
            guarded.stopping = true;
            self.cv.notify_one();
        }
        if let Some(handle) = self.worker.lock().unwrap_or_else(|p| p.into_inner()).take() {
            let _ = handle.join();
        }
        // Exclusive here (last `Arc`): drop encoders outside any lock.
        if let Ok(mut flush) = self.flush_mu.lock() {
            flush.encoders = None;
        }
        // Wipe key material (the `Keyring` wipes itself on drop).
        if let Ok(mut guarded) = self.mu.lock() {
            guarded.active_key.zeroize();
            guarded.settings.key.zeroize();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi_sqlite::SQLITE_IOERR_SHORT_READ;

    fn test_settings(cache_pages: usize) -> Settings {
        let mut key = [0u8; 32];
        for (i, b) in key.iter_mut().enumerate() {
            *b = i as u8;
        }
        // No timer interference: ~49-day interval, explicit flushes only.
        let mut settings = Settings::new(key, u32::MAX, cache_pages, true);
        settings.background_threads = 2;
        settings
    }

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("murmur-store-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn open(dir: &std::path::Path, settings: Settings) -> Arc<Store> {
        let store = Arc::new(Store::new(settings));
        assert!(store.start(dir, true), "start {dir:?}");
        store
    }

    fn pattern(page: u64) -> Vec<u8> {
        (0..BLOCK).map(|i| (page.wrapping_mul(31).wrapping_add(i as u64)) as u8).collect()
    }

    #[test]
    fn write_read_flush_reopen() {
        let dir = scratch("roundtrip");
        {
            let store = open(&dir, test_settings(64));
            for page in 0..3u64 {
                let data = pattern(page);
                assert_eq!(
                    store.write(&data, (page * BLOCK as u64) as i64),
                    SQLITE_OK
                );
            }
            // Pre-flush reads come from cache.
            for page in 0..3u64 {
                let mut out = vec![0u8; BLOCK];
                assert_eq!(
                    store.read(&mut out, (page * BLOCK as u64) as i64),
                    SQLITE_OK
                );
                assert_eq!(out, pattern(page));
            }
            assert_eq!(store.complete_transaction(), SQLITE_OK);
            assert_eq!(store.flush(), SQLITE_OK);
            let mut stats = sqlite_murmur_stats::default();
            assert_eq!(store.get_stats(&mut stats), SQLITE_OK);
            assert_eq!(stats.flush_batches, 1);
            assert_eq!(stats.persisted_blocks, 3);
            assert_eq!(stats.logical_size_bytes, 3 * BLOCK as u64);
            assert_eq!(stats.vfs_block_writes, 3);
            assert_eq!(stats.vfs_write_bytes, 3 * BLOCK as u64);
            assert!(stats.rocksdb_wal_bytes > 0);
        }
        // Reopen: persisted records decrypt.
        {
            let store = open(&dir, test_settings(64));
            for page in 0..3u64 {
                let mut out = vec![0u8; BLOCK];
                assert_eq!(
                    store.read(&mut out, (page * BLOCK as u64) as i64),
                    SQLITE_OK
                );
                assert_eq!(out, pattern(page));
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn short_read_zero_fills() {
        let dir = scratch("short");
        let store = open(&dir, test_settings(64));
        let mut out = vec![0x7fu8; 100];
        assert_eq!(store.read(&mut out, 0), SQLITE_IOERR_SHORT_READ);
        assert_eq!(out, vec![0u8; 100]);
        // Partial overlap: 10 bytes written, 100 requested.
        assert_eq!(store.write(&[9u8; 10], 0), SQLITE_OK);
        let mut out = vec![0x7fu8; 100];
        assert_eq!(store.read(&mut out, 0), SQLITE_IOERR_SHORT_READ);
        assert_eq!(&out[..10], &[9u8; 10]);
        assert_eq!(&out[10..], &vec![0u8; 90]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn truncate_shrink_grow() {
        let dir = scratch("trunc");
        let store = open(&dir, test_settings(64));
        let mut data = pattern(0);
        data.extend_from_slice(&pattern(1));
        assert_eq!(store.write(&data, 0), SQLITE_OK);
        assert_eq!(store.complete_transaction(), SQLITE_OK);
        assert_eq!(store.truncate(100), SQLITE_OK);
        let mut out = vec![0u8; 100];
        assert_eq!(store.read(&mut out, 0), SQLITE_OK);
        assert_eq!(out, &pattern(0)[..100]);
        // Shrink then grow: the gap reads zeros.
        assert_eq!(store.truncate(9000), SQLITE_OK);
        let mut out = vec![0x7fu8; 9000 - 100];
        assert_eq!(store.read(&mut out, 100), SQLITE_OK);
        assert_eq!(out, vec![0u8; 9000 - 100]);
        assert_eq!(store.complete_transaction(), SQLITE_OK);
        assert_eq!(store.flush(), SQLITE_OK);
        drop(store);
        let store = open(&dir, test_settings(64));
        let mut stats = sqlite_murmur_stats::default();
        assert_eq!(store.get_stats(&mut stats), SQLITE_OK);
        assert_eq!(stats.logical_size_bytes, 9000);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn lru_evicts_clean_oldest_and_skips_dirty() {
        let dir = scratch("lru");
        let store = open(&dir, test_settings(4));
        // Pages 0..4 persisted => all clean.
        for page in 0..4u64 {
            let data = pattern(page);
            assert_eq!(store.write(&data, (page * BLOCK as u64) as i64), SQLITE_OK);
        }
        assert_eq!(store.complete_transaction(), SQLITE_OK);
        assert_eq!(store.flush(), SQLITE_OK);
        {
            let guarded = lock(&store.mu);
            assert_eq!(guarded.clean_count, 4);
        }
        // Two dirty inserts over the limit evict the two oldest clean.
        for page in 4..6u64 {
            let data = pattern(page);
            assert_eq!(store.write(&data, (page * BLOCK as u64) as i64), SQLITE_OK);
        }
        {
            let guarded = lock(&store.mu);
            assert!(!guarded.cache.contains_key(&0));
            assert!(!guarded.cache.contains_key(&1));
            assert!(guarded.cache.contains_key(&2));
            assert!(guarded.cache.contains_key(&3));
            assert_eq!(guarded.clean_count, 2);
        }
        // Touch makes 2 newest; the next eviction takes 3 instead.
        lock(&store.mu).touch(2);
        {
            let data = pattern(6);
            assert_eq!(store.write(&data, 6 * BLOCK as i64), SQLITE_OK);
        }
        {
            let guarded = lock(&store.mu);
            assert!(guarded.cache.contains_key(&2));
            assert!(!guarded.cache.contains_key(&3));
        }
        // All-dirty caches never evict, however far over the limit.
        let dir2 = scratch("lru-dirty");
        let store2 = open(&dir2, test_settings(4));
        for page in 0..10u64 {
            let data = pattern(page);
            assert_eq!(store2.write(&data, (page * BLOCK as u64) as i64), SQLITE_OK);
        }
        {
            let guarded = lock(&store2.mu);
            assert_eq!(guarded.cache.len(), 10);
            assert_eq!(guarded.clean_count, 0);
        }
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dir2);
    }

    #[test]
    fn corrupt_meta_rejects_open() {
        let dir = scratch("corrupt");
        {
            let store = open(&dir, test_settings(64));
            assert_eq!(store.write(&[1u8; 16], 0), SQLITE_OK);
            assert_eq!(store.complete_transaction(), SQLITE_OK);
            assert_eq!(store.flush(), SQLITE_OK);
        }
        // Scribble meta:size via a raw handle.
        {
            let (options, _cache) = StoreOptions::for_store(false, false, 8 << 20);
            let db = Db::open(&dir, &options).expect("raw open");
            db.put(&WriteOptions::with_sync(true), META_SIZE, b"abc").expect("scribble");
        }
        let bad = Arc::new(Store::new(test_settings(64)));
        assert!(!bad.start(&dir, false));
        drop(bad);
        {
            let (options, _cache) = StoreOptions::for_store(false, false, 8 << 20);
            let db = Db::open(&dir, &options).expect("raw open");
            db.put(&WriteOptions::with_sync(true), META_SIZE, b"16").expect("repair");
            db.put(&WriteOptions::with_sync(true), META_CIPHER, b"rot13").expect("scribble");
        }
        let bad = Arc::new(Store::new(test_settings(64)));
        assert!(!bad.start(&dir, false));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_without_create_fails() {
        let dir = scratch("missing-no-create");
        let store = Arc::new(Store::new(test_settings(64)));
        assert!(!store.start(&dir, false));
    }

    #[test]
    fn concurrent_readers_writers_flusher() {
        let dir = scratch("concurrent");
        let store = open(&dir, test_settings(64));
        let mut handles = Vec::new();
        for writer in 0..4u64 {
            let store = Arc::clone(&store);
            handles.push(std::thread::spawn(move || {
                for round in 0..4u64 {
                    let page = writer * 16 + round;
                    let data = pattern(page);
                    assert_eq!(
                        store.write(&data, (page * BLOCK as u64) as i64),
                        SQLITE_OK
                    );
                    assert_eq!(store.complete_transaction(), SQLITE_OK);
                }
            }));
        }
        for _ in 0..2 {
            let store = Arc::clone(&store);
            handles.push(std::thread::spawn(move || {
                for _ in 0..20 {
                    let mut out = vec![0u8; BLOCK];
                    let _ = store.read(&mut out, 0);
                }
            }));
        }
        {
            let store = Arc::clone(&store);
            handles.push(std::thread::spawn(move || {
                for _ in 0..8 {
                    assert_eq!(store.flush(), SQLITE_OK);
                }
            }));
        }
        for handle in handles {
            handle.join().expect("worker panicked");
        }
        assert_eq!(store.flush(), SQLITE_OK);
        for writer in 0..4u64 {
            for round in 0..4u64 {
                let page = writer * 16 + round;
                let mut out = vec![0u8; BLOCK];
                assert_eq!(
                    store.read(&mut out, (page * BLOCK as u64) as i64),
                    SQLITE_OK
                );
                assert_eq!(out, pattern(page), "page {page}");
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
