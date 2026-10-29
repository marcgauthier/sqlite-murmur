//! Keyring control and PRAGMA setters for the store (FCTL 0x535805,
//! `rocksdb_*`, `background_threads`). Ported from `Store::keyringControl`,
//! `Store::rocksdbPragma`, and `Store::backgroundThreadsPragma`.
//!
//! Lock order mirrors `setCipher`: `mutation_mu` -> `flush_mu` -> `mu`;
//! IO/crypto run without `mu` (the prune install holds `mu` during the
//! envelope open exactly like the C++). Result strings are allocated
//! with `sqlite3_malloc64` (SQLite frees them); SQLite pre-zeroes the
//! PRAGMA out-param (`aFcntl[0] = 0`), so every path assigns it.

use std::ffi::{CString, c_char, c_int};
use zeroize::{Zeroize, Zeroizing};

use crate::abi::{
    KDF_ARGON2ID, KDF_DIRECT_HKDF, KEYRING_GET, KEYRING_INVENTORY, KEYRING_PARAM_KDF,
    KEYRING_PARAM_MEMORY, KEYRING_PARAM_THREADS, KEYRING_PARAM_TIME, KEYRING_PRUNE,
    KEYRING_ROTATE, KEYRING_SET, sqlite_murmur_keyring_args,
};
use crate::crypto::{is_legacy_record, page_key, record_key_id};
use crate::ffi_rocksdb::{ReadOptions, WriteOptions, is_io_error};
use crate::ffi_sqlite::{
    SQLITE_BUSY, SQLITE_CORRUPT, SQLITE_ERROR, SQLITE_IOERR_READ, SQLITE_IOERR_WRITE,
    SQLITE_MISUSE, SQLITE_NOMEM, SQLITE_NOTFOUND, SQLITE_OK, SQLITE_READONLY, sqlite3_malloc64,
};
use crate::keyring;
use crate::pool::EncodingPool;
use crate::store::{
    META_KEYRING, META_STORE_ID, Store, json_escape, keyring_current, lock, parse_unsigned,
};

/// Allocate `text` plus NUL via `sqlite3_malloc64` (null on failure).
pub(crate) fn malloc_string(text: &str) -> *mut c_char {
    let bytes = text.as_bytes();
    let Ok(len) = u64::try_from(bytes.len() + 1) else {
        return std::ptr::null_mut();
    };
    // SAFETY: `sqlite3_malloc64` returns malloc-like memory or null.
    let out = unsafe { sqlite3_malloc64(len) } as *mut u8;
    if out.is_null() {
        return std::ptr::null_mut();
    }
    // SAFETY: `len` bytes of fresh allocation; the last is the NUL.
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), out, bytes.len());
        *out.add(bytes.len()) = 0;
    }
    out as *mut c_char
}

fn join_ids(ids: &[u32]) -> String {
    ids.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(",")
}

impl Store {
    /// FCTL keyring control. `args` is non-null by construction (the VFS
    /// maps null to `SQLITE_MISUSE` first).
    pub fn keyring_control(&self, args: &mut sqlite_murmur_keyring_args, read_only: bool) -> c_int {
        if args.op == KEYRING_GET {
            let guarded = lock(&self.mu);
            args.sealed = if guarded.keyring.is_some() { 1 } else { 0 };
            match guarded.keyring.as_ref() {
                Some(ring) => {
                    args.kdf = u32::from(ring.kdf);
                    args.time = ring.argon_time;
                    args.memory_kib = ring.argon_memory_kib;
                    args.threads = u32::from(ring.argon_threads);
                }
                None => {
                    args.kdf = u32::from(guarded.staged_kdf);
                    args.time = guarded.staged_time;
                    args.memory_kib = guarded.staged_memory_kib;
                    args.threads = u32::from(guarded.staged_threads);
                }
            }
            args.json = std::ptr::null_mut();
            return SQLITE_OK;
        }
        if args.op == KEYRING_INVENTORY {
            let json = {
                let guarded = lock(&self.mu);
                match guarded.keyring.as_ref() {
                    None => "{\"sealed\":false}".to_string(),
                    Some(ring) => {
                        let mut json = String::from("{\"sealed\":true,");
                        json += &format!("\"seq\":{},", ring.seq);
                        json += "\"kdf\":\"";
                        json += if ring.kdf == KDF_ARGON2ID { "argon2id" } else { "direct" };
                        json += &format!(
                            "\",\"kdf_time\":{},\"kdf_memory_kib\":{},\"kdf_threads\":{},",
                            ring.argon_time, ring.argon_memory_kib, ring.argon_threads
                        );
                        json += &format!("\"wrap_id\":\"{}\",", json_escape(&ring.wrap_id));
                        json += &format!("\"current\":{},\"keys\":[", ring.current);
                        for (i, entry) in ring.keys.iter().enumerate() {
                            if i > 0 {
                                json += ",";
                            }
                            json += &format!(
                                "{{\"id\":{},\"created_nanos\":{},\"active\":{}}}",
                                entry.id,
                                entry.created_nanos,
                                if entry.active { "true" } else { "false" }
                            );
                        }
                        json += "]}";
                        json
                    }
                }
            };
            args.json = malloc_string(&json);
            return if args.json.is_null() { SQLITE_NOMEM } else { SQLITE_OK };
        }
        if args.op == KEYRING_ROTATE {
            return self.keyring_rotate(args, read_only);
        }
        if args.op == KEYRING_PRUNE {
            return self.keyring_prune(args, read_only);
        }
        if args.op != KEYRING_SET {
            return SQLITE_MISUSE;
        }
        self.keyring_set(args, read_only)
    }

    fn keyring_rotate(&self, args: &mut sqlite_murmur_keyring_args, read_only: bool) -> c_int {
        if read_only {
            return SQLITE_READONLY;
        }
        let _mutation = lock(&self.mutation_mu);
        let _serial = lock(&self.flush_mu);
        let (mut master, store_id, staged, sync) = {
            let guarded = lock(&self.mu);
            let Some(ring) = guarded.keyring.as_ref() else {
                return SQLITE_ERROR;
            };
            if ring.kdf != KDF_DIRECT_HKDF {
                return SQLITE_ERROR;
            }
            (
                Zeroizing::new(guarded.settings.key),
                guarded.keyring_store_id,
                ring.clone(),
                guarded.settings.rocksdb_sync,
            )
        };
        let (envelope, new_id) =
            match keyring::rotate(&staged, &store_id, Some(&master), None) {
                Ok(pair) => pair,
                Err(rc) => return rc,
            };
        drop(staged);
        let envelope_opts = WriteOptions::with_sync(sync);
        if self.db().put(&envelope_opts, META_KEYRING, &envelope).is_err() {
            return SQLITE_IOERR_WRITE;
        }
        {
            let mut guarded = lock(&self.mu);
            let ring = match keyring::open(
                &envelope,
                Some(&master),
                None,
                &guarded.keyring_store_id,
                &guarded.keyring_store_id,
            ) {
                Ok(ring) => ring,
                Err(_) => return SQLITE_CORRUPT,
            };
            let (active_id, active) = match keyring_current(&ring) {
                Some(pair) => pair,
                None => return SQLITE_CORRUPT,
            };
            if active_id != new_id {
                return SQLITE_CORRUPT;
            }
            let mut fresh = Zeroizing::new([0u8; 32]);
            fresh.copy_from_slice(active);
            guarded.keyring = Some(ring);
            guarded.active_key.copy_from_slice(&fresh[..]);
            guarded.generation += 1;
        }
        master.zeroize();
        args.value = u64::from(new_id);
        args.json = std::ptr::null_mut();
        SQLITE_OK
    }

    fn keyring_prune(&self, args: &mut sqlite_murmur_keyring_args, read_only: bool) -> c_int {
        if read_only {
            return SQLITE_READONLY;
        }
        let _mutation = lock(&self.mutation_mu);
        let _serial = lock(&self.flush_mu);
        {
            let guarded = lock(&self.mu);
            if guarded.keyring.is_none() {
                let empty =
                    "{\"pruned\":[],\"blocked_by_unkeyed\":0,\"retained\":[],\"unreadable\":0}";
                args.json = malloc_string(empty);
                return if args.json.is_null() { SQLITE_NOMEM } else { SQLITE_OK };
            }
        }
        let mut referenced = std::collections::HashSet::new();
        let mut unkeyed: u64 = 0;
        let mut unreadable: u64 = 0;
        {
            let ro = ReadOptions::new();
            let mut it = self.db().iterator(&ro);
            it.seek(&page_key(0));
            while it.valid() {
                let key = it.key();
                if key.len() != 9 || key[0] != b'p' {
                    break;
                }
                let record = it.value();
                match record_key_id(&record) {
                    Some(id) => {
                        if id != 0 {
                            referenced.insert(id);
                        } else {
                            unreadable += 1;
                        }
                    }
                    None if is_legacy_record(&record) => unkeyed += 1,
                    None => unreadable += 1,
                }
                it.next();
            }
            if it.status().is_err() {
                return SQLITE_IOERR_READ;
            }
        }
        let (drop_ids, retained) = {
            let guarded = lock(&self.mu);
            let ring = guarded.keyring.as_ref().expect("sealed above");
            let mut drop_ids = Vec::new();
            let mut retained = Vec::new();
            for entry in ring.keys.iter() {
                if entry.active || entry.id == ring.current {
                    retained.push(entry.id);
                    continue;
                }
                if unkeyed == 0 && !referenced.contains(&entry.id) {
                    drop_ids.push(entry.id);
                } else {
                    retained.push(entry.id);
                }
            }
            (drop_ids, retained)
        };
        if !drop_ids.is_empty() {
            let (master, store_id, staged, sync) = {
                let guarded = lock(&self.mu);
                let ring = guarded.keyring.as_ref().expect("sealed above");
                if ring.kdf != KDF_DIRECT_HKDF {
                    return SQLITE_ERROR;
                }
                (
                    Zeroizing::new(guarded.settings.key),
                    guarded.keyring_store_id,
                    ring.clone(),
                    guarded.settings.rocksdb_sync,
                )
            };
            let envelope = match keyring::drop_keys(&staged, &drop_ids, &store_id, Some(&master), None) {
                Ok(envelope) => envelope,
                Err(rc) => return rc,
            };
            drop(staged);
            drop(master);
            let envelope_opts = WriteOptions::with_sync(sync);
            if self.db().put(&envelope_opts, META_KEYRING, &envelope).is_err() {
                return SQLITE_IOERR_WRITE;
            }
            {
                // The C++ holds `mu` across this open and reads the live
                // master (unlike rotate's copied master): mirror as-is.
                let mut guarded = lock(&self.mu);
                let ring = match keyring::open(
                    &envelope,
                    Some(&guarded.settings.key),
                    None,
                    &guarded.keyring_store_id,
                    &guarded.keyring_store_id,
                ) {
                    Ok(ring) => ring,
                    Err(_) => return SQLITE_CORRUPT,
                };
                guarded.keyring = Some(ring);
                guarded.generation += 1;
            }
        }
        let json = format!(
            "{{\"pruned\":[{}],\"blocked_by_unkeyed\":{},\"retained\":[{}],\"unreadable\":{}}}",
            join_ids(&drop_ids),
            unkeyed,
            join_ids(&retained),
            unreadable
        );
        args.json = malloc_string(&json);
        if args.json.is_null() {
            return SQLITE_NOMEM;
        }
        SQLITE_OK
    }

    fn keyring_set(&self, args: &mut sqlite_murmur_keyring_args, read_only: bool) -> c_int {
        if read_only {
            return SQLITE_READONLY;
        }
        let range_ok = match args.param {
            KEYRING_PARAM_KDF => args.value <= 1,
            KEYRING_PARAM_TIME => (1..=32).contains(&args.value),
            KEYRING_PARAM_MEMORY => (1024..=16777216).contains(&args.value),
            KEYRING_PARAM_THREADS => (1..=32).contains(&args.value),
            _ => return SQLITE_MISUSE,
        };
        if !range_ok {
            return SQLITE_ERROR;
        }
        let _mutation = lock(&self.mutation_mu);
        let _serial = lock(&self.flush_mu);
        let (seal_kdf, mut master, sync) = {
            let mut guarded = lock(&self.mu);
            if guarded.keyring.is_some() {
                return SQLITE_BUSY;
            }
            let mut do_seal = false;
            let mut seal_kdf = 0u8;
            match args.param {
                KEYRING_PARAM_KDF => {
                    seal_kdf = args.value as u8;
                    do_seal = true;
                }
                KEYRING_PARAM_TIME => guarded.staged_time = args.value as u32,
                KEYRING_PARAM_MEMORY => guarded.staged_memory_kib = args.value as u32,
                _ => guarded.staged_threads = args.value as u8,
            }
            if !do_seal {
                return SQLITE_OK;
            }
            (seal_kdf, Zeroizing::new(guarded.settings.key), guarded.settings.rocksdb_sync)
        };
        // Seal: persist the store id first (reuse across retries), then
        // the envelope, then install in memory.
        let mut store_id = [0u8; 16];
        match self.db().get(&ReadOptions::new(), META_STORE_ID) {
            Ok(Some(id_bytes)) => {
                if id_bytes.len() != store_id.len() {
                    return SQLITE_CORRUPT;
                }
                store_id.copy_from_slice(&id_bytes);
            }
            Ok(None) => {
                if rand::RngCore::try_fill_bytes(&mut rand::rngs::OsRng, &mut store_id).is_err() {
                    return SQLITE_ERROR;
                }
                let id_opts = WriteOptions::with_sync(sync);
                if self.db().put(&id_opts, META_STORE_ID, &store_id).is_err() {
                    return SQLITE_IOERR_WRITE;
                }
            }
            Err(_) => return SQLITE_IOERR_READ,
        }
        let (kdf, mut time, mut memory_kib, mut threads) = {
            let guarded = lock(&self.mu);
            (seal_kdf, guarded.staged_time, guarded.staged_memory_kib, guarded.staged_threads)
        };
        if kdf == KDF_DIRECT_HKDF {
            time = 0;
            memory_kib = 0;
            threads = 0;
        }
        let envelope = match keyring::create(
            kdf, time, memory_kib, threads, b"", &store_id, &store_id, Some(&master), None,
        ) {
            Ok(envelope) => envelope,
            Err(rc) => return rc,
        };
        let envelope_opts = WriteOptions::with_sync(sync);
        if self.db().put(&envelope_opts, META_KEYRING, &envelope).is_err() {
            return SQLITE_IOERR_WRITE;
        }
        {
            let mut guarded = lock(&self.mu);
            if guarded.keyring.is_some() {
                // Serialized by `mutation_mu`; never taken.
                return SQLITE_BUSY;
            }
            let ring = match keyring::open(&envelope, Some(&master), None, &store_id, &store_id) {
                Ok(ring) => ring,
                Err(_) => return SQLITE_CORRUPT,
            };
            let (_, active) = match keyring_current(&ring) {
                Some(pair) => pair,
                None => return SQLITE_CORRUPT,
            };
            let mut fresh = Zeroizing::new([0u8; 32]);
            fresh.copy_from_slice(active);
            guarded.keyring_store_id = store_id;
            guarded.keyring = Some(ring);
            guarded.active_key.copy_from_slice(&fresh[..]);
            guarded.generation += 1;
        }
        master.zeroize();
        SQLITE_OK
    }

    /// `rocksdb_*` PRAGMA. Returns the result code plus the malloc'd
    /// result text (null when the C++ leaves `*out` untouched, which
    /// SQLite pre-zeroed anyway).
    pub fn rocksdb_pragma(
        &self,
        name: &[u8],
        value: Option<&[u8]>,
        read_only: bool,
    ) -> (c_int, *mut c_char) {
        enum Option_ {
            Sync,
            WriteBufferSize,
            WriteBufferNumber,
            BlockCacheBytes,
            BackgroundJobs,
        }
        let (option, backend, minimum, maximum) = if name.eq_ignore_ascii_case(b"rocksdb_sync") {
            (Option_::Sync, None, 0u64, 1u64)
        } else if name.eq_ignore_ascii_case(b"rocksdb_write_buffer_size") {
            (Option_::WriteBufferSize, Some("write_buffer_size"), 65536, i64::MAX as u64)
        } else if name.eq_ignore_ascii_case(b"rocksdb_max_write_buffer_number") {
            (
                Option_::WriteBufferNumber,
                Some("max_write_buffer_number"),
                2,
                i32::MAX as u64,
            )
        } else if name.eq_ignore_ascii_case(b"rocksdb_block_cache_bytes") {
            (Option_::BlockCacheBytes, None, 0, i64::MAX as u64)
        } else if name.eq_ignore_ascii_case(b"rocksdb_max_background_jobs") {
            (Option_::BackgroundJobs, Some("max_background_jobs"), 1, i32::MAX as u64)
        } else {
            return (SQLITE_NOTFOUND, std::ptr::null_mut());
        };
        if value.is_some() && read_only {
            let text = format!("cannot set {} on a read-only connection", String::from_utf8_lossy(name));
            let out = malloc_string(&text);
            return (if out.is_null() { SQLITE_NOMEM } else { SQLITE_READONLY }, out);
        }
        let mut number = 0u64;
        if let Some(text) = value {
            match parse_unsigned(text, maximum) {
                Some(parsed) if parsed >= minimum => number = parsed,
                _ => {
                    let text = format!("{} requires an integer from {minimum} through {maximum}", String::from_utf8_lossy(name));
                    let out = malloc_string(&text);
                    return (if out.is_null() { SQLITE_NOMEM } else { SQLITE_ERROR }, out);
                }
            }
        }
        // Serialize setters and readback with each other, cipher
        // selection and application flushes.
        let _serial = lock(&self.flush_mu);
        if value.is_some() {
            let backend_cxx: Option<CString> =
                backend.map(|name| CString::new(name).expect("static name"));
            let failed: Option<String> = match option {
                Option_::Sync => {
                    lock(&self.mu).settings.rocksdb_sync = number != 0;
                    None
                }
                Option_::BlockCacheBytes => {
                    self.block_cache.get().expect("store started").set_capacity(number as usize);
                    None
                }
                Option_::WriteBufferSize | Option_::WriteBufferNumber => {
                    let cxx_name = backend_cxx.as_ref().expect("backend");
                    let cxx_value = CString::new(number.to_string()).expect("digits");
                    self.db().set_options(&[(cxx_name, cxx_value.as_c_str())]).err()
                }
                Option_::BackgroundJobs => {
                    let cxx_name = backend_cxx.as_ref().expect("backend");
                    let cxx_value = CString::new(number.to_string()).expect("digits");
                    self.db().set_db_option(cxx_name, cxx_value.as_c_str()).err()
                }
            };
            if let Some(status) = failed {
                let text = format!("{}: {status}", String::from_utf8_lossy(name));
                let out = malloc_string(&text);
                if out.is_null() {
                    return (SQLITE_NOMEM, out);
                }
                return (if is_io_error(&status) { SQLITE_IOERR_WRITE } else { SQLITE_ERROR }, out);
            }
        }
        match option {
            Option_::Sync => number = u64::from(lock(&self.mu).settings.rocksdb_sync),
            Option_::BlockCacheBytes => {
                number = self.block_cache.get().expect("store started").capacity() as u64
            }
            Option_::WriteBufferSize | Option_::WriteBufferNumber | Option_::BackgroundJobs => {
                let cxx = CString::new(backend.expect("backend")).expect("name");
                match self.db().live_option(&cxx) {
                    Ok(live) => number = live,
                    Err(status) => {
                        let text = format!("{}: {status}", String::from_utf8_lossy(name));
                        let out = malloc_string(&text);
                        return (if out.is_null() { SQLITE_NOMEM } else { SQLITE_ERROR }, out);
                    }
                }
            }
        }
        let out = malloc_string(&number.to_string());
        (if out.is_null() { SQLITE_NOMEM } else { SQLITE_OK }, out)
    }

    /// `background_threads` PRAGMA (encoder pool sizing).
    pub fn background_threads_pragma(
        &self,
        value: Option<&[u8]>,
        read_only: bool,
    ) -> (c_int, *mut c_char) {
        let mut number = 0u64;
        if value.is_some() && read_only {
            // The C++ returns without touching `*out` (pre-zeroed).
            return (SQLITE_READONLY, std::ptr::null_mut());
        }
        if let Some(text) = value {
            match parse_unsigned(text, 256) {
                Some(parsed) if parsed != 0 => number = parsed,
                _ => {
                    let out = malloc_string(
                        "background_threads requires an integer from 1 through 256",
                    );
                    return (if out.is_null() { SQLITE_NOMEM } else { SQLITE_ERROR }, out);
                }
            }
        }
        let mut serial = lock(&self.flush_mu);
        if value.is_some() {
            let current = lock(&self.mu).settings.background_threads;
            if number != u64::from(current) {
                match EncodingPool::new(number as u32) {
                    Ok(replacement) => {
                        // The old pool stops/joins under the lock, like
                        // the C++ swap (its threads are idle on their
                        // condvar, so the join is prompt).
                        serial.encoders = Some(replacement);
                        lock(&self.mu).settings.background_threads = number as u32;
                    }
                    Err(_) => return (SQLITE_ERROR, std::ptr::null_mut()),
                }
            }
        }
        drop(serial);
        let out = malloc_string(&lock(&self.mu).settings.background_threads.to_string());
        (if out.is_null() { SQLITE_NOMEM } else { SQLITE_OK }, out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::{CStr, c_void};
    use crate::abi::{KEYRING_PARAM_KDF, KEYRING_PARAM_TIME};
    use crate::crypto::BLOCK;
    use crate::ffi_sqlite::{SQLITE_ERROR, sqlite3_free};
    use crate::store::{Settings, Store};
    use std::sync::Arc;

    fn test_settings() -> Settings {
        let mut key = [0u8; 32];
        for (i, b) in key.iter_mut().enumerate() {
            *b = i as u8;
        }
        let mut settings = Settings::new(key, u32::MAX, 64, true);
        settings.background_threads = 2;
        settings
    }

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("murmur-ctl-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn open(dir: &std::path::Path) -> Arc<Store> {
        let store = Arc::new(Store::new(test_settings()));
        assert!(store.start(dir, true));
        store
    }

    fn args(op: c_int, param: c_int, value: u64) -> sqlite_murmur_keyring_args {
        sqlite_murmur_keyring_args {
            op,
            param,
            value,
            sealed: 0,
            kdf: 0,
            time: 0,
            memory_kib: 0,
            threads: 0,
            json: std::ptr::null_mut(),
        }
    }

    /// Take the malloc'd JSON the control call produced (frees it).
    unsafe fn take_json(args: &mut sqlite_murmur_keyring_args) -> String {
        if args.json.is_null() {
            return String::new();
        }
        let text = unsafe { CStr::from_ptr(args.json) }.to_string_lossy().into_owned();
        unsafe { sqlite3_free(args.json as *mut c_void) };
        args.json = std::ptr::null_mut();
        text
    }

    unsafe fn take_text(out: *mut c_char) -> String {
        if out.is_null() {
            return String::new();
        }
        let text = unsafe { CStr::from_ptr(out) }.to_string_lossy().into_owned();
        unsafe { sqlite3_free(out as *mut c_void) };
        text
    }

    #[test]
    fn seal_inventory_rotate_prune_lifecycle() {
        let dir = scratch("lifecycle");
        let store = open(&dir);
        // Unsealed GET reports staged defaults.
        let mut get = args(KEYRING_GET, 0, 0);
        assert_eq!(store.keyring_control(&mut get, false), SQLITE_OK);
        assert_eq!((get.sealed, get.kdf, get.time, get.memory_kib, get.threads), (0, 0, 3, 65536, 4));
        let mut inv = args(KEYRING_INVENTORY, 0, 0);
        assert_eq!(store.keyring_control(&mut inv, false), SQLITE_OK);
        assert_eq!(unsafe { take_json(&mut inv) }, "{\"sealed\":false}");
        // Stage + seal.
        let mut stage = args(KEYRING_SET, KEYRING_PARAM_TIME, 5);
        assert_eq!(store.keyring_control(&mut stage, false), SQLITE_OK);
        let mut get = args(KEYRING_GET, 0, 0);
        assert_eq!(store.keyring_control(&mut get, false), SQLITE_OK);
        assert_eq!(get.time, 5);
        let mut seal = args(KEYRING_SET, KEYRING_PARAM_KDF, 0);
        assert_eq!(store.keyring_control(&mut seal, false), SQLITE_OK);
        let mut get = args(KEYRING_GET, 0, 0);
        assert_eq!(store.keyring_control(&mut get, false), SQLITE_OK);
        assert_eq!((get.sealed, get.kdf, get.time, get.memory_kib, get.threads), (1, 0, 0, 0, 0));
        let mut inv = args(KEYRING_INVENTORY, 0, 0);
        assert_eq!(store.keyring_control(&mut inv, false), SQLITE_OK);
        assert_eq!(
            unsafe { take_json(&mut inv) },
            "{\"sealed\":true,\"seq\":1,\"kdf\":\"direct\",\"kdf_time\":0,\
             \"kdf_memory_kib\":0,\"kdf_threads\":0,\"wrap_id\":\"\",\"current\":1,\
             \"keys\":[{\"id\":1,\"created_nanos\":0,\"active\":true}]}"
        );
        // Seal is one-way; rotate needs the seal (and works now).
        let mut seal2 = args(KEYRING_SET, KEYRING_PARAM_KDF, 0);
        assert_eq!(store.keyring_control(&mut seal2, false), SQLITE_BUSY);
        // Write id-1 pages, rotate, overwrite everything with id-2.
        for page in 0..2u64 {
            let data = vec![page as u8; BLOCK];
            assert_eq!(store.write(&data, (page * BLOCK as u64) as i64), SQLITE_OK);
        }
        assert_eq!(store.complete_transaction(), SQLITE_OK);
        assert_eq!(store.flush(), SQLITE_OK);
        let mut rot = args(KEYRING_ROTATE, 0, 0);
        assert_eq!(store.keyring_control(&mut rot, false), SQLITE_OK);
        assert_eq!(rot.value, 2);
        for page in 0..2u64 {
            let data = vec![0x80 + page as u8; BLOCK];
            assert_eq!(store.write(&data, (page * BLOCK as u64) as i64), SQLITE_OK);
        }
        assert_eq!(store.complete_transaction(), SQLITE_OK);
        assert_eq!(store.flush(), SQLITE_OK);
        // Id 1 is now unreferenced with no legacy pages: pruned.
        let mut prune = args(KEYRING_PRUNE, 0, 0);
        assert_eq!(store.keyring_control(&mut prune, false), SQLITE_OK);
        assert_eq!(
            unsafe { take_json(&mut prune) },
            "{\"pruned\":[1],\"blocked_by_unkeyed\":0,\"retained\":[2],\"unreadable\":0}"
        );
        // Everything still reads (id-2 records).
        for page in 0..2u64 {
            let mut out = vec![0u8; BLOCK];
            assert_eq!(store.read(&mut out, (page * BLOCK as u64) as i64), SQLITE_OK);
            assert_eq!(out, vec![0x80 + page as u8; BLOCK]);
        }
        drop(store);
        // Reopen loads the sealed envelope (id 2 active).
        let store = open(&dir);
        let mut inv = args(KEYRING_INVENTORY, 0, 0);
        assert_eq!(store.keyring_control(&mut inv, false), SQLITE_OK);
        assert!(unsafe { take_json(&mut inv) }.contains("\"current\":2"));
        for page in 0..2u64 {
            let mut out = vec![0u8; BLOCK];
            assert_eq!(store.read(&mut out, (page * BLOCK as u64) as i64), SQLITE_OK);
            assert_eq!(out, vec![0x80 + page as u8; BLOCK]);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn prune_blocked_by_legacy_pages() {
        let dir = scratch("prune-blocked");
        let store = open(&dir);
        // Legacy (EXP1) page 0 survives; seal, rotate, write keyed page 1.
        assert_eq!(store.write(&[7u8; BLOCK], 0), SQLITE_OK);
        assert_eq!(store.complete_transaction(), SQLITE_OK);
        assert_eq!(store.flush(), SQLITE_OK);
        let mut seal = args(KEYRING_SET, KEYRING_PARAM_KDF, 0);
        assert_eq!(store.keyring_control(&mut seal, false), SQLITE_OK);
        let mut rot = args(KEYRING_ROTATE, 0, 0);
        assert_eq!(store.keyring_control(&mut rot, false), SQLITE_OK);
        assert_eq!(store.write(&[8u8; BLOCK], BLOCK as i64), SQLITE_OK);
        assert_eq!(store.complete_transaction(), SQLITE_OK);
        assert_eq!(store.flush(), SQLITE_OK);
        // Id 1 is unreferenced but a legacy page survives: blocked.
        let mut prune = args(KEYRING_PRUNE, 0, 0);
        assert_eq!(store.keyring_control(&mut prune, false), SQLITE_OK);
        assert_eq!(
            unsafe { take_json(&mut prune) },
            "{\"pruned\":[],\"blocked_by_unkeyed\":1,\"retained\":[1,2],\"unreadable\":0}"
        );
        // Overwrite the legacy page with a keyed record: id 1 drops.
        assert_eq!(store.write(&[9u8; BLOCK], 0), SQLITE_OK);
        assert_eq!(store.complete_transaction(), SQLITE_OK);
        assert_eq!(store.flush(), SQLITE_OK);
        let mut prune = args(KEYRING_PRUNE, 0, 0);
        assert_eq!(store.keyring_control(&mut prune, false), SQLITE_OK);
        assert_eq!(
            unsafe { take_json(&mut prune) },
            "{\"pruned\":[1],\"blocked_by_unkeyed\":0,\"retained\":[2],\"unreadable\":0}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn misuse_and_readonly_paths() {
        let dir = scratch("misuse");
        let store = open(&dir);
        let mut bad = args(99, 0, 0);
        assert_eq!(store.keyring_control(&mut bad, false), SQLITE_MISUSE);
        let mut bad = args(KEYRING_SET, 99, 0);
        assert_eq!(store.keyring_control(&mut bad, false), SQLITE_MISUSE);
        let mut bad = args(KEYRING_SET, KEYRING_PARAM_KDF, 5);
        assert_eq!(store.keyring_control(&mut bad, false), SQLITE_ERROR);
        let mut bad = args(KEYRING_SET, KEYRING_PARAM_TIME, 99);
        assert_eq!(store.keyring_control(&mut bad, false), SQLITE_ERROR);
        let mut rot = args(KEYRING_ROTATE, 0, 0);
        assert_eq!(store.keyring_control(&mut rot, false), SQLITE_ERROR);
        let mut rot = args(KEYRING_ROTATE, 0, 0);
        assert_eq!(store.keyring_control(&mut rot, true), SQLITE_READONLY);
        let mut prune = args(KEYRING_PRUNE, 0, 0);
        assert_eq!(store.keyring_control(&mut prune, false), SQLITE_OK);
        assert_eq!(
            unsafe { take_json(&mut prune) },
            "{\"pruned\":[],\"blocked_by_unkeyed\":0,\"retained\":[],\"unreadable\":0}"
        );
        let mut seal = args(KEYRING_SET, KEYRING_PARAM_KDF, 0);
        assert_eq!(store.keyring_control(&mut seal, true), SQLITE_READONLY);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn pragma_set_and_readback() {
        let dir = scratch("pragma");
        let store = open(&dir);
        let (rc, out) = store.rocksdb_pragma(b"rocksdb_sync", None, false);
        assert_eq!(rc, SQLITE_OK);
        assert_eq!(unsafe { take_text(out) }, "1");
        let (rc, out) = store.rocksdb_pragma(b"ROCKSDB_SYNC", Some(b"0"), false);
        assert_eq!(rc, SQLITE_OK);
        assert_eq!(unsafe { take_text(out) }, "0");
        let (rc, out) = store.rocksdb_pragma(b"rocksdb_block_cache_bytes", None, false);
        assert_eq!(rc, SQLITE_OK);
        assert_eq!(unsafe { take_text(out) }, (32u64 * 1024 * 1024).to_string());
        let (rc, out) = store.rocksdb_pragma(b"rocksdb_block_cache_bytes", Some(b"4194304"), false);
        assert_eq!(rc, SQLITE_OK);
        assert_eq!(unsafe { take_text(out) }, "4194304");
        let (rc, out) = store.rocksdb_pragma(b"rocksdb_write_buffer_size", None, false);
        assert_eq!(rc, SQLITE_OK);
        assert_eq!(unsafe { take_text(out) }, "67108864");
        let (rc, out) =
            store.rocksdb_pragma(b"rocksdb_write_buffer_size", Some(b"134217728"), false);
        assert_eq!(rc, SQLITE_OK);
        assert_eq!(unsafe { take_text(out) }, "134217728");
        let (rc, out) = store.rocksdb_pragma(b"rocksdb_max_write_buffer_number", None, false);
        assert_eq!(rc, SQLITE_OK);
        assert_eq!(unsafe { take_text(out) }, "2");
        let (rc, out) = store.rocksdb_pragma(b"rocksdb_max_background_jobs", Some(b"1"), false);
        assert_eq!(rc, SQLITE_OK);
        assert_eq!(unsafe { take_text(out) }, "1");
        // Errors.
        let (rc, out) = store.rocksdb_pragma(b"rocksdb_nope", None, false);
        assert_eq!(rc, SQLITE_NOTFOUND);
        assert!(out.is_null());
        let (rc, out) = store.rocksdb_pragma(b"rocksdb_sync", Some(b"7"), false);
        assert_eq!(rc, SQLITE_ERROR);
        assert_eq!(
            unsafe { take_text(out) },
            "rocksdb_sync requires an integer from 0 through 1"
        );
        let (rc, out) = store.rocksdb_pragma(b"rocksdb_sync", Some(b"1"), true);
        assert_eq!(rc, SQLITE_READONLY);
        assert_eq!(unsafe { take_text(out) }, "cannot set rocksdb_sync on a read-only connection");
        // background_threads.
        let (rc, out) = store.background_threads_pragma(None, false);
        assert_eq!(rc, SQLITE_OK);
        assert_eq!(unsafe { take_text(out) }, "2");
        let (rc, out) = store.background_threads_pragma(Some(b"3"), false);
        assert_eq!(rc, SQLITE_OK);
        assert_eq!(unsafe { take_text(out) }, "3");
        let (rc, out) = store.background_threads_pragma(Some(b"0"), false);
        assert_eq!(rc, SQLITE_ERROR);
        assert_eq!(
            unsafe { take_text(out) },
            "background_threads requires an integer from 1 through 256"
        );
        let (rc, out) = store.background_threads_pragma(Some(b"3"), true);
        assert_eq!(rc, SQLITE_READONLY);
        assert!(out.is_null());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
