//! `repr(C)` mirrors of the stable C ABI in `include/sqlite_murmur.h`
//! and `include/sqlite_murmur_backup.h`. Field order is ABI (the header
//! notes the replication crate's twin mirror); layout tests pin it.

use std::ffi::{c_char, c_int, c_uchar, c_uint, c_void};

pub const FCTL_EXTENSION: c_int = 0x535801;
pub const FCTL_FLUSH: c_int = 0x535802;
pub const FCTL_BACKUP: c_int = 0x535803;
pub const FCTL_COMPACT: c_int = 0x535804;
pub const FCTL_KEYRING: c_int = 0x535805;

pub const KEYRING_GET: c_int = 1;
pub const KEYRING_SET: c_int = 2;
pub const KEYRING_INVENTORY: c_int = 3;
pub const KEYRING_ROTATE: c_int = 4;
pub const KEYRING_PRUNE: c_int = 5;
pub const KEYRING_PARAM_KDF: c_int = 1;
pub const KEYRING_PARAM_TIME: c_int = 2;
pub const KEYRING_PARAM_MEMORY: c_int = 3;
pub const KEYRING_PARAM_THREADS: c_int = 4;

pub const KDF_DIRECT_HKDF: u8 = 0;
pub const KDF_ARGON2ID: u8 = 1;
pub const ARGON_TIME_DEFAULT: u32 = 3;
pub const ARGON_MEMORY_KIB_DEFAULT: u32 = 65536;
pub const ARGON_THREADS_DEFAULT: u8 = 4;

pub const OPEN_STORAGE_ONLY: u32 = 1;

#[allow(non_camel_case_types)]
#[repr(C)]
pub struct sqlite_murmur_config {
    pub key: *const c_uchar,
    pub flush_interval_ms: c_uint,
    pub cache_pages: usize,
    pub rocksdb_sync: c_int,
}

#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct sqlite_murmur_stats {
    pub logical_size_bytes: u64,
    pub vfs_write_bytes: u64,
    pub vfs_block_writes: u64,
    pub flush_batches: u64,
    pub persisted_blocks: u64,
    pub uncompressed_bytes: u64,
    pub compressed_payload_bytes: u64,
    pub encrypted_record_bytes: u64,
    pub rocksdb_wal_bytes: u64,
    pub rocksdb_flush_bytes: u64,
    pub rocksdb_compaction_bytes: u64,
}

#[allow(non_camel_case_types)]
#[repr(C)]
pub struct sqlite_murmur_keyring_args {
    pub op: c_int,
    pub param: c_int,
    pub value: u64,
    pub sealed: c_int,
    pub kdf: c_uint,
    pub time: c_uint,
    pub memory_kib: c_uint,
    pub threads: c_uint,
    pub json: *mut c_char,
}

#[allow(non_camel_case_types)]
#[repr(C)]
pub struct sqlite_murmur_backup_args {
    pub dest: *const c_char,
    pub group: [c_uchar; 16],
    pub node: [c_uchar; 16],
}

#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Clone, Copy)]
pub struct sqlite_murmur_rekey_options {
    pub fault_after_pages: c_int,
}

#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Clone, Copy)]
pub struct sqlite_murmur_open_options {
    pub flags: c_uint,
    pub cipher: *const c_char,
    pub compression: *const c_char,
}

#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Clone, Copy)]
pub struct sqlite_murmur_extension {
    pub context: *mut c_void,
    pub pragma: Option<unsafe extern "C" fn(*mut c_void, *const c_char, *const c_char, *mut *mut c_char) -> c_int>,
    pub destroy: Option<unsafe extern "C" fn(*mut c_void)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layouts_match_headers() {
        // Ground truth from a C++ probe over include/*.h (LP64).
        assert_eq!(std::mem::size_of::<sqlite_murmur_stats>(), 88);
        assert_eq!(std::mem::size_of::<sqlite_murmur_keyring_args>(), 48);
        assert_eq!(std::mem::offset_of!(sqlite_murmur_keyring_args, value), 8);
        assert_eq!(std::mem::offset_of!(sqlite_murmur_keyring_args, json), 40);
        assert_eq!(std::mem::size_of::<sqlite_murmur_backup_args>(), 40);
        assert_eq!(std::mem::size_of::<sqlite_murmur_config>(), 32);
        assert_eq!(std::mem::size_of::<sqlite_murmur_extension>(), 24);
        assert_eq!(std::mem::size_of::<sqlite_murmur_rekey_options>(), 4);
        assert_eq!(std::mem::size_of::<sqlite_murmur_open_options>(), 24);
    }
}
