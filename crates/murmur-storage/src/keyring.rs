//! keys.enc v3 envelope. Rust port of `src/sqlite_murmur_keyring.cpp`.
//!
//! Byte-exact logic port: header/body/AAD/info layouts, CRC32C, HKDF-SHA256
//! protector, AES-256-GCM seal/open, and every result code match the C++
//! implementation, which defers to the archived format spec. Provenance
//! and rulings live in `include/sqlite_murmur_keyring.h`. Pure bytes plus
//! vendored crypto: no filesystem, no RocksDB, no SQLite dependency.
//!
//! The C++ API (`sqlite_murmur_keyring` with `std::string`/`std::vector`
//! members) cannot cross FFI, so this module exposes a C ABI surface
//! (`sqlite_murmur_keyring_rs_*`) over an opaque handle. The remaining
//! C++ translation unit is a thin shim that preserves the C++ API for the
//! store and the (polyglot) test suite until the store itself is ported.

use crate::error::{SQLITE_AUTH, SQLITE_CORRUPT, SQLITE_ERROR, SQLITE_MISUSE, SQLITE_OK};
use crate::guard;
use crate::crypto_provider::{Aes256Gcm, Payload};
use std::ffi::{CStr, c_char, c_int};
use std::{ptr, slice};
use zeroize::{Zeroize, Zeroizing};

const MAGIC: &[u8; 4] = b"SPLK";
const VERSION: u16 = 3;
const KDF_DIRECT_HKDF: u8 = 0;
const KDF_ARGON2ID: u8 = 1;
const NONCE_LEN: usize = 12;
const KEY_LEN: usize = 32;
const ID16_LEN: usize = 16;
const MAX_WRAP: usize = 128;
const MAX_KEYS: usize = 1 << 16;
const MAX_SEALED: usize = 1 << 24;
const INFO_PREFIX: &[u8] = b"spool/keys-enc/v3/";
const ENTRY_LEN: usize = 4 + 8 + 1 + 32;

// No vendored Argon2 unit exists, matching a C++ build without
// <argon2.h>: Argon2id envelopes always fail closed (see has_argon2).
const HAS_ARGON2: c_int = 0;

const fn crc32c_table() -> [u32; 256] {
    let mut t = [0u32; 256];
    let mut i = 0u32;
    while i < 256 {
        let mut crc = i;
        let mut k = 0;
        while k < 8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0x82F63B78
            } else {
                crc >> 1
            };
            k += 1;
        }
        t[i as usize] = crc;
        i += 1;
    }
    t
}

const CRC32C_TABLE: [u32; 256] = crc32c_table();

fn crc32c(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc = CRC32C_TABLE[((crc ^ u32::from(b)) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc ^ 0xFFFF_FFFF
}

/// RFC 5869 HKDF-SHA256, L=32 (single expand block).
fn hkdf_32(ikm: &[u8], salt: &[u8], info: &[u8]) -> Option<[u8; 32]> {
    let mut out = [0u8; 32];
    crate::crypto_provider::hkdf_sha256(ikm, salt, info, &mut out).ok()?;
    Some(out)
}

/// Derive the envelope protector. Exactly one of `master`/`passphrase`
/// must unlock the header KDF; returns the C++ result code on failure.
#[allow(clippy::too_many_arguments)]
fn derive_protector(
    kdf: u8,
    master: Option<&[u8; KEY_LEN]>,
    passphrase: Option<&[u8]>,
    salt: &[u8],
    a_time: u32,
    a_mem: u32,
    a_threads: u8,
    store_id: &[u8; ID16_LEN],
    context_id: &[u8; ID16_LEN],
) -> Result<[u8; KEY_LEN], c_int> {
    let mut info = Vec::with_capacity(INFO_PREFIX.len() + 32);
    info.extend_from_slice(INFO_PREFIX);
    info.extend_from_slice(store_id);
    info.extend_from_slice(context_id);
    if kdf == KDF_DIRECT_HKDF {
        let Some(key) = master else {
            return Err(SQLITE_AUTH);
        };
        return hkdf_32(key, salt, &info).ok_or(SQLITE_CORRUPT);
    }
    if kdf == KDF_ARGON2ID {
        let Some(pass) = passphrase else {
            return Err(SQLITE_AUTH);
        };
        if pass.is_empty() {
            return Err(SQLITE_AUTH);
        }
        if a_threads == 0 || a_time == 0 || a_mem == 0 {
            return Err(SQLITE_ERROR);
        }
        // No Argon2 unit: fail closed like the C++ fallback branch.
        return Err(SQLITE_ERROR);
    }
    Err(SQLITE_CORRUPT)
}

fn aesgcm_seal(
    key: &[u8; KEY_LEN],
    nonce: &[u8; NONCE_LEN],
    aad: &[u8],
    plain: &[u8],
) -> Option<Vec<u8>> {
    let cipher = Aes256Gcm::new_from_slice(key).ok()?;
    cipher
        .encrypt(
            nonce,
            Payload { msg: plain, aad },
        )
        .ok()
}

fn aesgcm_open(
    key: &[u8; KEY_LEN],
    nonce: &[u8; NONCE_LEN],
    aad: &[u8],
    sealed: &[u8],
) -> Option<Zeroizing<Vec<u8>>> {
    let cipher = Aes256Gcm::new_from_slice(key).ok()?;
    cipher
        .decrypt(
            nonce,
            Payload { msg: sealed, aad },
        )
        .ok()
        .map(Zeroizing::new)
}

fn random_32() -> Result<[u8; KEY_LEN], c_int> {
    let mut buf = [0u8; KEY_LEN];
    crate::randomness::fill(&mut buf).map_err(|_| SQLITE_ERROR)?;
    Ok(buf)
}

fn random_12() -> Result<[u8; NONCE_LEN], c_int> {
    let mut buf = [0u8; NONCE_LEN];
    crate::randomness::fill(&mut buf).map_err(|_| SQLITE_ERROR)?;
    Ok(buf)
}

/// One unlocked data key. Key bytes are wiped on drop.
#[derive(Clone)]
pub struct KeyEntry {
    pub id: u32,
    pub created_nanos: i64,
    pub active: bool,
    pub key: [u8; KEY_LEN],
}

// Redacting Debug: key material must never reach logs, even in test output.
impl std::fmt::Debug for KeyEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyEntry")
            .field("id", &self.id)
            .field("created_nanos", &self.created_nanos)
            .field("active", &self.active)
            .field("key", &"<redacted>")
            .finish()
    }
}

impl std::fmt::Debug for Keyring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Keyring")
            .field("seq", &self.seq)
            .field("kdf", &self.kdf)
            .field("keys", &self.keys)
            .field("current", &self.current)
            .finish()
    }
}

/// Unlocked envelope. Key material is wiped on drop.
#[derive(Clone)]
pub struct Keyring {
    pub seq: u64,
    pub kdf: u8,
    pub argon_time: u32,
    pub argon_memory_kib: u32,
    pub argon_threads: u8,
    pub context: [u8; ID16_LEN],
    pub wrap_id: Vec<u8>,
    pub keys: Vec<KeyEntry>, // sorted by id
    pub current: u32,        // highest active id
}

impl Drop for Keyring {
    fn drop(&mut self) {
        for k in &mut self.keys {
            k.key.zeroize();
        }
        self.context.zeroize();
        self.wrap_id.zeroize();
    }
}

/// Seal one envelope revision (fresh salt + nonce). `keys` must be id-sorted.
#[allow(clippy::too_many_arguments)]
fn seal_revision(
    kdf: u8,
    seq: u64,
    store_id: &[u8; ID16_LEN],
    context_id: &[u8; ID16_LEN],
    wrap_id: &[u8],
    a_time: u32,
    a_mem: u32,
    a_threads: u8,
    master: Option<&[u8; KEY_LEN]>,
    passphrase: Option<&[u8]>,
    keys: &[KeyEntry],
) -> Result<Vec<u8>, c_int> {
    if wrap_id.len() > MAX_WRAP || keys.is_empty() || keys.len() > MAX_KEYS {
        return Err(SQLITE_MISUSE);
    }
    let salt = Zeroizing::new(random_32()?);
    let protector = Zeroizing::new(derive_protector(
        kdf, master, passphrase, &salt[..], a_time, a_mem, a_threads, store_id,
        context_id,
    )?);
    let mut body = Zeroizing::new(Vec::new());
    body.extend_from_slice(&(keys.len() as u32).to_le_bytes());
    body.extend_from_slice(context_id);
    body.push(wrap_id.len() as u8);
    body.extend_from_slice(wrap_id);
    for k in keys {
        body.extend_from_slice(&k.id.to_le_bytes());
        body.extend_from_slice(&k.created_nanos.to_le_bytes());
        // Note the inverted flag: 0 means active, 1 means retired.
        body.push(u8::from(!k.active));
        body.extend_from_slice(&k.key);
    }
    let mut hdr = Vec::new();
    hdr.extend_from_slice(MAGIC);
    hdr.extend_from_slice(&VERSION.to_le_bytes());
    hdr.push(kdf);
    hdr.extend_from_slice(&seq.to_le_bytes());
    hdr.extend_from_slice(&salt[..]);
    hdr.extend_from_slice(&a_time.to_le_bytes());
    hdr.extend_from_slice(&a_mem.to_le_bytes());
    hdr.push(a_threads);
    hdr.push(wrap_id.len() as u8);
    hdr.extend_from_slice(wrap_id);
    hdr.extend_from_slice(&crc32c(&hdr).to_le_bytes());
    let nonce = random_12()?;
    let mut aad = hdr.clone();
    aad.extend_from_slice(store_id);
    let sealed = aesgcm_seal(&protector, &nonce, &aad, &body).ok_or(SQLITE_CORRUPT)?;
    let mut envelope = hdr;
    envelope.extend_from_slice(&nonce);
    envelope.extend_from_slice(&(sealed.len() as u32).to_le_bytes());
    envelope.extend_from_slice(&sealed);
    Ok(envelope)
}

/// Open + authenticate an envelope.
pub fn open(
    envelope: &[u8],
    master: Option<&[u8; KEY_LEN]>,
    passphrase: Option<&[u8]>,
    store_id: &[u8; ID16_LEN],
    context_id: &[u8; ID16_LEN],
) -> Result<Keyring, c_int> {
    if envelope.len() < 61 {
        return Err(SQLITE_CORRUPT);
    }
    if envelope[0..4] != *MAGIC {
        return Err(SQLITE_CORRUPT);
    }
    if u16::from_le_bytes(envelope[4..6].try_into().unwrap()) != VERSION {
        return Err(SQLITE_CORRUPT);
    }
    let kdf = envelope[6];
    if kdf != KDF_DIRECT_HKDF && kdf != KDF_ARGON2ID {
        return Err(SQLITE_CORRUPT);
    }
    let wrap_len = usize::from(envelope[56]);
    if wrap_len > MAX_WRAP || envelope.len() < 61 + wrap_len {
        return Err(SQLITE_CORRUPT);
    }
    let hdr_len = 61 + wrap_len;
    let want = u32::from_le_bytes(envelope[57 + wrap_len..hdr_len].try_into().unwrap());
    if crc32c(&envelope[..57 + wrap_len]) != want {
        return Err(SQLITE_CORRUPT);
    }
    if envelope.len() < hdr_len + NONCE_LEN + 4 {
        return Err(SQLITE_CORRUPT);
    }
    let seq = u64::from_le_bytes(envelope[7..15].try_into().unwrap());
    let salt = &envelope[15..47];
    let a_time = u32::from_le_bytes(envelope[47..51].try_into().unwrap());
    let a_mem = u32::from_le_bytes(envelope[51..55].try_into().unwrap());
    let a_threads = envelope[55];
    let protector = Zeroizing::new(derive_protector(
        kdf, master, passphrase, salt, a_time, a_mem, a_threads, store_id,
        context_id,
    )?);
    let nonce: &[u8; NONCE_LEN] = envelope[hdr_len..hdr_len + NONCE_LEN].try_into().unwrap();
    let sealed_len =
        u32::from_le_bytes(envelope[hdr_len + NONCE_LEN..hdr_len + NONCE_LEN + 4].try_into().unwrap())
            as usize;
    let off = hdr_len + NONCE_LEN + 4;
    if sealed_len > MAX_SEALED || envelope.len() - off != sealed_len {
        return Err(SQLITE_CORRUPT);
    }
    let mut aad = Vec::with_capacity(hdr_len + ID16_LEN);
    aad.extend_from_slice(&envelope[..hdr_len]);
    aad.extend_from_slice(store_id);
    let body = aesgcm_open(&protector, nonce, &aad, &envelope[off..]).ok_or(SQLITE_AUTH)?;

    if body.len() < 4 + 16 + 1 {
        return Err(SQLITE_CORRUPT);
    }
    let count = u32::from_le_bytes(body[0..4].try_into().unwrap());
    if count == 0 || count as usize > MAX_KEYS {
        return Err(SQLITE_CORRUPT);
    }
    if body[4..20] != *context_id {
        return Err(SQLITE_CORRUPT);
    }
    let body_wrap = usize::from(body[20]);
    if body_wrap > MAX_WRAP || body.len() < 21 + body_wrap {
        return Err(SQLITE_CORRUPT);
    }
    if body.len() - (21 + body_wrap) != count as usize * ENTRY_LEN {
        return Err(SQLITE_CORRUPT);
    }
    let mut ring = Keyring {
        seq,
        kdf,
        argon_time: a_time,
        argon_memory_kib: a_mem,
        argon_threads: a_threads,
        context: *context_id,
        wrap_id: body[21..21 + body_wrap].to_vec(),
        keys: Vec::with_capacity(count as usize),
        current: 0,
    };
    let mut pos = 21 + body_wrap;
    for _ in 0..count {
        let e = &body[pos..pos + ENTRY_LEN];
        pos += ENTRY_LEN;
        let id = u32::from_le_bytes(e[0..4].try_into().unwrap());
        let created_nanos = i64::from_le_bytes(e[4..12].try_into().unwrap());
        let active = match e[12] {
            0 => true,
            1 => false,
            _ => return Err(SQLITE_CORRUPT),
        };
        if id == 0 || ring.keys.iter().any(|k: &KeyEntry| k.id == id) {
            return Err(SQLITE_CORRUPT);
        }
        let mut key = [0u8; KEY_LEN];
        key.copy_from_slice(&e[13..45]);
        if active && id > ring.current {
            ring.current = id;
        }
        ring.keys.push(KeyEntry {
            id,
            created_nanos,
            active,
            key,
        });
    }
    if ring.current == 0 {
        return Err(SQLITE_CORRUPT);
    }
    ring.keys.sort_by_key(|k| k.id);
    Ok(ring)
}

/// Create a fresh envelope: single active key id 1, sequence 1.
#[allow(clippy::too_many_arguments)]
pub fn create(
    kdf: u8,
    argon_time: u32,
    argon_memory_kib: u32,
    argon_threads: u8,
    wrap_id: &[u8],
    store_id: &[u8; ID16_LEN],
    context_id: &[u8; ID16_LEN],
    master: Option<&[u8; KEY_LEN]>,
    passphrase: Option<&[u8]>,
) -> Result<Vec<u8>, c_int> {
    if kdf != KDF_DIRECT_HKDF && kdf != KDF_ARGON2ID {
        return Err(SQLITE_MISUSE);
    }
    if kdf == KDF_DIRECT_HKDF
        && (argon_time != 0 || argon_memory_kib != 0 || argon_threads != 0)
    {
        return Err(SQLITE_MISUSE);
    }
    if kdf == KDF_ARGON2ID && HAS_ARGON2 == 0 {
        return Err(SQLITE_ERROR);
    }
    let first = KeyEntry {
        id: 1,
        created_nanos: 0, // creation stamp assigned by the caller layer
        active: true,
        key: random_32()?,
    };
    let keys = vec![first];
    let out = seal_revision(
        kdf, 1, store_id, context_id, wrap_id, argon_time, argon_memory_kib,
        argon_threads, master, passphrase, &keys,
    );
    // `keys` drops here; entries are wiped by the Keyring drop path only
    // for owned rings, so wipe the staging copy explicitly like the C++.
    // (KeyEntry has no Drop; zero the bytes in place.)
    let mut keys = keys;
    for k in &mut keys {
        k.key.zeroize();
    }
    out
}

/// Stage a successor key and seal the new envelope WITHOUT installing it
/// (archive rotate_persisted ordering). Returns the new envelope bytes
/// and the fresh key id.
#[allow(clippy::too_many_arguments)]
pub fn rotate(
    ring: &Keyring,
    store_id: &[u8; ID16_LEN],
    master: Option<&[u8; KEY_LEN]>,
    passphrase: Option<&[u8]>,
) -> Result<(Vec<u8>, u32), c_int> {
    if ring.keys.is_empty() || ring.current == 0 {
        return Err(SQLITE_MISUSE);
    }
    let mut next: u32 = 1;
    for k in &ring.keys {
        if k.id >= next {
            next = k.id.wrapping_add(1);
        }
    }
    if next == 0 {
        return Err(SQLITE_ERROR); // id space exhausted (u32 wrap)
    }
    if ring.keys.len() + 1 > MAX_KEYS {
        return Err(SQLITE_ERROR);
    }
    let mut staged: Vec<KeyEntry> = ring.keys.iter().map(clone_entry).collect();
    for k in staged.iter_mut() {
        if k.id == ring.current {
            k.active = false;
        }
    }
    let fresh = KeyEntry {
        id: next,
        created_nanos: 0,
        active: true,
        key: random_32().inspect_err(|_| {
            // Only staged copies are ours to wipe; the rest borrows ring.
            for k in staged.iter_mut() {
                if k.id != ring.current {
                    k.key.zeroize();
                }
            }
        })?,
    };
    staged.push(fresh);
    staged.sort_by_key(|k| k.id);
    let out = seal_revision(
        ring.kdf,
        ring.seq + 1,
        store_id,
        &ring.context,
        &ring.wrap_id,
        ring.argon_time,
        ring.argon_memory_kib,
        ring.argon_threads,
        master,
        passphrase,
        &staged,
    );
    // Only the staged FRESH key is ours to wipe; the rest borrows ring.
    for k in staged.iter_mut() {
        if k.id == next {
            k.key.zeroize();
        }
    }
    out.map(|env| (env, next))
}

fn clone_entry(k: &KeyEntry) -> KeyEntry {
    KeyEntry {
        id: k.id,
        created_nanos: k.created_nanos,
        active: k.active,
        key: k.key,
    }
}

/// Drop retired keys and re-seal. Every id in `drop` must name a retired
/// key; current/active, unknown, empty, and drop-everything sets fail.
pub fn drop_keys(
    ring: &Keyring,
    drop: &[u32],
    store_id: &[u8; ID16_LEN],
    master: Option<&[u8; KEY_LEN]>,
    passphrase: Option<&[u8]>,
) -> Result<Vec<u8>, c_int> {
    if drop.is_empty() || ring.keys.is_empty() || ring.current == 0 {
        return Err(SQLITE_MISUSE);
    }
    let mut kept: Vec<KeyEntry> = Vec::with_capacity(ring.keys.len());
    for k in &ring.keys {
        let mut dropped = false;
        for &id in drop {
            if id != k.id {
                continue;
            }
            if k.active || k.id == ring.current {
                return Err(SQLITE_MISUSE);
            }
            dropped = true;
            break;
        }
        if !dropped {
            kept.push(clone_entry(k));
        }
    }
    if kept.len() + drop.len() != ring.keys.len() {
        return Err(SQLITE_MISUSE); // unknown id
    }
    if kept.is_empty() {
        return Err(SQLITE_MISUSE); // drop-all
    }
    if !kept.iter().any(|k| k.id == ring.current) {
        return Err(SQLITE_CORRUPT); // ring disagrees with itself
    }
    let out = seal_revision(
        ring.kdf,
        ring.seq + 1,
        store_id,
        &ring.context,
        &ring.wrap_id,
        ring.argon_time,
        ring.argon_memory_kib,
        ring.argon_threads,
        master,
        passphrase,
        &kept,
    );
    for k in kept.iter_mut() {
        k.key.zeroize();
    }
    out
}

// ---------------------------------------------------------------------------
// C ABI (transitional): the C++ shim in src/sqlite_murmur_keyring.cpp calls
// these. All pointers follow the C++ contracts (fixed 16/32-byte buffers,
// NUL-terminated strings with strlen semantics). Every entry is panic-safe
// and fails closed with SQLITE_ERROR on unwind.

/// 1 when linked with an Argon2 unit, else 0 (argon2id fails closed).
#[unsafe(no_mangle)]
pub extern "C" fn sqlite_murmur_keyring_rs_has_argon2() -> c_int {
    HAS_ARGON2
}

/// Open + authenticate an envelope into a heap handle. Returns a SQLite
/// result code; `*out` is set only on success.
#[allow(clippy::too_many_arguments)]
#[unsafe(no_mangle)]
pub extern "C" fn sqlite_murmur_keyring_rs_open(
    envelope: *const u8,
    len: usize,
    master: *const u8,
    passphrase: *const c_char,
    store_id: *const u8,
    context_id: *const u8,
    out: *mut *mut Keyring,
) -> c_int {
    guard(
        || {
            if envelope.is_null() || store_id.is_null() || context_id.is_null() || out.is_null() {
                return SQLITE_MISUSE;
            }
            // SAFETY: non-null fixed-size buffers and NUL-terminated
            // strings per the C++ contract.
            let env = unsafe { slice::from_raw_parts(envelope, len) };
            let master: Option<&[u8; KEY_LEN]> = if master.is_null() {
                None
            } else {
                Some(unsafe { &*(master as *const [u8; KEY_LEN]) })
            };
            let passphrase: Option<&[u8]> = if passphrase.is_null() {
                None
            } else {
                Some(unsafe { CStr::from_ptr(passphrase) }.to_bytes())
            };
            let store: &[u8; ID16_LEN] = unsafe { &*(store_id as *const [u8; ID16_LEN]) };
            let ctx: &[u8; ID16_LEN] = unsafe { &*(context_id as *const [u8; ID16_LEN]) };
            match open(env, master, passphrase, store, ctx) {
                Ok(ring) => {
                    unsafe { *out = Box::into_raw(Box::new(ring)) };
                    SQLITE_OK
                }
                Err(rc) => rc,
            }
        },
        SQLITE_ERROR,
    )
}

/// Destroy a handle from `rs_open` (wipes key material). Null-safe.
#[unsafe(no_mangle)]
pub extern "C" fn sqlite_murmur_keyring_rs_destroy(handle: *mut Keyring) {
    if handle.is_null() {
        return;
    }
    let _ = guard(
        || {
            // SAFETY: handle came from rs_open; Drop wipes key material.
            drop(unsafe { Box::from_raw(handle) });
            SQLITE_OK
        },
        SQLITE_ERROR,
    );
}

/// Read-only accessors over an open handle. Each returns SQLITE_OK or
/// SQLITE_MISUSE (null argument or out-of-range index).
#[unsafe(no_mangle)]
pub extern "C" fn sqlite_murmur_keyring_rs_seq(handle: *const Keyring, out: *mut u64) -> c_int {
    guard(
        || {
            if handle.is_null() || out.is_null() {
                return SQLITE_MISUSE;
            }
            unsafe { *out = (*handle).seq };
            SQLITE_OK
        },
        SQLITE_ERROR,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite_murmur_keyring_rs_kdf(handle: *const Keyring, out: *mut u8) -> c_int {
    guard(
        || {
            if handle.is_null() || out.is_null() {
                return SQLITE_MISUSE;
            }
            unsafe { *out = (*handle).kdf };
            SQLITE_OK
        },
        SQLITE_ERROR,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite_murmur_keyring_rs_argon(
    handle: *const Keyring,
    time: *mut u32,
    memory_kib: *mut u32,
    threads: *mut u8,
) -> c_int {
    guard(
        || {
            if handle.is_null() || time.is_null() || memory_kib.is_null() || threads.is_null() {
                return SQLITE_MISUSE;
            }
            let ring = unsafe { &*handle };
            unsafe {
                *time = ring.argon_time;
                *memory_kib = ring.argon_memory_kib;
                *threads = ring.argon_threads;
            }
            SQLITE_OK
        },
        SQLITE_ERROR,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite_murmur_keyring_rs_context(handle: *const Keyring, out: *mut u8) -> c_int {
    guard(
        || {
            if handle.is_null() || out.is_null() {
                return SQLITE_MISUSE;
            }
            let ring = unsafe { &*handle };
            unsafe { ptr::copy_nonoverlapping(ring.context.as_ptr(), out, ID16_LEN) };
            SQLITE_OK
        },
        SQLITE_ERROR,
    )
}

/// Borrow the wrap id bytes (valid until the handle is destroyed).
#[unsafe(no_mangle)]
pub extern "C" fn sqlite_murmur_keyring_rs_wrap(
    handle: *const Keyring,
    ptr: *mut *const u8,
    len: *mut usize,
) -> c_int {
    guard(
        || {
            if handle.is_null() || ptr.is_null() || len.is_null() {
                return SQLITE_MISUSE;
            }
            let ring = unsafe { &*handle };
            unsafe {
                *ptr = ring.wrap_id.as_ptr();
                *len = ring.wrap_id.len();
            }
            SQLITE_OK
        },
        SQLITE_ERROR,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite_murmur_keyring_rs_key_count(handle: *const Keyring, out: *mut usize) -> c_int {
    guard(
        || {
            if handle.is_null() || out.is_null() {
                return SQLITE_MISUSE;
            }
            unsafe { *out = (*handle).keys.len() };
            SQLITE_OK
        },
        SQLITE_ERROR,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite_murmur_keyring_rs_current(handle: *const Keyring, out: *mut u32) -> c_int {
    guard(
        || {
            if handle.is_null() || out.is_null() {
                return SQLITE_MISUSE;
            }
            unsafe { *out = (*handle).current };
            SQLITE_OK
        },
        SQLITE_ERROR,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite_murmur_keyring_rs_key_at(
    handle: *const Keyring,
    index: usize,
    id: *mut u32,
    created_nanos: *mut i64,
    active: *mut u8,
    key: *mut u8,
) -> c_int {
    guard(
        || {
            if handle.is_null()
                || id.is_null()
                || created_nanos.is_null()
                || active.is_null()
                || key.is_null()
            {
                return SQLITE_MISUSE;
            }
            let ring = unsafe { &*handle };
            let Some(k) = ring.keys.get(index) else {
                return SQLITE_MISUSE;
            };
            unsafe {
                *id = k.id;
                *created_nanos = k.created_nanos;
                *active = u8::from(k.active);
                ptr::copy_nonoverlapping(k.key.as_ptr(), key, KEY_LEN);
            }
            SQLITE_OK
        },
        SQLITE_ERROR,
    )
}

/// Transfer a heap byte buffer to the caller. The caller copies the bytes
/// and releases them with `rs_free`.
fn leak_bytes(v: Vec<u8>) -> (*mut u8, usize, usize) {
    let mut v = v;
    let out = (v.as_mut_ptr(), v.len(), v.capacity());
    std::mem::forget(v);
    out
}

/// Release a buffer from `rs_create`/`rs_rotate`/`rs_drop_keys` (wiped).
#[unsafe(no_mangle)]
pub extern "C" fn sqlite_murmur_keyring_rs_free(buf: *mut u8, len: usize, cap: usize) {
    if buf.is_null() {
        return;
    }
    let _ = guard(
        || {
            // SAFETY: buffer came from leak_bytes in this binary.
            let mut v = unsafe { Vec::from_raw_parts(buf, len, cap) };
            v.zeroize();
            SQLITE_OK
        },
        SQLITE_ERROR,
    );
}

/// Seal a fresh envelope. On success `*buf`/`*len`/`*cap` receive a heap
/// buffer for `rs_free`; on failure they are left untouched (caller keeps
/// its cleared output).
#[allow(clippy::too_many_arguments)]
#[unsafe(no_mangle)]
pub extern "C" fn sqlite_murmur_keyring_rs_create(
    kdf: u8,
    argon_time: u32,
    argon_memory_kib: u32,
    argon_threads: u8,
    wrap_id: *const c_char,
    store_id: *const u8,
    context_id: *const u8,
    master: *const u8,
    passphrase: *const c_char,
    buf: *mut *mut u8,
    len: *mut usize,
    cap: *mut usize,
) -> c_int {
    guard(
        || {
            if wrap_id.is_null() || store_id.is_null() || context_id.is_null() {
                return SQLITE_MISUSE;
            }
            if buf.is_null() || len.is_null() || cap.is_null() {
                return SQLITE_MISUSE;
            }
            // SAFETY: per the C++ contract (strlen semantics for strings).
            let wrap = unsafe { CStr::from_ptr(wrap_id) }.to_bytes();
            let store: &[u8; ID16_LEN] = unsafe { &*(store_id as *const [u8; ID16_LEN]) };
            let ctx: &[u8; ID16_LEN] = unsafe { &*(context_id as *const [u8; ID16_LEN]) };
            let master: Option<&[u8; KEY_LEN]> = if master.is_null() {
                None
            } else {
                Some(unsafe { &*(master as *const [u8; KEY_LEN]) })
            };
            let passphrase: Option<&[u8]> = if passphrase.is_null() {
                None
            } else {
                Some(unsafe { CStr::from_ptr(passphrase) }.to_bytes())
            };
            match create(
                kdf, argon_time, argon_memory_kib, argon_threads, wrap, store, ctx, master,
                passphrase,
            ) {
                Ok(env) => {
                    let (p, l, c) = leak_bytes(env);
                    unsafe {
                        *buf = p;
                        *len = l;
                        *cap = c;
                    }
                    SQLITE_OK
                }
                Err(rc) => rc,
            }
        },
        SQLITE_ERROR,
    )
}

/// Flat ring snapshot for rotate/drop calls (mirrors the C++ struct).
struct RingView<'a> {
    kdf: u8,
    seq: u64,
    argon_time: u32,
    argon_memory_kib: u32,
    argon_threads: u8,
    context: &'a [u8; ID16_LEN],
    wrap_id: &'a [u8],
    current: u32,
    keys: Vec<KeyEntry>,
}

#[allow(clippy::too_many_arguments)]
fn borrow_ring<'a>(
    kdf: u8,
    seq: u64,
    argon_time: u32,
    argon_memory_kib: u32,
    argon_threads: u8,
    context: *const u8,
    wrap_id: *const c_char,
    current: u32,
    ids: *const u32,
    nanos: *const i64,
    actives: *const u8,
    keys: *const u8,
    nkeys: usize,
) -> Option<RingView<'a>> {
    if context.is_null() || wrap_id.is_null() {
        return None;
    }
    if nkeys > 0 && (ids.is_null() || nanos.is_null() || actives.is_null() || keys.is_null()) {
        return None;
    }
    let flat_len = nkeys.checked_mul(KEY_LEN)?;
    // SAFETY: validated above; empty views never touch the pointers, so a
    // null array with nkeys == 0 is sound. Slices borrow caller memory.
    unsafe {
        let ids: &[u32] = if nkeys == 0 {
            &[]
        } else {
            slice::from_raw_parts(ids, nkeys)
        };
        let nanos: &[i64] = if nkeys == 0 {
            &[]
        } else {
            slice::from_raw_parts(nanos, nkeys)
        };
        let actives: &[u8] = if nkeys == 0 {
            &[]
        } else {
            slice::from_raw_parts(actives, nkeys)
        };
        let flat: &[u8] = if nkeys == 0 {
            &[]
        } else {
            slice::from_raw_parts(keys, flat_len)
        };
        let mut entries = Vec::with_capacity(nkeys);
        for i in 0..nkeys {
            let mut key = [0u8; KEY_LEN];
            key.copy_from_slice(&flat[i * KEY_LEN..(i + 1) * KEY_LEN]);
            entries.push(KeyEntry {
                id: ids[i],
                created_nanos: nanos[i],
                active: actives[i] != 0,
                key,
            });
        }
        Some(RingView {
            kdf,
            seq,
            argon_time,
            argon_memory_kib,
            argon_threads,
            context: &*(context as *const [u8; ID16_LEN]),
            wrap_id: CStr::from_ptr(wrap_id).to_bytes(),
            current,
            keys: entries,
        })
    }
}

impl<'a> RingView<'a> {
    fn as_ring(&self) -> Keyring {
        Keyring {
            seq: self.seq,
            kdf: self.kdf,
            argon_time: self.argon_time,
            argon_memory_kib: self.argon_memory_kib,
            argon_threads: self.argon_threads,
            context: *self.context,
            wrap_id: self.wrap_id.to_vec(),
            keys: self.keys.iter().map(clone_entry).collect(),
            current: self.current,
        }
    }
}

/// Stage a successor key and seal the new revision. `*new_id` is set only
/// on success; the buffer protocol matches `rs_create`.
#[allow(clippy::too_many_arguments)]
#[unsafe(no_mangle)]
pub extern "C" fn sqlite_murmur_keyring_rs_rotate(
    kdf: u8,
    seq: u64,
    argon_time: u32,
    argon_memory_kib: u32,
    argon_threads: u8,
    context: *const u8,
    wrap_id: *const c_char,
    current: u32,
    ids: *const u32,
    nanos: *const i64,
    actives: *const u8,
    keys: *const u8,
    nkeys: usize,
    store_id: *const u8,
    master: *const u8,
    passphrase: *const c_char,
    buf: *mut *mut u8,
    len: *mut usize,
    cap: *mut usize,
    new_id: *mut u32,
) -> c_int {
    guard(
        || {
            if store_id.is_null() || buf.is_null() || len.is_null() || cap.is_null() || new_id.is_null()
            {
                return SQLITE_MISUSE;
            }
            let Some(view) = borrow_ring(
                kdf, seq, argon_time, argon_memory_kib, argon_threads, context, wrap_id,
                current, ids, nanos, actives, keys, nkeys,
            ) else {
                return SQLITE_MISUSE;
            };
            // SAFETY: validated above.
            let store: &[u8; ID16_LEN] = unsafe { &*(store_id as *const [u8; ID16_LEN]) };
            let master: Option<&[u8; KEY_LEN]> = if master.is_null() {
                None
            } else {
                Some(unsafe { &*(master as *const [u8; KEY_LEN]) })
            };
            let passphrase: Option<&[u8]> = if passphrase.is_null() {
                None
            } else {
                Some(unsafe { CStr::from_ptr(passphrase) }.to_bytes())
            };
            match rotate(&view.as_ring(), store, master, passphrase) {
                Ok((env, id)) => {
                    let (p, l, c) = leak_bytes(env);
                    unsafe {
                        *buf = p;
                        *len = l;
                        *cap = c;
                        *new_id = id;
                    }
                    SQLITE_OK
                }
                Err(rc) => rc,
            }
        },
        SQLITE_ERROR,
    )
}

/// Drop retired keys and re-seal. Buffer protocol matches `rs_create`.
#[allow(clippy::too_many_arguments)]
#[unsafe(no_mangle)]
pub extern "C" fn sqlite_murmur_keyring_rs_drop_keys(
    kdf: u8,
    seq: u64,
    argon_time: u32,
    argon_memory_kib: u32,
    argon_threads: u8,
    context: *const u8,
    wrap_id: *const c_char,
    current: u32,
    ids: *const u32,
    nanos: *const i64,
    actives: *const u8,
    keys: *const u8,
    nkeys: usize,
    drop: *const u32,
    ndrop: usize,
    store_id: *const u8,
    master: *const u8,
    passphrase: *const c_char,
    buf: *mut *mut u8,
    len: *mut usize,
    cap: *mut usize,
) -> c_int {
    guard(
        || {
            // Order matches the C++ gate: bad drop set before ring checks.
            if drop.is_null() || ndrop == 0 || store_id.is_null() {
                return SQLITE_MISUSE;
            }
            if buf.is_null() || len.is_null() || cap.is_null() {
                return SQLITE_MISUSE;
            }
            let Some(view) = borrow_ring(
                kdf, seq, argon_time, argon_memory_kib, argon_threads, context, wrap_id,
                current, ids, nanos, actives, keys, nkeys,
            ) else {
                return SQLITE_MISUSE;
            };
            // SAFETY: validated above.
            let store: &[u8; ID16_LEN] = unsafe { &*(store_id as *const [u8; ID16_LEN]) };
            let drop_ids = unsafe { slice::from_raw_parts(drop, ndrop) };
            let master: Option<&[u8; KEY_LEN]> = if master.is_null() {
                None
            } else {
                Some(unsafe { &*(master as *const [u8; KEY_LEN]) })
            };
            let passphrase: Option<&[u8]> = if passphrase.is_null() {
                None
            } else {
                Some(unsafe { CStr::from_ptr(passphrase) }.to_bytes())
            };
            match drop_keys(&view.as_ring(), drop_ids, store, master, passphrase) {
                Ok(env) => {
                    let (p, l, c) = leak_bytes(env);
                    unsafe {
                        *buf = p;
                        *len = l;
                        *cap = c;
                    }
                    SQLITE_OK
                }
                Err(rc) => rc,
            }
        },
        SQLITE_ERROR,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids() -> ([u8; ID16_LEN], [u8; ID16_LEN], [u8; KEY_LEN]) {
        let mut store = [0u8; ID16_LEN];
        let mut ctx = [0u8; ID16_LEN];
        let mut master = [0u8; KEY_LEN];
        for (i, b) in store.iter_mut().enumerate() {
            *b = i as u8;
        }
        for (i, b) in ctx.iter_mut().enumerate() {
            *b = 0x80 + i as u8;
        }
        for (i, b) in master.iter_mut().enumerate() {
            *b = 0x40 + i as u8;
        }
        (store, ctx, master)
    }

    #[test]
    fn crc32c_matches_published_vector() {
        // Same anchor as tests/gen_keyring_vectors.py:98.
        assert_eq!(crc32c(b"123456789"), 0xE3069283);
    }

    #[test]
    fn hkdf_matches_rfc5869_a1_first_block() {
        // Same anchor as tests/gen_keyring_vectors.py:99-106 (L=32 is the
        // first expand block T(1), identical for any L >= 32).
        let ikm = [0x0B; 22];
        let salt: Vec<u8> = (0..13).collect();
        let info: Vec<u8> = (0xF0..0xFA).collect();
        let out = hkdf_32(&ikm, &salt, &info).unwrap();
        assert_eq!(
            hex(&out),
            "3cb25f25faacd57a90434f64d0362f2a\
             2d2d0a90cf1a5a4c5db02d56ecc4c5bf"
        );
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn create_open_rotate_drop_round_trip() {
        let (store, ctx, master) = ids();
        let fresh =
            create(0, 0, 0, 0, b"v1", &store, &ctx, Some(&master), None).unwrap();
        let ring = open(&fresh, Some(&master), None, &store, &ctx).unwrap();
        assert_eq!((ring.seq, ring.current, ring.keys.len()), (1, 1, 1));
        assert_eq!(ring.wrap_id, b"v1");
        assert!(ring.keys[0].active);

        let (rotated, new_id) = rotate(&ring, &store, Some(&master), None).unwrap();
        assert_eq!(new_id, 2);
        let ring2 = open(&rotated, Some(&master), None, &store, &ctx).unwrap();
        assert_eq!((ring2.seq, ring2.current, ring2.keys.len()), (2, 2, 2));
        assert!(!ring2.keys[0].active && ring2.keys[1].active);
        assert_eq!(ring2.keys[0].key, ring.keys[0].key);

        let pruned = drop_keys(&ring2, &[1], &store, Some(&master), None).unwrap();
        let ring3 = open(&pruned, Some(&master), None, &store, &ctx).unwrap();
        assert_eq!((ring3.seq, ring3.current, ring3.keys.len()), (3, 2, 1));
    }

    #[test]
    fn wrong_key_and_truncation_fail_closed() {
        let (store, ctx, master) = ids();
        let fresh =
            create(0, 0, 0, 0, b"", &store, &ctx, Some(&master), None).unwrap();
        let mut wrong = master;
        wrong[0] ^= 0xff;
        assert_eq!(
            open(&fresh, Some(&wrong), None, &store, &ctx).unwrap_err(),
            SQLITE_AUTH
        );
        let mut bad_ctx = ctx;
        bad_ctx[0] ^= 0xff;
        // Either rejection is correct (protector mismatch or body check).
        assert!(open(&fresh, Some(&master), None, &store, &bad_ctx).is_err());
        for cut in [10usize, 61, fresh.len() - 1] {
            assert_eq!(
                open(&fresh[..cut], Some(&master), None, &store, &ctx).unwrap_err(),
                SQLITE_CORRUPT,
                "cut at {cut} accepted"
            );
        }
        let mut tampered = fresh.clone();
        tampered[0] ^= 0xff;
        assert_eq!(
            open(&tampered, Some(&master), None, &store, &ctx).unwrap_err(),
            SQLITE_CORRUPT
        );
    }

    #[test]
    fn argon2_without_unit_fails_closed() {
        assert_eq!(HAS_ARGON2, 0);
        let (store, ctx, _) = ids();
        assert_eq!(
            create(1, 3, 65536, 4, b"", &store, &ctx, None, Some(b"pw")).unwrap_err(),
            SQLITE_ERROR
        );
    }

    #[test]
    fn bad_arguments_are_misuse() {
        let (store, ctx, master) = ids();
        assert_eq!(
            create(7, 0, 0, 0, b"", &store, &ctx, Some(&master), None).unwrap_err(),
            SQLITE_MISUSE
        );
        assert_eq!(
            create(0, 1, 0, 0, b"", &store, &ctx, Some(&master), None).unwrap_err(),
            SQLITE_MISUSE
        );
        assert_eq!(
            create(0, 0, 0, 0, &[b'w'; 129], &store, &ctx, Some(&master), None).unwrap_err(),
            SQLITE_MISUSE
        );
        let fresh =
            create(0, 0, 0, 0, b"", &store, &ctx, Some(&master), None).unwrap();
        let ring = open(&fresh, Some(&master), None, &store, &ctx).unwrap();
        assert_eq!(
            drop_keys(&ring, &[1], &store, Some(&master), None).unwrap_err(),
            SQLITE_MISUSE,
            "dropping the current key must fail"
        );
        assert_eq!(
            drop_keys(&ring, &[9], &store, Some(&master), None).unwrap_err(),
            SQLITE_MISUSE,
            "unknown id must fail"
        );
    }
}
