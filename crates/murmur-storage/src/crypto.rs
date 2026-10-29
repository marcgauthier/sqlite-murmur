//! Page-record crypto (EXP1-4), ported byte-for-byte from
//! `src/sqlite_murmur.cpp`.
//!
//! Layouts (`flag` = 1 byte compression marker, page ids big-endian,
//! key ids little-endian):
//!
//! - EXP1 `EXP1|flag|nonce12|ct|tag16`, AAD `flag|page_be8` (AES-GCM).
//! - EXP3 `EXP3|flag|keyid_le4|nonce12|ct|tag16`, AAD
//!   `flag|page_be8|keyid_le4` (sealed AES-GCM).
//! - EXP2 `EXP2|flag|nonce32|ct|tag32`, AAD `EXP2|flag|page_be8` (AEGIS).
//! - EXP4 `EXP4|flag|keyid_le4|nonce32|ct|tag32`, AAD
//!   `EXP4|flag|page_be8|keyid_le4` (sealed AEGIS).
//!
//! AES-GCM goes through the shared AWS-LC adapter (as in `keyring`),
//! AEGIS through the vendored libaegis FFI, nonces through AWS-LC.

use crate::crypto_provider::{Aes256Gcm, Payload};

use crate::ffi_zstd::{self, Dctx};
#[cfg(feature = "aegis256")]
use crate::ffi_aegis::{self, ABYTES_MAX, NPUBBYTES};

pub const BLOCK: usize = 4096;
pub const NONCE_LEN: usize = 12;
pub const TAG_LEN: usize = 16;
/// `ZSTD_compressBound(4096) + 1`, verified by unit test.
pub const SCRATCH_SIZE: usize = 4175;
pub const HEADER_AES: &[u8; 4] = b"EXP1";
pub const HEADER_AES_KEYED: &[u8; 4] = b"EXP3";
pub const HEADER_AEGIS: &[u8; 4] = b"EXP2";
pub const HEADER_AEGIS_KEYED: &[u8; 4] = b"EXP4";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Cipher {
    Aes256Gcm,
    Aegis256,
}

pub fn cipher_name(cipher: Cipher) -> &'static str {
    match cipher {
        Cipher::Aes256Gcm => "aes256gcm",
        Cipher::Aegis256 => "aegis256",
    }
}

/// RocksDB key for a page: `p` + big-endian u64.
pub fn page_key(page: u64) -> [u8; 9] {
    let mut out = [0u8; 9];
    out[0] = b'p';
    out[1..].copy_from_slice(&page.to_be_bytes());
    out
}

fn aes_aad(flag: u8, page: u64, key_id: Option<u32>) -> Vec<u8> {
    let mut aad = Vec::with_capacity(13);
    aad.push(flag);
    aad.extend_from_slice(&page.to_be_bytes());
    if let Some(id) = key_id {
        aad.extend_from_slice(&id.to_le_bytes());
    }
    aad
}

#[cfg(feature = "aegis256")]
fn aegis_aad(magic: &[u8; 4], flag: u8, page: u64, key_id: Option<u32>) -> Vec<u8> {
    let mut aad = Vec::with_capacity(17);
    aad.extend_from_slice(magic);
    aad.push(flag);
    aad.extend_from_slice(&page.to_be_bytes());
    if let Some(id) = key_id {
        aad.extend_from_slice(&id.to_le_bytes());
    }
    aad
}

fn random_nonce<const N: usize>() -> Option<[u8; N]> {
    let mut nonce = [0u8; N];
    crate::randomness::fill(&mut nonce).ok()?;
    Some(nonce)
}

fn encrypt_aes(
    key: &[u8; 32],
    key_id: Option<u32>,
    page: u64,
    data: &[u8],
    compressed: bool,
) -> Option<Vec<u8>> {
    if data.len() > i32::MAX as usize {
        return None;
    }
    if key_id == Some(0) {
        return None;
    }
    let flag = u8::from(compressed);
    let aad = aes_aad(flag, page, key_id);
    let nonce: [u8; NONCE_LEN] = random_nonce()?;
    let cipher = Aes256Gcm::new_from_slice(key).ok()?;
    let sealed = cipher
        .encrypt(
            &nonce,
            Payload { msg: data, aad: &aad },
        )
        .ok()?;
    let magic = if key_id.is_some() { HEADER_AES_KEYED } else { HEADER_AES };
    let mut out = Vec::with_capacity(9 + key_id.map_or(0, |_| 4) + sealed.len());
    out.extend_from_slice(magic);
    out.push(flag);
    if let Some(id) = key_id {
        out.extend_from_slice(&id.to_le_bytes());
    }
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&sealed);
    Some(out)
}

#[cfg(feature = "aegis256")]
fn encrypt_aegis(
    key: &[u8; 32],
    key_id: Option<u32>,
    page: u64,
    data: &[u8],
    compressed: bool,
) -> Option<Vec<u8>> {
    let flag = u8::from(compressed);
    let magic = if key_id.is_some() { HEADER_AEGIS_KEYED } else { HEADER_AEGIS };
    let aad = aegis_aad(magic, flag, page, key_id);
    let nonce: [u8; NPUBBYTES] = random_nonce()?;
    let mut out = Vec::with_capacity(9 + key_id.map_or(0, |_| 4) + data.len() + ABYTES_MAX);
    out.extend_from_slice(magic);
    out.push(flag);
    if let Some(id) = key_id {
        out.extend_from_slice(&id.to_le_bytes());
    }
    out.extend_from_slice(&nonce);
    let offset = out.len();
    out.resize(offset + data.len() + ABYTES_MAX, 0);
    let rc = unsafe {
        ffi_aegis::aegis256_encrypt(
            out[offset..].as_mut_ptr(),
            ABYTES_MAX,
            data.as_ptr(),
            data.len(),
            aad.as_ptr(),
            aad.len(),
            nonce.as_ptr(),
            key.as_ptr(),
        )
    };
    if rc != 0 {
        return None;
    }
    Some(out)
}

/// Encrypt one page payload. `key_id == 0` selects the legacy unkeyed
/// formats (EXP1/EXP2); nonzero selects the sealed formats (EXP3/EXP4).
/// Returns `None` on RNG failure, oversize input, or (without the
/// `aegis256` feature) AEGIS ciphers — mirroring the C++ `false`.
pub fn encrypt(
    cipher: Cipher,
    key: &[u8; 32],
    page: u64,
    data: &[u8],
    compressed: bool,
    key_id: u32,
) -> Option<Vec<u8>> {
    let keyed = if key_id == 0 { None } else { Some(key_id) };
    match cipher {
        Cipher::Aes256Gcm => encrypt_aes(key, keyed, page, data, compressed),
        #[cfg(feature = "aegis256")]
        Cipher::Aegis256 => encrypt_aegis(key, keyed, page, data, compressed),
        #[cfg(not(feature = "aegis256"))]
        Cipher::Aegis256 => None,
    }
}

/// Peek the key id of a keyed (EXP3/EXP4) record. `None` for legacy and
/// corrupt records; callers route those through the legacy trial chain.
pub fn record_key_id(record: &[u8]) -> Option<u32> {
    if record.len() < 4 {
        return None;
    }
    let exp3 = &record[..4] == HEADER_AES_KEYED;
    #[cfg(feature = "aegis256")]
    let exp4 = &record[..4] == HEADER_AEGIS_KEYED;
    #[cfg(not(feature = "aegis256"))]
    let exp4 = false;
    if !exp3 && !exp4 {
        return None;
    }
    const MIN_KEYED_AES: usize = 4 + 1 + 4 + NONCE_LEN + TAG_LEN;
    if exp3 && record.len() < MIN_KEYED_AES {
        return None;
    }
    #[cfg(feature = "aegis256")]
    if exp4 && record.len() < 4 + 1 + 4 + NPUBBYTES + ABYTES_MAX {
        return None;
    }
    Some(read_key_id(record))
}

/// Positively-identified unkeyed (EXP1/EXP2) records.
pub fn is_legacy_record(record: &[u8]) -> bool {
    if record.len() < 4 {
        return false;
    }
    const MIN_AES: usize = 4 + 1 + NONCE_LEN + TAG_LEN;
    if &record[..4] == HEADER_AES {
        return record.len() >= MIN_AES;
    }
    #[cfg(feature = "aegis256")]
    if &record[..4] == HEADER_AEGIS {
        return record.len() >= 5 + NPUBBYTES + ABYTES_MAX;
    }
    false
}

/// Scratch decryption state. Plaintext buffers wipe on reset/drop; the
/// zstd context is created lazily and reset when the workspace cycles.
pub struct DecodeWorkspace {
    plain: [u8; SCRATCH_SIZE],
    block: [u8; BLOCK],
    plain_used: usize,
    block_dirty: bool,
    dctx: Option<Dctx>,
}

impl DecodeWorkspace {
    pub fn new() -> Self {
        Self {
            plain: [0u8; SCRATCH_SIZE],
            block: [0u8; BLOCK],
            plain_used: 0,
            block_dirty: false,
            dctx: None,
        }
    }

    fn wipe_plain(&mut self) {
        if self.plain_used > 0 {
            let n = self.plain_used.min(SCRATCH_SIZE);
            crate::secure_wipe(&mut self.plain[..n]);
            self.plain_used = 0;
        }
    }

    pub(crate) fn wipe_block(&mut self) {
        if self.block_dirty {
            crate::secure_wipe(&mut self.block);
            self.block_dirty = false;
        }
    }

    pub(crate) fn mark_block_dirty(&mut self) {
        self.block_dirty = true;
    }

    pub(crate) fn block(&self) -> &[u8; BLOCK] {
        &self.block
    }

    pub(crate) fn block_mut(&mut self) -> &mut [u8; BLOCK] {
        &mut self.block
    }

    /// Wipe plaintext and reset the zstd session (pool-cycle hook).
    pub fn reset(&mut self) {
        self.wipe_plain();
        self.wipe_block();
        if let Some(dctx) = self.dctx.as_mut() {
            let _ = dctx.reset_session();
        }
    }

    fn decompress(&mut self, out: &mut [u8; BLOCK], size: usize) -> bool {
        if self.dctx.is_none() {
            // The C++ throws bad_alloc here (uncaught: abort). Failing
            // the decrypt closed is strictly better and unreachable in
            // practice (context creation only fails on OOM).
            self.dctx = Dctx::create();
            if self.dctx.is_none() {
                return false;
            }
        }
        let dctx = self.dctx.as_mut().expect("just created");
        matches!(dctx.decompress(out, &self.plain[..size]), Ok(BLOCK))
    }
}

impl Default for DecodeWorkspace {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for DecodeWorkspace {
    fn drop(&mut self) {
        self.reset();
    }
}

/// RAII counterpart of the C++ `PlainWiper`: wipes `plain` on scope exit.
struct PlainWiper<'a> {
    workspace: &'a mut DecodeWorkspace,
}

impl Drop for PlainWiper<'_> {
    fn drop(&mut self) {
        self.workspace.wipe_plain();
    }
}

fn read_key_id(record: &[u8]) -> u32 {
    let mut id = 0u32;
    for i in (0..4).rev() {
        id = (id << 8) | u32::from(record[5 + i]);
    }
    id
}

fn decrypt_aes_to_plain(
    key: &[u8; 32],
    keyed: bool,
    page: u64,
    record: &[u8],
    workspace: &mut DecodeWorkspace,
) -> Option<(bool, usize)> {
    let header_len = if keyed { 9 } else { 5 };
    let magic = if keyed { HEADER_AES_KEYED } else { HEADER_AES };
    if record.len() < header_len + NONCE_LEN + TAG_LEN || &record[..4] != magic {
        return None;
    }
    let flag = record[4];
    if flag > 1 {
        return None;
    }
    let key_id = keyed.then(|| read_key_id(record));
    let payload_size = record.len() - header_len - NONCE_LEN - TAG_LEN;
    if payload_size > ffi_zstd::compress_bound(BLOCK).unwrap_or(0) {
        return None;
    }
    let aad = aes_aad(flag, page, key_id);
    let nonce = &record[header_len..header_len + NONCE_LEN];
    let sealed = &record[header_len + NONCE_LEN..];
    let cipher = Aes256Gcm::new_from_slice(key).ok()?;
    let mut plain = cipher
        .decrypt(
            nonce,
            Payload { msg: sealed, aad: &aad },
        )
        .ok()?;
    debug_assert_eq!(plain.len(), payload_size);
    if plain.len() > SCRATCH_SIZE {
        return None;
    }
    let len = plain.len();
    workspace.plain[..len].copy_from_slice(&plain);
    crate::secure_wipe(&mut plain);
    // Mirror the C++ `plainUsed = payloadSize + 1` quirk (defensive extra
    // byte; always in bounds: payload <= compressBound < SCRATCH_SIZE).
    workspace.plain_used = (len + 1).min(SCRATCH_SIZE);
    Some((flag != 0, len))
}

#[cfg(feature = "aegis256")]
fn decrypt_aegis_to_plain(
    key: &[u8; 32],
    keyed: bool,
    page: u64,
    record: &[u8],
    workspace: &mut DecodeWorkspace,
) -> Option<(bool, usize)> {
    let header_len = if keyed { 9 } else { 5 };
    let magic = if keyed { HEADER_AEGIS_KEYED } else { HEADER_AEGIS };
    let overhead = header_len + NPUBBYTES + ABYTES_MAX;
    if record.len() < overhead || &record[..4] != magic {
        return None;
    }
    let flag = record[4];
    let key_id = keyed.then(|| read_key_id(record));
    let payload_size = record.len() - overhead;
    if flag > 1 || payload_size > ffi_zstd::compress_bound(BLOCK).unwrap_or(0) {
        return None;
    }
    let aad = aegis_aad(magic, flag, page, key_id);
    let nonce = &record[header_len..header_len + NPUBBYTES];
    let sealed = &record[header_len + NPUBBYTES..];
    debug_assert_eq!(sealed.len(), payload_size + ABYTES_MAX);
    workspace.plain_used = (payload_size + 1).min(SCRATCH_SIZE);
    let rc = unsafe {
        ffi_aegis::aegis256_decrypt(
            workspace.plain.as_mut_ptr(),
            sealed.as_ptr(),
            sealed.len(),
            ABYTES_MAX,
            aad.as_ptr(),
            aad.len(),
            nonce.as_ptr(),
            key.as_ptr(),
        )
    };
    if rc != 0 {
        return None;
    }
    Some((flag != 0, payload_size))
}

/// Finish a decrypt: raw blocks copy out (and must be exactly one
/// block); compressed payloads decompress (and must yield one block).
fn finish_decrypt(
    compressed: bool,
    payload_size: usize,
    out: &mut [u8; BLOCK],
    workspace: &mut DecodeWorkspace,
) -> bool {
    if !compressed {
        if payload_size != BLOCK {
            return false;
        }
        out.copy_from_slice(&workspace.plain[..BLOCK]);
        true
    } else {
        workspace.decompress(out, payload_size)
    }
}

/// Decrypt one page record into `out`. Dispatches on magic (EXP3/EXP4 go
/// keyed); unknown magic fails closed. Returns false on any failure —
/// authentication, bounds, flag, size, or decompression.
pub fn decrypt(
    cipher: Cipher,
    key: &[u8; 32],
    page: u64,
    record: &[u8],
    out: &mut [u8; BLOCK],
    workspace: &mut DecodeWorkspace,
) -> bool {
    workspace.block_dirty = true;
    match cipher {
        Cipher::Aes256Gcm => {
            let keyed = record.len() >= 4 && &record[..4] == HEADER_AES_KEYED;
            let step = decrypt_aes_to_plain(key, keyed, page, record, workspace);
            let _wiper = PlainWiper { workspace };
            match step {
                Some((compressed, size)) => {
                    finish_decrypt(compressed, size, out, &mut *_wiper.workspace)
                }
                None => false,
            }
        }
        #[cfg(feature = "aegis256")]
        Cipher::Aegis256 => {
            let keyed = record.len() >= 4 && &record[..4] == HEADER_AEGIS_KEYED;
            let step = decrypt_aegis_to_plain(key, keyed, page, record, workspace);
            let _wiper = PlainWiper { workspace };
            match step {
                Some((compressed, size)) => {
                    finish_decrypt(compressed, size, out, &mut *_wiper.workspace)
                }
                None => false,
            }
        }
        #[cfg(not(feature = "aegis256"))]
        Cipher::Aegis256 => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> [u8; 32] {
        let mut k = [0u8; 32];
        for (i, b) in k.iter_mut().enumerate() {
            *b = i as u8;
        }
        k
    }

    fn block_with(byte: u8) -> [u8; BLOCK] {
        [byte; BLOCK]
    }

    fn round_trip(cipher: Cipher, key_id: u32, compressed: bool) {
        let k = key();
        let page = 12345u64;
        let data = if compressed {
            // Highly compressible: exercises the zstd path on decrypt.
            vec![0x5au8; BLOCK]
        } else {
            block_with(0x5a).to_vec()
        };
        // Compress here like the encoder would (flag=1 means zstd frame).
        let (payload, flag) = if compressed {
            let mut cctx = crate::ffi_zstd::Cctx::create().expect("cctx");
            let mut dst = vec![0u8; crate::ffi_zstd::compress_bound(BLOCK).unwrap()];
            let n = cctx.compress(&mut dst, &data, 3).expect("compress");
            (dst[..n].to_vec(), true)
        } else {
            (data.clone(), false)
        };
        let record = encrypt(cipher, &k, page, &payload, flag, key_id).expect("encrypt");
        let expect_magic = match (cipher, key_id) {
            (Cipher::Aes256Gcm, 0) => HEADER_AES,
            (Cipher::Aes256Gcm, _) => HEADER_AES_KEYED,
            (Cipher::Aegis256, 0) => HEADER_AEGIS,
            (Cipher::Aegis256, _) => HEADER_AEGIS_KEYED,
        };
        assert_eq!(&record[..4], expect_magic);
        assert_eq!(record[4], u8::from(flag));
        assert_eq!(record_key_id(&record), if key_id == 0 { None } else { Some(key_id) });
        assert_eq!(is_legacy_record(&record), key_id == 0);

        let mut ws = DecodeWorkspace::new();
        let mut out = [0u8; BLOCK];
        assert!(decrypt(cipher, &k, page, &record, &mut out, &mut ws));
        assert_eq!(&out, &block_with(0x5a));

        // Tamper matrix: every region must fail closed.
        for pos in [0, 4, 6, record.len() / 2, record.len() - 1] {
            let mut bad = record.clone();
            bad[pos] ^= 0x80;
            let mut ws = DecodeWorkspace::new();
            let mut out = [0u8; BLOCK];
            assert!(!decrypt(cipher, &k, page, &bad, &mut out, &mut ws), "tamper at {pos}");
        }
        // Wrong key / wrong page fail closed.
        let mut wrong = k;
        wrong[0] ^= 1;
        let mut ws = DecodeWorkspace::new();
        let mut out = [0u8; BLOCK];
        assert!(!decrypt(cipher, &wrong, page, &record, &mut out, &mut ws));
        let mut ws = DecodeWorkspace::new();
        let mut out = [0u8; BLOCK];
        assert!(!decrypt(cipher, &k, page + 1, &record, &mut out, &mut ws));
    }

    #[test]
    fn scratch_matches_compress_bound() {
        assert_eq!(SCRATCH_SIZE, crate::ffi_zstd::compress_bound(BLOCK).unwrap() + 1);
        assert_eq!(page_key(0x0102030405060708), [b'p', 1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(cipher_name(Cipher::Aes256Gcm), "aes256gcm");
        assert_eq!(cipher_name(Cipher::Aegis256), "aegis256");
    }

    #[test]
    fn exp1_round_trip() {
        round_trip(Cipher::Aes256Gcm, 0, false);
        round_trip(Cipher::Aes256Gcm, 0, true);
    }

    #[test]
    fn exp3_round_trip() {
        round_trip(Cipher::Aes256Gcm, 7, false);
        round_trip(Cipher::Aes256Gcm, 7, true);
    }

    #[cfg(feature = "aegis256")]
    #[test]
    fn exp2_round_trip() {
        round_trip(Cipher::Aegis256, 0, false);
        round_trip(Cipher::Aegis256, 0, true);
    }

    #[cfg(feature = "aegis256")]
    #[test]
    fn exp4_round_trip() {
        round_trip(Cipher::Aegis256, 9, false);
        round_trip(Cipher::Aegis256, 9, true);
    }

    #[cfg(not(feature = "aegis256"))]
    #[test]
    fn aegis_absent_fails_closed() {
        let k = key();
        assert!(encrypt(Cipher::Aegis256, &k, 1, &[0u8; 16], false, 0).is_none());
        let mut ws = DecodeWorkspace::new();
        let mut out = [0u8; BLOCK];
        assert!(!decrypt(Cipher::Aegis256, &k, 1, b"EXP2junk", &mut out, &mut ws));
        assert_eq!(record_key_id(b"EXP4\x00\x01\x00\x00\x00"), None);
        assert!(!is_legacy_record(b"EXP2\x00paddingpaddingpad"));
    }

    #[test]
    fn corrupt_records_fail_closed() {
        let k = key();
        let mut ws = DecodeWorkspace::new();
        let mut out = [0u8; BLOCK];
        for record in [
            vec![],
            b"EX".to_vec(),
            b"BAD!".to_vec(),
            b"EXP1".to_vec(),
            b"EXP1\x02".to_vec(),
            vec![0u8; 4 + 1 + NONCE_LEN + TAG_LEN - 1],
            b"EXP9\x00AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_vec(),
        ] {
            assert!(!decrypt(Cipher::Aes256Gcm, &k, 3, &record, &mut out, &mut ws));
            assert_eq!(record_key_id(&record), None);
        }
        assert!(!is_legacy_record(b"EXP1"));
        assert!(!is_legacy_record(b"EXP3\x00\x01\x00\x00\x00AAAAAAAAAAAAAAAAAAAAAAAA"));
    }
}
