//! Verbatim-logic port of the archived SPFO container codec.
//!
//! Byte layout, framing, nonces, KDF, chunking, bounds, and error strings are
//! identical to `murmur-tx` `files.rs` (`Upload` seal path + `verify_input` /
//! `Download::next` open path); only the byte transport differs (row slices
//! instead of files). Proven equivalent by cross-implementation round-trip
//! tests in `objects.rs` (archived-seal to here-open and back, both
//! directions, sizes spanning every framing boundary). See PROVENANCE.md.

use crate::{Error, Result};
use aes_gcm::{
    Aes256Gcm, KeyInit,
    aead::{Aead, Payload},
};
use sha2::{Digest, Sha256};

/// Plaintext bytes per container frame (archived `CHUNK`).
pub(crate) const PLAIN_CHUNK: usize = 64 << 10;
/// Ciphertext bytes per chunk-table row (spec: slices are at most 65536 bytes).
pub(crate) const SLICE: usize = 64 << 10;
/// Magic + generation + salt + prefix header length.
const HEADER: usize = 40;

/// HKDF-SHA256 object key (archived `object_key`, verbatim).
pub(crate) fn object_key(
    master: &[u8; 32],
    salt: &[u8; 16],
) -> Result<zeroize::Zeroizing<[u8; 32]>> {
    let hk = hkdf::Hkdf::<Sha256>::new(Some(salt), master);
    let mut key = zeroize::Zeroizing::new([0; 32]);
    hk.expand(b"murmur/sqlite-file-object/v1", &mut *key)
        .map_err(|_| Error::Auth("object key derivation".into()))?;
    Ok(key)
}

/// Frame nonce: 8-byte prefix + big-endian frame index (archived `nonce`).
pub(crate) fn nonce(prefix: [u8; 8], index: u32) -> [u8; 12] {
    let mut n = [0; 12];
    n[..8].copy_from_slice(&prefix);
    n[8..].copy_from_slice(&index.to_be_bytes());
    n
}

/// Exact sealed container length for `plaintext_len` plaintext bytes: 40-byte
/// header, full 64KiB frames plus at most one partial frame (4 + len + 16
/// tag bytes each), and the terminal marker + encrypted `(length, digest)`
/// trailer. The trailer plaintext length comes from the real postcard
/// encoder, so no varint shape is reimplemented here.
pub(crate) fn container_len(plaintext_len: u64) -> Result<u64> {
    let full = plaintext_len / PLAIN_CHUNK as u64;
    let rem = plaintext_len % PLAIN_CHUNK as u64;
    let mut total = HEADER as u64;
    total = total
        .checked_add(
            full.checked_mul(PLAIN_CHUNK as u64 + 20)
                .ok_or(Error::Overloaded)?,
        )
        .ok_or(Error::Overloaded)?;
    if rem > 0 {
        total = total.checked_add(rem + 20).ok_or(Error::Overloaded)?;
    }
    let trailer = murmur_tx::encode(&(plaintext_len, [0u8; 32]))?;
    total = total
        .checked_add(8 + trailer.len() as u64 + 16)
        .ok_or(Error::Overloaded)?;
    Ok(total)
}

/// Expected chunk-row count for a complete object holding `plaintext_len`
/// plaintext bytes. `available()` compares the stored count against this.
pub(crate) fn expected_slices(plaintext_len: u64) -> Result<u64> {
    Ok(container_len(plaintext_len)?.div_ceil(SLICE as u64))
}

/// Minimal pull source over ordered bytes. Truncation surfaces through the
/// same `From<std::io::Error>` conversion the archived file-backed verifier
/// uses, so error values match; store failures propagate unchanged.
pub(crate) trait ByteSource {
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()>;
    fn read_max(&mut self, buf: &mut [u8]) -> Result<usize>;
}

impl<R: std::io::Read> ByteSource for R {
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        std::io::Read::read_exact(self, buf).map_err(Error::from)
    }

    fn read_max(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.read(buf).map_err(Error::from)
    }
}

/// Streaming container writer: plaintext in via [`SealWriter::feed`],
/// container bytes out through the `emit` callback in order. Framing mirrors
/// archived `Upload` exactly (full 64KiB frames, at most one partial last
/// frame, terminal); salt and prefix are random per seal like the archive.
/// Size caps are enforced by the driver before feeding (archived
/// `Upload::append` shape); this writer only frames and encrypts.
pub(crate) struct SealWriter<'e> {
    cipher: Aes256Gcm,
    header: Vec<u8>,
    prefix: [u8; 8],
    hash: Sha256,
    total: u64,
    index: u32,
    max: u64,
    pending: zeroize::Zeroizing<Vec<u8>>,
    emit: Box<dyn FnMut(&[u8]) -> Result<()> + Send + 'e>,
}

impl<'e> SealWriter<'e> {
    pub(crate) fn new(
        key: &[u8; 32],
        generation: u64,
        max: u64,
        emit: impl FnMut(&[u8]) -> Result<()> + Send + 'e,
    ) -> Result<Self> {
        let salt: [u8; 16] = rand::random();
        let derived = object_key(key, &salt)?;
        let prefix: [u8; 8] = rand::random();
        let mut header = b"SPFO0001".to_vec();
        header.extend(generation.to_be_bytes());
        header.extend(salt);
        header.extend(prefix);
        let mut this = Self {
            cipher: Aes256Gcm::new_from_slice(&*derived).unwrap(),
            header,
            prefix,
            hash: Sha256::new(),
            total: 0,
            index: 0,
            max,
            pending: zeroize::Zeroizing::new(Vec::with_capacity(PLAIN_CHUNK)),
            emit: Box::new(emit),
        };
        let header = this.header.clone();
        (this.emit)(&header)?;
        Ok(this)
    }

    pub(crate) fn total(&self) -> u64 {
        self.total
    }

    pub(crate) fn max(&self) -> u64 {
        self.max
    }

    pub(crate) fn feed(&mut self, mut bytes: &[u8]) -> Result<()> {
        let total = self
            .total
            .checked_add(bytes.len() as u64)
            .ok_or(Error::Overloaded)?;
        if total > self.max {
            return Err(Error::Overloaded);
        }
        self.hash.update(bytes);
        self.total = total;
        while !bytes.is_empty() {
            let n = (PLAIN_CHUNK - self.pending.len()).min(bytes.len());
            self.pending.extend_from_slice(&bytes[..n]);
            bytes = &bytes[n..];
            if self.pending.len() == PLAIN_CHUNK {
                self.flush_frame()?;
            }
        }
        Ok(())
    }

    fn flush_frame(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let mut aad = self.header.clone();
        aad.extend(self.index.to_be_bytes());
        let encrypted = self
            .cipher
            .encrypt(
                (&nonce(self.prefix, self.index)).into(),
                Payload {
                    msg: &self.pending,
                    aad: &aad,
                },
            )
            .map_err(|_| Error::Auth("object encryption".into()))?;
        let mut frame = (encrypted.len() as u32).to_be_bytes().to_vec();
        frame.extend_from_slice(&encrypted);
        (self.emit)(&frame)?;
        self.index = self.index.checked_add(1).ok_or(Error::Overloaded)?;
        use zeroize::Zeroize;
        self.pending.zeroize();
        self.pending.clear();
        Ok(())
    }

    /// Seal the container: partial frame, terminal marker, digest. Consumes
    /// the writer and returns `(plaintext_sha256, plaintext_bytes)`.
    pub(crate) fn finish(mut self) -> Result<([u8; 32], u64)> {
        self.flush_frame()?;
        let digest: [u8; 32] = self.hash.clone().finalize().into();
        let terminal = murmur_tx::encode(&(self.total, digest))?;
        let mut aad = self.header.clone();
        aad.extend(self.index.to_be_bytes());
        aad.extend(b"final");
        let encrypted = self
            .cipher
            .encrypt(
                (&nonce(self.prefix, self.index)).into(),
                Payload {
                    msg: &terminal,
                    aad: &aad,
                },
            )
            .map_err(|_| Error::Auth("object trailer encryption".into()))?;
        let mut frame = u32::MAX.to_be_bytes().to_vec();
        frame.extend_from_slice(&(encrypted.len() as u32).to_be_bytes());
        frame.extend_from_slice(&encrypted);
        (self.emit)(&frame)?;
        Ok((digest, self.total))
    }
}

/// Streaming container verifier/decryptor. Frame loop, key selection, bounds,
/// and terminal checks mirror archived `verify_input` exactly; the only
/// difference is the byte source (row slices instead of a file).
pub(crate) struct OpenReader<S> {
    source: S,
    cipher: Aes256Gcm,
    header: [u8; 40],
    prefix: [u8; 8],
    hash: Sha256,
    total: u64,
    index: u32,
    meta_bytes: u64,
    meta_digest: [u8; 32],
    max: u64,
}

impl<S: ByteSource> OpenReader<S> {
    pub(crate) fn open(
        mut source: S,
        key: &[u8; 32],
        previous: Option<&[u8; 32]>,
        current: u64,
        meta_bytes: u64,
        meta_digest: [u8; 32],
        max: u64,
    ) -> Result<Self> {
        let mut header = [0; 40];
        source.read_exact(&mut header)?;
        if &header[..8] != b"SPFO0001" {
            return Err(Error::Auth("object header".into()));
        }
        let generation = u64::from_be_bytes(header[8..16].try_into().unwrap());
        let key = if generation == current {
            key
        } else if generation.checked_add(1) == Some(current) {
            previous.ok_or_else(|| Error::Auth("object key unavailable".into()))?
        } else {
            return Err(Error::Auth("object key generation".into()));
        };
        let derived = object_key(key, header[16..32].try_into().unwrap())?;
        let cipher = Aes256Gcm::new_from_slice(&*derived).unwrap();
        let prefix = header[32..40].try_into().unwrap();
        Ok(Self {
            source,
            cipher,
            header,
            prefix,
            hash: Sha256::new(),
            total: 0,
            index: 0,
            meta_bytes,
            meta_digest,
            max,
        })
    }

    /// Decrypt the whole stream through the sink (archived `verify_input` body:
    /// per-frame auth, running length/digest, terminal + trailing checks).
    /// A sink (not `Write`) keeps seal-feed errors lossless on the rotation
    /// path; `Write` callers wrap with the same io conversion the archive uses.
    pub(crate) fn verify_to(&mut self, sink: &mut dyn FnMut(&[u8]) -> Result<()>) -> Result<()> {
        loop {
            let mut n = [0; 4];
            self.source.read_exact(&mut n)?;
            let mut size = u32::from_be_bytes(n) as usize;
            let final_chunk = size == u32::MAX as usize;
            if final_chunk {
                self.source.read_exact(&mut n)?;
                size = u32::from_be_bytes(n) as usize;
            }
            if size > PLAIN_CHUNK + 16 || size < 16 {
                return Err(Error::Auth("object chunk length".into()));
            }
            let mut encrypted = vec![0; size];
            self.source.read_exact(&mut encrypted)?;
            let mut aad = self.header.to_vec();
            aad.extend(self.index.to_be_bytes());
            if final_chunk {
                aad.extend(b"final");
            }
            let plain = self
                .cipher
                .decrypt(
                    (&nonce(self.prefix, self.index)).into(),
                    Payload {
                        msg: &encrypted,
                        aad: &aad,
                    },
                )
                .map_err(|_| Error::Auth("object authentication".into()))?;
            if final_chunk {
                let (length, digest): (u64, [u8; 32]) = murmur_tx::decode(&plain)?;
                if length != self.total
                    || length != self.meta_bytes
                    || digest != self.meta_digest
                    || <[u8; 32]>::from(self.hash.clone().finalize()) != digest
                    || self.source.read_max(&mut [0; 1])? != 0
                {
                    return Err(Error::Auth("object length or digest".into()));
                }
                return Ok(());
            }
            self.total = self
                .total
                .checked_add(plain.len() as u64)
                .ok_or(Error::Overloaded)?;
            if self.total > self.meta_bytes || self.total > self.max {
                return Err(Error::Overloaded);
            }
            self.hash.update(&plain);
            sink(&plain)?;
            self.index = self.index.checked_add(1).ok_or(Error::Overloaded)?;
        }
    }

    /// Decrypt the next plaintext frame (archived `Download::next` frame
    /// logic: terminal frames are never parsed here; length-bounded callers
    /// stop at `meta.bytes`, exactly like the archive).
    pub(crate) fn next_frame(&mut self) -> Result<Vec<u8>> {
        let mut n = [0; 4];
        self.source.read_exact(&mut n)?;
        let n = u32::from_be_bytes(n) as usize;
        if !(16..=PLAIN_CHUNK + 16).contains(&n) {
            return Err(Error::Auth("object chunk length".into()));
        }
        let mut encrypted = vec![0; n];
        self.source.read_exact(&mut encrypted)?;
        let mut aad = self.header.to_vec();
        aad.extend(self.index.to_be_bytes());
        let plain = self
            .cipher
            .decrypt(
                (&nonce(self.prefix, self.index)).into(),
                Payload {
                    msg: &encrypted,
                    aad: &aad,
                },
            )
            .map_err(|_| Error::Auth("object authentication".into()))?;
        self.index += 1;
        Ok(plain)
    }
}
