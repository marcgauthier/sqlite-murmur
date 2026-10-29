//! Safe wrappers over the vendored libzstd C API
//! (`third-party/zstd/lib/zstd.h`). Pages compress before encryption;
//! contexts are pooled per encoder thread, exactly like the C++
//! `EncodeWorkspace` / `DecodeWorkspace`.

#![allow(dead_code)]

use std::ffi::{CStr, c_char, c_void};

#[allow(non_camel_case_types)]
#[repr(C)]
pub struct ZSTD_CCtx {
    _private: [u8; 0],
}

#[allow(non_camel_case_types)]
#[repr(C)]
pub struct ZSTD_DCtx {
    _private: [u8; 0],
}

/// `ZSTD_reset_session_only` (zstd.h).
pub const RESET_SESSION_ONLY: i32 = 1;

unsafe extern "C" {
    pub(crate) unsafe fn ZSTD_createCCtx() -> *mut ZSTD_CCtx;
    pub(crate) unsafe fn ZSTD_freeCCtx(cctx: *mut ZSTD_CCtx) -> usize;
    pub(crate) unsafe fn ZSTD_compressCCtx(
        cctx: *mut ZSTD_CCtx,
        dst: *mut c_void,
        dst_capacity: usize,
        src: *const c_void,
        src_size: usize,
        compression_level: i32,
    ) -> usize;
    pub(crate) unsafe fn ZSTD_createDCtx() -> *mut ZSTD_DCtx;
    pub(crate) unsafe fn ZSTD_freeDCtx(dctx: *mut ZSTD_DCtx) -> usize;
    pub(crate) unsafe fn ZSTD_decompressDCtx(
        dctx: *mut ZSTD_DCtx,
        dst: *mut c_void,
        dst_capacity: usize,
        src: *const c_void,
        src_size: usize,
    ) -> usize;
    pub(crate) unsafe fn ZSTD_DCtx_reset(dctx: *mut ZSTD_DCtx, reset: i32) -> usize;
    pub(crate) unsafe fn ZSTD_compressBound(src_size: usize) -> usize;
    pub(crate) unsafe fn ZSTD_isError(result: usize) -> u32;
    pub(crate) unsafe fn ZSTD_getErrorName(result: usize) -> *const c_char;
    pub(crate) unsafe fn ZSTD_maxCLevel() -> i32;
}

fn error_name(code: usize) -> String {
    let name = unsafe { ZSTD_getErrorName(code) };
    if name.is_null() {
        return format!("zstd error {code}");
    }
    unsafe { CStr::from_ptr(name) }
        .to_string_lossy()
        .into_owned()
}

fn check(code: usize) -> Result<usize, String> {
    if unsafe { ZSTD_isError(code) } != 0 {
        return Err(error_name(code));
    }
    Ok(code)
}

pub fn compress_bound(src_size: usize) -> Result<usize, String> {
    // `ZSTD_compressBound` itself can fail for huge inputs.
    check(unsafe { ZSTD_compressBound(src_size) })
}

pub fn max_c_level() -> i32 {
    unsafe { ZSTD_maxCLevel() }
}

/// Compression context. `Send` (not `Sync`): the encoder pool moves a
/// context to a worker thread and uses it mutably from that thread only.
pub struct Cctx {
    raw: *mut ZSTD_CCtx,
}

// SAFETY: used through `&mut self` from one thread at a time.
unsafe impl Send for Cctx {}

impl Cctx {
    pub fn create() -> Option<Self> {
        let raw = unsafe { ZSTD_createCCtx() };
        if raw.is_null() {
            None
        } else {
            Some(Self { raw })
        }
    }

    pub fn compress(&mut self, dst: &mut [u8], src: &[u8], level: i32) -> Result<usize, String> {
        check(unsafe {
            ZSTD_compressCCtx(
                self.raw,
                dst.as_mut_ptr() as *mut c_void,
                dst.len(),
                src.as_ptr() as *const c_void,
                src.len(),
                level,
            )
        })
    }
}

impl Drop for Cctx {
    fn drop(&mut self) {
        unsafe { ZSTD_freeCCtx(self.raw) };
    }
}

/// Decompression context. `Send` (not `Sync`): same single-owner use.
pub struct Dctx {
    raw: *mut ZSTD_DCtx,
}

// SAFETY: used through `&mut self` from one thread at a time.
unsafe impl Send for Dctx {}

impl Dctx {
    pub fn create() -> Option<Self> {
        let raw = unsafe { ZSTD_createDCtx() };
        if raw.is_null() {
            None
        } else {
            Some(Self { raw })
        }
    }

    pub fn reset_session(&mut self) -> Result<(), String> {
        check(unsafe { ZSTD_DCtx_reset(self.raw, RESET_SESSION_ONLY) }).map(|_| ())
    }

    pub fn decompress(&mut self, dst: &mut [u8], src: &[u8]) -> Result<usize, String> {
        check(unsafe {
            ZSTD_decompressDCtx(
                self.raw,
                dst.as_mut_ptr() as *mut c_void,
                dst.len(),
                src.as_ptr() as *const c_void,
                src.len(),
            )
        })
    }
}

impl Drop for Dctx {
    fn drop(&mut self) {
        unsafe { ZSTD_freeDCtx(self.raw) };
    }
}

#[repr(C)]
struct InputBuffer {
    src: *const c_void,
    size: usize,
    pos: usize,
}

#[repr(C)]
struct OutputBuffer {
    dst: *mut c_void,
    size: usize,
    pos: usize,
}

unsafe extern "C" {
    fn ZSTD_decompressStream(
        ctx: *mut ZSTD_DCtx,
        output: *mut OutputBuffer,
        input: *mut InputBuffer,
    ) -> usize;
}

fn io_error(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into())
}

pub(crate) fn compress(src: &[u8], level: i32) -> std::io::Result<Vec<u8>> {
    let mut ctx = Cctx::create().ok_or_else(|| io_error("zstd context allocation failed"))?;
    let mut output = vec![0; compress_bound(src.len()).map_err(io_error)?];
    let size = ctx.compress(&mut output, src, level).map_err(io_error)?;
    output.truncate(size);
    Ok(output)
}

/// Decode concatenated frames, returning at most `limit` bytes (like Read::take).
/// Callers request their maximum plus one to detect an oversized payload. No
/// allocation uses the untrusted frame content size, and incomplete frames fail.
pub(crate) fn decompress_to_limit(src: &[u8], limit: usize) -> std::io::Result<Vec<u8>> {
    let ctx = Dctx::create().ok_or_else(|| io_error("zstd context allocation failed"))?;
    let mut input = InputBuffer {
        src: src.as_ptr().cast(),
        size: src.len(),
        pos: 0,
    };
    let mut plain = Vec::new();
    let mut buffer = [0u8; 16 << 10];
    while plain.len() < limit {
        let capacity = buffer.len().min(limit - plain.len());
        let mut output = OutputBuffer {
            dst: buffer.as_mut_ptr().cast(),
            size: capacity,
            pos: 0,
        };
        let previous = input.pos;
        // SAFETY: both C buffers describe live slices for this synchronous call;
        // the context is owned here and is never shared during decompression.
        let remaining = check(unsafe { ZSTD_decompressStream(ctx.raw, &mut output, &mut input) })
            .map_err(io_error)?;
        plain.extend_from_slice(&buffer[..output.pos]);
        if remaining == 0 && input.pos == input.size {
            return Ok(plain);
        }
        if previous == input.pos && output.pos == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "incomplete zstd frame",
            ));
        }
    }
    Ok(plain)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn original_stream_frames_and_bounds() {
        let old = include_bytes!("fixtures/zstd.bin");
        let plain = b"sqlite-murmur dependency compatibility";
        assert_eq!(decompress_to_limit(old, plain.len() + 1).unwrap(), plain);
        let new = compress(plain, 3).unwrap();
        let mut joined = old.to_vec();
        joined.extend(new);
        assert_eq!(decompress_to_limit(&joined, 1000).unwrap(), plain.repeat(2));
        assert_eq!(decompress_to_limit(&joined, 5).unwrap(), &plain[..5]);
        assert_eq!(
            decompress_to_limit(&joined, plain.len() * 2).unwrap(),
            plain.repeat(2)
        );
        for length in 1..old.len() {
            assert!(decompress_to_limit(&old[..length], 1000).is_err());
        }
        joined.push(0);
        assert!(decompress_to_limit(&joined, 1000).is_err());
        assert!(decompress_to_limit(b"corrupted frame", 1000).is_err());
        assert_eq!(
            decompress_to_limit(&compress(&[], 3).unwrap(), 1).unwrap(),
            []
        );
        let bomb = compress(&vec![0; 1 << 20], 3).unwrap();
        assert_eq!(decompress_to_limit(&bomb, 101).unwrap().len(), 101);
    }

    #[test]
    fn round_trip() {
        let mut cctx = Cctx::create().expect("cctx");
        let mut dctx = Dctx::create().expect("dctx");
        let src = vec![0xabu8; 4096];
        let mut dst = vec![0u8; compress_bound(src.len()).expect("bound")];
        let n = cctx.compress(&mut dst, &src, 3).expect("compress");
        assert!(n < src.len());
        let frame = dst[..n].to_vec();
        dctx.reset_session().expect("reset");
        let mut out = vec![0u8; 4096];
        let m = dctx.decompress(&mut out, &frame).expect("decompress");
        assert_eq!(m, 4096);
        assert_eq!(out, src);
        assert!(max_c_level() >= 3);
    }
}
