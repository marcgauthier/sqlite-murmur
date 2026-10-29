//! Safe wrappers over the vendored libzstd C API
//! (`third-party/zstd/lib/zstd.h`). Pages compress before encryption;
//! contexts are pooled per encoder thread, exactly like the C++
//! `EncodeWorkspace` / `DecodeWorkspace`.

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
    unsafe { CStr::from_ptr(name) }.to_string_lossy().into_owned()
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
        if raw.is_null() { None } else { Some(Self { raw }) }
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
        if raw.is_null() { None } else { Some(Self { raw }) }
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

#[cfg(test)]
mod tests {
    use super::*;

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
