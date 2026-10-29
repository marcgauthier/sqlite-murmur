//! Raw declarations over the vendored libaegis C API
//! (`third-party/aegis/src/include/aegis256.h`). Compiled only with the
//! `aegis256` cargo feature (mirrors `SQLITE_MURMUR_ENABLE_AEGIS256`).
//! Safe page-crypto wrappers live in `crypto`.

use std::ffi::c_uchar;

pub const NPUBBYTES: usize = 32;
pub const ABYTES_MIN: usize = 16;
pub const ABYTES_MAX: usize = 32;

unsafe extern "C" {
    pub(crate) unsafe fn aegis_init() -> i32;
    pub(crate) unsafe fn aegis256_encrypt(
        c: *mut c_uchar,
        maclen: usize,
        m: *const c_uchar,
        mlen: usize,
        ad: *const c_uchar,
        adlen: usize,
        npub: *const c_uchar,
        k: *const c_uchar,
    ) -> i32;
    pub(crate) unsafe fn aegis256_decrypt(
        m: *mut c_uchar,
        c: *const c_uchar,
        clen: usize,
        maclen: usize,
        ad: *const c_uchar,
        adlen: usize,
        npub: *const c_uchar,
        k: *const c_uchar,
    ) -> i32;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        assert!(unsafe { aegis_init() } >= 0);
        let key = [0x11u8; 32];
        let nonce = [0x22u8; NPUBBYTES];
        let ad = [0x33u8; 13];
        let pt = vec![0x44u8; 100];
        let mut ct = vec![0u8; 100 + ABYTES_MAX];
        let rc = unsafe {
            aegis256_encrypt(
                ct.as_mut_ptr(),
                ABYTES_MAX,
                pt.as_ptr(),
                pt.len(),
                ad.as_ptr(),
                ad.len(),
                nonce.as_ptr(),
                key.as_ptr(),
            )
        };
        assert_eq!(rc, 0);
        let mut out = vec![0u8; 100];
        let rc = unsafe {
            aegis256_decrypt(
                out.as_mut_ptr(),
                ct.as_ptr(),
                ct.len(),
                ABYTES_MAX,
                ad.as_ptr(),
                ad.len(),
                nonce.as_ptr(),
                key.as_ptr(),
            )
        };
        assert_eq!(rc, 0);
        assert_eq!(out, pt);
        // Tamper: decrypt must fail.
        ct[50] ^= 1;
        let rc = unsafe {
            aegis256_decrypt(
                out.as_mut_ptr(),
                ct.as_ptr(),
                ct.len(),
                ABYTES_MAX,
                ad.as_ptr(),
                ad.len(),
                nonce.as_ptr(),
                key.as_ptr(),
            )
        };
        assert_ne!(rc, 0);
    }
}
