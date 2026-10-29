//! Internal AWS-LC adapters. Included as source in each consuming crate so
//! independently linked Rust static archives retain their own module boundaries.
#![allow(dead_code)]

use aws_lc_rs::{aead, error::Unspecified, hkdf};
use zeroize::Zeroizing;

#[path = "sha256.rs"]
mod hash;
#[allow(unused_imports)] // Storage currently uses only AES-GCM and HKDF.
pub(crate) use hash::Sha256;

pub(crate) fn hkdf_sha256(
    ikm: &[u8],
    salt: &[u8],
    info: &[u8],
    output: &mut [u8],
) -> Result<(), Unspecified> {
    struct Length(usize);
    impl hkdf::KeyType for Length {
        fn len(&self) -> usize {
            self.0
        }
    }
    let prk = hkdf::Salt::new(hkdf::HKDF_SHA256, salt).extract(ikm);
    prk.expand(&[info], Length(output.len()))?.fill(output)
}

pub(crate) struct Payload<'a> {
    pub(crate) msg: &'a [u8],
    pub(crate) aad: &'a [u8],
}

pub(crate) struct Aes256Gcm(aead::LessSafeKey);

impl Aes256Gcm {
    pub(crate) fn new_from_slice(key: &[u8]) -> Result<Self, Unspecified> {
        Ok(Self(aead::LessSafeKey::new(aead::UnboundKey::new(
            &aead::AES_256_GCM,
            key,
        )?)))
    }

    pub(crate) fn encrypt(
        &self,
        nonce: &[u8],
        payload: Payload<'_>,
    ) -> Result<Vec<u8>, Unspecified> {
        // Wipe the scratch buffer on failure, including any copied plaintext.
        let mut buffer = Zeroizing::new(payload.msg.to_vec());
        self.0.seal_in_place_append_tag(
            aead::Nonce::try_assume_unique_for_key(nonce)?,
            aead::Aad::from(payload.aad),
            &mut *buffer,
        )?;
        Ok(std::mem::take(&mut *buffer))
    }

    pub(crate) fn decrypt(
        &self,
        nonce: &[u8],
        payload: Payload<'_>,
    ) -> Result<Vec<u8>, Unspecified> {
        // AWS-LC may modify the buffer before authentication fails. Never return
        // it on failure, and wipe the entire scratch buffer when dropping it.
        let mut buffer = Zeroizing::new(payload.msg.to_vec());
        let length = self
            .0
            .open_in_place(
                aead::Nonce::try_assume_unique_for_key(nonce)?,
                aead::Aad::from(payload.aad),
                &mut buffer,
            )?
            .len();
        buffer.truncate(length);
        Ok(std::mem::take(&mut *buffer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const PLAIN: &[u8] = b"sqlite-murmur dependency compatibility";

    #[test]
    fn original_provider_bytes_and_authentication() {
        let mut key = [0; 32];
        hkdf_sha256(
            &[1; 32],
            &[2; 16],
            b"murmur/sqlite-file-object/v1",
            &mut key,
        )
        .unwrap();
        assert_eq!(&key, include_bytes!("fixtures/hkdf.bin"));
        let cipher = Aes256Gcm::new_from_slice(&key).unwrap();
        let sealed = include_bytes!("fixtures/aes.bin");
        assert_eq!(
            cipher
                .encrypt(
                    &[4; 12],
                    Payload {
                        msg: PLAIN,
                        aad: b"compatibility AAD"
                    }
                )
                .unwrap(),
            sealed
        );
        assert_eq!(
            cipher
                .decrypt(
                    &[4; 12],
                    Payload {
                        msg: sealed,
                        aad: b"compatibility AAD"
                    }
                )
                .unwrap(),
            PLAIN
        );
        for length in 0..sealed.len() {
            assert!(
                cipher
                    .decrypt(
                        &[4; 12],
                        Payload {
                            msg: &sealed[..length],
                            aad: b"compatibility AAD"
                        }
                    )
                    .is_err()
            );
        }
        let mut altered = *sealed;
        altered[0] ^= 1;
        assert!(
            cipher
                .decrypt(
                    &[4; 12],
                    Payload {
                        msg: &altered,
                        aad: b"compatibility AAD"
                    }
                )
                .is_err()
        );
        assert!(
            cipher
                .decrypt(
                    &[4; 12],
                    Payload {
                        msg: sealed,
                        aad: b"wrong AAD"
                    }
                )
                .is_err()
        );
        assert!(
            cipher
                .decrypt(
                    &[5; 12],
                    Payload {
                        msg: sealed,
                        aad: b"compatibility AAD"
                    }
                )
                .is_err()
        );
        let wrong = Aes256Gcm::new_from_slice(&[9; 32]).unwrap();
        assert!(
            wrong
                .decrypt(
                    &[4; 12],
                    Payload {
                        msg: sealed,
                        aad: b"compatibility AAD"
                    }
                )
                .is_err()
        );
        assert_eq!(
            &Sha256::digest(PLAIN),
            include_bytes!("fixtures/sha256.bin")
        );
        let mut hash = Sha256::new();
        for chunk in PLAIN.chunks(3) {
            hash.update(chunk);
        }
        assert_eq!(hash.clone().finalize(), Sha256::digest(PLAIN));
        assert_eq!(hash.finalize(), Sha256::digest(PLAIN));
    }
}
