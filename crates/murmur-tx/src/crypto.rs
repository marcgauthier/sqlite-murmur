//! AWS-LC adapters shared by origin signing, snapshots and High/Low bundles.
use crate::{Error, Result};
use aws_lc_rs::{
    agreement,
    signature::{self, Ed25519KeyPair, KeyPair},
};

fn signing_key(seed: &[u8; 32]) -> Ed25519KeyPair {
    // Every 32-byte Ed25519 seed is valid; callers supply a fixed-size seed.
    Ed25519KeyPair::from_seed_unchecked(seed).expect("32-byte Ed25519 seed")
}
pub(crate) fn public_key(seed: &[u8; 32]) -> [u8; 32] {
    signing_key(seed)
        .public_key()
        .as_ref()
        .try_into()
        .expect("Ed25519 public key length")
}
pub(crate) fn sign(seed: &[u8; 32], bytes: &[u8]) -> Result<Vec<u8>> {
    signing_key(seed)
        .try_sign(bytes)
        .map(|s| s.as_ref().to_vec())
        .map_err(|_| Error::Auth("Ed25519 signing failed".into()))
}
pub(crate) fn verify(public: &[u8; 32], bytes: &[u8], signature: &[u8]) -> Result<()> {
    // Preserve the previous strict verifier's rejection of low-order public
    // keys and signature R points. AWS-LC verifies the Ed25519 equation but
    // accepts identity-point forgeries for a weak configured public key.
    if signature.len() != 64 || !strict_point(public) || !strict_point(&signature[..32]) {
        return Err(Error::Auth(
            "invalid Ed25519 point or signature length".into(),
        ));
    }
    signature::UnparsedPublicKey::new(&signature::ED25519, public)
        .verify(bytes, signature)
        .map_err(|_| Error::Auth("invalid Ed25519 signature".into()))
}
// Encoded-point input policy only; all group arithmetic and signature
// verification remain in AWS-LC. The sign bit distinguishes +/-x, so masking
// it leaves five y coordinates for the eight low-order Edwards25519 points.
// See RFC 8032 section 5.1.3 and the dalek EIGHT_TORSION test vectors.
fn strict_point(encoded: &[u8]) -> bool {
    let mut y: [u8; 32] = encoded.try_into().expect("32-byte encoded point");
    y[31] &= 0x7f;
    let mut prime = [0xff; 32];
    prime[0] = 0xed;
    prime[31] = 0x7f;
    if y.iter().rev().cmp(prime.iter().rev()) != std::cmp::Ordering::Less {
        return false;
    }
    const LOW_ORDER_Y: [[u8; 32]; 5] = [
        [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
        ],
        [
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
        ],
        [
            0x26, 0xe8, 0x95, 0x8f, 0xc2, 0xb2, 0x27, 0xb0, 0x45, 0xc3, 0xf4, 0x89, 0xf2, 0xef,
            0x98, 0xf0, 0xd5, 0xdf, 0xac, 0x05, 0xd3, 0xc6, 0x33, 0x39, 0xb1, 0x38, 0x02, 0x88,
            0x6d, 0x53, 0xfc, 0x05,
        ],
        [
            0xc7, 0x17, 0x6a, 0x70, 0x3d, 0x4d, 0xd8, 0x4f, 0xba, 0x3c, 0x0b, 0x76, 0x0d, 0x10,
            0x67, 0x0f, 0x2a, 0x20, 0x53, 0xfa, 0x2c, 0x39, 0xcc, 0xc6, 0x4e, 0xc7, 0xfd, 0x77,
            0x92, 0xac, 0x03, 0x7a,
        ],
        [
            0xec, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0x7f,
        ],
    ];
    !LOW_ORDER_Y.contains(&y)
}

fn private_key(seed: &[u8; 32]) -> agreement::PrivateKey {
    agreement::PrivateKey::from_private_key(&agreement::X25519, seed)
        .expect("32-byte X25519 private key")
}
pub(crate) fn recipient_public(seed: &[u8; 32]) -> [u8; 32] {
    private_key(seed)
        .compute_public_key()
        .expect("X25519 public key")
        .as_ref()
        .try_into()
        .expect("X25519 public key length")
}
pub(crate) fn agree(seed: &[u8; 32], peer: &[u8; 32]) -> Result<zeroize::Zeroizing<[u8; 32]>> {
    let peer = agreement::UnparsedPublicKey::new(&agreement::X25519, peer);
    // AWS-LC rejects all-zero shared secrets, including low-order peer keys.
    agreement::agree(
        &private_key(seed),
        &peer,
        Error::Auth("invalid X25519 peer key".into()),
        |secret| {
            Ok(zeroize::Zeroizing::new(
                secret.try_into().expect("X25519 secret length"),
            ))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    fn bytes<const N: usize>(hex: &str) -> [u8; N] {
        assert_eq!(hex.len(), 2 * N);
        std::array::from_fn(|i| u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).unwrap())
    }
    #[test]
    fn rfc8032_ed25519_seed_signature_and_rejections() {
        let seed = bytes("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60");
        let public = bytes("d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a");
        let signature: [u8; 64] = bytes(concat!(
            "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e06522490155",
            "5fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b"
        ));
        assert_eq!(public_key(&seed), public);
        assert_eq!(sign(&seed, b"").unwrap(), signature);
        verify(&public, b"", &signature).unwrap();
        assert!(verify(&public, b"tampered", &signature).is_err());
        assert!(verify(&public, b"", &signature[..63]).is_err());
        let mut noncanonical = signature;
        noncanonical[63] |= 0x80;
        assert!(verify(&public, b"", &noncanonical).is_err());
        let mut identity = [0; 32];
        identity[0] = 1;
        let mut forged = [0; 64];
        forged[0] = 1;
        assert!(verify(&identity, b"forged", &forged).is_err());
        assert!(verify(&[0; 32], b"forged", &forged).is_err());
    }
    #[test]
    fn low_order_and_noncanonical_edwards_encodings_are_rejected() {
        for hex in [
            "0000000000000000000000000000000000000000000000000000000000000000",
            "0100000000000000000000000000000000000000000000000000000000000000",
            "26e8958fc2b227b045c3f489f2ef98f0d5dfac05d3c63339b13802886d53fc05",
            "c7176a703d4dd84fba3c0b760d10670f2a2053fa2c39ccc64ec7fd7792ac037a",
            "ecffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
            "edffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
            "eeffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
        ] {
            let mut point: [u8; 32] = bytes(hex);
            for _ in 0..2 {
                assert!(!strict_point(&point));
                let mut signature = [0; 64];
                signature[..32].copy_from_slice(&point);
                assert!(verify(&public_key(&[7; 32]), b"message", &signature).is_err());
                assert!(verify(&point, b"message", &signature).is_err());
                point[31] ^= 0x80;
            }
        }
    }
    #[test]
    fn rfc7748_x25519_existing_keys_and_low_order_rejection() {
        let alice = bytes("77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a");
        let bob = bytes("5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb");
        let ap = bytes("8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a");
        let bp = bytes("de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f");
        let expected = bytes("4a5d9d5ba4ce2de1728e3bf480350f25e07e21c947d19e3376f09b3c1e161742");
        assert_eq!(recipient_public(&alice), ap);
        assert_eq!(recipient_public(&bob), bp);
        assert_eq!(*agree(&alice, &bp).unwrap(), expected);
        assert_eq!(*agree(&bob, &ap).unwrap(), expected);
        assert!(agree(&alice, &[0; 32]).is_err());
        let mut low_order = [0; 32];
        low_order[0] = 1;
        assert!(agree(&alice, &low_order).is_err());
    }
}
