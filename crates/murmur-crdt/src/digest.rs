//! SHA-256 for managed persistence, logical keys and stable schema identities.
use sha2::{Digest, Sha256};

pub fn hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(text, "{byte:02x}").unwrap();
    }
    text
}

pub fn hash_hex(bytes: &[u8]) -> String {
    hex(&hash(bytes))
}
