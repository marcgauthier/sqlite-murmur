//! The application's existing UUID-v5 URL namespace, implemented with AWS-LC.
//! SHA-1 is required for UUID-v5 compatibility, not used as a security digest.
use aws_lc_rs::digest;

pub(crate) fn v5_url(name: &[u8]) -> String {
    const NAMESPACE_URL: [u8; 16] = [
        0x6b, 0xa7, 0xb8, 0x11, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30,
        0xc8,
    ];
    let mut hash = digest::Context::new(&digest::SHA1_FOR_LEGACY_USE_ONLY);
    hash.update(&NAMESPACE_URL);
    hash.update(name);
    let digest = hash.finish();
    let mut bytes: [u8; 16] = digest.as_ref()[..16].try_into().unwrap();
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let mut out = String::with_capacity(36);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for (i, byte) in bytes.into_iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 15) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_original_uuid_crate_for_text_and_binary_inputs() {
        let names: &[&[u8]] = &[
            b"",
            b"www.widgets.com",
            b"sqlite-murmur",
            b"a\0b",
            &[0, 255, 128],
        ];
        let original: Vec<_> = include_str!("fixtures/uuid-v5.txt").lines().collect();
        assert_eq!(names.len(), original.len());
        for (name, expected) in names.iter().zip(original) {
            let actual = v5_url(name);
            assert_eq!(actual, expected);
            assert_eq!(&actual[14..15], "5");
            assert!(matches!(actual.as_bytes()[19], b'8' | b'9' | b'a' | b'b'));
        }
    }
}
