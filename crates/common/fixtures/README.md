# Dependency compatibility fixtures

Captured before migration using the repository's original pinned `aes-gcm`
0.10.3, `hkdf` 0.12.4, `sha2` 0.10.9, `postcard` 1.1.3, and `zstd` 0.13.3.
All values are test data, never production keys.

Plaintext: `sqlite-murmur dependency compatibility`.

- `hkdf.bin`: 32-byte HKDF-SHA256 output; IKM `[1; 32]`, salt `[2; 16]`,
  info `murmur/sqlite-file-object/v1`.
- `aes.bin`: AES-256-GCM ciphertext followed by the 16-byte tag; key
  `hkdf.bin`, nonce `[4; 12]`, AAD `compatibility AAD`.
- `sha256.bin`: SHA-256 of the plaintext.
- `object.spfo`: complete generation-zero SPFO0001 object; master key
  `[1; 32]`, salt `[2; 16]`, nonce prefix `[3; 8]`, one plaintext frame
  and authenticated Postcard `(length, digest)` trailer.
- `zstd.bin`: original streaming encoder's level-3 frame of the plaintext.

The AES test requires identical ciphertext, not just a round trip. The SPFO
test reads the original container through the application's current decoder.

## Round 2 UUID compatibility

`uuid-v5.txt` and `managed-uuids.txt` were captured using the original `uuid`
1.28.0 implementation before replacing it. The former covers empty, text, NUL,
and arbitrary binary names in the URL namespace. The latter covers the actual
managed-primary-key encoding: integer/real equality, NOCASE and RTRIM text,
Unicode/NUL text, and binary values. Tests read these files without regenerating
expected values.

## CIDR compatibility

`ipnet.json` was captured using the full upstream `ipnet` 2.12.2 before replacing
it with the internal subset. It records all IPv4/IPv6 prefix lengths, host bits,
IPv4-mapped IPv6 addresses, legacy leading-zero spellings, invalid inputs,
formatting, JSON, Postcard encoding, and containment against boundary addresses.
The regression test compares the local implementation to these original outputs.
