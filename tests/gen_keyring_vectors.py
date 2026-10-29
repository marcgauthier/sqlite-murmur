#!/usr/bin/env python3
"""Independent keys.enc v3 fixture generator for WP-D D2a (sqlite-murmur).

Implements the archived envelope byte spec (MURMUR-RUST spool/keyring.rs
doc comment + crypto.rs) from scratch in Python: CRC32C, HKDF-SHA256,
AES-256-GCM via `cryptography`, optional Argon2id via PyNaCl/libsodium.
Output is C++ byte arrays for tests/keyring_vectors.h. Deterministic:
all salts/nonces/keys are fixed documented seeds (NOT random) so vectors
are stable. Self-checks anchor CRC32C and HKDF to published vectors
(CRC32C("123456789"), RFC 5869 A.1) on every run.
Regen: python3 tests/gen_keyring_vectors.py
"""
import hashlib
import hmac
import sys

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

try:
    import nacl.bindings as _nacl

    HAVE_ARGON2ID = hasattr(_nacl, "crypto_pwhash_argon2id")
except Exception:
    HAVE_ARGON2ID = False


def crc32c(data: bytes) -> int:
    poly = 0x82F63B78  # reflected Castagnoli (normal form 0x1EDC6F41)
    table = []
    for i in range(256):
        crc = i
        for _ in range(8):
            crc = (crc >> 1) ^ poly if crc & 1 else crc >> 1
        table.append(crc)
    crc = 0xFFFFFFFF
    for b in data:
        crc = table[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


def hkdf_sha256(ikm: bytes, salt: bytes, info: bytes) -> bytes:
    prk = hmac.new(salt, ikm, hashlib.sha256).digest()
    okm = b""
    t = b""
    for i in range(1, 3):
        t = hmac.new(prk, t + info + bytes([i]), hashlib.sha256).digest()
        okm += t
    return okm[:32]


def protector(kdf, master, passphrase, salt, t, m, p, store, context):
    info = b"spool/keys-enc/v3/" + store + context
    if kdf == 0:
        assert master
        return hkdf_sha256(master, salt, info)
    assert passphrase and t and m and p
    stretched = _nacl.crypto_pwhash_argon2id(
        32, passphrase, salt, t, m * 1024, p
    )
    return hkdf_sha256(stretched, salt, info)


def build(kdf, seq, salt, a_time, a_mem, a_threads, wrap_hint, store,
          context, master, passphrase, keys):
    """keys: list of (id, created_i64, status, key32). Returns envelope."""
    body = len(keys).to_bytes(4, "little") + context
    body += bytes([len(wrap_hint)]) + wrap_hint
    for kid, created, status, k in keys:
        body += kid.to_bytes(4, "little")
        body += (created & 0xFFFFFFFFFFFFFFFF).to_bytes(8, "little")
        body += bytes([status]) + k
    hdr = b"SPLK" + (3).to_bytes(2, "little") + bytes([kdf])
    hdr += seq.to_bytes(8, "little") + salt
    hdr += a_time.to_bytes(4, "little") + a_mem.to_bytes(4, "little")
    hdr += bytes([a_threads, len(wrap_hint)]) + wrap_hint
    hdr += crc32c(hdr).to_bytes(4, "little")
    prot = protector(kdf, master, passphrase, salt, a_time, a_mem, a_threads,
                     store, context)
    # Fixed nonce per vector (documented below); production uses RAND_bytes.
    nonce = NONCE
    sealed = AESGCM(prot).encrypt(nonce, body, hdr + store)
    return hdr + nonce + len(sealed).to_bytes(4, "little") + sealed


def emit(name, blob, note):
    print(f"// {name}: {note} ({len(blob)} bytes)")
    print(f"static const unsigned char {name}[] = {{")
    for i in range(0, len(blob), 12):
        chunk = ", ".join(f"0x{b:02x}" for b in blob[i:i + 12])
        print(f"    {chunk},")
    print("};")
    print(f"static const std::size_t {name}_LEN = sizeof({name});")
    print()


# Anchor the independent implementation to published vectors (proves the
# Python side without trusting either this file or the C++ port).
assert crc32c(b"123456789") == 0xE3069283, "crc32c mismatch"
_rfc_ikm = bytes([0x0B]) * 22
_rfc_okm = hkdf_sha256(_rfc_ikm, bytes([0x00, 0x01, 0x02, 0x03, 0x04, 0x05,
                                        0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B,
                                        0x0C]),
                       bytes([0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7,
                              0xF8, 0xF9]))
assert _rfc_okm.hex() == ("3cb25f25faacd57a90434f64d0362f2a"
                             "2d2d0a90cf1a5a4c5db02d56ecc4c5bf"), "hkdf mismatch"
print("// self-checks: CRC32C(\"123456789\")=0xE3069283, RFC5869-A.1 L32 ok",
      file=sys.stderr)

STORE = bytes(range(16))
CONTEXT = bytes(0x80 + i for i in range(16))
MASTER = bytes(0x40 + i for i in range(32))
NONCE = bytes(0xC0 + i for i in range(12))
SALT = bytes(0x20 + i for i in range(32))
KEY1 = bytes(1 + i for i in range(32))
KEY2 = bytes(0xE0 + i for i in range(32))

v1 = build(0, 9, SALT, 0, 0, 0, b"wrap-hint", STORE, CONTEXT, MASTER, b"",
           [(1, 1700000000000000000, 0, KEY1)])
emit("VEC_DIRECT_ONE", v1, "KDF direct, seq 9, 1 active key id=1")

v2 = build(0, 41, bytes(0x60 + i for i in range(32)), 0, 0, 0, b"",
           STORE, CONTEXT, MASTER, b"",
           [(1, 1700000000000000000, 1, KEY1),
            (2, 1700000001000000000, 0, KEY2)])
emit("VEC_DIRECT_TWO", v2, "KDF direct, seq 41, key1 retired + key2 active")

if HAVE_ARGON2ID:
    va = build(1, 3, bytes(0x30 + i for i in range(32))[:16] +
               bytes(0x30 + i for i in range(16)), 3, 64 * 1024, 4,
               b"argon", STORE, CONTEXT, b"", b"correct horse passphrase",
               [(1, 1700000002000000000, 0, KEY1)])
    emit("VEC_ARGON2_REAL", va, "KDF argon2id t=3 m=64MiB p=4 REAL vector")
    print("// HAVE_ARGON2ID=1 (libsodium-derived; C++ -DUNIT later)", file=sys.stderr)
else:
    # Valid header + CRC, garbage sealed body: proves fail-closed KDF
    # ordering (unsupported-KDF error before any decrypt attempt).
    hdr = (b"SPLK" + (3).to_bytes(2, "little") + bytes([1]) +
           (3).to_bytes(8, "little") + SALT + (3).to_bytes(4, "little") +
           (64 * 1024).to_bytes(4, "little") + bytes([4, 5]) + b"argon")
    hdr += crc32c(hdr).to_bytes(4, "little")
    garbage = hdr + NONCE + (48).to_bytes(4, "little") + bytes(48)
    emit("VEC_ARGON2_HDR", garbage, "KDF argon2id header+CRC valid, body garbage")
    print("// HAVE_ARGON2ID=0 (no local argon2id; fail-closed vector only)", file=sys.stderr)

# Zero-cost argon2id header (valid CRC, garbage body): proves the param
# gate (threads/time/memory must be nonzero) returns ERROR rather than
# attempting derivation. Untestable by hand-tamper (any param edit breaks
# the header CRC first), hence generator-made.
_zhdr = (b"SPLK" + (3).to_bytes(2, "little") + bytes([1]) +
         (5).to_bytes(8, "little") + SALT + (0).to_bytes(4, "little") +
         (0).to_bytes(4, "little") + bytes([0, 0]))
_zhdr += crc32c(_zhdr).to_bytes(4, "little")
_zgarbage = _zhdr + NONCE + (48).to_bytes(4, "little") + bytes(48)
emit("VEC_ARGON2_ZEROPARAMS", _zgarbage,
     "KDF argon2id zero time/mem/threads, header+CRC valid, body garbage")

print(f"// STORE={STORE.hex()} CONTEXT={CONTEXT.hex()} MASTER={MASTER.hex()}")
print(f"// KEY1={KEY1.hex()}")
print(f"// KEY2={KEY2.hex()}")
