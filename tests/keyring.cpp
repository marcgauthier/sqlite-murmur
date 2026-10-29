// WP-D slice D2a tests: keys.enc v3 envelope. Decode agreement against
// independent Python-generated fixtures (tests/keyring_vectors.h) plus
// round-trip, rotation-ordering, and tamper matrices. Standalone: this
// file + src/sqlite_murmur_keyring.cpp + OpenSSL only.
// Lead merge: errors-trio for tests/keyring.cpp (granted L).

#include "sqlite_murmur_keyring.h"

#include "keyring_vectors.h"

#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

void require(bool ok, const std::string &msg) {
    if (!ok) throw std::runtime_error("keyring test: " + msg);
}

void fill(unsigned char *out, unsigned base) {
    for (int i = 0; i < 32; ++i)
        out[i] = static_cast<unsigned char>(base + i);
}

bool bytes_eq(const unsigned char *a, const unsigned char *b, std::size_t n) {
    return std::memcmp(a, b, n) == 0;
}

} // namespace

int main() {
    // Fixture identities (must match the generator seed comment).
    unsigned char store[16], context[16], master[32], key1[32], key2[32];
    for (int i = 0; i < 16; ++i) {
        store[i] = static_cast<unsigned char>(i);
        context[i] = static_cast<unsigned char>(0x80 + i);
    }
    fill(master, 0x40);
    fill(key1, 0x01);
    fill(key2, 0xe0);
    try {
        // 1. Independent decode agreement: Python-generated envelopes open
        // here with exact fields (proves byte-spec compatibility of the
        // header/CRC/AAD/HKDF/AES-GCM chain across implementations).
        {
            sqlite_murmur_keyring ring;
            require(sqlite_murmur_keyring_open(
                        VEC_DIRECT_ONE, VEC_DIRECT_ONE_LEN, master, nullptr,
                        store, context, ring) == SQLITE_OK,
                    "VEC_DIRECT_ONE failed to open");
            require(ring.seq == 9, "VEC1 seq wrong");
            require(ring.kdf == SQLITE_MURMUR_KDF_DIRECT_HKDF,
                    "VEC1 kdf wrong");
            require(ring.wrap_id == "wrap-hint", "VEC1 wrap wrong");
            require(ring.keys.size() == 1, "VEC1 key count wrong");
            require(ring.current == 1, "VEC1 current wrong");
            require(ring.keys[0].active, "VEC1 key inactive");
            require(ring.keys[0].created_nanos == 1700000000000000000ll,
                    "VEC1 created wrong");
            require(bytes_eq(ring.keys[0].key, key1, 32), "VEC1 key wrong");
            std::uint32_t id = 0;
            const unsigned char *cur =
                sqlite_murmur_keyring_current(ring, id);
            require(cur && id == 1 && bytes_eq(cur, key1, 32),
                    "VEC1 current lookup wrong");
            require(!sqlite_murmur_keyring_by_id(ring, 7),
                    "phantom key id found");
            sqlite_murmur_keyring_destroy(ring);
            require(ring.keys.empty(), "destroy left keys");
        }
        {
            sqlite_murmur_keyring ring;
            require(sqlite_murmur_keyring_open(
                        VEC_DIRECT_TWO, VEC_DIRECT_TWO_LEN, master, nullptr,
                        store, context, ring) == SQLITE_OK,
                    "VEC_DIRECT_TWO failed to open");
            require(ring.seq == 41, "VEC2 seq wrong");
            require(ring.keys.size() == 2, "VEC2 key count wrong");
            require(!ring.keys[0].active && ring.keys[0].id == 1,
                    "VEC2 key1 should be retired");
            require(ring.keys[1].active && ring.current == 2,
                    "VEC2 key2 should be current");
            require(bytes_eq(ring.keys[0].key, key1, 32), "VEC2 key1 wrong");
            require(bytes_eq(ring.keys[1].key, key2, 32), "VEC2 key2 wrong");
            sqlite_murmur_keyring_destroy(ring);
        }

        // 2. Authentication and binding failures fail closed with the
        // right codes: wrong key -> AUTH; wrong store id -> AUTH (KDF
        // info/AAD mismatch fails the GCM open); wrong context -> non-OK
        // (AUTH via the same path; the body context check is unreachable
        // after a protector mismatch). Lead merge-time comment fix: the
        // previous text claimed CORRUPT here, contradicting the asserts.
        {
            sqlite_murmur_keyring ring;
            unsigned char wrong[32];
            fill(wrong, 0x41);
            require(sqlite_murmur_keyring_open(
                        VEC_DIRECT_ONE, VEC_DIRECT_ONE_LEN, wrong, nullptr,
                        store, context, ring) == SQLITE_AUTH,
                    "wrong master key not AUTH");
            unsigned char bad_store[16], bad_ctx[16];
            for (int i = 0; i < 16; ++i) {
                bad_store[i] = store[i];
                bad_ctx[i] = context[i];
            }
            bad_store[0] ^= 0xff;
            bad_ctx[0] ^= 0xff;
            require(sqlite_murmur_keyring_open(
                        VEC_DIRECT_ONE, VEC_DIRECT_ONE_LEN, master, nullptr,
                        bad_store, context, ring) == SQLITE_AUTH,
                    "wrong store id not rejected");
            // Wrong context changes both the KDF info and the body check;
            // either rejection is correct, success is not.
            require(sqlite_murmur_keyring_open(
                        VEC_DIRECT_ONE, VEC_DIRECT_ONE_LEN, master, nullptr,
                        store, bad_ctx, ring) != SQLITE_OK,
                    "wrong context accepted");
            require(sqlite_murmur_keyring_open(nullptr, 10, master, nullptr,
                                               store, context,
                                               ring) == SQLITE_MISUSE,
                    "null envelope accepted");
        }

        // 3. Tamper matrix over VEC_DIRECT_TWO: every corruption class
        // fails (header CRC, magic, version, KDF id, sealed size,
        // ciphertext/auth tag, truncation).
        {
            const std::size_t hdr_len = 61; // empty wrap hint
            const std::vector<std::pair<std::size_t, const char *>> flips = {
                {0, "magic"}, {4, "version"}, {6, "kdf"},
                {20, "salt"}, {57, "header-crc"}, {60, "header-crc-tail"},
            };
            for (const auto &f : flips) {
                std::vector<unsigned char> tampered(
                    VEC_DIRECT_TWO, VEC_DIRECT_TWO + VEC_DIRECT_TWO_LEN);
                tampered[f.first] ^= 0xff;
                sqlite_murmur_keyring ring;
                const int rc = sqlite_murmur_keyring_open(
                    tampered.data(), tampered.size(), master, nullptr, store,
                    context, ring);
                // Tightened per F4: the header CRC precedes the GCM open,
                // so header tamper is deterministically CORRUPT (an AUTH
                // here would mean the CRC check was skipped).
                require(rc == SQLITE_CORRUPT,
                        std::string("tamper accepted: ") + f.second);
            }
            // Ciphertext flip (first sealed byte) breaks the GCM tag.
            {
                std::vector<unsigned char> tampered(
                    VEC_DIRECT_TWO, VEC_DIRECT_TWO + VEC_DIRECT_TWO_LEN);
                tampered[hdr_len + 12 + 4] ^= 0x01;
                sqlite_murmur_keyring ring;
                require(sqlite_murmur_keyring_open(
                            tampered.data(), tampered.size(), master, nullptr,
                            store, context, ring) == SQLITE_AUTH,
                        "ciphertext tamper not AUTH");
            }
            // Truncation at several points.
            for (std::size_t cut : {std::size_t{10}, std::size_t{61},
                                    VEC_DIRECT_TWO_LEN - 1}) {
                sqlite_murmur_keyring ring;
                require(sqlite_murmur_keyring_open(VEC_DIRECT_TWO, cut, master,
                                                   nullptr, store, context,
                                                   ring) == SQLITE_CORRUPT,
                        "truncation accepted");
            }
        }

        // 4. Create -> open round trip; rotate stages (old retires only in
        // the NEW bytes; the caller's ring is untouched until it opens
        // the persisted revision — archive rotate_persisted ordering).
        {
            unsigned char cstore[16], cctx[16], cmaster[32];
            for (int i = 0; i < 16; ++i) {
                cstore[i] = static_cast<unsigned char>(0x10 + i);
                cctx[i] = static_cast<unsigned char>(0x50 + i);
            }
            fill(cmaster, 0x70);
            std::string fresh;
            require(sqlite_murmur_keyring_create(
                        SQLITE_MURMUR_KDF_DIRECT_HKDF, 0, 0, 0, "v1", cstore,
                        cctx, cmaster, nullptr, fresh) == SQLITE_OK,
                    "create failed");
            sqlite_murmur_keyring ring;
            require(sqlite_murmur_keyring_open(
                        reinterpret_cast<const unsigned char *>(fresh.data()),
                        fresh.size(), cmaster, nullptr, cstore, cctx, ring) ==
                        SQLITE_OK,
                    "fresh envelope failed to open");
            require(ring.seq == 1 && ring.current == 1 &&
                        ring.keys.size() == 1 && ring.wrap_id == "v1",
                    "fresh envelope fields wrong");
            std::string rotated;
            std::uint32_t new_id = 0;
            require(sqlite_murmur_keyring_rotate(ring, cstore, cmaster,
                                                 nullptr, rotated,
                                                 new_id) == SQLITE_OK &&
                        new_id == 2,
                    "rotate failed");
            require(ring.current == 1 && ring.seq == 1,
                    "rotate mutated the caller's ring");
            sqlite_murmur_keyring ring2;
            require(sqlite_murmur_keyring_open(
                        reinterpret_cast<const unsigned char *>(
                            rotated.data()),
                        rotated.size(), cmaster, nullptr, cstore, cctx,
                        ring2) == SQLITE_OK,
                    "rotated envelope failed to open");
            require(ring2.seq == 2 && ring2.current == 2 &&
                        ring2.keys.size() == 2 && !ring2.keys[0].active &&
                        ring2.keys[1].active,
                    "rotated envelope fields wrong");
            // Old material still decrypts old bytes (retired keys work).
            require(bytes_eq(ring2.keys[0].key, ring.keys[0].key, 32),
                    "rotation changed old key material");
            sqlite_murmur_keyring_destroy(ring);
            sqlite_murmur_keyring_destroy(ring2);
        }

        // 5. Argon2id without a vendored unit fails closed and loudly;
        // with one, bad params still fail closed.
        {
            sqlite_murmur_keyring ring;
            const int have = sqlite_murmur_keyring_has_argon2();
            const int rc = sqlite_murmur_keyring_open(
                VEC_ARGON2_HDR, VEC_ARGON2_HDR_LEN, nullptr, "some passphrase",
                store, context, ring);
            if (!have) {
                require(rc == SQLITE_ERROR, "argon2 w/o unit not loud");
            } else {
                require(rc != SQLITE_OK, "garbage argon2 body accepted");
            }
            std::string ignored;
            require(sqlite_murmur_keyring_create(
                        SQLITE_MURMUR_KDF_ARGON2ID, 3, 65536, 4, "", store,
                        context, nullptr, "pw", ignored) ==
                        (have ? SQLITE_OK : SQLITE_ERROR),
                    "argon2 create gate wrong");
            // Zero-cost argon2 header (valid CRC, generator-made since
            // hand-tamper would break the CRC first): the param gate
            // must return ERROR with or without the Argon2 unit.
            sqlite_murmur_keyring zring;
            require(sqlite_murmur_keyring_open(
                        VEC_ARGON2_ZEROPARAMS, VEC_ARGON2_ZEROPARAMS_LEN,
                        nullptr, "some passphrase", store, context, zring) ==
                        SQLITE_ERROR,
                    "zero-params argon2 not rejected");
        }

        // 6. Hardening edges (F4): sealed-len flip, null master,
        // wrap-length boundary, rotate count-cap, non-empty-wrap tamper.
        {
            // Sealed-length direct flip: declared length no longer matches
            // the trailer, deterministically CORRUPT before any decrypt.
            std::vector<unsigned char> tampered(
                VEC_DIRECT_TWO, VEC_DIRECT_TWO + VEC_DIRECT_TWO_LEN);
            tampered[61 + 12] ^= 0xff; // first sealedlen byte
            sqlite_murmur_keyring ring;
            require(sqlite_murmur_keyring_open(
                        tampered.data(), tampered.size(), master, nullptr,
                        store, context, ring) == SQLITE_CORRUPT,
                    "sealed-len tamper accepted");

            // Null master on a direct envelope: AUTH (missing protector).
            require(sqlite_murmur_keyring_open(
                        VEC_DIRECT_ONE, VEC_DIRECT_ONE_LEN, nullptr, nullptr,
                        store, context, ring) == SQLITE_AUTH,
                    "null master not AUTH");

            // Wrap-length boundary: 128 seals, 129 is MISUSE.
            {
                unsigned char cstore[16], cctx[16], cmaster[32];
                for (int i = 0; i < 16; ++i) {
                    cstore[i] = static_cast<unsigned char>(i);
                    cctx[i] = static_cast<unsigned char>(0x80 + i);
                }
                fill(cmaster, 0x40);
                std::string ok_bytes;
                require(sqlite_murmur_keyring_create(
                            SQLITE_MURMUR_KDF_DIRECT_HKDF, 0, 0, 0,
                            std::string(128, 'w').c_str(), cstore, cctx,
                            cmaster, nullptr, ok_bytes) == SQLITE_OK,
                        "128-byte wrap rejected");
                std::string bad_bytes = "sentinel";
                require(sqlite_murmur_keyring_create(
                            SQLITE_MURMUR_KDF_DIRECT_HKDF, 0, 0, 0,
                            std::string(129, 'w').c_str(), cstore, cctx,
                            cmaster, nullptr, bad_bytes) == SQLITE_MISUSE,
                        "129-byte wrap accepted");
                require(bad_bytes.empty(), "failed create left output");
            }

            // Rotate count-cap: a full ring fails closed with no output.
            {
                sqlite_murmur_keyring full;
                full.seq = 1;
                full.kdf = SQLITE_MURMUR_KDF_DIRECT_HKDF;
                full.current = 65536;
                full.wrap_id = "cap";
                for (std::uint32_t id = 1; id <= 65536; ++id) {
                    sqlite_murmur_key_entry e;
                    e.id = id;
                    e.active = (id == 65536);
                    full.keys.push_back(e);
                }
                std::string staged = "sentinel";
                std::uint32_t new_id = 0;
                unsigned char cstore[16], cmaster[32];
                for (int i = 0; i < 16; ++i)
                    cstore[i] = static_cast<unsigned char>(i);
                fill(cmaster, 0x40);
                require(sqlite_murmur_keyring_rotate(
                            full, cstore, cmaster, nullptr, staged, new_id) !=
                            SQLITE_OK &&
                        staged.empty() && new_id == 0,
                    "rotate count-cap not fail-closed");
                sqlite_murmur_keyring_destroy(full);
            }

            // Non-empty-wrap tamper (VEC_DIRECT_ONE hint byte): CRC fail.
            {
                std::vector<unsigned char> one(
                    VEC_DIRECT_ONE, VEC_DIRECT_ONE + VEC_DIRECT_ONE_LEN);
                one[60] ^= 0xff; // inside "wrap-hint"
                sqlite_murmur_keyring r2;
                require(sqlite_murmur_keyring_open(
                            one.data(), one.size(), master, nullptr, store,
                            context, r2) == SQLITE_CORRUPT,
                        "wrap-hint tamper accepted");
            }
        }

        // 7. Drop-keys for prune (D2b): retired-only drops re-seal with
        // a bumped sequence; current/active/unknown/empty/drop-all fail
        // closed with no output.
        {
            unsigned char cstore[16], cctx[16], cmaster[32];
            for (int i = 0; i < 16; ++i) {
                cstore[i] = static_cast<unsigned char>(0x20 + i);
                cctx[i] = static_cast<unsigned char>(0x60 + i);
            }
            fill(cmaster, 0x90);
            std::string fresh, rotated;
            require(sqlite_murmur_keyring_create(
                        SQLITE_MURMUR_KDF_DIRECT_HKDF, 0, 0, 0, "drop", cstore,
                        cctx, cmaster, nullptr, fresh) == SQLITE_OK,
                    "drop setup create failed");
            sqlite_murmur_keyring ring;
            require(sqlite_murmur_keyring_open(
                        reinterpret_cast<const unsigned char *>(fresh.data()),
                        fresh.size(), cmaster, nullptr, cstore, cctx, ring) ==
                        SQLITE_OK,
                    "drop setup open failed");
            std::uint32_t new_id = 0;
            require(sqlite_murmur_keyring_rotate(ring, cstore, cmaster,
                                                 nullptr, rotated,
                                                 new_id) == SQLITE_OK,
                    "drop setup rotate failed");
            sqlite_murmur_keyring ring2;
            require(sqlite_murmur_keyring_open(
                        reinterpret_cast<const unsigned char *>(
                            rotated.data()),
                        rotated.size(), cmaster, nullptr, cstore, cctx,
                        ring2) == SQLITE_OK,
                    "drop setup reopen failed");
            const std::uint64_t seq_before = ring2.seq;
            std::string pruned;
            const std::uint32_t drop_one[] = {1};
            require(sqlite_murmur_keyring_drop_keys(
                        ring2, drop_one, 1, cstore, cmaster, nullptr,
                        pruned) == SQLITE_OK && !pruned.empty(),
                    "drop retired failed");
            sqlite_murmur_keyring ring3;
            require(sqlite_murmur_keyring_open(
                        reinterpret_cast<const unsigned char *>(pruned.data()),
                        pruned.size(), cmaster, nullptr, cstore, cctx,
                        ring3) == SQLITE_OK,
                    "dropped envelope failed to open");
            require(ring3.keys.size() == 1 && ring3.current == 2 &&
                        ring3.seq == seq_before + 1,
                    "dropped envelope fields wrong");
            const std::uint32_t bad_sets[][2] = {{2, 0}, {9, 0}, {1, 2}};
            for (const auto &bad : bad_sets) {
                const std::size_t n = (bad[0] == 1 && bad[1] == 2) ? 2 : 1;
                std::string out = "sentinel";
                require(sqlite_murmur_keyring_drop_keys(
                            ring2, bad, n, cstore, cmaster, nullptr, out) !=
                            SQLITE_OK && out.empty(),
                        "bad drop set accepted");
            }
            {
                std::string out = "sentinel";
                require(sqlite_murmur_keyring_drop_keys(
                            ring2, nullptr, 0, cstore, cmaster, nullptr,
                            out) == SQLITE_MISUSE && out.empty(),
                    "empty drop accepted");
            }
            sqlite_murmur_keyring_destroy(ring);
            sqlite_murmur_keyring_destroy(ring2);
            sqlite_murmur_keyring_destroy(ring3);
        }

        std::cout << "Murmur keyring: fixture agreement, auth/binding, "
                     "tamper matrix, create/rotate, argon2 gate, hardening, "
                     "drop-keys passed\n";
        return 0;
    } catch (const std::exception &e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
