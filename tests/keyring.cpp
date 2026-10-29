// WP-D slice D2a tests: keys.enc v3 envelope. Decode agreement against
// independent Python-generated fixtures (tests/keyring_vectors.h) plus
// round-trip, rotation-ordering, and tamper matrices. Links the product
// library: the envelope API is implemented in crates/murmur-storage and
// declared in include/sqlite_murmur_keyring.h (stable C ABI).

#include "sqlite_murmur_keyring.h"

#include "keyring_vectors.h"

#include <cstdint>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>
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

// Snapshot of an opened ring: mirrors the envelope fields so the
// assertions below read exactly what the envelope carries. Populated
// from a transient handle (destroyed before return); rotate/drop_keys
// take these flattened fields, never a live handle.
struct KeyEntry {
    std::uint32_t id = 0;
    std::int64_t created_nanos = 0;
    bool active = false;
    unsigned char key[32] = {0};
};

struct Ring {
    std::uint64_t seq = 0;
    std::uint8_t kdf = SQLITE_MURMUR_KDF_DIRECT_HKDF;
    std::uint32_t argon_time = 0;
    std::uint32_t argon_memory_kib = 0;
    std::uint8_t argon_threads = 0;
    unsigned char context[16] = {0};
    std::string wrap_id;
    std::vector<KeyEntry> keys; // sorted by id
    std::uint32_t current = 0;  // highest active id
};

std::pair<int, Ring> open_ring(const unsigned char *env, std::size_t len,
                               const unsigned char *master,
                               const char *passphrase,
                               const unsigned char *store,
                               const unsigned char *context) {
    sqlite_murmur_keyring_rs_t *handle = nullptr;
    const int rc = sqlite_murmur_keyring_rs_open(
        env, len, master, passphrase, store, context, &handle);
    Ring ring;
    if (rc != SQLITE_OK || !handle) return {rc, ring};
    std::size_t nkeys = 0;
    const unsigned char *wrap = nullptr;
    std::size_t wrap_len = 0;
    const int frc =
        sqlite_murmur_keyring_rs_seq(handle, &ring.seq) == SQLITE_OK &&
                sqlite_murmur_keyring_rs_kdf(handle, &ring.kdf) == SQLITE_OK &&
                sqlite_murmur_keyring_rs_argon(handle, &ring.argon_time,
                                               &ring.argon_memory_kib,
                                               &ring.argon_threads) ==
                    SQLITE_OK &&
                sqlite_murmur_keyring_rs_context(handle, ring.context) ==
                    SQLITE_OK &&
                sqlite_murmur_keyring_rs_wrap(handle, &wrap, &wrap_len) ==
                    SQLITE_OK &&
                sqlite_murmur_keyring_rs_key_count(handle, &nkeys) ==
                    SQLITE_OK &&
                sqlite_murmur_keyring_rs_current(handle, &ring.current) ==
                    SQLITE_OK
            ? SQLITE_OK
            : SQLITE_ERROR;
    if (frc == SQLITE_OK) {
        ring.wrap_id.assign(reinterpret_cast<const char *>(wrap), wrap_len);
        ring.keys.resize(nkeys);
        for (std::size_t i = 0; i < nkeys; ++i) {
            std::uint8_t active = 0;
            if (sqlite_murmur_keyring_rs_key_at(
                    handle, i, &ring.keys[i].id, &ring.keys[i].created_nanos,
                    &active, ring.keys[i].key) != SQLITE_OK) {
                sqlite_murmur_keyring_rs_destroy(handle);
                return {SQLITE_ERROR, Ring{}};
            }
            ring.keys[i].active = active != 0;
        }
    }
    sqlite_murmur_keyring_rs_destroy(handle);
    return {frc, ring};
}

void destroy_ring(Ring &ring) {
    for (auto &k : ring.keys)
        std::memset(k.key, 0, sizeof(k.key));
    ring.keys.clear();
    ring.seq = 0;
    ring.current = 0;
}

// Linear scan with current-id semantics: the live handle's
// rs_current reports the highest active id; by-id lookup scans.
const unsigned char *ring_current(const Ring &ring, std::uint32_t &id) {
    std::uint32_t best = 0;
    const unsigned char *found = nullptr;
    for (const auto &k : ring.keys) {
        if (k.active && k.id > best) {
            best = k.id;
            found = k.key;
        }
    }
    id = best;
    return found;
}

const unsigned char *ring_by_id(const Ring &ring, std::uint32_t id) {
    for (const auto &k : ring.keys)
        if (k.id == id) return k.key;
    return nullptr;
}

// Flattened-ring rotate/drop helpers: build the parallel arrays the
// _rs_ functions take and take the sealed buffer on success. On
// failure the out-params are untouched (asserted by callers).
struct FlatRing {
    std::vector<std::uint32_t> ids;
    std::vector<std::int64_t> nanos;
    std::vector<std::uint8_t> actives;
    std::vector<unsigned char> keys; // nkeys * 32
};

FlatRing flatten(const Ring &ring) {
    FlatRing f;
    f.ids.reserve(ring.keys.size());
    f.nanos.reserve(ring.keys.size());
    f.actives.reserve(ring.keys.size());
    f.keys.reserve(ring.keys.size() * 32);
    for (const auto &k : ring.keys) {
        f.ids.push_back(k.id);
        f.nanos.push_back(k.created_nanos);
        f.actives.push_back(k.active ? 1 : 0);
        f.keys.insert(f.keys.end(), k.key, k.key + 32);
    }
    return f;
}

std::string take_buf(unsigned char *buf, std::size_t len, std::size_t cap) {
    std::string out(reinterpret_cast<const char *>(buf), len);
    sqlite_murmur_keyring_rs_free(buf, len, cap);
    return out;
}

std::pair<int, std::string> create_envelope(
    std::uint8_t kdf, std::uint32_t argon_time, std::uint32_t argon_memory_kib,
    std::uint8_t argon_threads, const char *wrap_id, const unsigned char *store,
    const unsigned char *context, const unsigned char *master,
    const char *passphrase, unsigned char *&buf_out) {
    unsigned char *buf = nullptr;
    std::size_t len = 0, cap = 0;
    const int rc = sqlite_murmur_keyring_rs_create(
        kdf, argon_time, argon_memory_kib, argon_threads, wrap_id, store,
        context, master, passphrase, &buf, &len, &cap);
    buf_out = buf;
    if (rc != SQLITE_OK) return {rc, std::string()};
    return {rc, take_buf(buf, len, cap)};
}

struct RotateOut {
    int rc = SQLITE_ERROR;
    std::string bytes;
    std::uint32_t new_id = 0;
};

RotateOut rotate_envelope(const Ring &ring, const unsigned char *store,
                          const unsigned char *master, const char *passphrase,
                          unsigned char *&buf_out, std::uint32_t &id_out) {
    const FlatRing f = flatten(ring);
    unsigned char *buf = nullptr;
    std::size_t len = 0, cap = 0;
    std::uint32_t new_id = 0;
    const int rc = sqlite_murmur_keyring_rs_rotate(
        ring.kdf, ring.seq, ring.argon_time, ring.argon_memory_kib,
        ring.argon_threads, ring.context, ring.wrap_id.c_str(), ring.current,
        f.ids.data(), f.nanos.data(), f.actives.data(), f.keys.data(),
        ring.keys.size(), store, master, passphrase, &buf, &len, &cap,
        &new_id);
    buf_out = buf;
    id_out = new_id;
    RotateOut out;
    out.rc = rc;
    out.new_id = new_id;
    if (rc == SQLITE_OK) out.bytes = take_buf(buf, len, cap);
    return out;
}

std::pair<int, std::string> drop_envelope(const Ring &ring,
                                          const std::uint32_t *drop,
                                          std::size_t ndrop,
                                          const unsigned char *store,
                                          const unsigned char *master,
                                          const char *passphrase,
                                          unsigned char *&buf_out) {
    const FlatRing f = flatten(ring);
    unsigned char *buf = nullptr;
    std::size_t len = 0, cap = 0;
    const int rc = sqlite_murmur_keyring_rs_drop_keys(
        ring.kdf, ring.seq, ring.argon_time, ring.argon_memory_kib,
        ring.argon_threads, ring.context, ring.wrap_id.c_str(), ring.current,
        f.ids.data(), f.nanos.data(), f.actives.data(), f.keys.data(),
        ring.keys.size(), drop, ndrop, store, master, passphrase, &buf, &len,
        &cap);
    buf_out = buf;
    if (rc != SQLITE_OK) return {rc, std::string()};
    return {rc, take_buf(buf, len, cap)};
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
            auto [rc, ring] = open_ring(VEC_DIRECT_ONE, VEC_DIRECT_ONE_LEN,
                                        master, nullptr, store, context);
            require(rc == SQLITE_OK, "VEC_DIRECT_ONE failed to open");
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
            const unsigned char *cur = ring_current(ring, id);
            require(cur && id == 1 && bytes_eq(cur, key1, 32),
                    "VEC1 current lookup wrong");
            require(!ring_by_id(ring, 7), "phantom key id found");
            destroy_ring(ring);
            require(ring.keys.empty(), "destroy left keys");
        }
        {
            auto [rc, ring] = open_ring(VEC_DIRECT_TWO, VEC_DIRECT_TWO_LEN,
                                        master, nullptr, store, context);
            require(rc == SQLITE_OK, "VEC_DIRECT_TWO failed to open");
            require(ring.seq == 41, "VEC2 seq wrong");
            require(ring.keys.size() == 2, "VEC2 key count wrong");
            require(!ring.keys[0].active && ring.keys[0].id == 1,
                    "VEC2 key1 should be retired");
            require(ring.keys[1].active && ring.current == 2,
                    "VEC2 key2 should be current");
            require(bytes_eq(ring.keys[0].key, key1, 32), "VEC2 key1 wrong");
            require(bytes_eq(ring.keys[1].key, key2, 32), "VEC2 key2 wrong");
            destroy_ring(ring);
        }

        // 2. Authentication and binding failures fail closed with the
        // right codes: wrong key -> AUTH; wrong store id -> AUTH (KDF
        // info/AAD mismatch fails the GCM open); wrong context -> non-OK
        // (AUTH via the same path; the body context check is unreachable
        // after a protector mismatch).
        {
            unsigned char wrong[32];
            fill(wrong, 0x41);
            auto bad_key = open_ring(VEC_DIRECT_ONE, VEC_DIRECT_ONE_LEN,
                                     wrong, nullptr, store, context);
            require(bad_key.first == SQLITE_AUTH,
                    "wrong master key not AUTH");
            unsigned char bad_store[16], bad_ctx[16];
            for (int i = 0; i < 16; ++i) {
                bad_store[i] = store[i];
                bad_ctx[i] = context[i];
            }
            bad_store[0] ^= 0xff;
            bad_ctx[0] ^= 0xff;
            auto bad_sid = open_ring(VEC_DIRECT_ONE, VEC_DIRECT_ONE_LEN,
                                     master, nullptr, bad_store, context);
            require(bad_sid.first == SQLITE_AUTH,
                    "wrong store id not rejected");
            // Wrong context changes both the KDF info and the body check;
            // either rejection is correct, success is not.
            auto bad_ctx_open = open_ring(VEC_DIRECT_ONE, VEC_DIRECT_ONE_LEN,
                                              master, nullptr, store, bad_ctx);
            require(bad_ctx_open.first != SQLITE_OK,
                    "wrong context accepted");
            auto null_env =
                open_ring(nullptr, 10, master, nullptr, store, context);
            require(null_env.first == SQLITE_MISUSE,
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
                auto [rc, ring] =
                    open_ring(tampered.data(), tampered.size(), master,
                              nullptr, store, context);
                // The header CRC precedes the GCM open, so header tamper
                // is deterministically CORRUPT (an AUTH here would mean
                // the CRC check was skipped).
                require(rc == SQLITE_CORRUPT,
                        std::string("tamper accepted: ") + f.second);
            }
            // Ciphertext flip (first sealed byte) breaks the GCM tag.
            {
                std::vector<unsigned char> tampered(
                    VEC_DIRECT_TWO, VEC_DIRECT_TWO + VEC_DIRECT_TWO_LEN);
                tampered[hdr_len + 12 + 4] ^= 0x01;
                auto [rc, ring] =
                    open_ring(tampered.data(), tampered.size(), master,
                              nullptr, store, context);
                require(rc == SQLITE_AUTH, "ciphertext tamper not AUTH");
            }
            // Truncation at several points.
            for (std::size_t cut : {std::size_t{10}, std::size_t{61},
                                    VEC_DIRECT_TWO_LEN - 1}) {
                auto [rc, ring] = open_ring(VEC_DIRECT_TWO, cut, master,
                                            nullptr, store, context);
                require(rc == SQLITE_CORRUPT, "truncation accepted");
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
            unsigned char *cbuf = nullptr;
            auto [crc, fresh] = create_envelope(
                SQLITE_MURMUR_KDF_DIRECT_HKDF, 0, 0, 0, "v1", cstore, cctx,
                cmaster, nullptr, cbuf);
            require(crc == SQLITE_OK, "create failed");
            auto [orc, ring] = open_ring(
                reinterpret_cast<const unsigned char *>(fresh.data()),
                fresh.size(), cmaster, nullptr, cstore, cctx);
            require(orc == SQLITE_OK, "fresh envelope failed to open");
            require(ring.seq == 1 && ring.current == 1 &&
                        ring.keys.size() == 1 && ring.wrap_id == "v1",
                    "fresh envelope fields wrong");
            unsigned char *rbuf = nullptr;
            std::uint32_t new_id = 0;
            RotateOut rot = rotate_envelope(ring, cstore, cmaster, nullptr,
                                            rbuf, new_id);
            require(rot.rc == SQLITE_OK && rot.new_id == 2, "rotate failed");
            require(ring.current == 1 && ring.seq == 1,
                    "rotate mutated the caller's ring");
            auto [o2c, ring2] = open_ring(
                reinterpret_cast<const unsigned char *>(rot.bytes.data()),
                rot.bytes.size(), cmaster, nullptr, cstore, cctx);
            require(o2c == SQLITE_OK, "rotated envelope failed to open");
            require(ring2.seq == 2 && ring2.current == 2 &&
                        ring2.keys.size() == 2 && !ring2.keys[0].active &&
                        ring2.keys[1].active,
                    "rotated envelope fields wrong");
            // Old material still decrypts old bytes (retired keys work).
            require(bytes_eq(ring2.keys[0].key, ring.keys[0].key, 32),
                    "rotation changed old key material");
            destroy_ring(ring);
            destroy_ring(ring2);
        }
        // 5. Argon2id without a vendored unit fails closed and loudly;
        // with one, bad params still fail closed.
        {
            const int have = sqlite_murmur_keyring_rs_has_argon2();
            auto [arc, aring] =
                open_ring(VEC_ARGON2_HDR, VEC_ARGON2_HDR_LEN, nullptr,
                          "some passphrase", store, context);
            if (!have) {
                require(arc == SQLITE_ERROR, "argon2 w/o unit not loud");
            } else {
                require(arc != SQLITE_OK, "garbage argon2 body accepted");
            }
            unsigned char *abuf = nullptr;
            auto [acrc, ignored] = create_envelope(
                SQLITE_MURMUR_KDF_ARGON2ID, 3, 65536, 4, "", store, context,
                nullptr, "pw", abuf);
            require(acrc == (have ? SQLITE_OK : SQLITE_ERROR),
                    "argon2 create gate wrong");
            // Zero-cost argon2 header (valid CRC, generator-made since
            // hand-tamper would break the CRC first): the param gate
            // must return ERROR with or without the Argon2 unit.
            auto [zrc, zring] =
                open_ring(VEC_ARGON2_ZEROPARAMS, VEC_ARGON2_ZEROPARAMS_LEN,
                          nullptr, "some passphrase", store, context);
            require(zrc == SQLITE_ERROR, "zero-params argon2 not rejected");
        }

        // 6. Hardening edges (F4): sealed-len flip, null master,
        // wrap-length boundary, rotate count-cap, non-empty-wrap tamper.
        {
            // Sealed-length direct flip: declared length no longer matches
            // the trailer, deterministically CORRUPT before any decrypt.
            std::vector<unsigned char> tampered(
                VEC_DIRECT_TWO, VEC_DIRECT_TWO + VEC_DIRECT_TWO_LEN);
            tampered[61 + 12] ^= 0xff; // first sealedlen byte
            auto [src, sring] = open_ring(tampered.data(), tampered.size(),
                                          master, nullptr, store, context);
            require(src == SQLITE_CORRUPT, "sealed-len tamper accepted");

            // Null master on a direct envelope: AUTH (missing protector).
            auto [nrc, nring] = open_ring(VEC_DIRECT_ONE, VEC_DIRECT_ONE_LEN,
                                          nullptr, nullptr, store, context);
            require(nrc == SQLITE_AUTH, "null master not AUTH");

            // Wrap-length boundary: 128 seals, 129 is MISUSE.
            {
                unsigned char cstore[16], cctx[16], cmaster[32];
                for (int i = 0; i < 16; ++i) {
                    cstore[i] = static_cast<unsigned char>(i);
                    cctx[i] = static_cast<unsigned char>(0x80 + i);
                }
                fill(cmaster, 0x40);
                unsigned char *ok_buf = nullptr;
                auto [ok_rc, ok_bytes] = create_envelope(
                    SQLITE_MURMUR_KDF_DIRECT_HKDF, 0, 0, 0,
                    std::string(128, 'w').c_str(), cstore, cctx, cmaster,
                    nullptr, ok_buf);
                require(ok_rc == SQLITE_OK, "128-byte wrap rejected");
                unsigned char *bad_buf = nullptr;
                auto [bad_rc, bad_bytes] = create_envelope(
                    SQLITE_MURMUR_KDF_DIRECT_HKDF, 0, 0, 0,
                    std::string(129, 'w').c_str(), cstore, cctx, cmaster,
                    nullptr, bad_buf);
                require(bad_rc == SQLITE_MISUSE, "129-byte wrap accepted");
                require(bad_buf == nullptr && bad_bytes.empty(),
                        "failed create left output");
            }

            // Rotate count-cap: a full ring fails closed with no output.
            // The flattened _rs_ rotate takes the ring fields directly,
            // so the 65536-entry ring is built as parallel arrays.
            {
                const std::size_t nkeys = 65536;
                std::vector<std::uint32_t> ids(nkeys);
                std::vector<std::int64_t> nanos(nkeys, 0);
                std::vector<std::uint8_t> actives(nkeys, 0);
                std::vector<unsigned char> keys(nkeys * 32, 0);
                for (std::uint32_t id = 1; id <= nkeys; ++id)
                    ids[id - 1] = id;
                actives[nkeys - 1] = 1;
                unsigned char cstore[16], cctx[16], cmaster[32];
                for (int i = 0; i < 16; ++i) {
                    cstore[i] = static_cast<unsigned char>(i);
                    cctx[i] = 0;
                }
                fill(cmaster, 0x40);
                unsigned char *buf = nullptr;
                std::size_t len = 0, cap = 0;
                std::uint32_t new_id = 0;
                const int rc = sqlite_murmur_keyring_rs_rotate(
                    SQLITE_MURMUR_KDF_DIRECT_HKDF, 1, 0, 0, 0, cctx, "cap",
                    65536, ids.data(), nanos.data(), actives.data(),
                    keys.data(), nkeys, cstore, cmaster, nullptr, &buf, &len,
                    &cap, &new_id);
                require(rc != SQLITE_OK && buf == nullptr && new_id == 0,
                        "rotate count-cap not fail-closed");
            }

            // Non-empty-wrap tamper (VEC_DIRECT_ONE hint byte): CRC fail.
            {
                std::vector<unsigned char> one(
                    VEC_DIRECT_ONE, VEC_DIRECT_ONE + VEC_DIRECT_ONE_LEN);
                one[60] ^= 0xff; // inside "wrap-hint"
                auto [wrc, wring] = open_ring(one.data(), one.size(), master,
                                              nullptr, store, context);
                require(wrc == SQLITE_CORRUPT,
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
            unsigned char *cbuf = nullptr;
            auto [crc, fresh] = create_envelope(
                SQLITE_MURMUR_KDF_DIRECT_HKDF, 0, 0, 0, "drop", cstore, cctx,
                cmaster, nullptr, cbuf);
            require(crc == SQLITE_OK, "drop setup create failed");
            auto [orc, ring] = open_ring(
                reinterpret_cast<const unsigned char *>(fresh.data()),
                fresh.size(), cmaster, nullptr, cstore, cctx);
            require(orc == SQLITE_OK, "drop setup open failed");
            unsigned char *rbuf = nullptr;
            std::uint32_t new_id = 0;
            RotateOut rot = rotate_envelope(ring, cstore, cmaster, nullptr,
                                            rbuf, new_id);
            require(rot.rc == SQLITE_OK, "drop setup rotate failed");
            auto [o2c, ring2] = open_ring(
                reinterpret_cast<const unsigned char *>(rot.bytes.data()),
                rot.bytes.size(), cmaster, nullptr, cstore, cctx);
            require(o2c == SQLITE_OK, "drop setup reopen failed");
            const std::uint64_t seq_before = ring2.seq;
            unsigned char *dbuf = nullptr;
            const std::uint32_t drop_one[] = {1};
            auto [drc, pruned] = drop_envelope(ring2, drop_one, 1, cstore,
                                               cmaster, nullptr, dbuf);
            require(drc == SQLITE_OK && !pruned.empty(),
                    "drop retired failed");
            auto [o3c, ring3] = open_ring(
                reinterpret_cast<const unsigned char *>(pruned.data()),
                pruned.size(), cmaster, nullptr, cstore, cctx);
            require(o3c == SQLITE_OK, "dropped envelope failed to open");
            require(ring3.keys.size() == 1 && ring3.current == 2 &&
                        ring3.seq == seq_before + 1,
                    "dropped envelope fields wrong");
            const std::uint32_t bad_sets[][2] = {{2, 0}, {9, 0}, {1, 2}};
            for (const auto &bad : bad_sets) {
                const std::size_t n = (bad[0] == 1 && bad[1] == 2) ? 2 : 1;
                unsigned char *bbuf = nullptr;
                auto [brc, out] = drop_envelope(ring2, bad, n, cstore,
                                                cmaster, nullptr, bbuf);
                require(brc != SQLITE_OK && bbuf == nullptr && out.empty(),
                        "bad drop set accepted");
            }
            {
                unsigned char *ebuf = nullptr;
                auto [erc, out] = drop_envelope(ring2, nullptr, 0, cstore,
                                                cmaster, nullptr, ebuf);
                require(erc == SQLITE_MISUSE && ebuf == nullptr &&
                            out.empty(),
                        "empty drop accepted");
            }
            destroy_ring(ring);
            destroy_ring(ring2);
            destroy_ring(ring3);
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