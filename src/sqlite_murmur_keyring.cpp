// keys.enc v3 envelope (WP-D slice D2a). Logic port of archive
// spool/keyring.rs + spool/crypto.rs; see include/sqlite_murmur_keyring.h
// for provenance, format notes, and rulings. Pure bytes + OpenSSL: no
// filesystem, no RocksDB, no SQLite dependency in this file.

#include "sqlite_murmur_keyring.h"

#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>

#include <algorithm>
#include <array>
#include <cstring>

#if __has_include(<argon2.h>)
#include <argon2.h>
#define SQLITE_MURMUR_KEYRING_ARGON2 1
#else
#define SQLITE_MURMUR_KEYRING_ARGON2 0
#endif

namespace {

constexpr std::size_t kSalt = 32;
constexpr std::size_t kNonce = 12;
constexpr std::size_t kTag = 16;
constexpr std::size_t kKey = 32;
constexpr std::size_t kId16 = 16;
constexpr std::size_t kMaxWrap = 128;
constexpr std::uint32_t kMaxKeys = 1u << 16;
constexpr std::size_t kMaxSealed = 1u << 24;
constexpr char kInfo[] = "spool/keys-enc/v3/";

void wipe(void *p, std::size_t n) {
    volatile unsigned char *q = static_cast<unsigned char *>(p);
    while (n--) *q++ = 0;
}

std::uint32_t crc32c(const unsigned char *data, std::size_t n) {
    // Magic static: thread-safe one-time init (C++11). The previous
    // table+bool pair was an unsynchronized read-modify-write (formal
    // data race once Store::start calls keyring_open concurrently).
    // Lead merge-time fix; behavior identical.
    static const auto table = [] {
        std::array<std::uint32_t, 256> t{};
        // Reflected (LSB-first) table: reflected Castagnoli polynomial
        // 0x82F63B78 (normal form 0x1EDC6F41). Check: CRC32C("123456789")
        // must equal 0xE3069283 (verified via the fixture generator).
        for (std::uint32_t i = 0; i < 256; ++i) {
            std::uint32_t crc = i;
            for (int k = 0; k < 8; ++k)
                crc = (crc & 1) ? (crc >> 1) ^ 0x82F63B78u : crc >> 1;
            t[i] = crc;
        }
        return t;
    }();
    std::uint32_t crc = 0xFFFFFFFFu;
    for (std::size_t i = 0; i < n; ++i)
        crc = table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    return crc ^ 0xFFFFFFFFu;
}

void put_u16(std::string &s, std::uint16_t v) {
    s.push_back(static_cast<char>(v & 0xFF));
    s.push_back(static_cast<char>((v >> 8) & 0xFF));
}
void put_u32(std::string &s, std::uint32_t v) {
    for (int i = 0; i < 4; ++i)
        s.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}
void put_u64(std::string &s, std::uint64_t v) {
    for (int i = 0; i < 8; ++i)
        s.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}
std::uint16_t get_u16(const unsigned char *p) {
    return static_cast<std::uint16_t>(p[0] | (p[1] << 8));
}
std::uint32_t get_u32(const unsigned char *p) {
    std::uint32_t v = 0;
    for (int i = 3; i >= 0; --i) v = (v << 8) | p[i];
    return v;
}
std::uint64_t get_u64(const unsigned char *p) {
    std::uint64_t v = 0;
    for (int i = 7; i >= 0; --i) v = (v << 8) | p[i];
    return v;
}

// RFC 5869 HKDF-SHA256, L=32 (single expand block + 1).
bool hkdf_sha256(const unsigned char *ikm, std::size_t ikm_len,
                 const unsigned char *salt, std::size_t salt_len,
                 const unsigned char *info, std::size_t info_len,
                 unsigned char out[32]) {
    unsigned char prk[32];
    unsigned int prk_len = 0;
    if (!HMAC(EVP_sha256(), salt, static_cast<int>(salt_len), ikm, ikm_len,
              prk, &prk_len) ||
        prk_len != 32) {
        return false;
    }
    unsigned char t[32];
    unsigned int t_len = 0;
    std::string msg;
    msg.append(reinterpret_cast<const char *>(info), info_len);
    msg.push_back('\x01');
    const bool ok =
        HMAC(EVP_sha256(), prk, 32,
             reinterpret_cast<const unsigned char *>(msg.data()), msg.size(),
             t, &t_len) != nullptr &&
        t_len == 32;
    if (ok) std::memcpy(out, t, 32);
    wipe(prk, sizeof(prk));
    wipe(t, sizeof(t));
    return ok;
}

// Derive the envelope protector. Exactly one of master_key/passphrase.
// Returns SQLITE_OK / SQLITE_AUTH (missing input for the KDF) /
// SQLITE_ERROR (argon2 unavailable or bad params).
int derive_protector(std::uint8_t kdf, const unsigned char *master_key,
                     const char *passphrase, const unsigned char *salt,
                     std::uint32_t a_time, std::uint32_t a_mem,
                     std::uint8_t a_threads, const unsigned char store_id[16],
                     const unsigned char context_id[16],
                     unsigned char out[32]) {
    std::string info(kInfo, sizeof(kInfo) - 1);
    info.append(reinterpret_cast<const char *>(store_id), 16);
    info.append(reinterpret_cast<const char *>(context_id), 16);
    if (kdf == SQLITE_MURMUR_KDF_DIRECT_HKDF) {
        if (!master_key) return SQLITE_AUTH;
        if (!hkdf_sha256(master_key, 32, salt, kSalt,
                         reinterpret_cast<const unsigned char *>(info.data()),
                         info.size(), out))
            return SQLITE_CORRUPT;
        return SQLITE_OK;
    }
    if (kdf == SQLITE_MURMUR_KDF_ARGON2ID) {
        if (!passphrase || !*passphrase) return SQLITE_AUTH;
        if (a_threads == 0 || a_time == 0 || a_mem == 0) return SQLITE_ERROR;
#if SQLITE_MURMUR_KEYRING_ARGON2
        unsigned char stretched[32];
        const int rc = argon2id_hash_raw(
            a_time, a_mem, a_threads, passphrase, std::strlen(passphrase),
            salt, kSalt, stretched, sizeof(stretched));
        if (rc != ARGON2_OK) {
            wipe(stretched, sizeof(stretched));
            return SQLITE_ERROR;
        }
        const bool ok = hkdf_sha256(stretched, sizeof(stretched), salt, kSalt,
                                    reinterpret_cast<const unsigned char *>(
                                        info.data()),
                                    info.size(), out);
        wipe(stretched, sizeof(stretched));
        return ok ? SQLITE_OK : SQLITE_CORRUPT;
#else
        return SQLITE_ERROR;
#endif
    }
    return SQLITE_CORRUPT;
}

bool aesgcm_seal(const unsigned char key[32], const unsigned char nonce[12],
                 const unsigned char *aad, std::size_t aad_len,
                 const unsigned char *plain, std::size_t plain_len,
                 std::string &out) {
    out.assign(plain_len + kTag, '\0');
    EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
        out.clear();
        return false;
    }
    bool ok = false;
    do {
        int count = 0, final_count = 0;
        if (EVP_EncryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, nullptr,
                               nullptr) != 1)
            break;
        if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN,
                                static_cast<int>(kNonce), nullptr) != 1)
            break;
        if (EVP_EncryptInit_ex(ctx, nullptr, nullptr, key, nonce) != 1) break;
        if (aad_len > 0 &&
            EVP_EncryptUpdate(ctx, nullptr, &count, aad,
                              static_cast<int>(aad_len)) != 1)
            break;
        if (EVP_EncryptUpdate(ctx,
                              reinterpret_cast<unsigned char *>(&out[0]),
                              &count, plain,
                              static_cast<int>(plain_len)) != 1)
            break;
        if (EVP_EncryptFinal_ex(ctx,
                                reinterpret_cast<unsigned char *>(&out[0]) +
                                    count,
                                &final_count) != 1)
            break;
        if (static_cast<std::size_t>(count + final_count) != plain_len) break;
        if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG,
                                static_cast<int>(kTag),
                                &out[plain_len]) != 1)
            break;
        ok = true;
    } while (false);
    EVP_CIPHER_CTX_free(ctx);
    if (!ok) out.clear();
    return ok;
}

bool aesgcm_open(const unsigned char key[32], const unsigned char nonce[12],
                 const unsigned char *aad, std::size_t aad_len,
                 const unsigned char *sealed, std::size_t sealed_len,
                 std::string &out) {
    if (sealed_len < kTag) return false;
    const std::size_t plain_len = sealed_len - kTag;
    out.assign(plain_len + 1, '\0');
    EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
        out.clear();
        return false;
    }
    bool ok = false;
    do {
        int count = 0, final_count = 0;
        if (EVP_DecryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, nullptr,
                               nullptr) != 1)
            break;
        if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN,
                                static_cast<int>(kNonce), nullptr) != 1)
            break;
        if (EVP_DecryptInit_ex(ctx, nullptr, nullptr, key, nonce) != 1) break;
        if (aad_len > 0 &&
            EVP_DecryptUpdate(ctx, nullptr, &count, aad,
                              static_cast<int>(aad_len)) != 1)
            break;
        if (EVP_DecryptUpdate(ctx,
                              reinterpret_cast<unsigned char *>(&out[0]),
                              &count, sealed,
                              static_cast<int>(plain_len)) != 1)
            break;
        if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG,
                                static_cast<int>(kTag),
                                const_cast<unsigned char *>(sealed +
                                                            plain_len)) != 1)
            break;
        if (EVP_DecryptFinal_ex(ctx,
                                reinterpret_cast<unsigned char *>(&out[0]) +
                                    count,
                                &final_count) != 1)
            break;
        if (static_cast<std::size_t>(count + final_count) != plain_len) break;
        ok = true;
    } while (false);
    EVP_CIPHER_CTX_free(ctx);
    if (!ok) {
        wipe(&out[0], out.size());
        out.clear();
        return false;
    }
    out.resize(plain_len);
    return true;
}

// Seal one envelope revision (fresh salt + nonce). keys must be id-sorted.
int seal_revision(std::uint8_t kdf, std::uint64_t seq,
                  const unsigned char store_id[16],
                  const unsigned char context_id[16], const char *wrap_id,
                  std::uint32_t a_time, std::uint32_t a_mem,
                  std::uint8_t a_threads, const unsigned char *master_key,
                  const char *passphrase,
                  const std::vector<sqlite_murmur_key_entry> &keys,
                  std::string &envelope_out) {
    const std::size_t wrap_len = std::strlen(wrap_id);
    if (wrap_len > kMaxWrap || keys.empty() || keys.size() > kMaxKeys)
        return SQLITE_MISUSE;
    unsigned char salt[kSalt];
    if (RAND_bytes(salt, sizeof(salt)) != 1) return SQLITE_ERROR;
    unsigned char protector[32];
    const int prc = derive_protector(kdf, master_key, passphrase, salt, a_time,
                                     a_mem, a_threads, store_id, context_id,
                                     protector);
    if (prc != SQLITE_OK) {
        wipe(salt, sizeof(salt));
        return prc;
    }
    std::string body;
    put_u32(body, static_cast<std::uint32_t>(keys.size()));
    body.append(reinterpret_cast<const char *>(context_id), 16);
    body.push_back(static_cast<char>(wrap_len));
    body.append(wrap_id, wrap_len);
    for (const auto &k : keys) {
        put_u32(body, k.id);
        put_u64(body, static_cast<std::uint64_t>(k.created_nanos));
        body.push_back(k.active ? '\0' : '\x01');
        body.append(reinterpret_cast<const char *>(k.key), 32);
    }
    std::string hdr;
    hdr.append(SQLITE_MURMUR_KEYS_MAGIC, 4);
    put_u16(hdr, SQLITE_MURMUR_KEYS_VERSION);
    hdr.push_back(static_cast<char>(kdf));
    put_u64(hdr, seq);
    hdr.append(reinterpret_cast<const char *>(salt), kSalt);
    put_u32(hdr, a_time);
    put_u32(hdr, a_mem);
    hdr.push_back(static_cast<char>(a_threads));
    hdr.push_back(static_cast<char>(wrap_len));
    hdr.append(wrap_id, wrap_len);
    put_u32(hdr, crc32c(reinterpret_cast<const unsigned char *>(hdr.data()),
                        hdr.size()));
    unsigned char nonce[kNonce];
    if (RAND_bytes(nonce, sizeof(nonce)) != 1) {
        wipe(protector, sizeof(protector));
        wipe(salt, sizeof(salt));
        wipe(&body[0], body.size());
        return SQLITE_ERROR;
    }
    std::string aad = hdr;
    aad.append(reinterpret_cast<const char *>(store_id), 16);
    std::string sealed;
    const bool ok = aesgcm_seal(
        protector, nonce, reinterpret_cast<const unsigned char *>(aad.data()),
        aad.size(), reinterpret_cast<const unsigned char *>(body.data()),
        body.size(), sealed);
    wipe(protector, sizeof(protector));
    wipe(salt, sizeof(salt));
    wipe(&body[0], body.size());
    if (!ok) return SQLITE_CORRUPT;
    envelope_out = hdr;
    envelope_out.append(reinterpret_cast<const char *>(nonce), kNonce);
    put_u32(envelope_out, static_cast<std::uint32_t>(sealed.size()));
    envelope_out += sealed;
    return SQLITE_OK;
}

} // namespace

int sqlite_murmur_keyring_has_argon2() {
    return SQLITE_MURMUR_KEYRING_ARGON2;
}

void sqlite_murmur_keyring_destroy(sqlite_murmur_keyring &ring) {
    for (auto &k : ring.keys) wipe(k.key, sizeof(k.key));
    ring.keys.clear();
    ring.current = 0;
    ring.seq = 0;
    wipe(ring.context, sizeof(ring.context));
    if (!ring.wrap_id.empty()) {
        wipe(&ring.wrap_id[0], ring.wrap_id.size());
        ring.wrap_id.clear();
    }
}

int sqlite_murmur_keyring_open(const unsigned char *envelope, std::size_t len,
                               const unsigned char *master_key,
                               const char *passphrase,
                               const unsigned char store_id[16],
                               const unsigned char context_id[16],
                               sqlite_murmur_keyring &out) {
    sqlite_murmur_keyring_destroy(out);
    if (!envelope || !store_id || !context_id) return SQLITE_MISUSE;
    if (len < 61) return SQLITE_CORRUPT;
    if (std::memcmp(envelope, SQLITE_MURMUR_KEYS_MAGIC, 4) != 0)
        return SQLITE_CORRUPT;
    if (get_u16(envelope + 4) != SQLITE_MURMUR_KEYS_VERSION)
        return SQLITE_CORRUPT;
    const std::uint8_t kdf = envelope[6];
    if (kdf != SQLITE_MURMUR_KDF_DIRECT_HKDF &&
        kdf != SQLITE_MURMUR_KDF_ARGON2ID)
        return SQLITE_CORRUPT;
    const std::size_t wrap_len = envelope[56];
    if (wrap_len > kMaxWrap || len < 61 + wrap_len) return SQLITE_CORRUPT;
    const std::size_t hdr_len = 61 + wrap_len;
    const std::uint32_t want = get_u32(envelope + 57 + wrap_len);
    if (crc32c(envelope, 57 + wrap_len) != want) return SQLITE_CORRUPT;
    if (len < hdr_len + kNonce + 4) return SQLITE_CORRUPT;

    const std::uint64_t seq = get_u64(envelope + 7);
    const unsigned char *salt = envelope + 15;
    const std::uint32_t a_time = get_u32(envelope + 47);
    const std::uint32_t a_mem = get_u32(envelope + 51);
    const std::uint8_t a_threads = envelope[55];
    unsigned char protector[32];
    const int prc = derive_protector(kdf, master_key, passphrase, salt, a_time,
                                     a_mem, a_threads, store_id, context_id,
                                     protector);
    if (prc != SQLITE_OK) return prc;

    std::size_t off = hdr_len;
    const unsigned char *nonce = envelope + off;
    off += kNonce;
    const std::size_t sealed_len = get_u32(envelope + off);
    off += 4;
    if (sealed_len > kMaxSealed || len - off != sealed_len) {
        wipe(protector, sizeof(protector));
        return SQLITE_CORRUPT;
    }
    std::string aad;
    aad.append(reinterpret_cast<const char *>(envelope), hdr_len);
    aad.append(reinterpret_cast<const char *>(store_id), 16);
    std::string body;
    if (!aesgcm_open(protector, nonce,
                     reinterpret_cast<const unsigned char *>(aad.data()),
                     aad.size(), envelope + off, sealed_len, body)) {
        wipe(protector, sizeof(protector));
        return SQLITE_AUTH;
    }
    wipe(protector, sizeof(protector));

    const unsigned char *b =
        reinterpret_cast<const unsigned char *>(body.data());
    int rc = SQLITE_OK;
    sqlite_murmur_keyring ring;
    do {
        if (body.size() < 4 + 16 + 1) {
            rc = SQLITE_CORRUPT;
            break;
        }
        const std::uint32_t count = get_u32(b);
        if (count == 0 || count > kMaxKeys) {
            rc = SQLITE_CORRUPT;
            break;
        }
        if (std::memcmp(b + 4, context_id, 16) != 0) {
            rc = SQLITE_CORRUPT;
            break;
        }
        const std::size_t body_wrap = b[20];
        if (body_wrap > kMaxWrap || body.size() < 21 + body_wrap) {
            rc = SQLITE_CORRUPT;
            break;
        }
        constexpr std::size_t kEntry = 4 + 8 + 1 + 32;
        if (body.size() - (21 + body_wrap) !=
            static_cast<std::size_t>(count) * kEntry) {
            rc = SQLITE_CORRUPT;
            break;
        }
        ring.seq = seq;
        ring.kdf = kdf;
        ring.argon_time = a_time;
        ring.argon_memory_kib = a_mem;
        ring.argon_threads = a_threads;
        std::memcpy(ring.context, context_id, 16);
        ring.wrap_id.assign(reinterpret_cast<const char *>(b + 21), body_wrap);
        std::size_t pos = 21 + body_wrap;
        for (std::uint32_t i = 0; i < count; ++i) {
            const unsigned char *e = b + pos;
            pos += kEntry;
            sqlite_murmur_key_entry entry;
            entry.id = get_u32(e);
            entry.created_nanos =
                static_cast<std::int64_t>(get_u64(e + 4));
            if (e[12] == 0) entry.active = true;
            else if (e[12] == 1) entry.active = false;
            else {
                rc = SQLITE_CORRUPT;
                break;
            }
            if (entry.id == 0) {
                rc = SQLITE_CORRUPT;
                break;
            }
            bool dup = false;
            for (const auto &k : ring.keys) dup = dup || k.id == entry.id;
            if (dup) {
                rc = SQLITE_CORRUPT;
                break;
            }
            std::memcpy(entry.key, e + 13, 32);
            ring.keys.push_back(entry);
            if (entry.active && entry.id > ring.current)
                ring.current = entry.id;
        }
        if (rc == SQLITE_OK && ring.current == 0) rc = SQLITE_CORRUPT;
    } while (false);
    wipe(&body[0], body.size());
    if (rc != SQLITE_OK) {
        sqlite_murmur_keyring_destroy(ring);
        return rc;
    }
    std::sort(ring.keys.begin(), ring.keys.end(),
              [](const sqlite_murmur_key_entry &a,
                 const sqlite_murmur_key_entry &b) { return a.id < b.id; });
    out = ring;
    // The local copy's key bytes are now duplicated into out; wipe the
    // staging copy (out keeps its own live copies by design).
    sqlite_murmur_keyring_destroy(ring);
    return SQLITE_OK;
}

int sqlite_murmur_keyring_create(
    std::uint8_t kdf, std::uint32_t argon_time, std::uint32_t argon_memory_kib,
    std::uint8_t argon_threads, const char *wrap_id,
    const unsigned char store_id[16], const unsigned char context_id[16],
    const unsigned char *master_key, const char *passphrase,
    std::string &envelope_out) {
    envelope_out.clear();
    if (!wrap_id || !store_id || !context_id) return SQLITE_MISUSE;
    if (kdf != SQLITE_MURMUR_KDF_DIRECT_HKDF &&
        kdf != SQLITE_MURMUR_KDF_ARGON2ID)
        return SQLITE_MISUSE;
    if (kdf == SQLITE_MURMUR_KDF_DIRECT_HKDF &&
        (argon_time != 0 || argon_memory_kib != 0 || argon_threads != 0))
        return SQLITE_MISUSE;
    if (kdf == SQLITE_MURMUR_KDF_ARGON2ID && !sqlite_murmur_keyring_has_argon2())
        return SQLITE_ERROR;
    sqlite_murmur_key_entry first;
    first.id = 1;
    first.created_nanos = 0; // creation stamp assigned by the caller layer
    first.active = true;
    if (RAND_bytes(first.key, sizeof(first.key)) != 1) return SQLITE_ERROR;
    std::vector<sqlite_murmur_key_entry> keys;
    keys.push_back(first);
    wipe(first.key, sizeof(first.key));
    const int rc = seal_revision(kdf, 1, store_id, context_id, wrap_id,
                                 argon_time, argon_memory_kib, argon_threads,
                                 master_key, passphrase, keys, envelope_out);
    for (auto &k : keys) wipe(k.key, sizeof(k.key));
    return rc;
}

int sqlite_murmur_keyring_rotate(const sqlite_murmur_keyring &ring,
                                 const unsigned char store_id[16],
                                 const unsigned char *master_key,
                                 const char *passphrase,
                                 std::string &envelope_out,
                                 std::uint32_t &new_id_out) {
    envelope_out.clear();
    new_id_out = 0;
    if (!store_id || ring.keys.empty() || ring.current == 0) return SQLITE_MISUSE;
    std::uint32_t next = 1;
    for (const auto &k : ring.keys)
        if (k.id >= next) next = k.id + 1;
    if (next == 0) return SQLITE_ERROR; // id space exhausted (u32 wrap)
    if (ring.keys.size() + 1 > kMaxKeys) return SQLITE_ERROR;
    std::vector<sqlite_murmur_key_entry> staged = ring.keys;
    for (auto &k : staged)
        if (k.id == ring.current) k.active = false;
    sqlite_murmur_key_entry fresh;
    fresh.id = next;
    fresh.active = true;
    if (RAND_bytes(fresh.key, sizeof(fresh.key)) != 1) {
        for (auto &k : staged)
            if (k.id != ring.current) wipe(k.key, sizeof(k.key));
        return SQLITE_ERROR;
    }
    staged.push_back(fresh);
    wipe(fresh.key, sizeof(fresh.key));
    std::sort(staged.begin(), staged.end(),
              [](const sqlite_murmur_key_entry &a,
                 const sqlite_murmur_key_entry &b) { return a.id < b.id; });
    const int rc = seal_revision(ring.kdf, ring.seq + 1, store_id, ring.context,
                                 ring.wrap_id.c_str(), ring.argon_time,
                                 ring.argon_memory_kib, ring.argon_threads,
                                 master_key, passphrase, staged, envelope_out);
    // Only the staged FRESH key is ours to wipe; the rest borrows ring.
    for (auto &k : staged)
        if (k.id == next) wipe(k.key, sizeof(k.key));
    if (rc != SQLITE_OK) {
        envelope_out.clear();
        return rc;
    }
    new_id_out = next;
    return SQLITE_OK;
}

int sqlite_murmur_keyring_drop_keys(const sqlite_murmur_keyring &ring,
                                    const std::uint32_t *drop, std::size_t n,
                                    const unsigned char store_id[16],
                                    const unsigned char *master_key,
                                    const char *passphrase,
                                    std::string &envelope_out) {
    envelope_out.clear();
    if (!drop || n == 0 || !store_id || ring.keys.empty() || ring.current == 0)
        return SQLITE_MISUSE;
    std::vector<sqlite_murmur_key_entry> kept;
    kept.reserve(ring.keys.size());
    for (const auto &k : ring.keys) {
        bool dropped = false;
        for (std::size_t i = 0; i < n; ++i) {
            if (drop[i] != k.id) continue;
            if (k.active || k.id == ring.current) return SQLITE_MISUSE;
            dropped = true;
            break;
        }
        if (!dropped) kept.push_back(k);
    }
    if (kept.size() + n != ring.keys.size()) return SQLITE_MISUSE; // unknown id
    if (kept.empty()) return SQLITE_MISUSE;                        // drop-all
    bool has_current = false;
    for (const auto &k : kept) has_current = has_current || k.id == ring.current;
    if (!has_current) return SQLITE_CORRUPT; // ring disagrees with itself
    const int rc = seal_revision(ring.kdf, ring.seq + 1, store_id, ring.context,
                                 ring.wrap_id.c_str(), ring.argon_time,
                                 ring.argon_memory_kib, ring.argon_threads,
                                 master_key, passphrase, kept, envelope_out);
    if (rc != SQLITE_OK) envelope_out.clear();
    return rc;
}

const unsigned char *sqlite_murmur_keyring_current(
    const sqlite_murmur_keyring &ring, std::uint32_t &id_out) {
    for (const auto &k : ring.keys) {
        if (k.id == ring.current) {
            id_out = k.id;
            return k.key;
        }
    }
    return nullptr;
}

const unsigned char *sqlite_murmur_keyring_by_id(
    const sqlite_murmur_keyring &ring, std::uint32_t id) {
    for (const auto &k : ring.keys) {
        if (k.id == id) return k.key;
    }
    return nullptr;
}
