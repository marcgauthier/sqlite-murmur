#include "sqlite_murmur.h"
#include "sqlite_murmur_backup.h"
#include "sqlite_murmur_keyring.h"

#include <rocksdb/db.h>
#include <rocksdb/cache.h>
#if __has_include(<rocksdb/advanced_cache.h>)
#include <rocksdb/advanced_cache.h>
#endif
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/statistics.h>
#include <rocksdb/write_batch.h>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <zstd.h>
#ifdef SQLITE_MURMUR_HAS_AEGIS256
#include <aegis.h>
#endif

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <exception>
#include <limits>
#include <list>
#include <memory>
#include <mutex>
#include <new>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace {
constexpr size_t kBlock = 4096;
constexpr size_t kNonce = 12;
constexpr size_t kTag = 16;
constexpr char kHeader[] = "EXP1";
constexpr char kHeader3[] = "EXP3";
constexpr char kHeader4[] = "EXP4";
constexpr char kMetaSize[] = "meta:size";
constexpr char kMetaCipher[] = "meta:cipher";
// WP-D slice D2a: keys.enc v3 envelope bytes (verbatim) + the 16-byte store
// id it binds to. Both are plaintext meta keys (readable pre-key, like
// meta:cipher); the envelope carries its own authentication. Absent
// meta:keyring selects the legacy raw-registered-key path.
constexpr char kMetaKeyring[] = "meta:keyring";
constexpr char kMetaKeyringRekey[] = "meta:keyring.REKEY";
constexpr char kMetaStoreId[] = "meta:store_id";
enum class Cipher { AES256GCM, AEGIS256 };

const char *cipherName(Cipher cipher) {
    return cipher == Cipher::AES256GCM ? "aes256gcm" : "aegis256";
}
constexpr char kEndKey[] = "q";
constexpr const char *kVfsName = "sqlite-murmur";
constexpr size_t kRocksBlockCacheBytes = 32 * 1024 * 1024;

bool parseUnsigned(const char *text, uint64_t maximum, uint64_t &value) {
    value = 0;
    if (!text || !*text) return false;
    for (const char *c = text; *c; ++c) {
        if (*c < '0' || *c > '9') return false;
        const unsigned digit = *c - '0';
        if (value > maximum / 10 ||
            (value == maximum / 10 && digit > maximum % 10)) return false;
        value = value * 10 + digit;
    }
    return true;
}

using Block = std::array<unsigned char, kBlock>;

// Pending writes own fixed-size copies; only persisted cache entries are encrypted.
struct PageBuffer {
    Block bytes{};
    void wipe() noexcept { OPENSSL_cleanse(bytes.data(), bytes.size()); }
    ~PageBuffer() { wipe(); }
};
struct Record {
    std::shared_ptr<const std::string> encrypted;
    std::shared_ptr<const PageBuffer> plain;
    Record() = default;
    template<class D> explicit Record(std::unique_ptr<std::string, D> value) : encrypted(std::move(value)) {}
    template<class D> explicit Record(std::unique_ptr<PageBuffer, D> value) : plain(std::move(value)) {}
    explicit operator bool() const { return bool(encrypted) || bool(plain); }
    const std::string &operator*() const { return *encrypted; }
    void reset() { encrypted.reset(); plain.reset(); }
    bool operator==(const Record &other) const { return encrypted == other.encrypted && plain == other.plain; }
};
constexpr size_t kScratchSize = ZSTD_COMPRESSBOUND(kBlock) + 1;

struct DecodeWorkspace {
    std::array<unsigned char, kScratchSize> plain{};
    Block block{};
    size_t plainUsed = 0;
    bool blockDirty = false;
    std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> context{nullptr, ZSTD_freeDCtx};
    void wipePlain() noexcept {
        if (plainUsed) OPENSSL_cleanse(plain.data(), plainUsed);
        plainUsed = 0;
    }
    void wipeBlock() noexcept {
        if (blockDirty) OPENSSL_cleanse(block.data(), block.size());
        blockDirty = false;
    }
    void reset() noexcept {
        wipePlain();
        wipeBlock();
        if (context) (void)ZSTD_DCtx_reset(context.get(), ZSTD_reset_session_only);
    }
    ~DecodeWorkspace() { reset(); }
    bool decompress(Block &out, size_t size) {
        if (!context) {
            context.reset(ZSTD_createDCtx());
            if (!context) throw std::bad_alloc();
        }
        return ZSTD_decompressDCtx(context.get(), out.data(), out.size(), plain.data(), size) == kBlock;
    }
};

// Logical file length is separate from initialized storage, so reopening a RAM
// journal can reuse its allocation without zero-initializing every written byte.
constexpr size_t kJournalRetainBytes = 1024 * 1024;
struct MemFile {
    std::mutex mu;
    std::vector<unsigned char> bytes;
    size_t size = 0;
    size_t dirtyEnd = 0;
    void reset() noexcept {
        if (dirtyEnd) OPENSSL_cleanse(bytes.data(), dirtyEnd);
        dirtyEnd = size = 0;
    }
    ~MemFile() { reset(); }
};

// A pool owns only unused buffers. Leases keep the pool alive, including after
// the Store is destroyed; published encrypted versions return only at refcount 0.
template<class T, size_t Limit>
struct ReusePool : std::enable_shared_from_this<ReusePool<T, Limit>> {
    std::mutex mu;
    std::vector<std::unique_ptr<T>> idle;
#ifdef SQLITE_MURMUR_TEST_HOOKS
    std::atomic<size_t> created{0};
    std::atomic<size_t> reused{0};
#endif
    const size_t limit;
    explicit ReusePool(size_t retention = Limit) : limit(retention) { idle.reserve(limit); }
    void release(T *value) noexcept {
        std::unique_ptr<T> owned(value);
        reset(*value);
        std::lock_guard<std::mutex> guard(mu);
        if (idle.size() < limit && retain(*value)) idle.push_back(std::move(owned));
    }
    struct Return {
        std::shared_ptr<ReusePool> pool;
        void operator()(T *value) const noexcept { if (value) pool->release(value); }
    };
    using Lease = std::unique_ptr<T, Return>;
    Lease acquire() {
        std::unique_ptr<T> value;
        {
            std::lock_guard<std::mutex> guard(mu);
            if (!idle.empty()) { value = std::move(idle.back()); idle.pop_back(); }
        }
#ifdef SQLITE_MURMUR_TEST_HOOKS
        if (value) ++reused; else ++created;
#endif
        if (!value) value = std::make_unique<T>();
        return Lease(value.release(), Return{this->shared_from_this()});
    }
private:
    static void reset(DecodeWorkspace &value) noexcept { value.reset(); }
    static void reset(std::string &value) noexcept { value.clear(); }
    static void reset(PageBuffer &value) noexcept { value.wipe(); }
    static void reset(MemFile &value) noexcept { value.reset(); }
    static bool retain(const DecodeWorkspace &) noexcept { return true; }
    static bool retain(const PageBuffer &) noexcept { return true; }
    static bool retain(const MemFile &value) noexcept { return value.bytes.capacity() <= kJournalRetainBytes; }
    static bool retain(const std::string &value) noexcept { return value.capacity() <= 2 * (kScratchSize + 69); }
};
using RecordPool = ReusePool<std::string, 128>;
using WorkspacePool = ReusePool<DecodeWorkspace, 8>;
using PagePool = ReusePool<PageBuffer, 128>;
using JournalPool = ReusePool<MemFile, 1>;

struct PlainWiper {
    DecodeWorkspace &workspace;
    ~PlainWiper() { workspace.wipePlain(); }
};

struct BlockVersion {
    uint64_t id = 0;
    Record record;
};
// Normal SQLite reads/writes fit in 16 blocks, avoiding a metadata heap
// allocation for every VFS call. Larger requests retain the general path.
struct BlockVersions {
    std::array<BlockVersion, 16> small{};
    std::vector<BlockVersion> large;
    size_t count = 0;
    void resize(size_t n) { if (n > small.size()) large.resize(n); count = n; }
    size_t capacity() const { return std::max(small.size(), large.size()); }
    BlockVersion &operator[](size_t n) { return count > small.size() ? large[n] : small[n]; }
};

std::string pageKey(uint64_t page) {
    std::string out(9, '\0');
    out[0] = 'p';
    for (int i = 8; i >= 1; --i) {
        out[i] = static_cast<char>(page & 0xff);
        page >>= 8;
    }
    return out;
}

bool encryptAES(const unsigned char key[32], uint64_t page,
             const unsigned char *data, size_t n, bool compressed,
             std::string &result) {
    if (n > static_cast<size_t>(std::numeric_limits<int>::max())) return false;
    std::array<unsigned char, 9> aad{};
    aad[0] = compressed ? 1 : 0;
    for (int i = 8; i >= 1; --i) {
        aad[i] = static_cast<unsigned char>(page & 0xff);
        page >>= 8;
    }
    unsigned char nonce[kNonce];
    if (RAND_bytes(nonce, sizeof(nonce)) != 1) return false;
    result.assign(kHeader, 4);
    result.push_back(compressed ? '\1' : '\0');
    result.append(reinterpret_cast<const char *>(nonce), sizeof(nonce));
    const size_t offset = result.size();
    result.resize(offset + n + kTag);
    auto *ctx = EVP_CIPHER_CTX_new();
    if (!ctx) return false;
    bool ok = false;
    do {
        int count = 0, final_count = 0;
        if (EVP_EncryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, nullptr, nullptr) != 1) break;
        if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, kNonce, nullptr) != 1) break;
        if (EVP_EncryptInit_ex(ctx, nullptr, nullptr, key, nonce) != 1) break;
        if (EVP_EncryptUpdate(ctx, nullptr, &count, aad.data(), aad.size()) != 1) break;
        if (EVP_EncryptUpdate(ctx, reinterpret_cast<unsigned char *>(&result[offset]),
                              &count, data, static_cast<int>(n)) != 1) break;
        if (EVP_EncryptFinal_ex(ctx, reinterpret_cast<unsigned char *>(&result[offset]) + count,
                                &final_count) != 1) break;
        if (static_cast<size_t>(count + final_count) != n) break;
        if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, kTag,
                                &result[offset + n]) != 1) break;
        ok = true;
    } while (false);
    EVP_CIPHER_CTX_free(ctx);
    if (!ok) result.clear();
    return ok;
}

bool decryptAES(const unsigned char key[32], uint64_t page,
             const std::string &record, Block &out, DecodeWorkspace &workspace) {
    if (record.size() < 4 + 1 + kNonce + kTag ||
        std::memcmp(record.data(), kHeader, 4) != 0) return false;
    const unsigned char flag = static_cast<unsigned char>(record[4]);
    if (flag > 1) return false;
    const size_t payloadSize = record.size() - 4 - 1 - kNonce - kTag;
    if (payloadSize > ZSTD_compressBound(kBlock)) return false;
    std::array<unsigned char, 9> aad{};
    aad[0] = flag;
    uint64_t n = page;
    for (int i = 8; i >= 1; --i) {
        aad[i] = static_cast<unsigned char>(n & 0xff);
        n >>= 8;
    }
    const auto *nonce = reinterpret_cast<const unsigned char *>(record.data() + 5);
    const auto *cipher = reinterpret_cast<const unsigned char *>(record.data() + 5 + kNonce);
    auto &plain = workspace.plain;
    workspace.plainUsed = payloadSize + 1;
    PlainWiper wiper{workspace};
    auto *ctx = EVP_CIPHER_CTX_new();
    if (!ctx) return false;
    bool ok = false;
    do {
        int count = 0, final_count = 0;
        if (EVP_DecryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, nullptr, nullptr) != 1) break;
        if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, kNonce, nullptr) != 1) break;
        if (EVP_DecryptInit_ex(ctx, nullptr, nullptr, key, nonce) != 1) break;
        if (EVP_DecryptUpdate(ctx, nullptr, &count, aad.data(), aad.size()) != 1) break;
        if (EVP_DecryptUpdate(ctx, plain.data(), &count, cipher,
                              static_cast<int>(payloadSize)) != 1) break;
        if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, kTag,
               const_cast<char *>(record.data() + record.size() - kTag)) != 1) break;
        if (EVP_DecryptFinal_ex(ctx, plain.data() + count, &final_count) != 1) break;
        if (static_cast<size_t>(count + final_count) != payloadSize) break;
        if (flag == 0) {
            if (payloadSize != kBlock) break;
            std::memcpy(out.data(), plain.data(), kBlock);
        } else {
            if (!workspace.decompress(out, payloadSize)) break;
        }
        ok = true;
    } while (false);
    EVP_CIPHER_CTX_free(ctx);
    return ok;
}

// EXP1 is the original AES-256-GCM format. EXP2 uses AEGIS-256 with a
// 32-byte nonce and 32-byte tag. Authenticate its header, flag and page ID.
#ifdef SQLITE_MURMUR_HAS_AEGIS256
std::array<unsigned char, 13> aegisAAD(uint64_t page, unsigned char flag) {
    std::array<unsigned char, 13> aad{'E', 'X', 'P', '2', flag};
    for (int i = 12; i >= 5; --i) {
        aad[i] = static_cast<unsigned char>(page & 0xff);
        page >>= 8;
    }
    return aad;
}
// D2b: EXP4 extends the EXP2 AAD with the key id tail (ruling P).
std::array<unsigned char, 17> aegisAAD4(uint64_t page, unsigned char flag, uint32_t keyId) {
    std::array<unsigned char, 17> aad{'E', 'X', 'P', '4', flag};
    for (int i = 12; i >= 5; --i) {
        aad[i] = static_cast<unsigned char>(page & 0xff);
        page >>= 8;
    }
    for (int i = 0; i < 4; ++i)
        aad[13 + i] = static_cast<unsigned char>((keyId >> (8 * i)) & 0xff);
    return aad;
}
#endif

// D2b keyed-AES twins (defined below, beside decryptAES).
bool encryptAESKeyed(const unsigned char key[32], uint32_t keyId, uint64_t page,
                     const unsigned char *data, size_t n, bool compressed,
                     std::string &result);
bool decryptAESKeyed(const unsigned char key[32], uint64_t page,
                     const std::string &record, Block &out, DecodeWorkspace &workspace);

// D2b: peek the key id of a keyed (EXP3/EXP4) record. Returns true with
// id set (0 allowed; the caller rejects it) when the magic + minimum
// size check out. Returns false for legacy EXP1/EXP2 records (bounds
// checked) AND for corrupt records; callers route false through the
// legacy trial chain, which fails closed on garbage.
bool recordKeyId(const std::string &record, uint32_t &id) {
    if (record.size() < 4) return false;
    const bool exp3 = std::memcmp(record.data(), kHeader3, 4) == 0;
#ifdef SQLITE_MURMUR_HAS_AEGIS256
    const bool exp4 = std::memcmp(record.data(), kHeader4, 4) == 0;
#else
    const bool exp4 = false;
#endif
    if (!exp3 && !exp4) return false;
    constexpr size_t kMinKeyedAES = 4 + 1 + 4 + kNonce + kTag;
    if (exp3 && record.size() < kMinKeyedAES) return false;
#ifdef SQLITE_MURMUR_HAS_AEGIS256
    const size_t kMinKeyedAEGIS = 4 + 1 + 4 + aegis256_NPUBBYTES + aegis256_ABYTES_MAX;
    if (exp4 && record.size() < kMinKeyedAEGIS) return false;
#endif
    id = 0;
    for (int i = 3; i >= 0; --i)
        id = (id << 8) | static_cast<unsigned char>(record[5 + i]);
    return true;
}

// D2b: positively-identified unkeyed (EXP1/EXP2) records. Same magic +
// minimum-size checks the decrypt paths enforce, so anything this
// rejects is undecodable (fails decrypt identically with or without a
// prune) rather than silently unkeyed.
bool isLegacyRecord(const std::string &record) {
    if (record.size() < 4) return false;
    constexpr size_t kMinAES = 4 + 1 + kNonce + kTag;
    if (std::memcmp(record.data(), kHeader, 4) == 0)
        return record.size() >= kMinAES;
#ifdef SQLITE_MURMUR_HAS_AEGIS256
    const size_t kMinAEGIS = 5 + aegis256_NPUBBYTES + aegis256_ABYTES_MAX;
    if (std::memcmp(record.data(), "EXP2", 4) == 0)
        return record.size() >= kMinAEGIS;
#endif
    return false;
}

bool encrypt(Cipher cipher, const unsigned char key[32], uint64_t page,
             const unsigned char *data, size_t n, bool compressed,
             std::string &result, uint32_t keyId) {
    if (keyId != 0) {
        if (cipher == Cipher::AES256GCM)
            return encryptAESKeyed(key, keyId, page, data, n, compressed, result);
#ifdef SQLITE_MURMUR_HAS_AEGIS256
        unsigned char nonce[aegis256_NPUBBYTES];
        if (RAND_bytes(nonce, sizeof(nonce)) != 1) return false;
        const auto aad = aegisAAD4(page, compressed ? 1 : 0, keyId);
        result.assign(kHeader4, 4);
        result.push_back(compressed ? '\1' : '\0');
        for (int i = 0; i < 4; ++i)
            result.push_back(static_cast<char>((keyId >> (8 * i)) & 0xff));
        result.append(reinterpret_cast<const char *>(nonce), sizeof(nonce));
        const size_t offset = result.size();
        result.resize(offset + n + aegis256_ABYTES_MAX);
        auto *out = reinterpret_cast<unsigned char *>(&result[offset]);
        if (aegis256_encrypt(out, aegis256_ABYTES_MAX, data, n,
                             aad.data(), aad.size(), nonce, key) == 0) return true;
        result.clear();
#endif
        return false;
    }
    if (cipher == Cipher::AES256GCM)
        return encryptAES(key, page, data, n, compressed, result);
#ifdef SQLITE_MURMUR_HAS_AEGIS256
    unsigned char nonce[aegis256_NPUBBYTES];
    if (RAND_bytes(nonce, sizeof(nonce)) != 1) return false;
    const auto aad = aegisAAD(page, compressed ? 1 : 0);
    result.assign("EXP2", 4);
    result.push_back(compressed ? '\1' : '\0');
    result.append(reinterpret_cast<const char *>(nonce), sizeof(nonce));
    const size_t offset = result.size();
    result.resize(offset + n + aegis256_ABYTES_MAX);
    auto *out = reinterpret_cast<unsigned char *>(&result[offset]);
    if (aegis256_encrypt(out, aegis256_ABYTES_MAX, data, n,
                         aad.data(), aad.size(), nonce, key) == 0) return true;
    result.clear();
#endif
    return false;
}

// WP-D slice D2b: key-id record twins. EXP3/EXP4 extend EXP1/EXP2 with a
// u32le key id after the flag, covered by the AAD (ruling P). The legacy
// functions above are byte-identical and untouched; new code routes here
// by magic. Unknown magic fails closed (single-version store, no forward
// compat — ruling Q).
bool encryptAESKeyed(const unsigned char key[32], uint32_t keyId, uint64_t page,
             const unsigned char *data, size_t n, bool compressed,
             std::string &result) {
    if (keyId == 0 || n > static_cast<size_t>(std::numeric_limits<int>::max())) return false;
    std::array<unsigned char, 13> aad{};
    aad[0] = compressed ? 1 : 0;
    for (int i = 8; i >= 1; --i) {
        aad[i] = static_cast<unsigned char>(page & 0xff);
        page >>= 8;
    }
    for (int i = 0; i < 4; ++i)
        aad[9 + i] = static_cast<unsigned char>((keyId >> (8 * i)) & 0xff);
    unsigned char nonce[kNonce];
    if (RAND_bytes(nonce, sizeof(nonce)) != 1) return false;
    result.assign(kHeader3, 4);
    result.push_back(compressed ? '\1' : '\0');
    for (int i = 0; i < 4; ++i)
        result.push_back(static_cast<char>((keyId >> (8 * i)) & 0xff));
    result.append(reinterpret_cast<const char *>(nonce), sizeof(nonce));
    const size_t offset = result.size();
    result.resize(offset + n + kTag);
    auto *ctx = EVP_CIPHER_CTX_new();
    if (!ctx) return false;
    bool ok = false;
    do {
        int count = 0, final_count = 0;
        if (EVP_EncryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, nullptr, nullptr) != 1) break;
        if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, kNonce, nullptr) != 1) break;
        if (EVP_EncryptInit_ex(ctx, nullptr, nullptr, key, nonce) != 1) break;
        if (EVP_EncryptUpdate(ctx, nullptr, &count, aad.data(), aad.size()) != 1) break;
        if (EVP_EncryptUpdate(ctx, reinterpret_cast<unsigned char *>(&result[offset]),
                              &count, data, static_cast<int>(n)) != 1) break;
        if (EVP_EncryptFinal_ex(ctx, reinterpret_cast<unsigned char *>(&result[offset]) + count,
                                &final_count) != 1) break;
        if (static_cast<size_t>(count + final_count) != n) break;
        if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, kTag,
                                &result[offset + n]) != 1) break;
        ok = true;
    } while (false);
    EVP_CIPHER_CTX_free(ctx);
    if (!ok) result.clear();
    return ok;
}

bool decryptAESKeyed(const unsigned char key[32], uint64_t page,
             const std::string &record, Block &out, DecodeWorkspace &workspace) {
    if (record.size() < 4 + 1 + 4 + kNonce + kTag ||
        std::memcmp(record.data(), kHeader3, 4) != 0) return false;
    const unsigned char flag = static_cast<unsigned char>(record[4]);
    if (flag > 1) return false;
    uint32_t keyId = 0;
    for (int i = 3; i >= 0; --i) keyId = (keyId << 8) | static_cast<unsigned char>(record[5 + i]);
    const size_t payloadSize = record.size() - 4 - 1 - 4 - kNonce - kTag;
    if (payloadSize > ZSTD_compressBound(kBlock)) return false;
    std::array<unsigned char, 13> aad{};
    aad[0] = flag;
    uint64_t n = page;
    for (int i = 8; i >= 1; --i) {
        aad[i] = static_cast<unsigned char>(n & 0xff);
        n >>= 8;
    }
    for (int i = 0; i < 4; ++i)
        aad[9 + i] = static_cast<unsigned char>((keyId >> (8 * i)) & 0xff);
    const auto *nonce = reinterpret_cast<const unsigned char *>(record.data() + 9);
    const auto *cipher = reinterpret_cast<const unsigned char *>(record.data() + 9 + kNonce);
    auto &plain = workspace.plain;
    workspace.plainUsed = payloadSize + 1;
    PlainWiper wiper{workspace};
    auto *ctx = EVP_CIPHER_CTX_new();
    if (!ctx) return false;
    bool ok = false;
    do {
        int count = 0, final_count = 0;
        if (EVP_DecryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, nullptr, nullptr) != 1) break;
        if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, kNonce, nullptr) != 1) break;
        if (EVP_DecryptInit_ex(ctx, nullptr, nullptr, key, nonce) != 1) break;
        if (EVP_DecryptUpdate(ctx, nullptr, &count, aad.data(), aad.size()) != 1) break;
        if (EVP_DecryptUpdate(ctx, plain.data(), &count, cipher,
                              static_cast<int>(payloadSize)) != 1) break;
        if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, kTag,
               const_cast<char *>(record.data() + record.size() - kTag)) != 1) break;
        if (EVP_DecryptFinal_ex(ctx, plain.data() + count, &final_count) != 1) break;
        if (static_cast<size_t>(count + final_count) != payloadSize) break;
        if (flag == 0) {
            if (payloadSize != kBlock) break;
            std::memcpy(out.data(), plain.data(), kBlock);
        } else {
            if (!workspace.decompress(out, payloadSize)) break;
        }
        ok = true;
    } while (false);
    EVP_CIPHER_CTX_free(ctx);
    return ok;
}

bool decrypt(Cipher cipher, const unsigned char key[32], uint64_t page,
             const std::string &record, Block &out, DecodeWorkspace &workspace) {
    workspace.blockDirty = true;
    // D2b: magic dispatch. Unknown magic fails closed in the callee's
    // memcmp (single-version store, no forward compat — ruling Q).
    if (cipher == Cipher::AES256GCM) {
        if (record.size() >= 4 && std::memcmp(record.data(), kHeader3, 4) == 0)
            return decryptAESKeyed(key, page, record, out, workspace);
        return decryptAES(key, page, record, out, workspace);
    }
#ifdef SQLITE_MURMUR_HAS_AEGIS256
    if (record.size() >= 4 && std::memcmp(record.data(), kHeader4, 4) == 0) {
        constexpr size_t overhead4 = 9 + aegis256_NPUBBYTES + aegis256_ABYTES_MAX;
        if (record.size() < overhead4) return false;
        const auto flag4 = static_cast<unsigned char>(record[4]);
        uint32_t keyId4 = 0;
        for (int i = 3; i >= 0; --i)
            keyId4 = (keyId4 << 8) | static_cast<unsigned char>(record[5 + i]);
        const size_t payloadSize4 = record.size() - overhead4;
        if (flag4 > 1 || payloadSize4 > ZSTD_compressBound(kBlock)) return false;
        const auto aad4 = aegisAAD4(page, flag4, keyId4);
        const auto *nonce4 = reinterpret_cast<const unsigned char *>(record.data() + 9);
        const auto *data4 = nonce4 + aegis256_NPUBBYTES;
        auto &plain4 = workspace.plain;
        workspace.plainUsed = payloadSize4 + 1;
        PlainWiper wiper4{workspace};
        bool ok4 = aegis256_decrypt(plain4.data(), data4, payloadSize4 + aegis256_ABYTES_MAX,
                                    aegis256_ABYTES_MAX, aad4.data(), aad4.size(), nonce4, key) == 0;
        if (ok4) {
            if (flag4 == 0) {
                ok4 = payloadSize4 == kBlock;
                if (ok4) std::memcpy(out.data(), plain4.data(), kBlock);
            } else {
                ok4 = workspace.decompress(out, payloadSize4);
            }
        }
        return ok4;
    }
    constexpr size_t overhead = 5 + aegis256_NPUBBYTES + aegis256_ABYTES_MAX;
    if (record.size() < overhead || std::memcmp(record.data(), "EXP2", 4) != 0)
        return false;
    const auto flag = static_cast<unsigned char>(record[4]);
    const size_t payloadSize = record.size() - overhead;
    if (flag > 1 || payloadSize > ZSTD_compressBound(kBlock)) return false;
    const auto aad = aegisAAD(page, flag);
    const auto *nonce = reinterpret_cast<const unsigned char *>(record.data() + 5);
    const auto *data = nonce + aegis256_NPUBBYTES;
    auto &plain = workspace.plain;
    workspace.plainUsed = payloadSize + 1;
    PlainWiper wiper{workspace};
    bool ok = aegis256_decrypt(plain.data(), data, payloadSize + aegis256_ABYTES_MAX,
                              aegis256_ABYTES_MAX, aad.data(), aad.size(), nonce, key) == 0;
    if (ok) {
        if (flag == 0) {
            ok = payloadSize == kBlock;
            if (ok) std::memcpy(out.data(), plain.data(), kBlock);
        } else {
            ok = workspace.decompress(out, payloadSize);
        }
    }
    return ok;
#else
    return false;
#endif
}

// Detect physical cores on Linux; other platforms use the available CPU count.
unsigned physicalCoreCount() {
    static const unsigned count = [] {
        std::unordered_set<std::string> cores;
        std::error_code ec;
        const std::filesystem::path root("/sys/devices/system/cpu");
        for (const auto &entry : std::filesystem::directory_iterator(root, ec)) {
            const auto name = entry.path().filename().string();
            if (name.size() <= 3 || name.compare(0, 3, "cpu") != 0 ||
                !std::all_of(name.begin() + 3, name.end(), [](char c) { return c >= '0' && c <= '9'; })) continue;
            int package, core;
            std::ifstream packageFile(entry.path() / "topology/physical_package_id");
            std::ifstream coreFile(entry.path() / "topology/core_id");
            if (packageFile >> package && coreFile >> core)
                cores.insert(std::to_string(package) + ":" + std::to_string(core));
        }
        return std::max(1u, std::min(256u, cores.empty() ? std::thread::hardware_concurrency() : static_cast<unsigned>(cores.size())));
    }();
    return count;
}

struct EncoderWorkspace {
    DecodeWorkspace decode;
    std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> compression{nullptr, ZSTD_freeCCtx};
    std::array<unsigned char, kScratchSize> scratch{};
    void wipe() noexcept {
        decode.reset();
        OPENSSL_cleanse(scratch.data(), scratch.size());
    }
    ~EncoderWorkspace() { wipe(); }
};

// Persistent preparation workers, one reusable context/buffer set per worker.
// A single ordered coordinator publishes whole committed RocksDB batches.
class EncodingPool {
    std::mutex mu;
    std::condition_variable work, done;
    std::vector<std::thread> threads;
    std::function<void(size_t, EncoderWorkspace &)> task;
    std::atomic<size_t> next{0};
    size_t count = 0, active = 0, epoch = 0;
    bool stopping = false;
    std::exception_ptr error;
    void stop() noexcept {
        { std::lock_guard<std::mutex> guard(mu); stopping = true; work.notify_all(); }
        for (auto &thread : threads) if (thread.joinable()) thread.join();
    }
public:
    std::vector<std::unique_ptr<EncoderWorkspace>> workspaces;
    explicit EncodingPool(unsigned number) {
        try {
            for (unsigned i = 0; i < number; ++i) workspaces.push_back(std::make_unique<EncoderWorkspace>());
            for (unsigned i = 0; i < number; ++i) threads.emplace_back([this, i] {
                size_t seen = 0;
                std::unique_lock<std::mutex> lock(mu);
                for (;;) {
                    work.wait(lock, [&] { return stopping || epoch != seen; });
                    if (stopping) return;
                    seen = epoch;
                    lock.unlock();
                    for (;;) {
                        const auto index = next.fetch_add(1, std::memory_order_relaxed);
                        if (index >= count) break;
                        try { task(index, *workspaces[i]); }
                        catch (...) {
                            std::lock_guard<std::mutex> guard(mu);
                            if (!error) error = std::current_exception();
                        }
                        workspaces[i]->wipe();
                    }
                    lock.lock();
                    if (--active == 0) done.notify_one();
                }
            });
        } catch (...) { stop(); throw; }
    }
    ~EncodingPool() { stop(); }
    void run(size_t number, std::function<void(size_t, EncoderWorkspace &)> callback) {
        if (!number) return;
        std::unique_lock<std::mutex> lock(mu);
        task = std::move(callback); count = number; next = 0;
        active = threads.size(); error = nullptr; ++epoch;
        work.notify_all();
        done.wait(lock, [&] { return active == 0; });
        task = {};
        if (error) std::rethrow_exception(error);
    }
};

struct Settings {
    std::array<unsigned char, 32> key{};
    unsigned interval = 250;
    Cipher cipher = Cipher::AES256GCM;
    int compressionLevel = 3; // 0 disables compression; record flags remain self-describing.
    size_t cachePages = 16384;
    bool rocksdbSync = true;
    unsigned backgroundThreads = physicalCoreCount();
};

struct CachedPage {
    Record record;
    bool dirty = false;
    std::list<uint64_t>::iterator lru;
};

struct Store {
    Settings settings;
    std::shared_ptr<rocksdb::Cache> blockCache;
    std::unique_ptr<rocksdb::DB> db;
    std::shared_ptr<rocksdb::Statistics> statistics;
    sqlite_murmur_stats counters{};
    // Lock order: mutationMu -> flushMu (when needed) -> mu. Pool locks never
    // remain held while acquiring these locks or doing crypto/IO.
    std::mutex mutationMu;
    std::mutex mu;
    std::mutex flushMu;
    // Leaf serializing online backups: held across the whole checkpoint so
    // two same-dest backups cannot interleave check-then-remove (the loser
    // must never delete the winner's artifact). Never nested with other
    // Store locks.
    std::mutex backupMu;
    uint64_t generation = 0;
    // WP-D slice D2a keyring state. settings.key stays the registered
    // master forever; activeKey (valid iff sealed) drives page crypto once
    // the envelope exists. Pre-seal pages stay master-encrypted until a
    // flush rewrites them (lazy migration); decrypt falls back to retired
    // keys, then the master. The ring + activeKey install once under mu
    // with a generation bump: writers/flush are excluded by mutationMu /
    // flushMu, and lock-free readers either acquire mu after the install
    // (sees it) or trip the generation retry. D2b online rotation follows
    // the same install discipline.
    sqlite_murmur_keyring keyring{};
    bool keyringSealed = false;
    std::array<unsigned char, 32> activeKey{};
    std::array<unsigned char, 16> keyringStoreId{};
    // Staged KDF params (pre-seal pragma writes; archive defaults).
    std::uint8_t stagedKdf = SQLITE_MURMUR_KDF_DIRECT_HKDF;
    std::uint32_t stagedTime = 3;
    std::uint32_t stagedMemoryKib = 65536;
    std::uint8_t stagedThreads = 4;
    size_t cleanCount = 0;
    std::shared_ptr<RecordPool> recordPool = std::make_shared<RecordPool>();
    std::shared_ptr<WorkspacePool> workspacePool = std::make_shared<WorkspacePool>();
    std::shared_ptr<PagePool> pagePool;
    std::shared_ptr<JournalPool> journalPool = std::make_shared<JournalPool>();
    std::unique_ptr<EncodingPool> encoders;
    std::mutex flushRequestMu;
    std::condition_variable flushDone, writeSpace;
    uint64_t requestedFlush = 0, completedFlush = 0;
    int flushResult = SQLITE_OK;
    size_t inFlightPages = 0;
    struct FlushBatch {
        std::unordered_map<uint64_t, Record> pages;
        bool trim = false, haveSize = false;
        uint64_t trimFrom = 0, newSize = 0;
    };
    std::unique_ptr<FlushBatch> failedBatch; // Owned across retries; failure recovery needs no allocation.
#ifdef SQLITE_MURMUR_TEST_HOOKS
    size_t lruMoves = 0, evictionScans = 0;
    std::atomic<bool> testFailWrite{false};
    std::function<void(const char *)> testHook;
    void testPoint(const char *point) { if (testHook) testHook(point); }
#else
    void testPoint(const char *) {}
#endif
    std::condition_variable cv;
    std::thread worker;
    bool stopping = false;
    bool kick = false;
    bool intervalChanged = false;
    bool backgroundError = false;

    uint64_t size = 0;                        // current uncommitted view
    uint64_t lastCommittedSize = 0;
    bool sizeTouched = false;
    bool pendingSizeValid = false;
    uint64_t pendingSize = 0;
    bool txnTrim = false;
    uint64_t txnTrimAt = 0;
    bool pendingTrim = false;
    uint64_t pendingTrimAt = 0;
    std::unordered_set<uint64_t> touched;    // writes since last journal deletion
    std::unordered_map<uint64_t, Record> pending;
    std::unordered_map<uint64_t, CachedPage> cache;
    std::list<uint64_t> lru;
    std::unordered_map<const void *, int> locks;
    std::string path;

    explicit Store(Settings cfg) : settings(std::move(cfg)), pagePool(std::make_shared<PagePool>(settings.cachePages)) {}

    ~Store() {
        {
            std::lock_guard<std::mutex> guard(mu);
            stopping = true;
            cv.notify_one();
        }
        if (worker.joinable()) worker.join();
        // The coordinator drains committed copies before exiting.
        encoders.reset();
        sqlite_murmur_keyring_destroy(keyring);
        OPENSSL_cleanse(activeKey.data(), activeKey.size());
        OPENSSL_cleanse(settings.key.data(), settings.key.size());
    }

    bool start(const std::string &filename, bool create) {
        path = filename;
        rocksdb::Options options;
        options.create_if_missing = create;
        options.compression = rocksdb::kNoCompression; // encrypted values are incompressible
        statistics = rocksdb::CreateDBStatistics();
        statistics->set_stats_level(rocksdb::StatsLevel::kExceptHistogramOrTimers);
        options.statistics = statistics;
        // Keep an explicit cache handle so its capacity can change at runtime.
        blockCache = rocksdb::NewLRUCache(kRocksBlockCacheBytes);
        rocksdb::BlockBasedTableOptions tableOptions;
        tableOptions.block_cache = blockCache;
        options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(tableOptions));
        rocksdb::DB *ptr = nullptr;
        if (!rocksdb::DB::Open(options, filename, &ptr).ok()) return false;
        db.reset(ptr);
        std::string value;
        const auto status = db->Get(rocksdb::ReadOptions(), kMetaSize, &value);
        if (status.ok()) {
            try {
                size_t end = 0;
                size = std::stoull(value, &end);
                if (end != value.size()) return false;
            } catch (...) { return false; }
        } else if (!status.IsNotFound()) return false;
        value.clear();
        const auto cipherStatus = db->Get(rocksdb::ReadOptions(), kMetaCipher, &value);
        if (cipherStatus.ok()) {
            if (value == "aes256gcm") settings.cipher = Cipher::AES256GCM;
#ifdef SQLITE_MURMUR_HAS_AEGIS256
            else if (value == "aegis256") settings.cipher = Cipher::AEGIS256;
#endif
            else return false;
        } else if (!cipherStatus.IsNotFound()) return false;
        // WP-D slice D2a: open the keys.enc envelope when present. The
        // registered key becomes the KDF master; the envelope's active data
        // key replaces it for page crypto. Any failure fails closed (the
        // caller maps it to SQLITE_CANTOPEN, like a cipher mismatch).
        value.clear();
        const auto keyringStatus = db->Get(rocksdb::ReadOptions(), kMetaKeyring, &value);
        if (keyringStatus.ok()) {
            std::string storeId;
            const auto idStatus = db->Get(rocksdb::ReadOptions(), kMetaStoreId, &storeId);
            if (!idStatus.ok() || storeId.size() != keyringStoreId.size()) return false;
            // Archive parity: fresh stores seal context = store_id, so the
            // single meta key feeds both bindings (spool store.rs).
            const auto *binding = reinterpret_cast<const unsigned char *>(storeId.data());
            sqlite_murmur_keyring ring;
            const int keyringRc = sqlite_murmur_keyring_open(
                reinterpret_cast<const unsigned char *>(value.data()), value.size(),
                settings.key.data(), nullptr, binding, binding, ring);
            if (keyringRc != SQLITE_OK) {
                sqlite_murmur_keyring_destroy(ring);
                return false;
            }
            std::uint32_t activeId = 0;
            const unsigned char *active = sqlite_murmur_keyring_current(ring, activeId);
            if (!active) {
                sqlite_murmur_keyring_destroy(ring);
                return false;
            }
            // Snapshot the active key BEFORE destroying the staging ring
            // (`active` points into ring storage that destroy wipes).
            std::array<unsigned char, 32> fresh{};
            std::memcpy(fresh.data(), active, fresh.size());
            std::memcpy(keyringStoreId.data(), storeId.data(), keyringStoreId.size());
            keyring = ring;
            sqlite_murmur_keyring_destroy(ring);
            // The master stays in settings.key (permanent fallback for
            // pre-seal pages); the active data key takes over page crypto.
            activeKey = fresh;
            OPENSSL_cleanse(fresh.data(), fresh.size());
            keyringSealed = true;
        } else if (!keyringStatus.IsNotFound()) return false;
        lastCommittedSize = size;
        encoders = std::make_unique<EncodingPool>(settings.backgroundThreads);
        worker = std::thread([this] {
            std::unique_lock<std::mutex> lk(mu);
            for (;;) {
                cv.wait_for(lk, std::chrono::milliseconds(settings.interval),
                            [this] { return kick || stopping || intervalChanged; });
                if (intervalChanged && !kick && !stopping) {
                    intervalChanged = false;
                    continue;
                }
                intervalChanged = false;
                kick = false;
                const auto goal = requestedFlush;
                const bool finish = stopping;
                lk.unlock();
                const int rc = persist();
                lk.lock();
                backgroundError = rc != SQLITE_OK;
                if (goal > completedFlush) {
                    completedFlush = goal;
                    flushResult = rc;
                    flushDone.notify_all();
                }
                writeSpace.notify_all();
                if (finish) break;
            }
        });
        return true;
    }

    int setCipher(Cipher cipher) {
        std::lock_guard<std::mutex> mutation(mutationMu);
        std::lock_guard<std::mutex> serial(flushMu);
        rocksdb::WriteOptions opts;
        {
            std::lock_guard<std::mutex> guard(mu);
            if (settings.cipher == cipher) return SQLITE_OK;
            if (size != 0 || !cache.empty() || !pending.empty() ||
                std::any_of(locks.begin(), locks.end(),
                            [](const auto &entry) { return entry.second > SQLITE_LOCK_NONE; }))
                return SQLITE_BUSY;
            opts.sync = settings.rocksdbSync;
        }
        if (!db->Put(opts, kMetaCipher, cipherName(cipher)).ok()) return SQLITE_IOERR_WRITE;
        std::lock_guard<std::mutex> guard(mu);
        settings.cipher = cipher;
        ++generation;
        return SQLITE_OK;
    }

    // WP-D slice D2b: point-in-time key snapshot. decryptPage never
    // touches the live ring: read() copies under mu (a raced install
    // heals via the generation retry), while persist()/loadForMutation()
    // copy where seal/rotate are already excluded by flushMu/mutationMu.
    // RAII-wiped: every copy destroys its own key bytes.
    struct PageKeys {
        unsigned char page[32]{};
        unsigned char master[32]{};
        unsigned char active[32]{};
        uint32_t current = 0;
        bool sealed = false;
        std::vector<std::pair<uint32_t, std::array<unsigned char, 32>>> retired;
        ~PageKeys() {
            OPENSSL_cleanse(page, sizeof(page));
            OPENSSL_cleanse(master, sizeof(master));
            OPENSSL_cleanse(active, sizeof(active));
            for (auto &r : retired) OPENSSL_cleanse(r.second.data(), r.second.size());
        }
    };
    PageKeys snapshotPageKeys() const {
        PageKeys snap;
        snap.sealed = keyringSealed;
        std::memcpy(snap.master, settings.key.data(), sizeof(snap.master));
        if (keyringSealed) {
            std::memcpy(snap.active, activeKey.data(), sizeof(snap.active));
            std::memcpy(snap.page, activeKey.data(), sizeof(snap.page));
            snap.current = keyring.current;
            for (const auto &entry : keyring.keys) {
                if (entry.active) continue;
                std::array<unsigned char, 32> material{};
                std::memcpy(material.data(), entry.key, material.size());
                snap.retired.emplace_back(entry.id, material);
                OPENSSL_cleanse(material.data(), material.size());
            }
        } else {
            std::memcpy(snap.page, settings.key.data(), sizeof(snap.page));
        }
        return snap;
    }

    // WP-D slices D2a+D2b: keyed (EXP3/EXP4) records take the direct-id
    // path — unknown or zero id fails closed with NO trial backstop
    // (ruling Q; trial would mask corruption). Legacy records keep the
    // trial chain (page key, then retired ring keys, then the registered
    // master for pre-seal pages). decrypt() fully rewrites `out` on
    // success, so retries are safe; GCM fails closed on a wrong key,
    // so fallback can never misread.
    static bool decryptPage(Cipher cipher, uint64_t page, const std::string &record,
                            Block &out, DecodeWorkspace &workspace, const PageKeys &keys) {
        uint32_t id = 0;
        if (recordKeyId(record, id)) {
            if (id == 0) return false;
            const unsigned char *direct = nullptr;
            if (keys.sealed && id == keys.current) direct = keys.active;
            if (!direct) {
                for (const auto &entry : keys.retired) {
                    if (entry.first == id) {
                        direct = entry.second.data();
                        break;
                    }
                }
            }
            if (!direct) return false;
            return decrypt(cipher, direct, page, record, out, workspace);
        }
        if (decrypt(cipher, keys.page, page, record, out, workspace)) return true;
        if (!keys.sealed) return false;
        for (const auto &entry : keys.retired) {
            if (decrypt(cipher, entry.second.data(), page, record, out, workspace)) return true;
        }
        return decrypt(cipher, keys.master, page, record, out, workspace);
    }

    static std::string jsonEscape(const std::string &in) {
        static const char *hex = "0123456789abcdef";
        std::string out;
        for (char c : in) {
            switch (c) {
                case '"': out += "\\\""; break;
                case '\\': out += "\\\\"; break;
                case '\n': out += "\\n"; break;
                case '\r': out += "\\r"; break;
                case '\t': out += "\\t"; break;
                default: {
                    const auto u = static_cast<unsigned char>(c);
                    if (u < 0x20) {
                        out += "\\u00";
                        out += hex[(u >> 4) & 15];
                        out += hex[u & 15];
                    } else out += c;
                }
            }
        }
        return out;
    }

    // WP-D slice D2a keyring control (FCTL 0x535805). Lock order mirrors
    // setCipher: mutationMu -> flushMu -> mu; IO/crypto run without mu.
    int keyringControl(sqlite_murmur_keyring_args *args, bool readOnly) {
        if (!args) return SQLITE_MISUSE;
        if (args->op == SQLITE_MURMUR_KEYRING_GET) {
            std::lock_guard<std::mutex> guard(mu);
            args->sealed = keyringSealed ? 1 : 0;
            if (keyringSealed) {
                args->kdf = keyring.kdf;
                args->time = keyring.argon_time;
                args->memory_kib = keyring.argon_memory_kib;
                args->threads = keyring.argon_threads;
            } else {
                args->kdf = stagedKdf;
                args->time = stagedTime;
                args->memory_kib = stagedMemoryKib;
                args->threads = stagedThreads;
            }
            args->json = nullptr;
            return SQLITE_OK;
        }
        if (args->op == SQLITE_MURMUR_KEYRING_INVENTORY) {
            std::lock_guard<std::mutex> guard(mu);
            args->json = nullptr;
            std::string json;
            if (!keyringSealed) {
                json = "{\"sealed\":false}";
            } else {
                json = "{\"sealed\":true,";
                json += "\"seq\":" + std::to_string(keyring.seq) + ",";
                json += "\"kdf\":\"";
                json += (keyring.kdf == SQLITE_MURMUR_KDF_ARGON2ID ? "argon2id" : "direct");
                json += "\",\"kdf_time\":" + std::to_string(keyring.argon_time);
                json += ",\"kdf_memory_kib\":" + std::to_string(keyring.argon_memory_kib);
                json += ",\"kdf_threads\":" + std::to_string(keyring.argon_threads);
                json += ",\"wrap_id\":\"" + jsonEscape(keyring.wrap_id) + "\"";
                json += ",\"current\":" + std::to_string(keyring.current) + ",\"keys\":[";
                for (size_t i = 0; i < keyring.keys.size(); ++i) {
                    if (i) json += ",";
                    const auto &entry = keyring.keys[i];
                    json += "{\"id\":" + std::to_string(entry.id);
                    json += ",\"created_nanos\":" + std::to_string(entry.created_nanos);
                    json += ",\"active\":";
                    json += (entry.active ? "true" : "false");
                    json += "}";
                }
                json += "]}";
            }
            args->json = static_cast<char *>(sqlite3_malloc64(json.size() + 1));
            if (!args->json) return SQLITE_NOMEM;
            std::memcpy(args->json, json.data(), json.size() + 1);
            return SQLITE_OK;
        }
        // WP-D slice D2b: online rotation. Same lock discipline as seal
        // (mutationMu -> flushMu held throughout; mu for the install):
        // writers/flush excluded so the next id is computed + installed
        // atomically (no concurrent-rotate id reuse), lock-free readers
        // heal via the generation bump. New id returns in args->value.
        // Argon2-sealed stores fail closed: re-sealing needs the
        // passphrase, which the Store never holds (F2 register API).
        if (args->op == SQLITE_MURMUR_KEYRING_ROTATE) {
            if (readOnly) return SQLITE_READONLY;
            std::lock_guard<std::mutex> mutation(mutationMu);
            std::lock_guard<std::mutex> serial(flushMu);
            std::array<unsigned char, 32> master{};
            std::array<unsigned char, 16> storeId{};
            sqlite_murmur_keyring staged;
            bool sync = true;
            {
                std::lock_guard<std::mutex> guard(mu);
                if (!keyringSealed) return SQLITE_ERROR;
                if (keyring.kdf != SQLITE_MURMUR_KDF_DIRECT_HKDF)
                    return SQLITE_ERROR;
                master = settings.key;
                storeId = keyringStoreId;
                staged = keyring;
                sync = settings.rocksdbSync;
            }
            std::string envelope;
            std::uint32_t newId = 0;
            const int rotRc = sqlite_murmur_keyring_rotate(
                staged, storeId.data(), master.data(), nullptr, envelope, newId);
            sqlite_murmur_keyring_destroy(staged);
            if (rotRc != SQLITE_OK) {
                OPENSSL_cleanse(master.data(), master.size());
                return rotRc;
            }
            rocksdb::WriteOptions envelopeOpts;
            envelopeOpts.sync = sync;
            if (!db->Put(envelopeOpts, kMetaKeyring, envelope).ok()) {
                OPENSSL_cleanse(master.data(), master.size());
                return SQLITE_IOERR_WRITE;
            }
            {
                std::lock_guard<std::mutex> guard(mu);
                sqlite_murmur_keyring ring;
                const int openRc = sqlite_murmur_keyring_open(
                    reinterpret_cast<const unsigned char *>(envelope.data()), envelope.size(),
                    master.data(), nullptr, keyringStoreId.data(), keyringStoreId.data(), ring);
                OPENSSL_cleanse(master.data(), master.size());
                if (openRc != SQLITE_OK) {
                    sqlite_murmur_keyring_destroy(ring);
                    return SQLITE_CORRUPT;
                }
                std::uint32_t activeId = 0;
                const unsigned char *active = sqlite_murmur_keyring_current(ring, activeId);
                if (!active || activeId != newId) {
                    sqlite_murmur_keyring_destroy(ring);
                    return SQLITE_CORRUPT;
                }
                std::array<unsigned char, 32> fresh{};
                std::memcpy(fresh.data(), active, fresh.size());
                keyring = ring;
                sqlite_murmur_keyring_destroy(ring);
                activeKey = fresh;
                OPENSSL_cleanse(fresh.data(), fresh.size());
                ++generation;
            }
            args->value = newId;
            args->json = nullptr;
            return SQLITE_OK;
        }
        // WP-D slice D2b: conservative prune (ruling R). Under the rotate
        // locks (quiescent key set): scan the page key-space; EXP3/EXP4
        // ids pin keys exactly; ANY unkeyed EXP1/EXP2 record blocks all
        // pruning (it could use any key); undecodable records are
        // skipped and reported (they fail decrypt identically with or
        // without the prune, so skipping cannot strand a readable page).
        // Checkpoints need no pinning: each carries its own envelope
        // copy (ruling S). No-op prunes write nothing and skip the bump.
        if (args->op == SQLITE_MURMUR_KEYRING_PRUNE) {
            if (readOnly) return SQLITE_READONLY;
            std::lock_guard<std::mutex> mutation(mutationMu);
            std::lock_guard<std::mutex> serial(flushMu);
            {
                std::lock_guard<std::mutex> guard(mu);
                if (!keyringSealed) {
                    const std::string empty =
                        "{\"pruned\":[],\"blocked_by_unkeyed\":0,\"retained\":[],\"unreadable\":0}";
                    args->json = static_cast<char *>(sqlite3_malloc64(empty.size() + 1));
                    if (!args->json) return SQLITE_NOMEM;
                    std::memcpy(args->json, empty.data(), empty.size() + 1);
                    return SQLITE_OK;
                }
            }
            std::unordered_set<std::uint32_t> referenced;
            std::uint64_t unkeyed = 0, unreadable = 0;
            {
                std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(rocksdb::ReadOptions()));
                for (it->Seek(pageKey(0)); it->Valid(); it->Next()) {
                    const rocksdb::Slice k = it->key();
                    if (k.size() != 9 || k[0] != 'p') break;
                    const rocksdb::Slice v = it->value();
                    const std::string record(v.data(), v.size());
                    std::uint32_t id = 0;
                    if (recordKeyId(record, id)) {
                        if (id != 0) referenced.insert(id);
                        else ++unreadable;
                    } else if (isLegacyRecord(record)) {
                        ++unkeyed;
                    } else {
                        ++unreadable;
                    }
                }
                if (!it->status().ok()) return SQLITE_IOERR_READ;
            }
            std::vector<std::uint32_t> drop, retained;
            {
                std::lock_guard<std::mutex> guard(mu);
                for (const auto &entry : keyring.keys) {
                    if (entry.active || entry.id == keyring.current) {
                        retained.push_back(entry.id);
                        continue;
                    }
                    if (unkeyed == 0 &&
                        referenced.find(entry.id) == referenced.end())
                        drop.push_back(entry.id);
                    else
                        retained.push_back(entry.id);
                }
            }
            if (!drop.empty()) {
                std::array<unsigned char, 32> master{};
                std::array<unsigned char, 16> storeId{};
                sqlite_murmur_keyring staged;
                bool sync = true;
                {
                    std::lock_guard<std::mutex> guard(mu);
                    if (keyring.kdf != SQLITE_MURMUR_KDF_DIRECT_HKDF)
                        return SQLITE_ERROR; // F2: no passphrase in Store
                    master = settings.key;
                    storeId = keyringStoreId;
                    staged = keyring;
                    sync = settings.rocksdbSync;
                }
                std::string envelope;
                const int dropRc = sqlite_murmur_keyring_drop_keys(
                    staged, drop.data(), drop.size(), storeId.data(),
                    master.data(), nullptr, envelope);
                sqlite_murmur_keyring_destroy(staged);
                OPENSSL_cleanse(master.data(), master.size());
                if (dropRc != SQLITE_OK) return dropRc;
                rocksdb::WriteOptions envelopeOpts;
                envelopeOpts.sync = sync;
                if (!db->Put(envelopeOpts, kMetaKeyring, envelope).ok())
                    return SQLITE_IOERR_WRITE;
                {
                    std::lock_guard<std::mutex> guard(mu);
                    sqlite_murmur_keyring ring;
                    const int openRc = sqlite_murmur_keyring_open(
                        reinterpret_cast<const unsigned char *>(envelope.data()), envelope.size(),
                        settings.key.data(), nullptr, keyringStoreId.data(),
                        keyringStoreId.data(), ring);
                    if (openRc != SQLITE_OK) {
                        sqlite_murmur_keyring_destroy(ring);
                        return SQLITE_CORRUPT;
                    }
                    keyring = ring;
                    sqlite_murmur_keyring_destroy(ring);
                    ++generation;
                }
            }
            std::string json = "{\"pruned\":[";
            for (size_t i = 0; i < drop.size(); ++i)
                json += (i ? "," : "") + std::to_string(drop[i]);
            json += "],\"blocked_by_unkeyed\":" + std::to_string(unkeyed);
            json += ",\"retained\":[";
            for (size_t i = 0; i < retained.size(); ++i)
                json += (i ? "," : "") + std::to_string(retained[i]);
            json += "],\"unreadable\":" + std::to_string(unreadable) + "}";
            args->json = static_cast<char *>(sqlite3_malloc64(json.size() + 1));
            if (!args->json) return SQLITE_NOMEM;
            std::memcpy(args->json, json.data(), json.size() + 1);
            return SQLITE_OK;
        }
        if (args->op != SQLITE_MURMUR_KEYRING_SET) return SQLITE_MISUSE;
        if (readOnly) return SQLITE_READONLY;
        // Range gate (mirrors config.rs validate; keep the two in sync).
        bool rangeOk = false;
        switch (args->param) {
            case SQLITE_MURMUR_KEYRING_PARAM_KDF: rangeOk = args->value <= 1; break;
            case SQLITE_MURMUR_KEYRING_PARAM_TIME:
                rangeOk = args->value >= 1 && args->value <= 32;
                break;
            case SQLITE_MURMUR_KEYRING_PARAM_MEMORY:
                rangeOk = args->value >= 1024 && args->value <= 16777216;
                break;
            case SQLITE_MURMUR_KEYRING_PARAM_THREADS:
                rangeOk = args->value >= 1 && args->value <= 32;
                break;
            default: return SQLITE_MISUSE;
        }
        if (!rangeOk) return SQLITE_ERROR;
        std::lock_guard<std::mutex> mutation(mutationMu);
        std::lock_guard<std::mutex> serial(flushMu);
        std::array<unsigned char, 32> master{};
        bool doSeal = false;
        std::uint8_t sealKdf = 0;
        bool sync = true;
        {
            std::lock_guard<std::mutex> guard(mu);
            if (keyringSealed) return SQLITE_BUSY;
            // No empty-store guard (unlike setCipher): a seal is safe on a
            // live store — writers/flush are excluded by the held mutexes,
            // lock-free readers retry on the generation bump below, and
            // pre-seal pages stay readable via the master fallback until a
            // flush lazily re-encrypts them with the active key.
            switch (args->param) {
                case SQLITE_MURMUR_KEYRING_PARAM_KDF:
                    // Not staged: a failed seal leaves zero trace, and a
                    // successful one reports the header, not the staging.
                    sealKdf = static_cast<std::uint8_t>(args->value);
                    doSeal = true;
                    break;
                case SQLITE_MURMUR_KEYRING_PARAM_TIME:
                    stagedTime = static_cast<std::uint32_t>(args->value);
                    break;
                case SQLITE_MURMUR_KEYRING_PARAM_MEMORY:
                    stagedMemoryKib = static_cast<std::uint32_t>(args->value);
                    break;
                default:
                    stagedThreads = static_cast<std::uint8_t>(args->value);
                    break;
            }
            if (!doSeal) return SQLITE_OK;
            master = settings.key;
            sync = settings.rocksdbSync;
        }
        // Seal: selecting the KDF materializes the envelope immediately.
        // Persist-then-install (rotate_persisted spirit): a crash between
        // the store-id write and the envelope write is harmless (the next
        // seal reuses the id), and the in-memory install only happens after
        // both meta keys are durable.
        std::array<unsigned char, 16> storeId{};
        {
            std::string idBytes;
            const auto idStatus = db->Get(rocksdb::ReadOptions(), kMetaStoreId, &idBytes);
            if (idStatus.ok()) {
                if (idBytes.size() != storeId.size()) {
                    OPENSSL_cleanse(master.data(), master.size());
                    return SQLITE_CORRUPT;
                }
                std::memcpy(storeId.data(), idBytes.data(), storeId.size());
            } else if (idStatus.IsNotFound()) {
                if (RAND_bytes(storeId.data(), storeId.size()) != 1) {
                    OPENSSL_cleanse(master.data(), master.size());
                    return SQLITE_ERROR;
                }
                rocksdb::WriteOptions idOpts;
                idOpts.sync = sync;
                if (!db->Put(idOpts, kMetaStoreId,
                             rocksdb::Slice(reinterpret_cast<const char *>(storeId.data()),
                                            storeId.size())).ok()) {
                    OPENSSL_cleanse(master.data(), master.size());
                    return SQLITE_IOERR_WRITE;
                }
            } else {
                OPENSSL_cleanse(master.data(), master.size());
                return SQLITE_IOERR_READ;
            }
        }
        std::uint8_t kdf = 0;
        std::uint32_t time = 0, memoryKib = 0;
        std::uint8_t threads = 0;
        {
            std::lock_guard<std::mutex> guard(mu);
            kdf = sealKdf;
            time = stagedTime;
            memoryKib = stagedMemoryKib;
            threads = stagedThreads;
        }
        if (kdf == SQLITE_MURMUR_KDF_DIRECT_HKDF) {
            // Direct-HKDF has no cost params: the v3 header records zeros
            // (archive format; create() enforces it). Staged argon costs
            // only survive an argon2id seal.
            time = 0;
            memoryKib = 0;
            threads = 0;
        }
        std::string envelope;
        const int createRc = sqlite_murmur_keyring_create(
            kdf, time, memoryKib, threads, "", storeId.data(), storeId.data(),
            master.data(), nullptr, envelope);
        if (createRc != SQLITE_OK) {
            OPENSSL_cleanse(master.data(), master.size());
            return createRc;
        }
        rocksdb::WriteOptions envelopeOpts;
        envelopeOpts.sync = sync;
        if (!db->Put(envelopeOpts, kMetaKeyring, envelope).ok()) {
            OPENSSL_cleanse(master.data(), master.size());
            return SQLITE_IOERR_WRITE;
        }
        {
            std::lock_guard<std::mutex> guard(mu);
            if (keyringSealed) { // Serialized by mutationMu; never taken.
                OPENSSL_cleanse(master.data(), master.size());
                return SQLITE_BUSY;
            }
            sqlite_murmur_keyring ring;
            const int openRc = sqlite_murmur_keyring_open(
                reinterpret_cast<const unsigned char *>(envelope.data()), envelope.size(),
                master.data(), nullptr, storeId.data(), storeId.data(), ring);
            OPENSSL_cleanse(master.data(), master.size());
            if (openRc != SQLITE_OK) {
                sqlite_murmur_keyring_destroy(ring);
                return SQLITE_CORRUPT;
            }
            std::uint32_t activeId = 0;
            const unsigned char *active = sqlite_murmur_keyring_current(ring, activeId);
            if (!active) {
                sqlite_murmur_keyring_destroy(ring);
                return SQLITE_CORRUPT;
            }
            // Snapshot BEFORE destroying the staging ring (see start()).
            std::array<unsigned char, 32> fresh{};
            std::memcpy(fresh.data(), active, fresh.size());
            keyringStoreId = storeId;
            keyring = ring;
            sqlite_murmur_keyring_destroy(ring);
            activeKey = fresh;
            OPENSSL_cleanse(fresh.data(), fresh.size());
            keyringSealed = true;
            // Lock-free readers retry when the generation moves (the read
            // path re-fetches and re-decrypts, so a raced seal heals).
            ++generation;
        }
        return SQLITE_OK;
    }

    int rocksdbPragma(const char *name, const char *value, bool readOnly, char **out) {
        enum Option { Sync, WriteBufferSize, WriteBufferNumber, BlockCacheBytes, BackgroundJobs };
        Option option;
        const char *backendName = nullptr;
        uint64_t minimum = 0;
        uint64_t maximum = std::min<uint64_t>(std::numeric_limits<size_t>::max(),
                                             std::numeric_limits<sqlite3_int64>::max());
        if (sqlite3_stricmp(name, "rocksdb_sync") == 0) {
            option = Sync;
            maximum = 1;
        } else if (sqlite3_stricmp(name, "rocksdb_write_buffer_size") == 0) {
            option = WriteBufferSize;
            backendName = "write_buffer_size";
            minimum = 65536;
        } else if (sqlite3_stricmp(name, "rocksdb_max_write_buffer_number") == 0) {
            option = WriteBufferNumber;
            backendName = "max_write_buffer_number";
            minimum = 2;
            maximum = std::numeric_limits<int>::max();
        } else if (sqlite3_stricmp(name, "rocksdb_block_cache_bytes") == 0) {
            option = BlockCacheBytes;
        } else if (sqlite3_stricmp(name, "rocksdb_max_background_jobs") == 0) {
            option = BackgroundJobs;
            backendName = "max_background_jobs";
            minimum = 1;
            maximum = std::numeric_limits<int>::max();
        } else return SQLITE_NOTFOUND;

        if (value && readOnly) {
            *out = sqlite3_mprintf("cannot set %s on a read-only connection", name);
            return *out ? SQLITE_READONLY : SQLITE_NOMEM;
        }
        uint64_t number = 0;
        if (value && (!parseUnsigned(value, maximum, number) || number < minimum)) {
            *out = sqlite3_mprintf("%s requires an integer from %llu through %llu", name,
                                   static_cast<unsigned long long>(minimum),
                                   static_cast<unsigned long long>(maximum));
            return *out ? SQLITE_ERROR : SQLITE_NOMEM;
        }
        // Serialize setters and readback with each other, cipher selection and
        // application flushes. In particular, sync cannot change during a batch.
        std::lock_guard<std::mutex> serial(flushMu);
        if (value) {
            rocksdb::Status status;
            switch (option) {
            case Sync:
                settings.rocksdbSync = number != 0;
                break;
            case BlockCacheBytes:
                blockCache->SetCapacity(static_cast<size_t>(number));
                break;
            case WriteBufferSize:
            case WriteBufferNumber:
                status = db->SetOptions({{backendName, std::to_string(number)}});
                break;
            case BackgroundJobs:
                status = db->SetDBOptions({{backendName, std::to_string(number)}});
                break;
            }
            if (!status.ok()) {
                *out = sqlite3_mprintf("%s: %s", name, status.ToString().c_str());
                return *out ? (status.IsIOError() ? SQLITE_IOERR : SQLITE_ERROR) : SQLITE_NOMEM;
            }
        }
        switch (option) {
        case Sync: number = settings.rocksdbSync ? 1 : 0; break;
        case BlockCacheBytes: number = blockCache->GetCapacity(); break;
        case WriteBufferSize: number = db->GetOptions().write_buffer_size; break;
        case WriteBufferNumber: number = db->GetOptions().max_write_buffer_number; break;
        case BackgroundJobs: number = db->GetDBOptions().max_background_jobs; break;
        }
        *out = sqlite3_mprintf("%llu", static_cast<unsigned long long>(number));
        return *out ? SQLITE_OK : SQLITE_NOMEM;
    }

    int backgroundThreadsPragma(const char *value, bool readOnly, char **out) {
        uint64_t number = 0;
        if (value && readOnly) return SQLITE_READONLY;
        if (value && (!parseUnsigned(value, 256, number) || number == 0)) {
            *out = sqlite3_mprintf("background_threads requires an integer from 1 through 256");
            return *out ? SQLITE_ERROR : SQLITE_NOMEM;
        }
        try {
            std::lock_guard<std::mutex> serial(flushMu);
            if (value && number != settings.backgroundThreads) {
                auto replacement = std::make_unique<EncodingPool>(static_cast<unsigned>(number));
                encoders.swap(replacement);
                settings.backgroundThreads = static_cast<unsigned>(number);
            }
            *out = sqlite3_mprintf("%u", settings.backgroundThreads);
            return *out ? SQLITE_OK : SQLITE_NOMEM;
        } catch (const std::bad_alloc &) { return SQLITE_NOMEM; }
          catch (const std::system_error &) { return SQLITE_ERROR; }
    }

    auto newRecord() {
        auto buffer = recordPool->acquire();
        buffer->reserve(kBlock + 69);
        return buffer;
    }

    // All cache metadata methods require mu. LRU ordering remains exact.
    void touch(uint64_t id) {
        auto it = cache.find(id);
        if (it != cache.end() && it->second.lru != lru.begin()) {
            lru.splice(lru.begin(), lru, it->second.lru);
#ifdef SQLITE_MURMUR_TEST_HOOKS
            ++lruMoves;
#endif
        }
    }

    void insert(uint64_t id, Record record, bool dirty) {
        auto it = cache.find(id);
        if (it != cache.end()) {
            if (!it->second.dirty) --cleanCount;
            it->second.record = std::move(record);
            it->second.dirty = dirty;
            if (!dirty) ++cleanCount;
            touch(id);
        } else {
            lru.push_front(id);
            try { cache.emplace(id, CachedPage{std::move(record), dirty, lru.begin()}); }
            catch (...) { lru.pop_front(); throw; }
            if (!dirty) ++cleanCount;
        }
        evict();
    }

    void evict() {
        if (cleanCount == 0 || cache.size() <= settings.cachePages) return;
        auto it = lru.end();
        while (cleanCount && cache.size() > settings.cachePages && it != lru.begin()) {
            --it;
#ifdef SQLITE_MURMUR_TEST_HOOKS
            ++evictionScans;
#endif
            auto c = cache.find(*it);
            if (c != cache.end() && !c->second.dirty) {
                --cleanCount;
                cache.erase(c);
                it = lru.erase(it);
            }
        }
    }

    // mutationMu protects the logical version while crypto and IO run without mu.
    int loadForMutation(uint64_t id, DecodeWorkspace &workspace, Cipher cipher) {
        Record record;
        uint64_t currentSize;
        PageKeys keys;
        {
            std::lock_guard<std::mutex> guard(mu);
            currentSize = size;
            keys = snapshotPageKeys();
            auto it = cache.find(id);
            if (it != cache.end()) record = it->second.record;
        }
        if (id * kBlock >= currentSize) { workspace.block.fill(0); return SQLITE_OK; }
        if (!record) {
            auto buffer = newRecord();
            testPoint("mutation_get");
            const auto status = db->Get(rocksdb::ReadOptions(), pageKey(id), buffer.get());
            if (status.IsNotFound()) { workspace.block.fill(0); return SQLITE_OK; }
            if (!status.ok()) return SQLITE_IOERR_READ;
            record = Record(std::move(buffer));
        }
        testPoint("mutation_decode");
        if (record.plain) {
            workspace.block = record.plain->bytes;
            workspace.blockDirty = true;
        } else if (!decryptPage(cipher, id, *record, workspace.block, workspace, keys)) return SQLITE_IOERR_READ;
        if (currentSize - id * kBlock < kBlock)
            std::memset(workspace.block.data() + currentSize % kBlock, 0, kBlock - currentSize % kBlock);
        return SQLITE_OK;
    }

    int read(void *dst, int count, sqlite3_int64 offset) try {
        if (offset < 0 || count < 0) return SQLITE_IOERR_READ;
        if (count == 0) return SQLITE_OK;
        const uint64_t begin = static_cast<uint64_t>(offset);
        const size_t maximumBlocks = (begin % kBlock + static_cast<uint64_t>(count) + kBlock - 1) / kBlock;
        BlockVersions versions;
        versions.resize(maximumBlocks);
        WorkspacePool::Lease workspace;
        for (;;) {
            // Release stale snapshots outside the shared metadata lock.
            for (size_t i = 0; i < maximumBlocks; ++i) versions[i].record.reset();
            uint64_t epoch, avail;
            Cipher cipher;
            size_t blocks;
            PageKeys keys;
            {
                std::lock_guard<std::mutex> guard(mu);
                epoch = generation;
                cipher = settings.cipher;
                keys = snapshotPageKeys();
                avail = begin < size ? std::min<uint64_t>(count, size - begin) : 0;
                blocks = avail ? (begin % kBlock + avail + kBlock - 1) / kBlock : 0;
                for (size_t i = 0; i < blocks; ++i) {
                    auto &version = versions[i];
                    version.id = begin / kBlock + i;
                    auto it = cache.find(version.id);
                    if (it != cache.end()) version.record = it->second.record;
                }
            }
            testPoint("read_snapshot");
            int rc = SQLITE_OK;
            auto *output = static_cast<unsigned char *>(dst);
            uint64_t cursor = begin, remaining = avail;
            for (size_t i = 0; i < blocks; ++i) {
                auto &version = versions[i];
                if (!version.record) {
                    auto buffer = newRecord();
                    testPoint("read_get");
                    const auto status = db->Get(rocksdb::ReadOptions(), pageKey(version.id), buffer.get());
                    testPoint("read_got");
                    if (!status.ok() && !status.IsNotFound()) { rc = SQLITE_IOERR_READ; break; }
                    if (status.ok()) version.record = Record(std::move(buffer));
                }
                testPoint("read_decode");
                const size_t within = cursor % kBlock;
                const size_t n = static_cast<size_t>(std::min<uint64_t>(remaining, kBlock - within));
                if (version.record.plain) {
                    // The immutable snapshot pins this buffer through generation validation.
                    // Copy only the requested slice, without creating plaintext scratch.
                    std::memcpy(output, version.record.plain->bytes.data() + within, n);
                    testPoint("read_plain_copied");
                } else if (version.record) {
                    if (!workspace) workspace = workspacePool->acquire();
                    if (!decryptPage(cipher, version.id, *version.record, workspace->block, *workspace, keys)) {
                        rc = SQLITE_IOERR_READ;
                        break;
                    }
                    std::memcpy(output, workspace->block.data() + within, n);
                    workspace->wipeBlock();
                } else std::memset(output, 0, n);
                output += n;
                cursor += n;
                remaining -= n;
            }
            {
                std::lock_guard<std::mutex> guard(mu);
                if (generation != epoch) continue; // Also retry IO/authentication failures caused by a raced version.
                if (rc != SQLITE_OK) return rc;
                // Admit only authenticated current-generation results, in access order.
                for (size_t i = 0; i < blocks; ++i) {
                    auto &version = versions[i];
                    if (!version.record) continue;
                    if (cache.find(version.id) != cache.end()) touch(version.id);
                    else if (!version.record.plain) insert(version.id, version.record, false);
                }
            }
            if (avail < static_cast<uint64_t>(count)) {
                std::memset(static_cast<unsigned char *>(dst) + avail, 0, static_cast<size_t>(count - avail));
                return SQLITE_IOERR_SHORT_READ;
            }
            return SQLITE_OK;
        }
    } catch (const std::bad_alloc &) { return SQLITE_NOMEM; }

    int write(const void *src, int count, sqlite3_int64 offset) try {
        if (offset < 0 || count < 0 ||
            static_cast<uint64_t>(offset) > std::numeric_limits<uint64_t>::max() - static_cast<uint64_t>(count))
            return SQLITE_IOERR_WRITE;
        if (count == 0) return SQLITE_OK;
        std::lock_guard<std::mutex> mutation(mutationMu);
        Cipher cipher;
        {
            std::unique_lock<std::mutex> guard(mu);
            // Backpressure drains earlier committed work, never the active transaction.
            // One large transaction may exceed the cache target until it commits.
            if (!stopping && pending.size() + inFlightPages >= settings.cachePages) {
                kick = true; cv.notify_one();
                writeSpace.wait(guard, [&] {
                    return stopping || backgroundError || pending.size() + inFlightPages < settings.cachePages;
                });
            }
            if (backgroundError) return SQLITE_IOERR_WRITE;
            cipher = settings.cipher;
        }
        const auto *input = static_cast<const unsigned char *>(src);
        uint64_t cursor = static_cast<uint64_t>(offset);
        int left = count;
        const size_t blocks = (cursor % kBlock + static_cast<uint64_t>(count) + kBlock - 1) / kBlock;
        BlockVersions replacements;
        replacements.resize(blocks);
        auto workspace = workspacePool->acquire();
        for (size_t i = 0; i < blocks; ++i) {
            const uint64_t id = cursor / kBlock;
            const size_t within = cursor % kBlock;
            const size_t n = std::min<size_t>(left, kBlock - within);
            auto buffer = pagePool->acquire();
            if (within != 0 || n != kBlock) {
                const int rc = loadForMutation(id, *workspace, cipher);
                if (rc != SQLITE_OK) return rc;
                buffer->bytes = workspace->block;
                workspace->wipeBlock();
            }
            std::memcpy(buffer->bytes.data() + within, input, n);
            testPoint("write_copy");
            replacements[i] = BlockVersion{id, Record(std::move(buffer))};
            input += n;
            cursor += n;
            left -= static_cast<int>(n);
        }
        testPoint("write_publish");
        std::lock_guard<std::mutex> guard(mu);
        if (backgroundError) return SQLITE_IOERR_WRITE;
        ++generation; // Even a metadata allocation failure after partial publication invalidates readers.
        for (size_t i = 0; i < blocks; ++i) {
            touched.insert(replacements[i].id);
            insert(replacements[i].id, std::move(replacements[i].record), true);
            ++counters.vfs_block_writes;
        }
        size = std::max(size, cursor);
        counters.vfs_write_bytes += static_cast<uint64_t>(count);
        sizeTouched = true;
        if (touched.size() > settings.cachePages / 2) {
            kick = true;
            cv.notify_one();
        }
        return SQLITE_OK;
    } catch (const std::bad_alloc &) { return SQLITE_NOMEM; }

    int truncate(sqlite3_int64 length) try {
        if (length < 0) return SQLITE_IOERR_TRUNCATE;
        std::lock_guard<std::mutex> mutation(mutationMu);
        const uint64_t target = static_cast<uint64_t>(length);
        uint64_t oldSize;
        Cipher cipher;
        {
            std::lock_guard<std::mutex> guard(mu);
            if (backgroundError) return SQLITE_IOERR_TRUNCATE;
            oldSize = size;
            cipher = settings.cipher;
        }
        Record tail;
        if (target < oldSize && target % kBlock) {
            auto workspace = workspacePool->acquire();
            const int rc = loadForMutation(target / kBlock, *workspace, cipher);
            if (rc != SQLITE_OK) return rc;
            std::memset(workspace->block.data() + target % kBlock, 0, kBlock - target % kBlock);
            auto buffer = pagePool->acquire();
            buffer->bytes = workspace->block;
            workspace->wipeBlock();
            testPoint("truncate_copy");
            tail = Record(std::move(buffer));
        }
        std::lock_guard<std::mutex> guard(mu);
        if (backgroundError) return SQLITE_IOERR_TRUNCATE;
        ++generation;
        if (tail) {
            touched.insert(target / kBlock);
            insert(target / kBlock, std::move(tail), true);
        }
        if (target < oldSize) {
            const uint64_t firstGone = (target + kBlock - 1) / kBlock;
            if (!txnTrim || firstGone < txnTrimAt) txnTrimAt = firstGone;
            txnTrim = true;
            for (auto it = touched.begin(); it != touched.end();) {
                if (*it >= firstGone) it = touched.erase(it); else ++it;
            }
            for (auto it = cache.begin(); it != cache.end();) {
                if (it->first >= firstGone) {
                    if (!it->second.dirty) --cleanCount;
                    lru.erase(it->second.lru);
                    it = cache.erase(it);
                } else ++it;
            }
        }
        size = target;
        sizeTouched = true;
        return SQLITE_OK;
    } catch (const std::bad_alloc &) { return SQLITE_NOMEM; }

    int completeTransaction() try {
        std::lock_guard<std::mutex> mutation(mutationMu);
        std::lock_guard<std::mutex> guard(mu);
        // Allocate missing map nodes before publishing any part of this commit.
        std::unordered_map<uint64_t, Record> additions;
        for (uint64_t id : touched) {
            auto it = cache.find(id);
            if (it != cache.end() && (pending.find(id) == pending.end() || (txnTrim && id >= txnTrimAt)))
                additions.emplace(id, it->second.record);
        }
        const size_t needed = pending.size() + additions.size();
        if (needed > pending.bucket_count() * pending.max_load_factor()) pending.reserve(needed);
        if (txnTrim) {
            for (auto it = pending.begin(); it != pending.end();) {
                if (it->first >= txnTrimAt) it = pending.erase(it); else ++it;
            }
        }
        for (uint64_t id : touched) {
            auto c = cache.find(id);
            if (c == cache.end()) continue;
            auto p = pending.find(id);
            if (p != pending.end()) p->second = c->second.record;
            else pending.insert(additions.extract(id));
        }
        touched.clear();
        if (sizeTouched || txnTrim) {
            pendingSize = size;
            pendingSizeValid = true;
            lastCommittedSize = size;
        }
        if (txnTrim) {
            if (!pendingTrim || txnTrimAt < pendingTrimAt) pendingTrimAt = txnTrimAt;
            pendingTrim = true;
        }
        txnTrim = false;
        sizeTouched = false;
        if ((!pending.empty() || pendingSizeValid) && pending.size() + inFlightPages >= settings.cachePages) {
            kick = true;
            cv.notify_one();
        }
        return SQLITE_OK;
    } catch (const std::bad_alloc &) { return SQLITE_NOMEM; }

    // Public persistence barriers always dispatch work to the background coordinator.
    int flush() {
        std::lock_guard<std::mutex> request(flushRequestMu);
        std::unique_lock<std::mutex> guard(mu);
        const auto goal = ++requestedFlush;
        kick = true; cv.notify_one();
        flushDone.wait(guard, [&] { return completedFlush >= goal; });
        return flushResult;
    }

    int persist() try {
        std::lock_guard<std::mutex> serial(flushMu);
        // Retry the failed committed snapshot first, then drain newer commits.
        // Ownership transfers require no allocation, including on an OOM path.
        const unsigned passes = failedBatch ? 2 : 1;
        for (unsigned pass = 0; pass < passes; ++pass) {
            auto work = std::move(failedBatch);
            if (!work) {
                work = std::make_unique<FlushBatch>();
                std::lock_guard<std::mutex> guard(mu);
                if (pending.empty() && !pendingSizeValid) return SQLITE_OK;
                work->pages.swap(pending);
                inFlightPages = work->pages.size();
                work->trim = pendingTrim;
                work->trimFrom = pendingTrimAt;
                pendingTrim = false;
                work->haveSize = pendingSizeValid;
                work->newSize = pendingSize;
                pendingSizeValid = false;
            }
            std::vector<std::pair<uint64_t, Record>> cleaned;
            uint64_t payloadBytes = 0, recordBytes = 0;
            int rc = SQLITE_OK;
            try {
                testPoint("flush_snapshot");
                struct Prepared {
                    uint64_t id;
                    Record original;
                    Record encoded;
                    size_t payloadBytes = 0;
                };
                std::vector<Prepared> prepared;
                prepared.reserve(work->pages.size());
                for (const auto &entry : work->pages) prepared.push_back({entry.first, entry.second, {}, 0});
                const Cipher cipher = settings.cipher;
                const int level = settings.compressionLevel;
                // D2b: sealed stores encode EXP3/EXP4 with the current id
                // (lazy migration: rewritten pages pick up key ids while
                // pre-seal pages keep working via the trial chain). flushMu
                // excludes seal/rotate, so this snapshot is stable for the
                // whole persist and safe to share read-only across encoders.
                const PageKeys persistKeys = snapshotPageKeys();
                const uint32_t encodeKeyId =
                    persistKeys.sealed ? persistKeys.current : 0;
                std::atomic<bool> failed{false};
                encoders->run(prepared.size(), [&](size_t index, EncoderWorkspace &workspace) {
                    auto &entry = prepared[index];
                    testPoint("flush_encode");
                    const Block *block;
                    if (entry.original.plain) block = &entry.original.plain->bytes;
                    else {
                        if (!decryptPage(cipher, entry.id, *entry.original, workspace.decode.block, workspace.decode, persistKeys)) {
                            failed = true; return;
                        }
                        block = &workspace.decode.block;
                    }
                    size_t n = kBlock;
                    if (level != 0) {
                        if (!workspace.compression) {
                            workspace.compression.reset(ZSTD_createCCtx());
                            if (!workspace.compression) throw std::bad_alloc();
                        }
                        n = ZSTD_compressCCtx(workspace.compression.get(), workspace.scratch.data(), workspace.scratch.size(),
                                              block->data(), block->size(), level);
                    }
                    const bool compressed = level != 0 && !ZSTD_isError(n) && n < kBlock;
                    const auto *payload = compressed ? workspace.scratch.data() : block->data();
                    const size_t length = compressed ? n : block->size();
                    auto encoded = newRecord();
                    if (!encrypt(cipher, persistKeys.page, entry.id, payload, length, compressed, *encoded, encodeKeyId)) {
                        failed = true; return;
                    }
                    entry.payloadBytes = length;
                    entry.encoded = Record(std::move(encoded));
                });
                if (failed) rc = SQLITE_IOERR_WRITE;
                rocksdb::WriteBatch batch;
                if (work->trim) batch.DeleteRange(pageKey(work->trimFrom), kEndKey);
                cleaned.reserve(prepared.size());
                if (rc == SQLITE_OK) for (auto &entry : prepared) {
                    payloadBytes += entry.payloadBytes;
                    recordBytes += entry.encoded.encrypted->size();
                    batch.Put(pageKey(entry.id), *entry.encoded);
                    cleaned.emplace_back(entry.id, std::move(entry.encoded));
                }
                if (rc == SQLITE_OK) {
                    if (work->haveSize) batch.Put(kMetaSize, std::to_string(work->newSize));
                    rocksdb::WriteOptions options;
                    options.sync = settings.rocksdbSync;
                    options.disableWAL = false;
                    testPoint("flush_write");
#ifdef SQLITE_MURMUR_TEST_HOOKS
                    if (testFailWrite) rc = SQLITE_IOERR_WRITE;
                    else
#endif
                    if (!db->Write(options, &batch).ok()) rc = SQLITE_IOERR_WRITE;
                }
            } catch (const std::bad_alloc &) { rc = SQLITE_NOMEM; }
            if (rc != SQLITE_OK) {
                failedBatch = std::move(work);
                std::lock_guard<std::mutex> guard(mu);
                backgroundError = true;
                writeSpace.notify_all();
                return rc;
            }
            testPoint("flush_reconcile");
            {
                std::lock_guard<std::mutex> guard(mu);
                ++counters.flush_batches;
                counters.persisted_blocks += work->pages.size();
                counters.uncompressed_bytes += work->pages.size() * kBlock;
                counters.compressed_payload_bytes += payloadBytes;
                counters.encrypted_record_bytes += recordBytes;
                for (const auto &entry : cleaned) {
                    auto cached = cache.find(entry.first);
                    auto original = work->pages.find(entry.first);
                    if (cached != cache.end() && original != work->pages.end() &&
                        cached->second.record == original->second) {
                        cached->second.record = entry.second;
                        if (cached->second.dirty) ++cleanCount;
                        cached->second.dirty = false;
                    }
                }
                evict();
                backgroundError = false;
                inFlightPages = 0;
                writeSpace.notify_all();
            }
        }
        return SQLITE_OK;
    } catch (const std::bad_alloc &) { return SQLITE_NOMEM; }

    int getStats(sqlite_murmur_stats *out) {
        std::lock_guard<std::mutex> serial(flushMu);
        std::lock_guard<std::mutex> guard(mu);
        *out = counters;
        out->logical_size_bytes = lastCommittedSize;
        out->rocksdb_wal_bytes = statistics->getTickerCount(rocksdb::WAL_FILE_BYTES);
        out->rocksdb_flush_bytes = statistics->getTickerCount(rocksdb::FLUSH_WRITE_BYTES);
        out->rocksdb_compaction_bytes = statistics->getTickerCount(rocksdb::COMPACT_WRITE_BYTES);
        return SQLITE_OK;
    }

    int checkpoint() {
        const int rc = flush();
        if (rc != SQLITE_OK) return rc;
        std::lock_guard<std::mutex> serial(flushMu);
        rocksdb::FlushOptions flushOptions;
        flushOptions.wait = true;
        if (!db->Flush(flushOptions).ok()) return SQLITE_IOERR_WRITE;
        rocksdb::WaitForCompactOptions waitOptions;
        waitOptions.timeout = std::chrono::seconds(30);
        const auto status = db->WaitForCompact(waitOptions);
        if (status.IsTimedOut()) return SQLITE_BUSY;
        return status.ok() ? SQLITE_OK : SQLITE_IOERR_WRITE;
    }
};

struct MurmurFile {
    sqlite3_file base{};
    std::shared_ptr<Store> store;
    std::shared_ptr<MemFile> mem;
    std::string name;
    int lockLevel = SQLITE_LOCK_NONE;
    bool mainDb = false;
    bool readOnly = false;
    bool deleteOnClose = false;
    sqlite_murmur_extension extension{};
};

sqlite3_vfs gVfs{};
sqlite3_vfs *gParent = nullptr;
Settings gSettings;
std::mutex gMu;
std::unordered_map<std::string, std::weak_ptr<Store>> gStores;
std::unordered_map<std::string, std::shared_ptr<MemFile>> gMemFiles;
std::atomic<unsigned long long> gTempId{0};
bool gRegistered = false;
// WP-C §7a group file key (+ rotation previous). Guarded by gMu, wiped at
// unregister. Read once by the replication layer at Hosts configure time.
std::array<unsigned char, 32> gFileKey{};
std::array<unsigned char, 32> gFileKeyPrevious{};
bool gFileKeySet = false;
bool gFileKeyPreviousSet = false;

bool isJournal(const std::string &name) {
    return name.size() >= 8 && name.compare(name.size() - 8, 8, "-journal") == 0;
}

std::shared_ptr<Store> findStoreForJournal(const std::string &name) {
    if (!isJournal(name)) return {};
    const std::string base = name.substr(0, name.size() - 8);
    auto it = gStores.find(base);
    return it == gStores.end() ? nullptr : it->second.lock();
}

MurmurFile *f(sqlite3_file *p) { return reinterpret_cast<MurmurFile *>(p); }

int ioClose(sqlite3_file *p) {
    auto *file = f(p);
    if (file->mainDb && file->store) {
        std::lock_guard<std::mutex> guard(file->store->mu);
        file->store->locks.erase(file);
    }
    if (file->deleteOnClose && !file->name.empty()) {
        std::lock_guard<std::mutex> guard(gMu);
        gMemFiles.erase(file->name);
    }
    if (file->extension.destroy) file->extension.destroy(file->extension.context);
    file->~MurmurFile();
    return SQLITE_OK;
}

int ioRead(sqlite3_file *p, void *data, int n, sqlite3_int64 off) {
    auto *file = f(p);
    if (file->mainDb) return file->store->read(data, n, off);
    if (off < 0 || n < 0) return SQLITE_IOERR_READ;
    std::lock_guard<std::mutex> guard(file->mem->mu);
    auto &bytes = file->mem->bytes;
    const uint64_t pos = static_cast<uint64_t>(off);
    const size_t avail = pos < file->mem->size
        ? static_cast<size_t>(std::min<uint64_t>(n, file->mem->size - pos)) : 0;
    if (avail) std::memcpy(data, bytes.data() + pos, avail);
    if (avail < static_cast<size_t>(n)) {
        std::memset(static_cast<unsigned char *>(data) + avail, 0, n - avail);
        return SQLITE_IOERR_SHORT_READ;
    }
    return SQLITE_OK;
}

int ioWrite(sqlite3_file *p, const void *data, int n, sqlite3_int64 off) {
    auto *file = f(p);
    if (file->mainDb) return file->store->write(data, n, off);
    if (off < 0 || n < 0) return SQLITE_IOERR_WRITE;
    const uint64_t end = static_cast<uint64_t>(off) + static_cast<uint64_t>(n);
    if (end > static_cast<uint64_t>(std::numeric_limits<size_t>::max()))
        return SQLITE_FULL;
    std::lock_guard<std::mutex> guard(file->mem->mu);
    try {
        if (end > file->mem->bytes.size()) file->mem->bytes.resize(end, 0);
    } catch (...) { return SQLITE_NOMEM; }
    if (static_cast<uint64_t>(off) > file->mem->size)
        std::memset(file->mem->bytes.data() + file->mem->size, 0,
                    static_cast<size_t>(off) - file->mem->size);
    if (n) {
        std::memcpy(file->mem->bytes.data() + off, data, n);
        file->mem->dirtyEnd = std::max(file->mem->dirtyEnd, static_cast<size_t>(end));
    }
    file->mem->size = std::max(file->mem->size, static_cast<size_t>(end));
    return SQLITE_OK;
}

int ioTruncate(sqlite3_file *p, sqlite3_int64 n) {
    auto *file = f(p);
    if (file->mainDb) return file->store->truncate(n);
    if (n < 0 || static_cast<uint64_t>(n) > std::numeric_limits<size_t>::max())
        return SQLITE_IOERR_TRUNCATE;
    std::lock_guard<std::mutex> guard(file->mem->mu);
    const size_t target = static_cast<size_t>(n);
    try { if (target > file->mem->bytes.size()) file->mem->bytes.resize(target, 0); }
    catch (...) { return SQLITE_NOMEM; }
    if (target < file->mem->dirtyEnd) {
        OPENSSL_cleanse(file->mem->bytes.data() + target, file->mem->dirtyEnd - target);
        file->mem->dirtyEnd = target;
    }
    if (target > file->mem->size)
        std::memset(file->mem->bytes.data() + file->mem->size, 0, target - file->mem->size);
    file->mem->size = target;
    return SQLITE_OK;
}

int ioSync(sqlite3_file *, int) {
    // Deliberately deferred; commit completion is tracked separately.
    return SQLITE_OK;
}

int ioFileSize(sqlite3_file *p, sqlite3_int64 *out) {
    auto *file = f(p);
    if (file->mainDb) {
        std::lock_guard<std::mutex> guard(file->store->mu);
        *out = static_cast<sqlite3_int64>(file->store->size);
    } else {
        std::lock_guard<std::mutex> guard(file->mem->mu);
        *out = static_cast<sqlite3_int64>(file->mem->size);
    }
    return SQLITE_OK;
}

int ioLock(sqlite3_file *p, int level) {
    auto *file = f(p);
    if (!file->mainDb) return SQLITE_OK;
    std::lock_guard<std::mutex> guard(file->store->mu);
    if (level <= file->lockLevel) return SQLITE_OK;
    for (const auto &it : file->store->locks) {
        if (it.first == file) continue;
        const int other = it.second;
        if ((level == SQLITE_LOCK_SHARED && other >= SQLITE_LOCK_PENDING) ||
            (level >= SQLITE_LOCK_RESERVED && level <= SQLITE_LOCK_PENDING &&
             other >= SQLITE_LOCK_RESERVED) ||
            (level == SQLITE_LOCK_EXCLUSIVE && other > SQLITE_LOCK_NONE))
            return SQLITE_BUSY;
    }
    file->lockLevel = level;
    file->store->locks[file] = level;
    return SQLITE_OK;
}

int ioUnlock(sqlite3_file *p, int level) {
    auto *file = f(p);
    if (!file->mainDb) return SQLITE_OK;
    std::lock_guard<std::mutex> guard(file->store->mu);
    file->lockLevel = level;
    file->store->locks[file] = level;
    return SQLITE_OK;
}

int ioCheckReserved(sqlite3_file *p, int *out) {
    auto *file = f(p);
    *out = 0;
    if (!file->mainDb) return SQLITE_OK;
    std::lock_guard<std::mutex> guard(file->store->mu);
    for (const auto &entry : file->store->locks) {
        if (entry.second >= SQLITE_LOCK_RESERVED) { *out = 1; break; }
    }
    return SQLITE_OK;
}

int ioFileControl(sqlite3_file *p, int op, void *arg) {
    auto *file = f(p);
    if (file->mainDb && op == SQLITE_MURMUR_FCNTL_EXTENSION && arg) {
        if (file->extension.context) return SQLITE_MISUSE;
        file->extension = *static_cast<sqlite_murmur_extension *>(arg);
        return SQLITE_OK;
    }
    if (file->mainDb && op == SQLITE_MURMUR_FCNTL_FLUSH)
        return file->store->flush();
    if (file->mainDb && op == SQLITE_MURMUR_FCNTL_BACKUP && arg) {
        const int fr = file->store->flush();
        if (fr != SQLITE_OK) return fr;
        const char *cipher;
        {
            std::lock_guard<std::mutex> serial(file->store->flushMu);
            std::lock_guard<std::mutex> guard(file->store->mu);
            cipher = cipherName(file->store->settings.cipher);
        }
        std::lock_guard<std::mutex> backup(file->store->backupMu);
        return sqlite_murmur_backup_run(file->store->db.get(), cipher,
                                        static_cast<const sqlite_murmur_backup_args *>(arg), nullptr);
    }
    if (file->mainDb && op == SQLITE_MURMUR_FCNTL_COMPACT) {
        const int fr = file->store->flush();
        if (fr != SQLITE_OK) return fr;
        return sqlite_murmur_compact_run(file->store->db.get());
    }
    if (file->mainDb && op == SQLITE_MURMUR_FCNTL_KEYRING && arg) {
        return file->store->keyringControl(
            static_cast<sqlite_murmur_keyring_args *>(arg), file->readOnly);
    }
    if (file->mainDb && op == SQLITE_FCNTL_PRAGMA && arg) {
        auto **arguments = static_cast<char **>(arg);
        if (arguments[1] && sqlite3_strnicmp(arguments[1], "murmur_", 7) == 0) {
            if (file->extension.pragma)
                return file->extension.pragma(file->extension.context, arguments[1], arguments[2], &arguments[0]);
            arguments[0] = sqlite3_mprintf("load murmur-replication before using murmur PRAGMAs");
            return SQLITE_ERROR;
        }
        if (arguments[1] && sqlite3_stricmp(arguments[1], "background_threads") == 0)
            return file->store->backgroundThreadsPragma(arguments[2], file->readOnly, &arguments[0]);
        if (arguments[1] && sqlite3_stricmp(arguments[1], "compression") == 0) {
            int level = -1;
            if (arguments[2]) {
                if (file->readOnly) {
                    arguments[0] = sqlite3_mprintf("cannot set compression on a read-only connection");
                    return arguments[0] ? SQLITE_READONLY : SQLITE_NOMEM;
                }
                if (sqlite3_stricmp(arguments[2], "none") == 0) level = 0;
                else if (sqlite3_strnicmp(arguments[2], "zstd", 4) == 0) {
                    uint64_t parsed = 0;
                    if (parseUnsigned(arguments[2] + 4, ZSTD_maxCLevel(), parsed) && parsed > 0)
                        level = static_cast<int>(parsed);
                }
                if (level < 0) {
                    arguments[0] = sqlite3_mprintf("compression must be none or zstd1 through zstd%d", ZSTD_maxCLevel());
                    return arguments[0] ? SQLITE_ERROR : SQLITE_NOMEM;
                }
            }
            std::lock_guard<std::mutex> serial(file->store->flushMu);
            std::lock_guard<std::mutex> guard(file->store->mu);
            if (arguments[2]) file->store->settings.compressionLevel = level;
            level = file->store->settings.compressionLevel;
            arguments[0] = level ? sqlite3_mprintf("zstd%d", level) : sqlite3_mprintf("none");
            return arguments[0] ? SQLITE_OK : SQLITE_NOMEM;
        }
        if (arguments[1]) {
            const int rc = file->store->rocksdbPragma(arguments[1], arguments[2],
                                                     file->readOnly, &arguments[0]);
            if (rc != SQLITE_NOTFOUND) return rc;
        }
        if (arguments[1] && sqlite3_stricmp(arguments[1], "flush_interval_ms") == 0) {
            unsigned interval = 0;
            if (arguments[2]) {
                const char *value = arguments[2];
                bool valid = *value != '\0';
                for (const char *c = value; valid && *c; ++c) {
                    if (*c < '0' || *c > '9' ||
                        interval > (std::numeric_limits<unsigned>::max() - (*c - '0')) / 10)
                        valid = false;
                    else interval = interval * 10 + (*c - '0');
                }
                if (!valid) {
                    arguments[0] = sqlite3_mprintf("flush_interval_ms requires an unsigned integer");
                    return arguments[0] ? SQLITE_ERROR : SQLITE_NOMEM;
                }
                if (interval == 0) interval = 250;
            }
            std::lock_guard<std::mutex> guard(file->store->mu);
            if (arguments[2]) {
                file->store->settings.interval = interval;
                file->store->intervalChanged = true;
                file->store->cv.notify_one();
            }
            arguments[0] = sqlite3_mprintf("%u", file->store->settings.interval);
            return arguments[0] ? SQLITE_OK : SQLITE_NOMEM;
        }
        if (arguments[1] && sqlite3_stricmp(arguments[1], "cipher") == 0) {
            if (arguments[2]) {
                if (file->readOnly) {
                    arguments[0] = sqlite3_mprintf("cannot set cipher on a read-only connection");
                    return arguments[0] ? SQLITE_READONLY : SQLITE_NOMEM;
                }
                Cipher cipher;
                if (sqlite3_stricmp(arguments[2], "aes256gcm") == 0)
                    cipher = Cipher::AES256GCM;
                else if (sqlite3_stricmp(arguments[2], "aegis256") == 0) {
#ifdef SQLITE_MURMUR_HAS_AEGIS256
                    cipher = Cipher::AEGIS256;
#else
                    arguments[0] = sqlite3_mprintf("aegis256 requires a build with SQLITE_MURMUR_ENABLE_AEGIS256=ON");
                    return arguments[0] ? SQLITE_ERROR : SQLITE_NOMEM;
#endif
                } else {
                    arguments[0] = sqlite3_mprintf("cipher must be aes256gcm or aegis256");
                    return arguments[0] ? SQLITE_ERROR : SQLITE_NOMEM;
                }
                const int rc = file->store->setCipher(cipher);
                if (rc != SQLITE_OK) {
                    arguments[0] = sqlite3_mprintf(rc == SQLITE_BUSY
                        ? "cipher can only be changed on an empty database outside a transaction"
                        : "could not persist cipher setting");
                    return arguments[0] ? rc : SQLITE_NOMEM;
                }
            }
            std::lock_guard<std::mutex> guard(file->store->mu);
            arguments[0] = sqlite3_mprintf("%s", cipherName(file->store->settings.cipher));
            return arguments[0] ? SQLITE_OK : SQLITE_NOMEM;
        }
        if (arguments[1] && sqlite3_stricmp(arguments[1], "journal_mode") == 0 &&
            arguments[2] && sqlite3_stricmp(arguments[2], "delete") != 0) {
            arguments[0] = sqlite3_mprintf("sqlite-murmur requires journal_mode=DELETE");
            return SQLITE_ERROR;
        }
        if (arguments[1] && sqlite3_stricmp(arguments[1], "mmap_size") == 0) {
            arguments[0] = sqlite3_mprintf("0");
            return SQLITE_OK;
        }
    }
    if (file->mainDb && op == SQLITE_FCNTL_COMMIT_PHASETWO) {
        return file->store->completeTransaction();
    }
    if (op == SQLITE_FCNTL_MMAP_SIZE && arg) {
        *static_cast<sqlite3_int64 *>(arg) = 0;
        return SQLITE_OK;
    }
    return SQLITE_NOTFOUND;
}

int ioSectorSize(sqlite3_file *) { return static_cast<int>(kBlock); }
int ioDevice(sqlite3_file *) { return 0; }

const sqlite3_io_methods gMethods = {
    1, ioClose, ioRead, ioWrite, ioTruncate, ioSync, ioFileSize,
    ioLock, ioUnlock, ioCheckReserved, ioFileControl, ioSectorSize, ioDevice
};

int vfsOpen(sqlite3_vfs *, const char *zName, sqlite3_file *out,
            int flags, int *outFlags) {
    auto *file = new (out) MurmurFile();
    try {
        file->mainDb = (flags & SQLITE_OPEN_MAIN_DB) != 0;
        file->deleteOnClose = (flags & SQLITE_OPEN_DELETEONCLOSE) != 0;
        file->name = zName ? std::string(zName) :
            (":sqlite-murmur-temp:" + std::to_string(++gTempId));
        if (file->mainDb) {
            file->readOnly = (flags & SQLITE_OPEN_READONLY) != 0;
            std::lock_guard<std::mutex> guard(gMu);
            auto &weak = gStores[file->name];
            file->store = weak.lock();
            if (!file->store) {
                auto store = std::make_shared<Store>(gSettings);
                if (!store->start(file->name, (flags & SQLITE_OPEN_CREATE) != 0)) {
                    file->~MurmurFile();
                    return SQLITE_CANTOPEN;
                }
                weak = store;
                file->store = std::move(store);
            }
        } else {
            if (file->name.size() >= 4 &&
                file->name.compare(file->name.size() - 4, 4, "-wal") == 0) {
                file->~MurmurFile();
                return SQLITE_CANTOPEN; // WAL requires a different recovery design
            }
            std::lock_guard<std::mutex> guard(gMu);
            auto &slot = gMemFiles[file->name];
            if (!slot) {
                auto owner = (flags & SQLITE_OPEN_MAIN_JOURNAL) ? findStoreForJournal(file->name) : nullptr;
                if (owner) slot = std::shared_ptr<MemFile>(owner->journalPool->acquire());
                else slot = std::make_shared<MemFile>();
            }
            file->mem = slot;
        }
        file->base.pMethods = &gMethods;
        if (outFlags) *outFlags = flags;
        return SQLITE_OK;
    } catch (...) {
        file->~MurmurFile();
        return SQLITE_NOMEM;
    }
}

int vfsDelete(sqlite3_vfs *, const char *zName, int) {
    if (!zName) return SQLITE_IOERR_DELETE;
    const std::string name(zName);
    std::shared_ptr<Store> commit;
    std::shared_ptr<MemFile> removed;
    {
        std::lock_guard<std::mutex> guard(gMu);
        const auto it = gMemFiles.find(name);
        if (it != gMemFiles.end()) { removed = std::move(it->second); gMemFiles.erase(it); }
        commit = findStoreForJournal(name);
    }
    // Last-reference cleansing/pooling must not hold the global VFS mutex.
    removed.reset();
    if (commit) return commit->completeTransaction();
    // Never delete an open RocksDB directory via SQLite xDelete.
    return SQLITE_OK;
}

int vfsAccess(sqlite3_vfs *, const char *zName, int flags, int *out) try {
    if (!zName) { *out = 0; return SQLITE_OK; }
    const std::string name(zName);
    {
        std::lock_guard<std::mutex> guard(gMu);
        if (gMemFiles.find(name) != gMemFiles.end()) { *out = 1; return SQLITE_OK; }
        const size_t suffix = isJournal(name) ? 8 :
            (name.size() >= 4 && name.compare(name.size() - 4, 4, "-wal") == 0 ? 4 : 0);
        if (suffix) {
            // A registered main database may itself end in -journal or -wal.
            const auto main = gStores.find(name);
            if (main == gStores.end() || main->second.expired()) {
                const auto owner = gStores.find(name.substr(0, name.size() - suffix));
                if (owner != gStores.end() && !owner->second.expired()) {
                    // DELETE journals exist only in RAM, and WAL is unsupported.
                    *out = 0;
                    return SQLITE_OK;
                }
            }
        }
    }
    // Keep real filesystem checks for main databases and unowned paths, outside gMu.
    std::error_code ec;
    const bool present = std::filesystem::exists(name, ec);
    *out = !ec && present ? 1 : 0;
    if (flags == SQLITE_ACCESS_READWRITE) *out = *out && !ec;
    return SQLITE_OK;
} catch (const std::bad_alloc &) { return SQLITE_NOMEM; }

int vfsFull(sqlite3_vfs *, const char *name, int n, char *out) {
    return gParent->xFullPathname(gParent, name, n, out);
}
void *vfsDlOpen(sqlite3_vfs *, const char *z) { return gParent->xDlOpen(gParent, z); }
void vfsDlError(sqlite3_vfs *, int n, char *z) { gParent->xDlError(gParent, n, z); }
void (*vfsDlSym(sqlite3_vfs *, void *handle, const char *z))(void) {
    return gParent->xDlSym(gParent, handle, z);
}
void vfsDlClose(sqlite3_vfs *, void *handle) { gParent->xDlClose(gParent, handle); }
int vfsRandom(sqlite3_vfs *, int n, char *out) { return gParent->xRandomness(gParent, n, out); }
int vfsSleep(sqlite3_vfs *, int n) { return gParent->xSleep(gParent, n); }
int vfsTime(sqlite3_vfs *, double *out) { return gParent->xCurrentTime(gParent, out); }
int vfsError(sqlite3_vfs *, int n, char *out) {
    return gParent->xGetLastError ? gParent->xGetLastError(gParent, n, out) : 0;
}
} // namespace

extern "C" int sqlite_murmur_register(const sqlite_murmur_config *config) {
    if (!config || !config->key) return SQLITE_MISUSE;
    std::lock_guard<std::mutex> guard(gMu);
    if (gRegistered) return SQLITE_MISUSE;
#ifdef SQLITE_MURMUR_HAS_AEGIS256
    static std::once_flag aegisOnce;
    static int aegisStatus = -1;
    std::call_once(aegisOnce, [] { aegisStatus = aegis_init(); });
    if (aegisStatus < 0) return SQLITE_ERROR;
#endif
    gParent = sqlite3_vfs_find(nullptr);
    if (!gParent) return SQLITE_ERROR;
    std::copy_n(config->key, 32, gSettings.key.begin());
    gSettings.interval = config->flush_interval_ms ? config->flush_interval_ms : 250;
    gSettings.cachePages = config->cache_pages ? config->cache_pages : 16384;
    gSettings.rocksdbSync = config->rocksdb_sync != 0;
    gVfs = sqlite3_vfs{};
    gVfs.iVersion = 1;
    gVfs.szOsFile = sizeof(MurmurFile);
    gVfs.mxPathname = gParent->mxPathname;
    gVfs.zName = kVfsName;
    gVfs.pAppData = nullptr;
    gVfs.xOpen = vfsOpen;
    gVfs.xDelete = vfsDelete;
    gVfs.xAccess = vfsAccess;
    gVfs.xFullPathname = vfsFull;
    gVfs.xDlOpen = vfsDlOpen;
    gVfs.xDlError = vfsDlError;
    gVfs.xDlSym = vfsDlSym;
    gVfs.xDlClose = vfsDlClose;
    gVfs.xRandomness = vfsRandom;
    gVfs.xSleep = vfsSleep;
    gVfs.xCurrentTime = vfsTime;
    gVfs.xGetLastError = vfsError;
    const int rc = sqlite3_vfs_register(&gVfs, 0);
    gRegistered = rc == SQLITE_OK;
    return rc;
}

static int lookupStore(const char *path, std::shared_ptr<Store> &store) {
    if (!path) return SQLITE_MISUSE;
    std::lock_guard<std::mutex> guard(gMu);
    if (!gRegistered) return SQLITE_MISUSE;
    std::vector<char> full(gVfs.mxPathname + 1, 0);
    if (vfsFull(&gVfs, path, full.size(), full.data()) != SQLITE_OK)
        return SQLITE_CANTOPEN;
    auto it = gStores.find(full.data());
    if (it != gStores.end()) store = it->second.lock();
    return store ? SQLITE_OK : SQLITE_NOTFOUND;
}

extern "C" int sqlite_murmur_flush(const char *path) {
    std::shared_ptr<Store> store;
    const int rc = lookupStore(path, store);
    return rc == SQLITE_OK ? store->flush() : rc;
}

extern "C" int sqlite_murmur_get_stats(const char *path, sqlite_murmur_stats *out) {
    if (!out) return SQLITE_MISUSE;
    std::shared_ptr<Store> store;
    const int rc = lookupStore(path, store);
    return rc == SQLITE_OK ? store->getStats(out) : rc;
}

extern "C" int sqlite_murmur_checkpoint(const char *path) {
    std::shared_ptr<Store> store;
    const int rc = lookupStore(path, store);
    return rc == SQLITE_OK ? store->checkpoint() : rc;
}

extern "C" int sqlite_murmur_repair(const char *path) {
    if (!path || !*path) return SQLITE_MISUSE;
    // RepairDB would happily initialize a missing directory; a typo must
    // fail, not mkdir junk.
    std::error_code ec;
    if (!std::filesystem::is_directory(path, ec) || ec) return SQLITE_CANTOPEN;
    // Same-process live handles merge fcntl locks, so RepairDB alone would
    // not catch them: refuse here. Cross-process LOCKs fail inside RepairDB.
    std::shared_ptr<Store> store;
    if (lookupStore(path, store) == SQLITE_OK) return SQLITE_BUSY;
    // Same Options recipe as Store::start, minus create_if_missing: repair
    // never initializes a missing store.
    rocksdb::Options options;
    options.create_if_missing = false;
    options.compression = rocksdb::kNoCompression;
    auto blockCache = rocksdb::NewLRUCache(kRocksBlockCacheBytes);
    rocksdb::BlockBasedTableOptions tableOptions;
    tableOptions.block_cache = blockCache;
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(tableOptions));
    return rocksdb::RepairDB(path, options).ok() ? SQLITE_OK : SQLITE_IOERR;
}

// WP-D F3: offline key inventory. Mirrors repair's guards (missing ->
// CANTOPEN without initializing; same-process live -> BUSY), then
// temp-opens STORAGE_ONLY read-only and reuses the INVENTORY op, so the
// JSON shape stays single-sourced with murmur_key_inventory().
extern "C" int sqlite_murmur_key_inventory(const char *path, char **json_out) {
    if (!path || !*path || !json_out) return SQLITE_MISUSE;
    *json_out = nullptr;
    std::error_code ec;
    if (!std::filesystem::is_directory(path, ec) || ec) return SQLITE_CANTOPEN;
    std::shared_ptr<Store> store;
    if (lookupStore(path, store) == SQLITE_OK) return SQLITE_BUSY;
    sqlite3 *db = nullptr;
    sqlite_murmur_open_options opts{};
    opts.flags = SQLITE_MURMUR_OPEN_STORAGE_ONLY;
    const int rc = sqlite_murmur_open_v2(
        path, &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_FULLMUTEX, &opts);
    if (rc != SQLITE_OK || !db) {
        if (db) sqlite3_close(db);
        return SQLITE_CANTOPEN;
    }
    sqlite_murmur_keyring_args args{};
    args.op = SQLITE_MURMUR_KEYRING_INVENTORY;
    const int frc = sqlite3_file_control(
        db, "main", SQLITE_MURMUR_FCNTL_KEYRING, &args);
    sqlite_murmur_close(db);
    if (frc != SQLITE_OK) {
        if (args.json) sqlite3_free(args.json);
        return frc;
    }
    if (!args.json) return SQLITE_CORRUPT;
    *json_out = args.json;
    return SQLITE_OK;
}

// WP-D G2: offline full store rekey.
extern "C" int sqlite_murmur_rekey(const char *path,
                                    const unsigned char old_master[32],
                                    const unsigned char new_master[32],
                                    const sqlite_murmur_rekey_options *opts) {
    if (!path || !*path || !old_master || !new_master) return SQLITE_MISUSE;
    std::error_code ec;
    if (!std::filesystem::is_directory(path, ec) || ec) return SQLITE_CANTOPEN;
    std::shared_ptr<Store> existingStore;
    if (lookupStore(path, existingStore) == SQLITE_OK) return SQLITE_BUSY;

    rocksdb::Options options;
    options.create_if_missing = false;
    options.compression = rocksdb::kNoCompression;
    auto blockCache = rocksdb::NewLRUCache(kRocksBlockCacheBytes);
    rocksdb::BlockBasedTableOptions tableOptions;
    tableOptions.block_cache = blockCache;
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(tableOptions));

    rocksdb::DB *rawDb = nullptr;
    const auto dbStatus = rocksdb::DB::Open(options, path, &rawDb);
    if (!dbStatus.ok()) {
        if (dbStatus.IsBusy() || dbStatus.IsTryAgain()) return SQLITE_BUSY;
        return SQLITE_CANTOPEN;
    }
    std::unique_ptr<rocksdb::DB> db(rawDb);

    Cipher cipher = Cipher::AES256GCM;
    std::string cipherStr;
    if (db->Get(rocksdb::ReadOptions(), kMetaCipher, &cipherStr).ok()) {
        if (cipherStr == "aegis256") {
#ifdef SQLITE_MURMUR_HAS_AEGIS256
            cipher = Cipher::AEGIS256;
#else
            return SQLITE_ERROR;
#endif
        } else if (cipherStr != "aes256gcm") {
            return SQLITE_CORRUPT;
        }
    }

    std::string currentEnv;
    const auto envStatus = db->Get(rocksdb::ReadOptions(), kMetaKeyring, &currentEnv);
    if (!envStatus.ok()) {
        return SQLITE_ERROR; // R2: clean refusal for unsealed store
    }

    std::string storeId;
    if (!db->Get(rocksdb::ReadOptions(), kMetaStoreId, &storeId).ok() || storeId.size() != 16) {
        return SQLITE_CORRUPT;
    }
    const auto *binding = reinterpret_cast<const unsigned char *>(storeId.data());

    sqlite_murmur_keyring oldRing{};
    int keyringRc = sqlite_murmur_keyring_open(
        reinterpret_cast<const unsigned char *>(currentEnv.data()), currentEnv.size(),
        old_master, nullptr, binding, binding, oldRing);
    if (keyringRc != SQLITE_OK) {
        sqlite_murmur_keyring_destroy(oldRing);
        return keyringRc;
    }
    if (oldRing.kdf != SQLITE_MURMUR_KDF_DIRECT_HKDF) {
        sqlite_murmur_keyring_destroy(oldRing);
        return SQLITE_ERROR;
    }

    uint32_t oldCurrentId = 0;
    const unsigned char *oldCurrentRaw = sqlite_murmur_keyring_current(oldRing, oldCurrentId);
    if (!oldCurrentRaw || oldCurrentId == 0) {
        sqlite_murmur_keyring_destroy(oldRing);
        return SQLITE_CORRUPT;
    }
    Store::PageKeys oldKeys{};
    oldKeys.sealed = true;
    oldKeys.current = oldCurrentId;
    std::memcpy(oldKeys.page, oldCurrentRaw, 32);
    std::memcpy(oldKeys.active, oldCurrentRaw, 32);
    std::memcpy(oldKeys.master, old_master, 32);
    for (const auto &entry : oldRing.keys) {
        if (entry.active) continue;
        std::array<unsigned char, 32> mat{};
        std::memcpy(mat.data(), entry.key, 32);
        oldKeys.retired.emplace_back(entry.id, mat);
        OPENSSL_cleanse(mat.data(), 32);
    }

    std::string stagedEnv;
    rocksdb::WriteOptions writeOpts;
    writeOpts.sync = true;
    bool stagedExists = db->Get(rocksdb::ReadOptions(), kMetaKeyringRekey, &stagedEnv).ok() && !stagedEnv.empty();
    if (!stagedExists) {
        int createRc = sqlite_murmur_keyring_create(
            oldRing.kdf, oldRing.argon_time, oldRing.argon_memory_kib,
            oldRing.argon_threads, oldRing.wrap_id.c_str(),
            binding, binding, new_master, nullptr, stagedEnv);
        if (createRc != SQLITE_OK) {
            sqlite_murmur_keyring_destroy(oldRing);
            return createRc;
        }
        if (!db->Put(writeOpts, kMetaKeyringRekey, stagedEnv).ok()) {
            sqlite_murmur_keyring_destroy(oldRing);
            return SQLITE_IOERR_WRITE;
        }
    }

    sqlite_murmur_keyring newRing{};
    int newOpenRc = sqlite_murmur_keyring_open(
        reinterpret_cast<const unsigned char *>(stagedEnv.data()), stagedEnv.size(),
        new_master, nullptr, binding, binding, newRing);
    if (newOpenRc != SQLITE_OK) {
        sqlite_murmur_keyring_destroy(oldRing);
        sqlite_murmur_keyring_destroy(newRing);
        return SQLITE_CORRUPT;
    }
    uint32_t newActiveId = 0;
    const unsigned char *newActiveRaw = sqlite_murmur_keyring_current(newRing, newActiveId);
    if (!newActiveRaw || newActiveId == 0) {
        sqlite_murmur_keyring_destroy(oldRing);
        sqlite_murmur_keyring_destroy(newRing);
        return SQLITE_CORRUPT;
    }
    std::array<unsigned char, 32> newActiveKey{};
    std::memcpy(newActiveKey.data(), newActiveRaw, 32);

    DecodeWorkspace workspace;
    rocksdb::WriteBatch batch;
    int chunkCount = 0;
    int totalWritten = 0;

    std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> cctx{ZSTD_createCCtx(), ZSTD_freeCCtx};
    std::array<unsigned char, kScratchSize> scratch{};

    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(pageKey(0)); it->Valid(); it->Next()) {
        const rocksdb::Slice k = it->key();
        if (k.size() != 9 || k[0] != 'p') break;
        uint64_t page = 0;
        for (int i = 1; i <= 8; ++i) {
            page = (page << 8) | static_cast<unsigned char>(k[i]);
        }
        const rocksdb::Slice v = it->value();
        const std::string record(v.data(), v.size());

        Block block{};
        bool decrypted = Store::decryptPage(cipher, page, record, block, workspace, oldKeys);
        if (!decrypted) {
            decrypted = decrypt(cipher, newActiveKey.data(), page, record, block, workspace);
        }
        if (!decrypted) {
            sqlite_murmur_keyring_destroy(oldRing);
            sqlite_murmur_keyring_destroy(newRing);
            OPENSSL_cleanse(newActiveKey.data(), 32);
            return SQLITE_CORRUPT;
        }

        uint32_t origKeyId = 0;
        bool wasKeyed = recordKeyId(record, origKeyId);

        size_t n = block.size();
        if (cctx) {
            n = ZSTD_compressCCtx(cctx.get(), scratch.data(),
                                  scratch.size(), block.data(),
                                  block.size(), 3);
        }
        const bool compressed = cctx && !ZSTD_isError(n) && n < kBlock;
        const auto *payload = compressed ? scratch.data() : block.data();
        const size_t length = compressed ? n : block.size();

        std::string newRecord;
        uint32_t targetKeyId = wasKeyed ? newActiveId : 0;
        if (!encrypt(cipher, newActiveKey.data(), page, payload, length,
                     compressed, newRecord, targetKeyId)) {
            sqlite_murmur_keyring_destroy(oldRing);
            sqlite_murmur_keyring_destroy(newRing);
            OPENSSL_cleanse(newActiveKey.data(), 32);
            return SQLITE_IOERR_WRITE;
        }

        batch.Put(k, newRecord);
        // Plaintext (possibly compressed) no longer needed: batch holds
        // only the re-encrypted record. Cleanse before the next page.
        OPENSSL_cleanse(block.data(), block.size());
        OPENSSL_cleanse(scratch.data(), scratch.size());
        ++chunkCount;
        ++totalWritten;

        if (opts && opts->fault_after_pages > 0 && totalWritten >= opts->fault_after_pages) {
            db->Write(writeOpts, &batch);
            sqlite_murmur_keyring_destroy(oldRing);
            sqlite_murmur_keyring_destroy(newRing);
            OPENSSL_cleanse(newActiveKey.data(), 32);
            return SQLITE_IOERR_WRITE;
        }

        if (chunkCount >= 100) {
            if (!db->Write(writeOpts, &batch).ok()) {
                sqlite_murmur_keyring_destroy(oldRing);
                sqlite_murmur_keyring_destroy(newRing);
                OPENSSL_cleanse(newActiveKey.data(), 32);
                return SQLITE_IOERR_WRITE;
            }
            batch.Clear();
            chunkCount = 0;
        }
    }
    if (chunkCount > 0) {
        if (!db->Write(writeOpts, &batch).ok()) {
            sqlite_murmur_keyring_destroy(oldRing);
            sqlite_murmur_keyring_destroy(newRing);
            OPENSSL_cleanse(newActiveKey.data(), 32);
            return SQLITE_IOERR_WRITE;
        }
        batch.Clear();
    }

    rocksdb::WriteBatch flipBatch;
    flipBatch.Put(kMetaKeyring, stagedEnv);
    flipBatch.Delete(kMetaKeyringRekey);
    if (!db->Write(writeOpts, &flipBatch).ok()) {
        sqlite_murmur_keyring_destroy(oldRing);
        sqlite_murmur_keyring_destroy(newRing);
        OPENSSL_cleanse(newActiveKey.data(), 32);
        return SQLITE_IOERR_WRITE;
    }

    sqlite_murmur_keyring_destroy(oldRing);
    sqlite_murmur_keyring_destroy(newRing);
    OPENSSL_cleanse(newActiveKey.data(), 32);
    return SQLITE_OK;
}

extern "C" int sqlite_murmur_register_file_key(const unsigned char current[32],
                                                const unsigned char previous[32]) {
    if (!current) return SQLITE_MISUSE;
    std::lock_guard<std::mutex> guard(gMu);
    if (gFileKeySet) return SQLITE_MISUSE;
    std::copy_n(current, 32, gFileKey.begin());
    if (previous) {
        std::copy_n(previous, 32, gFileKeyPrevious.begin());
        gFileKeyPreviousSet = true;
    }
    gFileKeySet = true;
    return SQLITE_OK;
}

extern "C" int sqlite_murmur_file_key_current(unsigned char out[32]) {
    if (!out) return SQLITE_MISUSE;
    std::lock_guard<std::mutex> guard(gMu);
    if (!gFileKeySet) return SQLITE_NOTFOUND;
    std::copy_n(gFileKey.begin(), 32, out);
    return SQLITE_OK;
}

extern "C" int sqlite_murmur_file_key_previous(unsigned char out[32]) {
    if (!out) return SQLITE_MISUSE;
    std::lock_guard<std::mutex> guard(gMu);
    if (!gFileKeyPreviousSet) return SQLITE_NOTFOUND;
    std::copy_n(gFileKeyPrevious.begin(), 32, out);
    return SQLITE_OK;
}

extern "C" int sqlite_murmur_unregister(void) {
    std::lock_guard<std::mutex> guard(gMu);
    for (const auto &it : gStores) if (!it.second.expired()) return SQLITE_BUSY;
    if (!gRegistered) return SQLITE_MISUSE;
    const int rc = sqlite3_vfs_unregister(&gVfs);
    if (rc == SQLITE_OK) {
        OPENSSL_cleanse(gSettings.key.data(), gSettings.key.size());
        OPENSSL_cleanse(gFileKey.data(), gFileKey.size());
        OPENSSL_cleanse(gFileKeyPrevious.data(), gFileKeyPrevious.size());
        gFileKeySet = false;
        gFileKeyPreviousSet = false;
        gMemFiles.clear();
        gStores.clear();
        gRegistered = false;
    }
    return rc;
}
