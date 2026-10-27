#include "sqlite_express.h"

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <zstd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <filesystem>
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
constexpr char kMetaSize[] = "meta:size";
constexpr char kEndKey[] = "q";
constexpr const char *kVfsName = "sqlite-express";

using Block = std::array<unsigned char, kBlock>;

std::string pageKey(uint64_t page) {
    std::string out(9, '\0');
    out[0] = 'p';
    for (int i = 8; i >= 1; --i) {
        out[i] = static_cast<char>(page & 0xff);
        page >>= 8;
    }
    return out;
}

bool encrypt(const unsigned char key[32], uint64_t page,
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

bool decrypt(const unsigned char key[32], uint64_t page,
             const std::string &record, Block &out) {
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
    std::vector<unsigned char> plain(payloadSize + 1);
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
            if (ZSTD_decompress(out.data(), kBlock, plain.data(), payloadSize) != kBlock) break;
        }
        ok = true;
    } while (false);
    EVP_CIPHER_CTX_free(ctx);
    OPENSSL_cleanse(plain.data(), plain.size());
    return ok;
}

struct Settings {
    std::array<unsigned char, 32> key{};
    unsigned interval = 250;
    size_t cachePages = 16384;
    bool rocksdbSync = true;
};

struct CachedPage {
    std::shared_ptr<std::string> encrypted;
    bool dirty = false;
    std::list<uint64_t>::iterator lru;
};

struct Store {
    Settings settings;
    std::unique_ptr<rocksdb::DB> db;
    std::mutex mu;
    std::mutex flushMu;
    std::condition_variable cv;
    std::thread worker;
    bool stopping = false;
    bool kick = false;
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
    std::unordered_map<uint64_t, std::shared_ptr<std::string>> pending;
    std::unordered_map<uint64_t, CachedPage> cache;
    std::list<uint64_t> lru;
    std::unordered_map<const void *, int> locks;
    std::string path;

    explicit Store(Settings cfg) : settings(std::move(cfg)) {}

    ~Store() {
        {
            std::lock_guard<std::mutex> guard(mu);
            stopping = true;
            cv.notify_one();
        }
        if (worker.joinable()) worker.join();
        (void) flush();
        OPENSSL_cleanse(settings.key.data(), settings.key.size());
    }

    bool start(const std::string &filename, bool create) {
        path = filename;
        rocksdb::Options options;
        options.create_if_missing = create;
        options.compression = rocksdb::kNoCompression; // encrypted values are incompressible
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
        lastCommittedSize = size;
        worker = std::thread([this] {
            std::unique_lock<std::mutex> lk(mu);
            for (;;) {
                cv.wait_for(lk, std::chrono::milliseconds(settings.interval),
                            [this] { return kick || stopping; });
                if (stopping) break;
                kick = false;
                lk.unlock();
                (void) flush();
                lk.lock();
            }
        });
        return true;
    }

    void touch(uint64_t id) {
        auto it = cache.find(id);
        if (it != cache.end()) lru.splice(lru.begin(), lru, it->second.lru);
    }

    void insert(uint64_t id, std::shared_ptr<std::string> record, bool dirty) {
        auto it = cache.find(id);
        if (it != cache.end()) {
            it->second.encrypted = std::move(record);
            it->second.dirty = dirty;
            touch(id);
        } else {
            lru.push_front(id);
            cache.emplace(id, CachedPage{std::move(record), dirty, lru.begin()});
        }
        evict();
    }

    void evict() {
        // Never discard uncommitted or not-yet-durable bytes. The configured
        // capacity is a target for CLEAN pages; dirty pages can exceed it.
        auto it = lru.end();
        while (cache.size() > settings.cachePages && it != lru.begin()) {
            --it;
            auto c = cache.find(*it);
            if (c != cache.end() && !c->second.dirty) {
                cache.erase(c);
                it = lru.erase(it);
            }
        }
    }

    // Called with mu held. Always decrypt, even on an encrypted-cache hit.
    int load(uint64_t id, Block &plain) {
        auto hit = cache.find(id);
        if (hit != cache.end()) {
            touch(id);
            return decrypt(settings.key.data(), id, *hit->second.encrypted, plain)
                ? SQLITE_OK : SQLITE_IOERR_READ;
        }
        std::string value;
        const auto s = db->Get(rocksdb::ReadOptions(), pageKey(id), &value);
        if (s.IsNotFound()) {
            plain.fill(0);
            return SQLITE_OK;
        }
        if (!s.ok()) return SQLITE_IOERR_READ;
        if (!decrypt(settings.key.data(), id, value, plain)) return SQLITE_IOERR_READ;
        insert(id, std::make_shared<std::string>(std::move(value)), false);
        return SQLITE_OK;
    }

    int read(void *dst, int count, sqlite3_int64 offset) {
        if (offset < 0 || count < 0) return SQLITE_IOERR_READ;
        std::lock_guard<std::mutex> guard(mu);
        auto *p = static_cast<unsigned char *>(dst);
        const uint64_t begin = static_cast<uint64_t>(offset);
        const uint64_t avail = begin < size ? std::min<uint64_t>(count, size - begin) : 0;
        uint64_t cursor = begin;
        uint64_t left = avail;
        while (left) {
            const uint64_t id = cursor / kBlock;
            const size_t within = cursor % kBlock;
            const size_t n = static_cast<size_t>(std::min<uint64_t>(left, kBlock - within));
            Block block{};
            const int rc = load(id, block);
            if (rc != SQLITE_OK) return rc;
            std::memcpy(p, block.data() + within, n);
            p += n;
            cursor += n;
            left -= n;
        }
        if (avail < static_cast<uint64_t>(count)) {
            std::memset(p, 0, static_cast<size_t>(count - avail));
            return SQLITE_IOERR_SHORT_READ;
        }
        return SQLITE_OK;
    }

    int write(const void *src, int count, sqlite3_int64 offset) {
        if (offset < 0 || count < 0 ||
            static_cast<uint64_t>(offset) >
                std::numeric_limits<uint64_t>::max() - static_cast<uint64_t>(count))
            return SQLITE_IOERR_WRITE;
        std::lock_guard<std::mutex> guard(mu);
        if (backgroundError) return SQLITE_IOERR_WRITE;
        const auto *p = static_cast<const unsigned char *>(src);
        uint64_t cursor = static_cast<uint64_t>(offset);
        int left = count;
        while (left > 0) {
            const uint64_t id = cursor / kBlock;
            const size_t within = cursor % kBlock;
            const size_t n = std::min<size_t>(left, kBlock - within);
            Block block{};
            const int rc = load(id, block);
            if (rc != SQLITE_OK) return rc;
            std::memcpy(block.data() + within, p, n);
            auto record = std::make_shared<std::string>();
            if (!encrypt(settings.key.data(), id, block.data(), block.size(),
                         false, *record)) return SQLITE_IOERR_WRITE;
            insert(id, std::move(record), true);
            touched.insert(id);
            OPENSSL_cleanse(block.data(), block.size());
            p += n;
            left -= static_cast<int>(n);
            cursor += n;
        }
        size = std::max(size, cursor);
        if (count) sizeTouched = true;
        if (touched.size() > settings.cachePages / 2) {
            kick = true;
            cv.notify_one(); // only committed changes are eligible
        }
        return SQLITE_OK;
    }

    int truncate(sqlite3_int64 length) {
        if (length < 0) return SQLITE_IOERR_TRUNCATE;
        std::lock_guard<std::mutex> guard(mu);
        if (backgroundError) return SQLITE_IOERR_TRUNCATE;
        const uint64_t target = static_cast<uint64_t>(length);
        if (target < size) {
            const uint64_t firstGone = (target + kBlock - 1) / kBlock;
            if (!txnTrim || firstGone < txnTrimAt) txnTrimAt = firstGone;
            txnTrim = true;
            for (auto it = touched.begin(); it != touched.end();) {
                if (*it >= firstGone) it = touched.erase(it);
                else ++it;
            }
            for (auto it = cache.begin(); it != cache.end();) {
                if (it->first >= firstGone) {
                    lru.erase(it->second.lru);
                    it = cache.erase(it);
                } else ++it;
            }
            // A partially retained block needs a zeroed tail on regrowth.
            if (target % kBlock) {
                const uint64_t id = target / kBlock;
                Block block{};
                const int rc = load(id, block);
                if (rc != SQLITE_OK) return rc;
                std::memset(block.data() + target % kBlock, 0, kBlock - target % kBlock);
                auto record = std::make_shared<std::string>();
                if (!encrypt(settings.key.data(), id, block.data(), block.size(),
                             false, *record)) return SQLITE_IOERR_TRUNCATE;
                insert(id, std::move(record), true);
                touched.insert(id);
            }
        }
        size = target;
        sizeTouched = true;
        return SQLITE_OK;
    }

    // Journaling DELETE mode: xDelete(journal) is the commit/rollback-completion
    // boundary, after SQLite has made the main database logically consistent.
    // COMMIT_PHASETWO provides a fallback for first-time database creation.
    void completeTransaction() {
        std::lock_guard<std::mutex> guard(mu);
        // Truncation invalidates pending versions from earlier commits as well.
        // WriteBatch will delete that range before re-inserting current pages.
        if (txnTrim) {
            for (auto it = pending.begin(); it != pending.end();) {
                if (it->first >= txnTrimAt) it = pending.erase(it);
                else ++it;
            }
        }
        for (uint64_t id : touched) {
            auto it = cache.find(id);
            if (it != cache.end()) pending[id] = it->second.encrypted;
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
        if (!pending.empty() || pendingSizeValid) {
            // With a 250 ms timer, the normal flush remains coalesced.
            // The background worker also runs early under memory pressure.
            if (cache.size() > settings.cachePages * 2) {
                kick = true;
                cv.notify_one();
            }
        }
    }

    int flush() {
        std::lock_guard<std::mutex> serial(flushMu);
        std::unordered_map<uint64_t, std::shared_ptr<std::string>> pages;
        bool trim = false, haveSize = false;
        uint64_t trimFrom = 0, newSize = 0;
        {
            std::lock_guard<std::mutex> guard(mu);
            if (pending.empty() && !pendingSizeValid) return SQLITE_OK;
            pages.swap(pending);
            trim = pendingTrim;
            trimFrom = pendingTrimAt;
            pendingTrim = false;
            haveSize = pendingSizeValid;
            newSize = pendingSize;
            pendingSizeValid = false;
        }
        rocksdb::WriteBatch batch;
        if (trim) batch.DeleteRange(pageKey(trimFrom), kEndKey);
        std::vector<std::pair<uint64_t, std::shared_ptr<std::string>>> cleaned;
        cleaned.reserve(pages.size());
        bool ok = true;
        for (const auto &entry : pages) {
            Block block{};
            std::string encoded;
            if (!decrypt(settings.key.data(), entry.first, *entry.second, block)) {
                ok = false;
                break;
            }
            std::vector<unsigned char> zipped(ZSTD_compressBound(kBlock));
            size_t n = ZSTD_compress(zipped.data(), zipped.size(),
                                     block.data(), block.size(), 3);
            const bool useZip = !ZSTD_isError(n) && n < kBlock;
            const unsigned char *payload = useZip ? zipped.data() : block.data();
            const size_t length = useZip ? n : block.size();
            ok = encrypt(settings.key.data(), entry.first, payload, length, useZip, encoded);
            OPENSSL_cleanse(block.data(), block.size());
            OPENSSL_cleanse(zipped.data(), zipped.size());
            if (!ok) break;
            batch.Put(pageKey(entry.first), encoded);
            cleaned.emplace_back(entry.first,
                                 std::make_shared<std::string>(std::move(encoded)));
        }
        if (haveSize) batch.Put(kMetaSize, std::to_string(newSize));
        rocksdb::Status status;
        if (ok) {
            rocksdb::WriteOptions opts;
            opts.sync = settings.rocksdbSync;
            opts.disableWAL = false;
            status = db->Write(opts, &batch);
            ok = status.ok();
        }
        {
            std::lock_guard<std::mutex> guard(mu);
            if (ok) {
                for (const auto &entry : cleaned) {
                    auto c = cache.find(entry.first);
                    auto original = pages.find(entry.first);
                    if (c != cache.end() && original != pages.end() &&
                        c->second.encrypted == original->second) {
                        c->second.encrypted = entry.second;
                        c->second.dirty = false;
                    }
                }
                evict();
                backgroundError = false;
            } else {
                // Retain unpersisted pages, without overwriting newer commits.
                for (const auto &entry : pages) pending.emplace(entry.first, entry.second);
                if (trim) {
                    if (!pendingTrim || trimFrom < pendingTrimAt) pendingTrimAt = trimFrom;
                    pendingTrim = true;
                }
                if (haveSize && !pendingSizeValid) {
                    pendingSize = newSize;
                    pendingSizeValid = true;
                }
                backgroundError = true;
            }
        }
        return ok ? SQLITE_OK : SQLITE_IOERR_WRITE;
    }
};

struct MemFile {
    std::mutex mu;
    std::vector<unsigned char> bytes;
};
struct ExpressFile {
    sqlite3_file base{};
    std::shared_ptr<Store> store;
    std::shared_ptr<MemFile> mem;
    std::string name;
    int lockLevel = SQLITE_LOCK_NONE;
    bool mainDb = false;
    bool deleteOnClose = false;
};

sqlite3_vfs gVfs{};
sqlite3_vfs *gParent = nullptr;
Settings gSettings;
std::mutex gMu;
std::unordered_map<std::string, std::weak_ptr<Store>> gStores;
std::unordered_map<std::string, std::shared_ptr<MemFile>> gMemFiles;
std::atomic<unsigned long long> gTempId{0};
bool gRegistered = false;

bool isJournal(const std::string &name) {
    return name.size() >= 8 && name.compare(name.size() - 8, 8, "-journal") == 0;
}

std::shared_ptr<Store> findStoreForJournal(const std::string &name) {
    if (!isJournal(name)) return {};
    const std::string base = name.substr(0, name.size() - 8);
    auto it = gStores.find(base);
    return it == gStores.end() ? nullptr : it->second.lock();
}

ExpressFile *f(sqlite3_file *p) { return reinterpret_cast<ExpressFile *>(p); }

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
    file->~ExpressFile();
    return SQLITE_OK;
}

int ioRead(sqlite3_file *p, void *data, int n, sqlite3_int64 off) {
    auto *file = f(p);
    if (file->mainDb) return file->store->read(data, n, off);
    if (off < 0 || n < 0) return SQLITE_IOERR_READ;
    std::lock_guard<std::mutex> guard(file->mem->mu);
    auto &bytes = file->mem->bytes;
    const uint64_t pos = static_cast<uint64_t>(off);
    const size_t avail = pos < bytes.size()
        ? static_cast<size_t>(std::min<uint64_t>(n, bytes.size() - pos)) : 0;
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
    if (n) std::memcpy(file->mem->bytes.data() + off, data, n);
    return SQLITE_OK;
}

int ioTruncate(sqlite3_file *p, sqlite3_int64 n) {
    auto *file = f(p);
    if (file->mainDb) return file->store->truncate(n);
    if (n < 0 || static_cast<uint64_t>(n) > std::numeric_limits<size_t>::max())
        return SQLITE_IOERR_TRUNCATE;
    std::lock_guard<std::mutex> guard(file->mem->mu);
    try { file->mem->bytes.resize(static_cast<size_t>(n), 0); }
    catch (...) { return SQLITE_NOMEM; }
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
        *out = static_cast<sqlite3_int64>(file->mem->bytes.size());
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
    if (file->mainDb && op == SQLITE_FCNTL_PRAGMA && arg) {
        auto **arguments = static_cast<char **>(arg);
        if (arguments[1] && sqlite3_stricmp(arguments[1], "journal_mode") == 0 &&
            arguments[2] && sqlite3_stricmp(arguments[2], "delete") != 0) {
            arguments[0] = sqlite3_mprintf("sqlite-express requires journal_mode=DELETE");
            return SQLITE_ERROR;
        }
        if (arguments[1] && sqlite3_stricmp(arguments[1], "mmap_size") == 0) {
            arguments[0] = sqlite3_mprintf("0");
            return SQLITE_OK;
        }
    }
    if (file->mainDb && op == SQLITE_FCNTL_COMMIT_PHASETWO) {
        file->store->completeTransaction();
        return SQLITE_OK;
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
    auto *file = new (out) ExpressFile();
    try {
        file->mainDb = (flags & SQLITE_OPEN_MAIN_DB) != 0;
        file->deleteOnClose = (flags & SQLITE_OPEN_DELETEONCLOSE) != 0;
        file->name = zName ? std::string(zName) :
            (":sqlite-express-temp:" + std::to_string(++gTempId));
        if (file->mainDb) {
            std::lock_guard<std::mutex> guard(gMu);
            auto &weak = gStores[file->name];
            file->store = weak.lock();
            if (!file->store) {
                auto store = std::make_shared<Store>(gSettings);
                if (!store->start(file->name, (flags & SQLITE_OPEN_CREATE) != 0)) {
                    file->~ExpressFile();
                    return SQLITE_CANTOPEN;
                }
                weak = store;
                file->store = std::move(store);
            }
        } else {
            if (file->name.size() >= 4 &&
                file->name.compare(file->name.size() - 4, 4, "-wal") == 0) {
                file->~ExpressFile();
                return SQLITE_CANTOPEN; // WAL requires a different recovery design
            }
            std::lock_guard<std::mutex> guard(gMu);
            auto &slot = gMemFiles[file->name];
            if (!slot) slot = std::make_shared<MemFile>();
            file->mem = slot;
        }
        file->base.pMethods = &gMethods;
        if (outFlags) *outFlags = flags;
        return SQLITE_OK;
    } catch (...) {
        file->~ExpressFile();
        return SQLITE_NOMEM;
    }
}

int vfsDelete(sqlite3_vfs *, const char *zName, int) {
    if (!zName) return SQLITE_IOERR_DELETE;
    const std::string name(zName);
    std::shared_ptr<Store> commit;
    {
        std::lock_guard<std::mutex> guard(gMu);
        gMemFiles.erase(name);
        commit = findStoreForJournal(name);
    }
    if (commit) commit->completeTransaction();
    // Never delete an open RocksDB directory via SQLite xDelete.
    return SQLITE_OK;
}

int vfsAccess(sqlite3_vfs *, const char *zName, int flags, int *out) {
    if (!zName) { *out = 0; return SQLITE_OK; }
    std::lock_guard<std::mutex> guard(gMu);
    auto it = gMemFiles.find(zName);
    if (it != gMemFiles.end()) { *out = 1; return SQLITE_OK; }
    // SQLite uses EXISTS on the main database path before calling xOpen.
    std::error_code ec;
    const bool present = std::filesystem::exists(zName, ec);
    *out = !ec && present ? 1 : 0;
    if (flags == SQLITE_ACCESS_READWRITE) *out = *out && !ec;
    return SQLITE_OK;
}

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

extern "C" int sqlite_express_register(const sqlite_express_config *config) {
    if (!config || !config->key) return SQLITE_MISUSE;
    std::lock_guard<std::mutex> guard(gMu);
    if (gRegistered) return SQLITE_MISUSE;
    gParent = sqlite3_vfs_find(nullptr);
    if (!gParent) return SQLITE_ERROR;
    std::copy_n(config->key, 32, gSettings.key.begin());
    gSettings.interval = config->flush_interval_ms ? config->flush_interval_ms : 250;
    gSettings.cachePages = config->cache_pages ? config->cache_pages : 16384;
    gSettings.rocksdbSync = config->rocksdb_sync != 0;
    gVfs = sqlite3_vfs{};
    gVfs.iVersion = 1;
    gVfs.szOsFile = sizeof(ExpressFile);
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

extern "C" int sqlite_express_flush(const char *path) {
    if (!path) return SQLITE_MISUSE;
    std::string canonical;
    {
        std::lock_guard<std::mutex> guard(gMu);
        if (!gRegistered) return SQLITE_MISUSE;
        std::vector<char> full(gVfs.mxPathname + 1, 0);
        if (vfsFull(&gVfs, path, full.size(), full.data()) != SQLITE_OK)
            return SQLITE_CANTOPEN;
        canonical = full.data();
    }
    std::shared_ptr<Store> store;
    {
        std::lock_guard<std::mutex> guard(gMu);
        auto it = gStores.find(canonical);
        if (it != gStores.end()) store = it->second.lock();
    }
    return store ? store->flush() : SQLITE_NOTFOUND;
}

extern "C" int sqlite_express_unregister(void) {
    std::lock_guard<std::mutex> guard(gMu);
    for (const auto &it : gStores) if (!it.second.expired()) return SQLITE_BUSY;
    if (!gRegistered) return SQLITE_MISUSE;
    const int rc = sqlite3_vfs_unregister(&gVfs);
    if (rc == SQLITE_OK) {
        OPENSSL_cleanse(gSettings.key.data(), gSettings.key.size());
        gMemFiles.clear();
        gStores.clear();
        gRegistered = false;
    }
    return rc;
}
