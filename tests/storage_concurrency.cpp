// Compile private scheduling hooks only into this test executable, not the library.
#define SQLITE_MURMUR_TEST_HOOKS 1
#include "../src/sqlite_murmur.cpp"
#include <future>
#include <cstddef>
#include <iostream>
#include <stdexcept>

namespace {
void require(bool value, const char *message) { if (!value) throw std::runtime_error(message); }

struct Fixture {
    std::string path;
    Settings config;
    std::unique_ptr<Store> store;
    bool manualWorker;
    explicit Fixture(Cipher cipher, size_t capacity = 32, unsigned threads = 4, bool manual = true) : manualWorker(manual) {
        static std::atomic<unsigned> id{0};
        path = (std::filesystem::temp_directory_path() /
            ("sqlite-murmur-private-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
             "-" + std::to_string(++id))).string();
        config.cipher = cipher;
        config.interval = 60000;
        config.cachePages = capacity;
        config.backgroundThreads = threads;
        for (size_t i = 0; i < config.key.size(); ++i) config.key[i] = static_cast<unsigned char>(i + 17);
        reopen();
    }
    void reopen() {
        if (store) { store->testHook = {}; store.reset(); }
        store = std::make_unique<Store>(config);
        require(store->start(path, true), "start private Store");
        // Tests explicitly schedule the worker's flushes, avoiding timer races.
        if (manualWorker) {
            { std::lock_guard<std::mutex> guard(store->mu); store->stopping = true; store->cv.notify_one(); }
            store->worker.join();
        }
    }
    ~Fixture() {
        if (store) { store->testHook = {}; store.reset(); }
        std::filesystem::remove_all(path);
    }
    void checkCache() {
        std::lock_guard<std::mutex> guard(store->mu);
        const auto clean = std::count_if(store->cache.begin(), store->cache.end(), [](const auto &entry) { return !entry.second.dirty; });
        require(store->cleanCount == static_cast<size_t>(clean), "clean count matches cache contents");
        require(store->lru.size() == store->cache.size(), "LRU and map sizes match");
        for (auto it = store->lru.begin(); it != store->lru.end(); ++it) {
            auto found = store->cache.find(*it);
            require(found != store->cache.end() && found->second.lru == it, "LRU nodes match cache iterators");
        }
    }
    void write(char value, unsigned blocks = 1) {
        std::string bytes(blocks * kBlock, value);
        require(store->write(bytes.data(), static_cast<int>(bytes.size()), 0) == SQLITE_OK, "write private blocks");
    }
    void commit() { require(store->completeTransaction() == SQLITE_OK, "commit private blocks"); checkCache(); }
    void flush() { require((manualWorker ? store->persist() : store->flush()) == SQLITE_OK, "flush private blocks"); checkCache(); }
    void seed(char value, unsigned blocks = 1) { write(value, blocks); commit(); flush(); }
    std::string read(unsigned blocks = 1) {
        std::string bytes(blocks * kBlock, '?');
        require(store->read(bytes.data(), static_cast<int>(bytes.size()), 0) == SQLITE_OK, "read private blocks");
        return bytes;
    }
    void cold() {
        std::lock_guard<std::mutex> guard(store->mu);
        require(store->pending.empty() && !store->failedBatch, "cold cache needs persisted data");
        store->cache.clear(); store->lru.clear(); store->cleanCount = 0;
    }
};

struct Gate {
    std::mutex mu;
    std::condition_variable cv;
    bool entered = false, released = false;
    void pause() {
        std::unique_lock<std::mutex> guard(mu);
        entered = true; cv.notify_all();
        cv.wait(guard, [&] { return released; });
    }
    bool wait() {
        std::unique_lock<std::mutex> guard(mu);
        return cv.wait_for(guard, std::chrono::seconds(3), [&] { return entered; });
    }
    void release() { std::lock_guard<std::mutex> guard(mu); released = true; cv.notify_all(); }
};
template<class Future> bool ready(Future &future) { return future.wait_for(std::chrono::seconds(2)) == std::future_status::ready; }

void readRace(Cipher cipher, bool cold) {
    Fixture f(cipher); f.seed('A'); if (cold) f.cold();
    Gate gate;
    std::atomic<bool> once{false};
    f.store->testHook = [&](const char *point) {
        if (std::strcmp(point, cold ? "read_got" : "read_decode") == 0 && !once.exchange(true)) gate.pause();
    };
    auto reader = std::async(std::launch::async, [&] { return f.read(); });
    const bool entered = gate.wait();
    auto writer = std::async(std::launch::async, [&] { f.write('B'); f.commit(); f.flush(); });
    const bool progressed = ready(writer);
    gate.release(); writer.get();
    require(reader.get() == std::string(kBlock, 'B'), "read must retry changed snapshot, including cold Get results");
    require(entered && progressed, "slow read IO/decode must not hold shared state mutex");
    f.store->testHook = {};
    require(f.read() == std::string(kBlock, 'B'), "stale read must not poison cache");
    f.reopen(); require(f.read() == std::string(kBlock, 'B'), "new version persists");
}

void writeRace(Cipher cipher) {
    Fixture f(cipher); f.seed('A', 2);
    Gate gate; std::atomic<unsigned> calls{0};
    f.store->testHook = [&](const char *point) {
        if (std::strcmp(point, "write_copy") == 0 && ++calls == 2) gate.pause();
    };
    auto writer = std::async(std::launch::async, [&] { f.write('B', 2); });
    const bool entered = gate.wait();
    auto reader = std::async(std::launch::async, [&] { return f.read(2); });
    const bool progressed = ready(reader);
    gate.release(); writer.get();
    require(reader.get() == std::string(2 * kBlock, 'A'), "no partially prepared multi-block write is visible");
    require(entered && progressed, "copying a private write buffer must not hold shared state mutex");
    f.store->testHook = {};
    require(f.read(2) == std::string(2 * kBlock, 'B'), "prepared write published in full");
    f.commit(); f.flush(); f.reopen();
    require(f.read(2) == std::string(2 * kBlock, 'B'), "multi-block write persists");
}

void partialRace(Cipher cipher) {
    Fixture f(cipher); f.seed('A');
    Gate gate; std::atomic<bool> once{false};
    f.store->testHook = [&](const char *point) {
        if (std::strcmp(point, "write_copy") == 0 && !once.exchange(true)) gate.pause();
    };
    const char x = 'X', y = 'Y';
    auto first = std::async(std::launch::async, [&] { return f.store->write(&x, 1, 5); });
    const bool entered = gate.wait();
    auto second = std::async(std::launch::async, [&] { return f.store->write(&y, 1, 6); });
    const bool serialized = second.wait_for(std::chrono::milliseconds(50)) == std::future_status::timeout;
    auto reader = std::async(std::launch::async, [&] { return f.read(); });
    const bool progressed = ready(reader);
    const auto previous = progressed ? reader.get() : std::string();
    gate.release();
    require(first.get() == SQLITE_OK && second.get() == SQLITE_OK, "partial writers finish");
    if (!progressed) reader.get();
    require(entered && serialized && progressed && previous == std::string(kBlock, 'A'),
            "writer gate serializes mutations without blocking readers");
    f.store->testHook = {};
    std::string expected(kBlock, 'A'); expected[5] = 'X'; expected[6] = 'Y';
    require(f.read() == expected, "partial writes preserve each other's updates");
    f.commit(); f.flush(); f.reopen(); require(f.read() == expected, "partial updates persist");
}

void truncateRace(Cipher cipher) {
    Fixture f(cipher); f.seed('A', 2); f.cold();
    Gate gate; std::atomic<bool> once{false};
    f.store->testHook = [&](const char *point) {
        if (std::strcmp(point, "read_got") == 0 && !once.exchange(true)) gate.pause();
    };
    std::string output(2 * kBlock, '?');
    auto reader = std::async(std::launch::async, [&] { return f.store->read(output.data(), output.size(), 0); });
    const bool entered = gate.wait();
    auto truncator = std::async(std::launch::async, [&] {
        require(f.store->truncate(kBlock + 17) == SQLITE_OK, "partial-block truncate"); f.commit(); f.flush();
    });
    const bool progressed = ready(truncator);
    gate.release(); truncator.get();
    require(reader.get() == SQLITE_IOERR_SHORT_READ, "read retries size after truncate");
    require(entered && progressed, "truncate proceeds during unlocked read IO");
    require(output == std::string(kBlock + 17, 'A') + std::string(kBlock - 17, '\0'), "truncated read zero fills tail");
    f.store->testHook = {};
    f.reopen();
    require(f.store->size == kBlock + 17 && f.store->cleanCount == 0, "truncation and clean accounting survive reopen");
}

void flushRace(Cipher cipher, bool fail, bool shrink) {
    Fixture f(cipher); f.seed('A', 2); f.write('B', 2); f.commit();
    Gate gate; std::atomic<bool> once{false};
    f.store->testHook = [&](const char *point) {
        if (std::strcmp(point, "flush_write") == 0 && !once.exchange(true)) {
            gate.pause();
            if (fail) throw std::bad_alloc();
        }
    };
    auto flusher = std::async(std::launch::async, [&] { return f.store->persist(); });
    const bool entered = gate.wait();
    auto writer = std::async(std::launch::async, [&] {
        if (shrink) require(f.store->truncate(kBlock) == SQLITE_OK, "concurrent shrink");
        else f.write('C', 2);
        f.commit();
    });
    const bool progressed = ready(writer);
    gate.release(); writer.get();
    require(flusher.get() == (fail ? SQLITE_NOMEM : SQLITE_OK), "flush result matches injected allocation failure");
    require(entered && progressed, "RocksDB write phase must not hold shared cache mutex");
    f.store->testHook = {};
    if (fail) require(f.store->failedBatch && f.store->backgroundError, "failed snapshot retains committed pages");
    else require(f.store->cache.at(0).dirty, "old flush cannot mark newer page version clean");
    f.flush();
    require(!f.store->failedBatch && !f.store->backgroundError, "retry drains failed and newer committed snapshots");
    for (const auto &workspace : f.store->encoders->workspaces)
        require(std::all_of(workspace->scratch.begin(), workspace->scratch.end(), [](auto b) { return b == 0; }),
                "each worker's compressed plaintext scratch is wiped");
    f.reopen();
    require(f.store->size == (shrink ? kBlock : 2 * kBlock), "newer size wins after retry");
    require(f.read(shrink ? 1 : 2) == std::string(shrink ? kBlock : 2 * kBlock, shrink ? 'B' : 'C'),
            "newer committed data wins after old snapshot persistence");
}

void failureTests(Cipher cipher) {
    Fixture f(cipher); f.seed('A', 2);
    std::atomic<unsigned> calls{0};
    f.store->testHook = [&](const char *point) {
        if (std::strcmp(point, "write_copy") == 0 && ++calls == 2) throw std::bad_alloc();
    };
    std::string replacement(2 * kBlock, 'B');
    require(f.store->write(replacement.data(), replacement.size(), 0) == SQLITE_NOMEM, "write allocation errors are observable");
    f.store->testHook = {};
    require(f.read(2) == std::string(2 * kBlock, 'A'), "failed write preparation leaves old data intact");
    f.write('B', 2); f.commit();
    f.store->testFailWrite = true;
    require(f.store->persist() == SQLITE_IOERR_WRITE && f.store->failedBatch, "RocksDB failure retains committed snapshot");
    f.store->testFailWrite = false;
    f.flush(); f.reopen(); require(f.read(2) == replacement, "IO failure retry persists committed data");
}

void reuseTests(Cipher cipher) {
    Fixture f(cipher); f.seed('A');
    require(std::any_of(f.store->encoders->workspaces.begin(), f.store->encoders->workspaces.end(),
                        [](const auto &workspace) { return workspace->compression != nullptr; }),
            "background workers create persistent compression contexts");
    Record old = f.store->cache.at(0).record;
    for (int level : {0, 1, 3, 6, 22}) {
        { std::lock_guard<std::mutex> guard(f.store->flushMu); f.store->settings.compressionLevel = level; }
        f.write(static_cast<char>('B' + level)); f.commit(); f.flush(); f.cold();
        require(f.read() == std::string(kBlock, static_cast<char>('B' + level)), "context produces independent frames across level changes");

    }
    DecodeWorkspace scratch;
    Block oldPlain{};
    require(decrypt(cipher, f.config.key.data(), 0, *old, oldPlain, scratch) && std::all_of(oldPlain.begin(), oldPlain.end(), [](auto c) { return c == 'A'; }),
            "published old version stays immutable while buffers are recycled");
    auto wrongKey = f.config.key; wrongKey[0] ^= 1;
    require(!decrypt(cipher, wrongKey.data(), 0, *old, oldPlain, scratch), "wrong key rejected with reused workspace");
    require(std::all_of(scratch.plain.begin(), scratch.plain.end(), [](auto c) { return c == 0; }), "failed decode wipes scratch");
    std::weak_ptr<RecordPool> pool = f.store->recordPool;
    f.store.reset();
    require(!pool.expired(), "snapshot keeps its return pool alive beyond Store lifetime");
    old.reset();
    require(pool.expired(), "pool releases after last immutable snapshot");
}

void poolAndLRUTests(Cipher cipher) {
    Fixture f(cipher, 3);
    const auto add = [&](uint64_t id, bool dirty) {
        auto lease = f.store->newRecord(); lease->assign(100, 'x'); Record record(std::move(lease));
        std::lock_guard<std::mutex> guard(f.store->mu); f.store->insert(id, std::move(record), dirty);
    };
    add(1, false); add(2, false); add(3, false);
    f.store->touch(3); require(f.store->lruMoves == 0, "MRU hit avoids redundant list update");
    f.store->touch(1); add(4, false);
    require(f.store->cache.count(2) == 0 && f.store->lru == std::list<uint64_t>({4, 1, 3}), "exact LRU order preserved");
    require(f.store->cleanCount == 3, "clean entry accounting");
    f.store->cache.clear(); f.store->lru.clear(); f.store->cleanCount = 0;
    const auto scans = f.store->evictionScans;
    for (unsigned i = 0; i < 20; ++i) add(i, true);
    require(f.store->evictionScans == scans && f.store->cleanCount == 0, "all-dirty caches do not repeatedly scan LRU");
    add(25, false);
    require(f.store->cleanCount == 0 && f.store->cache.count(25) == 0, "dirty pages protected while clean page evicts");

    std::vector<Record> records;
    for (int i = 0; i < 160; ++i) { auto lease = f.store->newRecord(); records.emplace_back(std::move(lease)); }
    records.clear();
    require(f.store->recordPool->idle.size() == 128, "encrypted buffer pool bounded at 128");
    const auto created = f.store->recordPool->created.load();
    for (int i = 0; i < 100; ++i) { auto lease = f.store->newRecord(); lease->assign(100, 'x'); }
    require(f.store->recordPool->created == created && f.store->recordPool->reused > 0, "encrypted buffer capacity reused");
    std::vector<WorkspacePool::Lease> workspaces;
    for (int i = 0; i < 12; ++i) {
        auto lease = f.store->workspacePool->acquire();
        lease->plain.fill('P'); lease->block.fill('P');
        lease->plainUsed = lease->plain.size(); lease->blockDirty = true;
        workspaces.push_back(std::move(lease));
    }
    workspaces.clear();
    require(f.store->workspacePool->idle.size() == 8, "workspace pool bounded at eight");
    const auto workspaceCreated = f.store->workspacePool->created.load();
    for (int i = 0; i < 100; ++i) {
        auto lease = f.store->workspacePool->acquire();
        require(std::all_of(lease->plain.begin(), lease->plain.end(), [](auto c) { return c == 0; }) &&
                std::all_of(lease->block.begin(), lease->block.end(), [](auto c) { return c == 0; }), "pooled plaintext buffers wiped");
    }
    require(f.store->workspacePool->created == workspaceCreated, "workspace allocations stop after warm-up");
}
void copiedBufferTests(Cipher cipher) {
    Fixture f(cipher);
    std::string source(kBlock, 'P');
    require(f.store->write(source.data(), source.size(), 0) == SQLITE_OK, "copy caller buffer");
    const PageBuffer *address = f.store->cache.at(0).record.plain.get();
    require(address && address->bytes.size() == kBlock, "pending copy uses a fixed 4 KiB buffer");
    source.assign(kBlock, 'X');
    require(f.read() == std::string(kBlock, 'P'), "caller can immediately reuse SQLite's source buffer");
    f.commit();
    require(f.store->pending.at(0).plain.get() == address, "commit transfers the same buffer without copying it again");
    f.flush();
    require(!f.store->cache.at(0).record.plain && f.store->cache.at(0).record.encrypted,
            "only encrypted records remain in the clean cache");
    require(!f.store->pagePool->idle.empty(), "persisted copy returns to reusable pool");
    auto lease = f.store->pagePool->acquire();
    require(lease.get() == address && std::all_of(lease->bytes.begin(), lease->bytes.end(), [](auto c) { return c == 0; }),
            "reused page buffer is cleansed before reuse");
    lease.reset();
    const auto created = f.store->pagePool->created.load();
    for (unsigned i = 0; i < 40; ++i) { f.write('R'); f.commit(); f.flush(); }
    require(f.store->pagePool->created == created && f.store->pagePool->reused > 0, "fixed page buffers stop allocating after warm-up");
    f.write('M', 32); f.commit(); f.flush();
    const auto batchCreated = f.store->pagePool->created.load();
    require(f.store->pagePool->idle.size() == f.config.cachePages, "page pool can retain a full cache-sized batch");
    for (unsigned i = 0; i < 3; ++i) { f.write('N', 32); f.commit(); f.flush(); }
    require(f.store->pagePool->created == batchCreated, "large repeated batches also reuse every page buffer after warm-up");
}

void parallelEncodingTests(Cipher cipher, unsigned threads) {
    Fixture f(cipher, 64, threads);
    f.write('B', 32); f.commit();
    Gate gate;
    std::mutex seenMu;
    std::condition_variable allEntered;
    std::unordered_set<std::thread::id> seen;
    const auto caller = std::this_thread::get_id();
    f.store->testHook = [&](const char *point) {
        if (std::strcmp(point, "flush_encode") != 0) return;
        { std::lock_guard<std::mutex> guard(seenMu); seen.insert(std::this_thread::get_id()); allEntered.notify_all(); }
        gate.pause();
    };
    auto flusher = std::async(std::launch::async, [&] { f.flush(); });
    bool parallel;
    {
        std::unique_lock<std::mutex> guard(seenMu);
        parallel = allEntered.wait_for(guard, std::chrono::seconds(3), [&] { return seen.size() == threads; });
    }
    auto reader = std::async(std::launch::async, [&] { return f.read(32); });
    const bool progressed = ready(reader);
    gate.release(); flusher.get();
    require(reader.get() == std::string(32 * kBlock, 'B'), "pending copies stay readable during background compression");
    require(parallel && progressed && !seen.count(caller), "requested workers process pages in parallel outside the caller thread");
    f.store->testHook = {};
    f.reopen(); require(f.read(32) == std::string(32 * kBlock, 'B'), "parallel preparation preserves persisted pages");
}

void backpressureTests(Cipher cipher, bool fail) {
    Fixture f(cipher, 2, 2, false);
    f.seed('A', 2);
    Gate gate; std::atomic<bool> once{false};
    const auto foreground = std::this_thread::get_id();
    std::atomic<bool> wrongThread{false};
    f.store->testHook = [&](const char *point) {
        if (std::strcmp(point, "flush_encode") == 0 || std::strcmp(point, "flush_write") == 0)
            if (std::this_thread::get_id() == foreground) wrongThread = true;
        if (std::strcmp(point, "flush_snapshot") == 0 && !once.exchange(true)) gate.pause();
    };
    f.write('B', 2); f.commit();
    const bool entered = gate.wait();
    auto writer = std::async(std::launch::async, [&] {
        std::string bytes(kBlock, 'C');
        return f.store->write(bytes.data(), bytes.size(), 0);
    });
    const bool blocked = writer.wait_for(std::chrono::milliseconds(50)) == std::future_status::timeout;
    const auto pending = f.read(2);
    f.store->testFailWrite = fail;
    gate.release();
    const int rc = writer.get();
    require(entered && blocked && pending == std::string(2 * kBlock, 'B'), "committed-buffer pressure waits without blocking readers");
    if (fail) {
        require(rc == SQLITE_IOERR_WRITE, "background failure wakes blocked writer with an error");
        f.store->testFailWrite = false;
    } else {
        require(rc == SQLITE_OK, "writer resumes after worker frees pending buffers");
        f.commit();
    }
    // The public flush barrier also executes compression/encryption/write off-thread.
    require(f.store->flush() == SQLITE_OK && !wrongThread, "explicit flush dispatches work to background threads");
    f.store->testHook = {};
    f.reopen();
    require(f.read(2) == std::string(kBlock, fail ? 'B' : 'C') + std::string(kBlock, 'B'),
            "backpressure and failed-batch retry preserve the latest committed version");
}

void resizeDuringFlushTests(Cipher cipher) {
    Fixture f(cipher, 32, 4);
    f.write('S', 16); f.commit();
    Gate gate; std::atomic<bool> once{false};
    f.store->testHook = [&](const char *point) {
        if (std::strcmp(point, "flush_snapshot") == 0 && !once.exchange(true)) gate.pause();
    };
    auto flusher = std::async(std::launch::async, [&] { f.flush(); });
    const bool entered = gate.wait();
    auto setter = std::async(std::launch::async, [&] {
        char *result = nullptr;
        const int rc = f.store->backgroundThreadsPragma("8", false, &result);
        const bool correct = rc == SQLITE_OK && result && std::strcmp(result, "8") == 0;
        sqlite3_free(result); return correct;
    });
    const bool blocked = setter.wait_for(std::chrono::milliseconds(50)) == std::future_status::timeout;
    const auto data = f.read(16);
    gate.release(); flusher.get();
    require(setter.get() && entered && blocked && data == std::string(16 * kBlock, 'S'),
            "resizing waits for the current batch while readers continue");
    require(f.store->encoders->workspaces.size() == 8, "replacement pool has the selected worker count");
    f.store->testHook = {};
    f.write('T', 16); f.commit(); f.flush(); f.reopen();
    require(f.read(16) == std::string(16 * kBlock, 'T'), "resized pool persists subsequent batches");
}

void pendingReadEvictionTests(Cipher cipher) {
    Fixture f(cipher, 1);
    f.write('V', 2); f.commit();
    Gate gate; std::atomic<bool> once{false};
    f.store->testHook = [&](const char *point) {
        if (std::strcmp(point, "read_decode") == 0 && !once.exchange(true)) gate.pause();
    };
    auto reader = std::async(std::launch::async, [&] { return f.read(2); });
    const bool entered = gate.wait();
    f.flush();
    gate.release();
    require(reader.get() == std::string(2 * kBlock, 'V') && entered, "pending snapshot survives encryption and eviction");
    require(f.store->cache.size() == 1 && !f.store->cache.begin()->second.record.plain,
            "stale pending snapshot must not be admitted as plaintext into the clean cache");
    f.store->testHook = {};
}

void encodingFailureTests(Cipher cipher) {
    Fixture f(cipher, 64, 4);
    f.write('F', 16); f.commit();
    std::atomic<bool> once{false};
    f.store->testHook = [&](const char *point) {
        if (std::strcmp(point, "flush_encode") == 0 && !once.exchange(true)) throw std::bad_alloc();
    };
    require(f.store->persist() == SQLITE_NOMEM && f.store->failedBatch, "parallel worker failure retains the entire committed batch");
    f.store->testHook = {};
    f.flush(); f.reopen(); require(f.read(16) == std::string(16 * kBlock, 'F'), "failed parallel batch can be retried in full");
}

void directPendingReadTests(Cipher cipher) {
    Fixture fixture(cipher);
    std::string data(3*kBlock, '\0');
    for (size_t i=0;i<data.size();++i) data[i]=static_cast<char>((31*i+i/kBlock)%251);
    require(fixture.store->write(data.data(), data.size(), 0) == SQLITE_OK, "write nonuniform pending pages");
    fixture.commit();
    const auto created = fixture.store->workspacePool->created.load();
    const auto reused = fixture.store->workspacePool->reused.load();
    const auto check = [&](size_t offset, size_t count) {
        std::string output(count+2, '#');
        const size_t available = offset < data.size() ? std::min(count,data.size()-offset) : 0;
        const int rc = fixture.store->read(output.data()+1, count, offset);
        require(rc == (available == count ? SQLITE_OK : SQLITE_IOERR_SHORT_READ), "direct read returns expected EOF status");
        const std::string expected = (available ? data.substr(offset,available) : std::string()) + std::string(count-available,'\0');
        require(output.front()=='#' && output.back()=='#' && output.substr(1,count)==expected,
                "direct read copies exactly the requested slice and zero fills EOF");
    };
    check(0,100); check(17,123); check(kBlock-6,23); check(kBlock,kBlock);
    check(0,data.size()); check(data.size()-7,32); check(data.size()+1,100);
    require(fixture.store->workspacePool->created == created && fixture.store->workspacePool->reused == reused,
            "pending and EOF reads never acquire a decoding workspace");
    require(fixture.store->truncate(4*kBlock) == SQLITE_OK, "extend a missing zero block"); fixture.commit();
    std::string zeros(kBlock,'?');
    require(fixture.store->read(zeros.data(),zeros.size(),3*kBlock)==SQLITE_OK && zeros==std::string(kBlock,'\0'),
            "missing records fill the requested range with zeros");
    require(fixture.store->workspacePool->created == created && fixture.store->workspacePool->reused == reused,
            "zero-block reads also avoid scratch allocation");
    fixture.flush(); fixture.cold();
    check(0,100); // Now the same bytes must be loaded, authenticated and decompressed.
    require(fixture.store->workspacePool->created > created || fixture.store->workspacePool->reused > reused,
            "encrypted read acquires a decoding workspace lazily");
    fixture.store->settings.key[0] ^= 1;
    std::string rejected(100,'#');
    require(fixture.store->read(rejected.data(),rejected.size(),0)==SQLITE_IOERR_READ && rejected==std::string(100,'#'),
            "encrypted records still reject authentication failure before copying");
    fixture.store->settings.key[0] ^= 1;
    check(0,100);
    for (const auto &workspace : fixture.store->workspacePool->idle)
        require(std::all_of(workspace->block.begin(),workspace->block.end(),[](auto c){return c==0;}) &&
                std::all_of(workspace->plain.begin(),workspace->plain.end(),[](auto c){return c==0;}),
                "encrypted reads retain scratch cleansing on success and failure");
}

void directPendingReadRace(Cipher cipher, bool shrink) {
    Fixture fixture(cipher); fixture.seed('A',2); fixture.write('B',2); fixture.commit();
    std::weak_ptr<const PageBuffer> original = fixture.store->cache.at(0).record.plain;
    Gate gate; std::atomic<bool> once{false};
    fixture.store->testHook = [&](const char *point) {
        if (std::strcmp(point,"read_plain_copied")==0 && !once.exchange(true)) gate.pause();
    };
    std::string output(2*kBlock,'?');
    auto reader=std::async(std::launch::async,[&]{return fixture.store->read(output.data(),output.size(),0);});
    const bool entered=gate.wait();
    auto writer=std::async(std::launch::async,[&]{
        if(shrink) require(fixture.store->truncate(kBlock+17)==SQLITE_OK,"truncate during direct copy");
        else fixture.write('C',2);
        fixture.commit();fixture.flush();
    });
    const bool progressed=ready(writer);
    const bool pinned=!original.expired();
    gate.release();writer.get();
    const int rc=reader.get();
    require(entered && progressed && pinned,"direct read pins old buffer without holding the Store mutex");
    require(rc==(shrink?SQLITE_IOERR_SHORT_READ:SQLITE_OK),"direct read retries the changed file size/version");
    const std::string expected=shrink?std::string(kBlock+17,'B')+std::string(kBlock-17,'\0'):std::string(2*kBlock,'C');
    require(output==expected && original.expired(),"retry returns a coherent current view and releases old buffer");
    fixture.store->testHook={};fixture.reopen();
    std::string reopened(output.size(),'?');
    require(fixture.store->read(reopened.data(),reopened.size(),0)==rc && reopened==expected,
            "direct-read race leaves persistence intact");
}

void journalReuseTests() {
    Fixture fixture(Cipher::AES256GCM);
    std::shared_ptr<Store> store(std::move(fixture.store));
    { std::lock_guard<std::mutex> guard(gMu); gStores[fixture.path] = store; }
    const std::string name = fixture.path + "-journal";
    struct Journal {
        std::vector<std::max_align_t> storage;
        sqlite3_file *file = nullptr;
        explicit Journal(const std::string &name, int extra = 0, bool main = true) {
            storage.resize((sizeof(MurmurFile) + sizeof(std::max_align_t) - 1) / sizeof(std::max_align_t));
            file = reinterpret_cast<sqlite3_file *>(storage.data());
            require(vfsOpen(nullptr, name.c_str(), file, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
                            (main ? SQLITE_OPEN_MAIN_JOURNAL : SQLITE_OPEN_TEMP_JOURNAL) | extra, nullptr) == SQLITE_OK,
                    "open RAM journal");
        }
        void close() { if (file) { require(ioClose(file) == SQLITE_OK, "close RAM journal"); file = nullptr; } }
        ~Journal() { if (file) ioClose(file); }
        MemFile &mem() { return *f(file)->mem; }
        void write(const std::string &value, size_t offset) {
            require(ioWrite(file, value.data(), value.size(), offset) == SQLITE_OK, "write RAM journal");
        }
        std::string read(size_t count, int expected = SQLITE_OK) {
            std::string value(count, '?');
            require(ioRead(file, value.data(), count, 0) == expected, "read RAM journal"); return value;
        }
        size_t size() {
            sqlite3_int64 value = -1; require(ioFileSize(file, &value) == SQLITE_OK, "RAM journal logical size"); return value;
        }
    };
    const auto visible = [&] {
        int result = -1; require(vfsAccess(nullptr, name.c_str(), SQLITE_ACCESS_EXISTS, &result) == SQLITE_OK,
                                "RAM journal visibility"); return result;
    };
    Journal first(name), alias(name);
    require(&first.mem() == &alias.mem(), "open aliases share one active RAM journal");
    first.write(std::string(8192, 'S'), 0); first.write("Z", 12288);
    require(first.read(12289) == std::string(8192, 'S') + std::string(4096, '\0') + "Z", "journal write gaps are zero filled");
    auto *old = &first.mem(); auto *allocation = old->bytes.data(); const auto capacity = old->bytes.capacity();
    require(vfsDelete(nullptr, name.c_str(), 0) == SQLITE_OK && visible() == 0, "DELETE hides journal immediately");
    Journal next(name);
    require(&next.mem() != old && next.size() == 0, "open deleted journal gets a distinct empty file while old handles survive");
    require(alias.read(12289).back() == 'Z', "old handle retains deleted journal contents");
    first.close(); require(store->journalPool->idle.empty(), "buffer cannot recycle while another handle references it");
    alias.close();
    require(store->journalPool->idle.size() == 1 && store->journalPool->idle.front().get() == old,
            "last reference returns journal to per-Store pool");
    require(old->size == 0 && old->dirtyEnd == 0 && old->bytes.data() == allocation && old->bytes.capacity() == capacity &&
            std::all_of(old->bytes.begin(), old->bytes.end(), [](auto c) { return c == 0; }),
            "idle journal retains capacity with no logical data or plaintext");
    next.write("N", 64);
    require(next.read(65) == std::string(64, '\0') + "N", "new journal cannot expose old contents");
    next.close(); require(vfsDelete(nullptr, name.c_str(), 0) == SQLITE_OK, "delete replacement journal");
    Journal reused(name);
    require(&reused.mem() == old && reused.mem().bytes.data() == allocation && reused.size() == 0,
            "reopen reuses the exact journal allocation");
    require(reused.read(32, SQLITE_IOERR_SHORT_READ) == std::string(32, '\0'), "empty reused journal returns zero-filled short read");
    reused.write("ABCD", 0);
    require(ioTruncate(reused.file, 2) == SQLITE_OK && ioTruncate(reused.file, 4) == SQLITE_OK,
            "truncate and regrow RAM journal");
    require(reused.read(4) == std::string("AB\0\0", 4), "regrowth cannot reveal truncated data");
    reused.write("Q", 8); require(reused.read(9) == std::string("AB\0\0\0\0\0\0Q", 9), "reused file preserves sparse-write zeroing");
    reused.close(); require(vfsDelete(nullptr, name.c_str(), 0) == SQLITE_OK, "delete reused journal");
    const auto created = store->journalPool->created.load();
    for (unsigned i = 0; i < 20; ++i) {
        Journal journal(name); journal.write(std::string(8192, 'R'), 0);
        require(journal.mem().bytes.data() == allocation, "normal transactions keep reusing journal storage");
        journal.close(); require(vfsDelete(nullptr, name.c_str(), 0) == SQLITE_OK, "delete loop journal");
    }
    require(store->journalPool->created == created && store->journalPool->idle.size() == 1,
            "journal allocations stop after warm-up and idle pool stays bounded");
    { Journal temporary(name + "-temporary", SQLITE_OPEN_DELETEONCLOSE, false); temporary.write("T", 0); }
    require(store->journalPool->created == created, "unowned temporary files do not borrow the main-journal pool");
    auto oversize = std::make_shared<JournalPool>();
    { auto lease = oversize->acquire(); lease->bytes.resize(kJournalRetainBytes + 1, 'P'); lease->dirtyEnd = lease->size = lease->bytes.size(); }
    require(oversize->idle.empty(), "oversized journal allocations are released rather than retained");
    std::shared_ptr<MemFile> snapshot;
    {
        Journal last(name, SQLITE_OPEN_DELETEONCLOSE); last.write("L", 0); snapshot = f(last.file)->mem;
    }
    require(visible() == 0, "delete-on-close journal is hidden despite a surviving snapshot");
    std::weak_ptr<JournalPool> pool = store->journalPool;
    { std::lock_guard<std::mutex> guard(gMu); gStores.erase(fixture.path); }
    store.reset();
    require(!pool.expired(), "journal snapshot keeps its return pool alive beyond Store lifetime");
    snapshot.reset(); require(pool.expired(), "pool releases when the final snapshot is gone");
}

} // namespace

int main() {
    try {
        std::vector<Cipher> ciphers{Cipher::AES256GCM};
#ifdef SQLITE_MURMUR_HAS_AEGIS256
        require(aegis_init() >= 0, "initialize AEGIS");
        ciphers.push_back(Cipher::AEGIS256);
#endif
        journalReuseTests();
        for (auto cipher : ciphers) {
            readRace(cipher, false); readRace(cipher, true); writeRace(cipher); partialRace(cipher); truncateRace(cipher);
            flushRace(cipher, false, false); flushRace(cipher, true, false); flushRace(cipher, true, true);
            failureTests(cipher); reuseTests(cipher); poolAndLRUTests(cipher);
            copiedBufferTests(cipher);
            for (unsigned threads : {1u, 4u, 8u, 16u}) parallelEncodingTests(cipher, threads);
            backpressureTests(cipher, false); backpressureTests(cipher, true); encodingFailureTests(cipher);
            resizeDuringFlushTests(cipher); pendingReadEvictionTests(cipher);
            directPendingReadTests(cipher); directPendingReadRace(cipher,false); directPendingReadRace(cipher,true);
        }
        std::cout << "Storage versioning, lock boundaries, buffer reuse and exact LRU tests passed\n";
        return 0;
    } catch (const std::exception &error) { std::cerr << error.what() << '\n'; return 1; }
}
