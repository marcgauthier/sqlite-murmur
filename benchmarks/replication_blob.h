#pragma once
#include "sqlite3.h"
#include <array>
#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <vector>
#include <utility>

// CRSB v1: compatible with crsql_correctness.bulk.pack_changes. Preserve all
// nine SELECT * fields and the SQLite storage class of val, including empty blobs.
namespace replication_blob {
using Value = std::unique_ptr<sqlite3_value, decltype(&sqlite3_value_free)>;
using Row = std::array<Value, 9>;
struct Batch {
    std::vector<Row> rows;
    std::vector<unsigned char> blob;
};
inline void integer(std::vector<unsigned char> &out, uint64_t value, unsigned bytes) {
    for (unsigned i = 0; i < bytes; ++i) out.push_back(static_cast<unsigned char>(value >> (8 * i)));
}
inline void bytes(std::vector<unsigned char> &out, const void *data, size_t size) {
    if (size > UINT32_MAX) throw std::runtime_error("CRSB field too large");
    integer(out, size, 4);
    if (size) {
        const auto *p = static_cast<const unsigned char *>(data);
        out.insert(out.end(), p, p + size);
    }
}
inline Batch capture(sqlite3_stmt *changes) {
    if (sqlite3_column_count(changes) != 9) throw std::runtime_error("expected all nine crsql_changes columns");
    Batch batch;
    integer(batch.blob, 0x42535243, 4);
    integer(batch.blob, 1, 4);
    integer(batch.blob, 0, 4);
    int rc;
    while ((rc = sqlite3_step(changes)) == SQLITE_ROW) {
        Row row{{Value(nullptr, sqlite3_value_free), Value(nullptr, sqlite3_value_free),
                 Value(nullptr, sqlite3_value_free), Value(nullptr, sqlite3_value_free),
                 Value(nullptr, sqlite3_value_free), Value(nullptr, sqlite3_value_free),
                 Value(nullptr, sqlite3_value_free), Value(nullptr, sqlite3_value_free),
                 Value(nullptr, sqlite3_value_free)}};
        for (int i = 0; i < 9; ++i) {
            row[i].reset(sqlite3_value_dup(sqlite3_column_value(changes, i)));
            if (!row[i]) throw std::bad_alloc();
        }
        const auto field = [&](int column, bool text) {
            auto *v = row[column].get();
            const void *p = text ? static_cast<const void *>(sqlite3_value_text(v)) : sqlite3_value_blob(v);
            bytes(batch.blob, p, sqlite3_value_bytes(v));
        };
        field(0, true); field(1, false); field(2, true);
        auto *value = row[3].get();
        switch (sqlite3_value_type(value)) {
        case SQLITE_NULL: batch.blob.push_back(0); break;
        case SQLITE_INTEGER:
            batch.blob.push_back(1); integer(batch.blob, sqlite3_value_int64(value), 8); break;
        case SQLITE_FLOAT: {
            batch.blob.push_back(2);
            double number = sqlite3_value_double(value);
            uint64_t bits;
            static_assert(sizeof(bits) == sizeof(number), "CRSB needs 64-bit doubles");
            std::memcpy(&bits, &number, sizeof(bits));
            integer(batch.blob, bits, 8); break;
        }
        case SQLITE_TEXT: batch.blob.push_back(3); field(3, true); break;
        case SQLITE_BLOB: batch.blob.push_back(4); field(3, false); break;
        default: throw std::runtime_error("unsupported SQLite value");
        }
        integer(batch.blob, sqlite3_value_int64(row[4].get()), 8);
        integer(batch.blob, sqlite3_value_int64(row[5].get()), 8);
        field(6, false);
        integer(batch.blob, sqlite3_value_int64(row[7].get()), 8);
        integer(batch.blob, sqlite3_value_int64(row[8].get()), 8);
        batch.rows.push_back(std::move(row));
        if (batch.rows.size() > UINT32_MAX) throw std::runtime_error("too many CRSB changes");
    }
    if (rc != SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(sqlite3_db_handle(changes)));
    for (unsigned i = 0; i < 4; ++i) batch.blob[8 + i] = static_cast<unsigned char>(batch.rows.size() >> (8 * i));
    return batch;
}
} // namespace replication_blob
