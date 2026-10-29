// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// Murmur extension to the RocksDB C API (NOT upstream code).
//
// Upstream db/c.cc exposes DB::SetOptions but not DB::SetDBOptions, and no
// live getters for CF/DB options. sqlite-murmur's rocksdb_* PRAGMAs need
// both (dynamic max_background_jobs + readback of write_buffer_size,
// max_write_buffer_number, max_background_jobs). Rather than shadowing
// these in the caller and silently dropping the live thread adjustment,
// this file forwards them 1:1. See MURMUR-C-EXT.md.
//
// NOTE on struct rocksdb_t: the layout is re-declared here ODR-identically
// to db/c.cc (`DB* rep;`). The definition is file-local upstream, so any
// upstream change to it must be mirrored here; third-party fingerprinting
// (source-files.sha256) forces re-audit on vendor changes, and the
// static_asserts below pin the assumed layout.

#include <rocksdb/c.h>
#include <rocksdb/advanced_options.h>
#include <rocksdb/db.h>
#include <rocksdb/utilities/checkpoint.h>
#include <rocksdb/options.h>
#include <rocksdb/rocksdb_namespace.h>
#include <rocksdb/status.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <unordered_map>

using ROCKSDB_NAMESPACE::DB;

extern "C" {

// ODR-identical to the definition in db/c.cc. Do not extend.
struct rocksdb_t {
  DB* rep;
};

static_assert(sizeof(rocksdb_t) == sizeof(void*),
              "rocksdb_t layout drifted from db/c.cc");
static_assert(offsetof(rocksdb_t, rep) == 0,
              "rocksdb_t layout drifted from db/c.cc");

namespace {

bool SaveError(char** errptr, const ROCKSDB_NAMESPACE::Status& s) {
  if (s.ok()) return false;
  if (errptr == nullptr) return true;
  const std::string msg = s.ToString();
  if (*errptr != nullptr) free(*errptr);
  *errptr = strdup(msg.c_str());
  return true;
}

bool SaveInvalid(char** errptr, const char* what) {
  if (errptr == nullptr) return true;
  const std::string msg = std::string("Invalid argument: ") + what;
  if (*errptr != nullptr) free(*errptr);
  *errptr = strdup(msg.c_str());
  return true;
}

}  // namespace

extern ROCKSDB_LIBRARY_API int murmur_rocksdb_set_db_option(
    rocksdb_t* db, const char* name, const char* value, char** errptr) {
  if (db == nullptr || db->rep == nullptr || name == nullptr ||
      value == nullptr) {
    SaveInvalid(errptr, "null db, name, or value");
    return 1;
  }
  const auto status =
      db->rep->SetDBOptions({{std::string(name), std::string(value)}});
  return SaveError(errptr, status) ? 1 : 0;
}

extern ROCKSDB_LIBRARY_API int murmur_rocksdb_compact_range_full(
    rocksdb_t* db, char** errptr) {
  if (db == nullptr || db->rep == nullptr) {
    SaveInvalid(errptr, "null db");
    return 1;
  }
  ROCKSDB_NAMESPACE::CompactRangeOptions options;
  const auto status = db->rep->CompactRange(options, nullptr, nullptr);
  return SaveError(errptr, status) ? 1 : 0;
}

extern ROCKSDB_LIBRARY_API int murmur_rocksdb_checkpoint_create_with_seq(
    rocksdb_t* db, const char* checkpoint_dir, uint64_t* seq, char** errptr) {
  if (db == nullptr || db->rep == nullptr || checkpoint_dir == nullptr ||
      seq == nullptr) {
    SaveInvalid(errptr, "null db, dir, or seq");
    return 1;
  }
  ROCKSDB_NAMESPACE::Checkpoint* raw = nullptr;
  if (SaveError(errptr, ROCKSDB_NAMESPACE::Checkpoint::Create(db->rep, &raw)) ||
      raw == nullptr) {
    return 1;
  }
  const auto status = raw->CreateCheckpoint(checkpoint_dir, 0, seq);
  delete raw;
  return SaveError(errptr, status) ? 1 : 0;
}

extern ROCKSDB_LIBRARY_API int murmur_rocksdb_get_live_option(
    rocksdb_t* db, const char* name, uint64_t* out, char** errptr) {
  if (db == nullptr || db->rep == nullptr || name == nullptr ||
      out == nullptr) {
    SaveInvalid(errptr, "null db, name, or out");
    return 1;
  }
  if (std::strcmp(name, "write_buffer_size") == 0) {
    *out = static_cast<uint64_t>(db->rep->GetOptions().write_buffer_size);
    return 0;
  }
  if (std::strcmp(name, "max_write_buffer_number") == 0) {
    *out = static_cast<uint64_t>(
        db->rep->GetOptions().max_write_buffer_number);
    return 0;
  }
  if (std::strcmp(name, "max_background_jobs") == 0) {
    *out =
        static_cast<uint64_t>(db->rep->GetDBOptions().max_background_jobs);
    return 0;
  }
  SaveInvalid(errptr, "unknown live option");
  return 1;
}

}  // extern "C"
