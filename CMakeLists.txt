cmake_minimum_required(VERSION 3.16)
project(sqlite_murmur VERSION 0.1.0 LANGUAGES C CXX)

set(sqlite_murmur_top_level OFF)
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  set(sqlite_murmur_top_level ON)
endif()
option(SQLITE_MURMUR_BUILD_TESTS "Build sqlite-murmur tests" ${sqlite_murmur_top_level})
option(SQLITE_MURMUR_BUILD_BENCHMARKS "Build the standalone benchmark" ${sqlite_murmur_top_level})
option(SQLITE_MURMUR_ENABLE_AEGIS256 "Enable AEGIS-256 encryption using libaegis" ON)
option(SQLITE_MURMUR_ENABLE_CRSQLITE "Build the vendored static cr-sqlite extension" ON)
option(SQLITE_MURMUR_ENABLE_MURMUR "Build Rust QUIC replication support" ON)
option(SQLITE_MURMUR_BUILD_LOADABLE_EXTENSIONS "Also build standalone extension shared libraries" OFF)
if(SQLITE_MURMUR_ENABLE_MURMUR AND NOT SQLITE_MURMUR_ENABLE_CRSQLITE)
  message(FATAL_ERROR "Murmur replication requires SQLITE_MURMUR_ENABLE_CRSQLITE=ON")
endif()
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(GNUInstallDirs)
include(cmake/VendorDependencies.cmake)
if(SQLITE_MURMUR_ENABLE_CRSQLITE)
  include(cmake/CrSqlite.cmake)
endif()

# The storage implementation is Rust (crates/murmur-storage,
# crates/murmur-storage-api) behind the stable C ABI in include/.
# sqlite_murmur is therefore a header + link aggregate with no C++ sources.
add_library(sqlite_murmur INTERFACE)
add_library(sqlite_murmur::sqlite_murmur ALIAS sqlite_murmur)
target_include_directories(sqlite_murmur
  INTERFACE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
# Link order is load-bearing for one-pass static linking: the Rust
# archives (first sqlite3_* users) precede libsqlite_murmur_sqlite.a,
# whose single amalgamation object then resolves every later SQLite
# reference (including the cr-sqlite/murmur archives appended below).
# OpenSSL/RocksDB/zstd likewise follow their users.
include(cmake/MurmurStorage.cmake)
include(cmake/MurmurStorageApi.cmake)
target_link_libraries(sqlite_murmur INTERFACE
  $<BUILD_INTERFACE:storage_rust> $<INSTALL_INTERFACE:sqlite_murmur::murmur_storage>
  $<BUILD_INTERFACE:storage_api_rust> $<INSTALL_INTERFACE:sqlite_murmur::murmur_storage_api>)

if(SQLITE_MURMUR_ENABLE_AEGIS256)
  target_link_libraries(sqlite_murmur INTERFACE
    $<BUILD_INTERFACE:aegis> $<INSTALL_INTERFACE:sqlite_murmur::aegis>)
endif()

target_link_libraries(sqlite_murmur INTERFACE
  $<BUILD_INTERFACE:sqlite_murmur_sqlite> $<INSTALL_INTERFACE:sqlite_murmur::sqlite3>
  $<BUILD_INTERFACE:sqlite_murmur_crypto> $<INSTALL_INTERFACE:sqlite_murmur::crypto>
  $<BUILD_INTERFACE:rocksdb> $<INSTALL_INTERFACE:sqlite_murmur::rocksdb>
  $<BUILD_INTERFACE:libzstd_static> $<INSTALL_INTERFACE:sqlite_murmur::zstd>
  Threads::Threads ${CMAKE_DL_LIBS}
)

if(SQLITE_MURMUR_BUILD_TESTS)
  enable_testing()
  if(SQLITE_MURMUR_ENABLE_CRSQLITE)
    add_executable(sqlite_murmur_replication_test tests/replication.cpp)
    target_compile_definitions(sqlite_murmur_replication_test PRIVATE SQLITE_MURMUR_HAS_CRSQLITE=1)
    target_link_libraries(sqlite_murmur_replication_test PRIVATE sqlite_murmur sqlite_murmur_crsqlite)
    add_test(NAME sqlite_murmur_replication COMMAND sqlite_murmur_replication_test)
  endif()
  find_package(Python3 3.10 COMPONENTS Interpreter QUIET)
  if(Python3_Interpreter_FOUND)
    if(SQLITE_MURMUR_ENABLE_CRSQLITE)
      add_test(NAME sqlite_murmur_blob_packer
        COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/blob_packer.py $<TARGET_FILE:sqlite_murmur_replication_test>)
    endif()
    add_test(NAME sqlite_murmur_vendored_sources
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/third-party/verify.py)
  endif()
  add_executable(sqlite_murmur_test tests/integration.cpp)
  target_link_libraries(sqlite_murmur_test PRIVATE rocksdb)
  target_link_libraries(sqlite_murmur_test PRIVATE sqlite_murmur)
  add_test(NAME sqlite_murmur_integration COMMAND sqlite_murmur_test)
  if(SQLITE_MURMUR_ENABLE_AEGIS256)
    add_test(NAME sqlite_murmur_integration_aegis256 COMMAND sqlite_murmur_test aegis256)
  else()
    target_compile_definitions(sqlite_murmur_test PRIVATE SQLITE_MURMUR_TEST_NO_AEGIS256=1)
  endif()
  add_executable(sqlite_murmur_concurrent_test tests/concurrent.cpp)
  target_link_libraries(sqlite_murmur_concurrent_test PRIVATE sqlite_murmur)
  add_test(NAME sqlite_murmur_concurrent COMMAND sqlite_murmur_concurrent_test)
  add_executable(sqlite_murmur_storage_test tests/storage_concurrency.cpp)
  target_link_libraries(sqlite_murmur_storage_test PRIVATE sqlite_murmur)
  if(SQLITE_MURMUR_ENABLE_AEGIS256)
    target_compile_definitions(sqlite_murmur_storage_test PRIVATE SQLITE_MURMUR_HAS_AEGIS256=1)
  endif()
  add_test(NAME sqlite_murmur_storage_concurrency COMMAND sqlite_murmur_storage_test)
  set_tests_properties(sqlite_murmur_storage_concurrency PROPERTIES TIMEOUT 120)
  add_executable(sqlite_murmur_errors_test tests/errors.cpp)
  target_link_libraries(sqlite_murmur_errors_test PRIVATE sqlite_murmur)
  add_test(NAME sqlite_murmur_errors COMMAND sqlite_murmur_errors_test)
  add_executable(sqlite_murmur_backup_test tests/backup.cpp)
  target_link_libraries(sqlite_murmur_backup_test PRIVATE sqlite_murmur rocksdb)
  add_test(NAME sqlite_murmur_backup COMMAND sqlite_murmur_backup_test)
  add_executable(sqlite_murmur_keyring_test tests/keyring.cpp)
  target_link_libraries(sqlite_murmur_keyring_test PRIVATE sqlite_murmur)
  add_test(NAME sqlite_murmur_keyring COMMAND sqlite_murmur_keyring_test)
  add_executable(sqlite_murmur_compat_test tests/compat.cpp)
  target_link_libraries(sqlite_murmur_compat_test PRIVATE sqlite_murmur)
  if(SQLITE_MURMUR_ENABLE_AEGIS256)
    target_compile_definitions(sqlite_murmur_compat_test PRIVATE SQLITE_MURMUR_HAS_AEGIS256=1)
  endif()
  add_test(NAME sqlite_murmur_compat
    COMMAND sqlite_murmur_compat_test ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/compat-v1)
  # murmur-admin offline CLI (WP-D D3a, Rust) + in-process suite. Built
  # with tests so ctest gates it. The CLI executable has no C++ sources:
  # `main` comes from the admin_bin_rust archive.
  include(cmake/MurmurAdmin.cmake)
  # The executable's only "source" is the archive providing `main`.
  add_executable(murmur-admin $<TARGET_FILE:admin_bin_rust>)
  set_target_properties(murmur-admin PROPERTIES LINKER_LANGUAGE CXX)
  target_include_directories(murmur-admin PRIVATE tools/murmur-admin)
  target_link_libraries(murmur-admin PRIVATE admin_bin_rust admin_rust sqlite_murmur)
  # The in-process suite builds CRR fixtures, so it needs cr-sqlite.
  if(SQLITE_MURMUR_ENABLE_CRSQLITE)
    add_executable(murmur-admin-tests tools/murmur-admin/tests/admin.cpp)
    target_include_directories(murmur-admin-tests PRIVATE tools/murmur-admin)
    target_link_libraries(murmur-admin-tests PRIVATE admin_rust sqlite_murmur rocksdb)
    target_compile_options(murmur-admin-tests PRIVATE -Wall -Wextra -Wpedantic -Werror)
    add_test(NAME murmur_admin COMMAND murmur-admin-tests)
  endif()
endif()

if(SQLITE_MURMUR_ENABLE_MURMUR)
  include(cmake/MurmurReplication.cmake)
endif()
if(SQLITE_MURMUR_ENABLE_CRSQLITE)
  target_link_libraries(sqlite_murmur INTERFACE
    $<BUILD_INTERFACE:sqlite_murmur_crsqlite_static> $<INSTALL_INTERFACE:sqlite_murmur::crsqlite_static>)
endif()
if(SQLITE_MURMUR_ENABLE_MURMUR)
  target_link_libraries(sqlite_murmur INTERFACE
    $<BUILD_INTERFACE:murmur_rust> $<INSTALL_INTERFACE:sqlite_murmur::murmur_rust>)
endif()

if(SQLITE_MURMUR_BUILD_TESTS AND SQLITE_MURMUR_ENABLE_MURMUR)
  add_executable(sqlite_murmur_application_test tests/application.cpp)
  target_link_libraries(sqlite_murmur_application_test PRIVATE sqlite_murmur::sqlite_murmur)
  if(SQLITE_MURMUR_ENABLE_AEGIS256)
    target_compile_definitions(sqlite_murmur_application_test PRIVATE SQLITE_MURMUR_TEST_AEGIS=1)
  endif()
  add_test(NAME sqlite_murmur_application COMMAND sqlite_murmur_application_test)
endif()

if(SQLITE_MURMUR_BUILD_BENCHMARKS)
  add_executable(benchmark benchmarks/benchmark.cpp)
  target_link_libraries(benchmark PRIVATE sqlite_murmur)
  if(SQLITE_MURMUR_ENABLE_CRSQLITE)
    target_link_libraries(benchmark PRIVATE sqlite_murmur_crsqlite)
    target_compile_definitions(benchmark PRIVATE SQLITE_MURMUR_HAS_CRSQLITE=1)
  endif()
  if(SQLITE_MURMUR_ENABLE_AEGIS256)
    target_compile_definitions(benchmark PRIVATE SQLITE_MURMUR_HAS_AEGIS256=1)
  endif()
  if(SQLITE_MURMUR_BUILD_TESTS)
    if(Python3_Interpreter_FOUND)
      add_test(NAME sqlite_murmur_benchmark_smoke
        COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/benchmark_smoke.py
                $<TARGET_FILE:benchmark> ${SQLITE_MURMUR_ENABLE_AEGIS256} ${SQLITE_MURMUR_ENABLE_CRSQLITE})
      set_tests_properties(sqlite_murmur_benchmark_smoke PROPERTIES TIMEOUT 240)
    endif()
  endif()
endif()

if(sqlite_murmur_top_level)
  add_subdirectory(examples/consumer)
endif()

install(TARGETS sqlite_murmur EXPORT sqlite_murmurTargets)
install(FILES include/sqlite_murmur.h include/sqlite_murmur_errors.h include/sqlite_murmur_subscribe.h include/sqlite_murmur_backup.h include/sqlite_murmur_keyring.h third-party/sqlite/sqlite3.h DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT sqlite_murmurTargets
  NAMESPACE sqlite_murmur:: DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/sqlite_murmur)

# Install the statically linked dependency archives with relocatable package targets.
install(FILES $<TARGET_FILE:sqlite_murmur_sqlite> $<TARGET_FILE:rocksdb>
              $<TARGET_FILE:libzstd_static> $<TARGET_FILE:sqlite_murmur_crypto>
        DESTINATION ${CMAKE_INSTALL_LIBDIR})
if(SQLITE_MURMUR_ENABLE_AEGIS256)
  install(FILES $<TARGET_FILE:aegis> DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()
include(CMakePackageConfigHelpers)
configure_package_config_file(cmake/sqlite_murmurConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/sqlite_murmurConfig.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/sqlite_murmur)
write_basic_package_version_file(${CMAKE_CURRENT_BINARY_DIR}/sqlite_murmurConfigVersion.cmake
  VERSION ${PROJECT_VERSION} COMPATIBILITY SameMajorVersion)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/sqlite_murmurConfig.cmake
              ${CMAKE_CURRENT_BINARY_DIR}/sqlite_murmurConfigVersion.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/sqlite_murmur)
install(FILES third-party/README.md third-party/sources.json
        DESTINATION ${CMAKE_INSTALL_DATADIR}/doc/sqlite_murmur/third-party)
install(FILES third-party/sqlite/LICENSE.md
        DESTINATION ${CMAKE_INSTALL_DATADIR}/licenses/sqlite_murmur/sqlite)
install(FILES third-party/rocksdb/LICENSE.Apache third-party/rocksdb/LICENSE.leveldb third-party/rocksdb/COPYING
        DESTINATION ${CMAKE_INSTALL_DATADIR}/licenses/sqlite_murmur/rocksdb)
install(FILES third-party/zstd/LICENSE third-party/zstd/COPYING
        DESTINATION ${CMAKE_INSTALL_DATADIR}/licenses/sqlite_murmur/zstd)
install(FILES third-party/openssl/LICENSE.txt
        DESTINATION ${CMAKE_INSTALL_DATADIR}/licenses/sqlite_murmur/openssl)
if(SQLITE_MURMUR_ENABLE_AEGIS256)
  install(FILES third-party/aegis/LICENSE
          DESTINATION ${CMAKE_INSTALL_DATADIR}/licenses/sqlite_murmur/aegis)
endif()
