# All library sources are checked in. Configuration/build never downloads code.
include(ExternalProject)
find_package(Threads REQUIRED)
find_package(Perl REQUIRED)
find_program(SQLITE_EXPRESS_MAKE NAMES gmake make REQUIRED)
set(SQLITE_EXPRESS_OPENSSL_JOBS 2 CACHE STRING "Parallel jobs for the vendored OpenSSL build")
if(NOT SQLITE_EXPRESS_OPENSSL_JOBS MATCHES "^[1-9][0-9]*$")
  message(FATAL_ERROR "SQLITE_EXPRESS_OPENSSL_JOBS must be a positive integer")
endif()
set(_vendor "${PROJECT_SOURCE_DIR}/third-party")
set(_vendor_build "${PROJECT_BINARY_DIR}/third-party")

# SQLite's amalgamation includes its core and needs only platform libraries.
# Sessions capture replication changes for the murmur extension; the API is
# additive (opaque session handles), so no consumer layout changes.
add_library(sqlite_express_sqlite STATIC "${_vendor}/sqlite/sqlite3.c")
target_compile_definitions(sqlite_express_sqlite PRIVATE SQLITE_THREADSAFE=1
  SQLITE_ENABLE_SESSION SQLITE_ENABLE_PREUPDATE_HOOK)
target_include_directories(sqlite_express_sqlite PUBLIC "${_vendor}/sqlite")
target_link_libraries(sqlite_express_sqlite PUBLIC Threads::Threads ${CMAKE_DL_LIBS})
if(UNIX)
  set(SQLITE_EXPRESS_MATH_LIBRARY m)
  target_link_libraries(sqlite_express_sqlite PUBLIC m)
endif()
set_target_properties(sqlite_express_sqlite PROPERTIES POSITION_INDEPENDENT_CODE ON)

# Build Zstd using its upstream CMake files, without command-line tools/tests.
set(ZSTD_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_CONTRIB OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_STATIC ON CACHE BOOL "" FORCE)
set(ZSTD_MULTITHREAD_SUPPORT OFF CACHE BOOL "" FORCE)
add_subdirectory("${_vendor}/zstd/build/cmake" "${_vendor_build}/zstd" EXCLUDE_FROM_ALL)

# RocksDB receives encrypted blocks. No optional system codecs or allocators.
foreach(_option WITH_GFLAGS WITH_SNAPPY WITH_LZ4 WITH_ZLIB WITH_BZ2 WITH_ZSTD
    WITH_JEMALLOC WITH_LIBURING WITH_NUMA WITH_TBB WITH_TESTS WITH_BENCHMARK_TOOLS
    WITH_CORE_TOOLS WITH_TOOLS WITH_TRACE_TOOLS WITH_EXAMPLES WITH_BENCHMARK
    ROCKSDB_BUILD_SHARED FAIL_ON_WARNINGS)
  set(${_option} OFF CACHE BOOL "" FORCE)
endforeach()
set(PORTABLE ON CACHE BOOL "" FORCE)
add_subdirectory("${_vendor}/rocksdb" "${_vendor_build}/rocksdb" EXCLUDE_FROM_ALL)

# Use OpenSSL's own supported build, out of tree, for AES-GCM and secure RNG.
# Imported target dependencies ensure generated headers exist before compilation.
set(_openssl_install "${_vendor_build}/openssl-install")
file(MAKE_DIRECTORY "${_openssl_install}/include")
ExternalProject_Add(sqlite_express_openssl_build
  SOURCE_DIR "${_vendor}/openssl"
  BINARY_DIR "${_vendor_build}/openssl"
  INSTALL_DIR "${_openssl_install}"
  DOWNLOAD_COMMAND ""
  UPDATE_COMMAND ""
  CONFIGURE_COMMAND ${PERL_EXECUTABLE} <SOURCE_DIR>/Configure
    "--prefix=<INSTALL_DIR>" "--libdir=lib" "--openssldir=<INSTALL_DIR>/ssl"
    no-shared no-tests no-apps no-docs no-module -fPIC
    "CC=${CMAKE_C_COMPILER}" "AR=${CMAKE_AR}" "RANLIB=${CMAKE_RANLIB}"
  BUILD_COMMAND ${SQLITE_EXPRESS_MAKE} "-j${SQLITE_EXPRESS_OPENSSL_JOBS}" build_libs
  INSTALL_COMMAND ${SQLITE_EXPRESS_MAKE} install_dev
  BUILD_BYPRODUCTS "${_openssl_install}/lib/libcrypto.a"
  LOG_CONFIGURE ON LOG_BUILD ON LOG_INSTALL ON
)
add_library(sqlite_express_crypto STATIC IMPORTED GLOBAL)
set_target_properties(sqlite_express_crypto PROPERTIES
  IMPORTED_LOCATION "${_openssl_install}/lib/libcrypto.a"
  INTERFACE_INCLUDE_DIRECTORIES "${_openssl_install}/include"
  INTERFACE_LINK_LIBRARIES "Threads::Threads;${CMAKE_DL_LIBS}")
add_dependencies(sqlite_express_crypto sqlite_express_openssl_build)

if(SQLITE_EXPRESS_ENABLE_AEGIS256)
  add_subdirectory("${_vendor}/aegis" "${_vendor_build}/aegis" EXCLUDE_FROM_ALL)
  target_include_directories(aegis INTERFACE "$<BUILD_INTERFACE:${_vendor}/aegis/src/include>")
endif()
