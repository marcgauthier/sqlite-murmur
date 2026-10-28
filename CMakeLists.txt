cmake_minimum_required(VERSION 3.16)
project(sqlite_express VERSION 0.1.0 LANGUAGES C CXX)

option(SQLITE_EXPRESS_BUILD_TESTS "Build sqlite-express tests" ON)
option(SQLITE_EXPRESS_BUILD_BENCHMARKS "Build the standalone benchmark" ON)
option(SQLITE_EXPRESS_ENABLE_AEGIS256 "Enable AEGIS-256 encryption using libaegis" ON)
option(SQLITE_EXPRESS_ENABLE_MURMUR_EXT "Build the static murmur session replication extension" OFF)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(GNUInstallDirs)
include(cmake/VendorDependencies.cmake)
if(SQLITE_EXPRESS_ENABLE_MURMUR_EXT)
  include(cmake/MurmurExtension.cmake)
endif()

add_library(sqlite_express STATIC src/sqlite_express.cpp)
add_library(sqlite_express::sqlite_express ALIAS sqlite_express)
target_include_directories(sqlite_express
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(sqlite_express
  PUBLIC $<BUILD_INTERFACE:sqlite_express_sqlite> $<INSTALL_INTERFACE:sqlite_express::sqlite3>
  PRIVATE $<BUILD_INTERFACE:sqlite_express_crypto> $<INSTALL_INTERFACE:sqlite_express::crypto>
          $<BUILD_INTERFACE:rocksdb> $<INSTALL_INTERFACE:sqlite_express::rocksdb>
          $<BUILD_INTERFACE:libzstd_static> $<INSTALL_INTERFACE:sqlite_express::zstd>
          Threads::Threads ${CMAKE_DL_LIBS}
)
target_compile_options(sqlite_express PRIVATE -Wall -Wextra -Wpedantic)

if(SQLITE_EXPRESS_ENABLE_AEGIS256)
  target_link_libraries(sqlite_express PRIVATE
    $<BUILD_INTERFACE:aegis> $<INSTALL_INTERFACE:sqlite_express::aegis>)
  target_compile_definitions(sqlite_express PRIVATE SQLITE_EXPRESS_HAS_AEGIS256=1)
endif()

if(SQLITE_EXPRESS_BUILD_TESTS)
  enable_testing()
  find_package(Python3 3.10 COMPONENTS Interpreter QUIET)
  if(Python3_Interpreter_FOUND)
    add_test(NAME sqlite_express_vendored_sources
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/third-party/verify.py)
  endif()
  add_executable(sqlite_express_test tests/integration.cpp)
  target_link_libraries(sqlite_express_test PRIVATE rocksdb)
  target_link_libraries(sqlite_express_test PRIVATE sqlite_express)
  add_test(NAME sqlite_express_integration COMMAND sqlite_express_test)
  if(SQLITE_EXPRESS_ENABLE_AEGIS256)
    add_test(NAME sqlite_express_integration_aegis256 COMMAND sqlite_express_test aegis256)
  else()
    target_compile_definitions(sqlite_express_test PRIVATE SQLITE_EXPRESS_TEST_NO_AEGIS256=1)
  endif()
  add_executable(sqlite_express_concurrent_test tests/concurrent.cpp)
  target_link_libraries(sqlite_express_concurrent_test PRIVATE sqlite_express)
  add_test(NAME sqlite_express_concurrent COMMAND sqlite_express_concurrent_test)
  add_executable(sqlite_express_storage_test tests/storage_concurrency.cpp)
  target_include_directories(sqlite_express_storage_test PRIVATE include)
  target_link_libraries(sqlite_express_storage_test PRIVATE
    sqlite_express_sqlite sqlite_express_crypto rocksdb libzstd_static Threads::Threads ${CMAKE_DL_LIBS})
  if(SQLITE_EXPRESS_ENABLE_AEGIS256)
    target_link_libraries(sqlite_express_storage_test PRIVATE aegis)
    target_compile_definitions(sqlite_express_storage_test PRIVATE SQLITE_EXPRESS_HAS_AEGIS256=1)
  endif()
  add_test(NAME sqlite_express_storage_concurrency COMMAND sqlite_express_storage_test)
  set_tests_properties(sqlite_express_storage_concurrency PROPERTIES TIMEOUT 120)
endif()

if(SQLITE_EXPRESS_BUILD_BENCHMARKS)
  add_executable(benchmark benchmarks/benchmark.cpp)
  target_link_libraries(benchmark PRIVATE sqlite_express)
  if(SQLITE_EXPRESS_ENABLE_MURMUR_EXT)
    target_link_libraries(benchmark PRIVATE sqlite_express_murmur_ext)
    target_compile_definitions(benchmark PRIVATE SQLITE_EXPRESS_HAS_MURMUR_EXT=1)
  endif()
  if(SQLITE_EXPRESS_ENABLE_AEGIS256)
    target_compile_definitions(benchmark PRIVATE SQLITE_EXPRESS_HAS_AEGIS256=1)
  endif()
  if(SQLITE_EXPRESS_BUILD_TESTS)
    if(Python3_Interpreter_FOUND)
      add_test(NAME sqlite_express_benchmark_smoke
        COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/benchmark_smoke.py
                $<TARGET_FILE:benchmark> ${SQLITE_EXPRESS_ENABLE_AEGIS256} ${SQLITE_EXPRESS_ENABLE_MURMUR_EXT})
      set_tests_properties(sqlite_express_benchmark_smoke PROPERTIES TIMEOUT 120)
    endif()
  endif()
endif()

install(TARGETS sqlite_express EXPORT sqlite_expressTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES include/sqlite_express.h third-party/sqlite/sqlite3.h DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT sqlite_expressTargets
  NAMESPACE sqlite_express:: DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/sqlite_express)

# Install the statically linked dependency archives with relocatable package targets.
install(FILES $<TARGET_FILE:sqlite_express_sqlite> $<TARGET_FILE:rocksdb>
              $<TARGET_FILE:libzstd_static> $<TARGET_FILE:sqlite_express_crypto>
        DESTINATION ${CMAKE_INSTALL_LIBDIR})
if(SQLITE_EXPRESS_ENABLE_AEGIS256)
  install(FILES $<TARGET_FILE:aegis> DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()
include(CMakePackageConfigHelpers)
configure_package_config_file(cmake/sqlite_expressConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/sqlite_expressConfig.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/sqlite_express)
write_basic_package_version_file(${CMAKE_CURRENT_BINARY_DIR}/sqlite_expressConfigVersion.cmake
  VERSION ${PROJECT_VERSION} COMPATIBILITY SameMajorVersion)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/sqlite_expressConfig.cmake
              ${CMAKE_CURRENT_BINARY_DIR}/sqlite_expressConfigVersion.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/sqlite_express)
install(FILES third-party/README.md third-party/sources.json
        DESTINATION ${CMAKE_INSTALL_DATADIR}/doc/sqlite_express/third-party)
install(FILES third-party/sqlite/LICENSE.md
        DESTINATION ${CMAKE_INSTALL_DATADIR}/licenses/sqlite_express/sqlite)
install(FILES third-party/rocksdb/LICENSE.Apache third-party/rocksdb/LICENSE.leveldb third-party/rocksdb/COPYING
        DESTINATION ${CMAKE_INSTALL_DATADIR}/licenses/sqlite_express/rocksdb)
install(FILES third-party/zstd/LICENSE third-party/zstd/COPYING
        DESTINATION ${CMAKE_INSTALL_DATADIR}/licenses/sqlite_express/zstd)
install(FILES third-party/openssl/LICENSE.txt
        DESTINATION ${CMAKE_INSTALL_DATADIR}/licenses/sqlite_express/openssl)
if(SQLITE_EXPRESS_ENABLE_AEGIS256)
  install(FILES third-party/aegis/LICENSE
          DESTINATION ${CMAKE_INSTALL_DATADIR}/licenses/sqlite_express/aegis)
endif()
