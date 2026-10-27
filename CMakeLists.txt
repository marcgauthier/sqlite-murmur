cmake_minimum_required(VERSION 3.16)
project(sqlite_express VERSION 0.1.0 LANGUAGES CXX)

option(SQLITE_EXPRESS_BUILD_TESTS "Build sqlite-express tests" ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(SQLite3 REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)
find_library(ROCKSDB_LIBRARY NAMES rocksdb REQUIRED)
find_path(ROCKSDB_INCLUDE_DIR NAMES rocksdb/db.h REQUIRED)
find_library(ZSTD_LIBRARY NAMES zstd REQUIRED)
find_path(ZSTD_INCLUDE_DIR NAMES zstd.h REQUIRED)

add_library(sqlite_express STATIC src/sqlite_express.cpp)
add_library(sqlite_express::sqlite_express ALIAS sqlite_express)
target_include_directories(sqlite_express
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>
  PRIVATE ${ROCKSDB_INCLUDE_DIR} ${ZSTD_INCLUDE_DIR}
)
target_link_libraries(sqlite_express
  PUBLIC SQLite::SQLite3
  PRIVATE OpenSSL::Crypto ${ROCKSDB_LIBRARY} ${ZSTD_LIBRARY} Threads::Threads
)
target_compile_options(sqlite_express PRIVATE -Wall -Wextra -Wpedantic)

if(SQLITE_EXPRESS_BUILD_TESTS)
  enable_testing()
  add_executable(sqlite_express_test tests/integration.cpp)
  target_link_libraries(sqlite_express_test PRIVATE sqlite_express)
  add_test(NAME sqlite_express_integration COMMAND sqlite_express_test)
  add_executable(sqlite_express_concurrent_test tests/concurrent.cpp)
  target_link_libraries(sqlite_express_concurrent_test PRIVATE sqlite_express)
  add_test(NAME sqlite_express_concurrent COMMAND sqlite_express_concurrent_test)
endif()

include(GNUInstallDirs)
install(TARGETS sqlite_express EXPORT sqlite_expressTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES include/sqlite_express.h DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT sqlite_expressTargets
  NAMESPACE sqlite_express:: DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/sqlite_express)
