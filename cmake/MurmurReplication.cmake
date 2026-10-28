# Rust replication uses the host SQLite only. Dependencies are locked and local.
find_program(MURMUR_RUSTUP rustup HINTS "$ENV{HOME}/.cargo/bin" REQUIRED)
set(MURMUR_TOOLCHAIN "stable" CACHE STRING "Installed modern Rust toolchain for Murmur")
execute_process(COMMAND ${MURMUR_RUSTUP} run ${MURMUR_TOOLCHAIN} cargo --version
  RESULT_VARIABLE murmur_toolchain_status OUTPUT_QUIET ERROR_QUIET)
if(NOT murmur_toolchain_status EQUAL 0)
  message(FATAL_ERROR "Murmur requires installed Rust ${MURMUR_TOOLCHAIN}; install it with rustup toolchain install ${MURMUR_TOOLCHAIN} --profile minimal")
endif()
set(murmur_root "${PROJECT_SOURCE_DIR}/third-party/murmur-replication")
set(murmur_build "${PROJECT_BINARY_DIR}/murmur")
file(MAKE_DIRECTORY "${murmur_build}/cargo-home")
file(WRITE "${murmur_build}/cargo-home/config.toml"
  "[source.crates-io]\nreplace-with = 'vendored-sources'\n[source.vendored-sources]\ndirectory = '${murmur_root}/vendor'\n[net]\noffline = true\n")
file(GLOB_RECURSE murmur_sources CONFIGURE_DEPENDS "${murmur_root}/src/*.rs")
foreach(flavor static loadable)
  set(features --crate-type staticlib)
  if(flavor STREQUAL "loadable")
    set(features --crate-type cdylib --features loadable)
  endif()
  set(archive "${murmur_build}/${flavor}/release/libmurmur_replication.a")
  if(flavor STREQUAL "loadable")
    set(output "${murmur_build}/${flavor}/release/${CMAKE_SHARED_LIBRARY_PREFIX}murmur_replication${CMAKE_SHARED_LIBRARY_SUFFIX}")
  else()
    set(output "${archive}")
  endif()
  add_custom_command(OUTPUT "${output}"
    COMMAND ${CMAKE_COMMAND} -E env "CARGO_HOME=${murmur_build}/cargo-home"
      "CARGO_TARGET_DIR=${murmur_build}/${flavor}"
      ${MURMUR_RUSTUP} run ${MURMUR_TOOLCHAIN} cargo rustc --lib --release --frozen --manifest-path "${murmur_root}/Cargo.toml" ${features}
    DEPENDS ${murmur_sources} "${murmur_root}/Cargo.toml" "${murmur_root}/Cargo.lock"
    WORKING_DIRECTORY "${murmur_build}" VERBATIM)
  add_custom_target(murmur_${flavor}_build ALL DEPENDS "${output}")
  if(flavor STREQUAL "static")
    add_library(murmur_rust STATIC IMPORTED GLOBAL)
    set_target_properties(murmur_rust PROPERTIES IMPORTED_LOCATION "${archive}")
    add_dependencies(murmur_rust murmur_static_build)
  else()
    set(MURMUR_LOADABLE_LIBRARY "${output}")
  endif()
endforeach()
add_library(sqlite_express_murmur_replication INTERFACE)
add_library(sqlite_express::murmur_replication ALIAS sqlite_express_murmur_replication)
target_link_libraries(sqlite_express_murmur_replication INTERFACE murmur_rust murmur_crsqlite_static crsqlite_rust sqlite_express Threads::Threads ${CMAKE_DL_LIBS} m)
target_include_directories(sqlite_express_murmur_replication INTERFACE "${PROJECT_SOURCE_DIR}/include")

# Companion cr-sqlite: use its API-table build, never embed another SQLite.
set(crsql_load_build "${PROJECT_BINARY_DIR}/crsqlite-loadable")
set(crsql_load_archive "${crsql_load_build}/target/release/libcrsql_bundle_static.a")
add_custom_command(OUTPUT "${crsql_load_archive}"
  COMMAND ${CMAKE_COMMAND} -E env "CARGO_HOME=${crsql_build}/cargo-home"
    "CARGO_TARGET_DIR=${crsql_load_build}/target" "CRSQLITE_COMMIT_SHA=${CRSQLITE_REVISION}"
    ${CRSQLITE_RUSTUP} run ${CRSQLITE_TOOLCHAIN} cargo build --release --frozen
    --manifest-path "${crsql_root}/core/rs/bundle_static/Cargo.toml" --features loadable_extension
  DEPENDS ${crsql_rust_sources} "${crsql_root}/sqlite-express-revision.txt"
  WORKING_DIRECTORY "${PROJECT_BINARY_DIR}" VERBATIM)
add_custom_target(crsqlite_loadable_rust_build DEPENDS "${crsql_load_archive}")
add_library(crsqlite_loadable_rust STATIC IMPORTED GLOBAL)
set_target_properties(crsqlite_loadable_rust PROPERTIES IMPORTED_LOCATION "${crsql_load_archive}")
add_dependencies(crsqlite_loadable_rust crsqlite_loadable_rust_build)
add_library(murmur_crsqlite MODULE "${crsql_root}/core/src/crsqlite.c" "${crsql_root}/core/src/changes-vtab.c" "${crsql_root}/core/src/ext-data.c")
target_include_directories(murmur_crsqlite PRIVATE "${PROJECT_SOURCE_DIR}/third-party/sqlite" "${crsql_root}/core/src")
target_link_libraries(murmur_crsqlite PRIVATE crsqlite_loadable_rust Threads::Threads ${CMAKE_DL_LIBS} m)
set_target_properties(murmur_crsqlite PROPERTIES PREFIX "")
if(UNIX AND NOT APPLE)
  target_link_options(murmur_crsqlite PRIVATE "-Wl,--exclude-libs,ALL")
endif()

if(SQLITE_EXPRESS_BUILD_TESTS)
  add_executable(sqlite_express_murmur_test tests/murmur_replication.cpp)
  target_link_libraries(sqlite_express_murmur_test PRIVATE sqlite_express::murmur_replication)
  find_package(Python3 3.10 COMPONENTS Interpreter REQUIRED)
  add_test(NAME sqlite_express_murmur COMMAND ${Python3_EXECUTABLE} ${PROJECT_SOURCE_DIR}/tests/murmur_smoke.py $<TARGET_FILE:sqlite_express_murmur_test>)
  set_tests_properties(sqlite_express_murmur PROPERTIES TIMEOUT 120)
  add_executable(sqlite_express_murmur_loadable_test tests/murmur_replication.cpp)
  target_compile_definitions(sqlite_express_murmur_loadable_test PRIVATE MURMUR_TEST_LOADABLE=1)
  target_link_libraries(sqlite_express_murmur_loadable_test PRIVATE sqlite_express)
  add_dependencies(sqlite_express_murmur_loadable_test murmur_loadable_build murmur_crsqlite)
  add_test(NAME sqlite_express_murmur_loadable COMMAND ${Python3_EXECUTABLE} ${PROJECT_SOURCE_DIR}/tests/murmur_smoke.py $<TARGET_FILE:sqlite_express_murmur_loadable_test> "${MURMUR_LOADABLE_LIBRARY}" $<TARGET_FILE:murmur_crsqlite>)
  set_tests_properties(sqlite_express_murmur_loadable PROPERTIES TIMEOUT 120)
endif()
install(FILES "${murmur_build}/static/release/libmurmur_replication.a" "${MURMUR_LOADABLE_LIBRARY}" DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(TARGETS murmur_crsqlite LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES "${PROJECT_SOURCE_DIR}/include/sqlite_express_murmur.h" "${PROJECT_SOURCE_DIR}/include/sqlite_express_crsqlite.h" DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

# Install complete static dependencies for an independent CMake consumer.
add_library(murmur_crsqlite_static STATIC
  "${crsql_root}/core/src/crsqlite.c" "${crsql_root}/core/src/changes-vtab.c" "${crsql_root}/core/src/ext-data.c")
target_compile_definitions(murmur_crsqlite_static PRIVATE SQLITE_CORE)
target_include_directories(murmur_crsqlite_static PRIVATE "${PROJECT_SOURCE_DIR}/third-party/sqlite" "${crsql_root}/core/src")
set_target_properties(murmur_crsqlite_static PROPERTIES OUTPUT_NAME sqlite_express_crsqlite)
install(TARGETS murmur_crsqlite_static ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES "${crsql_isolated_archive}" DESTINATION ${CMAKE_INSTALL_LIBDIR})
# C glue and Rust CRR code reference each other; express the static-library cycle
# so CMake emits the required rescans for both build-tree and installed consumers.
target_link_libraries(murmur_crsqlite_static PUBLIC crsqlite_rust)
set_property(TARGET crsqlite_rust APPEND PROPERTY INTERFACE_LINK_LIBRARIES murmur_crsqlite_static)

install(FILES "${murmur_root}/README.md" "${murmur_root}/USAGE.md" "${murmur_root}/PROVENANCE.md"
  DESTINATION ${CMAKE_INSTALL_DATADIR}/doc/sqlite_express/murmur-replication)
