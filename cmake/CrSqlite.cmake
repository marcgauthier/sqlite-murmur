# Optional static cr-sqlite extension. Sources and locked Cargo crates are local.
find_program(CRSQLITE_RUSTUP rustup HINTS "$ENV{HOME}/.cargo/bin" REQUIRED)
set(CRSQLITE_TOOLCHAIN "nightly-2023-10-05" CACHE STRING "Installed Rust toolchain for cr-sqlite")
execute_process(COMMAND ${CRSQLITE_RUSTUP} run ${CRSQLITE_TOOLCHAIN} cargo --version
  RESULT_VARIABLE crsql_toolchain_status OUTPUT_QUIET ERROR_QUIET)
if(NOT crsql_toolchain_status EQUAL 0)
  message(FATAL_ERROR "cr-sqlite requires installed Rust ${CRSQLITE_TOOLCHAIN}; install it with rustup toolchain install ${CRSQLITE_TOOLCHAIN} --profile minimal")
endif()
set(crsql_root "${PROJECT_SOURCE_DIR}/third-party/cr-sqlite")
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${crsql_root}/sqlite-murmur-revision.txt")
file(STRINGS "${crsql_root}/sqlite-murmur-revision.txt" CRSQLITE_REVISION LIMIT_COUNT 1)
set(crsql_build "${PROJECT_BINARY_DIR}/crsqlite2")
file(MAKE_DIRECTORY "${crsql_build}/cargo-home")
file(WRITE "${crsql_build}/cargo-home/config.toml"
  "[source.crates-io]\nreplace-with = 'vendored-sources'\n[source.vendored-sources]\ndirectory = '${crsql_root}/vendor'\n[net]\noffline = true\n")
file(GLOB_RECURSE crsql_rust_sources CONFIGURE_DEPENDS "${crsql_root}/core/rs/*.rs" "${crsql_root}/core/rs/Cargo.*" "${crsql_root}/core/rs/*/Cargo.*")
set(crsql_archive "${crsql_build}/target/release/libcrsql_bundle_static.a")
add_custom_command(OUTPUT "${crsql_archive}"
  COMMAND ${CMAKE_COMMAND} -E env "CARGO_HOME=${crsql_build}/cargo-home"
    "CARGO_TARGET_DIR=${crsql_build}/target"
    "CRSQLITE_COMMIT_SHA=${CRSQLITE_REVISION}"
    ${CRSQLITE_RUSTUP} run ${CRSQLITE_TOOLCHAIN} cargo build --release --frozen
      --manifest-path "${crsql_root}/core/rs/bundle_static/Cargo.toml"
      --features static,omit_load_extension
  DEPENDS "${crsql_root}/sqlite-murmur-revision.txt" "${CMAKE_CURRENT_LIST_FILE}" ${crsql_rust_sources} "${crsql_root}/core/rs/sqlite-rs-embedded/sqlite3_capi/build.rs"
  WORKING_DIRECTORY "${crsql_build}" VERBATIM)
add_custom_target(crsqlite_rust_build DEPENDS "${crsql_archive}")
add_library(crsqlite_rust STATIC IMPORTED GLOBAL)
set_target_properties(crsqlite_rust PROPERTIES IMPORTED_LOCATION "${crsql_archive}")
add_dependencies(crsqlite_rust crsqlite_rust_build)
add_library(sqlite_murmur_crsqlite OBJECT
  "${crsql_root}/core/src/crsqlite.c" "${crsql_root}/core/src/changes-vtab.c"
  "${crsql_root}/core/src/ext-data.c")
add_library(sqlite_murmur::crsqlite ALIAS sqlite_murmur_crsqlite)
target_compile_definitions(sqlite_murmur_crsqlite PRIVATE SQLITE_CORE
  INTERFACE SQLITE_MURMUR_CRSQLITE_VERSION="${CRSQLITE_REVISION}")
target_include_directories(sqlite_murmur_crsqlite PUBLIC "${PROJECT_SOURCE_DIR}/include")
target_include_directories(sqlite_murmur_crsqlite PRIVATE "${PROJECT_SOURCE_DIR}/third-party/sqlite" "${crsql_root}/core/src")
target_link_libraries(sqlite_murmur_crsqlite PUBLIC crsqlite_rust sqlite_murmur_sqlite ${CMAKE_DL_LIBS} Threads::Threads m)

# Keep cr-sqlite's SQLite allocator independent of other statically linked Rust
# libraries, including murmur-replication. Never rename SQLite's public C ABI.
set(crsql_isolated_archive "${crsql_build}/libcrsql_bundle_isolated.a")
add_custom_command(OUTPUT "${crsql_isolated_archive}"
  COMMAND ${CMAKE_COMMAND} "-DNM=${CMAKE_NM}" "-DOBJCOPY=${CMAKE_OBJCOPY}"
    "-DINPUT=${crsql_archive}" "-DOUTPUT=${crsql_isolated_archive}"
    -P "${PROJECT_SOURCE_DIR}/cmake/IsolateRustRuntime.cmake"
  DEPENDS "${crsql_archive}" "${PROJECT_SOURCE_DIR}/cmake/IsolateRustRuntime.cmake" VERBATIM)
add_custom_target(crsqlite_isolated_build DEPENDS "${crsql_isolated_archive}")
set_target_properties(crsqlite_rust PROPERTIES IMPORTED_LOCATION "${crsql_isolated_archive}")
add_dependencies(crsqlite_rust crsqlite_isolated_build)

# The application-facing target carries this archive automatically.
add_library(sqlite_murmur_crsqlite_static STATIC
  "${crsql_root}/core/src/crsqlite.c" "${crsql_root}/core/src/changes-vtab.c" "${crsql_root}/core/src/ext-data.c")
target_compile_definitions(sqlite_murmur_crsqlite_static PRIVATE SQLITE_CORE)
target_include_directories(sqlite_murmur_crsqlite_static PRIVATE "${PROJECT_SOURCE_DIR}/third-party/sqlite" "${crsql_root}/core/src")
set_target_properties(sqlite_murmur_crsqlite_static PROPERTIES OUTPUT_NAME sqlite_murmur_crsqlite)
target_link_libraries(sqlite_murmur_crsqlite_static PUBLIC crsqlite_rust sqlite_murmur_sqlite)
set_property(TARGET crsqlite_rust APPEND PROPERTY INTERFACE_LINK_LIBRARIES sqlite_murmur_crsqlite_static)
install(TARGETS sqlite_murmur_crsqlite_static ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES "${crsql_isolated_archive}" DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES "${PROJECT_SOURCE_DIR}/include/sqlite_murmur_crsqlite.h" DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
