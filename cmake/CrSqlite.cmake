# Optional static cr-sqlite extension. Sources and locked Cargo crates are local.
find_program(CRSQLITE_RUSTUP rustup HINTS "$ENV{HOME}/.cargo/bin" REQUIRED)
set(CRSQLITE_TOOLCHAIN "nightly-2023-10-05" CACHE STRING "Installed Rust toolchain for cr-sqlite")
execute_process(COMMAND ${CRSQLITE_RUSTUP} run ${CRSQLITE_TOOLCHAIN} cargo --version
  RESULT_VARIABLE crsql_toolchain_status OUTPUT_QUIET ERROR_QUIET)
if(NOT crsql_toolchain_status EQUAL 0)
  message(FATAL_ERROR "cr-sqlite requires installed Rust ${CRSQLITE_TOOLCHAIN}; install it with rustup toolchain install ${CRSQLITE_TOOLCHAIN} --profile minimal")
endif()
set(crsql_root "${PROJECT_SOURCE_DIR}/third-party/cr-sqlite")
set(crsql_build "${PROJECT_BINARY_DIR}/crsqlite")
file(MAKE_DIRECTORY "${crsql_build}/cargo-home")
file(WRITE "${crsql_build}/cargo-home/config.toml"
  "[source.crates-io]\nreplace-with = 'vendored-sources'\n[source.vendored-sources]\ndirectory = '${crsql_root}/vendor'\n[net]\noffline = true\n")
file(GLOB_RECURSE crsql_rust_sources CONFIGURE_DEPENDS "${crsql_root}/core/rs/*.rs" "${crsql_root}/core/rs/Cargo.*" "${crsql_root}/core/rs/*/Cargo.*")
set(crsql_archive "${crsql_build}/target/release/libcrsql_bundle_static.a")
add_custom_command(OUTPUT "${crsql_archive}"
  COMMAND ${CMAKE_COMMAND} -E env "CARGO_HOME=${crsql_build}/cargo-home"
    "CARGO_TARGET_DIR=${crsql_build}/target"
    "CRSQLITE_COMMIT_SHA=0d62b52b4662ee1a762c9fd9264d48a91ab8df83"
    ${CRSQLITE_RUSTUP} run ${CRSQLITE_TOOLCHAIN} cargo build --release --frozen
      --manifest-path "${crsql_root}/core/rs/bundle_static/Cargo.toml"
      --features static,omit_load_extension
  DEPENDS ${crsql_rust_sources} "${crsql_root}/core/rs/sqlite-rs-embedded/sqlite3_capi/build.rs"
  WORKING_DIRECTORY "${crsql_build}" VERBATIM)
add_custom_target(crsqlite_rust_build DEPENDS "${crsql_archive}")
add_library(crsqlite_rust STATIC IMPORTED GLOBAL)
set_target_properties(crsqlite_rust PROPERTIES IMPORTED_LOCATION "${crsql_archive}")
add_dependencies(crsqlite_rust crsqlite_rust_build)
add_library(sqlite_express_crsqlite OBJECT
  "${crsql_root}/core/src/crsqlite.c" "${crsql_root}/core/src/changes-vtab.c"
  "${crsql_root}/core/src/ext-data.c")
add_library(sqlite_express::crsqlite ALIAS sqlite_express_crsqlite)
target_compile_definitions(sqlite_express_crsqlite PRIVATE SQLITE_CORE)
target_include_directories(sqlite_express_crsqlite PUBLIC "${PROJECT_SOURCE_DIR}/include")
target_include_directories(sqlite_express_crsqlite PRIVATE "${PROJECT_SOURCE_DIR}/third-party/sqlite" "${crsql_root}/core/src")
target_link_libraries(sqlite_express_crsqlite PUBLIC crsqlite_rust sqlite_express_sqlite ${CMAKE_DL_LIBS} Threads::Threads m)
