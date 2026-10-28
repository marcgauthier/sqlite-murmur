# Static murmur session replication extension. The Rust crate builds without
# default features (no loadable-extension API table): plain sqlite3_* calls and
# the manually declared session entry points stay undefined in the archive and
# resolve against this project's session-enabled static SQLite at final link.
# Engine crates (murmur-crdt/spool/replication) come from local path
# dependencies; registry crates resolve from the lockfile and the local cargo
# cache only (--frozen never touches the network).
find_program(MURMUR_EXT_RUSTUP rustup HINTS "$ENV{HOME}/.cargo/bin" REQUIRED)
set(MURMUR_EXT_TOOLCHAIN "stable" CACHE STRING "Installed modern Rust toolchain for the murmur extension")
execute_process(COMMAND ${MURMUR_EXT_RUSTUP} run ${MURMUR_EXT_TOOLCHAIN} cargo --version
  RESULT_VARIABLE murmur_ext_toolchain_status OUTPUT_QUIET ERROR_QUIET)
if(NOT murmur_ext_toolchain_status EQUAL 0)
  message(FATAL_ERROR "The murmur extension requires installed Rust ${MURMUR_EXT_TOOLCHAIN}; install it with rustup toolchain install ${MURMUR_EXT_TOOLCHAIN} --profile minimal")
endif()
set(murmur_ext_root "${PROJECT_SOURCE_DIR}/third-party/murmur-extension")
set(murmur_ext_build "${PROJECT_BINARY_DIR}/murmur-ext")
file(STRINGS "${murmur_ext_root}/crates/murmur-sqlite-ext/Cargo.toml" _murmur_ext_version_line REGEX "^version = ")
string(REGEX MATCH "[0-9]+\\.[0-9]+\\.[0-9]+" MURMUR_EXT_VERSION "${_murmur_ext_version_line}")
if(NOT MURMUR_EXT_VERSION)
  message(FATAL_ERROR "Cannot parse murmur-sqlite-ext version from Cargo.toml")
endif()
file(GLOB_RECURSE murmur_ext_sources CONFIGURE_DEPENDS
  "${murmur_ext_root}/crates/murmur-sqlite-ext/src/*.rs"
  "${murmur_ext_root}/crates/murmur-sqlite-ext/Cargo.toml"
  "${murmur_ext_root}/Cargo.toml" "${murmur_ext_root}/Cargo.lock")
# Local engine sources, when present, so edits there rebuild the archive.
file(GLOB_RECURSE murmur_ext_engine_sources CONFIGURE_DEPENDS
  "/media/marc/2TB/Archives/MURMUR-RUST/crdt/src/*.rs"
  "/media/marc/2TB/Archives/MURMUR-RUST/spool/src/*.rs"
  "/media/marc/2TB/Archives/MURMUR-RUST/replication/src/*.rs")
set(murmur_ext_archive "${murmur_ext_build}/release/libmurmur_sqlite_ext.a")
add_custom_command(OUTPUT "${murmur_ext_archive}"
  COMMAND ${CMAKE_COMMAND} -E env "CARGO_TARGET_DIR=${murmur_ext_build}"
    ${MURMUR_EXT_RUSTUP} run ${MURMUR_EXT_TOOLCHAIN} cargo rustc --lib --release --frozen
      --manifest-path "${murmur_ext_root}/crates/murmur-sqlite-ext/Cargo.toml"
      --no-default-features --crate-type staticlib
  DEPENDS ${murmur_ext_sources} ${murmur_ext_engine_sources} "${CMAKE_CURRENT_LIST_FILE}"
  WORKING_DIRECTORY "${murmur_ext_build}" VERBATIM)
add_custom_target(murmur_ext_build ALL DEPENDS "${murmur_ext_archive}")
add_library(murmur_ext_rust STATIC IMPORTED GLOBAL)
set_target_properties(murmur_ext_rust PROPERTIES IMPORTED_LOCATION "${murmur_ext_archive}")
add_dependencies(murmur_ext_rust murmur_ext_build)
add_library(sqlite_express_murmur_ext INTERFACE)
add_library(sqlite_express::murmur_ext ALIAS sqlite_express_murmur_ext)
target_link_libraries(sqlite_express_murmur_ext INTERFACE
  murmur_ext_rust sqlite_express Threads::Threads ${CMAKE_DL_LIBS} m)
target_include_directories(sqlite_express_murmur_ext INTERFACE "${PROJECT_SOURCE_DIR}/include")
target_compile_definitions(sqlite_express_murmur_ext INTERFACE
  SQLITE_EXPRESS_MURMUR_EXT_VERSION="${MURMUR_EXT_VERSION}")
