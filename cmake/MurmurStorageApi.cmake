# Rust open/close core: port of src/sqlite_murmur_api.cpp behind the stable
# C ABI in include/sqlite_murmur.h. Separate staticlib from murmur-storage
# on purpose: minimal consumers (error/keyring/backup tests) must never
# reference the extension initializers, whose transitive chain reaches C++
# symbols that only store-linking binaries provide. Rust codegen units
# group modules unpredictably, so only crate (archive) boundaries
# guarantee that separation. Built the same way as murmur-storage
# (frozen, offline, vendored sources, staticlib overriding the manifest
# rlib crate-type).
find_program(MURMUR_RUSTUP rustup HINTS "$ENV{HOME}/.cargo/bin" REQUIRED)
set(MURMUR_TOOLCHAIN "stable" CACHE STRING "Installed modern Rust toolchain for Murmur")
execute_process(COMMAND ${MURMUR_RUSTUP} run ${MURMUR_TOOLCHAIN} cargo --version
  RESULT_VARIABLE murmur_toolchain_status OUTPUT_QUIET ERROR_QUIET)
if(NOT murmur_toolchain_status EQUAL 0)
  message(FATAL_ERROR "Murmur storage requires installed Rust ${MURMUR_TOOLCHAIN}; install it with rustup toolchain install ${MURMUR_TOOLCHAIN} --profile minimal")
endif()
set(storage_api_features "")
if(SQLITE_MURMUR_ENABLE_CRSQLITE)
  list(APPEND storage_api_features "crsqlite")
endif()
if(SQLITE_MURMUR_ENABLE_MURMUR)
  list(APPEND storage_api_features "murmur")
endif()
# Encode the feature set in the build dir so option flips rebuild.
set(storage_api_flavor "base")
foreach(feature IN LISTS storage_api_features)
  string(APPEND storage_api_flavor "+${feature}")
endforeach()
set(storage_api_root "${PROJECT_SOURCE_DIR}/crates/murmur-storage-api")
set(storage_api_build "${PROJECT_BINARY_DIR}/murmur-storage-api/${storage_api_flavor}")
set(storage_api_feature_args "")
if(storage_api_features)
  list(JOIN storage_api_features "," storage_api_features_csv)
  set(storage_api_feature_args "--features" "${storage_api_features_csv}")
endif()
file(MAKE_DIRECTORY "${storage_api_build}/cargo-home")
file(WRITE "${storage_api_build}/cargo-home/config.toml"
  "[source.crates-io]\nreplace-with = 'vendored-sources'\n[source.vendored-sources]\ndirectory = '${PROJECT_SOURCE_DIR}/crates/murmur-replication/vendor'\n[net]\noffline = true\n")
file(GLOB_RECURSE storage_api_sources CONFIGURE_DEPENDS "${storage_api_root}/src/*.rs")
set(storage_api_archive "${storage_api_build}/release/libmurmur_storage_api.a")
add_custom_command(OUTPUT "${storage_api_archive}"
  COMMAND ${CMAKE_COMMAND} -E env "CARGO_HOME=${storage_api_build}/cargo-home"
    "CARGO_TARGET_DIR=${storage_api_build}"
    ${MURMUR_RUSTUP} run ${MURMUR_TOOLCHAIN} cargo rustc --lib --release --frozen --manifest-path "${storage_api_root}/Cargo.toml" --crate-type staticlib ${storage_api_feature_args}
  DEPENDS ${storage_api_sources} "${storage_api_root}/Cargo.toml" "${PROJECT_SOURCE_DIR}/crates/Cargo.toml" "${PROJECT_SOURCE_DIR}/crates/Cargo.lock"
  WORKING_DIRECTORY "${storage_api_build}" VERBATIM)
add_custom_target(murmur_storage_api_build ALL DEPENDS "${storage_api_archive}")
add_library(storage_api_rust STATIC IMPORTED GLOBAL)
set_target_properties(storage_api_rust PROPERTIES IMPORTED_LOCATION "${storage_api_archive}")
add_dependencies(storage_api_rust murmur_storage_api_build)
install(FILES "${storage_api_archive}" DESTINATION ${CMAKE_INSTALL_LIBDIR})
