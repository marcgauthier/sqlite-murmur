# Rust storage core: port of src/*.cpp behind the stable C ABI in include/.
# Built the same way as murmur-replication (frozen, offline, vendored
# sources, staticlib overriding the manifest rlib crate-type). Unconditional:
# ported symbols (starting with the error taxonomy) always come from here.
find_program(MURMUR_RUSTUP rustup HINTS "$ENV{HOME}/.cargo/bin" REQUIRED)
set(MURMUR_TOOLCHAIN "stable" CACHE STRING "Installed modern Rust toolchain for Murmur")
execute_process(COMMAND ${MURMUR_RUSTUP} run ${MURMUR_TOOLCHAIN} cargo --version
  RESULT_VARIABLE murmur_toolchain_status OUTPUT_QUIET ERROR_QUIET)
if(NOT murmur_toolchain_status EQUAL 0)
  message(FATAL_ERROR "Murmur storage requires installed Rust ${MURMUR_TOOLCHAIN}; install it with rustup toolchain install ${MURMUR_TOOLCHAIN} --profile minimal")
endif()
set(storage_features "")
if(SQLITE_MURMUR_ENABLE_AEGIS256)
  list(APPEND storage_features "aegis256")
endif()
# Encode the feature set in the build dir so option flips rebuild.
set(storage_flavor "base")
foreach(feature IN LISTS storage_features)
  string(APPEND storage_flavor "+${feature}")
endforeach()
set(storage_root "${PROJECT_SOURCE_DIR}/crates/murmur-storage")
set(storage_build "${PROJECT_BINARY_DIR}/murmur-storage/${storage_flavor}")
set(storage_feature_args "")
if(storage_features)
  list(JOIN storage_features "," storage_features_csv)
  set(storage_feature_args "--features" "${storage_features_csv}")
endif()
file(MAKE_DIRECTORY "${storage_build}/cargo-home")
file(WRITE "${storage_build}/cargo-home/config.toml"
  "[source.crates-io]\nreplace-with = 'vendored-sources'\n[source.vendored-sources]\ndirectory = '${PROJECT_SOURCE_DIR}/crates/murmur-replication/vendor'\n[net]\noffline = true\n")
file(GLOB_RECURSE storage_sources CONFIGURE_DEPENDS "${storage_root}/src/*.rs")
file(GLOB_RECURSE murmur_common_sources CONFIGURE_DEPENDS "${PROJECT_SOURCE_DIR}/crates/common/*.rs" "${PROJECT_SOURCE_DIR}/crates/common/fixtures/*")
list(APPEND storage_sources ${murmur_common_sources})
set(storage_archive "${storage_build}/release/libmurmur_storage.a")
add_custom_command(OUTPUT "${storage_archive}"
  COMMAND ${CMAKE_COMMAND} -E env "CARGO_HOME=${storage_build}/cargo-home"
    "MURMUR_BUILD_DIR=${PROJECT_BINARY_DIR}"
    "CARGO_TARGET_DIR=${storage_build}"
    ${MURMUR_RUSTUP} run ${MURMUR_TOOLCHAIN} cargo rustc --lib --release --frozen --manifest-path "${storage_root}/Cargo.toml" --crate-type staticlib ${storage_feature_args}
  DEPENDS ${storage_sources} "$<TARGET_FILE:libzstd_static>" "${storage_root}/build.rs" "${storage_root}/Cargo.toml" "${PROJECT_SOURCE_DIR}/crates/Cargo.toml" "${PROJECT_SOURCE_DIR}/crates/Cargo.lock"
  WORKING_DIRECTORY "${storage_build}" VERBATIM)
add_custom_target(murmur_storage_build ALL DEPENDS "${storage_archive}")
add_dependencies(murmur_storage_build rocksdb libzstd_static)
if(SQLITE_MURMUR_ENABLE_AEGIS256)
  add_dependencies(murmur_storage_build aegis)
endif()
add_library(storage_rust STATIC IMPORTED GLOBAL)
set_target_properties(storage_rust PROPERTIES IMPORTED_LOCATION "${storage_archive}")
add_dependencies(storage_rust murmur_storage_build)
install(FILES "${storage_archive}" DESTINATION ${CMAKE_INSTALL_LIBDIR})
