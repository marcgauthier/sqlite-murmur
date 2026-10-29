# Rust murmur-admin CLI: port of tools/murmur-admin/admin.cpp behind the C
# ABI in tools/murmur-admin/admin.h. Built the same way as murmur-storage
# (frozen, offline, vendored sources, staticlib overriding the manifest
# rlib crate-type). Two archives:
#   libmurmur_admin.a      the library (C ABI; linked into murmur-admin-tests)
#   libmurmur_admin_bin.a  the CLI entry point (provides `main`; linked with
#                          the library into the sourceless murmur-admin
#                          executable).
# Both crates are dependency-free (hand-written FFI only), so no feature
# flavors are needed.
find_program(MURMUR_RUSTUP rustup HINTS "$ENV{HOME}/.cargo/bin" REQUIRED)
set(MURMUR_TOOLCHAIN "stable" CACHE STRING "Installed modern Rust toolchain for Murmur")
execute_process(COMMAND ${MURMUR_RUSTUP} run ${MURMUR_TOOLCHAIN} cargo --version
  RESULT_VARIABLE murmur_toolchain_status OUTPUT_QUIET ERROR_QUIET)
if(NOT murmur_toolchain_status EQUAL 0)
  message(FATAL_ERROR "Murmur admin requires installed Rust ${MURMUR_TOOLCHAIN}; install it with rustup toolchain install ${MURMUR_TOOLCHAIN} --profile minimal")
endif()
set(admin_root "${PROJECT_SOURCE_DIR}/crates/murmur-admin")
set(admin_bin_root "${PROJECT_SOURCE_DIR}/crates/murmur-admin-bin")
set(admin_build "${PROJECT_BINARY_DIR}/murmur-admin-cargo")
file(MAKE_DIRECTORY "${admin_build}/cargo-home")
file(WRITE "${admin_build}/cargo-home/config.toml"
  "[source.crates-io]\nreplace-with = 'vendored-sources'\n[source.vendored-sources]\ndirectory = '${PROJECT_SOURCE_DIR}/crates/murmur-replication/vendor'\n[net]\noffline = true\n")
file(GLOB_RECURSE admin_sources CONFIGURE_DEPENDS "${admin_root}/src/*.rs")
set(admin_archive "${admin_build}/release/libmurmur_admin.a")
add_custom_command(OUTPUT "${admin_archive}"
  COMMAND ${CMAKE_COMMAND} -E env "CARGO_HOME=${admin_build}/cargo-home"
    "CARGO_TARGET_DIR=${admin_build}"
    ${MURMUR_RUSTUP} run ${MURMUR_TOOLCHAIN} cargo rustc --lib --release --frozen --manifest-path "${admin_root}/Cargo.toml" --crate-type staticlib
  DEPENDS ${admin_sources} "${admin_root}/Cargo.toml" "${PROJECT_SOURCE_DIR}/crates/Cargo.toml" "${PROJECT_SOURCE_DIR}/crates/Cargo.lock"
  WORKING_DIRECTORY "${admin_build}" VERBATIM)
add_custom_target(murmur_admin_build ALL DEPENDS "${admin_archive}")
add_library(admin_rust STATIC IMPORTED GLOBAL)
set_target_properties(admin_rust PROPERTIES IMPORTED_LOCATION "${admin_archive}")
add_dependencies(admin_rust murmur_admin_build)
file(GLOB_RECURSE admin_bin_sources CONFIGURE_DEPENDS "${admin_bin_root}/src/*.rs")
set(admin_bin_archive "${admin_build}/release/libmurmur_admin_bin.a")
add_custom_command(OUTPUT "${admin_bin_archive}"
  COMMAND ${CMAKE_COMMAND} -E env "CARGO_HOME=${admin_build}/cargo-home"
    "CARGO_TARGET_DIR=${admin_build}"
    ${MURMUR_RUSTUP} run ${MURMUR_TOOLCHAIN} cargo rustc --lib --release --frozen --manifest-path "${admin_bin_root}/Cargo.toml" --crate-type staticlib
  DEPENDS ${admin_bin_sources} ${admin_sources} "${admin_bin_root}/Cargo.toml" "${admin_root}/Cargo.toml" "${PROJECT_SOURCE_DIR}/crates/Cargo.toml" "${PROJECT_SOURCE_DIR}/crates/Cargo.lock"
  WORKING_DIRECTORY "${admin_build}" VERBATIM)
add_custom_target(murmur_admin_bin_build ALL DEPENDS "${admin_bin_archive}")
add_library(admin_bin_rust STATIC IMPORTED GLOBAL)
set_target_properties(admin_bin_rust PROPERTIES IMPORTED_LOCATION "${admin_bin_archive}")
add_dependencies(admin_bin_rust murmur_admin_bin_build murmur_admin_build)
