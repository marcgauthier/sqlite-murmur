cmake_minimum_required(VERSION 3.16)
# Works as part of the sqlite-murmur build or independently against an install.
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  project(sqlite_murmur_consumer LANGUAGES C CXX)
  find_package(sqlite_murmur CONFIG REQUIRED)
endif()
add_executable(sqlite_murmur_example main.c)
target_link_libraries(sqlite_murmur_example PRIVATE sqlite_murmur::sqlite_murmur)
# The implementation and its static dependencies include C++ code.
set_target_properties(sqlite_murmur_example PROPERTIES LINKER_LANGUAGE CXX)
