# cr-sqlite's no_std Rust 2023 archive owns a SQLite allocator. Isolate its
# unmangled runtime symbols from the modern std runtime in murmur-replication.
execute_process(COMMAND "${NM}" -g --defined-only "${INPUT}"
  OUTPUT_VARIABLE symbols RESULT_VARIABLE status)
if(NOT status EQUAL 0)
  message(FATAL_ERROR "Cannot inspect cr-sqlite Rust runtime")
endif()
string(REGEX MATCHALL "[ \t](__rust_[A-Za-z0-9_]+|rust_begin_unwind|rust_eh_personality|DW\\.ref\\.rust_eh_personality)(\n|$)" matches "${symbols}")
set(mapping "")
foreach(symbol IN LISTS matches)
  string(STRIP "${symbol}" symbol)
  string(APPEND mapping "${symbol} crsql_${symbol}\n")
endforeach()
string(REPLACE "\n" ";" entries "${mapping}")
list(FILTER entries EXCLUDE REGEX "^$")
list(REMOVE_DUPLICATES entries)
list(JOIN entries "\n" mapping)
file(WRITE "${OUTPUT}.symbols" "${mapping}\n")
execute_process(COMMAND "${OBJCOPY}" "--redefine-syms=${OUTPUT}.symbols" "${INPUT}" "${OUTPUT}"
  RESULT_VARIABLE status)
if(NOT status EQUAL 0)
  message(FATAL_ERROR "Cannot isolate cr-sqlite Rust runtime")
endif()
