#pragma once
// The pinned HC-tree source writes this exact native-endian 40-byte prefix
// in hctFileInitHdr() (src/hct_file.c). This is NOT a SQLite format-3 file.
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace hctree_format {
constexpr char kMagic[] = "Hctree database version 00000001";
static_assert(sizeof(kMagic) - 1 == 32, "HC-tree signature must be 32 bytes");
constexpr std::size_t kHeaderBytes = 40;

inline bool valid_header(const unsigned char *header, std::size_t bytes,
                         std::uint32_t expected_page_size) {
    if (!header || bytes < kHeaderBytes ||
        std::memcmp(header, kMagic, sizeof(kMagic) - 1) != 0)
        return false;
    std::int32_t page_size = 0;
    std::int32_t database_files = 0;
    std::memcpy(&page_size, header + 32, sizeof(page_size));
    std::memcpy(&database_files, header + 36, sizeof(database_files));
    return page_size > 0 &&
           static_cast<std::uint32_t>(page_size) == expected_page_size &&
           database_files >= 1;
}
} // namespace hctree_format
