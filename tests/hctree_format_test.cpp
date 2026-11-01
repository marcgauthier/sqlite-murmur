#include "hctree_format.h"
#include <cassert>
#include <cstdint>
#include <cstring>

int main() {
    unsigned char header[hctree_format::kHeaderBytes]{};
    std::memcpy(header, hctree_format::kMagic, 32);
    std::int32_t page_size = 4096;
    std::int32_t files = 1;
    std::memcpy(header + 32, &page_size, sizeof(page_size));
    std::memcpy(header + 36, &files, sizeof(files));
    assert(hctree_format::valid_header(header, sizeof(header), 4096));
    assert(!hctree_format::valid_header(header, 16, 4096));
    assert(!hctree_format::valid_header(header, sizeof(header), 8192));
    assert(!hctree_format::valid_header(nullptr, sizeof(header), 4096));
    header[0] = 'h';
    assert(!hctree_format::valid_header(header, sizeof(header), 4096));
    header[0] = 'H';
    page_size = 0;
    std::memcpy(header + 32, &page_size, sizeof(page_size));
    assert(!hctree_format::valid_header(header, sizeof(header), 4096));
    page_size = 4096;
    std::memcpy(header + 32, &page_size, sizeof(page_size));
    files = 0;
    std::memcpy(header + 36, &files, sizeof(files));
    assert(!hctree_format::valid_header(header, sizeof(header), 4096));
    return 0;
}
