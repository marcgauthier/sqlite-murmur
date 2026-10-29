// murmur-admin entry point (WP-D slice D3a). All logic lives in admin.cpp
// so the ctest suite can exercise commands in-process.

#include "admin.h"

#include <iostream>

int main(int argc, char **argv) {
    return admin_main(argc, argv, std::cout, std::cerr);
}
