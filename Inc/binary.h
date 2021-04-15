#ifndef LSM_TREE_BINARY_H
#define LSM_TREE_BINARY_H

#include <bits/stdc++.h>

void printBinary(const std::string &src, const std::string &dst) {
    std::ifstream in(src, std::ios::binary);
    std::ofstream out(dst, std::ios::out);
    while (!in.eof()) {
        uint8_t byte;
        in.read((char *) &byte, 1);
        for (int i = 0; i < 8; ++i) {
            out.put('0' + (byte & 1));
            byte >>= 1;
        }
        out.put(' ');
    }
}

#endif //LSM_TREE_BINARY_Ho