#ifndef LSM_TREE_SSTABLE_H
#define LSM_TREE_SSTABLE_H

#include <bits/stdc++.h>
#include <cstdint>

#define SETSIZE 10240

class BloomFilter {
public:
    uint8_t set[SETSIZE] = {0}; //SETSIZE is 10240,namely 10240 Byte = 10 KB
    void put(uint64_t key);

    bool find(uint64_t key);

    void setBit(unsigned int index);

    uint8_t getBit(unsigned int index);

    void printSet(unsigned int begin, unsigned int end); //for Debug;
};

class SSTable {
public:
    SSTable() = default;

    SSTable(uint64_t timeStamp, const std::vector<uint64_t> &orderedKey, const std::vector<std::string> &value);

    void write(const std::string &filePath);

    void putKeyOffsetPair(uint64_t key, uint32_t offset);

    void setHeader(uint64_t timeStamp, uint64_t entryNumber, int64_t minKey, int64_t maxKey);

private:
    struct Header {
        Header() : timeStamp(0), entryNumber(0), minKey(0), maxKey(0) {}

        uint64_t timeStamp; //时间戳
        uint64_t entryNumber; //键值对数量
        int64_t minKey; //最小键值
        int64_t maxKey; //最大键值
    };

    struct KeyOffsetPair {
        KeyOffsetPair(uint64_t _key, uint32_t _offset) : key(_key), offset(_offset) {}

        uint64_t key;
        uint32_t offset;
    };

    Header header;
    BloomFilter bloomFilter;
    std::vector<KeyOffsetPair> keyOffsetPairs;
    std::vector<std::string> data;
};


#endif //LSM_TREE_SSTABLE_H