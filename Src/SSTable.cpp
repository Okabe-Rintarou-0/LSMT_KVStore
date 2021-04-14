#include "../Inc/SSTable.h"
#include "MurmurHash3.h"
#include <iostream>

void BloomFilter::setBit(unsigned int index) {
    unsigned int byteIndex = index / 8; //位于第几个byte？
    unsigned int bitIndex = index % 8; //第几个bit?
    set[byteIndex] |= (1 << bitIndex);
}

uint8_t BloomFilter::getBit(unsigned int index) {
    unsigned int byteIndex = index / 8; //位于第几个byte？
    unsigned int bitIndex = index % 8; //第几个bit?
    return (set[byteIndex] >> bitIndex) & 1;
}

void BloomFilter::put(uint64_t key) {
    unsigned int hash[4] = {0};
    MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
    for (unsigned int item : hash)
        setBit(item % SETSIZE);
}

void BloomFilter::printSet(unsigned int begin, unsigned int end) {
    for (int i = begin; i < end; ++i) {
        uint8_t thisByte = set[i];
        for (int j = 0; j < 8; ++j) {
            std::cout << ((thisByte >> j) & 1) << " ";
        }
    }
    std::cout << std::endl;
};

bool BloomFilter::find(uint64_t key) {
    unsigned int hash[4] = {0};
    MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
    for (unsigned int item : hash) {
        uint8_t bit = getBit(item % SETSIZE);
        if (bit == 0)
            return false;
    }
    return true;
}

SSTable::SSTable(uint64_t timeStamp, const std::vector<uint64_t> &orderedKey, const std::vector<std::string> &value) {
    int keyNumber = orderedKey.size();
    uint32_t lastOffset = keyNumber * 12; //12 = sizeof(uint64_t) + sizeof(uint32_t),namely key + offset;
    uint32_t lastValueSize = 0;

    header.timeStamp = timeStamp;
    header.entryNumber = keyNumber;
    header.minKey = orderedKey[0];
    header.maxKey = orderedKey[keyNumber - 1];

    for (int i = 0; i < keyNumber; ++i) {
        auto thisKey = orderedKey[i];
        auto thisValue = value[i];
        bloomFilter.put(thisKey);

        auto thisOffset = lastOffset + lastValueSize;
        std::cout << "Put key " << thisKey << std::endl;
        std::cout << "Put offset " << thisOffset << std::endl;
        keyOffsetPairs.emplace_back(KeyOffsetPair(thisKey, thisOffset));
        lastOffset = thisOffset;
        lastValueSize = thisValue.size();
    }
}

void SSTable::write(const std::string &filePath) {
    std::ofstream file(filePath, std::ios::binary | std::ios::app);
    if (!file.is_open()) {
        std::cout << "File open failed." << std::endl;
        return;
    }
    file.write((const char *) &header.timeStamp, 8);
    file.write((const char *) &header.entryNumber, 8);
    file.write((const char *) &header.minKey, 8);
    file.write((const char *) &header.maxKey, 8);

    file.write((const char *) bloomFilter.set, SETSIZE);

    for (auto &pair:keyOffsetPairs) {
        file.write((const char *) &pair.key, 8);
        file.write((const char *) &pair.offset, 4);
    }
}

void SSTable::setHeader(uint64_t timeStamp, uint64_t entryNumber, int64_t minKey, int64_t maxKey) {
    header.timeStamp = timeStamp;
    header.entryNumber = entryNumber;
    header.minKey = minKey;
    header.maxKey = maxKey;
}

void SSTable::putKeyOffsetPair(uint64_t key, uint32_t offset) {
    keyOffsetPairs.emplace_back(KeyOffsetPair(key, offset));
}
