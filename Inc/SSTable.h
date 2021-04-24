#ifndef LSM_TREE_SSTABLE_H
#define LSM_TREE_SSTABLE_H

#include <bits/stdc++.h>
#include <cstdint>

#define SETSIZE 10240

class BloomFilter {
public:
    uint8_t set[SETSIZE] = {0}; //SETSIZE is 10240,namely 10240 Byte = 10 KB

    inline void unionSet(uint8_t srcSet[SETSIZE]) { //union two sets
        for (int i = 0; i < SETSIZE; ++i) {
            set[i] |= srcSet[i];
        }
    }

    void put(uint64_t key);

    bool find(uint64_t key) const;

    void setBit(unsigned int index);

    uint8_t getBit(unsigned int index) const;

    void printSet(unsigned int begin, unsigned int end) const; //for Debug;
};

class SSTable {
public:
    SSTable(std::string &directoryPath);

    SSTable(std::string &directoryPath, uint64_t timeStamp, const std::vector<uint64_t> &orderedKey,
            const std::vector<std::string> &value);

    std::string get(uint64_t key) const;

    std::string getForTest(uint64_t key) const;

    void write(const std::vector<std::string> &data) const;

    void putKeyOffsetPair(uint64_t key, uint32_t offset);

    void setHeader(uint64_t timeStamp, uint64_t entryNumber, int64_t minKey, int64_t maxKey);

    inline void setTimeStamp(uint64_t timeStamp) { header.timeStamp = timeStamp; }

    void setBloomFilter(void *bytes);

    void getKeys(std::vector<uint64_t> &dst) const;

    void getValues(std::vector<std::string> &dst) const;

    inline void setDirectoryPath(const std::string &directoryPath) {
        this->directoryPath = directoryPath;
        if (*(this->directoryPath.rbegin()) != '/')
            this->directoryPath.append("/");
    }

    inline std::string getFilePath() const {
        auto filePath = groupId ? directoryPath + std::to_string(header.timeStamp) + "_" + std::to_string(groupId) +
                                  ".sst" : directoryPath + std::to_string(header.timeStamp) + ".sst";
        return filePath;
    }

    inline uint64_t getTimeStamp() const { return header.timeStamp; }

    inline uint64_t getEntryNumber() const { return header.entryNumber; }

    inline int64_t getMinKey() const { return header.minKey; }

    inline int64_t getMaxKey() const { return header.maxKey; }

    inline void setGroupId(int _groupId) { groupId = _groupId; }

private:

    void readData(const std::string &filePath, std::string &dst, uint32_t thisOffset, uint32_t nextOffset) const;

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
    int groupId = 0;
    std::string directoryPath;
};


#endif //LSM_TREE_SSTABLE_H
