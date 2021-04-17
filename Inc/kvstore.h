#pragma once

#include "kvstore_api.h"
#include "MemTable.h"
#include "SSTable.h"

class KVStore : public KVStoreAPI {
    // You can add your implementation here
private:

    void removeDirectoryContentRecursively(std::string filePath);

    void mergeLevels();

    void handleLevelOverflow();

    void merge(const std::vector<uint64_t> &srcA_keys, const std::vector<std::string> &srcA_values, uint64_t timeStampA,
               const std::vector<uint64_t> &srcB_keys, const std::vector<std::string> &srcB_values, uint64_t timeStampB,
               std::vector<uint64_t> &dstKeys, std::vector<std::string> &dstValues);

    void
    splitIntoSSTables(std::vector<uint64_t> &srcKeys, std::vector<std::string> &srcValues, std::string &dstPath);

    void pushDown(unsigned int levelIndex);

    inline bool isOverflow(unsigned int levelIndex) const {
        return storeLevel.size() > levelIndex &&
               storeLevel[levelIndex].size() > 1 << (levelIndex + 1); //size > 2^(i+1) ?
    }

    std::string manageDir;
    MemTable *memTable;
    std::vector<std::list<SSTable *>> storeLevel;
    uint64_t nextTimeStamp;
public:
    KVStore(const std::string &dir);

    ~KVStore();

    uint64_t getEndNumber(const std::string &src);

    void getFileNamesUnderDirectory(std::string &filePath, const std::regex &pat, std::vector<std::string> &dst);

    void showMemTable(); //for Debug;

    void put(uint64_t key, const std::string &value) override;

    std::string get(uint64_t key) override;

    bool del(uint64_t key) override;

    void reset() override;

    SSTable *decodeSSTFile(const std::string &filePath);

    void MemTableToSSTable();

    void testGetValues(std::vector<std::string> &values) {
        if (storeLevel[0].size() > 0)(*storeLevel[0].begin())->getValues(values);
        std::cout << "result:" << std::endl;
        for (auto &value:values) {
            std::cout << value << " ";
        }
        std::cout << std::endl;
    }
};
