#pragma once

#include "kvstore_api.h"
#include "MemTable.h"
#include "SSTable.h"

class KVStore : public KVStoreAPI {
    // You can add your implementation here
private:

    void removeDirectoryContentRecursively(std::string filePath);

    void mergeLevels(int levelIndex);

    void handleLevelOverflow();

    void merge(const std::vector<uint64_t> &srcA_keys, const std::vector<std::string> &srcA_values, uint64_t timeStampA,
               const std::vector<uint64_t> &srcB_keys, const std::vector<std::string> &srcB_values, uint64_t timeStampB,
               std::vector<uint64_t> &dstKeys, std::vector<std::string> &dstValues);

    void
    splitIntoSSTables(std::vector<uint64_t> &srcKeys, std::vector<std::string> &srcValues, std::string &dstPath,
                      int levelIndex);

    uint64_t getEndNumber(const std::string &src);

    inline bool isOverflow(unsigned int levelIndex) const {
        return storeLevel.size() > levelIndex &&
               storeLevel[levelIndex].size() > 1 << (levelIndex + 1); //size > 2^(i+1) ?
    }

    SSTable *decodeSSTFile(const std::string &filePath);

    std::string manageDir;
    MemTable *memTable;
    std::vector<std::list<SSTable *>> storeLevel;
    uint64_t nextTimeStamp;
public:
    KVStore(const std::string &dir);

    ~KVStore();

    void getFileNamesUnderDirectory(std::string &filePath, const std::regex &pat, std::vector<std::string> &dst);

    void showMemTable(); //for Debug;

    void put(uint64_t key, const std::string &value) override;

    std::string get(uint64_t key) override;

    std::string getForTest0(uint64_t key);

    bool del(uint64_t key) override;

    void reset() override;

    void MemTableToSSTable();

};
