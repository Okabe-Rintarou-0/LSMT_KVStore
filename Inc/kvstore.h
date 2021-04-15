#pragma once

#include "kvstore_api.h"
#include "MemTable.h"
#include "SSTable.h"

class KVStore : public KVStoreAPI {
    // You can add your implementation here
private:
    void getFileNamesUnderDirectory(std::string &filePath, const std::regex &pat, std::vector<std::string> &dst);

    void removeDirectoryRecursively(std::string filePath);

    std::string manageDir;
    MemTable *memTable;
    std::vector<std::list<SSTable *>> storeLevel;
    uint64_t nextTimeStamp;
public:
    KVStore(const std::string &dir);

    ~KVStore();

    void showMemTable(); //for Debug;

    void put(uint64_t key, const std::string &value) override;

    std::string get(uint64_t key) override;

    bool del(uint64_t key) override;

    void reset() override;

    void writeData(const std::string &filePath, const std::vector<std::string> &values) const;

    SSTable *decodeSSTFile(const std::string &filePath);

    void MemTableToSSTable();
};
