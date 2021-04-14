#include "../Inc/kvstore.h"
#include "utils.h"
#include <bits/stdc++.h>

KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir), nextTimeStamp(1) {
    if (mkdir(dir.c_str())) {
        std::cout << "创建目录 \"" << dir << "\"成功！" << std::endl;
    } else
        std::cout << "创建目录失败！" << std::endl;
    memTable = new MemTable;
    storeLevel.emplace_back(std::list<SSTable *>()); //Form level 0;
}

KVStore::~KVStore() {
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &value) {
    memTable->put(key, value);
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key) {
    std::string *value = memTable->get(key);
    if (value) {
        std::cout << "Find key " << key << " in MemTable with value: " << *value << std::endl;
        return *value;
    } else
        std::cout << "Key " << key << " is not in MemTable!" << std::endl;
    return "";
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key) {
    if (memTable->remove(key)) {
        std::cout << "Delete key " << key << " in MemTable" << std::endl;
        return true;
    } else
        std::cout << "Key " << key << " is not in MemTable now" << std::endl;
    return false;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
}

void KVStore::showMemTable() {
    if (memTable)
        memTable->traverse();
}

void KVStore::writeData(const std::string &filePath, const std::vector<std::string> &values) const {
    std::ofstream file(filePath, std::ios::binary | std::ios::app);
    if (!file.is_open()) {
        std::cout << "File open failed." << std::endl;
        return;
    }
    for (auto &value:values) {
        file.write(value.c_str(), value.size());
    }
    file.close();
}

void KVStore::MemTableToSSTable() {
    std::vector<uint64_t> orderedKey;
    std::vector<std::string> value;
    memTable->getOrderedEntries(orderedKey, value);
    auto ssTable = new SSTable(nextTimeStamp, orderedKey, value);
    storeLevel[0].push_back(ssTable);
    std::string filePath = "../store/level-0/" + std::to_string(nextTimeStamp++) + ".sst";
    ssTable->write(filePath);
    writeData(filePath, value);
    decodeSSTFile(filePath); //for Debug;
    delete memTable;
    memTable = new MemTable;
}

SSTable *KVStore::decodeSSTFile(const std::string &filePath) {
    std::cout << "Now start decoding..." << std::endl;
    std::ifstream file(filePath, std::ios::binary);
    if (!file.is_open()) {
        std::cout << "Decode failed! Invalid Filepath!" << std::endl;
        return nullptr;
    }
    auto ssTable = new SSTable;

    uint64_t timeStamp, entryNumber;
    int64_t minKey, maxKey;
    file.read((char *) &timeStamp, 8);
    std::cout << "The time stamp is: " << timeStamp << std::endl;
    file.read((char *) &entryNumber, 8);
    std::cout << "The entryNumber is: " << timeStamp << std::endl;
    file.read((char *) &minKey, 8);
    std::cout << "The minKey is: " << timeStamp << std::endl;
    file.read((char *) &maxKey, 8);
    std::cout << "The maxKey is: " << timeStamp << std::endl;
    ssTable->setHeader(timeStamp, entryNumber, minKey, maxKey);

    //read bloom filter;

    std::vector<uint64_t> orderedKey;
    std::vector<std::string> values;
    uint64_t thisKey;
    uint32_t thisOffset;
    file.read((char *) &thisKey, 8);
    file.read((char *) &thisOffset, 4);
    std::cout << "Read key " << thisKey << std::endl;
    std::cout << "Read offset " << thisOffset << std::endl;
    ssTable->putKeyOffsetPair(thisKey, thisOffset);
    size_t indexOffsetPairSize = thisOffset;
    size_t presentSize = 12;
    while (presentSize < indexOffsetPairSize) {
        file.read((char *) &thisKey, 8);
        file.read((char *) &thisOffset, 4);
        std::cout << "Read key " << thisKey << std::endl;
        std::cout << "Read offset " << thisOffset << std::endl;
        presentSize += 12;
    }
}

