#include "../Inc/kvstore.h"
#include "utils.h"
#include "binary.h"
#include <io.h>
#include <bits/stdc++.h>

void KVStore::removeDirectoryRecursively(std::string filePath) {
    _finddata_t file{};
    if (*filePath.rbegin() != '/')
        filePath.append("/");//add '/'
    std::string searchDir = filePath + "*.*";
    auto handle = _findfirst(searchDir.c_str(), &file);
    long result;
    do {
        std::string thisFileName(file.name);
//        std::cout << "This file name: " << thisFileName << std::endl;
        if (thisFileName != "." && thisFileName != "..") {
            std::cout << "Remove file: " << thisFileName << std::endl;
            if (file.attrib == _A_SUBDIR)
                removeDirectoryRecursively(filePath + thisFileName);
            else
                utils::rmfile((filePath + thisFileName).c_str());
        }
        result = _findnext(handle, &file);
    } while (result != -1);
    utils::rmdir(filePath.c_str());
}

void KVStore::getFileNamesUnderDirectory(std::string &filePath, const std::regex &pat,
                                         std::vector<std::string> &dst) {
    std::cout << "Now begin searching in file path: " << filePath << std::endl;
    _finddata_t file{};
    if (*filePath.rbegin() != '/')
        filePath.append("/");//add '/'
    std::string searchDir = filePath + "*.*";
    std::cout << "searchDir: " << searchDir << std::endl;
    auto handle = _findfirst(searchDir.c_str(), &file);
    long result;
    do {
        std::string thisFileName(file.name);
//        std::cout << "This file name: " << thisFileName << std::endl;
        if (std::regex_match(thisFileName, pat)) {
            dst.push_back(filePath + thisFileName);
            std::cout << "Exists file: " << thisFileName << std::endl;
        }
        result = _findnext(handle, &file);
    } while (result != -1);
}

KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir), manageDir(dir), nextTimeStamp(1) {
    if (utils::mkdir(dir.c_str())) {
        std::cout << "创建目录 \"" << dir << "\"成功" << std::endl;
    } else
        std::cout << "创建目录失败！目录可能已存在" << std::endl;

    memTable = new MemTable;
    std::cout << "Check the directories under directory " << dir << "..." << std::endl;
    std::vector<std::string> directories;
    std::string searchPath = dir;
    getFileNamesUnderDirectory(searchPath, std::regex("level-[0-9]+"), directories);

    auto levelSize = directories.size();
    storeLevel.assign(levelSize, std::list<SSTable *>());

    auto thisLevelIndex = levelSize;
    for (auto it = directories.rbegin(); it != directories.rend(); ++it) {
        auto thisFilePath = *it;
        --levelSize;
        std::vector<std::string> ssTableFilePath;
        std::regex pat("[0-9]+\\.sst");
        getFileNamesUnderDirectory(thisFilePath, pat, ssTableFilePath);
        for (auto &sstFilePath:ssTableFilePath) {
            std::cout << "SST file: " << sstFilePath << std::endl;
            std::string targetFileName = std::to_string(nextTimeStamp) + ".sst";
            std::cout << "Renaming to " << targetFileName << "..." << std::endl;
            std::string newFilePath = std::regex_replace(sstFilePath, pat, targetFileName);
            rename(sstFilePath.c_str(), newFilePath.c_str());
            auto ssTable = decodeSSTFile(newFilePath);
            ssTable->setTimeStamp(nextTimeStamp++);
            storeLevel[levelSize].push_back(ssTable);
        }
    }
}

KVStore::~KVStore() {
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &value) {
    memTable->put(key, value);
    if (memTable->isOverflow())
        MemTableToSSTable(); // If the size of MemTable has overflowed, then revert it to SSTable.
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

    auto levelSize = storeLevel.size();

    for (int i = 0; i < levelSize; ++i) {
        std::cout << "Now begin searching key " << key << " in level " << i << "..." << std::endl;
        std::string filePathSuffix = "../store/level-" + std::to_string(i) + "/";
        std::list<SSTable *> &thisLevel = storeLevel[i];
        for (auto it = thisLevel.rbegin(); it != thisLevel.rend(); ++it) { //reverse,cause the timeStamp is ↑
            auto thisSSTable = *it;
            std::string thisFilePath = filePathSuffix + std::to_string(thisSSTable->getTimeStamp()) + ".sst";
            std::cout << "Now begin searching key " << key << " in SSTable with file path: " << thisFilePath << " ..."
                      << std::endl;
            auto thisValue = thisSSTable->get(thisFilePath, key);
            if (!thisValue.empty())return thisValue;
        }
    }
    return "";
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key) {
    if (memTable->remove(key)) {
        std::cout << "Delete key " << key << " in MemTable" << std::endl;
        memTable->put(key, "~DELETED~");
        return true;
    } else {
        std::cout << "Key " << key << " is not in MemTable now" << std::endl;
        memTable->put(key, "~DELETED~");
        return false;
    }
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
///TODO: 1.Realize MemTable::clear() to clear entries;
void KVStore::reset() {
    std::vector<std::string> directories;
    getFileNamesUnderDirectory(manageDir, std::regex("level-[0-9]+"), directories);
    for (auto &directory:directories) {
        removeDirectoryRecursively(directory);
    }
    storeLevel.clear();
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
    if (memTable->isEmpty())return;
    std::vector<uint64_t> orderedKey;
    std::vector<std::string> value;
    memTable->getOrderedEntries(orderedKey, value);
    auto ssTable = new SSTable(nextTimeStamp, orderedKey, value);
    auto levelPath = *manageDir.rbegin() == '/' ? manageDir + "level-0" : manageDir + "/level-0";
    utils::mkdir(levelPath.c_str());
    storeLevel[0].push_back(ssTable);
    std::string filePath = "../store/level-0/" + std::to_string(nextTimeStamp++) + ".sst";
    ssTable->write(filePath);
    writeData(filePath, value);
    printBinary(filePath, "../store/level-0/bin.txt");
    decodeSSTFile(filePath); //for Debug;
    delete memTable;
    memTable = new MemTable;
}

SSTable *KVStore::decodeSSTFile(const std::string &filePath) {
    std::cout << "Now start decoding with file path " << filePath << "..." << std::endl;
    std::ifstream file(filePath, std::ios::binary);
    if (!file.is_open()) {
        std::cout << "Decode failed! Invalid Filepath!" << std::endl;
        return nullptr;
    }
    auto ssTable = new SSTable;

    //read header
    uint64_t timeStamp, entryNumber;
    int64_t minKey, maxKey;
    file.read((char *) &timeStamp, 8);
    std::cout << "The time stamp is: " << timeStamp << std::endl;
    file.read((char *) &entryNumber, 8);
    std::cout << "The entryNumber is: " << entryNumber << std::endl;
    file.read((char *) &minKey, 8);
    std::cout << "The minKey is: " << minKey << std::endl;
    file.read((char *) &maxKey, 8);
    std::cout << "The maxKey is: " << maxKey << std::endl;
    ssTable->setHeader(timeStamp, entryNumber, minKey, maxKey);
    //read header

    //read bloom filter;
    uint8_t bloomFilter[SETSIZE];
    file.read((char *) &bloomFilter, SETSIZE);
    ssTable->setBloomFilter(bloomFilter);
    //read bloom filter;

    //read key and offset
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
        ssTable->putKeyOffsetPair(thisKey, thisOffset);
        presentSize += 12;
    }
    //read key and offset

    //For debug: show the data
    std::cout << "Data: ";
    while (true) {
        char ch;
        file.get(ch);
        if (file.eof())break;
        std::cout << ch << " ";
    }
    std::cout << std::endl;

    return ssTable;
}

