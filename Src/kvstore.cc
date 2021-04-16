#include "../Inc/kvstore.h"
#include "utils.h"
#include "setting.h"
#include <io.h>
#include <bits/stdc++.h>
#include <algorithm>

void KVStore::removeDirectoryContentRecursively(std::string filePath) {
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
#ifdef DEBUG
            std::cout << "Remove file: " << thisFileName << std::endl;
#endif
            auto thisFilePath = filePath + thisFileName;
            if (file.attrib == _A_SUBDIR) {
                removeDirectoryContentRecursively(thisFilePath);
                utils::rmdir(thisFilePath.c_str());
            } else
                utils::rmfile(thisFilePath.c_str());
        }
        result = _findnext(handle, &file);
    } while (result != -1);
}

void KVStore::getFileNamesUnderDirectory(std::string &filePath, const std::regex &pat,
                                         std::vector<std::string> &dst) {
//    std::cout << "Now begin searching in file path: " << filePath << std::endl;
    _finddata_t file{};
    if (*filePath.rbegin() != '/')
        filePath.append("/");//add '/'
    std::string searchDir = filePath + "*.*";
    auto handle = _findfirst(searchDir.c_str(), &file);
    long result;
    do {
        std::string thisFileName(file.name);
//        std::cout << "This file name: " << thisFileName << std::endl;
        if (std::regex_match(thisFileName, pat)) {
            dst.push_back(filePath + thisFileName);
//            std::cout << "Exists file: " << thisFileName << std::endl;
        }
        result = _findnext(handle, &file);
    } while (result != -1);
}

KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir), manageDir(dir), nextTimeStamp(1) {
    if (utils::mkdir(dir.c_str())) {
        std::cout << "创建目录 \"" << dir << "\"成功" << std::endl;
    } else
        std::cout << "创建目录失败！目录可能已存在" << std::endl;
    if (*manageDir.rbegin() != '/')manageDir.append("/");
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
//            std::cout << "SST file: " << sstFilePath << std::endl;
            std::string targetFileName = std::to_string(nextTimeStamp) + ".sst";
//            std::cout << "Renaming to " << targetFileName << "..." << std::endl;
            std::string newFilePath = std::regex_replace(sstFilePath, pat, targetFileName);
            rename(sstFilePath.c_str(), newFilePath.c_str());
            auto ssTable = decodeSSTFile(newFilePath);
            ssTable->setTimeStamp(nextTimeStamp++);
            storeLevel[levelSize].push_back(ssTable);
        }
    }
}

KVStore::~KVStore() {
    MemTableToSSTable();
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
#ifdef DEBUG
        std::cout << "Find key " << key << " in MemTable with value: " << *value << std::endl;
#endif
        if (*value != "~DELETED~")return *value;
        else return "";
    } else {
#ifdef DEBUG
        std::cout << "Key " << key << " is not in MemTable!" << std::endl;
#endif
    }
    auto levelSize = storeLevel.size();

    for (int i = 0; i < levelSize; ++i) {
#ifdef DEBUG
        std::cout << "Now begin searching key " << key << " in level " << i << "..." << std::endl;
#endif
        std::string filePathSuffix = "../store/level-" + std::to_string(i) + "/";
        std::list<SSTable *> &thisLevel = storeLevel[i];
        for (auto it = thisLevel.rbegin(); it != thisLevel.rend(); ++it) { //reverse,cause the timeStamp is ↑
            auto thisSSTable = *it;
            std::string thisFilePath = filePathSuffix + std::to_string(thisSSTable->getTimeStamp()) + ".sst";
#ifdef DEBUG
            std::cout << "Now begin searching key " << key << " in SSTable with file path: " << thisFilePath << " ..."
                      << std::endl;
#endif
            auto thisValue = thisSSTable->get(key);
            if (!thisValue.empty()) {
                if (thisValue != "~DELETED")
                    return thisValue;
                else return "";
            }
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
#ifdef DEBUG
        std::cout << "Delete key " << key << " in MemTable" << std::endl;
#endif
        memTable->put(key, "~DELETED~");
        return true;
    } else {
#ifdef DEBUG
        std::cout << "Key " << key << " is not in MemTable now" << std::endl;
#endif
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
        removeDirectoryContentRecursively(directory);
        utils::rmdir(directory.c_str());
    }
    memTable->clear();
    storeLevel.clear();
    nextTimeStamp = 1;
}

void KVStore::showMemTable() {
    if (memTable)
        memTable->traverse();
}

void
KVStore::merge(const std::vector<uint64_t> &srcA_keys, const std::vector<std::string> &srcA_values, uint64_t timeStampA,
               const std::vector<uint64_t> &srcB_keys, const std::vector<std::string> &srcB_values, uint64_t timeStampB,
               std::vector<uint64_t> &dstKeys, std::vector<std::string> &dstValues) {
    auto srcA_keyNumber = srcA_keys.size();
    auto srcB_keyNumber = srcB_keys.size();
    unsigned int i = 0, j = 0;
    while (i < srcA_keyNumber && j < srcB_keyNumber) {
        auto thisKeyA = srcA_keys[i];
        auto thisKeyB = srcB_keys[j];
//        std::cout << "merging... compare keyA " << thisKeyA << " to keyB " << thisKeyB << std::endl;
        uint64_t thisKey; //the key need to be pushed
        std::string thisValue; //the value need to be pushed
        if (thisKeyA < thisKeyB || (thisKeyA == thisKeyB && timeStampA > timeStampB)) {
            thisKey = thisKeyA;
            thisValue = srcA_values[i];
            ++i;
        } else {
            thisKey = thisKeyB;
            thisValue = srcB_values[j];
            ++j;
        }
//        std::cout << "merging... Push key " << thisKey << " and value " << thisValue << std::endl;
        dstKeys.push_back(thisKey);
        dstValues.push_back(thisValue);
    }
    for (; i < srcA_keyNumber; ++i) {
        dstKeys.push_back(srcA_keys[i]);
        dstValues.push_back(srcA_values[i]);
    }
    for (; j < srcB_keyNumber; ++j) {
        dstKeys.push_back(srcB_keys[j]);
        dstValues.push_back(srcB_values[j]);
    }
}

void KVStore::splitIntoSSTables(std::vector<uint64_t> &srcKeys, std::vector<std::string> &srcValues,
                                std::string &dstPath) {
    size_t presentSize = 0;
    int begin = 0;
    int keyNumber = srcValues.size();
    for (int i = 0; i < keyNumber; ++i) {
        auto value = srcValues[i];
        presentSize += 12 + value.size();
#ifdef DEBUG
        std::cout << "merging... Now merging size is: " << presentSize << std::endl;
#endif
        if (presentSize > 2097152) { //overflow 2MB
            std::vector<uint64_t> keys;
            std::vector<std::string> values;
            for (int j = begin; j < i; ++j) {
                keys.push_back(srcKeys[i]);
                values.push_back(srcValues[i]);
            }
            begin = i;
            auto newSSTable = new SSTable(dstPath, nextTimeStamp++, keys, values);
            newSSTable->write(values); ///TODO simplify?
            storeLevel[1].push_back(newSSTable);
            presentSize = 12 + values[i].size();
        }
    }
    std::vector<uint64_t> keys;
    std::vector<std::string> values;
    for (int i = begin; i < keyNumber; ++i) {
        keys.push_back(srcKeys[i]);
        values.push_back(srcValues[i]);
    }
    auto newSSTable = new SSTable(dstPath, nextTimeStamp++, keys, values);
    newSSTable->write(values);
    storeLevel[0].clear();
    storeLevel[1].push_back(newSSTable);
}

void KVStore::mergeLevels() {
//    std::cout << "Now merge level 0 and level 1..." << std::endl;
    std::vector<SSTable *> needMerge;
    //first statistic the key range of level 0
    std::list<SSTable *> &level_0 = storeLevel[0];
    int64_t rangeMin = std::numeric_limits<int64_t>::max();
    int64_t rangeMax = std::numeric_limits<int64_t>::min();
    for (auto &thisSSTable : level_0) {
        rangeMin = std::min(rangeMin, thisSSTable->getMinKey());
        rangeMax = std::max(rangeMax, thisSSTable->getMaxKey());
        needMerge.push_back(thisSSTable);
    }
    //first statistic the key range of level 0


    if (storeLevel.size() >= 2) { //Search in level 1
        std::list<SSTable *> &level_1 = storeLevel[1];
        for (auto &thisSSTable : level_1) {
            if (rangeMin <= thisSSTable->getMaxKey() || rangeMax >= thisSSTable->getMinKey())
                //in range,thus needing to merge
                needMerge.push_back(thisSSTable);
        }
    } else {
        storeLevel.emplace_back(); //add level 1
        utils::mkdir((manageDir + "/level-1").c_str());
    }

    std::vector<uint64_t> presentKeys;
    std::vector<std::string> presentValues;
    size_t presentSize = 0;
    auto targetDirectoryPath = manageDir + "/level-1";

    needMerge[0]->getKeys(presentKeys);
    needMerge[0]->getValues(presentValues);
    uint64_t presentTimeStamp = needMerge[0]->getTimeStamp();

//    SSTable *formedSSTable = nullptr;
    for (int i = 1; i < needMerge.size(); ++i) {
        auto thisSSTable = needMerge[i];
        auto thisTimeStamp = thisSSTable->getTimeStamp();
        std::vector<uint64_t> thisKeys;
        thisSSTable->getKeys(thisKeys);
        std::vector<std::string> thisValues;
        std::vector<uint64_t> mergedKeys;
        std::vector<std::string> mergedValues;
        thisSSTable->getValues(thisValues);
        merge(presentKeys, presentValues, presentTimeStamp, thisKeys, thisValues, thisTimeStamp, mergedKeys,
              mergedValues);


        presentTimeStamp = std::max(thisTimeStamp, presentTimeStamp);
        presentKeys = mergedKeys;
        presentValues = mergedValues;
    }
    splitIntoSSTables(presentKeys, presentValues, targetDirectoryPath);
    removeDirectoryContentRecursively(manageDir + "level-0");
}

void KVStore::pushDown(unsigned int levelIndex) {
    auto &thisLevel = storeLevel[levelIndex];
    auto pushDownSSTable = *thisLevel.begin();
    thisLevel.pop_front();
    auto newDirectoryPath = (manageDir + "level-" + std::to_string(levelIndex + 1));
    auto newPath = (newDirectoryPath + "/" + std::to_string(pushDownSSTable->getTimeStamp()) + ".sst");
    utils::mkdir(newDirectoryPath.c_str());
//    std::cout << "Rename " << pushDownSSTable->getFilePath() << " to " << newPath << std::endl;
    rename(pushDownSSTable->getFilePath().c_str(), newPath.c_str());
    pushDownSSTable->setDirectoryPath(newDirectoryPath);
    if (storeLevel.size() <= levelIndex + 1)
        storeLevel.emplace_back();
    storeLevel[levelIndex + 1].push_back(pushDownSSTable);
}

void KVStore::handleLevelOverflow() {
    mergeLevels();
    unsigned int levelIndex = 1;
    while (isOverflow(levelIndex)) {
//        std::cout << "This level: " << levelIndex << std::endl;
        do {
            pushDown(levelIndex);
        } while (isOverflow(levelIndex));
        ++levelIndex;
    }
}

void KVStore::MemTableToSSTable() {
    if (memTable->isEmpty())return;
    std::vector<uint64_t> orderedKey;
    std::vector<std::string> value;
    memTable->getOrderedEntries(orderedKey, value);
    auto levelPath = manageDir + "level-0";
    utils::mkdir(levelPath.c_str());
    if (storeLevel.empty())storeLevel.assign(1, std::list<SSTable *>());
    auto ssTable = new SSTable(levelPath, nextTimeStamp++, orderedKey, value);
    /*----------------Form SSTable-----------------*/
    storeLevel[0].push_back(ssTable);
    ssTable->write(value);
    /*----------------Form SSTable-----------------*/

    /*----------------Handle Overflow-----------------*/
    if (isOverflow(0))
        handleLevelOverflow();
    /*----------------Handle Overflow-----------------*/
//    printBinary(filePath, "../store/level-0/bin.txt");
//    decodeSSTFile(filePath); //for Debug;
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
    auto directoryPath = std::regex_replace(filePath, std::regex("[0-9]+.sst"), "");
    auto ssTable = new SSTable(directoryPath);

    //read header
    uint64_t timeStamp, entryNumber;
    int64_t minKey, maxKey;
    file.read((char *) &timeStamp, 8);
    file.read((char *) &entryNumber, 8);
    file.read((char *) &minKey, 8);
    file.read((char *) &maxKey, 8);
#ifdef DEBUG
    std::cout << "The entryNumber is: " << entryNumber << std::endl;
    std::cout << "The minKey is: " << minKey << std::endl;
    std::cout << "The time stamp is: " << timeStamp << std::endl;
    std::cout << "The maxKey is: " << maxKey << std::endl;
#endif
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
#ifdef DEBUG
    std::cout << "Read key " << thisKey << std::endl;
    std::cout << "Read offset " << thisOffset << std::endl;
#endif
    ssTable->putKeyOffsetPair(thisKey, thisOffset);
    size_t indexOffsetPairSize = thisOffset;
    size_t presentSize = 12;
    while (presentSize < indexOffsetPairSize) {
        file.read((char *) &thisKey, 8);
        file.read((char *) &thisOffset, 4);
#ifdef DEBUG
        std::cout << "Read key " << thisKey << std::endl;
        std::cout << "Read offset " << thisOffset << std::endl;
#endif
        ssTable->putKeyOffsetPair(thisKey, thisOffset);
        presentSize += 12;
    }
    //read key and offset

    //For debug: show the data
#ifdef DEBUG
    std::cout << "Data: ";
    while (true) {
        char ch;
        file.get(ch);
        if (file.eof())break;
        std::cout << ch << " ";
    }
    std::cout << std::endl;
#endif
    return ssTable;
}

