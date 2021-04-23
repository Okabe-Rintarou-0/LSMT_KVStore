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

//it will get the number before .sst
//for example,getEndNumber("data/level-0/220.sst") will return integer 220
uint64_t KVStore::getEndNumber(const std::string &src) {
    uint64_t figures = 1;
    uint64_t dst = 0;
    for (auto it = src.rbegin() + 4; it != src.rend(); ++it) {
        char ch = *it;
        if (ch > '9' || ch < '0')break;
        dst += (ch - '0') * figures;
        figures *= 10;
    }
    return dst;
}

void KVStore::getFileNamesUnderDirectory(std::string &filePath, const std::regex &pat,
                                         std::vector<std::string> &dst) {
    _finddata_t file{};
    if (*filePath.rbegin() != '/')
        filePath.append("/");//add '/'
    std::string searchDir = filePath + "*.*";
    auto handle = _findfirst(searchDir.c_str(), &file);
    long result;
    do {
        std::string thisFileName(file.name);
        if (std::regex_match(thisFileName, pat)) {
            dst.push_back(filePath + thisFileName);
        }
        result = _findnext(handle, &file);
    } while (result != -1);
    std::sort(dst.begin(), dst.end(),
              [this](const std::string &a, const std::string &b) { return getEndNumber(a) < getEndNumber(b); });
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

    for (auto it = directories.rbegin(); it != directories.rend(); ++it) {
        auto thisFilePath = *it;
        --levelSize;
        std::vector<std::string> ssTableFilePath;
        std::regex pat("[0-9]+\\.sst");
        getFileNamesUnderDirectory(thisFilePath, pat, ssTableFilePath);

        for (auto &sstFilePath:ssTableFilePath) {
            auto ssTable = decodeSSTFile(sstFilePath);
            storeLevel[levelSize].push_back(ssTable);
            nextTimeStamp = std::max(nextTimeStamp, ssTable->getTimeStamp()) + 1;
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
    auto existedVal = memTable->get(key);
    size_t increment;
    if (existedVal)
        increment = value.size() - existedVal->size();
    else
        increment = 12 + value.size();
    if (memTable->willOverflow(increment))
        MemTableToSSTable(); // If the size of MemTable has overflew, then revert it to SSTable.
    memTable->put(key, value);
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */

std::string KVStore::getForTest0(uint64_t key) {
    std::string *value = memTable->get(key);
    if (value) {
        if (*value != "~DELETED~")return *value;
        else return "";
    }
    auto levelSize = storeLevel.size();
    for (int i = 0; i < levelSize; ++i) {
        std::list<SSTable *> &thisLevel = storeLevel[i];
        for (auto it = thisLevel.rbegin(); it != thisLevel.rend(); ++it) { //reverse,cause the timeStamp is ↑
            auto thisSSTable = *it;
            std::string thisFilePath = thisSSTable->getFilePath();
            std::ifstream file(thisFilePath);
            file.seekg(8, std::ios::beg);
            uint64_t entryNumber;
            file.read((char *) &entryNumber, 8);
            file.seekg(10256, std::ios::cur);
            uint64_t thisKey;
            uint32_t thisOffset;
            uint64_t keyCounter = 0;
            std::string value;
            while (keyCounter <= entryNumber) {
                file.read((char *) &thisKey, 8);
                file.read((char *) &thisOffset, 4);
                if (key == thisKey) {
                    if (keyCounter == entryNumber) {
                        file.seekg(10272 + thisOffset, std::ios::beg);
                        while (true) {
                            char ch;
                            file.get(ch);
                            if (file.eof())return value;
                            value.push_back(ch);
                        }
                    } else {
                        uint32_t nextOffset;
                        file.seekg(8, std::ios::cur);
                        file.read((char *) &nextOffset, 4);
                        uint32_t size = nextOffset - thisOffset;
                        file.seekg(10272 + thisOffset, std::ios::beg);
                        for (int j = 0; j < size; ++j) {
                            char ch;
                            file.get(ch);
                            value.push_back(ch);
                        }
                        return value;
                    }
                }
                ++keyCounter;
            }
        }
    }
}

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
        std::list<SSTable *> &thisLevel = storeLevel[i];
        for (auto it = thisLevel.rbegin(); it != thisLevel.rend(); ++it) { //reverse,cause the timeStamp is ↑
            auto thisSSTable = *it;
            std::string thisFilePath = thisSSTable->getFilePath();
#ifdef DEBUG
            std::cout << "Now begin searching key " << key << " in SSTable with file path: " << thisFilePath << " ..."
                      << std::endl;
#endif
            auto thisValue = thisSSTable->get(key);
            if (!thisValue.empty()) {
                if (thisValue != "~DELETED~") {
                    return thisValue;
                } else {
//                    std::cout << "key: " << key << std::endl;
                    return "";
                }
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
    if (!get(key).empty()) {
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
    for (auto &level:storeLevel) {
        for (auto &thisSSTable:level) {
            delete thisSSTable;
        }
    }
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
        if (thisKeyA < thisKeyB) {
            thisKey = thisKeyA;
            thisValue = srcA_values[i];
            ++i;
        } else if (thisKeyA > thisKeyB) {
            thisKey = thisKeyB;
            thisValue = srcB_values[j];
            ++j;
        } else {
            if (timeStampA > timeStampB) {
                thisKey = thisKeyA;
                thisValue = srcA_values[i];
            } else {
                thisKey = thisKeyB;
                thisValue = srcB_values[j];
            }
            ++i, ++j;
        }
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
                                std::string &dstPath, int levelIndex) {
    auto &nextLevel = storeLevel[levelIndex + 1];
    size_t presentSize = 0;
    std::vector<uint64_t> keys;
    std::vector<std::string> values;
    int keyNumber = srcValues.size();
    for (int i = 0; i < keyNumber; ++i) {
        auto value = srcValues[i];
        if (levelIndex == storeLevel.size() - 2 && value == "~DELETED~")continue; //if deleted,then skip
        auto increment = 12 + value.size();
        if (presentSize + increment + 10272 > 2097152) {
            auto newSSTable = new SSTable(dstPath, nextTimeStamp++, keys, values);
            newSSTable->write(values); ///TODO simplify?
            nextLevel.push_back(newSSTable);
            presentSize = 0;
            keys.clear();
            values.clear();
        }
        presentSize += increment;
        keys.push_back(srcKeys[i]);
        values.push_back(srcValues[i]);
#ifdef DEBUG
        std::cout << "merging... Now merging size is: " << presentSize << std::endl;
#endif
    }
    auto newSSTable = new SSTable(dstPath, nextTimeStamp++, keys, values);
    newSSTable->write(values);
    nextLevel.push_back(newSSTable);
}

void KVStore::mergeLevels(int levelIndex) {
    std::vector<SSTable *> needMerge;
    //first statistic the key range of level 0
    std::list<SSTable *> &thisLevel = storeLevel[levelIndex];
    int64_t rangeMin = std::numeric_limits<int64_t>::max();
    int64_t rangeMax = std::numeric_limits<int64_t>::min();
    if (levelIndex == 0)
        for (auto &thisSSTable : thisLevel) {
            rangeMin = std::min(rangeMin, thisSSTable->getMinKey());
            rangeMax = std::max(rangeMax, thisSSTable->getMaxKey());
            needMerge.push_back(thisSSTable);
        }
    else {
        int overflowNumber = thisLevel.size() - (2 << levelIndex);
        auto iterator = thisLevel.begin();
        for (int i = 0; i < overflowNumber; ++i) {
            auto thisSSTable = *iterator;
            rangeMin = std::min(rangeMin, thisSSTable->getMinKey());
            rangeMax = std::max(rangeMax, thisSSTable->getMaxKey());
            needMerge.push_back(thisSSTable);
            ++iterator;
        }
    }
    //first statistic the key range of this level

    auto targetDirectoryPath = manageDir + "/level-" + std::to_string(levelIndex + 1);
    std::vector<std::list<SSTable *>::iterator> needDeleteIterator;
    if (storeLevel.size() > levelIndex + 1) { //Search in level-(levelIndex +1)
        std::list<SSTable *> &nextLevel = storeLevel[levelIndex + 1];
        for (auto it = nextLevel.begin(); it != nextLevel.end(); ++it) {
            auto thisSSTable = *it;
            if (rangeMin <= thisSSTable->getMaxKey() || rangeMax >= thisSSTable->getMinKey()) {
                //in range,thus needing to merge
                needMerge.push_back(thisSSTable);
                needDeleteIterator.push_back(it);
            }
        }
    } else {
        storeLevel.emplace_back(); //add level
        utils::mkdir(targetDirectoryPath.c_str());
    }

    std::vector<uint64_t> presentKeys;
    std::vector<std::string> presentValues;


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

//    std::ofstream out("../debug/debug.txt", std::ios::app);
//    for (int i = 0; i < presentKeys.size(); ++i) {
//        out << presentKeys[i] << ": " << presentValues[i] << std::endl;
//    }
//    out.close();
//
    //delete sstable in next level
    auto &nextLevel = storeLevel[levelIndex + 1];
    for (auto thisIterator:needDeleteIterator) {
        auto thisSSTable = *thisIterator;
        utils::rmfile(thisSSTable->getFilePath().c_str());
        delete thisSSTable;
        nextLevel.erase(thisIterator);
    }

    splitIntoSSTables(presentKeys, presentValues, targetDirectoryPath, levelIndex);
    if (levelIndex == 0) {
        thisLevel.clear();
        removeDirectoryContentRecursively(manageDir + "level-0");
    } else {
        int deletedNumber = thisLevel.size() - (2 << levelIndex);
        for (int i = 0; i < deletedNumber; ++i) {
            SSTable *thisSSTable = thisLevel.front();
            auto ssTableFilepath = thisSSTable->getFilePath();
            utils::rmfile(ssTableFilepath.c_str());
            thisLevel.pop_front();
        }
    }
}

void KVStore::handleLevelOverflow() {
    int levelIndex = 0;
    do {
        mergeLevels(levelIndex++);
    } while (isOverflow(levelIndex));
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
    memTable->clear();
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
    return ssTable;
}

