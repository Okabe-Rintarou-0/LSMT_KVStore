#ifndef LSM_TREE_MEMTABLE_H
#define LSM_TREE_MEMTABLE_H

#include "includes.h"
#include <bits/stdc++.h>

template<typename K, typename V>
struct Node {
    K key;
    V val;
    Node<K, V> *right, *down;

    Node(Node<K, V> *right, Node<K, V> *down, K key, V val) : right(right), down(down), key(key), val(val) {}

    Node() : right(nullptr), down(nullptr) {}
};

class SkipList {
public:
    SkipList();

    ~SkipList();

    void clear();

    void traverse();

    inline size_t size() {
        return currentSize;
    }

    inline bool isEmpty() const { return currentSize == 0; }

    std::string *get(const uint64_t &key);

    void put(const uint64_t &key, const std::string &val);

    bool remove(const uint64_t &key);


protected:
    Node<uint64_t, std::string> *head;
    int currentSize;
    size_t entrySize;
};

class MemTable : public SkipList {
public:
    bool willOverflow(size_t s);

    void getOrderedEntries(std::vector<uint64_t> &keyDst, std::vector<std::string> &valDst);
};


#endif //LSM_TREE_MEMTABLE_H
