#include "../Inc/MemTable.h"

SkipList::SkipList() : currentSize(0) {
    head = new Node<uint64_t, std::string>();  //初始化头结点
}

SkipList::~SkipList() {
    Node<uint64_t, std::string> *p = head;
    Node<uint64_t, std::string> *q = head;
    while (q) {
        q = p->down;
        while (p) {
            Node<uint64_t, std::string> *tmpNode = p;
            p = p->right;
            delete tmpNode;
        }
        p = q;
    }
}

std::string *SkipList::get(const uint64_t &key) {
    Node<uint64_t, std::string> *p = head;
    while (p) {
        while (p->right && p->right->key < key) {
            p = p->right;
        }
        if (p->right && p->right->key == key)
            return &(p->right->val);
        p = p->down;
    }
    return nullptr;
}

void SkipList::put(const uint64_t &key, const std::string &val) {
    std::vector<Node<uint64_t, std::string> *> pathList;    //从上至下记录搜索路径
    Node<uint64_t, std::string> *p = head;
    Node<uint64_t, std::string> *existedNode = nullptr;
    std::string originValue;
    while (p) {
        while (p->right && p->right->key < key) {
            p = p->right;
        }
        if (p->right && p->right->key == key) {
            existedNode = p->right;
            originValue = p->right->val;
            break;
        }
        pathList.push_back(p);
        p = p->down;
    }

    if (existedNode) {
        p = existedNode;
        while (p) {
            while (p->right && p->right->key <= key) {
                p = p->right;
            }
            p->val = val;
            p = p->down;
        }
        entrySize += val.size() - originValue.size();
        return;
    }

    bool insertUp = true;
    Node<uint64_t, std::string> *downNode = nullptr;
    while (insertUp && pathList.size() > 0) {   //从下至上搜索路径回溯，50%概率
        Node<uint64_t, std::string> *insert = pathList.back();
        pathList.pop_back();
        insert->right = new Node<uint64_t, std::string>(insert->right, downNode, key, val); //add新结点
        downNode = insert->right;    //把新结点赋值为downNode
        insertUp = (rand() & 1);   //50%概率
    }
    if (insertUp) {  //插入新的头结点，加层
        Node<uint64_t, std::string> *oldHead = head;
        head = new Node<uint64_t, std::string>;
        head->right = new Node<uint64_t, std::string>(NULL, downNode, key, val);
        head->down = oldHead;
    }
    ++currentSize;
    entrySize += sizeof(uint64_t) + val.size() + 4; //key + value + offset;
}

void SkipList::traverse() {
    Node<uint64_t, std::string> *p = head;
    Node<uint64_t, std::string> *q = head;
    while (q) {
        q = p->down;
        p = p->right;
        while (p) {
            std::cout << p->key << " ";
            p = p->right;
        }
        p = q;
        std::cout << std::endl;
    }
}


bool SkipList::remove(const uint64_t &key) {
    bool find = false;
    Node<uint64_t, std::string> *p = head;
    std::string value;
    while (p) {
        while (p->right && p->right->key < key) {
            p = p->right;
        }
        if (p->right && p->right->key == key) {
            value = p->right->val;
            find = true;
            Node<uint64_t, std::string> *tmpNode = p->right;
            p->right = tmpNode->right;
            delete tmpNode;
        }
        //target node has been found.
        p = p->down;
        if (head->right == nullptr) {
            auto tmpHead = head;
            head = head->down;
            delete tmpHead;
        }
    }
    if (find) {
        --currentSize;
        entrySize -= sizeof(uint64_t) + value.size() + 4; //offset: 4 bytes;
    }
    return find;
}

bool MemTable::isOverflow() {//To judge if it needs to revert to SSTable;
    //for debug:
    std::cout << "Entry Size: " << entrySize << std::endl;
    //for debug:
    size_t totalSize = 10272 + entrySize; //totalSize = 32 + 10240 + keyValueSize;
    return totalSize > 2097152; //2097152 Byte = 2 MB
}

void MemTable::getOrderedEntries(std::vector<uint64_t> &keyDst, std::vector<std::string> &valDst) {
    auto p = head;
    while (p->down) {
        p = p->down;
    }
    p = p->right;
    while (p) {
        keyDst.push_back(p->key);
        valDst.push_back(p->val);
        p = p->right;
    }
}