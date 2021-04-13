#ifndef LSM_TREE_SKIPLIST_H
#define LSM_TREE_SKIPLIST_H

#include <bits/stdc++.h>

using namespace std;

template<typename K, typename V>
struct Node {
    K key;
    V val;
    Node<K, V> *right, *down;

    Node(Node<K, V> *right, Node<K, V> *down, K key, V val) : right(right), down(down), key(key), val(val) {}

    Node() : right(nullptr), down(nullptr) {}
};

template<typename K, typename V>
class SkipList {
public:
    Node<K, V> *head;

    SkipList() : currentSize(0) {
        head = new Node<K, V>();  //初始化头结点
    }

    ~SkipList() { //delete the skipList by rows.
        Node<K, V> *p = head;
        Node<K, V> *q = head;
        while (q) {
            q = p->down;
            while (p) {
                Node<K, V> *tmpNode = p;
                p = p->right;
                delete tmpNode;
            }
            p = q;
        }
    }

    void traverse() {
        Node<K, V> *p = head;
        Node<K, V> *q = head;
        while (q) {
            q = p->down;
            while (p) {
                cout << p->key << " ";
                p = p->right;
            }
            p = q;
            cout << endl;
        }
    }

    size_t size() {
        return currentSize;
    }

    V *get(const K &key) {
        Node<K, V> *p = head;
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

    void put(const K &key, const V &val) {
        vector<Node<K, V> *> pathList;    //从上至下记录搜索路径
        Node<K, V> *p = head;
        Node<K, V> *existedNode = nullptr;
        while (p) {
            while (p->right && p->right->key < key) {
                p = p->right;
            }
            if (p->right && p->right->key == key) {
                existedNode = p->right;
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
            return;
        }

        bool insertUp = true;
        Node<K, V> *downNode = nullptr;
        while (insertUp && pathList.size() > 0) {   //从下至上搜索路径回溯，50%概率
            Node<K, V> *insert = pathList.back();
            pathList.pop_back();
            insert->right = new Node<K, V>(insert->right, downNode, key, val); //add新结点
            downNode = insert->right;    //把新结点赋值为downNode
            insertUp = (rand() & 1);   //50%概率
        }
        if (insertUp) {  //插入新的头结点，加层
            Node<K, V> *oldHead = head;
            head = new Node<K, V>();
            head->right = new Node<K, V>(NULL, downNode, key, val);
            head->down = oldHead;
        }
        ++currentSize;
    }

    bool remove(const K &key) {
        bool find;
        Node<K, V> *p = head;
        while (p) {
            while (p->right && p->right->key < key) {
                p = p->right;
            }
            if (p->right && p->right->key == key) {
                find = true;
                Node<K, V> *tmpNode = p->right;
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
        if (find)--currentSize;
        return find;
    }

private:
    int currentSize;
};


#endif //LSM_TREE_SKIPLIST_H