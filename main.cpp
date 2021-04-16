#include "Inc/includes.h"
#include "kvstore.h"
#include "SSTable.h"

using namespace std;

void testKVStore() {
    std::cout << "Testing KVStore..." << std::endl;
    KVStore kvStore("../store");
    while (true) {
        string op;
        cin >> op;
        if (op == "PUT") {
            uint64_t key;
            string value;
            cin >> key >> value;
            kvStore.put(key, value);
        } else if (op == "RESET") {
            kvStore.reset();
        } else if (op == "SHOW")
            kvStore.showMemTable();
        else if (op == "DEL") {
            uint64_t key;
            cin >> key;
            if (kvStore.del(key))cout << "Delete key " << key << " succeed!" << endl;
            else cout << "Delete key " << key << " fail" << endl;
        } else if (op == "GET") {
            uint64_t key;
            cin >> key;
            auto val = kvStore.get(key);
            if (!val.empty())
                cout << "Find " << key << ",value is: " << val << endl;
            else
                cout << "Key does not exist!" << endl;
        } else if (op == "QUIT") {
            std::cout << "Quit Test Succeed" << std::endl;
            return;
        } else if (op == "REVERT") {
            kvStore.MemTableToSSTable();
        } else if (op == "GETVAL") {
            std::vector<std::string> values;
            kvStore.testGetValues(values);
        }
    }
}

void testBloomFilter() {
    std::cout << "Testing Bloom Filter..." << std::endl;
    BloomFilter bloomFilter;
    while (1) {
        string op;
        cin >> op;
        if (op == "PUT") {
            uint64_t key;
            cin >> key;
            bloomFilter.put(key);
            std::cout << "BloomFilter put key :" << key << std::endl;
        } else if (op == "FIND") {
            uint64_t key;
            cin >> key;
            if (bloomFilter.find(key))
                std::cout << "Key " << key << " is in this bloom filter" << std::endl;
            else
                std::cout << "Key " << key << " is not in this bloom filter" << std::endl;
        } else if (op == "SHOW") {
            unsigned int begin, end;
            cin >> begin >> end;
            bloomFilter.printSet(begin, end);
        } else if (op == "QUIT") {
            std::cout << "Quit Test Succeed" << std::endl;
            return;
        }

    }
}

int main() {
    string option;
    while (cin >> option) {
        if (option == "BF")
            testBloomFilter();
        else if (option == "KV")
            testKVStore();
    }
    return 0;
}
