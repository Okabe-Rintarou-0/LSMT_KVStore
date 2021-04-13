#include <bits/stdc++.h>
#include "SkipList.h"

using namespace std;

int main() {
    SkipList<string, int> skipList;
    while (true) {
        string op;
        cin >> op;
        if (op == "PUT") {
            string key;
            int value;
            cin >> key >> value;
            skipList.put(key, value);
        } else if (op == "SHOW")
            skipList.traverse();
        else if (op == "REMOVE") {
            string key;
            cin >> key;

            cout << "Remove ";
            if (skipList.remove(key))cout << key << " succeed!" << endl;
            else cout << key << " fail" << endl;
        } else if (op == "GET") {
            string key;
            cin >> key;
            auto val = skipList.get(key);
            if (val)
                cout << "Find " << key << ",value is: " << *val << endl;
            else
                cout << "Key does not exist!" << endl;
        }
    }
    return 0;
}
