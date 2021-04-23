#include "kvstore.h"
#include <random>
#include <ctime>

#define TESTSIZE 10000

class PerformanceTest {
public:

    PerformanceTest() : store(new KVStore("../store")) {}

    inline void startTest() { startTime = clock(); }

    inline void endTest() { endTime = clock(); }

    inline double calcConsumedTime() { return (double) (endTime - startTime) / CLOCKS_PER_SEC; }

    void init() {
        if (store)
            store->reset();
        startTime = endTime = 0;
        totalTime = avgTime = 0;
    }

    void writeResult(const std::string &filePath) {
        std::ofstream file(filePath);
        std::string writeData = std::to_string(avgTime) + ",";
        file.write(writeData.c_str(), writeData.size());
    }

    void report() {
        std::cout << "consume total time: " << totalTime << std::endl;
        std::cout << "consume average time: " << avgTime << std::endl;
    }

    void prepare(int testSize, int valueSize) {
        init();
        for (int i = 0; i < testSize; ++i) {
            store->put(i, std::string(valueSize, 'x'));
        }
    }

    void prepareRandomly(int testSize) {
        srand((unsigned) time(NULL));
        for (int i = 0; i < testSize; ++i) {
            store->put((rand() % 10000) + 1, std::string((rand() % 1024) + 1, 'x'));
        }
    }

    void testGetByThreeWay(int testSize, int times, int type) {
        init();
        std::cout << "test get for testSize: " << testSize << " and valueSize: " << std::endl;
        prepareRandomly(testSize);
        for (int t = 0; t < times; ++t) {
            startTest();
            for (int i = 0; i < testSize; ++i) {
                switch (type) {
                    case 1:
                        store->getForTest0(i);
                        break;
                    case 2:
                        break;
                    case 3:
                        store->get(i);
                        break;
                }
            }
            endTest();
            totalTime += calcConsumedTime();
        }
        avgTime = totalTime / times;
        report();
        writeResult("testGetResult.txt");
    }

    void testGet(int testSize, int valueSize, int times = 1) {
        prepare(testSize, valueSize);
        std::cout << "test get for testSize: " << testSize << " and valueSize: " << valueSize << std::endl;
        for (int t = 0; t < times; ++t) {
            startTest();
            for (int i = 0; i < testSize; ++i) {
                store->get(i);
            }
            endTest();
            totalTime += calcConsumedTime();
        }
        avgTime = totalTime / times;
        report();
        writeResult("testGetResult.txt");
    }

    void testDelete(int testSize, int valueSize, int times = 1) {
        std::cout << "test delete for testSize: " << testSize << " and valueSize: " << valueSize << std::endl;
        for (int t = 0; t < times; ++t) {
            prepare(testSize, valueSize);
            startTest();
            for (int i = 0; i < testSize; ++i) {
                store->del(i);
            }
            endTest();
            totalTime += calcConsumedTime();
        }
        avgTime = totalTime / times;
        report();
        writeResult("testDeleteResult.txt");
    }

    void testPut(int testSize, int valueSize, int times = 1) {
        init();
        std::cout << "test put for testSize: " << testSize << " and valueSize: " << valueSize << std::endl;
        for (int t = 0; t < times; ++t) {
            startTest();
            for (int i = 0; i < testSize; ++i) {
                store->put(i, std::string(valueSize, 'x'));
            }
            endTest();
            totalTime += calcConsumedTime();
        }
        avgTime = totalTime / times;
        report();
        writeResult("testPutResult.txt");
    }

private:
    KVStore *store;
    clock_t startTime;
    clock_t endTime;
    double totalTime;
    double avgTime;
};

int main() {
    PerformanceTest test;
    for (int i = 5; i < 1100; i += 5)
        test.testDelete(TESTSIZE, i, 10);
    for (int i = 500; i < 10000; i += 500)
        test.testPut(TESTSIZE, i, 10);
//    test.testPut(TESTSIZE, 50, 10);
//    test.testPut(TESTSIZE, 100, 10);
//    test.testPut(TESTSIZE, 500, 10);
//    test.testPut(TESTSIZE, 1000, 10);
//    test.testPut(TESTSIZE, 1000, 10);
//    test.testPut(TESTSIZE, 1000, 10);

}