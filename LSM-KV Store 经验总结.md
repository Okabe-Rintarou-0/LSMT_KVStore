# LSM-KV Store 经验总结

## 1 写代码中遇到的问题

### 1.1 rename函数

在我尝试从把sst文件解析成SSTable缓存在内存的时候，希望使用rename函数修改文件的名称，(因为一些合并操作会导致文件的名称不是连续的正整数)，但是我忽略了一点，虽然在我的代码逻辑上(出错时的代码，现在已经解决了这些问题)，会在rename结束之后再读取rename之后的文件的内容。但是实际执行的时候，如果sst文件比较大，搬运将会需要花费很多的时间(毕竟的硬盘读写，很慢。)，而我的程序会在搬运sst的过程中读取内容，这是非常容易出错误的。实际上，要保证rename之后再读取内容，必须要保证两者是异步的。目前我的解决方案是暂时先把文件的时间戳读出来，取最大值作为KVStore的时间戳，后期再添加一个函数用于整理文件，即通过离散化把文件名映射到一个连续的自然数区间。

```c++
for (auto &sstFilePath:ssTableFilePath) {
    auto ssTable = decodeSSTFile(sstFilePath);
    storeLevel[levelSize].push_back(ssTable);
    nextTimeStamp = std::max(nextTimeStamp, ssTable->getTimeStamp()) + 1;
}
```

以上是目前的方案。

### 1.2 读取文件夹下所有文件

比较坑的是，读出来的文件虽然是按照升序排列的，但那是按照字符串的升序，会出现如下的情况

1.sst < 10.sst

10.sst < 2.sst

按理说应该2.sst在10.sst之前被读入内存中，这就会导致问题，因为我们要保证SSTable是按照时间戳从小到大排序的。

解决方案当然是排序一下，利用了std::sort + lambda function,这边我都有点忘了，可以通过[this]传递给匿名函数，否则无法调用类里的函数。

```c++
void KVStore::getFileNamesUnderDirectory(std::string &filePath, const std::regex &pat, std::vector<std::string> &dst) {
    _finddata_t file{};
    if (*filePath.rbegin() != '/')
        filePath.append("/");//add '/'
    std::string searchDir = filePath + "*.*";
    auto handle = _findfirst(searchDir.c_str(), &file);
    long result;
    do {
        std::string thisFileName(file.name);
        if (std::regex_match(thisFileName, pat)) {
            dst.push_back(filePath + thisFileName);;
        }
        result = _findnext(handle, &file);
    } while (result != -1);
    std::sort(dst.begin(), dst.end(),[this](const std::string &a, const std::string &b) { return getEndNumber(a) < getEndNumber(b); });
}
```

