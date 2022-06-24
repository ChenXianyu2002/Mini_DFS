//
// Created by 17813 on 2022/6/7.
//

#ifndef MINI_DFS_DATASERVER_H
#define MINI_DFS_DATASERVER_H

#include <string>
#include <mutex>
#include <vector>
#include <condition_variable>
#include "utils.h"

class DataServer {
private:
    int name;
    int size{};
    bool exist = true;
    std::string datadir;

    void upload();

    void read();

    void clear();

public:

    std::mutex mtx;
    std::condition_variable cv;
    std::string cmd;
    int fid{}, bufSize{}, chunkSize, firstOffset{0};
    char *buf{nullptr};
    char *md5_buf;
    std::vector<int> chunkIds;
    bool finish{true};
    MD5 md5;

    explicit DataServer(int name_, int chunkSize_);

    void operator()();

    int get_size() const { return size; };

    int get_name() const { return name; };

    void increase_size() { ++size; };

    void set_deleted() { exist = false; };

    ~DataServer() { delete[]md5_buf; }
};

#endif //MINI_DFS_DATASERVER_H
