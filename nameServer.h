//
// Created by 17813 on 2022/6/7.
//

#ifndef MINI_DFS_NAMESERVER_H
#define MINI_DFS_NAMESERVER_H

#include <vector>
#include <map>
#include <string>
#include "dataServer.h"


class NameServer {
private:
    std::vector<DataServer *> dataServers, dataServerBack;
    int idNum = 0;
    int numReplicate;
    double chunk_size;
    std::string meta_path;


    void load_meta(const std::string &meta_path);

    void save_meta(const std::string &meta_path);

public:
    std::map<std::string, int> meta;
    std::map<int, std::pair<int, std::vector<std::vector<int>>>> files_chunk_servers;

    explicit NameServer(int numReplicate_, int chunk_size_ = 2 * 1024 * 1024, std::string meta_path_ = "./meta");

    void creatDataServers(int serverNum);

    int getAliveServers();

    void operator()();

    void set_read(int fid, char *buf, const std::string &cmd = "read");

    void wait();

    void set_upload(int fid, char *buf, int buf_size);

    ~NameServer();
};


#endif //MINI_DFS_NAMESERVER_H
