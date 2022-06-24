//
// Created by 17813 on 2022/6/12.
//
#include <fstream>
#include <iostream>
#include "dataServer.h"


DataServer::DataServer(int name_, int chunkSize_) : name(name_), chunkSize(chunkSize_) {
    datadir = "./DataNode" + std::to_string(name_) + "/";
    std::ifstream fin(datadir);
    if (!fin) {
        system(("mkdir -p " + datadir).c_str());
    }
    md5_buf = new char[chunkSize_];
}

void DataServer::operator()() {
    while (this->exist) {
        std::unique_lock<std::mutex> lk(mtx);
        cv.wait(lk, [&]() { return !this->finish; });
        if (cmd == "upload")
            upload();
        else if (cmd == "read")
            read();
        else if (cmd == "check")
            check();
        else if (cmd == "clear") {
            clear();
            this->exist = false;
        }
        this->finish = true;
        lk.unlock();
        cv.notify_all();
    }
}

void DataServer::upload() {
    std::ofstream fout;
    std::ifstream fin;
    std::string chunkPath = datadir + std::to_string(fid) + "_";
    int actual_size;
    for (auto chunkId: chunkIds) {
        fout.open(chunkPath + std::to_string(chunkId));
        if (!fout) {
            std::cerr << "Create file error in DataServer: (block name) " << chunkPath << std::endl;
            --size;
            continue;
        }
        actual_size = std::min(chunkSize, bufSize - chunkId * chunkSize);
        fout.write(&buf[chunkId * chunkSize], actual_size);
        fout.close();
    }
    chunkIds.clear();
}

void DataServer::read() {
    std::ifstream fin;
    std::string chunkPath = datadir + std::to_string(fid) + "_";
    int actual_size;
    md5CheckSums.clear();
    for (auto chunkId: chunkIds) {
        fin.open(chunkPath + std::to_string(chunkId), std::ios::ate | std::ios::binary);
        if (!fin) {
            std::cerr << "Find error block: " << chunkPath << std::endl;
            continue;
        }
        actual_size = int(fin.tellg());
        fin.seekg(firstOffset, std::ifstream::beg);
        fin.read(&buf[chunkId * chunkSize + firstOffset], actual_size - firstOffset);
        firstOffset = 0;
        fin.close();
    }
    chunkIds.clear();
}

void DataServer::clear() {
    DIR *dir;
    struct dirent *ptr;
    std::string x, dirPath;
    dir = opendir((char *) datadir.c_str()); //打开一个目录
    while ((ptr = readdir(dir)) != nullptr) {
        x = ptr->d_name;
        if (x == "." or x == "..") continue;
        dirPath = datadir + x;
        if (std::remove(dirPath.c_str()) != 0) {
            std::cout << "Delete " << dirPath << " Failed." << std::endl;
        }
    }
    if (std::remove(datadir.c_str()) != 0) {
        std::cout << "Delete " << datadir << " Failed." << std::endl;
    }
}

void DataServer::check() {
    std::ifstream fin;
    std::string chunkPath = datadir + std::to_string(fid) + "_";
    int actual_size;
    md5CheckSums.clear();
    for (auto chunkId: chunkIds) {
        fin.open(chunkPath + std::to_string(chunkId), std::ios::ate | std::ios::binary);
        if (!fin) {
            std::cerr << "Open MD5 error in DataServer: (block name) " << chunkPath + std::to_string(chunkId)
                      << std::endl;
            continue;
        }
        actual_size = int(fin.tellg());
        fin.seekg(0, std::ifstream::beg);
        fin.read(md5_buf, actual_size);
        fin.close();
        md5.update(md5_buf, actual_size);
        md5.finalize();
        md5CheckSums.push_back(md5.toString());
        md5.init();
    }
    chunkIds.clear();
}
