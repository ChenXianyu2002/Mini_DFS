//
// Created by 17813 on 2022/6/9.
//
#include <iostream>
#include <string>
#include <utility>
#include <vector>
#include <sstream>
#include <fstream>
#include <cmath>
#include <thread>
#include "algorithm"
#include "utils.h"
#include "nameServer.h"

NameServer::NameServer(int numReplicate_, int chunk_size_, std::string meta_path_) : numReplicate(numReplicate_),
                                                                                     chunk_size(chunk_size_),
                                                                                     meta_path(std::move(meta_path_)) {}

void NameServer::load_meta(const std::string &meta_path_) {
    std::ifstream fin(meta_path_);
    if (!fin) {
        std::cerr << "Not Existing Meta Data!" << std::endl;
        return;
    }
    std::string file_path, tmp;
    int id = 0, fileSize, ChunkNum;
    int i, j;
    while (fin >> file_path) {
        fin >> id >> fileSize;
        fin.ignore();//next line
        ChunkNum = ceil(double(fileSize) / chunk_size);
        meta[file_path] = id;
        std::vector<std::vector<int>> blocks_servers;
        std::vector<int> servers;
        for (i = 0; i < ChunkNum; ++i) {
            std::getline(fin, tmp);
            std::stringstream ss(tmp);
            while (ss >> j) {
                servers.push_back(j);
            }
            blocks_servers.push_back(servers);
        }
        files_chunk_servers[id] = std::make_pair(fileSize, blocks_servers);
    }
    idNum = id + 1;
    std::cout << "Loaded Meta Data From " << meta_path_ << std::endl;
    fin.close();
}

void NameServer::save_meta(const std::string &meta_path_) {
    std::ofstream fout(meta_path_);
    if (!fout) {
        std::cerr << "Create file failed. Maybe wrong directory." << std::endl;
        return;
    }
    std::string file_path;
    int id, ChunkNum;
    for (const auto &iter: meta) {
        file_path = iter.first;
        id = iter.second;
        ChunkNum = ceil(files_chunk_servers[id].first / chunk_size);
        fout << file_path << "\t" << id << "\t" << ChunkNum << std::endl;
        for (const auto &servers: files_chunk_servers[id].second) {
            for (const auto &server: servers) {
                fout << server << "\t";
            }
            fout << std::endl;
        }
    }
    std::cout << "Saved Meta Data to" << meta_path_ << std::endl;
    fout.close();
}

bool size_compare(DataServer *d1, DataServer *d2) {
    return d2 == nullptr or (d1 != nullptr and d1->get_size() < d2->get_size());
}

void NameServer::operator()() {
    std::vector<std::string> parameters;
    int chunkNum;
    int fid, offset;
    std::ifstream fin;
    std::ofstream fout;
    char *buf;
    int did;
    while (true) {
        parameters = parse_cmd();
        std::vector<int> idx;
        if (parameters.empty()) {
            std::cerr << "Error! Please enter a non-empty command!" << std::endl;
            continue;
        }
        if (parameters[0] == "quit" || parameters[0] == "exit") {
            exit(0);
        } else if (parameters[0] == "list" || parameters[0] == "ls") {
            std::cout << "File\tFileID\tChunkNum" << std::endl;
            for (const auto &iter: meta) {
                fid = iter.second;
                std::cout << iter.first << "\t" << fid << "\t" << files_chunk_servers[fid].first << std::endl;
                for (const auto &servers: files_chunk_servers[fid].second) {
                    for (const auto &server: servers) {
                        std::cout << server << "\t";
                    }
                    std::cout << std::endl;
                }
            }
            continue;
        } else if (parameters[0] == "upload") {// upload file to miniDFS
            if (parameters.size() < 2) {
                std::cerr << "Usage: " << "upload source_file_path" << std::endl;
                continue;
            }
            if (meta.find(parameters[1]) != meta.end()) {
                std::cerr << "Create file error! \nMaybe the file : " << parameters[1] << " exists." << std::endl;
                continue;
            }
            fin.open(parameters[1], std::ios::ate | std::ios::binary);
            if (!fin) {
                std::cerr << "open file " << parameters[1] << " error" << std::endl;
                continue;
            }
            int totalSize = int(fin.tellg());
            buf = new char[totalSize];
            fin.seekg(0, std::ifstream::beg);
            fin.read(buf, totalSize);
            fin.close();
            chunkNum = ceil(totalSize / chunk_size);
            meta[parameters[1]] = idNum;
            std::vector<std::vector<int>> block_servers;
            for (int chunk = 0; chunk < chunkNum; ++chunk) {
                std::sort(dataServers.begin(), dataServers.end(), size_compare);
                std::vector<int> a;
                for (int r = 0; r < numReplicate; ++r) {
                    a.push_back(dataServers[r]->get_name());
                    dataServers[r]->increase_size();
                    dataServers[r]->chunkIds.push_back(chunk);
                }
                block_servers.emplace_back(a);
            }
            dataServers = dataServerBack;
            files_chunk_servers[idNum] = std::make_pair(totalSize, block_servers);
            set_upload(idNum, buf, totalSize);
            ++idNum;
        } else if (parameters[0] == "read" || parameters[0] == "fetch") {
            offset = 0;
            if (parameters[0] == "read") {
                if (parameters.size() < 2) {
                    std::cerr << "Usage: " << "read source_file_path" << std::endl;
                    continue;
                }
                if (meta.find(parameters[1]) == meta.end()) {
                    std::cerr << "Error: no such file in miniDFS." << std::endl;
                    continue;
                }
                fid = meta[parameters[1]];
            } else {
                if (parameters.size() < 2) {
                    std::cerr << "Usage: " << "fetch FileID Offset(default 0)" << std::endl;
                    continue;
                }
                try {
                    fid = std::stoi(parameters[1]);
                    if (parameters.size() > 2) offset = std::stoi(parameters[2]);
                } catch (std::exception &e) {
                    std::cerr << e.what() << std::endl;
                    continue;
                }
                if (fid >= idNum) {
                    std::cerr << "No such FileID " << fid << std::endl;
                    continue;
                }
                if (offset >= files_chunk_servers[fid].first) {
                    std::cerr << "Offset: " << offset << " out of filesize: " << files_chunk_servers[fid].first
                              << std::endl;
                    continue;
                }
            }
            int begin_chunk = offset / int(chunk_size);
            int chunk_offset = offset % int(chunk_size);
            std::vector<std::vector<int>> blocks_servers = files_chunk_servers[fid].second;
            for (auto chunk = begin_chunk; chunk < blocks_servers.size(); ++chunk) {
                dataServers[blocks_servers[chunk][0]]->chunkIds.push_back(chunk);
            }
            dataServers[blocks_servers[begin_chunk][0]]->firstOffset = chunk_offset;
            buf = new char[files_chunk_servers[fid].first];
            set_read(fid, buf);
        } else if (parameters[0] == "delete") {
            if (parameters.size() < 2) {
                std::cerr << "Usage: " << "delete DataNodeId" << std::endl;
                continue;
            }
            try {
                did = std::stoi(parameters[1]);
            }
            catch (std::exception &e) {
                std::cerr << e.what() << std::endl;
                continue;
            }
            if (getAliveServers() <= numReplicate) {
                std::cerr << "The Number of DataNode is less than " << numReplicate + 1
                          << ". The delete operation cannot be performed." << std::endl;

            }
            for (auto &iter: files_chunk_servers) {
                for (auto &servers: iter.second.second) {
                    auto remove_iter = std::remove(servers.begin(), servers.end(), did);
                    servers.erase(remove_iter, servers.end());
                }
            }
            auto server = dataServers[did];
            std::unique_lock<std::mutex> lk(server->mtx);
            server->cmd = "clear";
            server->finish = false;
            lk.unlock();
            server->cv.notify_all();
        } else if (parameters[0] == "rebuild") {
            for (did = 0; did < dataServers.size(); ++did) {
                if (dataServers[did] == nullptr) break;
            }
            if (did == dataServers.size()) {
                std::cout << "No Need to Rebuild DataServer." << std::endl;
                continue;
            }
            dataServers[did] = new DataServer(did, int(chunk_size));
            dataServerBack[did] = dataServers[did];
            std::thread th(std::ref(*dataServers[did]));
            th.detach();
            for (auto &iter: files_chunk_servers) {
                bool flag = false;
                std::vector<int> chunkIds;
                for (int i = 0; i < iter.second.second.size(); ++i) {
                    auto &servers = iter.second.second[i];
                    if (servers.size() < numReplicate) {
                        flag = true;
                        servers.push_back(did);
                        dataServers[servers[0]]->chunkIds.push_back(i);
                        chunkIds.push_back(i);
                        dataServers[did]->increase_size();
                    }
                }
                if (!flag) continue;
                buf = new char[iter.second.first];
                set_read(iter.first, buf);
                wait();
                dataServers[did]->chunkIds = chunkIds;
                set_upload(iter.first, buf, iter.second.first);
                wait();
            }
        } else if (parameters[0] == "put") {
            if (parameters.size() < 3) {
                std::cerr << "Usage: " << "put sourceDir targetDir" << std::endl;
                continue;
            }
            fin.open(parameters[1], std::ios::ate | std::ios::binary);
            if (!fin) {
                std::cerr << "Find error sourceDir: " << parameters[1] << std::endl;
                continue;
            }
            int totalSize = int(fin.tellg());
            buf = new char[totalSize];
            fin.seekg(0, std::ifstream::beg);
            fin.read(buf, totalSize);
            fin.close();

            fout.open(parameters[2], std::ios::ate | std::ios::binary);
            if (!fout) {
                std::cerr << "Find error targetDit: " << parameters[2] << std::endl;
                delete[]buf;
                continue;
            }
            fout.write(buf, totalSize);
            fout.close();

            delete[]buf;
            std::cout << "Put file from " << parameters[1] << " to " << parameters[2] << " success." << std::endl;
            continue;
        } else if (parameters[0] == "access") {
            if (parameters.size() < 2) {
                std::cerr << "Usage: " << "access sourceDir" << std::endl;
                continue;
            }
            fin.open(parameters[1], std::ios::ate | std::ios::binary);
            if (!fin) {
                std::cerr << "Find error sourceDir: " << parameters[1] << std::endl;
                continue;
            }
            int totalSize = int(fin.tellg());
            buf = new char[totalSize];
            fin.seekg(0, std::ifstream::beg);
            fin.read(buf, totalSize);
            fin.close();
            std::cout << buf << std::endl;
            delete[]buf;
            buf = nullptr;
        } else if (parameters[0] == "check") {
            if (parameters.size() < 2) {
                std::cerr << "Usage: " << "check fid" << std::endl;
                continue;
            }
            try {
                fid = std::stoi(parameters[1]);
            } catch (std::exception &e) {
                std::cerr << e.what() << std::endl;
                continue;
            }
            if (fid >= idNum) {
                std::cerr << "No such FileID " << fid << std::endl;
                continue;
            }
            std::vector<std::vector<int>> blocks_servers = files_chunk_servers[fid].second;
            for (auto chunk = 0; chunk < blocks_servers.size(); ++chunk) {
                for (int server_id = 0; server_id < blocks_servers[chunk].size(); ++server_id) {
                    dataServers[blocks_servers[chunk][server_id]]->chunkIds.push_back(chunk);
                }
            }
            set_read(fid, nullptr, "check");
        }

        // waiting for the finish of data server.
        wait();
        if (parameters[0] == "upload") {
            std::cout << "Upload success. The file ID is " << idNum - 1 << std::endl;
        } else if (parameters[0] == "read" || parameters[0] == "fetch") {
            std::cout << buf + offset << std::endl;
            delete[]buf;
            buf = nullptr;
        } else if (parameters[0] == "delete") {
            delete dataServers[did];
            dataServers[did] = nullptr;
            dataServerBack[did] = nullptr;
        } else if (parameters[0] == "check") {
            std::vector<std::vector<int>> blocks_servers = files_chunk_servers[fid].second;
            std::string md5_checksum;
            bool flag = true;
            for (const auto &block_servers: blocks_servers) {
                md5_checksum.clear();
                for (const auto &server_id: block_servers) {
                    if (md5_checksum.empty()) {
                        md5_checksum = dataServers[server_id]->md5CheckSums.front();
                        dataServers[server_id]->md5CheckSums.pop_front();
                    } else {
                        if (md5_checksum != dataServers[server_id]->md5CheckSums.front()) {
                            std::cout << "Check a MD5 difference!" << std::endl;
                            flag = false;
                        }
                        dataServers[server_id]->md5CheckSums.pop_front();
                    }
                }
            }
            if (flag) std::cout << "Check MD5 success!" << std::endl;
        }
    }
}

void NameServer::creatDataServers(int serverNum) {
    for (auto i = 0; i < serverNum; ++i) {
        dataServers.push_back(new DataServer(i, int(chunk_size)));
        std::thread th(std::ref(*dataServers[i]));
        th.detach();
    }
    dataServerBack = dataServers;
}

NameServer::~NameServer() {
    for (auto &dataServer: dataServers) {
        dataServer->set_deleted();
        delete dataServer;
    }
}

int NameServer::getAliveServers() {
    int aliveServers = 0;
    for (auto &dataServer: dataServers) {
        if (dataServer != nullptr) ++aliveServers;
    }
    return aliveServers;
}

void NameServer::set_read(int fid, char *buf, const std::string &cmd) {
    for (auto server: dataServers) {
        if (server == nullptr) continue;
        std::unique_lock<std::mutex> lk(server->mtx);
        server->cmd = cmd;
        server->fid = fid;
        server->bufSize = files_chunk_servers[fid].first;
        server->buf = buf;
        server->finish = false;
        lk.unlock();
        server->cv.notify_all();
    }
}

void NameServer::wait() {
    for (const auto &server: dataServers) {
        if (server == nullptr) continue;
        std::unique_lock<std::mutex> lk(server->mtx);
        (server->cv).wait(lk, [&]() { return server->finish; });
        lk.unlock();
        (server->cv).notify_all();
    }
}

void NameServer::set_upload(int fid, char *buf, int buf_size) {
    for (auto server: dataServers) {
        if (server == nullptr) continue;
        std::unique_lock<std::mutex> lk(server->mtx);
        server->cmd = "upload";
        server->fid = fid;
        server->bufSize = buf_size;
        server->buf = buf;
        server->finish = false;
        lk.unlock();
        server->cv.notify_all();
    }
}
