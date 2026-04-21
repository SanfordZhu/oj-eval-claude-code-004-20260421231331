#ifndef STORE_HPP
#define STORE_HPP

#include <cstdio>
#include <cstring>
#include <cstdint>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <list>

// Record store for fixed-size records T. Maintains a free list for erased
// records and supports read/write/append by id (offset index).
template <class T>
class RecordStore {
    std::fstream file;
    struct Header {
        int32_t count;
        int32_t free_head;
    } header;
    std::string filename;

    // small read cache
    static const int CACHE_SIZE = 256;
    using CacheList = std::list<std::pair<int32_t, std::pair<T, bool> > >;
    std::unordered_map<int32_t, typename CacheList::iterator> cache_map;
    CacheList cache_list;

    void evict_one() {
        auto last = --cache_list.end();
        if (last->second.second) {
            write_raw(last->first, last->second.second = false, last->second.first);
        }
        cache_map.erase(last->first);
        cache_list.pop_back();
    }

    void write_raw(int32_t id, bool /*dirty_clear*/, const T &v) {
        file.seekp(sizeof(Header) + (std::streamoff)id * sizeof(T));
        file.write(reinterpret_cast<const char *>(&v), sizeof(T));
    }
    void read_raw(int32_t id, T &v) {
        file.seekg(sizeof(Header) + (std::streamoff)id * sizeof(T));
        file.read(reinterpret_cast<char *>(&v), sizeof(T));
    }

public:
    void open(const std::string &fname) {
        filename = fname;
        file.open(fname, std::ios::in | std::ios::out | std::ios::binary);
        if (!file.is_open()) {
            file.clear();
            file.open(fname, std::ios::out | std::ios::binary);
            file.close();
            file.open(fname, std::ios::in | std::ios::out | std::ios::binary);
            header.count = 0;
            header.free_head = -1;
            write_header();
        } else {
            file.seekg(0);
            file.read(reinterpret_cast<char *>(&header), sizeof(Header));
        }
    }

    void write_header() {
        file.seekp(0);
        file.write(reinterpret_cast<const char *>(&header), sizeof(Header));
    }

    void flush() {
        for (auto &p : cache_list) {
            if (p.second.second) {
                write_raw(p.first, true, p.second.first);
                p.second.second = false;
            }
        }
        write_header();
        file.flush();
    }

    void close() {
        flush();
        file.close();
    }

    int32_t append(const T &v) {
        int32_t id;
        if (header.free_head != -1) {
            // read the free head, which stores next free in its bytes
            int32_t next;
            file.seekg(sizeof(Header) + (std::streamoff)header.free_head * sizeof(T));
            file.read(reinterpret_cast<char *>(&next), sizeof(int32_t));
            id = header.free_head;
            header.free_head = next;
        } else {
            id = header.count++;
        }
        write_raw(id, false, v);
        return id;
    }

    void erase(int32_t id) {
        // write sentinel by placing free_head ptr at start
        int32_t next = header.free_head;
        file.seekp(sizeof(Header) + (std::streamoff)id * sizeof(T));
        file.write(reinterpret_cast<const char *>(&next), sizeof(int32_t));
        header.free_head = id;
        // remove from cache
        auto it = cache_map.find(id);
        if (it != cache_map.end()) {
            cache_list.erase(it->second);
            cache_map.erase(it);
        }
    }

    T read(int32_t id) {
        auto it = cache_map.find(id);
        if (it != cache_map.end()) {
            cache_list.splice(cache_list.begin(), cache_list, it->second);
            return it->second->second.first;
        }
        if ((int)cache_list.size() >= CACHE_SIZE) evict_one();
        T v;
        read_raw(id, v);
        cache_list.push_front({id, {v, false}});
        cache_map[id] = cache_list.begin();
        return v;
    }

    void write(int32_t id, const T &v) {
        auto it = cache_map.find(id);
        if (it != cache_map.end()) {
            it->second->second.first = v;
            it->second->second.second = true;
            cache_list.splice(cache_list.begin(), cache_list, it->second);
            return;
        }
        if ((int)cache_list.size() >= CACHE_SIZE) evict_one();
        cache_list.push_front({id, {v, true}});
        cache_map[id] = cache_list.begin();
    }

    int32_t count() const { return header.count; }
};

#endif
