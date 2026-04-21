#ifndef BPTREE_HPP
#define BPTREE_HPP

#include <cstdio>
#include <cstring>
#include <cstdint>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <unordered_map>
#include <list>
#include <utility>

// A B+ Tree implementation with LRU-based page caching, storing key-value
// pairs in a single file. Keys are template types (trivially copyable).
// Values are int32_t (record offsets or counts). Internal and leaf nodes share
// the same fixed-size block layout.
template <class Key, int ORDER = 64>
class BPTree {
public:
    using Value = int32_t;
    struct Pair {
        Key key;
        Value val;
        bool operator<(const Pair &o) const {
            if (key < o.key) return true;
            if (o.key < key) return false;
            return val < o.val;
        }
        bool operator==(const Pair &o) const {
            return !(key < o.key) && !(o.key < key) && val == o.val;
        }
    };

private:
    static const int M = ORDER;
    static const int CACHE_SIZE = 1024;

    struct Node {
        int32_t is_leaf;
        int32_t size;
        int32_t parent;
        int32_t next;
        int32_t prev;
        int32_t self;
        Pair keys[M + 1];
        int32_t children[M + 2];
    };

    struct Header {
        int32_t root;
        int32_t head;
        int32_t total_blocks;
        int32_t free_list;
    };

    std::string filename;
    std::fstream file;
    Header header;

    struct CacheEntry {
        Node node;
        bool dirty;
    };
    using CacheList = std::list<std::pair<int32_t, CacheEntry> >;
    std::unordered_map<int32_t, typename CacheList::iterator> cache_map;
    CacheList cache_list;

    // Raw disk I/O
    void read_disk(int32_t id, Node &n) {
        file.seekg(sizeof(Header) + (std::streamoff)id * sizeof(Node));
        file.read(reinterpret_cast<char *>(&n), sizeof(Node));
    }
    void write_disk(int32_t id, const Node &n) {
        file.seekp(sizeof(Header) + (std::streamoff)id * sizeof(Node));
        file.write(reinterpret_cast<const char *>(&n), sizeof(Node));
    }

    // Cache access: returns a pointer into the cache. Pointer is stable until
    // the next cache eviction. Callers must finish using the pointer before
    // requesting another node that might trigger eviction.
    // For safety, we use a copy-based API.
    void load_node(int32_t id, Node &out) {
        auto it = cache_map.find(id);
        if (it != cache_map.end()) {
            cache_list.splice(cache_list.begin(), cache_list, it->second);
            out = it->second->second.node;
            return;
        }
        if ((int)cache_list.size() >= CACHE_SIZE) evict_one();
        CacheEntry e;
        e.dirty = false;
        read_disk(id, e.node);
        cache_list.push_front({id, e});
        cache_map[id] = cache_list.begin();
        out = e.node;
    }

    void store_node(int32_t id, const Node &in) {
        auto it = cache_map.find(id);
        if (it != cache_map.end()) {
            it->second->second.node = in;
            it->second->second.dirty = true;
            cache_list.splice(cache_list.begin(), cache_list, it->second);
            return;
        }
        if ((int)cache_list.size() >= CACHE_SIZE) evict_one();
        CacheEntry e;
        e.node = in;
        e.dirty = true;
        cache_list.push_front({id, e});
        cache_map[id] = cache_list.begin();
    }

    void evict_one() {
        auto last = --cache_list.end();
        if (last->second.dirty) {
            write_disk(last->first, last->second.node);
        }
        cache_map.erase(last->first);
        cache_list.pop_back();
    }

    void write_header() {
        file.seekp(0);
        file.write(reinterpret_cast<const char *>(&header), sizeof(Header));
    }

    int32_t alloc_node() {
        if (header.free_list != -1) {
            int32_t id = header.free_list;
            Node tmp;
            // Free nodes may be in cache (shouldn't, but safe)
            read_disk(id, tmp);
            header.free_list = tmp.next;
            return id;
        }
        return header.total_blocks++;
    }

    void free_node(int32_t id) {
        auto it = cache_map.find(id);
        if (it != cache_map.end()) {
            cache_list.erase(it->second);
            cache_map.erase(it);
        }
        Node n;
        memset(&n, 0, sizeof(Node));
        n.next = header.free_list;
        header.free_list = id;
        write_disk(id, n);
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
            header.root = -1;
            header.head = -1;
            header.total_blocks = 0;
            header.free_list = -1;
            write_header();
            file.flush();
        } else {
            file.seekg(0);
            file.read(reinterpret_cast<char *>(&header), sizeof(Header));
        }
    }

    void close() {
        flush();
        file.close();
    }

    void flush() {
        for (auto &p : cache_list) {
            if (p.second.dirty) {
                write_disk(p.first, p.second.node);
                p.second.dirty = false;
            }
        }
        write_header();
        file.flush();
    }

    int32_t find_leaf(const Pair &p) {
        if (header.root == -1) return -1;
        int32_t cur = header.root;
        Node n;
        while (true) {
            load_node(cur, n);
            if (n.is_leaf) return cur;
            int i = 0;
            while (i < n.size && !(p < n.keys[i])) i++;
            cur = n.children[i];
        }
    }

    bool insert(const Key &key, Value val) {
        Pair p{key, val};
        if (header.root == -1) {
            int32_t id = alloc_node();
            Node n;
            memset(&n, 0, sizeof(Node));
            n.is_leaf = 1;
            n.size = 1;
            n.self = id;
            n.parent = -1;
            n.next = -1;
            n.prev = -1;
            n.keys[0] = p;
            store_node(id, n);
            header.root = id;
            header.head = id;
            return true;
        }
        int32_t leaf_id = find_leaf(p);
        Node leaf;
        load_node(leaf_id, leaf);
        int pos = 0;
        while (pos < leaf.size && leaf.keys[pos] < p) pos++;
        if (pos < leaf.size && leaf.keys[pos] == p) return false;
        for (int i = leaf.size; i > pos; i--) leaf.keys[i] = leaf.keys[i - 1];
        leaf.keys[pos] = p;
        leaf.size++;
        store_node(leaf_id, leaf);
        if (leaf.size > M) {
            split_leaf(leaf_id);
        }
        return true;
    }

    bool erase(const Key &key, Value val) {
        Pair p{key, val};
        if (header.root == -1) return false;
        int32_t leaf_id = find_leaf(p);
        Node leaf;
        load_node(leaf_id, leaf);
        int pos = 0;
        while (pos < leaf.size && leaf.keys[pos] < p) pos++;
        if (pos >= leaf.size || !(leaf.keys[pos] == p)) return false;
        for (int i = pos; i < leaf.size - 1; i++) leaf.keys[i] = leaf.keys[i + 1];
        leaf.size--;
        store_node(leaf_id, leaf);
        if (leaf.size == 0 && leaf.self == header.root) {
            free_node(leaf.self);
            header.root = -1;
            header.head = -1;
        }
        return true;
    }

    std::vector<Value> find(const Key &key) {
        std::vector<Value> res;
        if (header.root == -1) return res;
        Pair p{key, INT32_MIN};
        int32_t leaf_id = find_leaf(p);
        Node leaf;
        while (leaf_id != -1) {
            load_node(leaf_id, leaf);
            int i = 0;
            while (i < leaf.size && leaf.keys[i].key < key) i++;
            bool past = false;
            while (i < leaf.size) {
                if (key < leaf.keys[i].key) { past = true; break; }
                res.push_back(leaf.keys[i].val);
                i++;
            }
            if (past) break;
            leaf_id = leaf.next;
        }
        return res;
    }

    template <class Fn>
    void iterate_all(Fn fn) {
        int32_t cur = header.head;
        Node n;
        while (cur != -1) {
            load_node(cur, n);
            for (int i = 0; i < n.size; i++) fn(n.keys[i].key, n.keys[i].val);
            cur = n.next;
        }
    }

private:
    void split_leaf(int32_t leaf_id) {
        Node leaf;
        load_node(leaf_id, leaf);
        int mid = leaf.size / 2;
        int32_t new_id = alloc_node();
        Node newn;
        memset(&newn, 0, sizeof(Node));
        newn.is_leaf = 1;
        newn.self = new_id;
        newn.size = leaf.size - mid;
        for (int i = 0; i < newn.size; i++) newn.keys[i] = leaf.keys[mid + i];
        newn.parent = leaf.parent;
        newn.prev = leaf.self;
        newn.next = leaf.next;

        int32_t old_next = leaf.next;
        leaf.size = mid;
        leaf.next = new_id;
        store_node(leaf_id, leaf);
        store_node(new_id, newn);
        if (old_next != -1) {
            Node nxt;
            load_node(old_next, nxt);
            nxt.prev = new_id;
            store_node(old_next, nxt);
        }

        Pair split_key = newn.keys[0];
        insert_into_parent(leaf_id, split_key, new_id);
    }

    void insert_into_parent(int32_t left_id, const Pair &key, int32_t right_id) {
        Node left;
        load_node(left_id, left);
        if (left.parent == -1) {
            int32_t root_id = alloc_node();
            Node root;
            memset(&root, 0, sizeof(Node));
            root.is_leaf = 0;
            root.self = root_id;
            root.size = 1;
            root.keys[0] = key;
            root.children[0] = left_id;
            root.children[1] = right_id;
            root.parent = -1;
            store_node(root_id, root);
            left.parent = root_id;
            store_node(left_id, left);
            Node right;
            load_node(right_id, right);
            right.parent = root_id;
            store_node(right_id, right);
            header.root = root_id;
            return;
        }
        int32_t parent_id = left.parent;
        Node parent;
        load_node(parent_id, parent);
        int pos = 0;
        while (pos <= parent.size && parent.children[pos] != left_id) pos++;
        for (int i = parent.size; i > pos; i--) parent.keys[i] = parent.keys[i - 1];
        for (int i = parent.size + 1; i > pos + 1; i--) parent.children[i] = parent.children[i - 1];
        parent.keys[pos] = key;
        parent.children[pos + 1] = right_id;
        parent.size++;
        store_node(parent_id, parent);
        Node right;
        load_node(right_id, right);
        right.parent = parent_id;
        store_node(right_id, right);
        if (parent.size > M) {
            split_internal(parent_id);
        }
    }

    void split_internal(int32_t node_id) {
        Node node;
        load_node(node_id, node);
        int mid = node.size / 2;
        Pair up_key = node.keys[mid];
        int32_t new_id = alloc_node();
        Node newn;
        memset(&newn, 0, sizeof(Node));
        newn.is_leaf = 0;
        newn.self = new_id;
        newn.size = node.size - mid - 1;
        for (int i = 0; i < newn.size; i++) newn.keys[i] = node.keys[mid + 1 + i];
        for (int i = 0; i <= newn.size; i++) newn.children[i] = node.children[mid + 1 + i];
        newn.parent = node.parent;
        node.size = mid;
        store_node(node_id, node);
        store_node(new_id, newn);
        // Update children of newn
        for (int i = 0; i <= newn.size; i++) {
            Node c;
            load_node(newn.children[i], c);
            c.parent = new_id;
            store_node(newn.children[i], c);
        }
        insert_into_parent(node_id, up_key, new_id);
    }
};

#endif
