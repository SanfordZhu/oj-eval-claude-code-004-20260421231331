#ifndef KEYS_HPP
#define KEYS_HPP

#include <cstring>
#include <string>

// Fixed-length string key for B+ tree indexes.
template <int N>
struct FixedKey {
    char data[N];
    FixedKey() { memset(data, 0, N); }
    FixedKey(const std::string &s) {
        memset(data, 0, N);
        int len = (int)s.size();
        if (len > N) len = N;
        memcpy(data, s.c_str(), len);
    }
    bool operator<(const FixedKey<N> &o) const {
        return memcmp(data, o.data, N) < 0;
    }
    bool operator==(const FixedKey<N> &o) const {
        return memcmp(data, o.data, N) == 0;
    }
    std::string str() const {
        // find null terminator
        int len = 0;
        while (len < N && data[len] != 0) len++;
        return std::string(data, len);
    }
};

using UserKey = FixedKey<32>;   // up to 30 chars
using ISBNKey = FixedKey<24>;   // up to 20 chars
using NameKey = FixedKey<64>;   // up to 60 chars
using AuthorKey = FixedKey<64>; // up to 60 chars
using KeywordKey = FixedKey<64>;// up to 60 chars

#endif
