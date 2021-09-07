#pragma once

#include <cstdlib>
#include <random>
#include <algorithm>
#include <parlay/utilities.h>
using namespace std;

// mt19937 generators[MAX_THREAD_NUM];
// uniform_int_distribution<int64_t> key_distribution[MAX_THREAD_NUM];
// uniform_int_distribution<int64_t> height_distribution[MAX_THREAD_NUM];
int64_t nxt_rand[MAX_THREAD_NUM];

inline int64_t randint64_rand() {
    uint64_t v = rand();
    v = (v << 31) + rand();
    v = (v << 2) + (rand() & 3);
    // printf("*%llu %llu %d\n", v, 1ull << 63, v >= (1ull << 63));
    if (v >= (1ull << 63)) {
        return v - (1ull << 63);
    } else {
        return v - (1ull << 63);
    }
}

inline void init_rand() {
    srand(time(NULL));
    for (int i = 0; i < MAX_THREAD_NUM; i ++) {
        nxt_rand[i] = parlay::hash64(randint64_rand());
    }
}

inline int64_t randint64(int thread_id) {
    int64_t& tmp = nxt_rand[thread_id];
    tmp = parlay::hash64(tmp);
    return tmp;
}

const int COMPRESSION_RATE = 100;
inline int64_t key_filter(int64_t key) {
    return key / COMPRESSION_RATE;
}

inline int64_t randkey(int thread_id) {
    return key_filter(randint64(thread_id));
}

template <class F>  // [valid, invalid]
inline int binary_search_local_l(int l, int r, F f) {
    // ASSERT(l >= -1 && r >= 0 && r > l);
    int mid = (l + r) >> 1;
    while (r - l > 1) {
        if (f(mid)) {
            l = mid;
        } else {
            r = mid;
        }
        mid = (l + r) >> 1;
    }
    return l;
}

template <class F>  // [invalid, valid]
inline int binary_search_local_r(int l, int r, F f) {
    // ASSERT(l >= -1 && r >= 0 && r > l);
    int mid = (l + r) >> 1;
    while (r - l > 1) {
        if (f(mid)) {
            r = mid;
        } else {
            l = mid;
        }
        mid = (l + r) >> 1;
    }
    return r;
}
