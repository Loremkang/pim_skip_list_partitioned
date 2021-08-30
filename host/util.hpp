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