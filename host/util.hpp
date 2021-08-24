#pragma once

#include <cstdlib>
#include <random>
#include <algorithm>
using namespace std;

mt19937 generators[MAX_THREAD_NUM];
uniform_int_distribution<int64_t> key_distribution[MAX_THREAD_NUM];
uniform_int_distribution<int64_t> height_distribution[MAX_THREAD_NUM];

inline void init_rand() {
    srand(time(NULL));
    // srand(147);
    for (int i = 0; i < MAX_THREAD_NUM; i ++) {
        key_distribution[i] = uniform_int_distribution<int64_t>(INT64_MIN, INT64_MAX);
        height_distribution[i] = uniform_int_distribution<int64_t>(0, INT64_MAX);
    }
}

// inline int64_t parallel_randheightint64(int thread_id) {
//     ASSERT(thread_id >= 0 && thread_id < MAX_THREAD_NUM);
//     return height_distribution[thread_id](generators[thread_id]);
// }

// inline int64_t parallel_randint64(int thread_id) {
//     ASSERT(thread_id >= 0 && thread_id < MAX_THREAD_NUM);
//     return key_distribution[thread_id](generators[thread_id]);
// }

inline int64_t randint64() {
    uint64_t v = rand();
    v = (v << 31) + rand();
    v = (v << 3) + (rand() & 3);
    // printf("*%llu %llu %d\n", v, 1ull << 63, v >= (1ull << 63));
    if (v >= (1ull << 63)) {
        return v - (1ull << 63);
    } else {
        return v - (1ull << 63);
    }
}
