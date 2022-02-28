#pragma once

// #include "task.h"
// #define KHB_DEBUG

#define MAX_TASK_BUFFER_SIZE_PER_DPU (3000 << 10) // 2 MB
#define MAX_TASK_COUNT_PER_DPU_PER_BLOCK ((2000 << 10) >> 3) // 1 MB = 125 K

#define MAX_L3_HEIGHT (20)

/* Size of the buffer on which the checksum will be performed */
// #define BUFFER_SIZE (200)
#define BATCH_SIZE (NR_DPUS * MAX_TASK_COUNT_PER_DPU)

// L0,1,2,3 50MB
#define LX_BUFFER_SIZE (37 << 20)

// HASH TABLE 8MB. should be power of 2
#define LX_HASHTABLE_SIZE ((4 << 20) >> 3)

static inline int hh(int64_t key, uint64_t height, uint64_t M) {
    // assert(height == 0);
    // printf("KEY: %lld\n", key);
    key = (key % M);
    // printf("KEY: %lld\n", key);
    key = (key < 0) ? (key + M) : key;
    // printf("KEY: %lld\n", key);
    key = key * 47 + height;
    // printf("KEY: %lld\n", key);
    return (key * 23 + 17) % M;
}

static inline int hash_to_dpu(int64_t key, uint64_t height, uint64_t M) {
    return hh(key, height, M);
}

static inline int lb(int64_t x) { return x & (-x); }

static inline int hh_dpu(int64_t key, uint64_t M) { return key & (M - 1); }

static inline int hash_to_addr(int64_t key, uint64_t M) {
    return hh_dpu(key, M);
}