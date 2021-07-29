#pragma once

#include <stdint.h>
#include <assert.h>
#include <stdio.h>

typedef struct pptr {
    uint32_t id;
    uint32_t addr;
} pptr;

#define null_pptr ((pptr){.id = (uint32_t)-1, .addr = (uint32_t)-1})

inline void print_pptr(pptr x, char* str) { // ?? strange bug. need to copy before printing.
    pptr y = x;
    printf("%d-%x", y.id, y.addr);
    printf("%s", str);
}

// typedef struct {
//     int64_t key;
//     pptr down, right; // dpuid & addr
//     pptr up, left; // not used yet
//     pptr addr; // used to debug
//     int64_t chk;
//     uint8_t height;
//     uint8_t up_chain_height;
// } node;

static inline int hh(int64_t key, uint64_t height, uint64_t M) {
    key = (key % M) * 47 + height;
    return (key * 23 + 17) % M;
}

static inline int hash_to_dpu(int64_t key, uint64_t height, uint64_t M) {
    return hh(key, height, M);
}

static inline int hash_to_addr(int64_t key, uint64_t height, uint64_t M) {
    return hh(key, height, M);
}