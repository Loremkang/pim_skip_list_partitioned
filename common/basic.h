#pragma once

#include <stdint.h>
#include <assert.h>
#include <stdio.h>

#define maddr __mram_ptr node *

#define EMPTY 0
#define L3_INIT 1
#define L3_INSERT 2
#define L3_REMOVE 3
// #define LOWER_INIT 1
// #define BUILD_UP_DOWN 3
// #define PREDECESSOR 4
// #define LOWER_INSERT 5
// #define FETCH_NODE 7
// #define BUILD_LEFT_RIGHT 8
// #define GET 9
// #define GET_HEIGHT 10
// #define FETCH_NODE_BY_HEIGHT 11


#define null_pptr ((pptr){.id = -1, .addr = -1})

typedef struct pptr {
    uint32_t id;
    uint32_t addr;
} pptr;

typedef struct {
    int64_t key;
    pptr down, right; // dpuid & addr
    pptr up, left; // not used yet
    pptr addr; // used to debug
    int64_t chk;
    uint8_t height;
    uint8_t up_chain_height;
} node;

inline int hh(int64_t key, uint64_t height, uint64_t M) {
    key = (key % M) * 47 + height;
    return (key * 23 + 17) % M;
}

inline int hash_to_dpu(int64_t key, uint64_t height, uint64_t M) {
    return hh(key, height, M);
}

inline int hash_to_addr(int64_t key, uint64_t height, uint64_t M) {
    return hh(key, height, M);
}

inline void print_pptr(pptr x, char* str) { // ?? strange bug. need to copy before printing.
    pptr y = x;
    printf("%d-%x", y.id, y.addr);
    printf("%s", str);
}