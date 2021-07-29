#pragma once

#include "common.h"
#include "node_dpu.h"


// L0,1,2,3 1MB
#define LX_BUFFER_SIZE (1 << 20)

// HASH TABLE 1MB
#define LX_HASHTABLE_SIZE ((1 << 20) >> 3)

extern __mram_ptr uint64_t l3ht[]; // must be 8 bytes aligned

extern __mram_ptr uint8_t l3buffer[];
extern int l3cnt;
extern mL3ptr root;

// __mram_noinit uint8_t l2buffer[LX_BUFFER_SIZE];
// int l2cnt;

// __mram_noinit uint8_t l1buffer[LX_BUFFER_SIZE];
// int l1cnt;

// __mram_noinit uint8_t l0buffer[LX_BUFFER_SIZE];
// int l0cnt;

// __mram_ptr uint8_t* l2ht[LX_HASHTABLE_SIZE];
// __mram_ptr uint8_t* l1ht[LX_HASHTABLE_SIZE];
// __mram_ptr uint8_t* l0ht[LX_HASHTABLE_SIZE];

inline void ht_insert(__mram_ptr uint64_t* ht, uint32_t pos, uint64_t val) {
    while (ht[pos] != 0) pos ++;
    ht[pos] = val;
}

inline mL3ptr ht_get_L3(int64_t key) {
    int ipos = hash_to_addr(key, 0, LX_HASHTABLE_SIZE);
    int pos = ipos;
    while (true) {
        if (l3ht[pos] == 0) continue;
        mL3ptr np = (mL3ptr)l3ht[pos];
        if (np->key == key) {
            return np;
        }
        pos ++;
        if (pos == ipos) {
            assert(false);
        }
    }
}

inline L3node* init_L3(int64_t key, int height, pptr down, uint8_t* buffer) {
    L3node *nn = (L3node*)buffer;
    nn->key = key;
    nn->height = height;
    nn->down = down;
    nn->left = (mppptr)(l3buffer + l3cnt + sizeof(L3node));
    nn->right =
        (mppptr)(l3buffer + l3cnt + sizeof(L3node) + sizeof(pptr) * height);
    memset(buffer + sizeof(L3node), -1, sizeof(pptr) * height * 2);
    return nn;
}

inline mL3ptr apply_L3(L3node* wptr, uint32_t size) {
    __mram_ptr void* mptr = l3buffer + l3cnt;
    // printf("applyL3 %x %x %u\n", wptr, mptr, size);
    mram_write((void*)wptr, mptr, size);
    l3cnt += size;
    return mptr;
}

inline mL3ptr get_new_L3(int64_t key, int height, pptr down, uint32_t* actual_size) {
    uint8_t buffer[40 + sizeof(pptr) * 2 * MAX_TOTAL_HEIGHT];
    L3node* nn = init_L3(key, height, down, buffer);
    *actual_size = sizeof(L3node) + sizeof(pptr) * height * 2;
    mL3ptr ret = apply_L3(nn, *actual_size);
    ht_insert(l3ht, hash_to_addr(key, 0, LX_HASHTABLE_SIZE), (uint64_t)ret);
    return ret;
}