#pragma once

#include "common.h"
#include "node_dpu.h"
#include "stdlib.h"
// #include "garbage_collection.h"

typedef struct ht_slot {
    uint32_t pos;  // ideal position in the hash table
    uint32_t v;    // value
} ht_slot;

#define null_ht_slot ((ht_slot){.pos = 0, .v = 0})


// #define LX_HASHTABLE_SIZE (1 << 10)

extern __mram_ptr ht_slot l3ht[];  // must be 8 bytes aligned. 0 as null.
extern int l3htcnt;

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

static inline void storage_init() {
    ht_slot hs = null_ht_slot;
    for (int i = 0; i < LX_HASHTABLE_SIZE; i++) {
        l3ht[i] = hs;
    }
    // L3_gc_init();
}
static inline void ht_insert(__mram_ptr ht_slot* ht, int32_t pos,
                             uint32_t val) {
    int ipos = pos;
    ht_slot hs = ht[pos];
    while (hs.v != 0) {  // find slot
        pos = (pos + 1) & (LX_HASHTABLE_SIZE - 1);
        hs = ht[pos];
        IN_DPU_ASSERT(pos != ipos, "htisnert: full\n");
    }
    ht[pos] = (ht_slot){.pos = ipos, .v = val};
    l3htcnt++;
}

static inline bool ht_no_greater_than(int a, int b) {  // a <= b with wrapping
    int delta = b - a;
    if (delta < 0) {
        delta += LX_HASHTABLE_SIZE;
    }
    return delta < (LX_HASHTABLE_SIZE >> 1);
}

static inline void ht_delete(__mram_ptr ht_slot* ht, int32_t pos,
                             uint32_t val) {
    int ipos = pos;  // initial position
    ht_slot hs = ht[pos];
    while (hs.v != val) {  // find slot
        pos = (pos + 1) & (LX_HASHTABLE_SIZE - 1);
        hs = ht[pos];
        IN_DPU_ASSERT(pos != ipos, "htisnert: full\n");
    }
    ipos = pos;  // position to delete
    pos = (pos + 1) & (LX_HASHTABLE_SIZE - 1);

    while (true) {
        hs = ht[pos];
        if (hs.v == 0) {
            ht[ipos] = null_ht_slot;
            break;
        } else if (ht_no_greater_than(hs.pos, ipos)) {
            ht[ipos] = hs;
            ipos = pos;
        } else {
        }
        pos = (pos + 1) & (LX_HASHTABLE_SIZE - 1);
        IN_DPU_ASSERT(pos != ipos, "htisnert: full\n");
    }
    l3htcnt--;
}

static inline uint32_t ht_search(int64_t key, int (*filter)(ht_slot, int64_t)) {
    int ipos = hash_to_addr(key, 0, LX_HASHTABLE_SIZE);
    int pos = ipos;
    while (true) {
        ht_slot hs = l3ht[pos];  // pull to wram
        int v = filter(hs, key);
        if (v == -1) {  // empty slot
            return (uint32_t)-1;
            // continue;
        } else if (v == 0) {  // incorrect value
            pos = (pos + 1) & (LX_HASHTABLE_SIZE - 1);
        } else if (v == 1) {  // correct value;
            return (uint32_t)hs.v;
        }
        IN_DPU_ASSERT(pos != ipos, "htisnert: full\n");
    }
}

static inline L3node* init_L3(int64_t key, int height, pptr down,
                              uint8_t* buffer) {
    L3node* nn = (L3node*)buffer;
    nn->key = key;
    nn->height = height;
    nn->down = down;
    nn->left = (mppptr)(l3buffer + l3cnt + sizeof(L3node));
    nn->right =
        (mppptr)(l3buffer + l3cnt + sizeof(L3node) + sizeof(pptr) * height);
    memset(buffer + sizeof(L3node), -1, sizeof(pptr) * height * 2);
    return nn;
}

static inline mL3ptr apply_L3(L3node* wptr, int height, uint32_t size) {
    __mram_ptr void* mptr = l3buffer + l3cnt;
    // mL3ptr mptr = L3_allocate(height);
    // printf("applyL3 %x %x %u\n", wptr, mptr, size);
    mram_write((void*)wptr, mptr, size);
    l3cnt += size;
    return mptr;
}

static inline mL3ptr get_new_L3(int64_t key, int height, pptr down,
                                uint32_t* actual_size) {
    uint8_t buffer[40 + sizeof(pptr) * 2 * MAX_L3_HEIGHT];
    L3node* nn = init_L3(key, height, down, buffer);
    *actual_size = sizeof(L3node) + sizeof(pptr) * height * 2;
    mL3ptr ret = apply_L3(nn, height, *actual_size);
    ht_insert(l3ht, hash_to_addr(key, 0, LX_HASHTABLE_SIZE), (uint32_t)ret);
    return ret;
}