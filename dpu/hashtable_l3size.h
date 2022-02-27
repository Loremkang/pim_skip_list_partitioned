#pragma once

#include <stdlib.h>
#include <mutex.h>
#include "common.h"
#include "node_dpu.h"
// #include "task_framework_dpu.h"
// #include "garbage_collection.h"

typedef struct ht_slot {
    uint32_t pos;  // ideal position in the hash table
    uint32_t v;    // value
} ht_slot;

#define null_ht_slot ((ht_slot){.pos = 0, .v = 0})


// #define LX_HASHTABLE_SIZE (1 << 10)
// L3
// MUTEX_INIT(get_new_L3_lock);
MUTEX_INIT(ht_lock);
extern __mram_ptr ht_slot l3ht[];  // must be 8 bytes aligned. 0 as null.
extern int l3htcnt;
extern __mram_ptr uint8_t l3buffer[];
// extern int l3cnt;

extern mBptr root;
extern int64_t DPU_ID;

__host bool storage_inited = false;
static inline void storage_init() {
    if (storage_inited) {
        return;
    }
    storage_inited = true;
    l3htcnt = 0;
    ht_slot hs = null_ht_slot;
    for (int i = 0; i < LX_HASHTABLE_SIZE; i++) {
        l3ht[i] = hs;
    }
    // L3_gc_init();
}

static inline int32_t ht_insert(__mram_ptr ht_slot* ht, int* cnt, int32_t pos,
                             uint32_t val) {
    mutex_lock(ht_lock);
    int ipos = pos;
    ht_slot hs;
    mram_read(ht + pos, &hs, sizeof(ht_slot));
    while (hs.v != 0) {  // find slot
        pos = (pos + 1) & (LX_HASHTABLE_SIZE - 1);
        mram_read(ht + pos, &hs, sizeof(ht_slot));
        IN_DPU_ASSERT(pos != ipos, "htisnert: full\n");
    }
    ht_slot hh = (ht_slot){.pos = ipos, .v = val};
    mram_write(&hh, ht + pos, sizeof(ht_slot));
    *cnt = *cnt + 1;
    mutex_unlock(ht_lock);
    return pos;
}

static inline bool ht_no_greater_than(int a, int b) {  // a <= b with wrapping
    int delta = b - a;
    if (delta < 0) {
        delta += LX_HASHTABLE_SIZE;
    }
    return delta < (LX_HASHTABLE_SIZE >> 1);
}

static inline void ht_delete(__mram_ptr ht_slot* ht, int* cnt, int32_t pos,
                             uint32_t val) {
    mutex_lock(ht_lock);
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
    *cnt = *cnt - 1;
    mutex_unlock(ht_lock);
}

static inline uint32_t ht_search(__mram_ptr ht_slot* ht, int64_t key,
                                 int (*filter)(ht_slot, int64_t)) {
    int ipos = hash_to_addr(key, LX_HASHTABLE_SIZE);
    int pos = ipos;
    while (true) {
        ht_slot hs = ht[pos];  // pull to wram
        int v = filter(hs, key);
        if (v == -1) {  // empty slot
            return INVALID_DPU_ADDR;
            // continue;
        } else if (v == 0) {  // incorrect value
            pos = (pos + 1) & (LX_HASHTABLE_SIZE - 1);
        } else if (v == 1) {  // correct value;
            return (uint32_t)hs.v;
        }
        IN_DPU_ASSERT(pos != ipos, "htisnert: full\n");
    }
}

// static inline void b_build_up(bnode* bn, mBptr nn, int n) {
//     for (int j = 0; j < n; j ++) {
//         mBptr tmp = (mBptr)bn->addrs[j].addr;
//         tmp->up = (pptr){.id = DPU_ID, .addr = nn};
//     }
// }

// static inline void b_node_fill_apply(bnode* bn, mBptr nn, int n, int64_t* keys,
//                                int64_t* addrs, bool change_up) {
//     bn->size = n;
//     for (int j = 0; j < n; j++) {
//         bn->keys[j] = keys[j];
//         bn->addrs[j] = I64_TO_PPTR(addrs[j]);
//     }
//     for (int j = n; j < DB_SIZE; j++) {
//         bn->keys[j] = INT64_MIN;
//         bn->addrs[j] = null_pptr;
//     }
//     if (change_up) {
//         b_build_up(bn, nn, n);
//     }
//     mram_write(bn, nn, sizeof(bnode));
// }

// L3
// static inline uint32_t L3_node_size(int height) {
//     return sizeof(L3node) + sizeof(pptr) * height * 2;
// }

// static inline L3node* init_L3(int64_t key, int64_t value, int height,
//                               uint8_t* buffer, __mram_ptr void* maddr) {
//     L3node* nn = (L3node*)buffer;
//     nn->key = key;
//     nn->value = value;
//     nn->height = height;
//     nn->left = (mppptr)(maddr + sizeof(L3node));
//     nn->right = (mppptr)(maddr + sizeof(L3node) + sizeof(pptr) * height);
//     // for (int i = 0; i < sizeof(pptr) * height * 2; i ++) {
//     //     buffer[sizeof(L3node) + i] = (uint8_t)-1;
//     // }
//     memset(buffer + sizeof(L3node), -1, sizeof(pptr) * height * 2);
//     return nn;
// }

// static inline __mram_ptr void* reserve_space_L3(uint32_t size) {
//     // mutex_lock(get_new_L3_lock);
//     __mram_ptr void* ret = l3buffer + l3cnt;
//     l3cnt += size;
//     // mutex_unlock(get_new_L3_lock);
//     return ret;
// }

// static inline mL3ptr get_new_L3(int64_t key, int64_t value, int height,
// __mram_ptr void* maddr) {
//     int size = L3_node_size(height);
//     // __mram_ptr void* maddr = reserve_space_L3(size);
//     uint8_t buffer[sizeof(L3node) + sizeof(pptr) * 2 * MAX_L3_HEIGHT];
//     L3node* nn = init_L3(key, value, height, buffer, maddr);
//     mram_write((void*)nn, maddr, size);
//     ht_insert(l3ht, &l3htcnt, hash_to_addr(key, LX_HASHTABLE_SIZE),
//               (uint32_t)maddr);
//     return (mL3ptr)maddr;
// }
