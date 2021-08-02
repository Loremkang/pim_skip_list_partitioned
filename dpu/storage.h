#pragma once

#include "common.h"
#include "node_dpu.h"


typedef struct ht_slot {
    uint32_t pos; // ideal position in the hash table
    uint32_t v; // value
} ht_slot;

#define null_ht_slot ((ht_slot){.pos = 0, .v = 0})

// L0,1,2,3 1MB
#define LX_BUFFER_SIZE (1 << 20)

// HASH TABLE 1MB. should be power of 2
// #define LX_HASHTABLE_SIZE ((1 << 20) >> 3)
#define LX_HASHTABLE_SIZE (1 << 10)

extern __mram_ptr ht_slot l3ht[]; // must be 8 bytes aligned. 0 as null.
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
    for (int i = 0; i < LX_HASHTABLE_SIZE; i ++) {
        l3ht[i] = hs;
    }
}
static inline void ht_insert(__mram_ptr ht_slot* ht, int32_t pos, uint32_t val) {
    int ipos = pos;
    ht_slot hs = ht[pos];
    while (hs.v != 0) { // find slot
        pos = (pos + 1) & (LX_HASHTABLE_SIZE - 1);
        hs = ht[pos];
        if (pos == ipos) { // full
            assert(false);
        }
    }
    ht[pos] = (ht_slot){.pos = ipos, .v = val};
    l3htcnt ++;
}

static inline bool ht_no_greater_than(int a, int b) { // a <= b with wrapping
    int delta = b - a;
    if (delta < 0) {
        delta += LX_HASHTABLE_SIZE;
    }
    return delta < (LX_HASHTABLE_SIZE >> 1);
}

static inline void ht_delete(__mram_ptr ht_slot* ht, int32_t pos, uint32_t val) {
    int ipos = pos; // initial position
    ht_slot hs = ht[pos];
    while (hs.v != val) { // find slot
        pos = (pos + 1) & (LX_HASHTABLE_SIZE - 1);
        hs = ht[pos];
        // printf("%d %d %d %d\n", pos, hs.pos, hs.v, val);
        if (pos == ipos) {  // full
        // printf("FULL!!");
        // return;
            assert(false);
        }
    }
    printf("ipos=%d pos=%d\n", ipos, pos);
    ipos = pos;  // position to delete
    pos = (pos + 1) & (LX_HASHTABLE_SIZE - 1);

    while (true) {
        hs = ht[pos];
        if (hs.v == 0) {
            // ht_slot hs2 = null_ht_slot;
            // mram_write(&hs2, &ht[ipos], sizeof(ht_slot));
            // ht[ipos].pos = ht[ipos].v = 0;
                ht[ipos] = null_ht_slot;
            // if (ipos < 90 && ipos > 80) {
            // printf("ht[ipos] %u %d %d\n", &ht[ipos], ht[ipos].pos, ht[ipos].v);
            // printf("ht[pos] %d %d\n", hs.pos, hs.v);
            // printf("%d %d\n", ipos, pos);
            // }
            break;
        } else if (ht_no_greater_than(hs.pos, ipos)) {
            // printf("move %d %d %d\n", pos, hs.pos, ipos);
            ht[ipos] = hs;
            ipos = pos;
        } else {
            // printf("skip %d %d %d\n", pos, hs.pos, hs.v);
        }
        pos = (pos + 1) & (LX_HASHTABLE_SIZE - 1);
        if (pos == ipos) {  // full
        // printf("FULL!!");
        // return;
            assert(false);
        }
    }
    l3htcnt --;
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
        if (pos == ipos) {
            assert(false);
        }
    }
}

// static inline mL3ptr ht_get_L3(int64_t key) {
//     int ipos = hash_to_addr(key, 0, LX_HASHTABLE_SIZE);
//     int pos = ipos;
//     while (true) {
//         if (l3ht[pos] == 0) continue;
//         mL3ptr np = (mL3ptr)l3ht[pos];
//         if (np->key == key) {
//             return np;
//         }
//         pos ++;
//         if (pos == ipos) {
//             assert(false);
//         }
//     }
// }

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

static inline mL3ptr apply_L3(L3node* wptr, uint32_t size) {
    __mram_ptr void* mptr = l3buffer + l3cnt;
    // printf("applyL3 %x %x %u\n", wptr, mptr, size);
    mram_write((void*)wptr, mptr, size);
    l3cnt += size;
    return mptr;
}

static inline mL3ptr get_new_L3(int64_t key, int height, pptr down,
                                uint32_t* actual_size) {
    uint8_t buffer[40 + sizeof(pptr) * 2 * MAX_TOTAL_HEIGHT];
    L3node* nn = init_L3(key, height, down, buffer);
    *actual_size = sizeof(L3node) + sizeof(pptr) * height * 2;
    mL3ptr ret = apply_L3(nn, *actual_size);
    ht_insert(l3ht, hash_to_addr(key, 0, LX_HASHTABLE_SIZE), (uint32_t)ret);
    return ret;
}