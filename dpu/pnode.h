#pragma once

#include <alloc.h>
// #include <barrier.h>
// #include <stdlib.h>

#include "hashtable_l3size.h"
#include "common.h"

// extern volatile int host_barrier;
// BARRIER_INIT(b_barrier, NR_TASKLETS);
MUTEX_INIT(p_lock);

// Bnode
__mram_noinit Pnode pbuffer[P_BUFFER_SIZE / sizeof(Pnode)];
__host uint32_t pcnt = 1;
// mPptr pbuffer_start, pbuffer_end;

extern __mram_ptr ht_slot l3ht[];
extern int l3htcnt;

static inline mPptr reserve_space_p(int len) {
    mutex_lock(p_lock);
    mPptr ret = pbuffer + pcnt;
    pcnt += len;
    mutex_unlock(p_lock);
    // SPACE_IN_DPU_ASSERT(pcnt < (P_BUFFER_SIZE / sizeof(Pnode)), "rsp! of\n");
    return ret;
}

static inline void p_newnode(int64_t _key, int64_t _value, mPptr newnode) {
    Pnode nn;
    nn.key = _key;
    nn.value = _value;
    // nn.up = null_pptr;

    // *newnode = nn;
    mram_write(&nn, newnode, sizeof(Pnode));
    IN_DPU_ASSERT(LX_HASHTABLE_SIZE == lb(LX_HASHTABLE_SIZE),
                  "hh_dpu! not 2^x\n");
    int ret = ht_insert(l3ht, &l3htcnt, hash_to_addr(_key, LX_HASHTABLE_SIZE),
              (uint32_t)newnode);
    (void)ret;
}

static inline int p_ht_get(ht_slot v, int64_t key) {
    if (v.v == 0) {
        return -1;
    }
    mPptr addr = (mPptr)v.v;
    if (addr->key == key) {
        return 1;
    }
    return 0;
}

static inline pptr p_get(int64_t key) {
    uint32_t htv = ht_search(l3ht, key, p_ht_get);
    if (htv == INVALID_DPU_ADDR) {
        return (pptr){.id = INVALID_DPU_ID, .addr = INVALID_DPU_ADDR};
    } else {
        return (pptr){.id = DPU_ID, .addr = htv};
    }
}

static inline int64_t p_get_key(pptr addr) {
    mPptr ptr = (mPptr)addr.addr;
    return ptr->key;
}

static inline int64_t p_get_value(pptr addr) {
    mPptr ptr = (mPptr)addr.addr;
    return ptr->value;
}

static inline void p_remove(int64_t key) {
    uint32_t htv = ht_search(l3ht, key, p_ht_get);
    if (htv == INVALID_DPU_ADDR) {
        return;
    } else {
        ht_delete(l3ht, &l3htcnt, hash_to_addr(key, LX_HASHTABLE_SIZE), htv);
        return;
    }
}