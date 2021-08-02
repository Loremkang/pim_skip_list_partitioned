#pragma once

#include "common.h"
#include "task_dpu.h"
#include "storage.h"

extern uint64_t DPU_ID;
extern __mram_ptr ht_slot l3ht[]; 

static inline void L3_init(L3_insert_task *tit) {
    assert(l3cnt == 8);
    storage_init();
    uint32_t actual_size;
    root = get_new_L3(LLONG_MIN, tit->height, tit->addr, &actual_size);
    L3_insert_reply tir = (L3_insert_reply){.key = tit->key, .addr = (pptr){.id = DPU_ID, .addr = (uint32_t)root}};
    push_task(L3_INIT, &tir, sizeof(L3_insert_reply));
}

// void L3_insert(L3_insert_task *tit);

static inline void L3_insert(L3_insert_task *tit) {
    uint32_t actual_size;
    mL3ptr newnode = get_new_L3(tit->key, tit->height,
                               tit->addr, &actual_size);

    assert(newnode->height <= root->height);

    // find search path
    mL3ptr tmp = root;
    int64_t ht = root->height - 1;
    while (ht >= 0) {
        pptr l = tmp->left[ht], r = tmp->right[ht];
        mL3ptr rn = (mL3ptr)r.addr;
        assert(l.id == DPU_ID || l.id == (uint32_t)-1);
        assert(r.id == DPU_ID || r.id == (uint32_t)-1);
        if (r.id != (uint32_t)-1 &&
            rn->key <= tit->key) {  // should not be equal
            tmp = rn;                // go right
            assert(rn->key != tit->key);
            continue;
        }
        if (ht < (newnode->height)) {  // insert to right
            newnode->right[ht] = r;
            newnode->left[ht] = (pptr){.id = DPU_ID, .addr = (uint32_t)tmp};
            if (r.id != (uint32_t)-1) {
                assert(rn->left[ht].id == DPU_ID);
                // rn->left[ht].addr = (uint32_t)newnode;
                rn->left[ht] = (pptr){.id = DPU_ID, .addr = (uint32_t)newnode};
            }
            tmp->right[ht] = (pptr){.id = DPU_ID, .addr = (uint32_t)newnode};
        }
        ht--;
    }
    L3_insert_reply tir = (L3_insert_reply){.key = tit->key, .addr = (pptr){.id = DPU_ID, .addr = (uint32_t)newnode}};
    push_task(L3_INSERT, &tir, sizeof(L3_insert_reply));
}

static inline int64_t L3_search(L3_search_task *tst);

// bool printtt;

static inline int L3_ht_get(ht_slot v, int64_t key) {
    if (v.v == 0) {
        return -1;
    }
    mL3ptr np = (mL3ptr)v.v;
    if (np->key == key) {
        return 1;
    }
    return 0;
}

static inline void L3_remove(L3_remove_task *trt) {
    uint32_t htv = ht_search(trt->key, L3_ht_get);
    if (htv == (uint32_t)-1) {
        assert(false);
        return; // not found;
    }
    mL3ptr tmp = (mL3ptr)htv;

    if (tmp == (uint32_t)-1)
    // mL3ptr tmp = ht_get_L3(trt->key);
    assert(tmp->key == trt->key);
    for (int ht = 0; ht < tmp->height; ht++) {
        pptr l = tmp->left[ht], r = tmp->right[ht];
        mL3ptr ln = (mL3ptr)l.addr, rn = (mL3ptr)r.addr;
        assert(l.id != (uint32_t)-1);
        if (r.id != (uint32_t)-1) {
            assert(rn->left[ht].id == DPU_ID);
            rn->left[ht] = l;
        }
        ln->right[ht] = r;
        // tmp->left[ht] = null_pptr;
        // tmp->right[ht] = null_pptr;
    }
    // printf("\n\t%lld\n", trt->key);
    ht_delete(l3ht, hash_to_addr(trt->key, 0, LX_HASHTABLE_SIZE), (uint32_t)tmp);
}

static inline int64_t L3_search(L3_search_task *tst) {
    mL3ptr tmp = root;
    int64_t ht = root->height - 1;
    while (ht >= 0) {
        // if (printtt) {
        //     printf("!!%lld %lld\n", ht, tmp->key);
        // }
        pptr r = tmp->right[ht];
        if (r.id != (uint32_t)-1 && ((mL3ptr)r.addr)->key <= tst->key) {
            tmp = (mL3ptr)r.addr;  // go right
            continue;
        }
        ht--;
    }
    L3_search_reply tsr = (L3_search_reply){.key = tst->key, .addr = tmp->down, .result_key = tmp->key};
    push_task(L3_SEARCH, &tsr, sizeof(L3_search_reply));
    return tmp->key;
}

static inline void L3_sancheck() {
    int h = (int)root->height;
    for (int i = 0; i < h; i ++) {
        mL3ptr tmp = root;
        pptr r = tmp->right[i];
        while (r.id != (uint32_t)-1) {
            mL3ptr rn = (mL3ptr)r.addr;
            // if (!(rn->left[i].addr == (uint32_t)tmp) || rn->key == 57192124) {
            //     printf("%d %lld %lld %lld\n%lu-%x ", i, tmp->key, rn->key, ((mL3ptr)rn->left[i].addr)->key, DPU_ID, (uint32_t)tmp);
            //     print_pptr(rn->left[i], "\n");
            //     // assert(false);
            // }
            tmp = rn;
            r = tmp->right[i];
        }
    }
}