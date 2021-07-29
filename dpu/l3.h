#pragma once

#include "common.h"
#include "task_dpu.h"
#include "storage.h"

extern uint64_t DPU_ID;

static inline void L3_init(L3_insert_task *tit) {
    assert(l3cnt == 8);
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
        assert(l.id == DPU_ID || l.id == (uint32_t)-1);
        assert(r.id == DPU_ID || r.id == (uint32_t)-1);
        if (r.addr != (uint32_t)-1 &&
            ((mL3ptr)r.addr)->key < tit->key) {  // should not be equal
            tmp = (mL3ptr)r.addr;                // go right
            continue;
        }
        if (ht < (newnode->height)) {  // insert to right
            newnode->right[ht] = r;
            newnode->left[ht] = (pptr){.id = DPU_ID, .addr = (uint32_t)tmp};
            if (r.id != (uint32_t)-1) {
                assert(((mL3ptr)r.addr)->left[ht].id == DPU_ID);
                ((mL3ptr)r.addr)->left[ht].addr = (uint32_t)newnode;
            }
            tmp->right[ht] = (pptr){.id = DPU_ID, .addr = (uint32_t)newnode};
        }
        ht--;
    }
    L3_insert_reply tir = (L3_insert_reply){.key = tit->key, .addr = (pptr){.id = DPU_ID, .addr = (uint32_t)newnode}};
    push_task(L3_INSERT, &tir, sizeof(L3_insert_reply));
}

static inline void L3_remove(L3_remove_task *trt) {
    mL3ptr tmp = ht_get_L3(trt->key);
    for (int ht = 0; ht < tmp->height; ht++) {
        pptr l = tmp->left[ht], r = tmp->right[ht];
        if (r.id != (uint32_t)-1) {
            assert(((mL3ptr)r.addr)->left[ht].id == DPU_ID);
            ((mL3ptr)r.addr)->left[ht] = l;
        }
        ((mL3ptr)l.addr)->right[ht] = r;
    }
}

static inline void L3_search(L3_search_task *tst) {
    mL3ptr tmp = root;
    int64_t ht = root->height - 1;
    while (ht >= 0) {
        pptr r = tmp->right[ht];
        if (r.id != (uint32_t)-1 && ((mL3ptr)r.addr)->key <= tst->key) {
            tmp = (mL3ptr)r.addr;  // go right
            continue;
        }
        ht--;
    }
    L3_search_reply tsr = (L3_search_reply){.key = tst->key, .addr = tmp->down, .result_key = tmp->key};
    push_task(L3_SEARCH, &tsr, sizeof(L3_search_reply));
}