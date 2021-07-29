#pragma once

#include "common.h"
#include "task_dpu.h"
#include "storage.h"

extern uint64_t DPU_ID;

inline void L3_init(L3_insert_task *tit) {
    assert(l3cnt == 8);
    uint32_t actual_size;
    root = get_new_L3(LLONG_MIN, tit->height, tit->addr, &actual_size);
    L3_insert_reply tir = (L3_insert_reply){.key = tit->key, .addr = (pptr){.id = DPU_ID, .addr = (uint32_t)root}};
    push_task(L3_INIT, &tir, sizeof(L3_insert_reply));
}

void L3_insert(L3_insert_task *tit);

inline void L3_remove(L3_remove_task *trt) {
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

inline void L3_search(L3_search_task *tst) {
    mL3ptr tmp = root;
    int64_t ht = root->height - 1;
    while (ht >= 0) {
        pptr r = tmp->right[ht];
        int64_t rkey = ((mL3ptr)r.addr)->key;
        if (r.id != (uint32_t)-1 && rkey <= tst->key) {
            tmp = (mL3ptr)r.addr;  // go right
            continue;
        }
        ht--;
    }
    L3_search_reply tsr = (L3_search_reply){.key = tst->key, .addr = tmp->down};
    push_task(L3_SEARCH, &tsr, sizeof(L3_search_reply));
}