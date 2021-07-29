#include <stdlib.h>
#include <assert.h>
#include "common.h"
#include "l3.h"

inline void L3_insert(L3_insert_task *tit) {
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
        int64_t rkey = ((mL3ptr)r.addr)->key;
        assert(rkey != tit->key);
        if (r.addr != (uint32_t)-1 &&
            rkey < tit->key) {  // should not be equal
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