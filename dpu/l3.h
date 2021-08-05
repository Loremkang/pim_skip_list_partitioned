#pragma once

#include "common.h"
#include "task_dpu.h"
#include "storage.h"

BARRIER_INIT(L3_barrier, NR_TASKLETS);

extern uint64_t DPU_ID;
extern __mram_ptr ht_slot l3ht[]; 

static inline void L3_init(L3_insert_task *tit) {
    // assert(l3cnt == 8);
    IN_DPU_ASSERT(l3cnt == 8, "L3init: Wrong l3cnt\n");
    storage_init();
    uint32_t actual_size;
    root = get_new_L3(LLONG_MIN, tit->height, tit->addr, &actual_size);
    L3_insert_reply tir = (L3_insert_reply){.key = tit->key, .addr = (pptr){.id = DPU_ID, .addr = (uint32_t)root}};
    push_task(L3_INIT, &tir, sizeof(L3_insert_reply));
}

// void L3_insert(L3_insert_task *tit);

static inline void L3_insert_parallel(int length, int64_t *keys,
                                      int8_t *heights, pptr *addrs,
                                      int8_t *max_height_shared,
                                      mL3ptr **right_predecessor_shared,
                                      mL3ptr **right_newnode_shared) {
    uint32_t tasklet_id = me();
    int8_t max_height = 0;
    mL3ptr *newnode = mem_alloc(sizeof(mL3ptr) * length);
    for (int i = 0; i < length; i++) {
        uint32_t actual_size;
        newnode[i] = get_new_L3(keys[i], heights[i], addrs[i], &actual_size);
        if (heights[i] > max_height) {
            max_height = heights[i];
        }
    }

    mL3ptr *predecessor = mem_alloc(sizeof(mL3ptr) * max_height);
    mL3ptr *left_predecessor = mem_alloc(sizeof(mL3ptr) * max_height);
    mL3ptr *left_newnode = mem_alloc(sizeof(mL3ptr) * max_height);
    mL3ptr *right_predecessor = right_predecessor_shared[tasklet_id];
    mL3ptr *right_newnode = right_newnode_shared[tasklet_id];
    max_height_shared[tasklet_id] = max_height;

    IN_DPU_ASSERT(max_height <= root->height,
                  "L3insert: Wrong newnode height\n");

    // mL3ptr (*predecessor)[max_height] =
    // mem_alloc(sizeof(mL3ptr[2][max_height]));

    memset(left_nodeid, -1, sizeof(left_nodeid));
    {
        int i = 0;
        L3_search(keys[0], 0, heights[0], predecessor);
        for (int ht = 0; ht < heights[0]; ht++) {
            left_predecessor[ht] = right_predecessor[ht] = predecessor[ht];
            left_newnode[ht] = right_newnode[ht] = newnode[0];
        }
        max_height = heights[0];
    }

    for (int i = 1; i < length; i++) {
        L3_search(keys[i], 0, heights[i], predecessor);
        int minheight = (max_height < heights[i]) ? max_height : heights[i];
        for (int ht = 0; ht < minheight; ht++) {
            if (right_predecessor[ht] == predecessor[ht]) {
                right_newnode[ht]->right[ht] =
                    (pptr){.id = DPU_ID, .addr = (uint32_t)newnode[i]};
                newnode[i]->left[ht] =
                    (pptr){.id = DPU_ID, .addr = (uint32_t)right_newnode[ht]};
            } else {
                right_newnode[ht]->right[ht] = (pptr){
                    .id = DPU_ID,
                    .addr = (uint32_t)(right_predecessor[ht]->right[ht])};
                newnode[i]->left[ht] =
                    (pptr){.id = DPU_ID, .addr = (uint32_t)predecessor[ht]};
            }
        }
        for (int ht = 0; ht < heights[i]; ht++) {
            right_predecessor[ht] = predecessor[ht];
            right_newnode[ht] = newnode[i];
        }
        if (heights[i] > max_height) {
            for (int ht = max_height; ht < heights[i]; ht++) {
                left_predecessor[ht] = predecessor[ht];
                left_newnode[ht] = newnode[i];
            }
            max_height = heights[i];
        }
    }

    barrier_wait(&L3_barrier);

    // if(tasklet_id == (NR_TASKLETS - 1)) {
    //     for(int ht = 0; ht < max_height; ht ++){
    //         right_newnode[ht]->right[ht] = right_predecessor[ht]->right[ht];
    //         if (right_predecessor[ht]->right[ht].id != -1) {
    //             mL3ptr rn = right_predecessor[ht]->right[ht].addr;
    //             rn->left[ht] = (pptr){.id = DPU_ID, .addr = right_newnode[ht]};
    //         }
    //     }
    // }

    int max_height_r = 0;
    for (int r = tasklet_id + 1; r < NR_TASKLETS; r ++) {
        max_height_r = (max_height_shared[r] > max_height_r) ? max_height_shared[r] : max_height_r;
    }

    for (int ht = max_height_r; ht < max_height; ht ++) {
        right_newnode[ht]->right[ht] = right_predecessor[ht]->right[ht];
        if (right_predecessor[ht]->right[ht].id != -1) {
            mL3ptr rn = right_predecessor[ht]->right[ht].addr;
            rn->left[ht] = (pptr){.id = DPU_ID, .addr = right_newnode[ht]};
        }
    }

    for (int l = tasklet_id - 1, ht = 0; ht < max_height; ht++) {
        while (l >= 0 && ht >= max_height_shared[l]) {
            l --;
        }
        if (l >= 0 && ht < max_height_shared[l]) {
            mL3ptr *right_predecessor_l = right_predecessor_shared[l];
            mL3ptr *right_newnode_l = right_newnode_shared[l];
            if (right_predecessor_l[ht] == left_predecessor[ht]) {
                right_newnode_l[ht]->right[ht] =
                    (pptr){.id = DPU_ID, .addr = (uint32_t)left_newnode[ht]};
                left_newnode[ht]->left[ht] =
                    (pptr){.id = DPU_ID, .addr = (uint32_t)right_newnode_l[ht]};
            } else {
                // build l_newnode <-> right_successor
                right_newnode_l[ht]->right[ht] =
                    right_predecessor_l[ht]->right[ht];
                IN_DPU_ASSERT(right_predecessor_l[ht]->right[ht].id != -1);
                mL3ptr rn = right_predecessor[ht]->right[ht].addr;
                rn->left[ht] =
                    (pptr){.id = DPU_ID, .addr = right_newnode_l[ht]};

                // build left_successor <-> newnode
                left_newnode[ht]->left[ht] =
                    (pptr){.id = DPU_ID, .addr = left_predecessor[ht]};
                left_predecessor[ht]->right[ht] =
                    (pptr){.id = DPU_ID, .addr = left_newnode[ht]};
            }
        }
        if (l < 0) {
            // build left_successor <-> newnode
            left_newnode[ht]->left[ht] =
                (pptr){.id = DPU_ID, .addr = left_predecessor[ht]};
            left_predecessor[ht]->right[ht] =
                (pptr){.id = DPU_ID, .addr = left_newnode[ht]};
        }
    }
    for (int i = 0; i < length; i++) {
        L3_insert_reply tir = (L3_insert_reply){
            .key = tit->key,
            .addr = (pptr){.id = DPU_ID, .addr = (uint32_t)newnode[i]}};
        push_task(L3_INSERT, &tir, sizeof(L3_insert_reply));
    }
}

static inline void L3_insert(L3_insert_task *tit, mL3ptr *rightmost) {
    uint32_t actual_size;
    mL3ptr newnode = get_new_L3(tit->key, tit->height, tit->addr, &actual_size);
    if (rightmost != NULL) {
        for (int i = 0; i < tit->height; i++) {
            rightmost[i] = newnode;
        }
    }

    // assert(newnode->height <= root->height);
    printf("%lld %lld\n", newnode->height, root->height);
    IN_DPU_ASSERT(newnode->height <= root->height,
                  "L3insert: Wrong newnode height\n");

    // find search path
    mL3ptr tmp = root;
    int64_t ht = root->height - 1;
    while (ht >= 0) {
        pptr l = tmp->left[ht], r = tmp->right[ht];
        mL3ptr rn = (mL3ptr)r.addr;
        IN_DPU_ASSERT(l.id == DPU_ID || l.id == (uint32_t)-1,
                      "L3insert: wrong l.id\n");
        IN_DPU_ASSERT(r.id == DPU_ID || r.id == (uint32_t)-1,
                      "L3insert: wrong r.id\n");
        if (r.id != (uint32_t)-1 &&
            rn->key <= tit->key) {  // should not be equal
            tmp = rn;               // go right
            // assert(rn->key != tit->key);
            IN_DPU_ASSERT(rn->key != tit->key, "L3insert: replicated insert\n");
            continue;
        }
        if (ht < (newnode->height)) {  // insert to right
            newnode->right[ht] = r;
            newnode->left[ht] = (pptr){.id = DPU_ID, .addr = (uint32_t)tmp};
            if (r.id != (uint32_t)-1) {
                // assert(rn->left[ht].id == DPU_ID);
                IN_DPU_ASSERT(rn->left[ht].id == DPU_ID,
                              "L3insert: wrong right->left.id");
                // rn->left[ht].addr = (uint32_t)newnode;
                rn->left[ht] = (pptr){.id = DPU_ID, .addr = (uint32_t)newnode};
            }
            tmp->right[ht] = (pptr){.id = DPU_ID, .addr = (uint32_t)newnode};
        }
        ht--;
    }
    L3_insert_reply tir = (L3_insert_reply){
        .key = tit->key,
        .addr = (pptr){.id = DPU_ID, .addr = (uint32_t)newnode}};
    push_task(L3_INSERT, &tir, sizeof(L3_insert_reply));
}

static inline int64_t L3_search(int64_t key, int record_height_l,
                                int record_height_r, mL3ptr *rightmost) {
    mL3ptr tmp = root;
    int64_t ht = root->height - 1;
    while (ht >= 0) {
        pptr r = tmp->right[ht];
        if (r.id != (uint32_t)-1 && ((mL3ptr)r.addr)->key <= key) {
            tmp = (mL3ptr)r.addr;  // go right
            continue;
        }
        if (left != NULL && record_height_l <= ht && ht < record_height_r) {
            rightmost[ht] = tmp;
        }
        ht--;
    }
    L3_search_reply tsr = (L3_search_reply){
        .key = key, .addr = tmp->down, .result_key = tmp->key};
    push_task(L3_SEARCH, &tsr, sizeof(L3_search_reply));
    return tmp->key;
}

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

static inline void L3_remove_parallel(int length, int64_t *keys,
                                      int8_t *max_height_shared,
                                      mL3ptr **left_node_shared) {
    uint32_t tasklet_id = me();
    mL3ptr *nodes = mem_alloc(sizeof(mL3ptr) * length);
    int8_t *heights = mem_alloc(sizeof(int8_t) * length);
    int8_t max_height = 0;
    for (int i = 0; i < length; i++) {
        uint32_t htv = ht_search(trt->key, L3_ht_get);
        IN_DPU_ASSERT(htv != (uint32_t)-1, "L3remove: key not found\n");
        nodes[i] = htv;
        heights[i] = nodes[i]->height;
        max_height = (heights[i] > max_height) ? heights[i] : max_height;
    }
    mL3ptr *left_node = left_node_shared[tasklet_id];
    // mL3ptr *left_node = mem_alloc(sizeof(mL3ptr) * max_height);
    // mL3ptr *right_node = mem_alloc(sizeof(mL3ptr) * max_height);
    {
        max_height = 0;
        for (int i = 0; i < length; i++) {
            if (heights[i] > max_height) {
                for (int ht = max_height; ht < heights[i]; ht++) {
                    left_node[ht] = nodes[i];
                }
                max_height = heights[i];
            }
        }
    }

    //     max_height = 0;
    //     for (int i = length - 1; i > 0; i --) {
    //         if (heights[i] > max_height) {
    //             for (int ht = max_height; ht < heights[i]; ht ++) {
    //                 right_node[ht] = nodes[i];
    //             }
    //             max_height = heights[i];
    //         }
    //     }
    // }

    for (int i = 0; i < length; i++) {
        for (int ht = 0; ht < heights[i]; ht++) {
            if (nodes[i] == left_node[ht]) {
                continue;
            }
            mL3ptr ln = nodes[i]->left[ht].addr;
            ln->right[ht] = nodes[i]->right[ht];
            if (nodes[i]->right[ht].id != -1) {
                mL3ptr rn = nodes[i]->right[ht].addr;
                rn->left[ht] = nodes[i]->left[ht];
            }
        }
    }
    max_height_shared = max_height;

    barrier_wait(&L3_barrier);

    for (int l = tasklet_id - 1, ht = 0; ht < max_height; ht++) {
        while (l >= 0 && ht >= max_height_shared[l]) {
            l --;
        }
        if (l < 0 || (left_node[ht]->left[ht].addr != left_node_shared[l][ht])) { // left most node in the level
            int r = tasklet_id + 1;
            mL3ptr rn = left_node[ht];
            for (; r < NR_TASKLETS; r ++) {
                if (heights[r] <= ht) {
                    continue;
                }
                if (rn->right[ht].id == -1 || rn->right[ht].addr != left_node_shared[r][ht]) {
                    break;
                }
                rn = left_node_shared[r][ht];
            }
            IN_DPU_ASSERT(left_node[ht]->left[ht].id == DPU_ID);
            mL3ptr ln = (mL3ptr)(left_node[ht]->left[ht].addr);
            ln->right[ht] = rn->right[ht];
            if (rn->right[ht].id != (uint32_t)-1) {
                rn = (mL3ptr)rn->right[ht].addr;
                rn->left[ht] = left_node[ht]->left[ht];
            }
        } else { // not the left most node
            // do nothing
        }
    }
}

static inline void L3_remove(L3_remove_task *trt) {
    uint32_t htv = ht_search(trt->key, L3_ht_get);
    if (htv == (uint32_t)-1) {
        // assert(false); // not found;
        IN_DPU_ASSERT(false, "L3remove: key not found\n");
    }
    mL3ptr tmp = (mL3ptr)htv;

    // mL3ptr tmp = ht_get_L3(trt->key);
    // assert(tmp->key == trt->key);
    IN_DPU_ASSERT(tmp->key == trt->key, "L3remove: wrong key found\n");
    for (int ht = 0; ht < tmp->height; ht++) {
        pptr l = tmp->left[ht], r = tmp->right[ht];
        mL3ptr ln = (mL3ptr)l.addr, rn = (mL3ptr)r.addr;
        // assert(l.id != (uint32_t)-1);
        IN_DPU_ASSERT(l.id != (uint32_t)-1, "L3remove: left doesn't exist\n");
        if (r.id != (uint32_t)-1) {
            // assert(rn->left[ht].id == DPU_ID);
            IN_DPU_ASSERT(rn->left[ht].id == DPU_ID,
                          "L3remove: left.id illegal\n");
            rn->left[ht] = l;
        }
        ln->right[ht] = r;
        // tmp->left[ht] = null_pptr;
        // tmp->right[ht] = null_pptr;
    }
    // printf("\n\t%lld\n", trt->key);
    ht_delete(l3ht, hash_to_addr(trt->key, 0, LX_HASHTABLE_SIZE),
              (uint32_t)tmp);
}

static inline void L3_sancheck() {
    int h = (int)root->height;
    for (int i = 0; i < h; i++) {
        mL3ptr tmp = root;
        pptr r = tmp->right[i];
        while (r.id != (uint32_t)-1) {
            mL3ptr rn = (mL3ptr)r.addr;
            // if (!(rn->left[i].addr == (uint32_t)tmp) || rn->key ==
            // 57192124)
            // {
            //     printf("%d %lld %lld %lld\n%lu-%x ", i, tmp->key,
            //     rn->key,
            //     ((mL3ptr)rn->left[i].addr)->key, DPU_ID, (uint32_t)tmp);
            //     print_pptr(rn->left[i], "\n");
            //     // assert(false);
            // }
            tmp = rn;
            r = tmp->right[i];
        }
    }
}