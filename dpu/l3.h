#pragma once

#include "common.h"
#include "task_dpu.h"
#include "storage.h"
#include <barrier.h>
#include <alloc.h>

// BARRIER_INIT(L3_barrier, NR_TASKLETS);

// extern volatile int host_barrier;
BARRIER_INIT(L3_barrier, NR_TASKLETS);
MUTEX_INIT(L3_lock);
// extern const mutex_id_t L3_lock;

// extern int NR_TASKLETS;

extern uint64_t DPU_ID, DPU_EPOCH_NUMBER;
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

static inline void L3_get(int64_t key) {
    uint32_t htv = ht_search(key, L3_ht_get);
    // IN_DPU_ASSERT(htv != INVALID_DPU_ADDR, "L3remove: key not found\n");
    L3_get_reply tgr =
        (L3_get_reply){.key = key,
                       .addr = (pptr){.id = DPU_ID, .addr = htv},
                       .available = (htv != INVALID_DPU_ADDR) ? 1 : 0};
    push_task(L3_GET, &tgr, sizeof(L3_get_reply));
}

static inline int64_t L3_search(int64_t key, int record_height_l,
                                int record_height_r, mL3ptr *rightmost) {
    mL3ptr tmp = root;
    int64_t ht = root->height - 1;
    while (ht >= 0) {
        pptr r = tmp->right[ht];
        if (r.id != INVALID_DPU_ID && ((mL3ptr)r.addr)->key <= key) {
            tmp = (mL3ptr)r.addr;  // go right
            continue;
        }
        if (rightmost != NULL && record_height_l <= ht &&
            ht < record_height_r) {
            rightmost[ht] = tmp;
        }
        ht--;
    }
    // IN_DPU_ASSERT(rightmost != NULL, "L3 search: rightmost error");
    if (rightmost == NULL) {  // pure search task
        L3_search_reply tsr = (L3_search_reply){
            .key = key, .addr = tmp->down, .result_key = tmp->key};
        push_task(L3_SEARCH, &tsr, sizeof(L3_search_reply));
    }
    return tmp->key;
}

static inline void print_nodes(int length, mL3ptr *newnode, bool quit) {
    uint32_t tasklet_id = me();
    mutex_lock(L3_lock);
    printf("*** %d ***\n", tasklet_id);
    for (int i = 0; i < length; i++) {
        // printf("*%d %lld %x\n", i, newnode[i]->key, (uint32_t)newnode[i]);
        printf("*%d %lld %x\n", i, newnode[i]->key, (uint32_t)newnode[i]);
        for (int ht = 0; ht < newnode[i]->height; ht++) {
            printf("%x %x\n", newnode[i]->left[ht].addr,
                   newnode[i]->right[ht].addr);
        }
    }
    mutex_unlock(L3_lock);
    if (quit) {
        EXIT();
    }
}

static inline void L3_insert_parallel(int length, int64_t *keys,
                                      int8_t *heights, pptr *addrs,
                                      int8_t *max_height_shared,
                                      mL3ptr *right_predecessor_shared,
                                      mL3ptr *right_newnode_shared) {
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
    // if (DPU_EPOCH_NUMBER == 3) {
    // EXIT();
    // }

    // mutex_lock(L3_lock);
    mL3ptr *predecessor = mem_alloc(sizeof(mL3ptr) * max_height);
    mL3ptr *left_predecessor = mem_alloc(sizeof(mL3ptr) * max_height);
    mL3ptr *left_newnode = mem_alloc(sizeof(mL3ptr) * max_height);
    // mutex_unlock(L3_lock);

    mL3ptr *right_predecessor =
        right_predecessor_shared + tasklet_id * MAX_L3_HEIGHT;
    mL3ptr *right_newnode = right_newnode_shared + tasklet_id * MAX_L3_HEIGHT;
    max_height_shared[tasklet_id] = max_height;

    IN_DPU_ASSERT(max_height <= root->height,
                  "L3insert: Wrong newnode height\n");

    if (length > 0) {
        int i = 0;
        L3_search(keys[i], 0, heights[i], predecessor);
        for (int ht = 0; ht < heights[i]; ht++) {
            left_predecessor[ht] = right_predecessor[ht] = predecessor[ht];
            left_newnode[ht] = right_newnode[ht] = newnode[i];
        }
        max_height = heights[i];
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
                // IN_DPU_ASSERT(false, "L3 insert parallel : P1 ERROR");
                right_newnode[ht]->right[ht] =
                    (pptr){.id = OLD_NODES_DPU_ID,
                           .addr = right_predecessor[ht]->right[ht].addr};
                IN_DPU_ASSERT(
                    right_predecessor[ht]->right[ht].id != INVALID_DPU_ID,
                    "L3 insert parallel: Wrong rp->right");
                // mL3ptr rn = (mL3ptr)right_predecessor[ht]->right[ht].addr;
                // rn->left[ht] =
                //     (pptr){.id = DPU_ID, .addr =
                //     (uint32_t)right_newnode[ht]};

                newnode[i]->left[ht] = (pptr){
                    .id = OLD_NODES_DPU_ID, .addr = (uint32_t)predecessor[ht]};
                // predecessor[ht]->right[ht] =
                //     (pptr){.id = DPU_ID, .addr = (uint32_t)newnode[i]};
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
    // barrier_wait(&L3_barrier);
    // if (DPU_EPOCH_NUMBER == 2) {
    //     print_nodes(length, newnode);
    // }

    barrier_wait(&L3_barrier);

    int max_height_r = 0;
    for (int r = tasklet_id + 1; r < NR_TASKLETS; r++) {
        max_height_r = (max_height_shared[r] > max_height_r)
                           ? max_height_shared[r]
                           : max_height_r;
    }
    for (int ht = max_height_r; ht < max_height; ht++) {
        // right_newnode[ht]->right[ht] = right_predecessor[ht]->right[ht];
        if (right_predecessor[ht]->right[ht].id != INVALID_DPU_ID) {
            right_newnode[ht]->right[ht] =
                (pptr){.id = OLD_NODES_DPU_ID,
                       .addr = right_predecessor[ht]->right[ht].addr};
        } else {
            right_newnode[ht]->right[ht] = null_pptr;
        }
        // if (right_predecessor[ht]->right[ht].id != INVALID_DPU_ID) {
        //     mL3ptr rn = (mL3ptr)right_predecessor[ht]->right[ht].addr;
        //     rn->left[ht] =
        //         (pptr){.id = DPU_ID, .addr = (uint32_t)right_newnode[ht]};
        // }
    }
    // barrier_wait(&L3_barrier);
    // if (DPU_EPOCH_NUMBER == 1) {
    //     print_nodes(length, newnode, false);
    // }

    // mutex_lock(L3_lock);
    // printf("*** %d *** %d root=%x\n", tasklet_id,
    // max_height_shared[tasklet_id], root); for (int i = 0; i < length; i++) {
    //     // printf("*%d %lld %x\n", i, newnode[i]->key, (uint32_t)newnode[i]);
    //     printf("*%d %lld %x\n", i, newnode[i]->key, (uint32_t)newnode[i]);
    //     for (int ht = 0; ht < newnode[i]->height; ht++) {
    //         printf("%x %x\n", newnode[i]->left[ht].addr,
    //                newnode[i]->right[ht].addr);
    //     }
    // }

    // printf(" *** right *** \n");
    // for (int i = 0; i < max_height; i++) {
    //     printf("%x %x\n", right_newnode[i], right_predecessor[i]);
    // }

    // printf(" *** left %d *** \n", tasklet_id);
    // for (int i = 0; i < max_height; i++) {
    //     printf("%x %x\n", left_newnode[i], left_predecessor[i]);
    // }

    // mutex_unlock(L3_lock);
    // EXIT();

    for (int l = (int)tasklet_id - 1, ht = 0; ht < max_height; ht++) {
        // printf("%u %d %d\n", tasklet_id, ht, l);
        while (l >= 0 && ht >= max_height_shared[l]) {
            l--;
            IN_DPU_ASSERT(l >= -1 && l <= NR_TASKLETS, "L3 insert: l overflow");
        }
        if (l >= 0 && ht < max_height_shared[l]) {
            mL3ptr *right_predecessor_l =
                right_predecessor_shared + l * MAX_L3_HEIGHT;
            mL3ptr *right_newnode_l = right_newnode_shared + l * MAX_L3_HEIGHT;
            if (right_predecessor_l[ht] == left_predecessor[ht]) {
                right_newnode_l[ht]->right[ht] =
                    (pptr){.id = DPU_ID, .addr = (uint32_t)left_newnode[ht]};
                left_newnode[ht]->left[ht] =
                    (pptr){.id = DPU_ID, .addr = (uint32_t)right_newnode_l[ht]};
            } else {
                // IN_DPU_ASSERT(false, "ERROR!");
                // build l_newnode <-> right_successor
                // right_newnode_l[ht]->right[ht] =
                // right_predecessor_l[ht]->right[ht];
                IN_DPU_ASSERT(
                    right_predecessor_l[ht]->right[ht].id != INVALID_DPU_ID,
                    "L3insertparallel: build l_newnode <-> right_successor "
                    "id error");
                IN_DPU_ASSERT(
                    right_predecessor_l[ht]->right[ht].addr != INVALID_DPU_ADDR,
                    "L3insertparallel: build l_newnode <-> right_successor "
                    "addr error");
                right_newnode_l[ht]->right[ht] =
                    (pptr){.id = OLD_NODES_DPU_ID,
                           .addr = right_predecessor_l[ht]->right[ht].addr};

                // mL3ptr rn =
                // (mL3ptr)right_predecessor_l[ht]->right[ht].addr;
                // rn->left[ht] =
                //     (pptr){.id = DPU_ID, .addr =
                //     (uint32_t)right_newnode_l[ht]};

                // build left_successor <-> newnode
                left_newnode[ht]->left[ht] =
                    (pptr){.id = OLD_NODES_DPU_ID,
                           .addr = (uint32_t)left_predecessor[ht]};
                // left_predecessor[ht]->right[ht] =
                //     (pptr){.id = DPU_ID, .addr = (uint32_t)left_newnode[ht]};
            }
        }
        if (l < 0) {
            // build left_successor <-> newnode
            left_newnode[ht]->left[ht] = (pptr){
                .id = OLD_NODES_DPU_ID, .addr = (uint32_t)left_predecessor[ht]};
            // left_predecessor[ht]->right[ht] =
            //     (pptr){.id = DPU_ID, .addr = (uint32_t)left_newnode[ht]};
        }
    }
    // barrier_wait(&L3_barrier);
    // if (DPU_EPOCH_NUMBER == 1) {
    //     print_nodes(length, newnode, false);
    // }

    barrier_wait(&L3_barrier);

    for (int i = 0; i < length; i++) {
        for (int ht = 0; ht < heights[i]; ht++) {
            if (newnode[i]->left[ht].id == OLD_NODES_DPU_ID) {
                mL3ptr ln = (mL3ptr)newnode[i]->left[ht].addr;
                newnode[i]->left[ht] =
                    (pptr){.id = DPU_ID, .addr = (uint32_t)ln};
                ln->right[ht] =
                    (pptr){.id = DPU_ID, .addr = (uint32_t)newnode[i]};
            }
            if (newnode[i]->right[ht].id == OLD_NODES_DPU_ID) {
                mL3ptr rn = (mL3ptr)newnode[i]->right[ht].addr;
                newnode[i]->right[ht] =
                    (pptr){.id = DPU_ID, .addr = (uint32_t)rn};
                rn->left[ht] =
                    (pptr){.id = DPU_ID, .addr = (uint32_t)newnode[i]};
            }
        }
    }

    for (int i = 0; i < length; i++) {
        L3_insert_reply tir = (L3_insert_reply){
            .key = keys[i],
            .addr = (pptr){.id = DPU_ID, .addr = (uint32_t)newnode[i]}};
        // printf("%d %lld\n", i, keys[i]);
        push_task(L3_INSERT, &tir, sizeof(L3_insert_reply));
    }
    // IN_DPU_ASSERT(false, "\n");
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
        IN_DPU_ASSERT(l.id == DPU_ID || l.id == INVALID_DPU_ID,
                      "L3insert: wrong l.id\n");
        IN_DPU_ASSERT(r.id == DPU_ID || r.id == INVALID_DPU_ID,
                      "L3insert: wrong r.id\n");
        if (r.id != INVALID_DPU_ID &&
            rn->key <= tit->key) {  // should not be equal
            tmp = rn;               // go right
            // assert(rn->key != tit->key);
            IN_DPU_ASSERT(rn->key != tit->key, "L3insert: replicated insert\n");
            continue;
        }
        if (ht < (newnode->height)) {  // insert to right
            newnode->right[ht] = r;
            newnode->left[ht] = (pptr){.id = DPU_ID, .addr = (uint32_t)tmp};
            if (r.id != INVALID_DPU_ID) {
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

static inline void L3_remove_parallel(int length, int64_t *keys,
                                      int8_t *max_height_shared,
                                      mL3ptr *left_node_shared) {
    uint32_t tasklet_id = me();
    mL3ptr *nodes = mem_alloc(sizeof(mL3ptr) * length);
    int8_t *heights = mem_alloc(sizeof(int8_t) * length);
    int8_t max_height = 0;
    for (int i = 0; i < length; i++) {
        uint32_t htv = ht_search(keys[i], L3_ht_get);
        // IN_DPU_ASSERT(htv != INVALID_DPU_ADDR, "L3remove: key not found\n");
        nodes[i] = (mL3ptr)htv;
        if (htv == INVALID_DPU_ADDR) {  // not found
            heights[i] = 0;
        } else {
            heights[i] = nodes[i]->height;
        }
        // max_height = (heights[i] > max_height) ? heights[i] : max_height;
    }
    mL3ptr *left_node = left_node_shared + tasklet_id * MAX_L3_HEIGHT;

    max_height = 0;
    for (int i = 0; i < length; i++) {
        int min_height = (heights[i] < max_height) ? heights[i] : max_height;
        for (int ht = 0; ht < min_height; ht++) {
            mL3ptr ln = (mL3ptr)(nodes[i]->left[ht].addr);
            ln->right[ht] = nodes[i]->right[ht];
            if (nodes[i]->right[ht].id != INVALID_DPU_ID) {
                mL3ptr rn = (mL3ptr)(nodes[i]->right[ht].addr);
                rn->left[ht] = nodes[i]->left[ht];
            }
            nodes[i]->left[ht] = nodes[i]->right[ht] = null_pptr;
        }
        if (heights[i] > max_height) {
            for (int ht = max_height; ht < heights[i]; ht++) {
                left_node[ht] = nodes[i];
            }
            max_height = heights[i];
        }
    }

    max_height_shared[tasklet_id] = max_height;

    barrier_wait(&L3_barrier);

    for (int l = (int)tasklet_id - 1, ht = 0; ht < max_height; ht++) {
        while (l >= 0 && ht >= max_height_shared[l]) {
            l--;
        }
        mL3ptr *left_node_l = left_node_shared + l * MAX_L3_HEIGHT;
        if (l < 0 || ((mL3ptr)left_node[ht]->left[ht].addr !=
                      left_node_l[ht])) {  // left most node in the level
            int r = tasklet_id + 1;
            mL3ptr rn = left_node[ht];
            for (; r < NR_TASKLETS; r++) {
                if (max_height_shared[r] <= ht) {
                    continue;
                }
                mL3ptr *left_node_r = left_node_shared + r * MAX_L3_HEIGHT;
                if (rn->right[ht].id == INVALID_DPU_ID ||
                    (mL3ptr)rn->right[ht].addr != left_node_r[ht]) {
                    break;
                }
                rn = left_node_r[ht];
            }
            IN_DPU_ASSERT(left_node[ht]->left[ht].id == DPU_ID,
                          "L3 remove parallel: wrong leftid\n");
            mL3ptr ln = (mL3ptr)(left_node[ht]->left[ht].addr);
            ln->right[ht] = rn->right[ht];
            if (rn->right[ht].id != INVALID_DPU_ID) {
                rn = (mL3ptr)rn->right[ht].addr;
                rn->left[ht] = left_node[ht]->left[ht];
            }
        } else {  // not the left most node
            IN_DPU_ASSERT(
                ((mL3ptr)left_node_l[ht]->right[ht].addr == left_node[ht]),
                "L3 remove parallel: wrong skip");
            // do nothing
        }
    }
}

static inline void L3_remove(L3_remove_task *trt) {
    uint32_t htv = ht_search(trt->key, L3_ht_get);
    if (htv == INVALID_DPU_ADDR) {
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
        // assert(l.id != INVALID_DPU_ID);
        IN_DPU_ASSERT(l.id != INVALID_DPU_ID, "L3remove: left doesn't exist\n");
        if (r.id != INVALID_DPU_ID) {
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
        // printf("%d %lld\n", i, tmp->key);
        pptr r = tmp->right[i];
        while (r.id != INVALID_DPU_ID) {
            mL3ptr rn = (mL3ptr)r.addr;
            // printf("%lld %d-%x %d-%x\n", rn->key, tmp->right[i].id,
            //        tmp->right[i].addr, rn->left[i].id, rn->left[i].addr);
            IN_DPU_ASSERT(
                rn->left[i].id == DPU_ID && rn->left[i].addr == (uint32_t)tmp,
                "Sancheck fail\n");
            tmp = rn;
            // printf("%d %lld\n", i, tmp->key);
            r = tmp->right[i];
        }
    }
}