#pragma once

#include "common.h"
#include "task_dpu.h"
#include "storage.h"
#include <barrier.h>
#include <alloc.h>
// #include <profiling.h>

// PROFILING_INIT(prof_newnode);
// PROFILING_INIT(prof_internal);
// PROFILING_INIT(prof_external);
// PROFILING_INIT(prof_finish);


// BARRIER_INIT(L3_barrier, NR_TASKLETS);

// extern volatile int host_barrier;
BARRIER_INIT(L3_barrier, NR_TASKLETS);
MUTEX_INIT(L3_lock);
// extern const mutex_id_t L3_lock;

// extern int NR_TASKLETS;

extern int64_t DPU_ID, dpu_epoch_number;
extern __mram_ptr uint8_t* send_task_start;
extern __mram_ptr ht_slot l3ht[]; 

static inline void L3_init(L3_init_task *tit) {
    IN_DPU_ASSERT(l3cnt == 8, "L3init: Wrong l3cnt\n");
    __mram_ptr void* maddr = reserve_space_L3(L3_node_size(tit->height));
    root = get_new_L3(LLONG_MIN, tit->height, tit->addr, maddr);
    L3_init_reply tir = (L3_init_reply){.addr = (pptr){.id = DPU_ID, .addr = (uint32_t)root}};
    mram_write(&tir, send_task_start, sizeof(L3_init_reply));
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

static inline void L3_get(int64_t key, int i) {
    uint32_t htv = ht_search(l3ht, key, L3_ht_get);
    // IN_DPU_ASSERT(htv != INVALID_DPU_ADDR, "L3remove: key not found\n");
    L3_get_reply tgr =
        (L3_get_reply){//.key = key,
                       //.addr = (pptr){.id = DPU_ID, .addr = htv},
                       .available = (htv != INVALID_DPU_ADDR) ? 1 : 0};
    __mram_ptr L3_get_reply* dst = (__mram_ptr L3_get_reply*)send_task_start;
    mram_write(&tgr, &dst[i], sizeof(L3_get_reply));
}

static inline int64_t L3_search(int64_t key, int i, int record_height,
                                mL3ptr *rightmost) {
    mL3ptr tmp = root;
    int64_t ht = root->height - 1;
    while (ht >= 0) {
        pptr r = tmp->right[ht];
        if (r.id != INVALID_DPU_ID && ((mL3ptr)r.addr)->key <= key) {
            tmp = (mL3ptr)r.addr;  // go right
            continue;
        }
        if (rightmost != NULL && ht < record_height) {
            rightmost[ht] = tmp;
        }
        ht--;
    }
    // IN_DPU_ASSERT(rightmost != NULL, "L3 search: rightmost error");
    if (rightmost == NULL) {  // pure search task
        L3_search_reply tsr = (L3_search_reply){//.key = key,
                                   //.addr = tmp->down,
                                   .result_key = tmp->key};
        __mram_ptr L3_search_reply* dst = (__mram_ptr L3_search_reply*)send_task_start;
        mram_write(&tsr, &dst[i], sizeof(L3_search_reply));
    }
    return tmp->key;
}

static inline void print_nodes(int length, mL3ptr *newnode, bool quit, bool lock) {
    uint32_t tasklet_id = me();
    if (lock) mutex_lock(L3_lock);
    printf("*** %d ***\n", tasklet_id);
    for (int i = 0; i < length; i++) {
        // printf("*%d %lld %x\n", i, newnode[i]->key, (uint32_t)newnode[i]);
        printf("*%d %lld %x\n", i, newnode[i]->key, (uint32_t)newnode[i]);
        for (int ht = 0; ht < newnode[i]->height; ht++) {
            printf("%x %x\n", newnode[i]->left[ht].addr,
                   newnode[i]->right[ht].addr);
        }
    }
    if (lock) mutex_unlock(L3_lock);
    if (quit) {
        EXIT();
    }
}

static inline void L3_insert_parallel(int length, int l, int64_t *keys,
                                      int8_t *heights, pptr *addrs,
                                      uint32_t *newnode_size,
                                      int8_t *max_height_shared,
                                      mL3ptr *right_predecessor_shared,
                                      mL3ptr *right_newnode_shared) {
    uint32_t tasklet_id = me();
    int8_t max_height = 0;
    mL3ptr *newnode = mem_alloc(sizeof(mL3ptr) * length);

    barrier_wait(&L3_barrier);

    if (tasklet_id == 0) {
        for (int i = 0; i < NR_TASKLETS; i++) {
            newnode_size[i] = (uint32_t)reserve_space_L3(newnode_size[i]);
        }
    }

    barrier_wait(&L3_barrier);

    __mram_ptr void* maddr = (__mram_ptr void*) newnode_size[tasklet_id];

    for (int i = 0; i < length; i++) {
        newnode[i] = get_new_L3(keys[i], heights[i], addrs[i], maddr);
        maddr += L3_node_size(heights[i]);
        if (heights[i] > max_height) {
            max_height = heights[i];
        }
    }

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
        // print_nodes(heights[0], predecessor, true);
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
    }

    for (int l = (int)tasklet_id - 1, ht = 0; ht < max_height; ht++) {
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

                left_newnode[ht]->left[ht] =
                    (pptr){.id = OLD_NODES_DPU_ID,
                           .addr = (uint32_t)left_predecessor[ht]};
            }
        }
        if (l < 0) {
            left_newnode[ht]->left[ht] = (pptr){
                .id = OLD_NODES_DPU_ID, .addr = (uint32_t)left_predecessor[ht]};
        }
    }

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

    L3_insert_reply *tir = mem_alloc(sizeof(L3_insert_reply) * length);
    for (int i = 0; i < length; i++) {
        tir[i] = (L3_insert_reply){
            .addr = (pptr){.id = DPU_ID, .addr = (uint32_t)newnode[i]}};
    }
    __mram_ptr L3_insert_reply *dst =
        (__mram_ptr L3_insert_reply *)send_task_start;
    mram_write(tir, &dst[l], sizeof(L3_insert_reply) * length);
}

static inline void L3_remove_parallel(int length, int64_t *keys,
                                      int8_t *max_height_shared,
                                      mL3ptr *left_node_shared) {
    uint32_t tasklet_id = me();
    mL3ptr *nodes = mem_alloc(sizeof(mL3ptr) * length);
    int8_t *heights = mem_alloc(sizeof(int8_t) * length);
    int8_t max_height = 0;
    for (int i = 0; i < length; i++) {
        uint32_t htv = ht_search(l3ht, keys[i], L3_ht_get);
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

    barrier_wait(&L3_barrier);

    for (int i = 0; i < length; i++) {
        if ((uint32_t)nodes[i] != INVALID_DPU_ADDR) {
            ht_delete(l3ht, &l3htcnt, hash_to_addr(keys[i], 0, LX_HASHTABLE_SIZE),
                      (uint32_t)nodes[i]);
        }
    }
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