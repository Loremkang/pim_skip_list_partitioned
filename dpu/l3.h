#pragma once

#include "common.h"
#include "task_framework_dpu.h"
// #include "storage.h"
#include "hashtable_l3size.h"
#include <barrier.h>
#include <alloc.h>
#include "gc.h"

// Range Scan
#include "data_block.h"

// #include <profiling.h>

// PROFILING_INIT(prof_newnode);
// PROFILING_INIT(prof_internal);
// PROFILING_INIT(prof_external);
// PROFILING_INIT(prof_finish);

BARRIER_INIT(L3_barrier, NR_TASKLETS);
MUTEX_INIT(L3_lock);

extern int64_t DPU_ID;
extern __mram_ptr ht_slot l3ht[]; 

static inline void L3_init(int64_t key, int64_t value, int height) {
    IN_DPU_ASSERT(l3cnt == 8, "L3init: Wrong l3cnt\n");
    __mram_ptr void* maddr = reserve_space_L3(L3_node_size(height));
    root = get_new_L3(key, value, height, maddr);
    gc_init();
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

static inline bool L3_get(int64_t key, int64_t* value) {
    uint32_t htv = ht_search(l3ht, key, L3_ht_get);
    // IN_DPU_ASSERT(htv != INVALID_DPU_ADDR, "L3remove: key not found\n");
    if (htv == INVALID_DPU_ADDR) {
        return false;
    }
    mL3ptr nn = (mL3ptr)htv;
    *value = nn->value;
    return true;
}

static inline bool L3_get_ml3ptr(int64_t key, mL3ptr* nn) {
    uint32_t htv = ht_search(l3ht, key, L3_ht_get);
    // IN_DPU_ASSERT(htv != INVALID_DPU_ADDR, "L3remove: key not found\n");
    if (htv == INVALID_DPU_ADDR) {
        return false;
    }
    *nn = (mL3ptr)htv;
    return true;
}

static inline int64_t L3_search(int64_t key, int record_height,
                                mL3ptr *rightmost, int64_t *value) {
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
    if (value != NULL) {
        *value = tmp->value;
    }
    return tmp->key;
}

static inline void print_nodes(int length, mL3ptr *newnode, bool quit,
                               bool lock) {
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

#define L3_TEMP_BUFFER_SIZE (500000)
mpint64_t newnode_buffer;
mpint64_t height_buffer;
// __mram_noinit int64_t newnode_buffer[L3_TEMP_BUFFER_SIZE];
// __mram_noinit int64_t height_buffer[L3_TEMP_BUFFER_SIZE];

static inline void L3_insert_parallel(int length, int l,
                                      __mram_ptr L3_insert_task *mram_tit,
                                      int8_t *max_height_shared,
                                      mL3ptr *right_predecessor_shared,
                                      mL3ptr *right_newnode_shared) {
    const uint32_t OLD_NODES_DPU_ID = (uint32_t)-2;
    uint32_t tasklet_id = me();
    int8_t max_height = 0;
    __mram_ptr int64_t *newnode = &newnode_buffer[l];
    IN_DPU_ASSERT(l + length < L3_TEMP_BUFFER_SIZE,
                  "L3 insert parallel: new node buffer overflow");
    // mL3ptr *newnode = mem_alloc(sizeof(mL3ptr) * length);

    barrier_wait(&L3_barrier);

    mutex_lock(L3_lock);
    for (int i = 0; i < length; i ++) {
        int64_t height = mram_tit[i].height;
        pptr recycle = alloc_node((int)height, 1);
        if (recycle.id == 0) { // fail to get from gc
            newnode[i] = (int64_t)reserve_space_L3(L3_node_size((int)height));
        } else {
            // assert(false);
            IN_DPU_ASSERT(recycle.id == 1, "invalid recycle.id");
            newnode[i] = (int64_t)recycle.addr;
        }
    }
    mutex_unlock(L3_lock);

    // if (tasklet_id == 0) {
    //     for (int i = 0; i < NR_TASKLETS; i++) {
    //         newnode_size[i] = (uint32_t)reserve_space_L3(newnode_size[i]);
    //     }
    //     IN_DPU_ASSERT(l3cnt < LX_BUFFER_SIZE, "L3 buffer overflow\n");
    // }

    // barrier_wait(&L3_barrier);

    // EXIT();
    // __mram_ptr void *maddr = (__mram_ptr void *)newnode_size[tasklet_id];

    for (int i = 0; i < length; i++) {
        int64_t key = mram_tit[i].key;
        int64_t height = mram_tit[i].height;
        int64_t value = mram_tit[i].value;
        // pptr addr = mram_tit[i].addr;
        // newnode[i] = (int64_t)get_new_L3(key, value, height, maddr);
        newnode[i] = (int64_t)get_new_L3(key, value, height, (__mram_ptr void *)newnode[i]);
        // maddr += L3_node_size(height);
        if (height > max_height) {
            max_height = height;
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
        int height = mram_tit[i].height;
        int64_t key = mram_tit[i].key;
        int64_t result = L3_search(key, height, predecessor, NULL);
        IN_DPU_ASSERT(result != key, "duplicated key");
        for (int ht = 0; ht < height; ht++) {
            mL3ptr nn = (mL3ptr)newnode[i];
            left_predecessor[ht] = right_predecessor[ht] = predecessor[ht];
            left_newnode[ht] = right_newnode[ht] = nn;
        }
        max_height = height;
        // print_nodes(heights[0], predecessor, true);
    }

    for (int i = 1; i < length; i++) {
        int height = mram_tit[i].height;
        int64_t key = mram_tit[i].key;
        int64_t result = L3_search(key, height, predecessor, NULL);
        mL3ptr nn = (mL3ptr)newnode[i];

        IN_DPU_ASSERT(result != key, "duplicated key");
        int minheight = (max_height < height) ? max_height : height;
        for (int ht = 0; ht < minheight; ht++) {
            if (right_predecessor[ht] == predecessor[ht]) {
                right_newnode[ht]->right[ht] =
                    (pptr){.id = DPU_ID, .addr = (uint32_t)nn};
                nn->left[ht] =
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

                nn->left[ht] = (pptr){.id = OLD_NODES_DPU_ID,
                                      .addr = (uint32_t)predecessor[ht]};
                // predecessor[ht]->right[ht] =
                //     (pptr){.id = DPU_ID, .addr = (uint32_t)newnode[i]};
            }
        }
        for (int ht = 0; ht < height; ht++) {
            right_predecessor[ht] = predecessor[ht];
            right_newnode[ht] = nn;
        }
        if (height > max_height) {
            for (int ht = max_height; ht < height; ht++) {
                left_predecessor[ht] = predecessor[ht];
                left_newnode[ht] = nn;
            }
            max_height = height;
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
                // if (right_predecessor_l[ht]->right[ht].id == INVALID_DPU_ID)
                // {
                //     // printf("%x\n",
                //     right_predecessor_l[ht]->right[ht].addr);
                //     // printf("%d %d\n", tasklet_id, l);
                //     // printf("%x %x\n", right_predecessor_l[ht],
                //     left_predecessor[ht]);
                //     // print_pptr(right_predecessor_l[ht]->right[ht], "\n");
                //     // print_pptr(left_predecessor[ht]->right[ht], "\n");
                //     for (int i = 0; i < NR_TASKLETS; i ++) {
                //         printf("%d %d\n", i, max_height_shared[i]);
                //     }
                // }
                IN_DPU_ASSERT(
                    right_predecessor_l[ht]->right[ht].id != INVALID_DPU_ID,
                    "L3 insert parallel: build l_newnode <-> right_successor "
                    "id error");
                IN_DPU_ASSERT(
                    right_predecessor_l[ht]->right[ht].addr != INVALID_DPU_ADDR,
                    "L3 insert parallel: build l_newnode <-> right_successor "
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
        int height = mram_tit[i].height;
        mL3ptr nn = (mL3ptr)newnode[i];
        for (int ht = 0; ht < height; ht++) {
            if (nn->left[ht].id == OLD_NODES_DPU_ID) {
                mL3ptr ln = (mL3ptr)nn->left[ht].addr;
                nn->left[ht] = (pptr){.id = DPU_ID, .addr = (uint32_t)ln};
                ln->right[ht] = (pptr){.id = DPU_ID, .addr = (uint32_t)nn};
            }
            if (nn->right[ht].id == OLD_NODES_DPU_ID) {
                mL3ptr rn = (mL3ptr)nn->right[ht].addr;
                nn->right[ht] = (pptr){.id = DPU_ID, .addr = (uint32_t)rn};
                rn->left[ht] = (pptr){.id = DPU_ID, .addr = (uint32_t)nn};
            }
        }
    }

    // __mram_ptr L3_insert_reply *dst =
    //     (__mram_ptr L3_insert_reply *)send_task_start;
    // for (int i = 0; i < length; i++) {
    //     L3_insert_reply tir = (L3_insert_reply){
    //         .addr = (pptr){.id = DPU_ID, .addr = (uint32_t)newnode[i]}};
    //     mram_write(&tir, &dst[l + i], sizeof(L3_insert_reply));
    // }

    // L3_insert_reply *tir = mem_alloc(sizeof(L3_insert_reply) * length);
    // for (int i = 0; i < length; i++) {
    //     tir[i] = (L3_insert_reply){
    //         .addr = (pptr){.id = DPU_ID, .addr = (uint32_t)newnode[i]}};
    // }
    // mram_write(tir, &dst[l], sizeof(L3_insert_reply) * length);
}

static inline void L3_remove_parallel(int length, int l,
                                      __mram_ptr L3_remove_task *mram_trt,
                                      int8_t *max_height_shared,
                                      mL3ptr *left_node_shared) {
    uint32_t tasklet_id = me();

    __mram_ptr int64_t *nodes = &newnode_buffer[l];
    __mram_ptr int64_t *heights = &height_buffer[l];
    IN_DPU_ASSERT(l + length < L3_TEMP_BUFFER_SIZE,
                  "L3 remove parallel: new node buffer overflow");

    // mL3ptr *nodes = mem_alloc(sizeof(mL3ptr) * length);
    // int8_t *heights = mem_alloc(sizeof(int8_t) * length);

    int8_t max_height = 0;
    for (int i = 0; i < length; i++) {
        int64_t key = mram_trt[i].key;
        uint32_t htv = ht_search(l3ht, key, L3_ht_get);
        // IN_DPU_ASSERT(htv != INVALID_DPU_ADDR, "L3remove: key not found\n");
        mL3ptr nn = (mL3ptr)htv;
        nodes[i] = (int64_t)nn;
        if (htv == INVALID_DPU_ADDR) {  // not found
            heights[i] = 0;
        } else {
            heights[i] = nn->height;
        }
        // max_height = (heights[i] > max_height) ? heights[i] : max_height;
    }
    mL3ptr *left_node = left_node_shared + tasklet_id * MAX_L3_HEIGHT;

    max_height = 0;
    for (int i = 0; i < length; i++) {
        mL3ptr nn = (mL3ptr)nodes[i];
        int min_height = (heights[i] < max_height) ? heights[i] : max_height;
        for (int ht = 0; ht < min_height; ht++) {
            mL3ptr ln = (mL3ptr)(nn->left[ht].addr);
            ln->right[ht] = nn->right[ht];
            if (nn->right[ht].id != INVALID_DPU_ID) {
                mL3ptr rn = (mL3ptr)(nn->right[ht].addr);
                rn->left[ht] = nn->left[ht];
            }
            nn->left[ht] = nn->right[ht] = null_pptr;
        }
        if (heights[i] > max_height) {
            for (int ht = max_height; ht < heights[i]; ht++) {
                left_node[ht] = nn;
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
            int64_t key = mram_trt[i].key;
            ht_delete(l3ht, &l3htcnt, hash_to_addr(key, LX_HASHTABLE_SIZE),
                      (uint32_t)nodes[i]);
        }
    }
    mutex_lock(L3_lock);
    for (int i = 0; i < length; i ++) {
        if ((uint32_t)nodes[i] != INVALID_DPU_ADDR) {
            free_node((mL3ptr)nodes[i], heights[i]);
        }
    }
    mutex_unlock(L3_lock);
}

static inline void L3_sancheck() {
    int h = (int)root->height;
    for (int i = 0; i < h; i++) {
        mL3ptr tmp = root;
        // printf("%d %lld\n", i, tmp->key);
        pptr r = tmp->right[i];
        while (r.id != INVALID_DPU_ID) {
            mL3ptr rn = (mL3ptr)r.addr;
            if (i == 0) {
                printf("%lld %d-%x %d-%x\n", rn->key, tmp->right[i].id,
                       tmp->right[i].addr, rn->left[i].id, rn->left[i].addr);
            }
            IN_DPU_ASSERT(
                rn->left[i].id == DPU_ID && rn->left[i].addr == (uint32_t)tmp,
                "Sancheck fail\n");
            tmp = rn;
            // printf("%d %lld\n", i, tmp->key);
            r = tmp->right[i];
        }
    }
}

// Range Scan
static inline int64_t L3_scan(int64_t lkey, int64_t rkey,
                              varlen_buffer *key_buf, varlen_buffer *val_buf) {
    varlen_buffer_reset(key_buf);
    varlen_buffer_reset(val_buf);
    int64_t num = 0;
    mL3ptr tmp = root;
    int64_t ht = root->height - 1;
    while (ht >= 0) {
        pptr r = tmp->right[ht];
        if (r.id != INVALID_DPU_ID && ((mL3ptr)r.addr)->key <= lkey) {
            tmp = (mL3ptr)r.addr;  // go right
            continue;
        }
        ht--;
    }
    if(tmp->key >= lkey && tmp->key <= rkey) {
        num = 1;
        varlen_buffer_push(key_buf, tmp->key);
        varlen_buffer_push(val_buf, tmp->value);
    }
    ht = 1;
    while(ht == 1) {
        pptr r = tmp->right[0];
        if (r.id != INVALID_DPU_ID && ((mL3ptr)r.addr)->key <= rkey) {
            tmp = (mL3ptr)r.addr;  // go right
            num++;
            varlen_buffer_push(key_buf, tmp->key);
            varlen_buffer_push(val_buf, tmp->value);
            continue;
        }
        ht = 0;
    }
    return num;
}
