#pragma once

#include "common.h"
#include "task_dpu.h"
#include "storage.h"
#include <barrier.h>
#include <alloc.h>


// extern volatile int host_barrier;
BARRIER_INIT(L2_barrier, NR_TASKLETS);
MUTEX_INIT(L2_lock);

// extern int NR_TASKLETS;

extern int64_t DPU_ID, dpu_epoch_number;
extern __mram_ptr uint8_t* send_buffer;
extern __mram_ptr uint8_t* send_task_start;
extern __mram_ptr int64_t* send_task_count;
extern __mram_ptr int64_t* send_size;

extern __mram_ptr ht_slot l2ht[]; 


static inline void L2_init(L2_init_task *sit) {
    IN_DPU_ASSERT(l2cnt == 8, "L2init: Wrong l2cnt\n");
    __mram_ptr void* maddr = reserve_space_L2(L2_node_size(sit->height));
    mL2ptr ret = get_new_L2(LLONG_MIN, sit->height, sit->addr, maddr);
    L2_init_reply sir = (L2_init_reply){.addr = (pptr){.id = DPU_ID, .addr = (uint32_t)ret}};
    mram_write(&sir, send_task_start, sizeof(L2_init_reply));
}

static inline int L2_ht_get(ht_slot v, int64_t key) {
    if (v.v == 0) {
        return -1;
    }
    mL2ptr np = (mL2ptr)v.v;
    if (np->key == key) {
        return 1;
    }
    return 0;
}

static inline void L2_get(int i, int64_t key) {
    uint32_t htv = ht_search(l2ht, key, L2_ht_get);
    // IN_DPU_ASSERT(htv != INVALID_DPU_ADDR, "L2remove: key not found\n");
    L2_get_reply sgr =
        (L2_get_reply){//.key = key,
                       //.addr = (pptr){.id = DPU_ID, .addr = htv},
                       .available = (htv != INVALID_DPU_ADDR) ? 1 : 0};
    __mram_ptr L2_get_reply* dst = (__mram_ptr L2_get_reply*)send_task_start;
    mram_write(&sgr, &dst[i], sizeof(L2_get_reply));
}

static inline void L2_get_node(int i, pptr addr, int height) {
    IN_DPU_ASSERT(addr.id == DPU_ID, "L2 get node: wrong addr.id");
    mL2ptr nn = (mL2ptr)addr.addr;
    if (!(height >= -1 && height < nn->height)) {
        printf("** %lld %d %lld %d-%x\n", nn->key, height, nn->height, addr.id, addr.addr);
    }
    IN_DPU_ASSERT(height >= -1 && height < nn->height, "L2 get node: wrong height");
    L2_get_node_reply sgnr;
    if (height == -1) {
        sgnr = (L2_get_node_reply){.chk = nn->key, .right = null_pptr};
    } else {
        sgnr = (L2_get_node_reply){.chk = nn->chk[height], .right = nn->right[height]};
    }

    __mram_ptr L2_get_node_reply* dst = (__mram_ptr L2_get_node_reply*)send_task_start;
    mram_write(&sgnr, &dst[i], sizeof(L2_get_node_reply));
}

static inline void L2_print_node(int i, mL2ptr node) {
    printf("*%d %lld %x\n", i, node->key, (uint32_t)node);
    int height = (node->height > LOWER_PART_HEIGHT) ? LOWER_PART_HEIGHT : node->height;
    for (int ht = 0; ht < height; ht++) {
        printf("%lld %d-%x %d-%x\n", node->chk[ht], node->left[ht].id, node->left[ht].addr,
                node->right[ht].id, node->right[ht].addr);
    }
}
static inline void L2_print_nodes(int length, mL2ptr *newnode, bool quit, bool lock) {
    uint32_t tasklet_id = me();
    if (lock) mutex_lock(L2_lock);
    printf("*** %d ***\n", tasklet_id);
    for (int i = 0; i < length; i++) {
        // printf("*%d %lld %x\n", i, newnode[i]->key, (uint32_t)newnode[i]);
        L2_print_node(i, newnode[i]);
        // printf("*%d %lld %x\n", i, newnode[i]->key, (uint32_t)newnode[i]);
        // for (int ht = 0; ht < newnode[i]->height; ht++) {
        //     printf("%lld %x %x\n", newnode[i]->chk[ht], newnode[i]->left[ht].addr,
        //            newnode[i]->right[ht].addr);
        // }
    }
    if (lock) mutex_unlock(L2_lock);
    if (quit) {
        EXIT();
    }
}

static inline void L2_insert_parallel(int l, int length, int64_t *keys,
                                      int8_t *heights, pptr *addrs,
                                      uint32_t *newnode_size) {
    uint32_t tasklet_id = me();
    mL2ptr *newnode = mem_alloc(sizeof(mL2ptr) * length);

    barrier_wait(&L2_barrier);

    if (tasklet_id == 0) {
        for (int i = 0; i < NR_TASKLETS; i++) {
            newnode_size[i] = (uint32_t)reserve_space_L2(newnode_size[i]);
        }
    }

    barrier_wait(&L2_barrier);

    __mram_ptr void* maddr = (__mram_ptr void*) newnode_size[tasklet_id];
    __mram_ptr void* rmaddr;
    if (tasklet_id + 1 < NR_TASKLETS) {
        rmaddr = (__mram_ptr void*) newnode_size[tasklet_id + 1];
    }

    barrier_wait(&L2_barrier); // !!!!!

    for (int i = 0; i < length; i++) {
        newnode[i] = get_new_L2(keys[i], heights[i], addrs[i], maddr);
        int l2height = (heights[i] > LOWER_PART_HEIGHT) ? LOWER_PART_HEIGHT : heights[i];
        maddr += L2_node_size(l2height);
    }
    if (tasklet_id + 1 < NR_TASKLETS) {
        IN_DPU_ASSERT(maddr == rmaddr, "L2 insert parallel: wrong maddr");
    } else {
        IN_DPU_ASSERT(maddr == (l2buffer + l2cnt), "L2 insert parallel: wrong maddr");
    }

    L2_insert_reply* sir = mem_alloc(sizeof(L2_insert_reply) * length);
    for (int i = 0; i < length; i++) {
        sir[i] = (L2_insert_reply){
            .addr = (pptr){.id = DPU_ID, .addr = (uint32_t)newnode[i]}};
    }
    __mram_ptr L2_insert_reply* dst =
        (__mram_ptr L2_insert_reply*)send_task_start;
    mram_write(sir, &dst[l], sizeof(L2_insert_reply) * length);
}

static inline void L2_build_up(pptr addr, pptr up) {
    IN_DPU_ASSERT(addr.id == DPU_ID, "L2 build up: wrong id");
    mL2ptr nn = (mL2ptr)addr.addr;
    nn->up = (pptr){.id = DPU_ID, .addr = up.addr};
}

static inline void L2_build_lr(int64_t height, pptr addr, pptr val, int64_t chk) {
    IN_DPU_ASSERT(addr.id == DPU_ID, "L2 build lr: wrong id");
    mL2ptr nn = (mL2ptr)addr.addr;
    if (height >= 0) {
        IN_DPU_ASSERT(height >= 0 && height < nn->height, "L2 build lr: wrong height");
        nn->right[height] = val;
        nn->chk[height] = chk;
    } else {
        height = -1 - height;
        IN_DPU_ASSERT(height >= 0 && height < nn->height, "L2 build lr: wrong height");
        nn->left[height] = val;
        IN_DPU_ASSERT(chk == -1, "L2 build lr: wrong chk");
    }
}

static inline void L2_remove_parallel(int length, int64_t* keys,
                                      int32_t* oldnode_size,
                                      int32_t* oldnode_count) {
    uint32_t tasklet_id = me();
    mL2ptr* nodes = mem_alloc(sizeof(mL2ptr) * length);
    int8_t* heights = mem_alloc(sizeof(int8_t) * length);
    int cnt = 0;
    for (int i = 0; i < length; i++) {
        uint32_t htv = ht_search(l2ht, keys[i], L2_ht_get);
        if (htv == INVALID_DPU_ADDR) {
            continue;
        }
        nodes[cnt] = (mL2ptr)htv;
        IN_DPU_ASSERT(nodes[i]->height <= LOWER_PART_HEIGHT,
                      "L2 remove parallel: wrong height");
        heights[cnt] = nodes[cnt]->height;
        oldnode_size[tasklet_id] += L2_node_size(heights[cnt]);
        cnt++;
    }
    oldnode_count[tasklet_id] = cnt;
    length = cnt;

    barrier_wait(&L2_barrier);

    if (tasklet_id == 0) {
        for (int i = 1; i < NR_TASKLETS; i++) {
            oldnode_size[i] += oldnode_size[i - 1];
            oldnode_count[i] += oldnode_count[i - 1];
        }
        *send_task_count = oldnode_count[NR_TASKLETS - 1];
        *send_size = oldnode_count[NR_TASKLETS - 1];
    }

    barrier_wait(&L2_barrier);

    __mram_ptr int64_t* send_offsets = send_size + 1;

    int buffer_cnt = (tasklet_id == 0) ? 0 : oldnode_size[tasklet_id - 1];
    __mram_ptr void* send_tasks =
        (__mram_ptr void*)(send_buffer +
                           sizeof(int64_t) *
                               (2 + oldnode_count[NR_TASKLETS - 1]));

    int l = (tasklet_id == 0) ? 0 : oldnode_count[tasklet_id - 1];
    int r = oldnode_count[tasklet_id];
    IN_DPU_ASSERT(l + length == r, "L2 remove: wrong l, r");

    void* buffer = mem_alloc(L2_node_size(LOWER_PART_HEIGHT));
    L2_remove_reply* srr = (L2_remove_reply*)buffer;
    for (int i = 0; i < length; i++) {
        srr->height = heights[i];
        pptr* left = buffer + sizeof(L2_remove_reply);
        pptr* right =
            buffer + sizeof(L2_remove_reply) + sizeof(pptr) * heights[i];
        for (int ht = 0; ht < heights[i]; ht++) {
            left[ht] = nodes[i]->left[ht];
            right[ht] = nodes[i]->right[ht];
        }
        send_offsets[l + i] = buffer_cnt;
        IN_DPU_ASSERT((void*)(&send_offsets[l + i]) < send_tasks,
                      "L2 remove: wrong offset");
        int siz = L2_node_size(heights[i]);
        mram_write(buffer, send_tasks + buffer_cnt, siz);
        buffer_cnt += siz;
    }
}

inline void L2_print_all_nodes() {
    for (int i = 0; i < LX_HASHTABLE_SIZE; i ++) {
        if (l2ht[i].v != 0) {
            L2_print_node(i, (mL2ptr)l2ht[i].v);
            // printf("%d\n", l2ht[i].v);
        }
    }
}