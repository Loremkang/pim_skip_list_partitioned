/*
 * Copyright (c) 2014-2017 - uPmem
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * An example of checksum computation with multiple tasklets.
 *
 * Every tasklet processes specific areas of the MRAM, following the "rake"
 * strategy:
 *  - Tasklet number T is first processing block number TxN, where N is a
 *    constant block size
 *  - It then handles block number (TxN) + (NxM) where M is the number of
 *    scheduled tasklets
 *  - And so on...
 *
 * The host is in charge of computing the final checksum by adding all the
 * individual results.
 */
#include <stdint.h>
#include <stdio.h>
#include <defs.h>
#include <mram.h>
#include <alloc.h>
#include <perfcounter.h>
#include <barrier.h>
#include <string.h>
#include "common.h"
#include "task_dpu.h"
#include "l3.h"

/* -------------- Storage -------------- */

BARRIER_INIT(init_barrier, NR_TASKLETS);

// __host volatile int host_barrier;

// __host int num_tasklets;
// __host uint64_t task_type;

// DPU ID
__host uint64_t DPU_ID;

// Task Buffers
// __host uint8_t DPU_RECEIVE_BUFFER[MAX_TASK_BUFFER_SIZE_PER_DPU];
// __host uint64_t DPU_RECEIVE_BUFFER_OFFSET[MAX_TASK_COUNT_PER_DPU];
// __host uint64_t DPU_RECEIVE_BUFFER_SIZE;
// __host uint64_t DPU_RECEIVE_BUFFER_TASK_COUNT;

// __host uint8_t DPU_SEND_BUFFER[MAX_TASK_BUFFER_SIZE_PER_DPU];
// __host uint64_t DPU_SEND_BUFFER_OFFSET[MAX_TASK_COUNT_PER_DPU];
// __host uint64_t DPU_SEND_BUFFER_SIZE;
// __host uint64_t DPU_SEND_BUFFER_TASK_COUNT;

__host uint64_t DPU_EPOCH_NUMBER;
__host uint64_t DPU_RECEIVE_BUFFER_SIZE;
__host uint64_t DPU_RECEIVE_BUFFER_TASK_COUNT;

__host uint64_t DPU_SEND_BUFFER_SIZE;
__host uint64_t DPU_SEND_BUFFER_TASK_COUNT;

// Node Buffers & Hash Tables
__mram_noinit ht_slot l3ht[LX_HASHTABLE_SIZE]; // must be 8 bytes aligned

mL3ptr *bufferA_shared, *bufferB_shared;
int8_t *max_height_shared;

__mram_noinit uint8_t l3buffer[LX_BUFFER_SIZE];
__host int l3cnt = 8;
__host int l3htcnt;

__host mL3ptr root;

__host __mram_ptr uint64_t* receive_buffer_offset;

#define MRAM_TASK_BUFFER

#ifdef MRAM_TASK_BUFFER
#define init_task(to, from, len) mram_read(from, to, len)
typedef __mram_ptr task* mptask;
#else
#define init_task(to, from, len) memcpy(to, from, len)
typedef task* mptask;
#endif

void execute(mptask t, int l, int r) {
    uint32_t tasklet_id = me();
    // printf("EXEC ");
    if (t->type == L3_INIT) {
        // printf("L3_INIT\n");
        L3_insert_task tit;
        // mram_read(t->buffer, &tit, sizeof(L3_insert_task));
        init_task(&tit, t->buffer, sizeof(L3_insert_task));
        L3_init(&tit);
    } else if (t->type == L3_INSERT) {
        // printf("L3_INSERT\n");
        int length = r - l;
        int64_t* keys = mem_alloc(sizeof(int64_t) * length);
        int8_t* heights = mem_alloc(sizeof(int8_t) * length);
        pptr* addrs = mem_alloc(sizeof(pptr) * length);

        L3_insert_task tit;
        for (int i = 0; i < length; i++) {
            t = (mptask)(DPU_MRAM_HEAP_POINTER + DPU_RECEIVE_BUFFER +
                         (uint32_t)receive_buffer_offset[i + l]);
            init_task(&tit, t->buffer, sizeof(L3_insert_task));
            keys[i] = tit.key;
            heights[i] = (int8_t)tit.height;
            addrs[i] = tit.addr;
            // printf("%u %lld %d\n", receive_buffer_offset[i + length], keys[i], heights[i]);
            IN_DPU_ASSERT(heights[i] > 0 && heights[i] < MAX_L3_HEIGHT, "execute: invalid height\n");
        }

        // int8_t *max_height_shared = mem_alloc(sizeof(int8_t) * NR_TASKLETS);
        mL3ptr *right_predecessor_shared = bufferA_shared;
            // mem_alloc(sizeof(mL3ptr) * NR_TASKLETS * MAX_L3_HEIGHT);
        mL3ptr *right_newnode_shared = bufferB_shared;
            // mem_alloc(sizeof(mL3ptr) * NR_TASKLETS * MAX_L3_HEIGHT);
        // for (int i = 0; i < length; i ++) {
        //     printf("%lld\n", keys[i]);
        // }
        // IN_DPU_ASSERT(false, "\n");
        L3_insert_parallel(length, keys, heights, addrs, max_height_shared,
                           right_predecessor_shared, right_newnode_shared);

    } else if (t->type == L3_REMOVE) {
        int length = r - l;
        int64_t* keys = mem_alloc(sizeof(int64_t) * length);
        L3_remove_task trt;
        for (int i = 0; i < length; i++) {
            t = (mptask)(DPU_MRAM_HEAP_POINTER + DPU_RECEIVE_BUFFER +
                         (uint32_t)receive_buffer_offset[i + l]);
            init_task(&trt, t->buffer, sizeof(L3_remove_task));
            keys[i] = trt.key;
        }

        mL3ptr *left_node_shared = bufferA_shared;
            // mem_alloc(sizeof(mL3ptr) * NR_TASKLETS * MAX_L3_HEIGHT);
        // int8_t *max_height_shared = mem_alloc(sizeof(int8_t) * NR_TASKLETS);
        L3_remove_parallel(length, keys, max_height_shared, left_node_shared);
    } else if (t->type == L3_SEARCH) {
        L3_search_task tst;
        for (int i = l; i < r; i++) {
            t = (mptask)(DPU_MRAM_HEAP_POINTER + DPU_RECEIVE_BUFFER +
                         (uint32_t)receive_buffer_offset[i]);
            init_task(&tst, t->buffer, sizeof(L3_search_task));
            printf("%d*%lld\n", i, tst.key);
            L3_search(tst.key, 0, 0, NULL);
        }
        // EXIT();
    } else if (t->type == L3_SANCHECK) {
        if (tasklet_id == 0) {
            L3_sancheck();
        }
    } else {
        IN_DPU_ASSERT(false, "Wrong Task Type\n");
        // assert(false);
    }
}

void garbage_func();

int main() {
    uint32_t tasklet_id = me();
    if (tasklet_id == 0) {
        // printf("%lu\n", DPU_ID);
        // uint32_t mram_base_addr_A = (uint32_t)DPU_MRAM_HEAP_POINTER;
        // printf("!!! %x\n", mram_base_addr_A);
        // printf("** %d %d %d\n", sizeof(uint64_t), sizeof(unsigned long long),
        // sizeof(unsigned long));
        DPU_SEND_BUFFER_SIZE = DPU_SEND_BUFFER_TASK_COUNT = 0;
        mem_reset();

        // num_tasklets = NR_TASKLETS;
        // if ((int)DPU_RECEIVE_BUFFER_TASK_COUNT < num_tasklets) {
        //     num_tasklets = (int)DPU_RECEIVE_BUFFER_TASK_COUNT;
        // }

        // if (DPU_RECEIVE_BUFFER_TASK_COUNT != 0) {
        //     mptask t = (mptask)(DPU_MRAM_HEAP_POINTER + DPU_RECEIVE_BUFFER);
        // }

        // host_barrier = num_tasklets;
        bufferA_shared = mem_alloc(sizeof(mL3ptr) * NR_TASKLETS * MAX_L3_HEIGHT);
        bufferB_shared = mem_alloc(sizeof(mL3ptr) * NR_TASKLETS * MAX_L3_HEIGHT);
        max_height_shared = mem_alloc(sizeof(int8_t) * NR_TASKLETS);
        for (int i = 0; i < NR_TASKLETS; i ++) {
            max_height_shared[i] = 0;
        }
        printf("%lu * %x * %x * %x\n", DPU_RECEIVE_BUFFER_TASK_COUNT,
               (uint32_t)bufferA_shared, (uint32_t)bufferB_shared,
               (uint32_t)max_height_shared);
    }

    barrier_wait(&init_barrier);

    if (DPU_RECEIVE_BUFFER_TASK_COUNT == 0) {
        return 0;
    }

    uint32_t lft, rt;
    // if (num_tasklets < NR_TASKLETS) {
    // if ((int)tasklet_id >= num_tasklets) {
    // return 0;
    // }
    // lft = tasklet_id;
    // rt = tasklet_id + 1;
    // } else {
    lft = DPU_RECEIVE_BUFFER_TASK_COUNT * tasklet_id / NR_TASKLETS;
    rt = DPU_RECEIVE_BUFFER_TASK_COUNT * (tasklet_id + 1) / NR_TASKLETS;
    // }

    receive_buffer_offset = DPU_MRAM_HEAP_POINTER + DPU_RECEIVE_BUFFER_OFFSET;
    // mptask t = (mptask)(DPU_MRAM_HEAP_POINTER + DPU_RECEIVE_BUFFER +
    // (uint32_t)receive_buffer_offset[i]);
    mptask t = (mptask)(DPU_MRAM_HEAP_POINTER + DPU_RECEIVE_BUFFER);
    execute(t, lft, rt);
    // for (uint32_t i = lft; i < rt; i++) {
    //     printf("%u\n", i, receive_buffer_offset[i]);
    //     execute(t);
    // }
    if (tasklet_id == 0) {
        printf("epoch=%lu l3cnt=%d l3htcnt=%d\n", DPU_EPOCH_NUMBER, l3cnt,
               l3htcnt);
    }

    // for (int i = 0; i < DPU_RECEIVE_BUFFER_TASK_COUNT; i ++)
    // int lft = DPU_RECEIVE_BUFFER_CNT * tasklet_id / NR_TASKLETS;
    // int rt = DPU_RECEIVE_BUFFER_CNT * (tasklet_id + 1) / NR_TASKLETS;
    // if (rt > DPU_RECEIVE_BUFFER_CNT) {
    //     rt = DPU_RECEIVE_BUFFER_CNT;
    // }

    // // bool failed = false;
    // for (int i = lft; i < rt; i ++) {

    //     task *t = &DPU_RECEIVE_BUFFER[i];
    //     if (t->order[t->nxt] != DPU_ID) {
    //         printf("id != nxt ERROR!!\n");
    //         // failed = true;
    //     }
    //     t->nxt ++;
    //     DPU_SEND_BUFFER[i] = DPU_RECEIVE_BUFFER[i];
    // }
    return 0;
}
