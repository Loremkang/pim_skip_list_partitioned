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
__host int64_t DPU_ID;

__host int64_t dpu_epoch_number;
__host int64_t dpu_task_type;
__host int64_t dpu_task_count;

// Node Buffers & Hash Tables
__mram_noinit ht_slot l3ht[LX_HASHTABLE_SIZE]; // must be 8 bytes aligned

mL3ptr *bufferA_shared, *bufferB_shared;
int8_t *max_height_shared;

__mram_noinit uint8_t l3buffer[LX_BUFFER_SIZE];
__host int l3cnt = 8;
__host int l3htcnt = 0;

__host mL3ptr root;

__host __mram_ptr uint64_t* receive_buffer_offset;

__host __mram_ptr uint8_t* receive_buffer = DPU_MRAM_HEAP_POINTER;
__host __mram_ptr uint8_t* receive_task_start = DPU_MRAM_HEAP_POINTER + sizeof(int64_t) * 3;
__host __mram_ptr uint8_t* send_buffer = DPU_MRAM_HEAP_POINTER + MAX_TASK_BUFFER_SIZE_PER_DPU;
__host __mram_ptr uint8_t* send_task_start = DPU_MRAM_HEAP_POINTER + MAX_TASK_BUFFER_SIZE_PER_DPU + sizeof(int64_t);

#define MRAM_TASK_BUFFER

void execute(int l, int r) {
    uint32_t tasklet_id = me();
    // printf("EXEC ");
    switch (dpu_task_type) {
        case L3_INIT_TSK: {
            if (tasklet_id == 0) {
                L3_insert_task tit;
                mram_read(receive_task_start, &tit, sizeof(L3_init_task));
                L3_init(&tit);
            }
            break;
        }

        case L3_INSERT_TSK: {
            int length = r - l;
            int64_t* keys = mem_alloc(sizeof(int64_t) * length);
            pptr* addrs = mem_alloc(sizeof(pptr) * length);
            int8_t* heights = mem_alloc(sizeof(int8_t) * length);

            __mram_ptr L3_insert_task* tit = (__mram_ptr L3_insert_task*) receive_task_start;
            tit += l;
            for (int i = 0; i < length; i++) {
                keys[i] = tit[i].key;
                addrs[i] = tit[i].addr;
                heights[i] = tit[i].height;
                IN_DPU_ASSERT(heights[i] > 0 && heights[i] < MAX_L3_HEIGHT,
                              "execute: invalid height\n");
            }

            mL3ptr* right_predecessor_shared = bufferA_shared;
            mL3ptr* right_newnode_shared = bufferB_shared;
            L3_insert_parallel(length, l, keys, heights, addrs, max_height_shared,
                               right_predecessor_shared, right_newnode_shared);
            break;
        }

        case L3_REMOVE_TSK: {
            int length = r - l;
            int64_t* keys = mem_alloc(sizeof(int64_t) * length);
            __mram_ptr L3_remove_task* trt = (__mram_ptr L3_remove_task*)receive_task_start;
            trt += l;
            for (int i = 0; i < length; i++) {
                keys[i] = trt[i].key;
            }

            mL3ptr* left_node_shared = bufferA_shared;
            L3_remove_parallel(length, keys, max_height_shared,
                               left_node_shared);
            break;
        }
        case L3_SEARCH_TSK: {
            __mram_ptr L3_search_task* tst = (__mram_ptr L3_search_task*)receive_task_start;
            for (int i = l; i < r; i ++) {
                L3_search(tst[i].key, i, 0, NULL);
            }
            break;
        }
        case L3_GET_TSK: {
            __mram_ptr L3_get_task* tgt = (__mram_ptr L3_get_task*)receive_task_start;
            for (int i = l; i < r; i++) {
                L3_get(tgt[i].key, i);
            }
            break;
        }
        case L3_SANCHECK_TSK: {
            if (tasklet_id == 0) {
                L3_sancheck();
            }
            break;
        }
        default: {
            IN_DPU_ASSERT(false, "Wrong Task Type\n");
            break;
        }
    }
    IN_DPU_SUCCEES();
}

void garbage_func();

int main() {
    uint32_t tasklet_id = me();

    if (tasklet_id == 0) {
        // DPU_SEND_BUFFER_SIZE = DPU_SEND_BUFFER_TASK_COUNT = 0;
        mem_reset();

        bufferA_shared =
            mem_alloc(sizeof(mL3ptr) * NR_TASKLETS * MAX_L3_HEIGHT);
        bufferB_shared =
            mem_alloc(sizeof(mL3ptr) * NR_TASKLETS * MAX_L3_HEIGHT);
        max_height_shared = mem_alloc(sizeof(int8_t) * NR_TASKLETS);
        for (int i = 0; i < NR_TASKLETS; i++) {
            max_height_shared[i] = 0;
        }

        mram_read(receive_buffer, &dpu_epoch_number, sizeof(int64_t));
        mram_read(receive_buffer + sizeof(int64_t), &dpu_task_type,
                  sizeof(int64_t));
        mram_read(receive_buffer + sizeof(int64_t) * 2, &dpu_task_count,
                  sizeof(int64_t));
    }

    barrier_wait(&init_barrier);

    if (dpu_task_count == 0) {
        return 0;
    }

    uint32_t lft = dpu_task_count * tasklet_id / NR_TASKLETS;
    uint32_t rt = dpu_task_count * (tasklet_id + 1) / NR_TASKLETS;

    execute(lft, rt);
    if (tasklet_id == 0) {
        printf("epoch=%lld l3cnt=%d l3htcnt=%d\n", dpu_epoch_number, l3cnt,
               l3htcnt);
    }
    return 0;
}
