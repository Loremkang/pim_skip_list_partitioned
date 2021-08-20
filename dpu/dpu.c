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
#include "l2.h"

/* -------------- Storage -------------- */

BARRIER_INIT(init_barrier, NR_TASKLETS);

// __host volatile int host_barrier;

// __host int num_tasklets;
// __host uint64_t task_type;

// DPU ID
__host int64_t DPU_ID = -1;

__host int64_t dpu_epoch_number;
__host int64_t dpu_task_type;
__host int64_t dpu_task_count;

#define MRAM_BUFFER_SIZE (128)
void *bufferA_shared, *bufferB_shared;
int8_t *max_height_shared;
uint32_t *newnode_size;

// Node Buffers & Hash Tables
// L3
__mram_noinit ht_slot l3ht[LX_HASHTABLE_SIZE]; // must be 8 bytes aligned
__host int l3htcnt = 0;
__mram_noinit uint8_t l3buffer[LX_BUFFER_SIZE];
__host int l3cnt = 8;

// L2
__mram_noinit ht_slot l2ht[LX_HASHTABLE_SIZE]; // must be 8 bytes aligned
__host int l2htcnt = 0;
__mram_noinit uint8_t l2buffer[LX_BUFFER_SIZE];
__host int l2cnt = 8;


__host mL3ptr root;

__host __mram_ptr uint64_t* receive_buffer_offset;

// send
__host __mram_ptr uint8_t* receive_buffer = DPU_MRAM_HEAP_POINTER;
__host __mram_ptr uint8_t* receive_task_start = DPU_MRAM_HEAP_POINTER + sizeof(int64_t) * 3;

// receive
__host __mram_ptr uint8_t* send_buffer = DPU_MRAM_HEAP_POINTER + DPU_SEND_BUFFER_OFFSET;
__host __mram_ptr uint8_t* send_task_start = DPU_MRAM_HEAP_POINTER + DPU_SEND_BUFFER_OFFSET + sizeof(int64_t);

// fixed length
__host __mram_ptr int64_t* send_task_count = DPU_MRAM_HEAP_POINTER + DPU_SEND_BUFFER_OFFSET;
__host __mram_ptr int64_t* send_size = DPU_MRAM_HEAP_POINTER + DPU_SEND_BUFFER_OFFSET + sizeof(int64_t);


#define MRAM_TASK_BUFFER

static inline void init(init_task *it) {
    DPU_ID = it->id;
    storage_init();
}

void execute(int l, int r) {
    IN_DPU_ASSERT(dpu_task_type == INIT_TSK || DPU_ID != -1, "execute: id not initialized");
    uint32_t tasklet_id = me();
    // printf("EXEC ");
    __mram_ptr int64_t* buffer_type = (__mram_ptr int64_t*)send_buffer;
    *buffer_type = BUFFER_FIXED_LENGTH; // default

    switch (dpu_task_type) {
        case INIT_TSK: {
            if (tasklet_id == 0) {
                init_task it;
                mram_read(receive_task_start, &it, sizeof(init_task));
                init(&it);
            }
            break;
        }

        case L3_INIT_TSK: {
            if (tasklet_id == 0) {
                L3_init_task tit;
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

            int step = 10;
            L3_insert_task* buffer = mem_alloc(sizeof(L3_insert_task) * step);


            __mram_ptr L3_insert_task* tit =
                (__mram_ptr L3_insert_task*)receive_task_start;
            tit += l;

            newnode_size[tasklet_id] = 0;
            for (int i = 0; i < length; i += step) {
                mram_read(tit + i, buffer, sizeof(L3_insert_task) * step);
                for (int j = 0; j < step; j ++) {
                    if (i + j >= length) break;
                    keys[i + j] = buffer[j].key;
                    addrs[i + j] = buffer[j].addr;
                    heights[i + j] = buffer[j].height;
                    newnode_size[tasklet_id] += L3_node_size(heights[i + j]);
                    IN_DPU_ASSERT(
                        heights[i + j] > 0 && heights[i + j] < MAX_L3_HEIGHT,
                        "execute: invalid height\n");
                }
            }

            mL3ptr* right_predecessor_shared = bufferA_shared;
            mL3ptr* right_newnode_shared = bufferB_shared;
            L3_insert_parallel(length, l, keys, heights, addrs,
                               newnode_size, max_height_shared, right_predecessor_shared,
                               right_newnode_shared);
            break;
        }

        case L3_REMOVE_TSK: {
            int length = r - l;
            int64_t* keys = mem_alloc(sizeof(int64_t) * length);
            __mram_ptr L3_remove_task* trt =
                (__mram_ptr L3_remove_task*)receive_task_start;
            trt += l;
            mram_read(trt, keys, sizeof(int64_t) * length);

            mL3ptr* left_node_shared = bufferA_shared;
            L3_remove_parallel(length, keys, max_height_shared,
                               left_node_shared);
            break;
        }

        case L3_SEARCH_TSK: {
            __mram_ptr L3_search_task* tst =
                (__mram_ptr L3_search_task*)receive_task_start;
            for (int i = l; i < r; i++) {
                L3_search(tst[i].key, i, 0, NULL);
            }
            break;
        }

        case L3_GET_TSK: {
            __mram_ptr L3_get_task* tgt =
                (__mram_ptr L3_get_task*)receive_task_start;
            for (int i = l; i < r; i++) {
                L3_get(tgt[i].key, i);
            }
            break;
        }

        case L2_INIT_TSK: {
            if (tasklet_id == 0) {
                L2_init_task sit;
                mram_read(receive_task_start, &sit, sizeof(L2_init_task));
                L2_init(&sit);
            }
            break;
        }

        case L2_INSERT_TSK: {
            int length = r - l;
            int64_t* keys = mem_alloc(sizeof(int64_t) * length);
            pptr* addrs = mem_alloc(sizeof(pptr) * length);
            int8_t* heights = mem_alloc(sizeof(int8_t) * length);

            int step = 10;
            L2_insert_task* buffer = mem_alloc(sizeof(L2_insert_task) * step);

            __mram_ptr L2_insert_task* sit =
                (__mram_ptr L2_insert_task*)receive_task_start;
            sit += l;

            newnode_size[tasklet_id] = 0;
            for (int i = 0; i < length; i += step) {
                mram_read(sit + i, buffer, sizeof(L2_insert_task) * step);
                for (int j = 0; j < step; j++) {
                    if (i + j >= length) break;
                    keys[i + j] = buffer[j].key;
                    addrs[i + j] = buffer[j].addr;
                    heights[i + j] = buffer[j].height;
                    newnode_size[tasklet_id] += L2_node_size(heights[i + j]);
                    IN_DPU_ASSERT(
                        heights[i + j] > 0 && heights[i + j] < LOWER_PART_HEIGHT,
                        "execute: invalid height\n");
                }
            }
            L2_insert_parallel(l, length, keys, heights, addrs, newnode_size);
            break;
        }

        case L2_REMOVE_TSK: {
            int length = r - l;
            int64_t* keys = mem_alloc(sizeof(int64_t) * length);
            __mram_ptr L2_remove_task* trt =
                (__mram_ptr L2_remove_task*)receive_task_start;
            trt += l;
            mram_read(trt, keys, sizeof(int64_t) * length);

            int32_t* oldnode_size = bufferA_shared;
            int32_t* oldnode_count = bufferB_shared;
            L2_remove_parallel(length, keys, oldnode_size, oldnode_count);
            break;
        }

        case L2_GET_TSK: {
            __mram_ptr L2_get_task* sgt =
                (__mram_ptr L2_get_task*)receive_task_start;
            for (int i = l; i < r; i++) {
                L2_get(sgt[i].key, i);
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
        newnode_size = mem_alloc(sizeof(uint32_t) * NR_TASKLETS);
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
