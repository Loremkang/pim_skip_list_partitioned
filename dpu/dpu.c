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
#include <defs.h>
#include <mram.h>
#include <perfcounter.h>
#include <stdint.h>
#include <stdio.h>

#include "common.h"

/* Use blocks of 256 bytes */

// __dma_aligned uint8_t DPU_CACHES[NR_TASKLETS][BLOCK_SIZE];
// __host dpu_results_t DPU_RESULTS;

// __host uint64_t DPU_STATE; // 0 for ready; 1 for working
// __host uint64_t DPU_KILLED; // set to 1 to kill
__host uint64_t DPU_ID;

__mram_noinit uint8_t DPU_RECEIVE_BUFFER[MAX_TASK_BUFFER_SIZE_PER_DPU];
__mram_noinit uint64_t DPU_RECEIVE_BUFFER_OFFSET[MAX_TASK_COUNT_PER_DPU];
__mram_noinit uint64_t DPU_RECEIVE_BUFFER_SIZE;
__mram_noinit uint64_t DPU_RECEIVE_BUFFER_TASK_COUNT;

__mram_noinit uint8_t DPU_SEND_BUFFER[MAX_TASK_BUFFER_SIZE_PER_DPU];
__mram_noinit uint64_t DPU_SEND_BUFFER_OFFSET[MAX_TASK_COUNT_PER_DPU];
__mram_noinit uint64_t DPU_SEND_BUFFER_SIZE;
__mram_noinit uint64_t DPU_SEND_BUFFER_TASK_COUNT;

// __mram_noinit uint8_t DPU_BUFFER[BUFFER_SIZE];

void twoval(twoval_task* tt, int i) {
    // printf("%d %lu %lu\n", i, tt->a[0], tt->a[1]);
    assert(tt->a[1] == tt->a[0] + 1);
}

void threeval(threeval_task* tt, int i) {
    assert(tt->a[1] == tt->a[0] + 2);
    assert(tt->a[2] == tt->a[1] + 2);
    // printf("%d %lu %lu %lu\n", i, tt->a[0], tt->a[1], tt->a[2]);
}

void execute(__mram_ptr task *t, int i) {
    // printf("EXEC ");
    if (t->type == 0) {
        // printf("UPPER INSERT\n");
        // assert(DPU_RECEIVE_BUFFER_OFFSET[])
        twoval_task tt;
        mram_read(t->buffer, &tt, sizeof(twoval_task));
        twoval(&tt, i);
    } else if (t->type == 1) {
        // printf("LOWER INIT\n");
        threeval_task tt;
        mram_read(t->buffer, &tt, sizeof(threeval_task));
        threeval(&tt, i);
    } else {
        // printf("UNKNOWN TYPE %d\n", t->type);
        assert(false);
    }
}

int main()
{
    uint32_t tasklet_id = me();
    if (tasklet_id == 0) {
        printf("%lu\n", DPU_ID);
        // printf("** %d %d %d\n", sizeof(uint64_t), sizeof(unsigned long long), sizeof(unsigned long));
    }
    
    uint32_t lft = DPU_RECEIVE_BUFFER_TASK_COUNT * tasklet_id / NR_TASKLETS;
    uint32_t rt = DPU_RECEIVE_BUFFER_TASK_COUNT * (tasklet_id + 1) / NR_TASKLETS;

    for (uint32_t i = lft; i < rt; i++) {
        __mram_ptr task *t = (__mram_ptr task*)(DPU_RECEIVE_BUFFER + DPU_RECEIVE_BUFFER_OFFSET[i]);
        execute(t, i);
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
