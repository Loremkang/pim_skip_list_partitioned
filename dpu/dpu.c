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
#define BLOCK_SIZE (256)

// __dma_aligned uint8_t DPU_CACHES[NR_TASKLETS][BLOCK_SIZE];
// __host dpu_results_t DPU_RESULTS;

// __host uint64_t DPU_STATE; // 0 for ready; 1 for working
// __host uint64_t DPU_KILLED; // set to 1 to kill
__host uint64_t DPU_ID;

__host task DPU_RECEIVE_BUFFER[MAX_TASK_PER_DPU];
__host uint64_t DPU_RECEIVE_BUFFER_CNT;

__host task DPU_SEND_BUFFER[MAX_TASK_PER_DPU];
__host uint64_t DPU_SEND_BUFFER_CNT;

__mram_noinit uint8_t DPU_BUFFER[BUFFER_SIZE];



/**
 * @fn main
 * @brief main function executed by each tasklet
 * @return the checksum result
 */
int main()
{
    uint32_t tasklet_id = me();
    if (tasklet_id == 0) {
        printf("%lu\n", DPU_ID);
        printf("%lu\n", DPU_RECEIVE_BUFFER_CNT);
        printf("%lx\n", &DPU_ID);
        printf("%lx\n", &(DPU_RECEIVE_BUFFER[0]));
        printf("%lx\n", &DPU_RECEIVE_BUFFER_CNT);
        printf("%lx\n", &(DPU_BUFFER[0]));
        DPU_SEND_BUFFER_CNT = DPU_RECEIVE_BUFFER_CNT;
    //     printf("%lu\n", DPU_KILLED);
    }

    int lft = DPU_RECEIVE_BUFFER_CNT * tasklet_id / NR_TASKLETS;
    int rt = DPU_RECEIVE_BUFFER_CNT * (tasklet_id + 1) / NR_TASKLETS;
    if (rt > DPU_RECEIVE_BUFFER_CNT) {
        rt = DPU_RECEIVE_BUFFER_CNT;
    }

    // bool failed = false;
    for (int i = lft; i < rt; i ++) {
        task *t = &DPU_RECEIVE_BUFFER[i];
        if (t->order[t->nxt] != DPU_ID) {
            printf("id != nxt ERROR!!\n");
            // failed = true;
        }
        t->nxt ++;
        DPU_SEND_BUFFER[i] = DPU_RECEIVE_BUFFER[i];
    }

    // printf("%d, l = %d, r = %d, Succeed = %d\n", NR_TASKLETS, lft, rt, !failed);

    // while (!DPU_KILLED) {
    // }


    // DPU_CACHES;
    // DPU_RESULTS;
    // DPU_BUFFER;
    // uint8_t *cache = DPU_CACHES[tasklet_id];
    // dpu_result_t *result = &DPU_RESULTS.tasklet_result[tasklet_id];
    // uint32_t checksum = 0;

    // /* Initialize once the cycle counter */
    // if (tasklet_id == 0)
    //     perfcounter_config(COUNT_CYCLES, true);

    // // for (uint32_t buffer_idx = tasklet_id * BLOCK_SIZE; buffer_idx < BUFFER_SIZE; buffer_idx += (NR_TASKLETS * BLOCK_SIZE))
    // {

    // //     /* load cache with current mram block. */
    // //     mram_read(&DPU_BUFFER[buffer_idx], cache, BLOCK_SIZE);

    // //     /* computes the checksum of a cached block */
    // //     for (uint32_t cache_idx = 0; cache_idx < BLOCK_SIZE; cache_idx++) {
    // //         checksum += cache[cache_idx];
    // //     }
    // // }

    // /* keep the 32-bit LSB on the 64-bit cycle counter */
    // result->cycles = (uint32_t)perfcounter_get();
    // result->checksum = checksum;

    // printf("[%02d] Checksum = 0x%08x\n", tasklet_id, result->checksum);
    // printf("Finished! %d %d %d\n", *(int*)DPU_CACHES, *(int*)&DPU_RESULTS, *(int*)DPU_BUFFER);
    return 0;
}
