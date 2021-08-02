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
#include <perfcounter.h>
#include <string.h>
#include "common.h"
#include "task_dpu.h"
#include "l3.h"

/* -------------- Storage -------------- */

// DPU ID
__host uint64_t DPU_ID;

// Task Buffers
__host uint8_t DPU_RECEIVE_BUFFER[MAX_TASK_BUFFER_SIZE_PER_DPU];
__host uint64_t DPU_RECEIVE_BUFFER_OFFSET[MAX_TASK_COUNT_PER_DPU];
__host uint64_t DPU_RECEIVE_BUFFER_SIZE;
__host uint64_t DPU_RECEIVE_BUFFER_TASK_COUNT;

__host uint8_t DPU_SEND_BUFFER[MAX_TASK_BUFFER_SIZE_PER_DPU];
__host uint64_t DPU_SEND_BUFFER_OFFSET[MAX_TASK_COUNT_PER_DPU];
__host uint64_t DPU_SEND_BUFFER_SIZE;
__host uint64_t DPU_SEND_BUFFER_TASK_COUNT;


// Node Buffers & Hash Tables
__mram_noinit ht_slot l3ht[LX_HASHTABLE_SIZE]; // must be 8 bytes aligned

__mram_noinit uint8_t l3buffer[LX_BUFFER_SIZE];
__host int l3cnt = 8;
__host int l3htcnt;

__host mL3ptr root;



#ifdef MRAM_TASK_BUFFER
#define init_task(to, from, len) mram_read(from, to, len)
typedef __mram_ptr task* mptask;
#else
#define init_task(to, from, len) memcpy(to, from, len)
typedef task* mptask;
#endif


void execute(mptask t) {
    // printf("EXEC ");
    if (t->type == L3_INIT) {
        // printf("L3_INIT\n");
        L3_insert_task tit;
        // mram_read(t->buffer, &tit, sizeof(L3_insert_task));
        init_task(&tit, t->buffer, sizeof(L3_insert_task));
        L3_init(&tit);
    } else if (t->type == L3_INSERT) {
        // printf("L3_INSERT\n");
        L3_insert_task tit;
        init_task(&tit, t->buffer, sizeof(L3_insert_task));
        L3_insert(&tit);
    } else if (t->type == L3_REMOVE) {
        L3_remove_task trt;
        init_task(&trt, t->buffer, sizeof(L3_remove_task));
        L3_remove(&trt);
    } else if (t->type == L3_SEARCH) {
        L3_search_task tst;
        init_task(&tst, t->buffer, sizeof(L3_search_task));
        L3_search(&tst);
    } else if (t->type == L3_SANCHECK) {
        L3_sancheck();
    } else {
        IN_DPU_ASSERT(false, "Wrong Task Type\n");
        // assert(false);
    }
}

void garbage_func();

int main()
{
    uint32_t tasklet_id = me();
    if (tasklet_id == 0) {
        printf("%lu\n", DPU_ID);
        // uint32_t mram_base_addr_A = (uint32_t)DPU_MRAM_HEAP_POINTER;
        // printf("!!! %x\n", mram_base_addr_A);
        // printf("** %d %d %d\n", sizeof(uint64_t), sizeof(unsigned long long), sizeof(unsigned long));
        DPU_SEND_BUFFER_SIZE = DPU_SEND_BUFFER_TASK_COUNT = 0;
    } else {
        return 0;
    }
    
    uint32_t lft = DPU_RECEIVE_BUFFER_TASK_COUNT * tasklet_id / NR_TASKLETS;
    uint32_t rt = DPU_RECEIVE_BUFFER_TASK_COUNT * (tasklet_id + 1) / NR_TASKLETS;

    for (uint32_t i = lft; i < rt; i++) {
        // printf("%u %u %u\n", i, lft, rt);
        mptask t = (mptask)(DPU_RECEIVE_BUFFER + DPU_RECEIVE_BUFFER_OFFSET[i]);
        execute(t);
    }
    printf("l3cnt=%d l3htcnt=%d\n", l3cnt, l3htcnt);

    

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
