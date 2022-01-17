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
#include <alloc.h>
#include <perfcounter.h>
#include <barrier.h>
#include <stdint.h>
#include <stdio.h>
#include "driver.h"
#include "task.h"
#include "task_framework_dpu.h"

static inline void init(dpu_init_task *it) {
    DPU_ID = it->dpu_id;
    printf("id=%d\n", (int)DPU_ID);
    // storage_init();
}

void exec_dpu_init_task(int lft, int rt) {
    (void)lft; (void)rt;
    int tid = me();
    init_block_with_type(dpu_init_task, empty_task_reply);
    if (tid == 0) {
        init_task_reader(0, 0);
        dpu_init_task* it = (dpu_init_task*)get_task_cached(0, 0);
        init(it);
    }
}

void execute(int lft, int rt) {
    uint32_t tid = me();
    switch (recv_block_task_type) {
        case dpu_init_task_id: {
            exec_dpu_init_task(lft, rt);
            break;
        }
        default: {
            printf("TT = %lld\n", recv_block_task_type);
            IN_DPU_ASSERT(false, "WTT\n");
            break;
        }
    }
    finish_reply(recv_block_task_cnt, tid);
}

int main() {
    run();
    return 0;
}
