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
#include "l3.h"
// #include "node_dpu.h"
#include "task_framework_dpu.h"

int *L3_lfts, *L3_rts, *L3_sizes;

#ifdef DPU_ENERGY
__host uint64_t op_count = 0;
__host uint64_t db_size_count = 0;
__host uint64_t cycle_count = 0;
#endif

static inline void dpu_init(dpu_init_task* it) {
    DPU_ID = it->dpu_id;
    bcnt = 1;
    storage_init();
}

void exec_dpu_init_task(int lft, int rt) {
    (void)lft;
    (void)rt;
    int tid = me();
    init_block_with_type(dpu_init_task, empty_task_reply);
    if (tid == 0) {
        init_task_reader(0);
        dpu_init_task* it = (dpu_init_task*)get_task_cached(0);
        dpu_init(it);
    }
}

void exec_L3_init_task(int lft, int rt) {
    (void)lft;
    (void)rt;
    int tid = me();
    init_block_with_type(L3_init_task, empty_task_reply);
    if (tid == 0) {
        init_task_reader(0);
        L3_init();
    }
}

void exec_L3_get_task(int lft, int rt) {
    (void)lft;
    (void)rt;
}

void exec_L3_search_task(int lft, int rt) {
    init_block_with_type(L3_search_task, L3_search_reply);
    init_task_reader(lft);
    int tid = me();
    for (int i = lft; i < rt; i++) {
        L3_search_task* tst = (L3_search_task*)get_task_cached(i);
        mBptr nn;
        int64_t val = INT64_MIN;
        int64_t key = b_search(tst->key, &nn, &val);
        L3_search_reply tsr = (L3_search_reply){.key = key, .value = val};
        push_fixed_reply(i, &tsr);
    }
}

void exec_L3_insert_task(int lft, int rt) {
    init_block_with_type(L3_insert_task, empty_task_reply);
    init_task_reader(lft);

    for (int i = lft; i < rt; i++) {
        L3_insert_task* tit = (L3_insert_task*)get_task_cached(i);
        mod_keys[i] = tit->key;
        mod_values[i] = I64_TO_PPTR(tit->value);
    }

    b_insert_parallel(recv_block_task_cnt, lft, rt);
}

void exec_L3_remove_task(int lft, int rt) {
    init_block_with_type(L3_remove_task, empty_task_reply);
    init_task_reader(lft);
    
    for (int i = lft; i < rt; i++) {
        L3_remove_task* trt = (L3_remove_task*)get_task_cached(i);
        mod_keys[i] = trt->key;
    }
    b_remove_parallel(recv_block_task_cnt, lft, rt);
}

void exec_L3_get_min_task(int lft, int rt) {
    (void)lft;
    (void)rt;
    init_block_with_type(L3_get_min_task, L3_get_min_reply);
    int64_t key = b_get_min_key();
    L3_get_min_reply tgmr = (L3_get_min_reply){.key = key};
    push_fixed_reply(0, &tgmr);
}

// Range Scan
void exec_L3_scan_task(int lft, int rt) {
    uint32_t tasklet_id = me();
    init_block_with_type(L3_scan_task, L3_scan_reply);
    init_task_reader(lft);
    varlen_buffer *key_buf, *val_buf;
    varlen_buffer *up_buf, *down_buf;
    up_buf = varlen_buffer_new(VARLEN_BUFFER_SIZE, mrambuffer);
    down_buf = varlen_buffer_new(VARLEN_BUFFER_SIZE, mrambuffer + (M_BUFFER_SIZE >> 2));
    key_buf = varlen_buffer_new(VARLEN_BUFFER_SIZE, mrambuffer + (M_BUFFER_SIZE >> 1));
    val_buf = varlen_buffer_new(VARLEN_BUFFER_SIZE, mrambuffer + (M_BUFFER_SIZE >> 2) + (M_BUFFER_SIZE >> 1));
    int64_t len;
    L3_scan_task* tsct;
    mpint64_t rep;
    for (int i = lft; i < rt; i ++) {
        tsct = (L3_scan_task*)get_task_cached(i);
        L3_scan(tsct->lkey, tsct->rkey, key_buf, val_buf, up_buf, down_buf);
        len = key_buf->len;
        rep = (mpint64_t)push_variable_reply_zero_copy(tasklet_id, S64((len << 1) + 1));
        rep[0] = len;
        varlen_buffer_to_mram(key_buf, rep + 1);
        varlen_buffer_to_mram(val_buf, rep + 1 + len);
    }
}

void execute(int lft, int rt) {
    uint32_t tid = me();
    switch (recv_block_task_type) {
        case dpu_init_task_id: {
            exec_dpu_init_task(lft, rt);
            break;
        }
        case L3_init_task_id: {
            exec_L3_init_task(lft, rt);
            break;
        }
        case L3_get_task_id: {
            exec_L3_get_task(lft, rt);
            break;
        }
        case L3_search_task_id: {
            exec_L3_search_task(lft, rt);
            break;
        }
        case L3_insert_task_id: {
            exec_L3_insert_task(lft, rt);
            break;
        }
        case L3_remove_task_id: {
            exec_L3_remove_task(lft, rt);
            break;
        }
        case L3_get_min_task_id: {
            exec_L3_get_min_task(lft, rt);
            break;
        }
        // Range Scan
        case L3_scan_task_id: {
            exec_L3_scan_task(lft, rt);
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

void init() {
    L3_lfts = mem_alloc(sizeof(int) * NR_TASKLETS);
    L3_rts = mem_alloc(sizeof(int) * NR_TASKLETS);
    L3_sizes = mem_alloc(sizeof(int) * NR_TASKLETS);
}

int main() {
    run();
    return 0;
}
