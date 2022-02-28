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

void *bufferA_shared, *bufferB_shared;
int8_t *max_height_shared;
uint32_t *newnode_size;

#ifdef DPU_ENERGY
__host uint64_t op_count = 0;
__host uint64_t db_size_count = 0;
__host uint64_t cycle_count = 0;
#endif

static inline void dpu_init(dpu_init_task *it) {
    DPU_ID = it->dpu_id;
    l3cnt = 8;
    storage_init();
    // printf("id=%d\n", (int)DPU_ID);
    // printf("size=%d\n", sizeof(L3node));
    // storage_init();
}

void exec_dpu_init_task(int lft, int rt) {
    (void)lft; (void)rt;
    int tid = me();
    init_block_with_type(dpu_init_task, empty_task_reply);
    if (tid == 0) {
        init_task_reader(0);
        dpu_init_task* it = (dpu_init_task*)get_task_cached(0);
        dpu_init(it);
    }
}

void exec_L3_init_task(int lft, int rt) {
    (void)lft; (void)rt;
    int tid = me();
    init_block_with_type(L3_init_task, empty_task_reply);
    if (tid == 0) {
        init_task_reader(0);
        L3_init_task* tit = (L3_init_task*)get_task_cached(0);
        L3_init(tit->key, tit->value, tit->height);
    }
}

void exec_L3_get_task(int lft, int rt) {
    init_block_with_type(L3_get_task, L3_get_reply);
    init_task_reader(lft);
    for (int i = lft; i < rt; i ++) {
        L3_get_task* tgt = (L3_get_task*)get_task_cached(i);
        int64_t val = INT64_MIN;
        bool exist = L3_get(tgt->key, &val);
        L3_get_reply tgr = (L3_get_reply){.valid = exist, .value = val};
        push_fixed_reply(i, &tgr);
    }
}

void exec_L3_search_task(int lft, int rt) {
    init_block_with_type(L3_search_task, L3_search_reply);
    init_task_reader(lft);
    for (int i = lft; i < rt; i ++) {
        L3_search_task* tst = (L3_search_task*)get_task_cached(i);
        int64_t val = INT64_MIN;
        int64_t key = L3_search(tst->key, 0, NULL, &val);
        L3_search_reply tsr = (L3_search_reply){.key = key, .value = val};
        push_fixed_reply(i, &tsr);
    }
}

void exec_L3_insert_task(int lft, int rt) {
    init_block_with_type(L3_insert_task, empty_task_reply);

    int n = rt - lft;
    int tid = me();

    __mram_ptr L3_insert_task* mram_tit =
        (__mram_ptr L3_insert_task*)recv_block_tasks;
    mram_tit += lft;

    newnode_size[tid] = 0;
    for (int i = 0; i < n; i++) {
        int height = mram_tit[i].height;
        newnode_size[tid] += L3_node_size(height);
        IN_DPU_ASSERT(height > 0 && height < MAX_L3_HEIGHT,
                        "execute: invalid height\n");
    }

    mL3ptr* right_predecessor_shared = bufferA_shared;
    mL3ptr* right_newnode_shared = bufferB_shared;
    L3_insert_parallel(n, lft, mram_tit, newnode_size, max_height_shared,
                        right_predecessor_shared, right_newnode_shared);
}

void exec_L3_remove_task(int lft, int rt) {
    init_block_with_type(L3_remove_task, empty_task_reply);

    int n = rt - lft;

    __mram_ptr L3_remove_task* mram_trt =
        (__mram_ptr L3_remove_task*)recv_block_tasks;
    mram_trt += lft;

    mL3ptr* left_node_shared = bufferA_shared;
    L3_remove_parallel(n, lft, mram_trt, max_height_shared,
                        left_node_shared);
}

void exec_L3_get_min_task(int lft, int rt) {
    (void)lft; (void)rt;
    init_block_with_type(L3_get_min_task, L3_get_min_reply);
    pptr r_first = root->right[0];
    mL3ptr nn = (mL3ptr)r_first.addr;
    L3_get_min_reply tgmr = (L3_get_min_reply){.key = nn->key};
    push_fixed_reply(0, &tgmr);
}

// Range Scan
void exec_L3_scan_task(int lft, int rt) {
    uint32_t tasklet_id = me();
    init_block_with_type(L3_scan_task, L3_scan_reply);
    init_task_reader(lft);
    varlen_buffer *key_buf, *val_buf;
    key_buf = varlen_buffer_new(VARLEN_BUFFER_SIZE, mrambuffer);
    val_buf = varlen_buffer_new(VARLEN_BUFFER_SIZE, mrambuffer + (M_BUFFER_SIZE >> 1));
    int64_t len;
    L3_scan_task* tsct;
    mpint64_t rep;
    for (int i = lft; i < rt; i ++) {
        tsct = (L3_scan_task*)get_task_cached(i);
        len = L3_scan(tsct->lkey, tsct->rkey, key_buf, val_buf);
        IN_DPU_ASSERT((len == key_buf->len && len == val_buf->len), "ScanErr\n");
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
    bufferA_shared = mem_alloc(sizeof(mL3ptr) * NR_TASKLETS * MAX_L3_HEIGHT);
    bufferB_shared = mem_alloc(sizeof(mL3ptr) * NR_TASKLETS * MAX_L3_HEIGHT);
    max_height_shared = mem_alloc(sizeof(int8_t) * NR_TASKLETS);
    newnode_size = mem_alloc(sizeof(uint32_t) * NR_TASKLETS);
    for (int i = 0; i < NR_TASKLETS; i++) {
        max_height_shared[i] = 0;
    }
}

int main() {
    run();
    return 0;
}
