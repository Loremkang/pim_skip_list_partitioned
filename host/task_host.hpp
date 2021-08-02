#pragma once

#include <dpu.h>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cassert>
#include <cstdbool>
#include <algorithm>
#include <mutex>
#include "common.h"
using namespace std;

extern int nr_of_dpus;
extern dpu_set_t dpu_set, dpu;
extern uint32_t each_dpu;

static uint8_t send_buffer[MAX_DPU][MAX_TASK_BUFFER_SIZE_PER_DPU];
static uint64_t send_buffer_offset[MAX_DPU][MAX_TASK_COUNT_PER_DPU];
static uint64_t send_buffer_size[MAX_DPU];
static uint64_t send_buffer_task_count[MAX_DPU];
static mutex send_buffer_mutex[MAX_DPU];

static uint8_t receive_buffer[MAX_DPU][MAX_TASK_BUFFER_SIZE_PER_DPU];
static uint64_t receive_buffer_offset[MAX_DPU][MAX_TASK_COUNT_PER_DPU];
static uint64_t receive_buffer_size[MAX_DPU];
static uint64_t receive_buffer_task_count[MAX_DPU];

// static uint8_t task_buffer[MAX_DPU][MAX_TASK_SIZE];

inline void init_send_buffer() {
    memset(send_buffer_size, 0, sizeof(send_buffer_size));
    memset(send_buffer_task_count, 0, sizeof(send_buffer_task_count));
}

inline void push_task(int nodeid, uint64_t type, void* buffer, size_t length) {
    if (nodeid == -1) {
        for (int target = 0; target < nr_of_dpus; target++) {
            push_task(target, type, buffer, length);
        }
    } else {
        send_buffer_mutex[nodeid].lock();
        assert(send_buffer_size[nodeid] + length + sizeof(uint64_t) <=
               MAX_TASK_BUFFER_SIZE_PER_DPU);
        assert(send_buffer_task_count[nodeid] + 1 <= MAX_TASK_COUNT_PER_DPU);
        uint8_t* pos = &send_buffer[nodeid][0] + send_buffer_size[nodeid];
        send_buffer_offset[nodeid][send_buffer_task_count[nodeid]++] =
            send_buffer_size[nodeid];

        memcpy(pos, &type, sizeof(uint64_t));
        memcpy(pos + sizeof(uint64_t), buffer, length);
        send_buffer_size[nodeid] += length + sizeof(uint64_t);
        send_buffer_mutex[nodeid].unlock();
    }
}

template <class F>
inline void apply_to_all_reply(bool parallel, F f) {
    if (parallel) {
        parlay::parallel_for(0, nr_of_dpus, [&](size_t i) {
            for (int j = 0; j < (int)receive_buffer_task_count[i]; j++) {
                task* t = (task*)(&receive_buffer[i][0] +
                                  receive_buffer_offset[i][j]);
                f(t);
            }
        });
    } else {
        for (int i = 0; i < nr_of_dpus; i++) {
            for (int j = 0; j < (int)receive_buffer_task_count[i]; j++) {
                task* t = (task*)(&receive_buffer[i][0] +
                                  receive_buffer_offset[i][j]);
                f(t);
            }
        }
    }
}

inline bool send_task() {
    uint64_t maxsize = 0, maxcount = 0;
    for (int i = 0; i < nr_of_dpus; i++) {
        maxsize = max(maxsize, send_buffer_size[i]);
        maxcount = max(maxcount, send_buffer_task_count[i]);
    }

    if (maxsize == 0 || maxcount == 0) {
        ASSERT(maxsize == 0 && maxcount == 0);
        return false;
    }

    printf("Max Send: task count=%lu, size=%lu\n", maxcount, maxsize);

    DPU_FOREACH(dpu_set, dpu, each_dpu) {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &send_buffer[each_dpu][0]));
    }
    DPU_ASSERT(dpu_push_xfer(dpu_set, DPU_XFER_TO_DPU, XSTR(DPU_RECEIVE_BUFFER),
                             0, maxsize, DPU_XFER_ASYNC));

    DPU_FOREACH(dpu_set, dpu, each_dpu) {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &send_buffer_offset[each_dpu][0]));
    }
    DPU_ASSERT(dpu_push_xfer(dpu_set, DPU_XFER_TO_DPU,
                             XSTR(DPU_RECEIVE_BUFFER_OFFSET), 0,
                             sizeof(uint64_t) * maxcount, DPU_XFER_ASYNC));

    DPU_FOREACH(dpu_set, dpu, each_dpu) {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &send_buffer_size[each_dpu]));
    }
    DPU_ASSERT(dpu_push_xfer(dpu_set, DPU_XFER_TO_DPU,
                             XSTR(DPU_RECEIVE_BUFFER_SIZE), 0, sizeof(uint64_t),
                             DPU_XFER_ASYNC));

    DPU_FOREACH(dpu_set, dpu, each_dpu) {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &send_buffer_task_count[each_dpu]));
    }
    DPU_ASSERT(dpu_push_xfer(dpu_set, DPU_XFER_TO_DPU,
                             XSTR(DPU_RECEIVE_BUFFER_TASK_COUNT), 0,
                             sizeof(uint64_t), DPU_XFER_ASYNC));

    DPU_ASSERT(dpu_sync(dpu_set));

    return true;
}

inline bool receive_task() {
    DPU_FOREACH(dpu_set, dpu, each_dpu) {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &receive_buffer_size[each_dpu]));
    }
    DPU_ASSERT(dpu_push_xfer(dpu_set, DPU_XFER_FROM_DPU,
                             XSTR(DPU_SEND_BUFFER_SIZE), 0, sizeof(uint64_t),
                             DPU_XFER_ASYNC));

    DPU_FOREACH(dpu_set, dpu, each_dpu) {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &receive_buffer_task_count[each_dpu]));
    }
    DPU_ASSERT(dpu_push_xfer(dpu_set, DPU_XFER_FROM_DPU,
                             XSTR(DPU_SEND_BUFFER_TASK_COUNT), 0,
                             sizeof(uint64_t), DPU_XFER_ASYNC));

    DPU_ASSERT(dpu_sync(dpu_set));

    uint64_t maxsize = 0, maxcount = 0;
    for (int i = 0; i < nr_of_dpus; i++) {
        maxsize = max(maxsize, receive_buffer_size[i]);
        maxcount = max(maxcount, receive_buffer_task_count[i]);
    }

    if (maxsize == 0 || maxcount == 0) {
        ASSERT(maxsize == 0 && maxcount == 0);
        return false;
    }

    printf("Max Receive: task count=%lu, size=%lu\n", maxcount, maxsize);

    DPU_FOREACH(dpu_set, dpu, each_dpu) {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &receive_buffer[each_dpu][0]));
    }
    DPU_ASSERT(dpu_push_xfer(dpu_set, DPU_XFER_FROM_DPU, XSTR(DPU_SEND_BUFFER),
                             0, maxsize, DPU_XFER_ASYNC));

    DPU_FOREACH(dpu_set, dpu, each_dpu) {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &receive_buffer_offset[each_dpu][0]));
    }
    DPU_ASSERT(dpu_push_xfer(dpu_set, DPU_XFER_FROM_DPU,
                             XSTR(DPU_SEND_BUFFER_OFFSET), 0,
                             sizeof(uint64_t) * maxcount, DPU_XFER_ASYNC));

    DPU_ASSERT(dpu_sync(dpu_set));
    return true;
}

inline bool exec() {
    memset(receive_buffer_size, 0, sizeof(receive_buffer_size));
    memset(receive_buffer_task_count, 0, sizeof(receive_buffer_task_count));
    if (send_task()) {
        DPU_ASSERT(dpu_launch(dpu_set, DPU_SYNCHRONOUS));
        return receive_task();
    } else {
        return false;
    }
}

inline void print_log() {
    struct dpu_set_t dpu;
    DPU_FOREACH(dpu_set, dpu) { DPU_ASSERT(dpu_log_read(dpu, stdout)); break;}
}