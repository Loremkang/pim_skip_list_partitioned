#pragma once

#include <dpu.h>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cassert>
#include <cstdbool>
#include <algorithm>
#include <mutex>
#include "timer.hpp"
#include "common.h"
using namespace std;

extern int nr_of_dpus;
extern dpu_set_t dpu_set, dpu;
extern uint32_t each_dpu;
extern int64_t epoch_number;

enum Buffer_State {
    idle,
    send_direct,
    send_broadcast,
    receive_direct,
    receive_broadcast
};
static Buffer_State buffer_state = idle;
static int64_t send_buffer_type[MAX_DPU];
static int64_t send_buffer_offset[MAX_DPU];
static int64_t send_buffer_count[MAX_DPU];

static int64_t receive_buffer_type[MAX_DPU];
static int64_t receive_buffer_offset[MAX_DPU];
static int64_t receive_buffer_count[MAX_DPU];

static uint8_t io_buffer[MAX_DPU][MAX_TASK_BUFFER_SIZE_PER_DPU]; // for broadcast, 0 as send, 1 as receive

inline void print_log(bool show_all_dpu = false) {
    DPU_FOREACH(dpu_set, dpu) {
        DPU_ASSERT(dpu_log_read(dpu, stdout));
        if (!show_all_dpu) {
            break;
        }
    }
}

inline void init_io_buffer(bool broadcast) {
    ASSERT(buffer_state == idle);
    if (broadcast) {
        buffer_state = send_broadcast;
        send_buffer_count[0] = 0;
        receive_buffer_count[1] = 0;
        send_buffer_offset[0] = 24;
        receive_buffer_offset[1] = 8;
    } else {
        buffer_state = send_direct;
        memset(send_buffer_count, 0, sizeof(send_buffer_count));
        memset(receive_buffer_count, 0, sizeof(receive_buffer_count));
        for (int i = 0; i < MAX_DPU; i++) {
            send_buffer_offset[i] = 24;
            receive_buffer_offset[i] = 8;
        }
    }
}

inline void set_io_buffer_type(int64_t type, int64_t reply_type) {
    ASSERT((buffer_state == send_direct) || (buffer_state == send_broadcast));
    if (buffer_state == send_broadcast) {
        send_buffer_type[0] = type;
        receive_buffer_type[1] = reply_type;
    } else {
        for (int i = 0; i < nr_of_dpus; i ++) {
            send_buffer_type[i] = type;
            receive_buffer_type[i] = reply_type;
        }
    }
}

inline void push_task(void* buffer, size_t length, size_t reply_length,
                      int send_id) {  // -1 for broadcast
    ASSERT((buffer_state == send_direct) || (buffer_state == send_broadcast));
    int receive_id;
    if (buffer_state == send_broadcast) {
        ASSERT(send_id == -1);
        send_id = 0;
        receive_id = 1;
    } else {
        ASSERT(send_id >= 0 && send_id < nr_of_dpus);
        receive_id = send_id;
    }
    memcpy(io_buffer[send_id] + send_buffer_offset[send_id], buffer, length);
    send_buffer_count[send_id]++;
    send_buffer_offset[send_id] += length;
    if (receive_buffer_type[receive_id] != EMPTY) {
        receive_buffer_count[receive_id]++;
        receive_buffer_offset[receive_id] += reply_length;
    }   
}

template <class F, class T>
inline void apply_to_all_reply(bool parallel, T t, F f) {
    assert(buffer_state == receive_broadcast || buffer_state == receive_direct);
    auto kernel = [&](size_t i) {
        T* tasks = (T*)(&io_buffer[i][8]);
        for (int j = 0; j < receive_buffer_count[i]; j++) {
            f(tasks[j], i, j);
        }
    };
    if (buffer_state == receive_broadcast) {
        kernel(1);
    } else {
        if (parallel) {
            parlay::parallel_for(0, nr_of_dpus, kernel);
        } else {
            for (size_t i = 0; i < (size_t)nr_of_dpus; i++) {
                kernel(i);
            }
        }
    }
    buffer_state = idle;
}

// template <class F, class T>
// inline void apply_to_all_request(bool parallel, F f, T t) {
//     if (parallel) {
//         parlay::parallel_for(0, nr_of_dpus, [&](size_t i) {
//             for (int j = 0; j < (int)send_buffer_task_count[i]; j++) {
//                 task* t =
//                     (task*)(&send_buffer[i][0] + send_buffer_offset[i][j]);
//                 f(t);
//             }
//         });
//     } else {
//         for (int i = 0; i < nr_of_dpus; i++) {
//             for (int j = 0; j < (int)send_buffer_task_count[i]; j++) {
//                 task* t =
//                     (task*)(&send_buffer[i][0] + send_buffer_offset[i][j]);
//                 f(t);
//             }
//         }
//     }
// }

inline bool send_task() {
    ASSERT((buffer_state == send_direct) || (buffer_state == send_broadcast));
    if (buffer_state == send_broadcast) {
        if (send_buffer_count[0] == 0) {
            cout<<"Empty Send Task"<<endl;
            ASSERT(false);
            // return false;
        }
        printf("Broadcast Send: task size=%lu\n", send_buffer_offset[0]);
        memcpy(io_buffer[0], &epoch_number, sizeof(int64_t));
        memcpy(io_buffer[0] + sizeof(int64_t), &send_buffer_type[0],
               sizeof(int64_t));
        memcpy(io_buffer[0] + sizeof(int64_t) * 2, &send_buffer_count[0],
               sizeof(int64_t));
        DPU_ASSERT(dpu_broadcast_to(dpu_set, DPU_MRAM_HEAP_POINTER_NAME, 0,
                                    io_buffer[0], send_buffer_offset[0],
                                    DPU_XFER_ASYNC));
    } else {
        int64_t maxsize = 0;
        for (int i = 0; i < nr_of_dpus; i++) {
            maxsize = max(maxsize, send_buffer_offset[i]);
            memcpy(io_buffer[i], &epoch_number, sizeof(int64_t));
            memcpy(io_buffer[i] + sizeof(int64_t), &send_buffer_type[i],
                   sizeof(int64_t));
            memcpy(io_buffer[i] + sizeof(int64_t) * 2, &send_buffer_count[i],
                   sizeof(int64_t));
        }
        printf("Parallel Send: task size=%lu\n", maxsize);
        if (maxsize == 24) {
            cout<<"Empty Send Task"<<endl;
            ASSERT(false);
            // return false;
        }
        DPU_FOREACH(dpu_set, dpu, each_dpu) {
            DPU_ASSERT(dpu_prepare_xfer(dpu, &io_buffer[each_dpu][0]));
        }
        DPU_ASSERT(dpu_push_xfer(dpu_set, DPU_XFER_TO_DPU,
                                 DPU_MRAM_HEAP_POINTER_NAME, 0, maxsize,
                                 DPU_XFER_ASYNC));
    }
    return true;
}

inline bool receive_task() {
    ASSERT((buffer_state == send_direct) || (buffer_state == send_broadcast));
    if (buffer_state == send_broadcast) {
        printf("Broadcast Receive: task size=%lu\n", receive_buffer_offset[1]);
        if (receive_buffer_count[1] == 0) {
            DPU_ASSERT(dpu_sync(dpu_set));
            buffer_state = idle;
            return false;
        }
        DPU_FOREACH(dpu_set, dpu, each_dpu) {
            if (each_dpu == 0) {
                DPU_ASSERT(dpu_copy_from(dpu, DPU_MRAM_HEAP_POINTER_NAME,
                                         MAX_TASK_BUFFER_SIZE_PER_DPU,
                                         io_buffer[1],
                                         receive_buffer_offset[1]));
            }
        }
    } else {
        int64_t maxsize = 0;
        for (int i = 0; i < nr_of_dpus; i++) {
            maxsize = max(maxsize, receive_buffer_offset[i]);
        }
        printf("Parallel Send: task size=%lu\n", maxsize);
        if (maxsize == 8) {
            DPU_ASSERT(dpu_sync(dpu_set));        
            buffer_state = idle;
            return false;
        }
        DPU_FOREACH(dpu_set, dpu, each_dpu) {
            DPU_ASSERT(dpu_prepare_xfer(dpu, &io_buffer[each_dpu][0]));
        }
        DPU_ASSERT(dpu_push_xfer(
            dpu_set, DPU_XFER_FROM_DPU, DPU_MRAM_HEAP_POINTER_NAME,
            MAX_TASK_BUFFER_SIZE_PER_DPU, maxsize, DPU_XFER_ASYNC));
    }
    DPU_ASSERT(dpu_sync(dpu_set));
    if (buffer_state == send_broadcast) {
        int64_t *err = (int64_t *)io_buffer[1];
        if (*err == -1) {
            print_log();
            printf("Quit From DPU!\n");
            exit(-1);
        }
    } else {
        for (int i = 0; i < nr_of_dpus; i++) {
            int64_t *err = (int64_t *)io_buffer[i];
            if (*err == -1) {
                print_log();
                printf("Quit From DPU!\n");
                exit(-1);
            }
        }
    }
    if (buffer_state == send_broadcast) {
        buffer_state = receive_broadcast;
    } else {
        buffer_state = receive_direct;
    }
    return true;
}

extern timer send_task_timer;
extern timer receive_task_timer;
extern timer execute_timer;
extern timer exec_timer;

inline bool exec() {
    // memset(receive_buffer_size, 0, sizeof(receive_buffer_size));
    // memset(receive_buffer_task_count, 0,
    // sizeof(receive_buffer_task_count));

    // exec_timer.start();
    // send_task_timer.start();
    if (send_task()) {
        // DPU_ASSERT(dpu_sync(dpu_set));
        // send_task_timer.end();
        // execute_timer.start();
        DPU_ASSERT(dpu_launch(dpu_set, DPU_ASYNCHRONOUS));
        // execute_timer.end();
        // receive_task_timer.start();
        bool ret = receive_task();
        // receive_task_timer.end();
        // exec_timer.end();
        return ret;
    } else {
        return false;
    }
}