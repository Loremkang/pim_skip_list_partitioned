#pragma once

#include <dpu.h>

#include <algorithm>
#include <cassert>
#include <cstdbool>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <atomic>

#include "common.h"
#include "timer.hpp"
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
static atomic<int64_t> send_buffer_offset[MAX_DPU];
static atomic<int64_t> send_buffer_count[MAX_DPU];

static int64_t receive_buffer_type[MAX_DPU];
static atomic<int64_t> receive_buffer_offset[MAX_DPU];
static atomic<int64_t> receive_buffer_count[MAX_DPU];

static uint8_t io_buffer[MAX_DPU]
                        [MAX_TASK_BUFFER_SIZE_PER_DPU];  // for broadcast, 0 as
                                                         // send, 1 as receive

inline void print_log(uint32_t position, bool show_all_dpu = false) {
    DPU_FOREACH(dpu_set, dpu, each_dpu) {
        if (show_all_dpu || each_dpu == position) {
            cout << "DPU ID = " << each_dpu << endl;
            DPU_ASSERT(dpu_log_read(dpu, stdout));
            if (!show_all_dpu) {
                break;
            }
        }
    }
}

inline void init_io_buffer(bool broadcast) {
    ASSERT(buffer_state == idle);
    if (broadcast) {
        buffer_state = send_broadcast;
        send_buffer_count[0] = 0;
        receive_buffer_count[1] = 0;
        send_buffer_offset[0] = sizeof(int64_t) * 3;
        receive_buffer_offset[1] = sizeof(int64_t);
    } else {
        buffer_state = send_direct;
        memset(send_buffer_count, 0, sizeof(send_buffer_count));
        memset(receive_buffer_count, 0, sizeof(receive_buffer_count));
        for (int i = 0; i < MAX_DPU; i++) {
            send_buffer_offset[i] = sizeof(int64_t) * 3;
            receive_buffer_offset[i] = sizeof(int64_t);
        }
    }
}

inline void set_io_buffer_type(int64_t type, int64_t reply_type) {
    ASSERT((buffer_state == send_direct) || (buffer_state == send_broadcast));
    if (buffer_state == send_broadcast) {
        send_buffer_type[0] = type;
        receive_buffer_type[1] = reply_type;
    } else {
        for (int i = 0; i < nr_of_dpus; i++) {
            send_buffer_type[i] = type;
            receive_buffer_type[i] = reply_type;
        }
    }
}

inline int64_t push_task(void* buffer, size_t length, size_t reply_length,
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

    int64_t offset = (send_buffer_offset[send_id] += length) - length;
    memcpy(io_buffer[send_id] + offset, buffer, length);
    send_buffer_count[send_id]++;
    if (receive_buffer_type[receive_id] != EMPTY) {
        receive_buffer_count[receive_id]++;
        receive_buffer_offset[receive_id] += reply_length;
    }
    // ASSERT(send_buffer_offset[send_id].is_lock_free());
    // ASSERT(send_buffer_count[send_id].is_lock_free());
    ASSERT(send_buffer_offset[send_id] < MAX_TASK_BUFFER_SIZE_PER_DPU);
    ASSERT(receive_buffer_offset[send_id] < MAX_TASK_BUFFER_SIZE_PER_DPU);
    return (offset - sizeof(int64_t) * 3) / length;
}

inline uint8_t* get_reply(int offset, int length,
                          int receive_id) {  // works only for fixed length
    ASSERT(buffer_state == receive_broadcast || buffer_state == receive_direct);
    if (buffer_state == receive_broadcast) {
        ASSERT(receive_id == -1);
        receive_id = 1;
    } else {
        ASSERT(receive_id >= 0 && receive_id < nr_of_dpus);
    }
    ASSERT((*(int64_t*)io_buffer[receive_id]) == 0);
    uint8_t* tasks = &io_buffer[receive_id][sizeof(int64_t)];
    return tasks + offset * length;
}

template <class F, class T>
inline void apply_to_all_reply(bool parallel, T t, F f) {
    (void)t;
    ASSERT(buffer_state == receive_broadcast || buffer_state == receive_direct);
    auto kernel = [&](size_t i) {
        int64_t task_count = *(int64_t*)(io_buffer[i]);
        ASSERT(task_count >= 0);
        if (task_count > 0) {
            cout<<"Variable length buffer"<<endl;
            // printf("Variable length buffer\n");
            int64_t* offsets = (int64_t*)(&io_buffer[i][sizeof(int64_t) * 2]);
            uint8_t* task_start =
                io_buffer[i] + sizeof(int64_t) * (task_count + 2);
            for (int j = 0; j < task_count; j++) {
                T* task = (T*)(task_start + offsets[j]);
                f(*task, i, j);
            }
        } else {
            T* tasks = (T*)(&io_buffer[i][sizeof(int64_t)]);
            for (int j = 0; j < receive_buffer_count[i]; j++) {
                f(tasks[j], i, j);
            }
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
//                     (task*)(&send_buffer[i][0] +
//                     send_buffer_offset[i][j]);
//                 f(t);
//             }
//         });
//     } else {
//         for (int i = 0; i < nr_of_dpus; i++) {
//             for (int j = 0; j < (int)send_buffer_task_count[i]; j++) {
//                 task* t =
//                     (task*)(&send_buffer[i][0] +
//                     send_buffer_offset[i][j]);
//                 f(t);
//             }
//         }
//     }
// }

inline bool send_task() {
    ASSERT((buffer_state == send_direct) || (buffer_state == send_broadcast));
    if (buffer_state == send_broadcast) {
        if (send_buffer_count[0] == 0) {
            cout << "Empty Broadcast Task" << endl;
            // ASSERT(false);
            buffer_state = idle;
            return false;
        }
        // cout<<"Broadcast Send: task size="<<send_buffer_offset[0].load()<<endl;
        // printf("Broadcast Send: task size=%lu\n", send_buffer_offset[0].load());
        memcpy(io_buffer[0], &epoch_number, sizeof(int64_t));
        memcpy(io_buffer[0] + sizeof(int64_t), &send_buffer_type[0],
               sizeof(int64_t));
        memcpy(io_buffer[0] + sizeof(int64_t) * 2, &send_buffer_count[0],
               sizeof(int64_t));
        DPU_ASSERT(dpu_broadcast_to(dpu_set, DPU_MRAM_HEAP_POINTER_NAME, 0,
                                    io_buffer[0], send_buffer_offset[0].load(),
                                    DPU_XFER_ASYNC));
    } else {
        int64_t max_size = 0;
        for (int i = 0; i < nr_of_dpus; i++) {
            max_size = max(max_size, send_buffer_offset[i].load());
            memcpy(io_buffer[i], &epoch_number, sizeof(int64_t));
            memcpy(io_buffer[i] + sizeof(int64_t), &send_buffer_type[i],
                   sizeof(int64_t));
            memcpy(io_buffer[i] + sizeof(int64_t) * 2, &send_buffer_count[i],
                   sizeof(int64_t));
        }
        // cout<<"Parallel Send: task size="<<max_size<<endl;
        // printf("Parallel Send: task size=%lu\n", max_size);
        if (max_size == sizeof(int64_t) * 3) {
            cout << "Empty Send Task" << endl;
            // ASSERT(false);
            buffer_state = idle;
            return false;
        }
        DPU_FOREACH(dpu_set, dpu, each_dpu) {
            DPU_ASSERT(dpu_prepare_xfer(dpu, &io_buffer[each_dpu][0]));
        }
        DPU_ASSERT(dpu_push_xfer(dpu_set, DPU_XFER_TO_DPU,
                                 DPU_MRAM_HEAP_POINTER_NAME, 0, max_size,
                                 DPU_XFER_ASYNC));
    }
    return true;
}

inline bool receive_task() {
    ASSERT((buffer_state == send_direct) || (buffer_state == send_broadcast));
    int64_t max_size = 0;
    if (buffer_state == send_broadcast) {
        max_size = receive_buffer_offset[1].load();
        // cout<<"Broadcast Receive: task size="<<max_size<<endl;
        // printf("Broadcast Receive: task size=%lu\n", max_size);
        if (receive_buffer_count[1] == 0) {
            DPU_ASSERT(dpu_sync(dpu_set));
            buffer_state = idle;
            return false;
        }
        DPU_FOREACH(dpu_set, dpu, each_dpu) {
            if (each_dpu == 0) {
                DPU_ASSERT(dpu_copy_from(dpu, DPU_MRAM_HEAP_POINTER_NAME,
                                         DPU_SEND_BUFFER_OFFSET, io_buffer[1],
                                         max_size));
            }
        }
    } else {
        for (int i = 0; i < nr_of_dpus; i++) {
            max_size = max(max_size, receive_buffer_offset[i].load());
        }
        // cout<<"Parallel Receive: task size="<<max_size<<endl;
        // printf("Parallel Receive: task size=%lu\n", max_size);
        if (max_size == sizeof(int64_t)) {
            DPU_ASSERT(dpu_sync(dpu_set));
            buffer_state = idle;
            return false;
        }
        DPU_FOREACH(dpu_set, dpu, each_dpu) {
            DPU_ASSERT(dpu_prepare_xfer(dpu, &io_buffer[each_dpu][0]));
        }
        DPU_ASSERT(dpu_push_xfer(
            dpu_set, DPU_XFER_FROM_DPU, DPU_MRAM_HEAP_POINTER_NAME,
            DPU_SEND_BUFFER_OFFSET, max_size, DPU_XFER_ASYNC));
    }
    DPU_ASSERT(dpu_sync(dpu_set));

    int more_fetching = 0;

    auto kernel = [&](size_t i) {
        int64_t task_count = *(int64_t*)io_buffer[i];
        if (task_count == -1) {  // error
            print_log(i);
            cout<<"Quit From DPU!"<<endl;
            exit(-1);
        } else if (task_count > 0) {  // variable length
            ASSERT(max_size > (int64_t)(2 * sizeof(int64_t)));
            int64_t task_size = *(int64_t*)(&io_buffer[sizeof(int64_t)]);
            int64_t actual_size =
                sizeof(int64_t) * (2 + task_count) + task_size;
            if (more_fetching < actual_size - max_size) {
                more_fetching = actual_size - max_size;
            }
        }
    };

    if (buffer_state == send_broadcast) {
        kernel(1);
        buffer_state = receive_broadcast;
        if (more_fetching > 0) {
            ASSERT(false);
        }
    } else {
        for (int i = 0; i < nr_of_dpus; i++) {
            kernel(i);
        }
        buffer_state = receive_direct;
        if (more_fetching > 0) {
            DPU_FOREACH(dpu_set, dpu, each_dpu) {
                DPU_ASSERT(
                    dpu_prepare_xfer(dpu, &io_buffer[each_dpu][max_size]));
            }
            DPU_ASSERT(dpu_push_xfer(dpu_set, DPU_XFER_FROM_DPU,
                                     DPU_MRAM_HEAP_POINTER_NAME,
                                     DPU_SEND_BUFFER_OFFSET + max_size,
                                     more_fetching, DPU_XFER_DEFAULT));
        }
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