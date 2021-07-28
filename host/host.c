/*
 * Copyright (c) 2014-2019 - UPMEM
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
 * @file host.c
 * @brief Template for a Host Application Source File.
 */

#include <dpu.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

#include "common.h"

#ifndef DPU_BINARY
#define DPU_BINARY "build/task_dpu"
#endif

#define ANSI_COLOR_RED "\x1b[31m"
#define ANSI_COLOR_GREEN "\x1b[32m"
#define ANSI_COLOR_RESET "\x1b[0m"

static task taskinit[BUFFER_SIZE];

static task send_buffer[MAX_DPU][MAX_TASK_PER_DPU];
static uint64_t send_buffer_cnt[MAX_DPU];

static task receive_buffer[MAX_DPU][MAX_TASK_PER_DPU];
static uint64_t receive_buffer_cnt[MAX_DPU];

uint32_t each_dpu;
struct dpu_set_t dpu;

void swap(int *a, int *b) {
    int t = *a;
    *a = *b;
    *b = t;
}

uint64_t max(uint64_t a, uint64_t b) {
    return (a < b) ? b : a;
}

void print_task(task t) {
    printf("******\nTask: \n");
    printf("Finished: ");
    for (int i = 0; i < NR_DPUS; i ++) {printf("%d", t.finished[i] ? 1 : 0);}
    printf("\n");
    printf("Nxt: %d; End: %d \n", t.nxt, t.end);
    printf("Order: ");
    for (int i = 0; i < NR_DPUS; i ++) {printf("%d ", t.order[i]);}
    printf("\n\n");
}

void init_send_buffer() {
    memset(send_buffer_cnt, 0, sizeof(send_buffer_cnt));
    memset(send_buffer, 0, sizeof(send_buffer));
}

void redirect_task(task *arr, int len) {
    for (int i = 0; i < len; i ++) {
        task* t = arr + i;
        ASSERT(t->nxt < NR_DPUS);
        int target = t->order[t->nxt];
        send_buffer[target][send_buffer_cnt[target] ++] = *t;
    }
}

void send_task(struct dpu_set_t dpu_set) {
    uint64_t maxlen = 0;
    for (int i = 0; i < NR_DPUS; i ++) {
        maxlen = max(maxlen, send_buffer_cnt[i]);
    }

    printf("max task: %lu\n", maxlen);

    DPU_FOREACH(dpu_set, dpu, each_dpu) {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &send_buffer[each_dpu][0]));
    }
    DPU_ASSERT(dpu_push_xfer(dpu_set, DPU_XFER_TO_DPU, XSTR(DPU_RECEIVE_BUFFER), 0, sizeof(task) * maxlen, DPU_XFER_ASYNC));

    DPU_FOREACH(dpu_set, dpu, each_dpu) {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &send_buffer_cnt[each_dpu]));
    }
    DPU_ASSERT(dpu_push_xfer(dpu_set, DPU_XFER_TO_DPU, XSTR(DPU_RECEIVE_BUFFER_CNT), 0, sizeof(uint64_t), DPU_XFER_ASYNC));

    DPU_ASSERT(dpu_sync(dpu_set));

    // uint64_t killed = 0;
    // DPU_ASSERT(dpu_broadcast_to(dpu_set, XSTR(DPU_KILLED), 0, &killed, sizeof(uint64_t), DPU_XFER_DEFAULT));

    // uint64_t set_state = (1 << NR_TASKLETS) - 1;
    
    // DPU_ASSERT(dpu_broadcast_to(dpu_set, XSTR(DPU_STATE), 0, &set_state, sizeof(uint64_t), DPU_XFER_DEFAULT));

}

void receive_task(struct dpu_set_t dpu_set) {
    // memset(send_buffer_cnt, 0, sizeof(send_buffer_cnt));


    DPU_FOREACH(dpu_set, dpu, each_dpu) {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &receive_buffer_cnt[each_dpu]));
    }
    DPU_ASSERT(dpu_push_xfer(dpu_set, DPU_XFER_FROM_DPU, XSTR(DPU_SEND_BUFFER_CNT), 0, sizeof(uint64_t), DPU_XFER_DEFAULT));

    uint64_t maxlen = 0;
    for (int i = 0; i < NR_DPUS; i ++) {
        maxlen = max(maxlen, receive_buffer_cnt[i]);
    }
    printf("max task: %lu\n", maxlen);

    DPU_FOREACH(dpu_set, dpu, each_dpu) {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &receive_buffer[each_dpu][0]));
    }
    DPU_ASSERT(dpu_push_xfer(dpu_set, DPU_XFER_FROM_DPU, XSTR(DPU_SEND_BUFFER), 0, sizeof(task) * maxlen, DPU_XFER_DEFAULT));
}

void kill_dpus(struct dpu_set_t dpu_set) {
    uint64_t killed = 1;
    printf("killing\n");
    DPU_ASSERT(dpu_broadcast_to(dpu_set, XSTR(DPU_KILLED), 0, &killed, sizeof(uint64_t), DPU_XFER_ASYNC));
    printf("killed\n");
}

/**
 * @brief creates a "test file"
 *
 * @return the theorical checksum value
 */
void create_tasks()
{
    srand(time(NULL));

    for (unsigned int i = 0; i < BUFFER_SIZE; i++) {
        memset(taskinit[i].finished, 0, sizeof(taskinit[i].finished));
        taskinit[i].nxt = 0; taskinit[i].end = NR_DPUS;
        for (int j = 0; j < NR_DPUS; j ++) {
            taskinit[i].order[j] = j;
        }
        for (int j = 0; j < NR_DPUS; j ++) {
            swap(&taskinit[i].order[j], &taskinit[i].order[j + rand() % (NR_DPUS - j)]);
        }
    }
}

/**
 * @brief Main of the Host Application.
 */
int main()
{
    struct dpu_set_t dpu_set, dpu;
    uint32_t nr_of_dpus;
    // uint32_t theoretical_checksum, dpu_checksum;
    // uint32_t dpu_cycles;
    bool status = true;

    DPU_ASSERT(dpu_alloc(NR_DPUS, "backend=simulator", &dpu_set));
    DPU_ASSERT(dpu_load(dpu_set, DPU_BINARY, NULL));

    DPU_ASSERT(dpu_get_nr_dpus(dpu_set, &nr_of_dpus));
    printf("Allocated %d DPU(s)\n", nr_of_dpus);

    DPU_FOREACH(dpu_set, dpu, each_dpu) {
        uint64_t id = each_dpu;
        dpu_copy_to(dpu, XSTR(DPU_ID), 0, &id, sizeof(uint64_t));
    }

    // Create an "input file" with arbitrary data.
    // Compute its theoretical checksum value.
    // theoretical_checksum = 
    create_tasks();
    for (int i = 0; i < BUFFER_SIZE; i ++) {
        // print_task(taskinit[i]);
    }

    init_send_buffer();

    redirect_task(taskinit, BUFFER_SIZE);

    for (int i = 0; i < NR_DPUS; i++) {
        send_task(dpu_set);

        printf("Run program on DPU(s)\n");
        DPU_ASSERT(dpu_launch(dpu_set, DPU_SYNCHRONOUS));

        receive_task(dpu_set);

        if (i == NR_DPUS - 1) {
            break;
        }

        init_send_buffer();
        for (int j = 0; j < NR_DPUS; j ++) {
            redirect_task(receive_buffer[j], receive_buffer_cnt[j]);
        }
    }

    for (int i = 0; i < NR_DPUS; i ++) {
        for (int j = 0; j < receive_buffer_cnt[i]; j ++) {
            print_task(receive_buffer[i][j]);
        }
    }


    // DPU_ASSERT(dpu_sync(dpu_set));
    DPU_FOREACH (dpu_set, dpu) {
        DPU_ASSERT(dpu_log_read(dpu, stdout));
    }

    // printf("Retrieve results\n");
    // dpu_results_t results[nr_of_dpus];
    // uint32_t each_dpu;
    // DPU_FOREACH (dpu_set, dpu, each_dpu) {
    //     DPU_ASSERT(dpu_prepare_xfer(dpu, &results[each_dpu]));
    // }
    // DPU_ASSERT(dpu_push_xfer(dpu_set, DPU_XFER_FROM_DPU, XSTR(DPU_RESULTS), 0, sizeof(dpu_results_t), DPU_XFER_DEFAULT));

    // DPU_FOREACH (dpu_set, dpu, each_dpu) {
    //     bool dpu_status;
    //     dpu_checksum = 0;
    //     dpu_cycles = 0;

    //     // Retrieve tasklet results and compute the final checksum.
    //     for (unsigned int each_tasklet = 0; each_tasklet < NR_TASKLETS; each_tasklet++) {
    //         dpu_result_t *result = &results[each_dpu].tasklet_result[each_tasklet];

    //         dpu_checksum += result->checksum;
    //         if (result->cycles > dpu_cycles)
    //             dpu_cycles = result->cycles;
    //     }

    //     dpu_status = (dpu_checksum == theoretical_checksum);
    //     status = status && dpu_status;

    //     printf("DPU execution time  = %g cycles\n", (double)dpu_cycles);
    //     printf("performance         = %g cycles/byte\n", (double)dpu_cycles / BUFFER_SIZE);
    //     printf("checksum computed by the DPU = 0x%08x\n", dpu_checksum);
    //     printf("actual checksum value        = 0x%08x\n", theoretical_checksum);
    //     if (dpu_status) {
    //         printf("[" ANSI_COLOR_GREEN "OK" ANSI_COLOR_RESET "] checksums are equal\n");
    //     } else {
    //         printf("[" ANSI_COLOR_RED "ERROR" ANSI_COLOR_RESET "] checksums differ!\n");
    //     }
    // }

    DPU_ASSERT(dpu_free(dpu_set));
    return status ? 0 : -1;
}
