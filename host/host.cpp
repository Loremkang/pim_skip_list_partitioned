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

extern "C" {
    #include <dpu.h>
}

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdbool>
#include <iostream>
#include "task_host.hpp"

#include "common.h"

#ifndef DPU_BINARY
#define DPU_BINARY "build/fast_skip_list_dpu"
#endif

#define ANSI_COLOR_RED "\x1b[31m"
#define ANSI_COLOR_GREEN "\x1b[32m"
#define ANSI_COLOR_RESET "\x1b[0m"
using namespace std;

uint32_t each_dpu;
struct dpu_set_t dpu;

// void print_task(task t) {
//     printf("******\nTask: \n");
//     printf("Finished: ");
//     for (int i = 0; i < NR_DPUS; i ++) {printf("%d", t.finished[i] ? 1 : 0);}
//     printf("\n");
//     printf("Nxt: %d; End: %d \n", t.nxt, t.end);
//     printf("Order: ");
//     for (int i = 0; i < NR_DPUS; i ++) {printf("%d ", t.order[i]);}
//     printf("\n\n");
// }

// void redirect_task(task *arr, int len) {
//     for (int i = 0; i < len; i ++) {
//         task* t = arr + i;
//         ASSERT(t->nxt < NR_DPUS);
//         int target = t->order[t->nxt];
//         send_buffer[target][send_buffer_cnt[target] ++] = *t;
//     }
// }

/**
 * @brief Main of the Host Application.
 */
int main()
{
    struct dpu_set_t dpu_set, dpu;
    uint32_t nr_of_dpus;

    DPU_ASSERT(dpu_alloc(NR_DPUS, "backend=simulator", &dpu_set));
    DPU_ASSERT(dpu_load(dpu_set, DPU_BINARY, NULL));

    DPU_ASSERT(dpu_get_nr_dpus(dpu_set, &nr_of_dpus));
    printf("Allocated %d DPU(s)\n", nr_of_dpus);

    DPU_FOREACH(dpu_set, dpu, each_dpu) {
        uint64_t id = each_dpu;
        dpu_copy_to(dpu, XSTR(DPU_ID), 0, &id, sizeof(uint64_t));
    }

    init_send_buffer();
    for (int i = 0; i < (1 << 15); i ++) {
        int target = rand() % nr_of_dpus;
        int task_type = rand() % 2;
        if (task_type == 0) {
            twoval a;
            a.a[0] = i; a.a[1] = i + 1;
            push_task(target, task_type, &a, sizeof(twoval));
        } else if (task_type == 1) {
            threeval a;
            a.a[0] = i; a.a[1] = i + 2; a.a[2] = i + 4;
            push_task(target, task_type, &a, sizeof(threeval));
        }
    }
    exec(dpu_set, nr_of_dpus);
    print_log(dpu_set);

    // Create an "input file" with arbitrary data.
    // Compute its theoretical checksum value.
    // theoretical_checksum = 
    // create_tasks();
    // for (int i = 0; i < BUFFER_SIZE; i ++) {
    //     // print_task(taskinit[i]);
    // }

    // init_send_buffer();

    // redirect_task(taskinit, BUFFER_SIZE);

    // for (int i = 0; i < NR_DPUS; i++) {
    //     send_task(dpu_set);

    //     printf("Run program on DPU(s)\n");
    //     DPU_ASSERT(dpu_launch(dpu_set, DPU_SYNCHRONOUS));

    //     receive_task(dpu_set);

    //     if (i == NR_DPUS - 1) {
    //         break;
    //     }

    //     init_send_buffer();
    //     for (int j = 0; j < NR_DPUS; j ++) {
    //         redirect_task(receive_buffer[j], receive_buffer_cnt[j]);
    //     }
    // }

    // for (int i = 0; i < NR_DPUS; i ++) {
    //     for (int j = 0; j < receive_buffer_cnt[i]; j ++) {
    //         print_task(receive_buffer[i][j]);
    //     }
    // }


    // // DPU_ASSERT(dpu_sync(dpu_set));
    // DPU_FOREACH (dpu_set, dpu) {
    //     DPU_ASSERT(dpu_log_read(dpu, stdout));
    // }

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
    return 0;
}
