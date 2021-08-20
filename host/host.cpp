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

#define __mram_ptr 

bool print_debug = false;

extern "C" {
    #include <dpu.h>
}

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdbool>
#include <iostream>
#include <set>
#include <parlay/parallel.h>
#include <atomic>
#include "task_host.hpp"

#include "host.hpp"
#include "test_framework.hpp"
#include "operations.hpp"
#include "timer.hpp"


#ifndef DPU_BINARY
#define DPU_BINARY "build/fast_skip_list_dpu"
#endif

// #define ANSI_COLOR_RED "\x1b[31m"
// #define ANSI_COLOR_GREEN "\x1b[32m"
// #define ANSI_COLOR_RESET "\x1b[0m"
using namespace std;

dpu_set_t dpu_set;
struct dpu_set_t dpu;
uint32_t each_dpu;
int nr_of_dpus;
int64_t epoch_number;

int64_t op_keys[BATCH_SIZE];
int64_t op_results[BATCH_SIZE];
int8_t op_heights[BATCH_SIZE];

void init_rand() {
    srand(time(NULL));
    // srand(147);
}

void init_dpus() {
    init_io_buffer(false);
    set_io_buffer_type(INIT_TSK, EMPTY);
    for (int i = 0; i < nr_of_dpus; i ++) {
        init_task it = (init_task){.id = i};
        push_task(&it, sizeof(init_task), 0, i);
    }
    ASSERT(!exec());
}

/**
 * @brief Main of the Host Application.
 */
int main() {
    init_rand();

    timer init_timer("init");

    init_timer.start();
    DPU_ASSERT(dpu_alloc(DPU_ALLOCATE_ALL, "backend=simulator", &dpu_set));
    DPU_ASSERT(dpu_load(dpu_set, DPU_BINARY, NULL));

    DPU_ASSERT(dpu_get_nr_dpus(dpu_set, (uint32_t *)&nr_of_dpus));
    printf("Allocated %d DPU(s)\n", nr_of_dpus);

    init_dpus();

    init_skiplist(14);
    init_test_framework();
    init_timer.end();

    turnon_all_timers(false);

    int BATCH_SIZE_PER_DPU = 500;
    for (int i = 0; i < 25; i ++) {
        insert_test(BATCH_SIZE_PER_DPU, true);
    }

    turnon_all_timers(true);

    for (int i = 0; i < 50; i ++) {
        // turnon_all_timers(true);
        insert_test(BATCH_SIZE_PER_DPU, true);
        // turnon_all_timers(false);
        assert(predecessor_test(BATCH_SIZE_PER_DPU * MAX_DPU, true));
        remove_test(BATCH_SIZE_PER_DPU, true);
    }
    print_all_timers();
    // init_timer.print();
    // insert_timer.print();
    // predecessor_timer.print();
    // remove_timer.print();
    // L3_sancheck();
    DPU_ASSERT(dpu_free(dpu_set));
    return 0;
}
