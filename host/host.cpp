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

void init_rand() {
    srand(time(NULL));
    // srand(147);
}

/**
 * @brief Main of the Host Application.
 */
int main() {
    init_rand();

    DPU_ASSERT(dpu_alloc(NR_DPUS, "backend=simulator", &dpu_set));
    DPU_ASSERT(dpu_load(dpu_set, DPU_BINARY, NULL));

    DPU_ASSERT(dpu_get_nr_dpus(dpu_set, (uint32_t *)&nr_of_dpus));
    printf("Allocated %d DPU(s)\n", nr_of_dpus);

    DPU_FOREACH(dpu_set, dpu, each_dpu) {
        uint64_t id = each_dpu;
        dpu_copy_to(dpu, XSTR(DPU_ID), 0, &id, sizeof(uint64_t));
    }

    init_skiplist(MAX_L3_HEIGHT);
    init_test_framework();
    // insert_test(100);
    for (int i = 0; i < 5; i ++) {
        insert_test(1000);
    }
    for (int i = 0; i < 10; i ++) {
        insert_test(1000);
        assert(predecessor_test(1000));
        remove_test(1000);
    }
    // remove_test(1000);
    // assert(get_test(100));
    // assert(predecessor_test(100));
    
    L3_sancheck();
    return 0;
    for (int i = 0; i < 10; i ++) {
        insert_test(1000);
        assert(predecessor_test(1000));
    }
    
    return 0;

    insert_test(10000);
    for (int i = 0; i < 1000; i ++) {
        insert_test(500);
        assert(predecessor_test(100));

        remove_test(500);
    }
    
    assert(predecessor_test(10));

    return 0;
    for (int i = 0; i < 100; i++) {
        insert_test(50);
        assert(predecessor_test(50));
    }

    DPU_ASSERT(dpu_free(dpu_set));
    return 0;
}
