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

extern "C" {
    #include <dpu.h>
}

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdbool>
#include <iostream>
#include <parlay/parallel.h>
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
uint32_t nr_of_dpus;

void init_skiplist(struct dpu_set_t dpu_set, uint32_t height) {
    
    init_send_buffer();     

    L3_insert_task tit = (L3_insert_task){.key = LLONG_MIN, .addr = null_pptr, .height = height - LOWER_PART_HEIGHT};
    push_task(-1, L3_INIT, &tit, sizeof(L3_insert_task));

    printf("INIT UPPER PART -INF\n");
    ASSERT(exec(dpu_set, nr_of_dpus));  // insert upper part -INF
    print_log(dpu_set);
    exit(-1);
}

/**
 * @brief Main of the Host Application.
 */
int main()
{
    struct dpu_set_t dpu_set, dpu;

    DPU_ASSERT(dpu_alloc(NR_DPUS, "backend=simulator", &dpu_set));
    DPU_ASSERT(dpu_load(dpu_set, DPU_BINARY, NULL));

    DPU_ASSERT(dpu_get_nr_dpus(dpu_set, &nr_of_dpus));
    printf("Allocated %d DPU(s)\n", nr_of_dpus);

    DPU_FOREACH(dpu_set, dpu, each_dpu) {
        uint64_t id = each_dpu;
        dpu_copy_to(dpu, XSTR(DPU_ID), 0, &id, sizeof(uint64_t));
    }
    
    init_skiplist(dpu_set, 20);

    DPU_ASSERT(dpu_free(dpu_set));
    return 0;
}
