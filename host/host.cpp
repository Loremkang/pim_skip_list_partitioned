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

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdbool>
#include <iostream>

#include "task.hpp"
#include "task_framework_host.hpp"
#include "driver.hpp"

#ifndef DPU_BINARY
#define DPU_BINARY "build/fast_skip_list_dpu"
#endif

// #define ANSI_COLOR_RED "\x1b[31m"
// #define ANSI_COLOR_GREEN "\x1b[32m"
// #define ANSI_COLOR_RESET "\x1b[0m"
using namespace std;

void init_dpus() {
    printf("\n********** INIT DPUS **********\n");
    auto io = alloc_io_manager();
    io->init();
    IO_Task_Batch* batch = io->alloc<dpu_init_task, empty_task_reply>(direct);

    parlay::parallel_for(0, nr_of_dpus, [&](size_t i) {
        auto it = (dpu_init_task*)batch->push_task_zero_copy(i, -1, false);
        *it = (dpu_init_task){.dpu_id = (int64_t)i};
    });
    io->finish_task_batch();
    ASSERT(!io->exec());
}

int pim_skip_list_debug() {
    rn_gen::init();
    pim_skip_list::init();
    {
        const int T = 5;
        const int n = 1e6;
        auto kvs = parlay::tabulate(T * n, [&](int64_t i) {
            return (key_value){.key = rn_gen::parallel_rand(),
                               .value = rn_gen::parallel_rand()};
        });
        for (int i = 0; i < T; i++) {
            pim_skip_list::insert(make_slice(kvs).cut(i * n, (i + 1) * n));
        }
        
        for (int i = 0; i < 1; i++) {
            auto valid = parlay::tabulate(n * T, [&](size_t i) -> bool {
                return (rn_gen::parallel_rand() % T) == 0;
            });
            auto keys =
                parlay::tabulate(n * T, [&](size_t i) { return kvs[i].key; });
            auto to_remove = parlay::pack(keys, valid);
            pim_skip_list::remove(make_slice(to_remove));
        }

        for (int i = 0; i < 1; i++) {
            auto keys =
                parlay::tabulate(n, [&](size_t i) { return rn_gen::parallel_rand(); });
            auto result = pim_skip_list::predecessor(make_slice(keys));
        }
    }
    return 0;
}

/**
 * @brief Main of the Host Application.
 */
int main(int argc, char* argv[]) {
    init_io_managers();
    driver::init();
    dpu_control::alloc(DPU_ALLOCATE_ALL);
    dpu_control::load(DPU_BINARY);
    init_dpus();

    // return pim_skip_list_debug();

    // dpu_control::print_all_log();
    driver::exec(argc, argv);
    pim_skip_list::dpu_energy_stats();

    dpu_control::free();
    return 0;
}
