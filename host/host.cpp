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
    ASSERT(io == &io_managers[0]);
    io->init();
    IO_Task_Batch* batch = io->alloc<dpu_init_task, empty_task_reply>(direct);

    parlay::parallel_for(0, nr_of_dpus, [&](size_t i) {
        auto it = (dpu_init_task*)batch->push_task_zero_copy(i, -1, false);
        *it = (dpu_init_task){.dpu_id = (int64_t)i};
    });
    io->finish_task_batch();
    ASSERT(!io->exec());
}

/**
 * @brief Main of the Host Application.
 */
int main(int argc, char* argv[]) {
    driver::init();
    dpu_control::alloc(DPU_ALLOCATE_ALL);
    dpu_control::load(DPU_BINARY);
    init_dpus();
    dpu_control::print_log([&](size_t i) {return true;});
    return 0;
    driver::exec(argc, argv);

    {
        // init_skiplist(19);
        // init_test_framework();
        // init_timer.end();

        // bool file_test = true;
        // if (file_test) {
        //     int actual_batch_size = 1000000;

        //     task* tasks;
        //     int64_t init_total_length;
        //     read_task_file(init_file_name, tasks, init_total_length);
        //     ASSERT(init_total_length == 8e8);

        //     int init_round = 800;
        //     execute(tasks, actual_batch_size, init_round);

        //     reset_all_timers();

        //     int64_t test_total_length;
        //     read_task_file(base_dir + "2insert.buffer", tasks,
        //     test_total_length);
        //     // read_task_file(base_dir + "insert.buffer", tasks,
        //     test_total_length); ASSERT(test_total_length == 2e8);

        //     total_io = 0;
        //     int test_round = 200;
        //     execute(tasks, actual_batch_size, test_round);
        //     cout<<"total io: "<<total_io<<endl;
        // } else {
        //     // turnon_all_timers(false);

        //     // int BATCH_SIZE_PER_DPU = 1000000 / MAX_DPU;

        //     // bool check_result = false;

        //     // for (int i = 0; i < 100; i++) {
        //     //     insert_test(BATCH_SIZE_PER_DPU * MAX_DPU, check_result);
        //     // }

        //     // turnon_all_timers(true);

        //     // for (int i = 0; i < 100; i++) {
        //     //     insert_test(BATCH_SIZE_PER_DPU * MAX_DPU, check_result);
        //     //     assert(
        //     //         predecessor_test(BATCH_SIZE_PER_DPU * MAX_DPU,
        //     check_result));
        //     // }
        // }
        // print_all_timers(pt_full);
        // print_all_timers(pt_succinct_time);
        // print_all_timers(pt_name);
        // // init_timer.print();
        // // insert_timer.print();
        // // predecessor_timer.print();
        // // remove_timer.print();
        // // L3_sancheck();
        // DPU_ASSERT(dpu_free(dpu_set));
        // return 0;
    }

    dpu_control::free();
    return 0;
}