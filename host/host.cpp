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
#include <set>
#include <parlay/parallel.h>
#include <atomic>
#include "task_host.hpp"
#include <libcuckoo/cuckoohash_map.hh>

#include "common.h"


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

set<int64_t> golden_L3;

int64_t op_keys[BATCH_SIZE];
int64_t op_results[BATCH_SIZE];

void init_rand() {
    srand(time(NULL));
    srand(147);
}

void init_skiplist(uint32_t height) {
    init_send_buffer();     
    L3_insert_task tit = (L3_insert_task){.key = LLONG_MIN, .addr = null_pptr, .height = height - LOWER_PART_HEIGHT};
    push_task(-1, L3_INIT, &tit, sizeof(L3_insert_task));

    printf("INIT UPPER PART -INF\n");
    ASSERT(exec());  // insert upper part -INF
    // print_log();
    // exit(-1);
}

void predecessor(int length) {
    printf("START PREDECESSOR\n");
    // sort(op_keys, op_keys + length);

    libcuckoo::cuckoohash_map<int64_t, int> key2offset;
    key2offset.reserve(length * 2);

    init_send_buffer();
    parlay::parallel_for(0, nr_of_dpus, [&](size_t i) {
        int l = length * i / nr_of_dpus;
        int r = length * (i + 1) / nr_of_dpus;
        for (int j = l; j < r; j++) {
            if (key2offset.contains(op_keys[j])) {
                continue;
            }
            L3_search_task tst = (L3_search_task){.key = op_keys[j]};
            push_task(i, L3_SEARCH, &tst, sizeof(L3_search_task));
            key2offset.insert(op_keys[j], j);
        }
    });
    exec();

    
    apply_to_all_reply(false, [&](task *t) {
        // assert(t->type == L3_SEARCH);
        L3_search_reply *tsr = (L3_search_reply*)t->buffer;
        int j = 0;
        assert(key2offset.find(tsr->key, j));
        op_results[j] = tsr->result_key;
        // printf("%ld %ld\n", tsr->key, tsr->result_key);
    });

    parlay::parallel_for(0, length, [&](size_t i) {
        int j = 0;
        assert(key2offset.find(op_keys[i], j));
        op_results[i] = op_results[j];
    });

    for (int i = 0; i < length; i ++) {
        printf("%ld %ld\n", op_keys[i], op_results[i]);
    }
}

bool predecessor_test(int length) {
    for (int i = 0; i < length; i++) {
        op_keys[i] = rand() % VALUE_LIMIT;
    }
    // sort(op_keys, op_keys + length);
    predecessor(length);
    // cout << "Predecessor: " << endl;
    // for (int i = 0; i < length; i++) {
    //     set<int64_t>::iterator it = golden_L3.upper_bound(op_keys[i]);
    //     it--;
    //     if (op_result[i] != *it) {
    //         cout << op_keys[i] << ' ' << op_result[i] << ' ' << *it << endl;
    //         return false;
    //     }
    // }
    // cout << endl;
    return true;
}

void insert(int length) {

}

void insert_test(int length) {
    cout << "\n*** Start Insert Test ***" << endl;
    for (int i = 0; i < length; i++) {
        op_keys[i] = rand() % VALUE_LIMIT;
        golden_L3.insert(op_keys[i]);
    }
    insert(length);
    cout << endl << "\n*** End Insert Test ***" << endl;
}

/**
 * @brief Main of the Host Application.
 */
int main()
{
    DPU_ASSERT(dpu_alloc(NR_DPUS, "backend=simulator", &dpu_set));
    DPU_ASSERT(dpu_load(dpu_set, DPU_BINARY, NULL));

    DPU_ASSERT(dpu_get_nr_dpus(dpu_set, (uint32_t*)&nr_of_dpus));
    printf("Allocated %d DPU(s)\n", nr_of_dpus);

    DPU_FOREACH(dpu_set, dpu, each_dpu) {
        uint64_t id = each_dpu;
        dpu_copy_to(dpu, XSTR(DPU_ID), 0, &id, sizeof(uint64_t));
    }
    
    init_skiplist(20);

    predecessor_test(20);

    DPU_ASSERT(dpu_free(dpu_set));
    return 0;
}
