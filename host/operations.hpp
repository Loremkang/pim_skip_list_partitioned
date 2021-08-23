#pragma once

#include <iostream>
#include <cstdio>
#include <libcuckoo/cuckoohash_map.hh>
#include "task_host.hpp"
#include "timer.hpp"
#include "host.hpp"
using namespace std;

extern bool print_debug;
extern int64_t epoch_number;

int maxheight; // setting max height

void init_skiplist(uint32_t height) {
    epoch_number ++;
    maxheight = height;

    printf("\n********** INIT SKIP LIST **********\n");

    printf("\n**** INIT L2 ****\n");
    pptr l2node = null_pptr;
    {
        init_io_buffer(false);
        set_io_buffer_type(L2_INIT_TSK, L2_INIT_REP);
        L2_init_task sit = (L2_init_task){
            .key = LLONG_MIN, .addr = null_pptr, .height = height};
        int target = hash_to_dpu(LLONG_MIN, 0, nr_of_dpus);
        push_task(&sit, sizeof(L2_init_task), sizeof(L2_init_reply), target);
        ASSERT(exec());  // insert upper part -INF
        
        L2_init_reply _;
        apply_to_all_reply(false, _, [&](L2_init_reply &ssr, int i, int j) {
            (void)i;(void)j;
            ASSERT(buffer_state == receive_direct);
            ASSERT(l2node.id == INVALID_DPU_ID); // happen only once
            l2node = ssr.addr;
        });
    }

    pptr l3node = null_pptr;
    {
        init_io_buffer(true);
        set_io_buffer_type(L3_INIT_TSK, L3_INIT_REP);
        maxheight = height;
        L3_init_task tit = (L3_init_task){
            .key = LLONG_MIN, .addr = l2node, .height = height};
        push_task(&tit, sizeof(L3_init_task), sizeof(L3_init_reply), -1);
        ASSERT(exec());  // insert upper part -INF

        L3_init_reply _;
        apply_to_all_reply(false, _, [&](L3_init_reply &tsr, int i, int j) {
            (void)i;(void)j;
            ASSERT(buffer_state == receive_broadcast);
            ASSERT(l3node.id == INVALID_DPU_ID); // happen only once
            l3node = tsr.addr;
        });
    }

    {
        init_io_buffer(false);
        set_io_buffer_type(L2_BUILD_UP_TSK, EMPTY);
        uint32_t target = hash_to_dpu(LLONG_MIN, 0, nr_of_dpus);
        L2_build_up_task sbut = (L2_build_up_task){
            .addr = l2node, .up = (pptr){.id = target, .addr = l3node.addr}};
        push_task(&sbut, sizeof(L2_build_up_task), 0, target);
        ASSERT(!exec());  // insert upper part -INF
    }
}

void get(int length) {
    epoch_number++;
    // printf("START GET\n");

    libcuckoo::cuckoohash_map<int64_t, int> key2offset;
    key2offset.reserve(length * 2);

    init_io_buffer(false);
    set_io_buffer_type(L3_GET_TSK, L3_GET_REP);
    parlay::parallel_for(0, nr_of_dpus, [&](size_t i) {
        int l = length * i / nr_of_dpus;
        int r = length * (i + 1) / nr_of_dpus;
        for (int j = l; j < r; j++) {
            L3_get_task tgt = (L3_get_task){.key = op_keys[j]};
            push_task(&tgt, sizeof(L3_get_task), sizeof(L3_get_reply), i);
        }
    });
    ASSERT(exec());

    // apply_to_all_reply(false, [&](task *t) {
    //     L3_get_reply *tsr = (L3_get_reply *)t->buffer;
    //     int j = 0;
    //     assert(key2offset.find(tsr->key, j));
    //     op_results[j] = tsr->available;
    //     // printf("%ld %ld\n", tsr->key, tsr->result_key);
    // });

    // parlay::parallel_for(0, length, [&](size_t i) {
    //     int j = 0;
    //     assert(key2offset.find(op_keys[i], j));
    //     op_results[i] = op_results[j];
    // });
}

timer predecessor_task_generate("predecessor_task_generate");
timer predecessor_get_result("predecessor_get_result");

void predecessor(int length) {
    epoch_number++;
    // printf("START PREDECESSOR\n");

    predecessor_task_generate.start();

    init_io_buffer(false);
    set_io_buffer_type(L3_SEARCH_TSK, L3_SEARCH_REP);
    parlay::parallel_for(0, nr_of_dpus, [&](size_t i) {
        int l = (int64_t)length * i / nr_of_dpus;
        int r = (int64_t)length * (i + 1) / nr_of_dpus;
        for (int j = l; j < r; j++) {
            L3_search_task tst = (L3_search_task){.key = op_keys[j]};
            push_task(&tst, sizeof(L3_search_task), sizeof(L3_search_reply), i);
        }
    });
    predecessor_task_generate.end();

    ASSERT(exec());

    predecessor_get_result.start();

    // L3_search_reply tsr* = io_buffer[8];
    L3_search_reply _;
    apply_to_all_reply(true, _, [&](L3_search_reply &tsr, int i, int j) {
        ASSERT(buffer_state == receive_direct);
        int offset = (int64_t)length * i / nr_of_dpus + j;
        ASSERT(offset >= 0 &&
               offset < ((int64_t)length * (i + 1) / nr_of_dpus));
        op_results[offset] = tsr.result_key;
    });

    predecessor_get_result.end();

    // for (int i = 0; i < length; i ++) {
    //     printf("%ld %ld\n", op_keys[i], op_results[i]);
    // }
}

void deduplication(int64_t *arr, int &length) {  // assume sorted
    sort(arr, arr + length);
    int l = 1;
    for (int i = 1; i < length; i++) {
        if (arr[i] != arr[i - 1]) {
            arr[l++] = arr[i];
        }
    }
    length = l;
}

timer insert_task_generate("insert_task_generate");

void insert(int length) {
    insert_task_generate.start();
    epoch_number++;
    deduplication(op_keys, length);
    // printf("\n*** Insert: L3 insert\n");
    init_io_buffer(true);
    set_io_buffer_type(L3_INSERT_TSK, L3_INSERT_REP);

    for (int i = 0; i < length; i++) {
        int h = 1;
        while (rand() & 1) {
            h++;
        }
        h = min(h, maxheight);
        op_heights[i] = h;
    }

    for (int i = 0; i < length; i++) {
        L3_insert_task tit = (L3_insert_task){
            .key = op_keys[i], .addr = null_pptr, .height = op_heights[i]};
        push_task(&tit, sizeof(L3_insert_task), sizeof(L3_insert_reply), -1);
    }
    insert_task_generate.end();

    ASSERT(exec());
    buffer_state = idle;
}

timer remove_task_generate("remove_task_generate");

void remove(int length) {
    remove_task_generate.start();
    epoch_number++;
    deduplication(op_keys, length);
    init_io_buffer(true);
    set_io_buffer_type(L3_REMOVE_TSK, EMPTY);

    for (int i = 0; i < length; i++) {
        L3_remove_task trt = (L3_remove_task){.key = op_keys[i]};
        push_task(&trt, sizeof(L3_remove_task), 0, -1);
    }
    remove_task_generate.end();

    ASSERT(!exec());
    // buffer_state = idle;
    // if (print_debug) {
    //     print_log();
    // }
    // exit(-1);
}
