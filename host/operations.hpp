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
    init_send_buffer();
    // maxheight = height - LOWER_PART_HEIGHT;
    maxheight = height;
    L3_insert_task tit = (L3_insert_task){.key = LLONG_MIN,
                                          .addr = null_pptr,
                                        //   .height = height - LOWER_PART_HEIGHT};
                                          .height = height};
    push_task(-1, L3_INIT, &tit, sizeof(L3_insert_task));

    // printf("INIT UPPER PART -INF\n");
    ASSERT(exec());  // insert upper part -INF
    // print_log();
}

void tick() {
    init_send_buffer();
    for (int i = 0; i < nr_of_dpus; i ++) {
        tick_task tt = (tick_task){.nothing = 0};
        push_task(i, TICK, &tt, sizeof(tick_task));
    }
    exec();
}

void get(int length) {
    epoch_number ++;
    // printf("START GET\n");

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
            L3_get_task tgt = (L3_get_task){.key = op_keys[j]};
            push_task_thread_unsafe(i, L3_GET, &tgt, sizeof(L3_get_task));
            key2offset.insert(op_keys[j], j);
        }
    });
    exec();


    apply_to_all_reply(false, [&](task *t) {
        L3_get_reply *tsr = (L3_get_reply *)t->buffer;
        int j = 0;
        assert(key2offset.find(tsr->key, j));
        op_results[j] = tsr->available;
        // printf("%ld %ld\n", tsr->key, tsr->result_key);
    });

    parlay::parallel_for(0, length, [&](size_t i) {
        int j = 0;
        assert(key2offset.find(op_keys[i], j));
        op_results[i] = op_results[j];
    });
}

timer predecessor_task_generate("predecessor_task_generate");
timer predecessor_get_result("predecessor_get_result");

void predecessor(int length) {
    epoch_number ++;
    // printf("START PREDECESSOR\n");

    predecessor_task_generate.start();

    init_send_buffer();
    parlay::parallel_for(0, nr_of_dpus, [&](size_t i) {
        int l = length * i / nr_of_dpus;
        int r = length * (i + 1) / nr_of_dpus;
        for (int j = l; j < r; j++) {
            L3_search_task tst = (L3_search_task){.key = op_keys[j], .offset = j};
            push_task(i, L3_SEARCH, &tst, sizeof(L3_search_task));
        }
    });
    predecessor_task_generate.end();

    exec();

    predecessor_get_result.start();

    apply_to_all_reply(true, [&](task *t) {
        L3_search_reply *tsr = (L3_search_reply *)t->buffer;
        int j = tsr->offset;
        op_results[j] = tsr->result_key;
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
    init_send_buffer();

    for (int i = 0; i < length; i++) {
        int h = 1;
        while (rand() & 1) {
            h++;
        }
        h = min(h, maxheight);
        op_heights[i] = h;
    }

    parlay::parallel_for(0, nr_of_dpus, [&](size_t target) {
        for (int i = 0; i < length; i++) {
            L3_insert_task tit = (L3_insert_task){
                .key = op_keys[i], .addr = null_pptr, .height = op_heights[i]};
            push_task_thread_unsafe(target, L3_INSERT, &tit,
                                    sizeof(L3_insert_task));
        }
    });

    // if (print_debug) {
    //     printf("upper insert %ld %d %d-%x\n", op_keys[i], h, tit.addr.id,
    //            tit.addr.addr);
    // }
    insert_task_generate.end();
    exec();
    // print_log();
}

timer remove_task_generate("remove_task_generate");

void remove(int length) {
    remove_task_generate.start();
    epoch_number++;
    deduplication(op_keys, length);
    // for (int i = 0; i < length; i ++) {
    //     cout<<op_keys[i]<<endl;
    // }
    // exit(-1);

    // remove upper part
    // printf("\n*** Remove: remove upper part\n");
    init_send_buffer();

    parlay::parallel_for(0, nr_of_dpus, [&](size_t target) {
        for (int i = 0; i < length; i++) {
            L3_remove_task trt = (L3_remove_task){.key = op_keys[i]};
            push_task_thread_unsafe(target, L3_REMOVE, &trt,
                                    sizeof(L3_remove_task));
        }
    });
    remove_task_generate.end();

    exec();
    // if (print_debug) {
    //     print_log();
    // }
    // exit(-1);
}
