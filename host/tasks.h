#pragma once

#include <iostream>
#include <cstddef>
#include "common.h"

struct get_task {
    int64_t key;
};

struct update_task {
    int64_t key;
    int64_t value;
};

struct predecessor_task {
    int64_t key;
};

struct scan_task {
    int64_t lkey;
    int64_t rkey;
};

struct insert_task {
    int64_t key;
    int64_t value;
};

struct remove_task {
    int64_t key;
};

enum task_t {
    empty_t,
    get_t,
    update_t,
    predecessor_t,
    scan_t,
    insert_t,
    remove_t
};

struct task {
    union {
        get_task g;
        update_task u;
        predecessor_task p;
        scan_task s;
        insert_task i;
        remove_task r;
    } tsk;
    task_t type;
};

// keep the order of these arrays
int64_t get_keys[BATCH_SIZE];
int64_t update_keys[BATCH_SIZE], update_values[BATCH_SIZE];
int64_t scan_keys[BATCH_SIZE];
int64_t predecessor_keys[BATCH_SIZE];
int64_t insert_keys[BATCH_SIZE], insert_values[BATCH_SIZE];
int64_t remove_keys[BATCH_SIZE];

int tasks_count[TASK_TYPE];