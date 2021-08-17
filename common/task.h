#pragma once

#include "basic.h"

#define EMPTY 0

#define L3_INIT_TSK 1
#define L3_INIT_REP 2

#define L3_INSERT_TSK 3
#define L3_INSERT_REP 4

#define L3_REMOVE_TSK 5

#define L3_SEARCH_TSK 7
#define L3_SEARCH_REP 8

#define L3_SANCHECK_TSK 9

#define L3_GET_TSK 10
#define L3_GET_REP 11

#define TICK 12

#include <stdint.h>
#include <assert.h>

typedef struct {
    int64_t key;
    pptr addr; // addr of the lower part root
    int64_t height;
} L3_insert_task;

typedef struct {
    pptr addr; // addr of the upper part node
} L3_insert_reply;

typedef L3_insert_task L3_init_task;
typedef L3_insert_reply L3_init_reply;

typedef struct {
    int64_t key;
} L3_remove_task;

typedef struct {
    int64_t key;
} L3_search_task;

typedef struct {
    int64_t result_key;
} L3_search_reply;

typedef struct {
    int64_t key;
} L3_get_task;

typedef struct {
    int64_t available;
} L3_get_reply;

typedef struct {
    int64_t nothing;
} L3_sancheck_task;

typedef struct {
    int64_t nothing;
} tick_task;