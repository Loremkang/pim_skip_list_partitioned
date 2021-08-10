#pragma once

#include "basic.h"

#define EMPTY 0

#define L3_INIT 1
#define L3_INSERT 2
#define L3_REMOVE 3
#define L3_SEARCH 4
#define L3_SANCHECK 5
#define L3_GET 6

// #define LOWER_INIT 1
// #define BUILD_UP_DOWN 3
// #define PREDECESSOR 4
// #define LOWER_INSERT 5
// #define FETCH_NODE 7
// #define BUILD_LEFT_RIGHT 8
// #define GET 9
// #define GET_HEIGHT 10
// #define FETCH_NODE_BY_HEIGHT 11

#include <stdint.h>
#include <assert.h>

typedef struct {
    int64_t key;
    pptr addr; // addr of the lower part root
    int64_t height;
} L3_insert_task;

typedef struct {
    int64_t key;
    pptr addr; // addr of the upper part node
} L3_insert_reply;

typedef struct {
    int64_t key;
} L3_remove_task;

typedef struct {
    int64_t key;
} L3_search_task;

typedef struct {
    int64_t key;
    pptr addr;
    int64_t result_key;
} L3_search_reply;

typedef struct {
    int64_t key;
} L3_get_task;

typedef struct {
    int64_t key;
    pptr addr;
    int64_t available;
} L3_get_reply;

typedef struct {
    int64_t nothing;
} L3_sancheck_task;

typedef struct task {
    uint64_t type;
    uint8_t buffer[];
} task;
