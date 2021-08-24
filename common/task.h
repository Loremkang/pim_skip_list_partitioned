#pragma once

#include "basic.h"

/* -------------------------- Buffer Identifier -------------------------- */
#define BUFFER_ERROR -1
#define BUFFER_FIXED_LENGTH 0

/* -------------------------- Task Type -------------------------- */

#define EMPTY 0

// l1
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

// l2
#define L2_INIT_TSK 101
#define L2_INIT_REP 102

#define L2_INSERT_TSK 103
#define L2_INSERT_REP 104

#define L2_BUILD_LR_TSK 105

#define L2_BUILD_UP_TSK 106

#define L2_REMOVE_TSK 107
#define L2_REMOVE_REP 108

#define L2_GET_NODE_TSK 109
#define L2_GET_NODE_REP 110

#define L2_GET_TSK 111
#define L2_GET_REP 112

// util
#define INIT_TSK 501
#define TICK_TSK 502

#include <stdint.h>
#include <assert.h>

/* -------------------------- Level 3 -------------------------- */
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
    // int64_t result_key;
    pptr addr;
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

/* -------------------------- Level 2 -------------------------- */

typedef L3_insert_task L2_init_task;
typedef L3_insert_reply L2_init_reply;

typedef L3_insert_task L2_insert_task;
typedef L3_insert_reply L2_insert_reply;

typedef struct {
    int64_t height; // positive for right, negative for left
    pptr addr;
    pptr val;
    int64_t chk;
    // pptr left, right;
} L2_build_lr_task; // not variable length now

typedef struct {
    pptr addr;
    pptr up;
} L2_build_up_task;

typedef L3_remove_task L2_remove_task;

typedef struct {
    int64_t height;
    // pptr[] pointers; // variable length
} L2_remove_reply;

static inline int64_t L2_remove_reply_size(int64_t height) {
    return sizeof(L2_remove_reply) + sizeof(pptr) * 2 * height;
}

typedef struct {
    pptr addr;
    int64_t height;
} L2_get_node_task;

typedef struct {
    int64_t chk;
    pptr right;
} L2_get_node_reply;

typedef L3_get_task L2_get_task;
typedef L3_get_reply L2_get_reply;

/* -------------------------- Util -------------------------- */

typedef struct {
    int64_t nothing;
} tick_task;

typedef struct {
    int64_t id;
} init_task;