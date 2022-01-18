#pragma once

#include <defs.h>
#include <mram.h>
#include <perfcounter.h>
#include <string.h>
#include "pptr.h"
#include "macro.h"

// typedef struct L3node {
//     int64_t size;
//     int64_t height;
//     pptr up, left, right;
//     int64_t keys[DB_SIZE];
//     mpvoid addrs[DB_SIZE];
// } L3node;

typedef struct L3node {
    int64_t key;
    int64_t height;
    int64_t value;
    mppptr left __attribute__((aligned (8)));
    mppptr right __attribute__((aligned (8)));
} L3node;

typedef __mram_ptr struct L3node* mL3ptr;

extern mL3ptr root;
