#pragma once

#include <defs.h>
#include <mram.h>
#include <perfcounter.h>
#include <string.h>
#include "pptr.h"
#include "macro.h"

typedef struct bnode {
    pptr up;
    int64_t height;
    int64_t size;
    int64_t keys[DB_SIZE];
    pptr addrs[DB_SIZE];
} bnode;

typedef __mram_ptr struct bnode* mBptr;

// extern mL3ptr root;
extern mBptr root;
extern mBptr min_node;