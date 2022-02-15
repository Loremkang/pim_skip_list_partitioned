#pragma once

#include <defs.h>
#include <mram.h>
#include <perfcounter.h>
#include <string.h>
#include "pptr.h"
#include "macro.h"

typedef struct bnode {
    pptr up, right;
    int64_t height;
    int64_t size;
    int64_t keys[DB_SIZE];
    pptr addrs[DB_SIZE];
} bnode;

typedef __mram_ptr struct bnode* mBptr;

// extern mL3ptr root;
extern mBptr root;
// extern mBptr min_node;
extern int64_t DPU_ID;

static pptr mbptr_to_pptr(const mBptr addr) {
    return (pptr){.id = DPU_ID, .addr = (uint32_t)addr};
}

static inline mBptr pptr_to_mbptr(pptr x) { return (mBptr)x.addr; }