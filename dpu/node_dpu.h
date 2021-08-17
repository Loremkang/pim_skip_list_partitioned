#pragma once

#include <defs.h>
#include <mram.h>
#include <perfcounter.h>
#include <string.h>
#include "util.h"

typedef __mram_ptr pptr* mppptr;

typedef struct L3node {
    int64_t key;
    int64_t height;
    pptr down;
    mppptr left __attribute__((aligned (8)));
    mppptr right __attribute__((aligned (8)));
} L3node;

typedef __mram_ptr struct L3node* mL3ptr;

extern mL3ptr root;
