#pragma once

#include <stdint.h>
#include <assert.h>

typedef struct twoval {
    uint64_t a[2];
} twoval_task;

typedef struct threeval {
    uint64_t a[3];
} threeval_task;

typedef struct task {
    uint64_t type;
    uint8_t buffer[];
} task;
