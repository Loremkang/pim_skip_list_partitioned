#pragma once

#include "task.h"
// #define KHB_DEBUG

#define MAX_L3_HEIGHT (20)

#define TASK_TYPE (7)

/* Size of the buffer on which the checksum will be performed */
// #define BUFFER_SIZE (200)
// #define LOWER_PART_HEIGHT (6)
#define BATCH_SIZE (NR_DPUS * MAX_TASK_COUNT_PER_DPU)

// L0,1,2,3 50MB
#define LX_BUFFER_SIZE (45 << 20)

// HASH TABLE 8MB. should be power of 2
#define LX_HASHTABLE_SIZE ((8 << 20) >> 3)