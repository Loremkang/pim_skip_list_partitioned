#pragma once

#include "task.h"
#include "basic.h"
#include "common_node.h"
#define KHB_DEBUG

#ifdef KHB_DEBUG
#define ASSERT(x) assert(x)
#else
#define ASSERT(x) x
#endif


#define XSTR(x) STR(x)
#define STR(x) #x

#define MAX_DPU (NR_DPUS)
#define MAX_TASK_BUFFER_SIZE_PER_DPU (200 << 10) // 200KB
#define MAX_TASK_COUNT_PER_DPU (1536)
#define MAX_L3_HEIGHT (20)
#define MAX_THREAD_NUM (100)

/* DPU offsets */

#define DPU_SEND_BUFFER_OFFSET (MAX_TASK_BUFFER_SIZE_PER_DPU)

/* DPU variable that will be read of write by the host */
#define DPU_ID id
#define INVALID_DPU_ID ((uint32_t)-1)
#define OLD_NODES_DPU_ID ((uint32_t)-2)
#define INVALID_DPU_ADDR ((uint32_t)-1)

/* Size of the buffer on which the checksum will be performed */
// #define BUFFER_SIZE (200)
// #define LOWER_PART_HEIGHT (6)
#define BATCH_SIZE (MAX_DPU * MAX_TASK_COUNT_PER_DPU)

// L0,1,2,3 50MB
#define LX_BUFFER_SIZE (40 << 20)

// HASH TABLE 8MB. should be power of 2
#define LX_HASHTABLE_SIZE ((8 << 20) >> 3)

/* Structure used by both the host and the dpu to communicate information */