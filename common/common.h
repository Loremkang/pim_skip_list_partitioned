#pragma once

#include "task.h"
#include "basic.h"
#include "common_node.h"
#define KHB_DEBUG

#ifdef KHB_DEBUG
#define ASSERT(x) assert(x)
#else
#define ASSERT(x)  
#endif


#define XSTR(x) STR(x)
#define STR(x) #x

#define MAX_DPU (NR_DPUS)
#define MAX_TASK_BUFFER_SIZE_PER_DPU (20 << 10) // 20KB
#define MAX_TASK_COUNT_PER_DPU (512)
#define MAX_L3_HEIGHT (20)

/* DPU variable that will be read of write by the host */
#define DPU_ID id
#define INVALID_DPU_ID ((uint32_t)-1)
#define OLD_NODES_DPU_ID ((uint32_t)-2)
#define INVALID_DPU_ADDR ((uint32_t)-1)

/* Size of the buffer on which the checksum will be performed */
// #define BUFFER_SIZE (200)
// #define LOWER_PART_HEIGHT (6)
#define BATCH_SIZE (MAX_DPU * MAX_TASK_COUNT_PER_DPU)

// L0,1,2,3 12MB
#define LX_BUFFER_SIZE (12 << 20)

// HASH TABLE 2MB. should be power of 2
#define LX_HASHTABLE_SIZE ((2 << 20) >> 3)

/* Structure used by both the host and the dpu to communicate information */