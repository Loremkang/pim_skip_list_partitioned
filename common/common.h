#pragma once

#include "task.h"
#include "basic.h"
#include "common_node.h"
#define KHB_DEBUG

#ifdef KHB_DEBUG
#define ASSERT(x) assert(x)
#define IN_DPU_ASSERT(x, y) {if(!(x)){printf("%s", (y));DPU_SEND_BUFFER_SIZE=LLONG_MAX;exit(0);}}
#else
#define ASSERT(x)  
#define DPU_ASSERT(x, y)
#endif


#define XSTR(x) STR(x)
#define STR(x) #x

#define MAX_DPU (64)
#define MAX_TASK_BUFFER_SIZE_PER_DPU (1 << 12) // 4K
#define MAX_TASK_COUNT_PER_DPU (1 << 8) // 256
#define MAX_TASK_SIZE (1 << 10)
#define MAX_TOTAL_HEIGHT (30)

/* DPU variable that will be read of write by the host */
#define DPU_ID id

#define DPU_SEND_BUFFER dpu_send_buffer
#define DPU_SEND_BUFFER_OFFSET dpu_send_buffer_offset
#define DPU_SEND_BUFFER_SIZE dpu_send_buffer_size
#define DPU_SEND_BUFFER_TASK_COUNT dpu_send_buffer_task_count

#define DPU_RECEIVE_BUFFER dpu_receive_buffer
#define DPU_RECEIVE_BUFFER_OFFSET dpu_receive_buffer_offset
#define DPU_RECEIVE_BUFFER_SIZE dpu_receive_buffer_size
#define DPU_RECEIVE_BUFFER_TASK_COUNT dpu_receive_buffer_task_count

// #define DPU_BUFFER dpu_mram_buffer
// #define DPU_CACHES dpu_wram_caches
// #define DPU_RESULTS dpu_wram_results

#define DPU_TASKQUEUE dpu_task_queue

/* Size of the buffer on which the checksum will be performed */
// #define BUFFER_SIZE (200)
#define LOWER_PART_HEIGHT (6)
#define BATCH_SIZE (MAX_DPU * MAX_TASK_COUNT_PER_DPU / 8)

/* Structure used by both the host and the dpu to communicate information */