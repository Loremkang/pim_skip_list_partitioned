#ifndef __COMMON_H__
#define __COMMON_H__

#define KHB_DEBUG

#ifdef KHB_DEBUG
#define ASSERT(x) assert(x)
#else
#define ASSERT(x)  
#endif

#define XSTR(x) STR(x)
#define STR(x) #x

#define MAX_DPU (64)
#define MAX_TASK_PER_DPU (20)

/* DPU variable that will be read of write by the host */
#define DPU_ID id
#define DPU_KILLED killed
#define DPU_STATE state
#define DPU_SEND_BUFFER dpu_send_buffer
#define DPU_SEND_BUFFER_CNT dpu_send_buffer_cnt
#define DPU_RECEIVE_BUFFER dpu_receive_buffer
#define DPU_RECEIVE_BUFFER_CNT dpu_receive_buffer_cnt
// #define DPU_BUFFER dpu_mram_buffer
// #define DPU_CACHES dpu_wram_caches
// #define DPU_RESULTS dpu_wram_results

#define DPU_TASKQUEUE dpu_task_queue

/* Size of the buffer on which the checksum will be performed */
#define BUFFER_SIZE (200)

/* Structure used by both the host and the dpu to communicate information */

#include <stdint.h>
#include <assert.h>

typedef struct {
    bool finished[MAX_DPU];
    int nxt; int end;
    int order[MAX_DPU];
} task;

#endif /* __COMMON_H__ */
