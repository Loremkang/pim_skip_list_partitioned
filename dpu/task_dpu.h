#pragma once

#include "common.h"
#include "node_dpu.h"
#include <stdlib.h>
#include <mutex.h>

// #define extern__mram_noinit __mram_ptr __section(".mram.noinit") __dma_aligned __used
// #define extern__mram_noinit 

extern __mram_ptr uint8_t* send_buffer;
// extern int64_t DPU_RECEIVE_BUFFER_TASK_COUNT;
// extern int64_t DPU_SEND_BUFFER_TASK_COUNT;

// static inline void push_task(uint64_t type, void* buffer, size_t length) {
//     IN_DPU_ASSERT(
//         (DPU_SEND_BUFFER_SIZE + length + sizeof(uint64_t) <=
//          MAX_TASK_BUFFER_SIZE_PER_DPU) &&
//             (DPU_SEND_BUFFER_TASK_COUNT + 1 <= MAX_TASK_COUNT_PER_DPU),
//         "pushtask: full\n");
//     __mram_ptr uint8_t* pos = DPU_MRAM_HEAP_POINTER + DPU_SEND_BUFFER + DPU_SEND_BUFFER_SIZE;
//     __mram_ptr uint64_t* send_buffer_offset = DPU_MRAM_HEAP_POINTER + DPU_SEND_BUFFER_OFFSET;
//     send_buffer_offset[DPU_SEND_BUFFER_TASK_COUNT++] = DPU_SEND_BUFFER_SIZE;

//     mram_write(&type, pos, sizeof(uint64_t));
//     mram_write(buffer, pos + sizeof(uint64_t), length);
//     DPU_SEND_BUFFER_SIZE += length + sizeof(uint64_t);
// }