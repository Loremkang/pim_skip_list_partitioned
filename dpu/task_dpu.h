#pragma once

#include "common.h"
#include "node_dpu.h"

#define extern__mram_noinit __mram_ptr __section(".mram.noinit") __dma_aligned
// #define extern__mram_noinit __mram_noinit

extern extern__mram_noinit uint8_t DPU_RECEIVE_BUFFER[];
extern extern__mram_noinit uint64_t DPU_RECEIVE_BUFFER_OFFSET[];
extern extern__mram_noinit uint64_t DPU_RECEIVE_BUFFER_SIZE;
extern extern__mram_noinit uint64_t DPU_RECEIVE_BUFFER_TASK_COUNT;

extern extern__mram_noinit uint8_t DPU_SEND_BUFFER[];
extern extern__mram_noinit uint64_t DPU_SEND_BUFFER_OFFSET[];
extern extern__mram_noinit uint64_t DPU_SEND_BUFFER_SIZE;
extern extern__mram_noinit uint64_t DPU_SEND_BUFFER_TASK_COUNT; // __mram removing __used

inline void push_task(uint64_t type, void* buffer, size_t length) {
    assert(DPU_SEND_BUFFER_SIZE + length + sizeof(uint64_t) <= MAX_TASK_BUFFER_SIZE_PER_DPU);
    assert(DPU_SEND_BUFFER_TASK_COUNT + 1 <= MAX_TASK_COUNT_PER_DPU);
    __mram_ptr uint8_t* pos = &DPU_SEND_BUFFER[0] + DPU_SEND_BUFFER_SIZE;
    DPU_SEND_BUFFER_OFFSET[DPU_SEND_BUFFER_TASK_COUNT ++] = DPU_SEND_BUFFER_SIZE;

    memcpy(pos, &type, sizeof(uint64_t));
    memcpy(pos + sizeof(uint64_t), buffer, length);
    DPU_SEND_BUFFER_SIZE += length + sizeof(uint64_t);
}