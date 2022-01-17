#pragma once

#include <mram.h>

#include "macro.h"

__host mpuint8_t wram_heap_save_addr = NULL_pt(mpuint8_t);

__mram_noinit int64_t
    send_varlen_offset_tmp[NR_TASKLETS][MAX_TASK_COUNT_PER_TASKLET_PER_BLOCK];
__mram_noinit uint8_t
    send_varlen_buffer_tmp[NR_TASKLETS][MAX_TASK_BUFFER_SIZE_PER_TASKLET];

// dpu.c
int64_t DPU_ID;  // = -1;

// task_dpu.h
extern mpint64_t send_varlen_offset[];
extern mpuint8_t send_varlen_buffer[];

typedef struct WRAMHeap {
    int64_t DPU_ID;
    mpint64_t send_varlen_offset[NR_TASKLETS];
    mpuint8_t send_varlen_buffer[NR_TASKLETS];
} WRAMHeap;  //` __attribute__((aligned (8)));

__mram_noinit uint8_t wram_heap_save_addr_tmp[sizeof(WRAMHeap) << 1];

void wram_heap_save() {
    mpuint8_t saveAddr = wram_heap_save_addr;
    WRAMHeap heapInfo = (WRAMHeap){.DPU_ID = DPU_ID};
    for (int i = 0; i < NR_TASKLETS; i++) {
        heapInfo.send_varlen_offset[i] = send_varlen_offset[i];
        heapInfo.send_varlen_buffer[i] = send_varlen_buffer[i];
    }

    if (saveAddr == NULL_pt(mpuint8_t)) saveAddr = wram_heap_save_addr_tmp;
    mram_write(&heapInfo, (mpuint8_t)saveAddr, sizeof(WRAMHeap));
    wram_heap_save_addr = saveAddr;
}

void wram_heap_init() {
    for (int i = 0; i < NR_TASKLETS; i++) {
        send_varlen_offset[i] = &(send_varlen_offset_tmp[i][0]);
        send_varlen_buffer[i] = &(send_varlen_buffer_tmp[i][0]);
    }
}

void wram_heap_load() {
    mpuint8_t saveAddr = wram_heap_save_addr;
    if (saveAddr == NULL_pt(mpuint8_t))
        wram_heap_init();
    else {
        WRAMHeap heapInfo;
        mram_read((mpuint8_t)saveAddr, &heapInfo, sizeof(WRAMHeap));
        DPU_ID = heapInfo.DPU_ID;
        for (int i = 0; i < NR_TASKLETS; i++) {
            send_varlen_offset[i] = heapInfo.send_varlen_offset[i];
            send_varlen_buffer[i] = heapInfo.send_varlen_buffer[i];
        }
    }
}
