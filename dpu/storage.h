#pragma once

#include <mram.h>

#include "macro.h"
#include "common.h"
#include "hashtable_l3size.h"

__host mpuint8_t wram_heap_save_addr = NULL_pt(mpuint8_t);

// only top 2 inited because of task_framework_dpu.h
__mram_noinit int64_t
    send_varlen_offset_tmp[NR_TASKLETS][MAX_TASK_COUNT_PER_TASKLET_PER_BLOCK];
__mram_noinit uint8_t
    send_varlen_buffer_tmp[NR_TASKLETS][MAX_TASK_BUFFER_SIZE_PER_TASKLET];

__mram_noinit ht_slot l3ht[LX_HASHTABLE_SIZE]; // must be 8 bytes aligned
__host int l3htcnt = 0;
__mram_noinit uint8_t l3buffer[LX_BUFFER_SIZE];
__host int l3cnt = 8;
__host mL3ptr root;

// dpu.c
int64_t DPU_ID;  // = -1;

// task_dpu.h
extern mpint64_t send_varlen_offset[];
extern mpuint8_t send_varlen_buffer[];

typedef struct WRAMHeap {
    int64_t DPU_ID;
    mpint64_t send_varlen_offset[NR_TASKLETS];
    mpuint8_t send_varlen_buffer[NR_TASKLETS];
#ifdef DPU_ENERGY
    uint64_t op_cnt;
    uint64_t db_size_cnt;
    uint64_t cycle_cnt;
#endif
} WRAMHeap;  //` __attribute__((aligned (8)));

__mram_noinit uint8_t wram_heap_save_addr_tmp[sizeof(WRAMHeap) << 1];

void wram_heap_save() {
    mpuint8_t saveAddr = wram_heap_save_addr;
    WRAMHeap heapInfo = (WRAMHeap){.DPU_ID = DPU_ID};
    for (int i = 0; i < NR_TASKLETS; i++) {
        heapInfo.send_varlen_offset[i] = send_varlen_offset[i];
        heapInfo.send_varlen_buffer[i] = send_varlen_buffer[i];
    }
#ifdef DPU_ENERGY
    heapInfo.op_cnt = op_count;
    heapInfo.db_size_cnt = db_size_count;
    heapInfo.cycle_cnt = cycle_count;
#endif
    if (saveAddr == NULL_pt(mpuint8_t)) saveAddr = wram_heap_save_addr_tmp;
    mram_write(&heapInfo, (mpuint8_t)saveAddr, sizeof(WRAMHeap));
    wram_heap_save_addr = saveAddr;
}

void wram_heap_init() {
    for (int i = 0; i < NR_TASKLETS; i++) {
        send_varlen_offset[i] = &(send_varlen_offset_tmp[i][0]);
        send_varlen_buffer[i] = &(send_varlen_buffer_tmp[i][0]);
    }
#ifdef DPU_ENERGY
    op_count = 0;
    db_size_count = 0;
    cycle_count = 0;
#endif
}

void wram_heap_load() {
    mpuint8_t saveAddr = wram_heap_save_addr;
    if (saveAddr == NULL_pt(mpuint8_t)) {
        // printf("%d\n", (int)send_varlen_buffer_tmp);
        // exit(0);
        // EXIT();
        wram_heap_init();
    } else {
        WRAMHeap heapInfo;
        mram_read((mpuint8_t)saveAddr, &heapInfo, sizeof(WRAMHeap));
        DPU_ID = heapInfo.DPU_ID;
        for (int i = 0; i < NR_TASKLETS; i++) {
            send_varlen_offset[i] = heapInfo.send_varlen_offset[i];
            send_varlen_buffer[i] = heapInfo.send_varlen_buffer[i];
        }
#ifdef DPU_ENERGY
        op_count = heapInfo.op_cnt;
        db_size_count = heapInfo.db_size_cnt;
        cycle_count = heapInfo.cycle_cnt;
#endif
    }
}
