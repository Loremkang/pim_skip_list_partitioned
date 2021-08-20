#pragma once

#ifdef KHB_DEBUG
#define IN_DPU_ASSERT(x, y) {if(!(x)){printf("%s", (y));(*(__mram_ptr int64_t*)send_buffer) = BUFFER_ERROR;exit(0);}}
#define IN_DPU_SUCCEES() {(*(__mram_ptr int64_t*)send_buffer) = 0;}
#define EXIT() {IN_DPU_ASSERT(false, "\n");}
#else
#define IN_DPU_SUCCEES()
#define DPU_ASSERT(x, y)
#endif