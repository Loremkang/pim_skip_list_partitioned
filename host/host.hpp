#pragma once

#include "common.h"

extern dpu_set_t dpu_set, dpu;
extern uint32_t each_dpu;
extern int nr_of_dpus;

extern int64_t op_keys[];
extern int64_t op_results[];
extern int8_t op_heights[];
extern int8_t insert_heights[];
extern pptr op_addrs[];
extern pptr op_addrs2[];
extern int32_t op_taskpos[];