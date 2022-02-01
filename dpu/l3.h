#pragma once

#include "common.h"
#include "task_framework_dpu.h"
// #include "storage.h"
#include "hashtable_l3size.h"
#include <barrier.h>
#include <alloc.h>
// #include <profiling.h>

// PROFILING_INIT(prof_newnode);
// PROFILING_INIT(prof_internal);
// PROFILING_INIT(prof_external);
// PROFILING_INIT(prof_finish);

#define PPTR_TO_MBPTR(x) ((mBptr)(x.addr))
#define ADDR_TO_PPTR(x) ((pptr){.id = DPU_ID, .addr = (uint32_t)x})

BARRIER_INIT(L3_barrier1, NR_TASKLETS);
BARRIER_INIT(L3_barrier2, NR_TASKLETS);
BARRIER_INIT(L3_barrier3, NR_TASKLETS);

MUTEX_INIT(L3_lock);

extern int64_t DPU_ID;
extern __mram_ptr ht_slot l3ht[]; 

static inline void b_init(int64_t key, int64_t value, int height) {
    IN_DPU_ASSERT(bcnt == 1, "L3init: Wrong bcnt\n");
    mBptr nn = bbuffer + bcnt ++;
    bnode bn;
    b_node_init(&bn, 0, null_pptr);
    bn.size = 1;
    bn.keys[0] = INT64_MIN;
    bn.addrs[0] = I64_TO_PPTR(value);
    mram_write(&bn, nn, sizeof(bnode));
    min_node = root = nn;
}

static inline int64_t b_search(int64_t key, mBptr *addr, int64_t *value) {
    mBptr tmp = root;
    bnode bn;
    while (true) {
        mram_read(tmp, &bn, sizeof(bnode));
        int64_t pred = INT64_MIN;
        pptr nxt_addr;
#pragma clang loop unroll(full)
        for (int i = 0; i < DB_SIZE; i++) {
            if (VALID_PPTR(bn.addrs[i]) && bn.keys[i] <= key) {
                pred = bn.keys[i];
                nxt_addr = bn.addrs[i];
            }
        }
        if (bn.height > 0) {
            tmp = PPTR_TO_MBPTR(nxt_addr);
        } else {
            *addr = tmp;
            *value = PPTR_TO_I64(nxt_addr);
            return pred;
        }
    }
}

#define L3_TEMP_BUFFER_SIZE (50000)
__mram_noinit int64_t mod_keys[L3_TEMP_BUFFER_SIZE];
__mram_noinit pptr mod_values[L3_TEMP_BUFFER_SIZE];
__mram_noinit pptr mod_addrs[L3_TEMP_BUFFER_SIZE];

static inline int get_r(mppptr addrs, int n, int l) {
    int r;
    pptr p1 = addrs[l].addr;
    for (r = l; r < n; r ++) {
        pptr p2 = addrs[r].addr;
        if (EQUAL_PPTR(p1, p2)) {
            continue;
        } else {
            break;
        }
    }
    return r;
}

extern int *L3_lfts, *L3_rts;
int64_t L3_n;

static inline void b_insert_onelevel(int n, int tid, int ht) {
    bnode bn;
    int64_t nnkeys[DB_SIZE];
    pptr nnvalues[DB_SIZE];
    int lft = L3_lfts[tid];
    int rt = L3_rts[tid];
    int nxtlft = lft;
    int nxtrt = nxtlft;
    int siz = 0;

    int l, r;  // catch all inserts to the same node
    for (l = lft; l < rt; l = r) {
        r = get_r(mod_addrs, n, l);
        pptr addr = mod_addrs[l];
        mBptr nn;
        pptr up;
        int nnsize;
        bool newroot = false;
        if (VALID_PPTR(addr)) {
            nn = PPTR_TO_MBPTR(addr);
            mram_read(nn, &bn, sizeof(bnode));
            up = bn.up;
            nnsize = bn.size;
            for (int i = 0; i < nnsize; i++) {
                nnkeys[i] = bn.keys[i];
                nnvalues[i] = bn.addrs[i];
            }
        } else {
            nn = bbuffer + bcnt++;
            up = null_pptr;
            nnsize = 1;
            nnkeys[0] = INT64_MIN;
            nnvalues[0] = ADDR_TO_PPTR(root);
            if (ht > 0) {
                root->up = ADDR_TO_PPTR(nn);
                for (int i = l; i < r; i++) {
                    pptr child_addr = mod_values[i];
                    mBptr ch = PPTR_TO_MBPTR(child_addr);
                    // printf("ch=%llx\n", ch);
                    ch->up = ADDR_TO_PPTR(nn);
                }
            }
            root = nn;
        }
        b_node_init(&bn, ht, up);
        int totsize = 0;

        {
            int nnl = 0;
            int i = l;
            while (i < r || nnl < nnsize) {
                if (i < r && nnl < nnsize) {
                    if (mod_keys[i] == nnkeys[nnl]) {
                        i++;
                        totsize--;  // replace
                    } else if (mod_keys[i] < nnkeys[nnl]) {
                        i++;
                    } else {
                        nnl++;
                    }
                } else if (i == r) {
                    nnl++;
                } else {
                    i++;
                }
                totsize++;
            }
        }

        int nnl = 0;
        bn.size = 0;
        const int HF_DB_SIZE = DB_SIZE >> 1;
        // int blocks = ((totsize - 1) / DB_SIZE) + 1;
        // int block_size = ((totsize - 1) / blocks) + 1;

        int l0 = l;
        for (int i = 0; nnl < nnsize || l < r; i++) {
            if (nnl < nnsize && (l == r || nnkeys[nnl] < mod_keys[l])) {
                bn.keys[bn.size] = nnkeys[nnl];
                bn.addrs[bn.size] = nnvalues[nnl];
                nnl++;
            } else {
                bn.keys[bn.size] = mod_keys[l];
                bn.addrs[bn.size] = mod_values[l];
                if (nnl < nnsize && nnkeys[nnl] == mod_keys[l]) {  // replace
                    nnl++;
                }
                l++;
            }
            bn.size++;
            if (bn.size == 1 && (i > 0)) {  // newnode
                mod_keys[nxtrt] = bn.keys[0];
                mod_values[nxtrt] = ADDR_TO_PPTR(nn);
                // mod_values[nxtrt] = bn.addrs[0];
                mod_addrs[nxtrt] = bn.up;
                nxtrt++;
            }
            if (bn.size == DB_SIZE ||
                (i + HF_DB_SIZE + 1 == totsize && bn.size > HF_DB_SIZE)) {
                mram_write(&bn, nn, sizeof(bnode));
                pptr up = bn.up;
                int64_t ht = bn.height;
                b_node_init(&bn, ht, up);
                nn = bbuffer + bcnt++;
            }
            if (nnl == nnsize && l == r) {
                // printf("i=%d\ttotsize=%d\n", i, totsize);
                IN_DPU_ASSERT(i + 1 == totsize, "l3i! totsiz\n");
            }
        }
        if (bn.size != 0) {
            mram_write(&bn, nn, sizeof(bnode));
        }
        IN_DPU_ASSERT(l == r && nnl == nnsize, "l3i! lrnns\n");
    }
    L3_lfts[tid] = nxtlft;
    L3_rts[tid] = nxtrt;
    // if (nxtlft != nxtrt) {
    //     printf("after ht=%d\ttid=%d\tlft=%d\trt=%d\n", ht, tid, nxtlft, nxtrt);
    // }
}


const int SERIAL_HEIGHT = 2;

void b_insert_parallel(int n, int l, int r, __mram_ptr L3_insert_task* tit) {
    int tid = me();

    // bottom up
    for (int i = l; i < r; i++) {
        int64_t key = tit[i].key;
        int64_t value = tit[i].value;
        mod_values[i] = I64_TO_PPTR(value);

        bnode *nn;
        int64_t pred = b_search(key, &nn, &value);
        mod_addrs[i] = ADDR_TO_PPTR(nn);
        mod_keys[i] = kvs[i].key;
    }

    // printf("%d %d %d\n", tid, l, r);
    // barrier_wait(&L3_barrier2);

    // if (tid == 0) {
    //     for (int i = 0; i < n; i++) {
    //         printf("%2d\t%llx\n", i, mod_addrs[i].addr);
    //     }
    // }

    L3_n = n;
    barrier_wait(&L3_barrier1);

    for (int ht = 0; ht <= root->height + 1; ht++) {
        if (ht < SERIAL_HEIGHT) {
            // distribute work
            n = L3_n;
            int lft = n * tid / NR_TASKLETS;
            int rt = n * (tid + 1) / NR_TASKLETS;
            // printf("t0 n=%d\ttid=%d\tlft=%d\trt=%d\n", n, tid, lft, rt);
            if (rt > lft) {
                if (lft != 0) {
                    lft = get_r(mod_addrs, n, lft - 1);
                }
                rt = get_r(mod_addrs, n, rt);
            }
            // printf("t1 n=%d\ttid=%d\tlft=%d\trt=%d\n", n, tid, lft, rt);
            L3_lfts[tid] = lft;
            L3_rts[tid] = rt;

            barrier_wait(&L3_barrier2);

            // execute
            b_insert_onelevel(n, tid, ht);
            barrier_wait(&L3_barrier1);

            // distribute work
            if (tid == 0) {
                n = 0;
                for (int i = 0; i < NR_TASKLETS; i++) {
                    for (int j = L3_lfts[i]; j < L3_rts[i]; j++) {
                        mod_keys[n] = mod_keys[j];
                        mod_values[n] = mod_values[j];
                        mod_addrs[n] = mod_addrs[j];
                        // printf("n=%d\tk=%llx\tv=%llx\ta=%llx\n", n, mod_keys[n],
                        //        mod_values[n], mod_addrs[n]);
                        n++;
                    }
                }
                L3_n = n;
                // printf("n=%d\n", n);
            }
            barrier_wait(&L3_barrier2);
        } else {
            if (tid == 0) {
                // printf("SOLO:%d\n", n);
                L3_lfts[0] = 0;
                L3_rts[0] = n;
                b_insert_onelevel(n, tid, ht);
                n = L3_rts[0];
            } else {
                break;
            }
        }
    }
    barrier_wait(&L3_barrier3);
}