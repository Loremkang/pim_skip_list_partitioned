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

static inline void b_init(int64_t key, int64_t value) {
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
            if (VALID_PPTR(bn.addrs[i], NR_DPUS) && bn.keys[i] <= key) {
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

static inline int64_t b_get_min_key() {
    mBptr tmp = root;
    while (tmp->height > 0) {
        pptr nxt_addr = tmp->addrs[0];
        tmp = PPTR_TO_MBPTR(nxt_addr);
    }
    IN_DPU_ASSERT(tmp->size > 1, "bgmk! inv\n");
    return tmp->keys[1];
}

#define L3_TEMP_BUFFER_SIZE (50000)
__mram_noinit int64_t mod_keys[L3_TEMP_BUFFER_SIZE];
__mram_noinit pptr mod_values[L3_TEMP_BUFFER_SIZE];
__mram_noinit pptr mod_addrs[L3_TEMP_BUFFER_SIZE];

static inline int get_r(mppptr addrs, int n, int l) {
    int r;
    pptr p1 = addrs[l];
    for (r = l; r < n; r ++) {
        pptr p2 = addrs[r];
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


    int l, r;  // catch all inserts to the same node
    for (l = lft; l < rt; l = r) {
        int64_t keybuf = INT64_MIN;
        pptr valuebuf = null_pptr, addrbuf = null_pptr;
        int l0 = l;
        mBptr nn0;
        r = get_r(mod_addrs, n, l);
        IN_DPU_ASSERT(r <= rt, "l3i! rof\n");
        // printf("tid=%d lft=%d rt=%d l=%d r=%d\n", tid, lft, rt, l, r);
        pptr addr = mod_addrs[l];
        mBptr nn;
        pptr up;
        int nnsize;

        if (VALID_PPTR(addr, NR_DPUS)) {
            nn0 = nn = PPTR_TO_MBPTR(addr);
            mram_read(nn, &bn, sizeof(bnode));
            up = bn.up;
            nnsize = bn.size;
            for (int i = 0; i < nnsize; i++) {
                nnkeys[i] = bn.keys[i];
                nnvalues[i] = bn.addrs[i];
            }
            for (int i = 1; i < nnsize; i++) {
                IN_DPU_ASSERT_EXEC(nnkeys[i] > nnkeys[i - 1], {
                    printf("bn! addr=%x\tht=%d\n", nn, ht);
                    for (int x = 0; x < nnsize; x++) {
                        printf("[%d]key=%lld\n", x, nnkeys[x]);
                    }
                });
            }
        } else {
            nn = bbuffer + bcnt++;
            up = null_pptr;
            nnsize = 1;
            nnkeys[0] = INT64_MIN;
            nnvalues[0] = ADDR_TO_PPTR(root);
            if (ht > 0) {
                pptr nnptr = ADDR_TO_PPTR(nn);
                root->up = nnptr;
                for (int i = l; i < r; i++) {
                    pptr child_addr = mod_values[i];
                    mBptr ch = PPTR_TO_MBPTR(child_addr);
                    // printf("ch=%llx\n", ch);
                    ch->up = nnptr;
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
        IN_DPU_ASSERT_EXEC(totsize == (nnsize + r - l), {
            printf("replicated!\n");
            for (int x = 0; x < nnsize; x++) {
                printf("nn[%d]=%lld\n", x, nnkeys[x]);
            }
            for (int x = l0; x < r; x++) {
                printf("mod[%d]=%lld\n", x, mod_keys[x]);
            }
        });

        int nnl = 0;
        bn.size = 0;
        const int HF_DB_SIZE = DB_SIZE >> 1;
        // int blocks = ((totsize - 1) / DB_SIZE) + 1;
        // int block_size = ((totsize - 1) / blocks) + 1;

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
                if (VALID_PPTR(valuebuf, NR_DPUS) ||
                    VALID_PPTR(addrbuf, NR_DPUS) || (keybuf != INT64_MIN)) {
                    mod_keys[nxtrt] = keybuf;
                    mod_values[nxtrt] = valuebuf;
                    mod_addrs[nxtrt] = addrbuf;
                    printf("write to %d\n", nxtrt);
                    nxtrt++;
                }
                IN_DPU_ASSERT_EXEC(bn.keys[0] > keybuf, {
                    printf("ret! pre=%lld\tnew=%lld\n", keybuf, bn.keys[0]);
                    printf("bbuffer=%x bcnt=%d\n", bbuffer, bcnt);
                    printf("ht=%d addr=%x\n", ht, nn0);
                    printf("l=%d\tnxtrt=%d\tr=%d\tnnl=%d\n", l0, nxtrt, r, nnl);
                    printf("i=%d\ttotsize=%d\n", i, totsize);
                    for (int x = 0; x < nnsize; x++) {
                        printf("nn[%d]=%lld\n", x, nnkeys[x]);
                    }
                    for (int x = 0; x < bn.size; x++) {
                        printf("nn0[%d]=%lld\n", x, bn.keys[x]);
                    }
                    for (int x = l0; x < r; x++) {
                        printf("mod[%d]=%lld\n", x, mod_keys[x]);
                    }
                })
                keybuf = bn.keys[0];
                addrbuf = bn.up;
                pptr nnptr = ADDR_TO_PPTR(nn);
                valuebuf = nnptr;
            }
            if (bn.size == DB_SIZE ||
                (i + HF_DB_SIZE + 1 == totsize && bn.size > HF_DB_SIZE)) {
                for (int x = 1; x < bn.size; x++) {
                    IN_DPU_ASSERT_EXEC(bn.keys[x] != bn.keys[x - 1], {
                        printf("mid\n");
                        printf("bbuffer=%x bcnt=%d\n", bbuffer, bcnt);
                        printf("ht=%d addr=%x\n", ht, nn0);
                        printf("l=%d\tnxtrt=%d\tr=%d\tnnl=%d\n", l0, nxtrt, r, nnl);
                        printf("i=%d\ttotsize=%d\n", i, totsize);
                        for (int x = 0; x < nnsize; x++) {
                            printf("nn[%d]=%lld\n", x, nnkeys[x]);
                        }
                        for (int x = 0; x < bn.size; x++) {
                            printf("nn0[%d]=%lld\n", x, bn.keys[x]);
                        }
                        for (int x = l0; x < r; x++) {
                            printf("mod[%d]=%lld\n", x, mod_keys[x]);
                        }
                    });
                }
                mram_write(&bn, nn, sizeof(bnode));
                pptr up = bn.up;
                int64_t ht = bn.height;
                b_node_init(&bn, ht, up);
                nn = bbuffer + bcnt++;
            }
            if (nnl == nnsize && l == r) {
                IN_DPU_ASSERT_EXEC(i + 1 == totsize, {
                    printf("bbuffer=%x bcnt=%d\n", bbuffer, bcnt);
                    printf("ht=%d addr=%x\n", ht, nn0);
                    printf("l=%d\tr=%d\tnnl=%d\n", l0, r, nnl);
                    printf("i=%d\ttotsize=%d\n", i, totsize);
                    for (int x = 0; x < nnsize; x++) {
                        printf("nn[%d]=%lld\n", x, nnkeys[x]);
                    }
                    mram_read(nn0, &bn, sizeof(bnode));
                    printf("nn0size=%lld nnsize=%d\n", bn.size, nnsize);
                    for (int x = 0; x < bn.size; x++) {
                        printf("nn0[%d]=%lld\n", x, bn.keys[x]);
                    }
                    for (int x = l0; x < r; x++) {
                        printf("mod[%d]=%lld\n", x, mod_keys[x]);
                    }
                });
            }
        }
        if (bn.size != 0) {
            if (VALID_PPTR(valuebuf, NR_DPUS) || VALID_PPTR(addrbuf, NR_DPUS) ||
                (keybuf != INT64_MIN)) {
                mod_keys[nxtrt] = keybuf;
                mod_values[nxtrt] = valuebuf;
                mod_addrs[nxtrt] = addrbuf;
                printf("write to %d\n", nxtrt);
                nxtrt++;
            }
            for (int x = 1; x < bn.size; x++) {
                IN_DPU_ASSERT_EXEC(bn.keys[x] != bn.keys[x - 1], {
                    printf("end\n");
                    printf("bbuffer=%x bcnt=%d\n", bbuffer, bcnt);
                    printf("ht=%d addr=%x\n", ht, nn0);
                    printf("l=%d\tr=%d\tnnl=%d\n", l0, r, nnl);
                    for (int x = 0; x < nnsize; x++) {
                        printf("nn[%d]=%lld\n", x, nnkeys[x]);
                    }
                    for (int x = 0; x < bn.size; x++) {
                        printf("nn0[%d]=%lld\n", x, bn.keys[x]);
                    }
                    for (int x = l0; x < r; x++) {
                        printf("mod[%d]=%lld\n", x, mod_keys[x]);
                    }
                });
            }
            mram_write(&bn, nn, sizeof(bnode));
        }
        IN_DPU_ASSERT(nxtrt <= r, "l3i! nxtof\n");
        IN_DPU_ASSERT(l == r && nnl == nnsize, "l3i! lrnns\n");
    }
    L3_lfts[tid] = nxtlft;
    L3_rts[tid] = nxtrt;
    // if (nxtlft != nxtrt) {
    //     printf("after ht=%d\ttid=%d\tlft=%d\trt=%d\n", ht, tid, nxtlft,
    //     nxtrt);
    // }
}

const int SERIAL_HEIGHT = 0;

void b_insert_parallel(int n, int l, int r) {
    int tid = me();

    // bottom up

    for (int i = l; i < r; i++) {
        int64_t key = mod_keys[i];
        int64_t value;

        mBptr nn;
        b_search(key, &nn, &value);
        mod_addrs[i] = ADDR_TO_PPTR(nn);
        if (i > 0) {
            int64_t keyl = mod_keys[i - 1];
            IN_DPU_ASSERT(keyl < key, "bip! eq\n");
        }
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

    // if (tid == 0) {
    //     for (int i = 0; i < n; i ++) {
    //         int64_t key = mod_keys[i];
    //         pptr value = mod_values[i];
    //         pptr addr = mod_addrs[i];
    //         printf("[%d]key=%llx\tvalue=%llx\taddr=%llx\n", i, key,
    //         PPTR_TO_I64(value), PPTR_TO_I64(addr));
    //     }
    // }
    // EXIT();

    for (int ht = 0; ht <= root->height + 1; ht++) {
        if (ht < SERIAL_HEIGHT) {
            // distribute work
            {
                printf("distributed %d\n", tid);
                if (tid == 0) {
                    for (int i = 0; i < NR_TASKLETS; i++) {
                        // int i = tid;
                        n = L3_n;
                        int lft = n * i / NR_TASKLETS;
                        int rt = n * (i + 1) / NR_TASKLETS;
                        // printf("t0 n=%d\ttid=%d\tlft=%d\trt=%d\n", n, i, lft,
                        // rt);
                        if (rt > lft) {
                            if (lft != 0) {
                                lft = get_r(mod_addrs, n, lft - 1);
                            }
                            rt = get_r(mod_addrs, n, rt);
                        }
                        // printf("t1 n=%d\ttid=%d\tlft=%d\trt=%d\n", n, i, lft,
                        // rt);
                        L3_lfts[i] = lft;
                        L3_rts[i] = rt;

                        if (rt != n && lft != rt) {
                            pptr p1 = mod_addrs[lft];
                            pptr p2 = mod_addrs[rt];
                            IN_DPU_ASSERT_EXEC(!EQUAL_PPTR(p1, p2), {
                                printf(
                                    "bi! div2 "
                                    "tid=%d\tlft=%d\trt=%d"
                                    "\tn=%d"
                                    "ht=%d"
                                    "\n",
                                    i, lft, rt, n, ht);
                                for (int x = lft; x <= rt; x ++) {
                                    p1 = mod_addrs[x];
                                    printf("addr[%d]=%llx\n", x, p1);
                                }
                            });
                        }
                    }
                }
            }

            barrier_wait(&L3_barrier2);

            // execute
            b_insert_onelevel(n, tid, ht);
            barrier_wait(&L3_barrier3);

            // distribute work
            if (tid == 0) {
                n = 0;
                for (int i = 0; i < NR_TASKLETS; i++) {
                    printf("l=%d\tr=%d\n", L3_lfts[i], L3_rts[i]);
                    for (int j = L3_lfts[i]; j < L3_rts[i]; j++) {
                        pptr val, addr;
                        mod_keys[n] = mod_keys[j];
                        val = mod_values[n] = mod_values[j];
                        addr = mod_addrs[n] = mod_addrs[j];
                        printf("n=%d\tk=%llx\tv=%llx\ta=%llx\n", n, mod_keys[n],
                               PPTR_TO_I64(val), PPTR_TO_I64(addr));
                        n++;
                    }
                }
                L3_n = n;
                // printf("n=%d\n", n);
            }
            barrier_wait(&L3_barrier1);
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