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

BARRIER_INIT(L3_barrier1, NR_TASKLETS);
BARRIER_INIT(L3_barrier2, NR_TASKLETS);
BARRIER_INIT(L3_barrier3, NR_TASKLETS);

MUTEX_INIT(L3_lock);

extern int64_t DPU_ID;
extern __mram_ptr ht_slot l3ht[];

void print_bnode(mBptr nn) {
    bnode bn;
    mram_read(nn, &bn, sizeof(bnode));
    printf("addr=%x\n", bn);
    printf("ht=%lld\tup=%llx\tright=%llx\n", bn.height, pptr_to_int64(bn.up), pptr_to_int64(bn.right));
    printf("size=%lld\n", bn.size);
    for (int i = 0; i < DB_SIZE; i++) {
        if (bn.height > 0) {
            printf("[%d]:key=%lld\tval=%llx\n", i, bn.keys[i], pptr_to_int64(bn.addrs[i]));
        } else {
            printf("[%d]:key=%lld\tval=%lld\n", i, bn.keys[i], pptr_to_int64(bn.addrs[i]));
        }
    }
    printf("\n");
}


#define RETURN_FALSE(x)   \
    {                     \
        if (!(x)) {       \
            return false; \
        }                 \
    }

#define RETURN_FALSE_PRINT(x, args...) \
    {                                  \
        if (!(x)) {                    \
            printf(args);              \
            return false;              \
        }                              \
    }

bool print_tree(mBptr nn) {
    print_bnode(nn);
    bnode bn;
    mram_read(nn, &bn, sizeof(bnode));
    if (bn.height > 0) {
        for (int i = 0; i < bn.size; i++) {
            RETURN_FALSE_PRINT(valid_pptr(bn.addrs[i]), "pt! inv %d", i);
            RETURN_FALSE(print_tree(pptr_to_mbptr(bn.addrs[i])));
        }
    }
}

bool sancheck(mBptr nn) {
    bnode bn;
    mram_read(nn, &bn, sizeof(bnode));
    for (int i = 1; i < bn.size; i++) {
        RETURN_FALSE_PRINT(bn.keys[i] > bn.keys[i - 1],
                           "sc: keyinv %lld %lld\n", bn.keys[i],
                           bn.keys[i - 1]);
    }
    if (bn.height > 0) {
        for (int i = 0; i < bn.size; i++) {
            RETURN_FALSE_PRINT(valid_pptr(bn.addrs[i]),
                               "sc: addrinv %d %llx\n", i,
                               pptr_to_int64(bn.addrs[i]));
            mBptr nn = pptr_to_mbptr(bn.addrs[i]);
            pptr addr = mbptr_to_pptr(nn);
            RETURN_FALSE_PRINT(equal_pptr(nn->up, addr), "sc: up\n");
            RETURN_FALSE(sancheck(nn));
        }
        for (int i = 0; i < bn.size; i++) {
            mBptr ch = pptr_to_mbptr(bn.addrs[i]);
            RETURN_FALSE_PRINT(ch->height == bn.height - 1, "sc: ht\n");
            RETURN_FALSE_PRINT(ch->size > 0, "sc: empty\n");
            RETURN_FALSE_PRINT(bn.keys[i] == ch->keys[0],
                               "sc: wrongkey %lld %lld\n", bn.keys[i],
                               ch->keys[0]);
            if (i < bn.size - 1) {
                RETURN_FALSE_PRINT(ch->keys[ch->size - 1] < bn.keys[i + 1],
                                   "sc: wrongchkey\n");
            }
        }
        for (int i = 1; i < bn.size; i++) {
            mBptr l = pptr_to_mbptr(bn.addrs[i - 1]);
            RETURN_FALSE_PRINT(equal_pptr(l->right, bn.addrs[i]),
                               "sc: wrongintr\n");
        }
        RETURN_FALSE(bn.size > 0);
        if (valid_pptr(bn.right)) {
            mBptr r = pptr_to_mbptr(bn.right);
            mBptr l = pptr_to_mbptr(bn.addrs[bn.size - 1]);
            RETURN_FALSE_PRINT(equal_pptr(l->right, r->addrs[0]),
                               "sc: wrongoutr\n");
        }
    }
    return true;
}

inline void b_node_init(bnode *bn, int ht, pptr up, pptr right) {
    bn->height = ht;
    bn->up = up;
    bn->right = right;
    bn->size = 0;
    // #pragma clang loop unroll(full)
    for (int i = 0; i < DB_SIZE; i++) {
        bn->keys[i] = INT64_MIN;
        bn->addrs[i] = null_pptr;
    }
}

inline void b_node_fill(mBptr nn, bnode *bn, int size, int64_t *keys,
                        pptr *addrs) {
    mram_read(nn, bn, sizeof(bnode));
    bn->size = size;
    for (int i = 0; i < size; i++) {
        bn->keys[i] = keys[i];
        bn->addrs[i] = addrs[i];
    }
    for (int i = size; i < DB_SIZE; i++) {
        bn->keys[i] = INT64_MIN;
        bn->addrs[i] = null_pptr;
    }
    mram_write(bn, nn, sizeof(bnode));
    if (bn->height > 0) {
        for (int i = 0; i < size; i ++) {
            mBptr ch = pptr_to_mbptr(bn->addrs[i]);
            ch->up = mbptr_to_pptr(nn);
        }
    }
}

const int bcnt_limit = LX_BUFFER_SIZE / sizeof(bnode);

inline mBptr alloc_bn() {
    mutex_lock(L3_lock);
    mBptr nn = bbuffer + bcnt++;
    mutex_unlock(L3_lock);
    IN_DPU_ASSERT_EXEC(bcnt < bcnt_limit, { printf("bcnt! of\n"); });
    return nn;
}

void L3_init() {
    assert(bcnt == 1);
    mBptr nn = alloc_bn();
    bnode bn;
    b_node_init(&bn, 0, null_pptr, null_pptr);
    bn.size = 1;
    bn.keys[0] = INT64_MIN;
    bn.addrs[0] = null_pptr;
    mram_write(&bn, nn, sizeof(bnode));
    min_node = root = nn;
}

static inline int64_t b_search(int64_t key, mBptr *addr, int64_t *value) {
    mBptr tmp = root;
    bnode bn;
    while (true) {
        mram_read(tmp, &bn, sizeof(bnode));
        int64_t pred = INT64_MIN;
        pptr nxt_addr = null_pptr;
        // int64_t pred = bn.keys[0]; // can be INT64_MIN
        // pptr nxt_addr = bn.addrs[0]; // can be null_pptr
// #pragma clang loop unroll(full)
        for (int i = 0; i < bn.size; i++) {
            if (bn.keys[i] <= key) {
                pred = bn.keys[i];
                nxt_addr = bn.addrs[i];
            }
            // if (bn.keys[i] <= key && valid_pptr(bn.addrs[i])) {
            //     pred = bn.keys[i];
            //     nxt_addr = bn.addrs[i];
            // }
        }
        // IN_DPU_ASSERT((valid_pptr(nxt_addr) || bn.height == 0), "bs! inv\n");
        if (bn.height > 0) {
            tmp = pptr_to_mbptr(nxt_addr);
        } else {
            *addr = tmp;
            *value = pptr_to_int64(nxt_addr);
            return pred;
        }
    }
}

static inline int64_t b_get_min_key() {
    mBptr tmp = root;
    while (tmp->height > 0) {
        pptr nxt_addr = tmp->addrs[0];
        tmp = pptr_to_mbptr(nxt_addr);
    }
    IN_DPU_ASSERT(tmp->size > 1, "bgmk! inv\n");
    return tmp->keys[1];
}

static inline int get_r(mppptr addrs, int n, int l) {
    int r;
    pptr p1 = addrs[l];
    for (r = l; r < n; r++) {
        pptr p2 = addrs[r];
        if (EQUAL_PPTR(p1, p2)) {
            continue;
        } else {
            break;
        }
    }
    return r;
}

#define L3_TEMP_BUFFER_SIZE (50000)
__mram_noinit int64_t mod_keys[L3_TEMP_BUFFER_SIZE];
__mram_noinit pptr mod_values[L3_TEMP_BUFFER_SIZE];
__mram_noinit pptr mod_addrs[L3_TEMP_BUFFER_SIZE];
__mram_noinit int64_t mod_type[L3_TEMP_BUFFER_SIZE];

__mram_noinit int64_t mod_keys2[L3_TEMP_BUFFER_SIZE / 4];
__mram_noinit pptr mod_values2[L3_TEMP_BUFFER_SIZE / 4];
__mram_noinit pptr mod_addrs2[L3_TEMP_BUFFER_SIZE / 4];
__mram_noinit int64_t mod_type2[L3_TEMP_BUFFER_SIZE / 4];

const int remove_type = 1;
const int change_key_type = 2;
const int underflow_type = 4;

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
        // if (tid < 2 && (l == lft || r == rt) && (r - l < n)) {
        //     mu.lock();
        //     printf("tid=%d\tlft=%d\trt=%d\n", tid, lft, rt);
        //     printf("tid=%d\tl=%d\tr=%d\n", tid, l, r);
        //     for (int i = l; i < r; i ++) {
        //         printf("key=%lld\taddr=%llx\n", mod_keys[i],
        //         mod_addrs[i].addr);
        //     }
        //     mu.unlock();
        // }
        // printf("tid=%d\tl=%d\tr=%d\n", tid, l, r);
        pptr addr = mod_addrs[l];
        mBptr nn;
        pptr up, right;
        int nnsize;
        mBptr nn0;
        if (valid_pptr(addr)) {
            nn0 = nn = pptr_to_mbptr(addr);
            mram_read(nn, &bn, sizeof(bnode));
            up = bn.up;
            right = bn.right;
            nnsize = bn.size;
            for (int i = 0; i < nnsize; i++) {
                nnkeys[i] = bn.keys[i];
                nnvalues[i] = bn.addrs[i];
            }
        } else {
            nn = alloc_bn();
            up = null_pptr;
            right = null_pptr;
            nnsize = 1;
            nnkeys[0] = INT64_MIN;
            nnvalues[0] = mbptr_to_pptr(root);
            if (ht > 0) {
                root->up = mbptr_to_pptr(nn);
                for (int i = l; i < r; i++) {
                    pptr child_addr = mod_values[i];
                    mBptr ch = pptr_to_mbptr(child_addr);
                    // printf("ch=%llx\n", ch);
                    ch->up = mbptr_to_pptr(nn);
                }
            }
            root = nn;
        }
        b_node_init(&bn, ht, up, right);
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
                mod_keys2[nxtrt] = bn.keys[0];
                mod_values2[nxtrt] = mbptr_to_pptr(nn);
                mod_addrs2[nxtrt] = bn.up;
                nxtrt++;
                IN_DPU_ASSERT_EXEC(bn.keys[0] != INT64_MIN, {
                    printf("mod_keys2 = INT64_MIN\n");
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
                    printf("i=%d\ttotsize=%d\n", i, totsize);
                });
            }
            if (bn.size == DB_SIZE ||
                (i + HF_DB_SIZE + 1 == totsize && bn.size > HF_DB_SIZE)) {
                // if (bn.keys[0] == -7125798835171897ll &&
                //     bn.keys[1] == -6795333434612133ll) {
                //     printf("**\n");
                //     printf("bbuffer=%x bcnt=%d\n", bbuffer, bcnt);
                //     printf("ht=%d addr=%x\n", ht, nn0);
                //     printf("lft=%d\trt=%d\n", lft, rt);
                //     printf("l=%d\tr=%d\tnnl=%d\n", l0, r, nnl);
                //     printf("i=%d\ttotsize=%d\n", i, totsize);
                //     for (int x = 0; x < nnsize; x++) {
                //         printf("nn[%d]=%lld\n", x, nnkeys[x]);
                //     }
                //     mram_read(nn0, &bn, sizeof(bnode));
                //     printf("nn0size=%lld nnsize=%d\n", bn.size, nnsize);
                //     for (int x = 0; x < bn.size; x++) {
                //         printf("nn0[%d]=%lld\n", x, bn.keys[x]);
                //     }
                //     for (int x = l0; x < r; x++) {
                //         printf("mod[%d]=%lld\n", x, mod_keys[x]);
                //     }
                //     printf("i=%d\ttotsize=%d\n", i, totsize);
                //     printf("**\n");
                // }
                for (int i = 0; i < bn.size; i++) {
                    if (bn.height > 0) {
                        assert(valid_pptr(bn.addrs[i]));
                        mBptr ch = pptr_to_mbptr(bn.addrs[i]);
                        ch->up = mbptr_to_pptr(nn);
                    }
                }
                if (nnl == nnsize && l == r) {
                    mram_write(&bn, nn, sizeof(bnode));
                } else {
                    pptr up = bn.up, right = bn.right;
                    int64_t ht = bn.height;

                    mBptr nxt_nn = alloc_bn();
                    bn.right = mbptr_to_pptr(nxt_nn);
                    mram_write(&bn, nn, sizeof(bnode));

                    b_node_init(&bn, ht, up, right);
                    nn = nxt_nn;
                }

                // pptr up = bn.up, right = bn.right;
                // int64_t ht = bn.height;
                // b_node_init(&bn, ht, up, right);
                // nn = alloc_bn();
            }
            if (nnl == nnsize && l == r) {
                IN_DPU_ASSERT_EXEC(i + 1 == totsize, {
                    printf("i+1 != totsize\n");
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
                    printf("i=%d\ttotsize=%d\n", i, totsize);
                });
            }
        }
        if (bn.size != 0) {
            for (int i = 0; i < bn.size; i++) {
                if (bn.height > 0) {
                    assert(valid_pptr(bn.addrs[i]));
                    mBptr ch = pptr_to_mbptr(bn.addrs[i]);
                    ch->up = mbptr_to_pptr(nn);
                }
            }
            mram_write(&bn, nn, sizeof(bnode));
        }
        IN_DPU_ASSERT_EXEC(l == r && nnl == nnsize, {
            printf("l=%d\tr=%d\tnnl=%d\tnnsize=%d\n", l, r, nnl, nnsize);
        });
    }
    L3_lfts[tid] = nxtlft;
    L3_rts[tid] = nxtrt;
    // if (nxtlft != nxtrt) {
    //     printf("after ht=%d\ttid=%d\tlft=%d\trt=%d\n", ht, tid, nxtlft,
    //     nxtrt);
    // }
}

const int SERIAL_HEIGHT = 2;

void b_insert_parallel(int n, int l, int r) {
    int tid = me();

    barrier_wait(&L3_barrier2);

    // bottom up
    for (int i = l; i < r; i++) {
        int64_t key = mod_keys[i];
        int64_t value;

        mBptr nn;
        b_search(key, &nn, &value);
        mod_addrs[i] = mbptr_to_pptr(nn);
        if (i > 0) {
            int64_t keyl = mod_keys[i - 1];
            IN_DPU_ASSERT_EXEC(keyl < key, {
                printf("bip! eq %d %d %d %d %lld %lld\n", i, tid, l, r, key,
                       keyl);
                for (int i = 0; i < n; i++) {
                    printf("key[%d]=%lld\n", i, mod_keys[i]);
                }
            });
        }
    }

    L3_n = n;
    barrier_wait(&L3_barrier1);

    for (int ht = 0; ht <= root->height + 1; ht++) {
        if (ht < SERIAL_HEIGHT) {
            // distribute work
            n = L3_n;
            if (tid == 0) {
                printf("PARALLEL:%d\n", n);
            }
            int lft = n * tid / NR_TASKLETS;
            int rt = n * (tid + 1) / NR_TASKLETS;
            // printf("t0 n=%d\ttid=%d\tlft=%d\trt=%d\n", n, tid, lft, rt);
            if (rt > lft) {
                if (lft != 0) {
                    lft = get_r(mod_addrs, n, lft - 1);
                }
                assert(rt > 0);
                rt = get_r(mod_addrs, n, rt - 1);
            }

            // printf("t1 n=%d\ttid=%d\tlft=%d\trt=%d\n", n, tid, lft, rt);
            L3_lfts[tid] = lft;
            L3_rts[tid] = rt;
            // printf("l=%d\tr=%d\n", L3_lfts[i], L3_rts[i]);
            barrier_wait(&L3_barrier2);

            // execute
            b_insert_onelevel(n, tid, ht);
            barrier_wait(&L3_barrier3);

            // distribute work
            if (tid == 0) {
                n = 0;
                for (int i = 0; i < NR_TASKLETS; i++) {
                    for (int j = L3_lfts[i]; j < L3_rts[i]; j++) {
                        mod_keys[n] = mod_keys2[j];
                        mod_values[n] = mod_values2[j];
                        mod_addrs[n] = mod_addrs2[j];
                        assert(mod_keys[n] != INT64_MIN);
                        if (n > 0) {
                            IN_DPU_ASSERT(mod_keys[n] > mod_keys[n - 1],
                                          "bip! rev\n");
                        }
                        n++;
                    }
                }
                L3_n = n;
                // printf("n=%d\n", n);
            }
            barrier_wait(&L3_barrier1);
        } else {
            if (tid == 0 && n > 0) {
                printf("SOLO:%d\n", n);
                L3_lfts[0] = 0;
                L3_rts[0] = n;
                b_insert_onelevel(n, tid, ht);
                n = L3_rts[0];
                for (int i = 0; i < n; i++) {
                    mod_keys[i] = mod_keys2[i];
                    mod_values[i] = mod_values2[i];
                    mod_addrs[i] = mod_addrs2[i];
                }
            } else {
                break;
            }
        }
    }
    barrier_wait(&L3_barrier3);
}

// void b_insert_parallel(int n, int l, int r) {
//     int tid = me();

//     // bottom up

//     for (int i = l; i < r; i++) {
//         int64_t key = mod_keys[i];
//         int64_t value;

//         mBptr nn;
//         b_search(key, &nn, &value);
//         mod_addrs[i] = mbptr_to_pptr(nn);
//         if (i > 0) {
//             int64_t keyl = mod_keys[i - 1];
//             IN_DPU_ASSERT(keyl < key, "bip! eq\n");
//         }
//     }

//     // printf("%d %d %d\n", tid, l, r);
//     // barrier_wait(&L3_barrier2);

//     // if (tid == 0) {
//     //     for (int i = 0; i < n; i++) {
//     //         printf("%2d\t%llx\n", i, mod_addrs[i].addr);
//     //     }
//     // }

//     L3_n = n;
//     barrier_wait(&L3_barrier1);

//     // if (tid == 0) {
//     //     for (int i = 0; i < n; i ++) {
//     //         int64_t key = mod_keys[i];
//     //         pptr value = mod_values[i];
//     //         pptr addr = mod_addrs[i];
//     //         printf("[%d]key=%llx\tvalue=%llx\taddr=%llx\n", i, key,
//     //         PPTR_TO_I64(value), PPTR_TO_I64(addr));
//     //     }
//     // }
//     // EXIT();

//     for (int ht = 0; ht <= root->height + 1; ht++) {
//         if (ht < SERIAL_HEIGHT) {
//             // distribute work
//             {
//                 printf("distributed %d\n", tid);
//                 if (tid == 0) {
//                     for (int i = 0; i < NR_TASKLETS; i++) {
//                         // int i = tid;
//                         n = L3_n;
//                         int lft = n * i / NR_TASKLETS;
//                         int rt = n * (i + 1) / NR_TASKLETS;
//                         // printf("t0 n=%d\ttid=%d\tlft=%d\trt=%d\n", n, i,
//                         lft,
//                         // rt);
//                         if (rt > lft) {
//                             if (lft != 0) {
//                                 lft = get_r(mod_addrs, n, lft - 1);
//                             }
//                             rt = get_r(mod_addrs, n, rt);
//                         }
//                         // printf("t1 n=%d\ttid=%d\tlft=%d\trt=%d\n", n, i,
//                         lft,
//                         // rt);
//                         L3_lfts[i] = lft;
//                         L3_rts[i] = rt;

//                         if (rt != n && lft != rt) {
//                             pptr p1 = mod_addrs[lft];
//                             pptr p2 = mod_addrs[rt];
//                             IN_DPU_ASSERT_EXEC(!EQUAL_PPTR(p1, p2), {
//                                 printf(
//                                     "bi! div2 "
//                                     "tid=%d\tlft=%d\trt=%d"
//                                     "\tn=%d"
//                                     "ht=%d"
//                                     "\n",
//                                     i, lft, rt, n, ht);
//                                 for (int x = lft; x <= rt; x++) {
//                                     p1 = mod_addrs[x];
//                                     printf("addr[%d]=%llx\n", x, p1);
//                                 }
//                             });
//                         }
//                     }
//                 }
//             }

//             barrier_wait(&L3_barrier2);

//             // execute
//             b_insert_onelevel(n, tid, ht);
//             barrier_wait(&L3_barrier3);

//             // distribute work
//             if (tid == 0) {
//                 n = 0;
//                 for (int i = 0; i < NR_TASKLETS; i++) {
//                     printf("l=%d\tr=%d\n", L3_lfts[i], L3_rts[i]);
//                     for (int j = L3_lfts[i]; j < L3_rts[i]; j++) {
//                         pptr val, addr;
//                         mod_keys[n] = mod_keys[j];
//                         val = mod_values[n] = mod_values[j];
//                         addr = mod_addrs[n] = mod_addrs[j];
//                         printf("n=%d\tk=%llx\tv=%llx\ta=%llx\n", n,
//                         mod_keys[n],
//                                PPTR_TO_I64(val), PPTR_TO_I64(addr));
//                         n++;
//                     }
//                 }
//                 L3_n = n;
//                 // printf("n=%d\n", n);
//             }
//             barrier_wait(&L3_barrier1);
//         } else {
//             if (tid == 0) {
//                 // printf("SOLO:%d\n", n);
//                 L3_lfts[0] = 0;
//                 L3_rts[0] = n;
//                 b_insert_onelevel(n, tid, ht);
//                 n = L3_rts[0];
//             } else {
//                 break;
//             }
//         }
//     }
//     barrier_wait(&L3_barrier3);
// }