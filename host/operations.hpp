#pragma once

#include <iostream>
#include <cstdio>
#include <libcuckoo/cuckoohash_map.hh>
#include "task_host.hpp"
#include "timer.hpp"
#include "host.hpp"
#include "util.hpp"
#include <parlay/primitives.h>
#include <parlay/range.h>
#include <parlay/sequence.h>

using namespace std;

extern bool print_debug;
extern int64_t epoch_number;

int maxheight; // setting max height

void init_skiplist(uint32_t height) {
    epoch_number ++;
    ASSERT(height > LOWER_PART_HEIGHT);
    maxheight = height;

    printf("\n********** INIT SKIP LIST **********\n");

    printf("\n**** INIT L2 ****\n");
    pptr l2node = null_pptr;
    {
        init_io_buffer(false);
        set_io_buffer_type(L2_INIT_TSK, L2_INIT_REP);
        L2_init_task sit = (L2_init_task){
            .key = LLONG_MIN, .addr = null_pptr, .height = LOWER_PART_HEIGHT};
        int target = hash_to_dpu(LLONG_MIN, 0, nr_of_dpus);
        push_task(&sit, sizeof(L2_init_task), sizeof(L2_init_reply), target);
        ASSERT(exec());  // insert upper part -INF
        
        L2_init_reply _;
        apply_to_all_reply(false, _, [&](L2_init_reply &ssr, int i, int j) {
            (void)i;(void)j;
            ASSERT(buffer_state == receive_direct);
            ASSERT(l2node.id == INVALID_DPU_ID); // happen only once
            l2node = ssr.addr;
        });
    }

    pptr l3node = null_pptr;
    {
        init_io_buffer(true);
        set_io_buffer_type(L3_INIT_TSK, L3_INIT_REP);
        L3_init_task tit = (L3_init_task){
            .key = LLONG_MIN, .addr = l2node, .height = height - LOWER_PART_HEIGHT};
        push_task(&tit, sizeof(L3_init_task), sizeof(L3_init_reply), -1);
        ASSERT(exec());  // insert upper part -INF

        L3_init_reply _;
        apply_to_all_reply(false, _, [&](L3_init_reply &tsr, int i, int j) {
            (void)i;(void)j;
            ASSERT(buffer_state == receive_broadcast);
            ASSERT(l3node.id == INVALID_DPU_ID); // happen only once
            l3node = tsr.addr;
        });
    }

    {
        init_io_buffer(false);
        set_io_buffer_type(L2_BUILD_UP_TSK, EMPTY);
        uint32_t target = hash_to_dpu(LLONG_MIN, 0, nr_of_dpus);
        L2_build_up_task sbut = (L2_build_up_task){
            .addr = l2node, .up = (pptr){.id = target, .addr = l3node.addr}};
        push_task(&sbut, sizeof(L2_build_up_task), 0, target);
        ASSERT(!exec());  // insert upper part -INF
    }
}

void get(int length) {
    epoch_number++;
    // printf("START GET\n");

    libcuckoo::cuckoohash_map<int64_t, int> key2offset;
    key2offset.reserve(length * 2);

    init_io_buffer(false);
    set_io_buffer_type(L2_GET_TSK, L2_GET_REP);
    parlay::parallel_for(0, nr_of_dpus, [&](size_t i) {
        int l = length * i / nr_of_dpus;
        int r = length * (i + 1) / nr_of_dpus;
        for (int j = l; j < r; j++) {
            L2_get_task tgt = (L2_get_task){.key = op_keys[j]};
            push_task(&tgt, sizeof(L2_get_task), sizeof(L2_get_task), i);
        }
    });
    ASSERT(exec());

    L2_get_reply _;
    apply_to_all_reply(true, _, [&](L2_get_reply &sgr, int i, int j) {
        ASSERT(buffer_state == receive_direct);
        int offset = (int64_t)length * i / nr_of_dpus + j;
        ASSERT(offset >= 0 &&
               offset < ((int64_t)length * (i + 1) / nr_of_dpus));
        op_results[offset] = sgr.available;
    });
}

template <class F> // [invalid, valid] [l, r)
inline int binary_search_local(int l, int r, F f) {
    ASSERT(l >= 0 && r >= 0 && r > l);
    int mid = (l + r) >> 1;
    while (r - l > 1) {
        if (f(mid)) {
            l = mid;
        } else {
            r = mid;
        }
        mid = (l + r) >> 1;
    }
    return l;
}

timer predecessor_L3_task_generate("predecessor_L3_task_generate");
timer predecessor_L3("predecessor_L3");
timer predecessor_L3_get_result("predecessor_L3_get_result");
timer predecessor_L2("predecessor_L2");
timer predecessor_L2_execute("predecessor_L2_execute");
timer predecessor_L2_task_generate("predecessor_L2_task_generate");
timer predecessor_L2_get_result("predecessor_L2_get_result");
timer predecessor_L2_pack("predecessor_L2_pack");

void predecessor(int length, int32_t *heights = NULL, pptr **paths = NULL,
                 pptr **rights = NULL, int64_t **chks = NULL,
                 int64_t *keys = NULL) {
    if (keys == NULL) {
        keys = op_keys;
    }

    epoch_number++;
    // printf("START PREDECESSOR\n");

    predecessor_L3.start();
    {
        predecessor_L3_task_generate.start();
        init_io_buffer(false);
        set_io_buffer_type(L3_SEARCH_TSK, L3_SEARCH_REP);
        parlay::parallel_for(0, nr_of_dpus, [&](size_t i) {
            int l = (int64_t)length * i / nr_of_dpus;
            int r = (int64_t)length * (i + 1) / nr_of_dpus;
            for (int j = l; j < r; j++) {
                L3_search_task tst = (L3_search_task){.key = keys[j]};
                push_task(&tst, sizeof(L3_search_task), sizeof(L3_search_reply),
                          i);
            }
        });
        predecessor_L3_task_generate.end();

        ASSERT(exec());

        predecessor_L3_get_result.start();
        L3_search_reply _;
        apply_to_all_reply(true, _, [&](L3_search_reply &tsr, int i, int j) {
            ASSERT(buffer_state == receive_direct);
            int offset = (int64_t)length * i / nr_of_dpus + j;
            ASSERT(offset >= 0 &&
                   offset < ((int64_t)length * (i + 1) / nr_of_dpus));
            op_addrs[offset] = tsr.addr;
        });
        predecessor_L3_get_result.end();
    }
    predecessor_L3.end();

    // if (paths == NULL) return;

    predecessor_L2.start();
    {
        parlay::parallel_for(0, length, [&](size_t i) {
            op_heights[i] = LOWER_PART_HEIGHT - 1;
        });

        int cnt = 0;
        while (true) {
            cnt++;
            predecessor_L2_task_generate.start();

            predecessor_L2_pack.start();
            init_io_buffer(false);
            set_io_buffer_type(L2_GET_NODE_TSK, L2_GET_NODE_REP);

            auto task_start = parlay::tabulate(length, [](int i) -> bool {
                return i >= 0 && op_heights[i] >= -1 && not_equal_pptr(op_addrs[i - 1], op_addrs[i]);
            });
            task_start[0] = (op_heights[0] >= -1);

            auto ll = parlay::pack_index(task_start);

            // auto new_idx = parlay::delayed_tabulate(idx.size(), [&](int i) -> bool {
            //     return op_heights[idx[i]] >= -1;
            // });
            // auto ll_idx = parlay::pack_index(new_idx);
            // auto rr_idx = parlay::map(ll_idx, [](int x) -> int {return (x + 1);});
            // auto ll = parlay::map(ll_idx, [&](int x) -> int {return idx[x];});
            // auto rr = parlay::map(ll_idx.cut, [&](int x) -> int {return idx[x + 1];});
            predecessor_L2_pack.end();
            // idx.push_back(length);
            // auto rr = parlay::map(rr_idx, [&](int x) -> int {return idx[x];});
            // auto ll = parlay::filter(idx, [&](int i) {
            //     return op_heights[i] >= -1;
            // });
            // auto rr = parlay::tabulate()
            // auto target = parlay::map(idx, [&](int x) {
            //     return op_addrs[x].id;
            // });

            // parlay::parallel_for(0, length, [&](size_t i) {
            //     op_taskpos[i] = -1;
            // });

            parlay::parallel_for(0, ll.size(), [&](size_t x) {
                int i = ll[x];
                L2_get_node_task sgnt = (L2_get_node_task){
                    .addr = op_addrs[i], .height = op_heights[i]};
                op_taskpos[i] =
                    push_task(&sgnt, sizeof(L2_get_node_task),
                                sizeof(L2_get_node_reply), op_addrs[i].id);
            });

            predecessor_L2_task_generate.end();

            predecessor_L2_execute.start();
            if (!exec()) break;
            predecessor_L2_execute.end();
            // cout<<"**"<<endl;

            // parlay::parallel_for(0, length, [&](size_t i) {
            predecessor_L2_get_result.start();
            parlay::parallel_for(0, ll.size(), [&](size_t i) {
                int loop_l = ll[i];
                int loop_r = (i == ll.size() - 1) ? length : ll[i + 1];
                L2_get_node_reply *sgnr = (L2_get_node_reply *)get_reply(
                    op_taskpos[loop_l], sizeof(L2_get_node_reply), op_addrs[loop_l].id);
                int64_t chk = sgnr->chk;
                pptr r = sgnr->right;

                // auto tmpkeys = parlay::make_slice(keys + loop_l, keys + loop_r);
                // auto divider = parlay::find_if(tmpkeys, [](int64_t v) {return v >= chk;})
                // auto tmpheights = parlay::make_slice(op_heights + )
                // int mid = divider - (keys + loop_l);
                int validr = binary_search_local(loop_l, loop_r, [&](int i) {return (op_heights[i] != -2);}) + 1;

                parlay::parallel_for(loop_l, validr, [&](size_t j) {
                    ASSERT(op_heights[j] >= -1);
                    if (op_heights[j] == -1) {
                        op_results[j] = chk;
                        op_heights[j]--;
                    } else {
                        if (r.id != INVALID_DPU_ID && keys[j] >= chk) {
                            op_addrs[j] = r;
                        } else {
                            if (heights != NULL) {
                                int ht = op_heights[j];
                                // if (ht < 0) {
                                //     cout << ht << endl;
                                //     exit(-1);
                                // }
                                ASSERT(ht >= 0);
                                if (ht < heights[j]) {
                                    paths[j][ht] = op_addrs[loop_l];
                                    if ((int)j < length - 1 &&
                                        &paths[j][ht] >= &paths[j + 1][0]) {
                                        cout << j << ' ' << ht << endl;
                                        cout << paths[j] << ' ' << &paths[j][ht]
                                             << ' ' << paths[j + 1] << endl;
                                        exit(-1);
                                    }
                                    rights[j][ht] = r;
                                    chks[j][ht] = chk;
                                }
                            }
                            op_heights[j]--;
                        }
                    }
                }, 1000);
            });
            predecessor_L2_get_result.end();
            buffer_state = idle;
        }
        cout << "Rounds: " << cnt << endl;
    }
    predecessor_L2.end();

    // for (int i = 0; i < length; i ++) {
    //     printf("%ld %ld\n", keys[i], op_results[i]);
    // }
}

auto deduplication(int64_t *arr, int &length) {  // assume sorted
    auto seq = parlay::make_slice(arr, arr + length);
    parlay::sort_inplace(seq);

    auto dup = parlay::tabulate(
        length, [&](int i) -> bool { return i == 0 || seq[i] != seq[i - 1]; });
    auto packed = parlay::pack(seq, dup);
    length = packed.size();
    return packed;

    // sort(arr, arr + length);
    // auto seq = parlay::sequence(arr, arr + length);
    // parlay::sort_inplace(seq);
    // parlay::sort_inplace();
    // parlay::sort_inplace
    // arr = seq.data();
    // int l = 1;
    // for (int i = 1; i < length; i++) {
    //     if (arr[i] != arr[i - 1]) {
    //         arr[l++] = arr[i];
    //     }
    // }
    // length = l;
}

int insert_offset_buffer[BATCH_SIZE * 2];
timer insert_init("insert_init");
timer insert_predecessor("insert_predecessor");
timer insert_L2("insert_L2");
timer insert_L2_taskgen("insert_L2_taskgen");
timer insert_L3("insert_L3");
timer insert_up("insert_up");
timer insert_lr("insert_lr");
timer insert_lr_taskgen("insert_lr_taskgen");
timer insert_lr_execute("insert_lr_execute");

pptr insert_path_addrs_buf[BATCH_SIZE * 2];
pptr insert_path_rights_buf[BATCH_SIZE * 2];
int64_t insert_path_chks_buf[BATCH_SIZE * 2];

pptr *insert_path_addrs[BATCH_SIZE];
pptr *insert_path_rights[BATCH_SIZE];
int64_t *insert_path_chks[BATCH_SIZE];

void insert(int length) {
    // printf("\n********** INIT SKIP LIST **********\n");

    insert_init.start();

    printf("\n**** INIT HEIGHT ****\n");
    epoch_number++;
    auto keys = deduplication(op_keys, length);

    parlay::parallel_for(0, length, [&](size_t i) {
        int32_t t = randint64(parlay::worker_id());
        t = t & (-t);
        int h = __builtin_ctz(t) + 1;
        h = min(h, maxheight);
        insert_heights[i] = h;
    });

    parlay::slice ins_heights_slice =
        parlay::make_slice(insert_heights, insert_heights + length);

    auto height_prefix_sum_pair = parlay::scan(ins_heights_slice);
    auto height_total = height_prefix_sum_pair.second;
    auto height_prefix_sum = height_prefix_sum_pair.first;

    auto insert_path_addrs = parlay::map(height_prefix_sum, [&](int32_t x) {
        return insert_path_addrs_buf + x;
    });
    auto insert_path_rights = parlay::map(height_prefix_sum, [&](int32_t x) {
        return insert_path_rights_buf + x;
    });
    auto insert_path_chks = parlay::map(
        height_prefix_sum, [&](int32_t x) { return insert_path_chks_buf + x; });

    ASSERT(height_total < BATCH_SIZE * 2);

    insert_init.end();

    printf("\n**** INSERT PREDECESSOR ****\n");
    insert_predecessor.start();
    {
        predecessor(length, insert_heights, insert_path_addrs.data(),
                    insert_path_rights.data(), insert_path_chks.data(),
                    keys.data());
    }
    insert_predecessor.end();

    printf("\n**** INSERT L2 ****\n");
    insert_L2.start();
    {
        insert_L2_taskgen.start();
        // insert_task_generate.start();
        init_io_buffer(false);
        set_io_buffer_type(L2_INSERT_TSK, L2_INSERT_REP);

        parlay::parallel_for(0, length, [&](size_t i) {
            int target = hash_to_dpu(keys[i], 0, nr_of_dpus);
            L2_insert_task sit = (L2_insert_task){
                .key = keys[i], .addr = null_pptr, .height = insert_heights[i]};
            op_taskpos[i] = push_task(&sit, sizeof(L2_insert_task),
                                      sizeof(L2_insert_reply), target);
        });
        insert_L2_taskgen.end();

        ASSERT(exec());

        parlay::parallel_for(0, length, [&](size_t i) {
            if (op_taskpos[i] != -1) {
                L2_insert_reply *sir = (L2_insert_reply *)get_reply(
                    op_taskpos[i], sizeof(L2_insert_reply),
                    hash_to_dpu(keys[i], 0, nr_of_dpus));
                op_addrs[i] = sir->addr;
                op_taskpos[i] = -1;
            }
        });
        buffer_state = idle;
    }
    insert_L2.end();

    printf("\n**** INSERT L3 ****\n");
    insert_L3.start();
    bool reach_L3 = false;
    {
        // insert_task_generate.start();
        init_io_buffer(true);
        set_io_buffer_type(L3_INSERT_TSK, L3_INSERT_REP);

        int L3_id[MAX_TASK_COUNT_PER_DPU];
        atomic<int> cnt = 0;

        parlay::parallel_for(0, length, [&](size_t i) {
            if (insert_heights[i] > LOWER_PART_HEIGHT) {
                L3_id[cnt++] = i;
                reach_L3 = true;
            }
        });
        sort(L3_id, L3_id + cnt.load());

        for (int t = 0; t < cnt; t++) {
            int i = L3_id[t];
            // printf("T3: %d %d %ld\n", t, i, keys[i]);
            L3_insert_task tit = (L3_insert_task){
                .key = keys[i],
                .addr = op_addrs[i],
                .height = insert_heights[i] - LOWER_PART_HEIGHT};
            op_taskpos[i] = push_task(&tit, sizeof(L3_insert_task),
                                      sizeof(L3_insert_reply), -1);
        }

        ASSERT(exec() == reach_L3);

        if (reach_L3) {
            parlay::parallel_for(0, length, [&](size_t i) {
                if (op_taskpos[i] != -1) {
                    L3_insert_reply *tir = (L3_insert_reply *)get_reply(
                        op_taskpos[i], sizeof(L3_insert_reply), -1);
                    op_addrs2[i] = tir->addr;
                    op_taskpos[i] = -1;
                }
            });
            buffer_state = idle;
            // insert_task_generate.end();
        }
    }
    insert_L3.end();

    printf("\n**** BUILD L2 UP ****\n");
    insert_up.start();
    if (reach_L3) {
        // insert_task_generate.start();
        init_io_buffer(false);
        set_io_buffer_type(L2_BUILD_UP_TSK, EMPTY);

        parlay::parallel_for(0, length, [&](size_t i) {
            if (insert_heights[i] > LOWER_PART_HEIGHT) {
                int target = hash_to_dpu(keys[i], 0, nr_of_dpus);
                L2_build_up_task sbut =
                    (L2_build_up_task){.addr = op_addrs[i], .up = op_addrs2[i]};
                push_task(&sbut, sizeof(L2_build_up_task), 0, target);
            }
        });

        ASSERT(!exec());
        // insert_task_generate.end();
    }
    insert_up.end();

    printf("\n**** BUILD L2 LR ****\n");
    insert_lr.start();
    {
        insert_lr_taskgen.start();
        const int BLOCK = 128;

        int node_count[LOWER_PART_HEIGHT + 1];
        int node_count_threadlocal[BLOCK + 1][LOWER_PART_HEIGHT + 1];
        int *node_offset_threadlocal[BLOCK + 1][LOWER_PART_HEIGHT + 1];

        memset(node_count, 0, sizeof(node_count));
        memset(node_count_threadlocal, 0, sizeof(node_count_threadlocal));

        int *node_id[LOWER_PART_HEIGHT + 1];

        std::mutex reduce_mutex;

        parlay::parallel_for(
            0, BLOCK,
            [&](size_t i) {
                int l = (int64_t)length * i / BLOCK;
                int r = (int64_t)length * (i + 1) / BLOCK;
                for (int j = l; j < r; j++) {
                    for (int ht = 0; ht < insert_heights[j]; ht++) {
                        if (ht >= LOWER_PART_HEIGHT) break;
                        node_count_threadlocal[i][ht]++;
                    }
                }
                reduce_mutex.lock();
                for (int ht = 0; ht < LOWER_PART_HEIGHT; ht++) {
                    node_count[ht] += node_count_threadlocal[i][ht];
                }
                reduce_mutex.unlock();
            },
            1);

        node_id[0] = insert_offset_buffer;
        for (int i = 1; i <= LOWER_PART_HEIGHT; i++) {
            node_id[i] = node_id[i - 1] + node_count[i - 1];
            // printf("*1 %d %ld\n", i, node_id[i] - node_id[i - 1]);
        }
        ASSERT(node_id[LOWER_PART_HEIGHT] <=
               insert_offset_buffer + BATCH_SIZE * 2);

        for (int i = 0; i <= BLOCK; i++) {
            for (int ht = 0; ht < LOWER_PART_HEIGHT; ht++) {
                if (i == 0) {
                    node_offset_threadlocal[0][ht] = node_id[ht];
                } else {
                    node_offset_threadlocal[i][ht] =
                        node_offset_threadlocal[i - 1][ht] +
                        node_count_threadlocal[i - 1][ht];
                }
                if (i == BLOCK) {
                    ASSERT(node_offset_threadlocal[i][ht] == node_id[ht + 1]);
                }
                // printf("*2 %d %d %ld\n", i, ht,
                //        node_offset_threadlocal[i][ht] -
                //        insert_offset_buffer);
            }
        }

        parlay::parallel_for(0, BLOCK, [&](size_t i) {
            int l = (int64_t)length * i / BLOCK;
            int r = (int64_t)length * (i + 1) / BLOCK;
            for (int j = l; j < r; j++) {
                for (int ht = 0; ht < insert_heights[j]; ht++) {
                    if (ht >= LOWER_PART_HEIGHT) continue;
                    *(node_offset_threadlocal[i][ht]++) = j;
                }
            }
        });

        init_io_buffer(false);
        set_io_buffer_type(L2_BUILD_LR_TSK, EMPTY);

        bool print = false;
        for (int ht = 0; ht < LOWER_PART_HEIGHT; ht++) {
            parlay::parallel_for(0, node_count[ht], [&](size_t j) {
                // for (int j = 0; j < node_count[ht]; j ++) {
                int id = node_id[ht][j];
                int l = (j == 0) ? -1 : node_id[ht][j - 1];
                int r =
                    ((int)j == node_count[ht] - 1) ? -1 : node_id[ht][j + 1];
                ASSERT(insert_heights[id] > ht);
                ASSERT(l == -1 || insert_heights[l] > ht);
                ASSERT(r == -1 || insert_heights[r] > ht);
                if (l == -1 || !equal_pptr(insert_path_addrs[id][ht],
                                           insert_path_addrs[l][ht])) {
                    // no new node on the left,
                    // build right of the
                    // predecessor, build left
                    // to the predecessor
                    // ASSERT(l == -1);
                    L2_build_lr_task sblt =
                        (L2_build_lr_task){.addr = insert_path_addrs[id][ht],
                                           .chk = keys[id],
                                           .height = ht,
                                           .val = op_addrs[id]};
                    push_task(&sblt, sizeof(L2_build_lr_task), 0, sblt.addr.id);
                    if (print) {
                        printf("%d-%x %ld %ld %d-%x\n", sblt.addr.id,
                               sblt.addr.addr, sblt.chk, sblt.height,
                               sblt.val.id, sblt.val.addr);
                    }

                    sblt = (L2_build_lr_task){.addr = op_addrs[id],
                                              .chk = -1,
                                              .height = -1 - ht,
                                              .val = insert_path_addrs[id][ht]};
                    push_task(&sblt, sizeof(L2_build_lr_task), 0, sblt.addr.id);
                    if (print) {
                        printf("%d-%x %ld %ld %d-%x\n", sblt.addr.id,
                               sblt.addr.addr, sblt.chk, sblt.height,
                               sblt.val.id, sblt.val.addr);
                    }
                } else {
                    L2_build_lr_task sblt =
                        (L2_build_lr_task){.addr = op_addrs[id],
                                           .chk = -1,
                                           .height = -1 - ht,
                                           .val = op_addrs[l]};
                    push_task(&sblt, sizeof(L2_build_lr_task), 0, sblt.addr.id);
                    if (print) {
                        printf("%d-%x %ld %ld %d-%x\n", sblt.addr.id,
                               sblt.addr.addr, sblt.chk, sblt.height,
                               sblt.val.id, sblt.val.addr);
                    }
                }
                if (r == -1 || !equal_pptr(insert_path_addrs[id][ht],
                                           insert_path_addrs[r][ht])) {
                    // ASSERT(r == -1);
                    L2_build_lr_task sblt =
                        (L2_build_lr_task){.addr = op_addrs[id],
                                           .chk = insert_path_chks[id][ht],
                                           .height = ht,
                                           .val = insert_path_rights[id][ht]};
                    push_task(&sblt, sizeof(L2_build_lr_task), 0, sblt.addr.id);
                    if (print) {
                        printf("%d-%x %ld %ld %d-%x\n", sblt.addr.id,
                               sblt.addr.addr, sblt.chk, sblt.height,
                               sblt.val.id, sblt.val.addr);
                    }

                    if (insert_path_rights[id][ht].id != INVALID_DPU_ID) {
                        // ASSERT(false);
                        sblt = (L2_build_lr_task){
                            .addr = insert_path_addrs[id][ht],
                            .chk = -1,
                            .height = -1 - ht,
                            .val = op_addrs[id]};
                        push_task(&sblt, sizeof(L2_build_lr_task), 0,
                                  sblt.addr.id);
                        if (print) {
                            printf("%d-%x %ld %ld %d-%x\n", sblt.addr.id,
                                   sblt.addr.addr, sblt.chk, sblt.height,
                                   sblt.val.id, sblt.val.addr);
                        }
                    }
                } else {
                    L2_build_lr_task sblt =
                        (L2_build_lr_task){.addr = op_addrs[id],
                                           .chk = keys[r],
                                           .height = ht,
                                           .val = op_addrs[r]};
                    push_task(&sblt, sizeof(L2_build_lr_task), 0, sblt.addr.id);
                    if (print) {
                        printf("%d-%x %ld %ld %d-%x\n", sblt.addr.id,
                               sblt.addr.addr, sblt.chk, sblt.height,
                               sblt.val.id, sblt.val.addr);
                    }
                }
                // }
            });

            // for (int j = 0; j < node_count[i]; j ++) {
            //     printf("%d ", node_id[i][j]);
            // }
            // printf("\n");
        }

        insert_lr_taskgen.end();
        insert_lr_execute.start();
        ASSERT(!exec());
        insert_lr_execute.end();
    }
    insert_lr.end();
    // cout<<"FINISHED!"<<endl;

    // for (int i = 0; i < length; i++) {
    //     printf("%d %d ", i, insert_heights[i]);
    //     if (insert_heights[i] > LOWER_PART_HEIGHT) {
    //         print_pptr(op_addrs[i], " ");
    //         print_pptr(op_addrs2[i], "\n");
    //     } else {
    //         print_pptr(op_addrs[i], "\n");
    //     }
    // }
    // exit(-1);
}

timer remove_task_generate("remove_task_generate");

void remove(int length) {
    remove_task_generate.start();
    epoch_number++;
    auto keys = deduplication(op_keys, length);
    init_io_buffer(true);
    set_io_buffer_type(L3_REMOVE_TSK, EMPTY);

    for (int i = 0; i < length; i++) {
        L3_remove_task trt = (L3_remove_task){.key = keys[i]};
        push_task(&trt, sizeof(L3_remove_task), 0, -1);
    }
    remove_task_generate.end();

    ASSERT(!exec());
    // buffer_state = idle;
    // if (print_debug) {
    //     print_log();
    // }
    // exit(-1);
}
