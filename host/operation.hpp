#pragma once
#include <parlay/primitives.h>

#include "dpu_control.hpp"
#include "oracle.hpp"
#include "task.hpp"
#include "task_framework_host.hpp"
#include "value.hpp"

// Range Scan
#include "host_util.hpp"

using namespace std;
using namespace parlay;

namespace pim_skip_list {

bool init_state = false;

// static int64_t key_split[NR_DPUS + 10];
// static int64_t min_key[NR_DPUS + 10];
parlay::sequence<int64_t> key_split;
parlay::sequence<int64_t> min_key;
int max_height = 19;

#ifdef DPU_ENERGY
uint64_t op_total;
uint64_t db_size_total;
uint64_t cycle_total;
#endif

void dpu_energy_stats(bool flag = false) {
#ifdef DPU_ENERGY
    uint64_t db_iter = 0, op_iter = 0, cycle_iter = 0, instr_iter = 0;
    cout << "Before: " << op_total << " " << db_size_total << " " << cycle_total
         << " " << endl;
    op_total = 0, db_size_total = 0, cycle_total = 0;
    DPU_FOREACH(dpu_set, dpu, each_dpu) {
        DPU_ASSERT(
            dpu_copy_from(dpu, "op_count", 0, &op_iter, sizeof(uint64_t)));
        DPU_ASSERT(
            dpu_copy_from(dpu, "db_size_count", 0, &db_iter, sizeof(uint64_t)));
        DPU_ASSERT(dpu_copy_from(dpu, "cycle_count", 0, &cycle_iter,
                                 sizeof(uint64_t)));
        op_total += op_iter;
        db_size_total += db_iter;
        cycle_total += cycle_iter;
        if (flag) {
            cout << "DPU ID: " << each_dpu << " " << op_iter << " " << db_iter
                 << " ";
            cout << ((op_iter > 0) ? (db_iter / op_iter) : 0) << " "
                 << cycle_iter << endl;
        }
    }
    cout << "After: " << op_total << " " << db_size_total << " " << cycle_total
         << " " << endl;
#endif
}

void init_splits() {
    printf("\n********** INIT SPLITS **********\n");
    key_split = parlay::sequence<int64_t>(nr_of_dpus);
    min_key = parlay::sequence<int64_t>(nr_of_dpus);
    uint64_t split = UINT64_MAX / nr_of_dpus;
    key_split[0] = INT64_MIN;
    for (int i = 1; i < nr_of_dpus; i++) {
        key_split[i] = key_split[i - 1] + split;
        // cout<<key_split[i]<<endl;
    }
    for (int i = 0; i < nr_of_dpus - 1; i++) {
        min_key[i] = key_split[i + 1];
    }
    // for (int i = 0; i < nr_of_dpus; i ++) {
    //     printf("split[%d]=%lld\n", i, key_split[i]);
    // }
    min_key[nr_of_dpus - 1] = INT64_MAX;
    // key_split[nr_of_dpus] = INT64_MAX;
}

void init_skiplist() {
    printf("\n********** INIT SKIP LIST **********\n");

    auto io = alloc_io_manager();
    io->init();
    auto batch = io->alloc<L3_init_task, empty_task_reply>(direct);
    parlay::sequence<int> location(nr_of_dpus);
    batch->push_task_sorted(
        nr_of_dpus, nr_of_dpus,
        [&](size_t i) {
            return (L3_init_task){{.key = key_split[i],
                                   .value = INT64_MIN,
                                   .height = max_height}};
        },
        [&](size_t i) { return i; }, make_slice(location));
    io->finish_task_batch();
    time_nested("exec", [&]() { ASSERT(!io->exec()); });
    io->reset();
}

void init() {
    // init_dpus();
    // init_splits();
    time_nested("init splits", init_splits);
    time_nested("init_skiplist", init_skiplist);
    // init_skiplist();
}

// Range Scan
template <typename I64Iterator>
int find_target(int64_t key, slice<I64Iterator, I64Iterator> target, int ll = 0,
                int rr = -1) {
    int l = ((ll < 0) ? 0 : ll), r = ((rr <= ll) ? target.size() : rr);
    while (r - l > 1) {
        int mid = (l + r) >> 1;
        if (target[mid] <= key) {
            l = mid;
        } else {
            r = mid;
        }
    }
    return l;
}

// Range Scan
template <typename I64Iterator>
auto find_range_target(scan_operation scan_op,
                       slice<I64Iterator, I64Iterator> target, int ll = 0,
                       int rr = -1) {
    int64_t lkey = scan_op.lkey, rkey = scan_op.rkey;
    int l = ((ll < 0) ? 0 : ll), r = ((rr <= ll) ? target.size() : rr);
    int mid;
    while (r - l > 1) {
        mid = (l + r) >> 1;
        if (target[mid] <= lkey) {
            l = mid;
        } else if (target[mid - 1] >= rkey) {
            r = mid;
        } else {
            break;
        }
    }
    int res_l = find_target(lkey, target, l, mid);
    int res_r = find_target(rkey, target, mid, r);
    if (res_r >= target.size()) res_r = target.size() - 1;
    return std::make_pair(res_l, res_r);
}

template <typename IntIterator1, typename IntIterator2, typename IntIterator3>
void find_targets(slice<IntIterator1, IntIterator1> in,
                  slice<IntIterator2, IntIterator2> target,
                  slice<IntIterator3, IntIterator3> split) {
    int n = in.size();
    parlay::sequence<int> starts(nr_of_dpus, 0);
    time_nested("bs", [&]() {
        parallel_for(
            0, nr_of_dpus,
            [&](size_t i) {
                starts[i] = find_target(split[i], in);
                while (starts[i] < n && in[starts[i]] < split[i]) {
                    starts[i]++;
                }
                if (starts[i] < n) {
                    target[starts[i]] = i;
                }
            },
            1024 / log2_up(n));
    });
    parlay::scan_inclusive_inplace(target, parlay::maxm<int>());
}

auto get(slice<int64_t*, int64_t*> ops) {
    assert(false);
    key_value x;
    return parlay::sequence<key_value>(ops.size(), x);
    // return false;
}
void update(slice<key_value*, key_value*> ops) {
    assert(false);
    // return false;
}
auto predecessor(slice<int64_t*, int64_t*> keys) {
    int n = keys.size();
    auto splits = make_slice(min_key);

    time_start("find_target");
    auto target = parlay::tabulate(
        n, [&](size_t i) { return find_target(keys[i], splits); });
    time_end("find_target");

    IO_Manager* io;
    IO_Task_Batch* batch;

    auto location = parlay::sequence<int>(n);
    time_nested("taskgen", [&]() {
        io = alloc_io_manager();
        io->init();
        batch = io->alloc<L3_search_task, L3_search_reply>(direct);
        time_nested("push_task", [&]() {
            batch->push_task_from_array_by_isort<false>(
                n, [&](size_t i) { return (L3_search_task){.key = keys[i]}; },
                make_slice(target), make_slice(location));
        });
        io->finish_task_batch();
    });

    time_nested("exec", [&]() { ASSERT(io->exec()); });
    time_start("get_result");
    auto result = parlay::tabulate(n, [&](size_t i) {
        auto reply = (L3_search_reply*)batch->ith(target[i], location[i]);
        return (key_value){.key = reply->key, .value = reply->value};
    });
    time_end("get_result");
    io->reset();
    return result;
}

void insert(slice<key_value*, key_value*> kvs) {
    int n = kvs.size();

    parlay::sequence<key_value> kv_sorted;
    time_nested("sort", [&]() {
        kv_sorted =
            parlay::sort(kvs, [](auto t1, auto t2) { return t1.key < t2.key; });
    });

    auto keys_sorted = parlay::delayed_seq<int64_t>(
        n, [&](size_t i) { return kv_sorted[i].key; });

    IO_Manager* io;
    IO_Task_Batch* batch;

    auto target = parlay::sequence<int>(n, 0);
    time_nested("find", [&]() {
        find_targets(make_slice(keys_sorted), make_slice(target),
                     make_slice(key_split));
    });

    auto heights = parlay::sequence<int>(n);
    time_nested("init height", [&]() {
        parlay::parallel_for(0, n, [&](size_t i) {
            int32_t t = rn_gen::parallel_rand();
            t = t & (-t);
            int h = __builtin_ctz(t) + 1;
            h = min(h, max_height);
            heights[i] = h;
        });
    });

    auto location = parlay::sequence<int>(n);
    time_nested("taskgen", [&]() {
        io = alloc_io_manager();
        io->init();
        batch = io->alloc<L3_insert_task, empty_task_reply>(direct);
        batch->push_task_sorted(
            n, nr_of_dpus,
            [&](size_t i) {
                return (L3_insert_task){{.key = kv_sorted[i].key,
                                         .value = kv_sorted[i].value,
                                         .height = heights[i]}};
            },
            [&](size_t i) { return target[i]; }, make_slice(location));
        io->finish_task_batch();
    });

    time_nested("taskgen2", [&]() {
        batch = io->alloc<L3_get_min_task, L3_get_min_reply>(direct);
        batch->push_task_sorted(
            nr_of_dpus, nr_of_dpus,
            [&](size_t i) {
                (void)i;
                return (L3_get_min_task){.key = 0};
            },
            [&](size_t i) { return i; },
            make_slice(location).cut(0, nr_of_dpus));
        io->finish_task_batch();
    });

    time_nested("exec", [&]() { ASSERT(io->exec()); });

    time_nested("result", [&]() {
        for (int i = 0; i < nr_of_dpus; i++) {
            auto rep = (L3_get_min_reply*)batch->ith(i, 0);
            assert(rep->key <= min_key[i]);
            min_key[i] = rep->key;
        }
    });

    io->reset();
    return;
}

void remove(slice<int64_t*, int64_t*> keys) {
    int n = keys.size();

    parlay::sequence<int64_t> keys_sorted;
    time_nested("sort", [&]() { keys_sorted = parlay::sort(keys); });

    IO_Manager* io;
    IO_Task_Batch* batch;

    auto target = parlay::sequence<int>(n, 0);
    time_nested("find", [&]() {
        find_targets(make_slice(keys_sorted), make_slice(target),
                     make_slice(key_split));
    });

    auto location = parlay::sequence<int>(n);
    time_nested("taskgen", [&]() {
        io = alloc_io_manager();
        io->init();
        batch = io->alloc<L3_remove_task, empty_task_reply>(direct);
        batch->push_task_sorted(
            n, nr_of_dpus,
            [&](size_t i) { return (L3_remove_task){.key = keys_sorted[i]}; },
            [&](size_t i) { return target[i]; }, make_slice(location));
        io->finish_task_batch();
    });
    time_nested("exec", [&]() { ASSERT(!io->exec()); });
    io->reset();
    return;
}

// Range Scan
auto scan(slice<scan_operation*, scan_operation*> op_set) {
    int n = op_set.size();
    time_start("merge_range");
    auto ops =
        parlay::sort(op_set, [&](scan_operation s1, scan_operation s2) -> bool {
            return (s1.lkey < s2.lkey) ||
                   ((s1.lkey == s2.lkey) && (s1.rkey < s2.rkey));
        });
    auto range_prefix_scan =
        parlay::scan(ops, scan_op_rkey_nlt<scan_operation>());
    auto range_prefix_sum = parlay::make_slice(range_prefix_scan.first);
    auto scan_start_arr = parlay::tabulate(n, [&](size_t i) -> bool {
        return (i == 0) || (range_prefix_sum[i].rkey < ops[i].lkey);
    });
    auto scan_start = parlay::pack_index(scan_start_arr);
    int nn = scan_start.size();
    auto ops_merged = parlay::tabulate(nn, [&](int i) {
        return make_scan_op<scan_operation>(
            ops[scan_start[i]].lkey,
            ((i != nn - 1) ? range_prefix_sum[scan_start[i + 1]].rkey
                           : range_prefix_scan.second.rkey));
    });
    time_end("merge_range");

    time_start("find_target");
    auto splits = parlay::make_slice(min_key);
    auto target = parlay::tabulate(
        nn, [&](size_t i) { return find_range_target(ops_merged[i], splits); });

    IO_Manager* io;
    IO_Task_Batch* batch;
    auto node_nums = parlay::tabulate(
        nn, [&](size_t i) { return (target[i].second - target[i].first + 1); });
    auto node_num_prefix_scan = parlay::scan(node_nums);
    auto node_num = node_num_prefix_scan.second;
    auto target_scan = parlay::sequence<int>(node_num);
    auto location = parlay::sequence<int>(node_num);
    auto ops_scan = parlay::sequence<L3_scan_task>(node_num);
    auto node_num_prefix_sum = parlay::make_slice(node_num_prefix_scan.first);
    time_end("find_target");

    time_nested("taskgen", [&]() {
        parlay::parallel_for(0, nn, [&](size_t i) {
            for (int j = 0; j < node_nums[i]; j++) {
                target_scan[node_num_prefix_sum[i] + j] = target[i].first + j;
                ops_scan[node_num_prefix_sum[i] + j] =
                    make_scan_op<L3_scan_task>(ops_merged[i].lkey,
                                               ops_merged[i].rkey);
            }
        });
        io = alloc_io_manager();
        io->init();
        batch = io->alloc<L3_scan_task, L3_scan_reply>(direct);
        time_nested("push_task", [&]() {
            batch->push_task_from_array_by_isort<false>(
                node_num, [&](size_t i) { return ops_scan[i]; },
                make_slice(target_scan), make_slice(location));
        });
        io->finish_task_batch();
    });

    time_nested("exec", [&]() { ASSERT(io->exec()); });

    time_start("get_result");
    auto kv_nums = parlay::tabulate(node_num, [&](size_t i) {
        auto reply = (L3_scan_reply*)batch->ith(target_scan[i], location[i]);
        return (reply->length);
    });
    auto kv_nums_prefix_scan = parlay::scan(kv_nums);
    auto kv_num = kv_nums_prefix_scan.second;
    auto kv_nums_prefix_sum = parlay::make_slice(kv_nums_prefix_scan.first);
    auto kv_set1 = parlay::sequence<key_value>(kv_num);
    parlay::parallel_for(0, node_num, [&](size_t i) {
        auto reply = (L3_scan_reply*)batch->ith(target_scan[i], location[i]);
        for (int j = 0; j < kv_nums[i]; j++) {
            kv_set1[kv_nums_prefix_sum[i] + j].key = reply->vals[j];
            kv_set1[kv_nums_prefix_sum[i] + j].value =
                reply->vals[j + reply->length];
        }
    });
    time_end("get_result");
    io->reset();

    time_start("reassemble_result");
    parlay::sort_inplace(kv_set1, [&](key_value kv1, key_value kv2) -> bool {
        return (kv1.key < kv2.key) ||
               ((kv1.key == kv2.key) && (kv1.value < kv2.value));
    });
    auto kv_set =
        parlay::unique(kv_set1, [&](key_value kv1, key_value kv2) -> bool {
            return (kv1.key == kv2.key);
        });
    auto kv_n = kv_set.size();
    auto index_set = parlay::tabulate(n, [&](size_t i) {
        int ll = 0, rr = kv_n;
        int64_t lkey = op_set[i].lkey, rkey = op_set[i].rkey;
        int mid, res_ll, res_rr;
        while (rr - ll > 1) {
            mid = (ll + rr) >> 1;
            if (lkey >= kv_set[mid].key)
                ll = mid;
            else if (rkey < kv_set[mid].key)
                rr = mid;
            else {
                break;
            }
        }
        int lll = ll, rrr = rr, mmm = mid;

        ll = lll;
        rr = mmm;
        while (rr - ll > 1) {
            mid = (ll + rr) >> 1;
            if (lkey < kv_set[mid].key)
                rr = mid;
            else
                ll = mid;
        }
        if (lkey <= kv_set[ll].key)
            res_ll = ll;
        else
            res_ll = ll + 1;

        ll = mmm;
        rr = rrr;
        while (rr - ll > 1) {
            mid = (ll + rr) >> 1;
            if (rkey <= kv_set[mid].key)
                rr = mid;
            else
                ll = mid;
        }
        if (ll >= kv_n - 1)
            res_rr = kv_n;
        else if (kv_set[ll + 1].key <= rkey)
            res_rr = ll + 2;
        else
            res_rr = ll + 1;
        return std::make_pair(res_ll, res_rr);
    });
    time_end("reassemble_result");
    return make_pair(kv_set, index_set);
}
};  // namespace pim_skip_list
