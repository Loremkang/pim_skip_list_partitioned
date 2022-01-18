#pragma once
#include <parlay/primitives.h>
#include "oracle.hpp"
#include "task.hpp"
#include "dpu_control.hpp"
#include "task_framework_host.hpp"
#include "value.hpp"
using namespace std;
using namespace parlay;

class pim_skip_list {
   public:
    // static int64_t key_split[NR_DPUS + 10];
    static parlay::sequence<int64_t> key_split;
    static parlay::sequence<int64_t> min_key;
    // static int64_t min_key[NR_DPUS + 10];
    static int max_height;

   private:
    static void init_splits() {
        printf("\n********** INIT SPLITS **********\n");
        uint64_t split = UINT64_MAX / nr_of_dpus;
        key_split[0] = INT64_MIN;
        for (int i = 1; i < nr_of_dpus; i++) {
            key_split[i] = key_split[i - 1] + split;
            // cout<<key_split[i]<<endl;
        }
        for (int i = 0; i < nr_of_dpus - 1; i ++) {
            min_key[i] = key_split[i + 1];
        }
        min_key[nr_of_dpus - 1] = INT64_MAX;
        // key_split[nr_of_dpus] = INT64_MAX;
    }

    static void init_skiplist() {
        printf("\n********** INIT SKIP LIST **********\n");

        auto io = alloc_io_manager();
        ASSERT(io == &io_managers[0]);
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

   public:
    static void init() {
        // init_dpus();
        // init_splits();
        time_nested("init splits", init_splits);
        time_nested("init_skiplist", init_skiplist);
        // init_skiplist();
    }

    static int64_t find_target(int64_t key, slice<int64_t*, int64_t*> target) {
        int l = 0, r = nr_of_dpus;
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

    template <typename IntIterator1, typename IntIterator2,
              typename IntIterator3>
    static void find_targets(slice<IntIterator1, IntIterator1> in,
                            slice<IntIterator2, IntIterator2> target,
                            slice<IntIterator3, IntIterator3> split) {
        int n = in.size();
        int j = 0;
        for (int i = 0; i < n; i++) {
            while (j < nr_of_dpus && split[j] <= in[i]) {
                j++;
            }
            target[i] = j - 1;
            ASSERT(target[i] >= 0);
        }
    }



    static auto get(slice<int64_t*, int64_t*> ops) {
        assert(false);
        return parlay::sequence<int64_t>(ops.size(), 0);
        // return false;
    }
    static void update(slice<key_value*, key_value*> ops) {
        assert(false);
        // return false;
    }
    static void scan(slice<scan_operation*, scan_operation*> ops) {
        assert(false);
        // return false;
    }
    static auto predecessor(slice<int64_t*, int64_t*> keys) {
        int n = keys.size();
        auto keys_with_offset = parlay::delayed_seq<pair<int, int64_t>>(
            n, [&](size_t i) { return make_pair(i, keys[i]); });
        parlay::sequence<pair<int, int64_t>> keys_with_offset_sorted;

        time_nested("sort", [&]() {
            keys_with_offset_sorted = parlay::sort(
                keys_with_offset,
                [](auto t1, auto t2) { return t1.second < t2.second; });
        });
        auto keys_sorted = parlay::delayed_seq<int64_t>(
            n, [&](size_t i) { return keys_with_offset_sorted[i].second; });

        IO_Manager* io;
        IO_Task_Batch* batch;

        auto target = parlay::sequence<int>(n);
        time_nested("find", [&]() {
            find_targets(make_slice(keys_sorted), make_slice(target), make_slice(min_key));
        });

        auto location = parlay::sequence<int>(n);
        time_nested("taskgen", [&]() {
            io = alloc_io_manager();
            ASSERT(io == &io_managers[0]);
            io->init();
            batch = io->alloc<L3_search_task, L3_search_reply>(direct);
            batch->push_task_sorted(
                n, nr_of_dpus,
                [&](size_t i) {
                    return (L3_search_task){.key = keys_sorted[i]};
                },
                [&](size_t i) { return target[i]; }, make_slice(location));
            io->finish_task_batch();
        });

        time_nested("exec", [&]() { ASSERT(io->exec()); });
        auto result = parlay::tabulate(n, [&](size_t i) {
            auto reply = (L3_search_reply*)batch->ith(target[i], location[i]);
            return make_pair(
                keys_with_offset_sorted[i].first,
                (key_value){.key = reply->key, .value = reply->value});
        });
        io->reset();
        auto ret = parlay::sort(
            result, [](auto t1, auto t2) { return t1.first < t2.first; });
        return parlay::tabulate(n, [&](size_t i) { return ret[i].second; });
    }

    static void insert(slice<key_value*, key_value*> kvs) {
        int n = kvs.size();

        parlay::sequence<key_value> kv_sorted;
        time_nested("sort", [&]() {
            kv_sorted = parlay::sort(
                kvs, [](auto t1, auto t2) { return t1.key < t2.key; });
        });

        auto keys_sorted = parlay::delayed_seq<int64_t>(
            n, [&](size_t i) { return kv_sorted[i].key; });

        IO_Manager* io;
        IO_Task_Batch* batch;

        auto target = parlay::sequence<int>(n);
        time_nested("find", [&]() {
            find_targets(make_slice(keys_sorted), make_slice(target), make_slice(key_split));
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
            ASSERT(io == &io_managers[0]);
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

    static void remove(slice<int64_t*, int64_t*> keys) {
        int n = keys.size();

        parlay::sequence<int64_t> keys_sorted;
        time_nested("sort", [&]() { keys_sorted = parlay::sort(keys); });

        IO_Manager* io;
        IO_Task_Batch* batch;

        auto target = parlay::sequence<int>(n);
        time_nested("find", [&]() {
            find_targets(make_slice(keys_sorted), make_slice(target),
                        make_slice(key_split));
        });

        auto location = parlay::sequence<int>(n);
        time_nested("taskgen", [&]() {
            io = alloc_io_manager();
            ASSERT(io == &io_managers[0]);
            io->init();
            batch = io->alloc<L3_remove_task, empty_task_reply>(direct);
            batch->push_task_sorted(
                n, nr_of_dpus,
                [&](size_t i) {
                    return (L3_remove_task){.key = keys_sorted[i]};
                },
                [&](size_t i) { return target[i]; }, make_slice(location));
            io->finish_task_batch();
        });
        time_nested("exec", [&]() { ASSERT(!io->exec()); });
        io->reset();
        return;
    }
};

parlay::sequence<int64_t> pim_skip_list::key_split(NR_DPUS + 10);
parlay::sequence<int64_t> pim_skip_list::min_key(NR_DPUS + 10);
// int64_t pim_skip_list::key_split[NR_DPUS + 10];
int pim_skip_list::max_height = 19;