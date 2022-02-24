#pragma once
#include <parlay/primitives.h>

#include "dpu_control.hpp"
#include "oracle.hpp"
#include "task.hpp"
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

    static void init_skiplist() {
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

   public:
    static void init() {
        // init_dpus();
        // init_splits();
        time_nested("init splits", init_splits);
        time_nested("init_skiplist", init_skiplist);
        // init_skiplist();
    }

    template<typename I64Iterator>
    static int find_target(int64_t key, slice<I64Iterator, I64Iterator> target) {
        int l = 0, r = target.size();
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

    static auto get(slice<int64_t*, int64_t*> ops) {
        assert(false);
        return parlay::sequence<int64_t>(ops.size(), 0);
        // return false;
    }
    static void update(slice<key_value*, key_value*> ops) {
        assert(false);
        // return false;
    }
    static auto predecessor(slice<int64_t*, int64_t*> keys) {
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
                    n,
                    [&](size_t i) { return (L3_search_task){.key = keys[i]}; },
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

    static void remove(slice<int64_t*, int64_t*> keys) {
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

    static auto scan(slice<scan_operation*, scan_operation*> ops) {
        auto kvs = parlay::sequence<key_value>(1);
        auto ids = parlay::sequence<pair<int64_t, int64_t>>(1);
        return std::make_pair(kvs, ids);
    }
};

parlay::sequence<int64_t> pim_skip_list::key_split;
parlay::sequence<int64_t> pim_skip_list::min_key;
int pim_skip_list::max_height = 19;
