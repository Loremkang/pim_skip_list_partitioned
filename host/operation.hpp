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
        ASSERT(io == &io_managers[0]);
        io->init();
        auto batch = io->alloc<L3_init_task, empty_task_reply>(direct);
        parlay::sequence<int> location(nr_of_dpus);
        batch->push_task_sorted(
            nr_of_dpus, nr_of_dpus,
            [&](size_t i) {
                return (L3_init_task){
                    {.key = INT64_MIN, .value = INT64_MIN}};
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

    template <typename I64Iterator>
    static int find_target(int64_t key,
                           slice<I64Iterator, I64Iterator> target) {
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
        // parlay::scan_inplace(target, );
        // for (int i = 1; i < n; i++) {
        //     if (target[i] == 0) {
        //         target[i] = target[i - 1];
        //     }
        // }
        // int j = 0;
        // for (int i = 0; i < n; i++) {
        //     while (j < nr_of_dpus && split[j] <= in[i]) {
        //         j++;
        //     }
        //     target[i] = j - 1;
        //     ASSERT(target[i] >= 0);
        // }
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
        auto splits = make_slice(min_key);

        time_start("find_target");
        auto target = parlay::tabulate(
            n, [&](size_t i) { return find_target(keys[i], splits); });
        time_end("find_target");

        // for (int i = 0; i < 100; i ++) {
        //     printf("query=%lld\npos=%d\nsplit=%lld\n\n", keys[i], target[i],
        //     splits[target[i]]);
        // }
        // exit(0);

        IO_Manager* io;
        IO_Task_Batch* batch;

        auto location = parlay::sequence<int>(n);
        time_nested("taskgen", [&]() {
            io = alloc_io_manager();
            ASSERT(io == &io_managers[0]);
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
        // for (int i = 0; i < 100; i ++) {
        //     printf("q=%lld k=%lld v=%lld\n", keys[i], result[i].key, result[i].value);
        // }
        // exit(0);
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

        auto location = parlay::sequence<int>(n);
        time_nested("taskgen", [&]() {
            io = alloc_io_manager();
            ASSERT(io == &io_managers[0]);
            io->init();
            batch = io->alloc<L3_insert_task, empty_task_reply>(direct);
            batch->push_task_sorted(
                n, nr_of_dpus,
                [&](size_t i) {
                    return (L3_insert_task){
                        {.key = kv_sorted[i].key, .value = kv_sorted[i].value}};
                },
                [&](size_t i) { return target[i]; }, make_slice(location));
            io->finish_task_batch();
        });

        // for (int i = 0; i < nr_of_dpus; i ++) {
        //     for (int j = 0; j < batch->tbs[i].count(); j ++) {
        //         auto t = (L3_insert_task*)batch->ith(i, j);
        //         printf("[%d,%d]key=%llx\tvalue=%llx\n", i, j, t->key, t->value);
        //     }
        // }
        // exit(0);

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
                // printf("%d %lld\n", i, min_key[i]);
            }
        });

        io->reset();
        // for (int i = 0; i < nr_of_dpus; i ++) {
        //     printf("minkey[%d]=%lld\n", i, min_key[i]);
        // }
        // exit(0);
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

parlay::sequence<int64_t> pim_skip_list::key_split;
parlay::sequence<int64_t> pim_skip_list::min_key;