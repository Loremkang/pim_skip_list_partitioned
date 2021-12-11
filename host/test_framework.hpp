#pragma once

#include <iostream>
#include <cstdio>
#include <set>
#include "operations.hpp"
#include "timer.hpp"
#include "util.hpp"
#include "oracle.hpp"
using namespace std;

// const int VALUE_LIMIT = ;

extern bool print_debug;
extern int64_t key_split[];

// static set<int64_t> golden_L3;
Oracle oracle; 

void init_test_framework() {
    epoch_number = 0;
    auto oracle_init_batch = parlay::tabulate(nr_of_dpus, [&](int i) -> int64_t {
        return key_split[i];
    });
    oracle.insert_batch(oracle_init_batch);
}

// timer get_timer("get");

typedef parlay::sequence<int64_t> i64_seq;

bool result_eq(int length, const i64_seq& a, const i64_seq& b) {
    bool ret = true;
    debug_parallel_for(0, length, [&](size_t i) {
        if (a[i] != b[i]) {
            ret = false;
        }
    });
    return ret;
}

bool get_test(int length, bool check_result) {
    debug_parallel_for(0, length, [&](size_t i) {
        int tid = parlay::worker_id();
        if (tid & 1) {
            op_keys[i] = oracle.get();
        } else {
            op_keys[i] = randint64(tid);
        }
    });
    // for (int i = 0; i < length; i++) {
    //     if (i & 1) {
    //     // if (randint64(parlay::worker_id()) & 1) {
    //         // auto it = golden_L3.begin();
    //         op_keys[i] = randint64(parlay::worker_id());
    //         set<int64_t>::iterator it = golden_L3.upper_bound(op_keys[i]);
    //         // int rnd = (randint64(parlay::worker_id()) % (golden_L3.size() - 2)) +
    //         //           1;  // don't ask -INF or INF
    //         // advance(it, rnd);
    //         op_keys[i] = *it;
    //     } else {
    //         op_keys[i] = randint64(parlay::worker_id());
    //     }
    // }

    // sort(op_keys, op_keys + length);
    cout << "\n*** Start Get Test ***" << endl;
    // get_timer.start();
    time_root("get", [&]() {
        get(length);
    });
    
    // get_timer.end();

    // if (print_debug) {
    //     for_each(golden_L3.begin(), golden_L3.end(),
    //              [&](int64_t v) { cout << "*" << v << endl; });
    //     cout << endl;
    // }
    bool succeed = true;
    if (check_result) {
        auto keys = parlay::tabulate(
            length, [](size_t i) -> int64_t { return op_keys[i]; });
        auto predecessors = parlay::sequence<int64_t>(length);
        oracle.predecessor_batch(keys, predecessors);
        // auto predecessors = oracle.predecessor_batch(keys);
        auto correct = parlay::tabulate(length, [&](size_t i) -> int64_t {
            return (keys[i] == predecessors[i]) ? 1ll : 0ll;
        });
        auto result = parlay::tabulate(
            length, [](size_t i) -> int64_t { return op_results[i]; });

        if (!result_eq(length, correct, result)) {
            for (int i = 0; i < length; i++) {
                if (correct[i] != result[i]) {
                    printf("query=%lx get=%lx ans=%lx\n", keys[i], result[i],
                           correct[i]);
                    succeed = false;
                }
            }
        }
    }

    cout << endl << "\n*** End Get Test ***" << endl;
    return succeed;
}

// timer predecessor_timer("predecessor");

bool predecessor_test_fixed_key(int64_t key) {
    int length = 1;
    op_keys[0] = key;
    auto op_keys_slice = parlay::make_slice(op_keys, op_keys + length);
    parlay::sort_inplace(op_keys_slice);

    cout << "\n*** Start Predecessor Test Fixed Key ***" << endl;

    time_root("predecessor",
           [&]() { predecessor(predecessor_only, length); });


    // if (print_debug) {
    //     for_each(golden_L3.begin(), golden_L3.end(),
    //              [&](int64_t v) { cout << "*" << v << endl; });
    // }
    // cout << endl;

    bool succeed = true;
    bool check_result = true;
    if (check_result) {
        auto keys = parlay::sort(op_keys_slice);
        cout << "finished!" << endl;

        auto correct = parlay::sequence<int64_t>(length);
        oracle.predecessor_batch(keys, correct);

        auto result = parlay::tabulate(
            length, [](size_t i) -> int64_t { return op_results[i]; });

        cout << "finished!" << endl;
        // exit(0);

        if (!result_eq(length, correct, result)) {
            for (int i = 0; i < length; i++) {
                if (correct[i] != result[i]) {
                    printf("query=%lx get=%lx ans=%lx\n", keys[i], result[i],
                           correct[i]);
                    // return false;
                    succeed = false;
                }
            }
        }
    }
    cout << endl << "\n*** End Predecessor Test ***" << endl;
    return succeed;
}

bool predecessor_test(int length, bool check_result) {
    debug_parallel_for(0, length, [&](size_t i) {
        op_keys[i] = randint64(parlay::worker_id());
    });
    // auto keys = parlay::tabulate(
        // length, [](int i) -> bool { return randkey(parlay::worker_id()); });
    auto op_keys_slice = parlay::make_slice(op_keys, op_keys + length);
    parlay::sort_inplace(op_keys_slice);

    cout << "\n*** Start Predecessor Test ***" << endl;

    time_root("predecessor",
           [&]() { predecessor(predecessor_only, length); });


    // if (print_debug) {
    //     for_each(golden_L3.begin(), golden_L3.end(),
    //              [&](int64_t v) { cout << "*" << v << endl; });
    // }
    // cout << endl;

    bool succeed = true;
    if (check_result) {
        auto keys = parlay::sort(op_keys_slice);
        cout << "finished!" << endl;

        auto correct = parlay::sequence<int64_t>(length);
        oracle.predecessor_batch(keys, correct);

        auto result = parlay::tabulate(
            length, [](size_t i) -> int64_t { return op_results[i]; });

        cout << "finished!" << endl;
        // exit(0);

        if (!result_eq(length, correct, result)) {
            for (int i = 0; i < length; i++) {
                if (correct[i] != result[i]) {
                    printf("query=%lx get=%lx ans=%lx\n", keys[i], result[i],
                           correct[i]);
                    printf("predecessor[get] = %lx\n", oracle.get_predecessor(result[i]));
                    // return false;
                    succeed = false;
                }
            }
        }
    }
    cout << endl << "\n*** End Predecessor Test ***" << endl;
    return succeed;
}

bool insert_test(int length, bool check_result) {
    cout << "\n*** Start Insert Test ***" << endl;
    debug_parallel_for(0, length, [&](size_t i) {
        op_keys[i] = randkey(parlay::worker_id());
    });

    time_root("insert", [&]() { insert(length); });

    if (check_result) {
        oracle.insert_batch(parlay::tabulate(
            length, [](size_t i) -> int64_t { return op_keys[i]; }));
    }

    cout << endl << "\n*** End Insert Test ***" << endl;
    return true;
}

// void remove_test(int length, bool check_result) {
//     if (check_result) {
//         assert(inserted_set.size() > 0);
//     }
//     cout << "\n*** Start Remove Test ***" << endl;
//     cout<<inserted_set.size()<<endl;
//     for (int i = 0; i < length; i++) {
//         if (randint64(parlay::worker_id()) & 1) {
//             // auto it = inserted_set.begin();
//             // int rnd = (randint64(parlay::worker_id()) % (inserted_set.size()));
//             // advance(it, rnd);
//             int64_t rd = randkey(parlay::worker_id());
//             set<int64_t>::iterator it = inserted_set.upper_bound(rd);
//             it--;
//             op_keys[i] = *it;
//         } else {
//             op_keys[i] = randkey(parlay::worker_id());
//         }
//     }

//     remove_timer.start();
//     remove(length);
//     remove_timer.end();

//     if (check_result) {
//         auto dedup_keys = deduplication(op_keys, length);
//         int l = dedup_keys.size();
//         for (int i = 0; i < l; i++) {
//             inserted_set.erase(dedup_keys[i]);
//             predecessor_set.erase(dedup_keys[i]);
//         }
//     }

//     cout << endl << "\n*** End Remove Test ***" << endl;
// }

void remove_test(int length, bool check_result) {
    (void)length;
    (void)check_result;
    // if (check_result) {
    //     assert(golden_L3.size() > 2);
    // }
    // cout << "\n*** Start Remove Test ***" << endl;
    // for (int i = 0; i < length; i++) {
    //     if (randint64(parlay::worker_id()) & 1) {
    //         auto it = golden_L3.begin();
    //         int rnd =
    //             (randint64(parlay::worker_id()) % (golden_L3.size() - 2)) +
    //             1;  // don't ask -INF or INF
    //         advance(it, rnd);
    //         op_keys[i] = *it;
    //     } else {
    //         op_keys[i] = randint64(parlay::worker_id());
    //     }
    // }

    // time_root("remove", [&]() { remove(length); });

    // if (check_result) {
    //     auto dedup_keys = deduplication(op_keys, length);
    //     int l = dedup_keys.size();
    //     for (int i = 0; i < l; i++) {
    //         golden_L3.erase(dedup_keys[i]);
    //     }
    // }

    // cout << endl << "\n*** End Remove Test ***" << endl;
}

void L3_sancheck() {
    // cout << "\n*** Start L3 Sancheck ***" << endl;
    // auto io = init_io_buffer(true); ASSERT(io == &io_managers[0]);
    // io->set_io_buffer_type(L3_SANCHECK_TSK, EMPTY);
    // L3_sancheck_task tst;
    // io->push_task(&tst, sizeof(L3_sancheck_task), 0, -1);
    // ASSERT(!io->exec());
    // print_log(0);
    // cout << endl << "\n*** End L3 Sancheck ***" << endl;
}

void L2_print_all_nodes() {
    // cout << "\n*** Start L2 Print All Nodes ***" << endl;
    // auto io = init_io_buffer(true); ASSERT(io == &io_managers[0]);
    // io->set_io_buffer_type(L2_PRINT_ALL_NODES_TSK, EMPTY);
    // L2_print_all_nodes_task spant;
    // io->push_task(&spant, sizeof(L2_print_all_nodes_task), 0, -1);
    // ASSERT(!io->exec());
    // print_log(0, true);
    // cout << endl << "\n*** End L3 Print All Nodes ***" << endl;
}