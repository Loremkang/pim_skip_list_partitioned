#pragma once

#include <iostream>
#include <cstdio>
#include <set>
#include "operations.hpp"
#include "timer.hpp"
#include "util.hpp"
using namespace std;

// const int VALUE_LIMIT = ;

extern bool print_debug;

static set<int64_t> predecessor_set, inserted_set;

extern int64_t key_split[];
void init_test_framework() {
    epoch_number = 0;
    predecessor_set.clear();
    inserted_set.clear();
    for (int i = 0; i < nr_of_dpus; i ++) {
        predecessor_set.insert(key_split[i]);
    }
    predecessor_set.insert(LLONG_MIN);
    predecessor_set.insert(LLONG_MAX);
}

timer get_timer("get");

bool get_test(int length, bool check_result) {
    for (int i = 0; i < length; i++) {
        if (randint64(parlay::worker_id()) & 1) {
            auto it = predecessor_set.begin();
            int rnd = (randint64(parlay::worker_id()) % (predecessor_set.size() - 2)) +
                      1;  // don't ask -INF or INF
            advance(it, rnd);
            op_keys[i] = *it;
        } else {
            op_keys[i] = randkey(parlay::worker_id());
        }
    }

    // sort(op_keys, op_keys + length);
    cout << "\n*** Start Get Test ***" << endl;
    get_timer.start();
    get(length);
    get_timer.end();

    if (check_result) {
        for (int i = 0; i < length; i++) {
            int64_t golden_result =
                (predecessor_set.find(op_keys[i]) != predecessor_set.end()) ? 1 : 0;
            if (op_results[i] != golden_result) {
                cout << op_keys[i] << ' ' << op_results[i] << ' '
                     << golden_result << endl;
                return false;
            }
        }
    }

    cout << endl << "\n*** End Get Test ***" << endl;
    return true;
}

timer predecessor_timer("predecessor");

bool predecessor_test(int length, bool check_result) {
    parlay::parallel_for(0, length,
                         [&](size_t i) { op_keys[i] = randkey(parlay::worker_id()); });
    // memset(op_results, 0, sizeof(int64_t) * BATCH_SIZE);

    // sort(op_keys, op_keys + length);
    cout << "\n*** Start Predecessor Test ***" << endl;
    predecessor_timer.start();
    predecessor(length);
    predecessor_timer.end();

    // if (print_debug) {
    //     for_each(predecessor_set.begin(), predecessor_set.end(),
    //              [&](int64_t v) { cout << "*" << v << endl; });
    // }

    cout << endl;
    if (check_result) {
        for (int i = 0; i < length; i++) {
            set<int64_t>::iterator it = predecessor_set.upper_bound(op_keys[i]);
            it--;
            if (op_results[i] != *it) {
                cout << i << ' ' << op_keys[i] << ' ' << op_results[i] << ' '
                     << *it << endl;
                return false;
            }
        }
    }
    cout << endl << "\n*** End Predecessor Test ***" << endl;
    return true;
}

timer insert_timer("insert");

bool insert_test(int length, bool check_result) {
    cout << "\n*** Start Insert Test ***" << endl;
    if (check_result) {
        for (int i = 0; i < length; i++) {
            op_keys[i] = randkey(parlay::worker_id());
            predecessor_set.insert(op_keys[i]);
            inserted_set.insert(op_keys[i]);
        }
    } else {
        for (int i = 0; i < length; i++) {
            op_keys[i] = randkey(parlay::worker_id());
        }
    }

    insert_timer.start();
    insert(length);
    insert_timer.end();
    cout << endl << "\n*** End Insert Test ***" << endl;
    return true;
}

timer remove_timer("remove");

void remove_test(int length, bool check_result) {
    if (check_result) {
        assert(inserted_set.size() > 0);
    }
    cout << "\n*** Start Remove Test ***" << endl;
    cout<<inserted_set.size()<<endl;
    for (int i = 0; i < length; i++) {
        if (randint64(parlay::worker_id()) & 1) {
            // auto it = inserted_set.begin();
            // int rnd = (randint64(parlay::worker_id()) % (inserted_set.size()));
            // advance(it, rnd);
            int64_t rd = randkey(parlay::worker_id());
            set<int64_t>::iterator it = inserted_set.upper_bound(rd);
            it--;
            op_keys[i] = *it;
        } else {
            op_keys[i] = randkey(parlay::worker_id());
        }
    }

    remove_timer.start();
    remove(length);
    remove_timer.end();

    if (check_result) {
        auto dedup_keys = deduplication(op_keys, length);
        int l = dedup_keys.size();
        for (int i = 0; i < l; i++) {
            inserted_set.erase(dedup_keys[i]);
            predecessor_set.erase(dedup_keys[i]);
        }
    }

    cout << endl << "\n*** End Remove Test ***" << endl;
}

void L3_sancheck() {
    cout << "\n*** Start L3 Sancheck ***" << endl;
    init_io_buffer(true);
    set_io_buffer_type(L3_SANCHECK_TSK, EMPTY);
    L3_sancheck_task tst;
    push_task(&tst, sizeof(L3_sancheck_task), 0, -1);
    ASSERT(!exec());
    print_log(0, true);
    cout << endl << "\n*** End L3 Sancheck ***" << endl;
}

void L2_print_all_nodes() {
    cout << "\n*** Start L2 Print All Nodes ***" << endl;
    init_io_buffer(true);
    set_io_buffer_type(L2_PRINT_ALL_NODES_TSK, EMPTY);
    L2_print_all_nodes_task spant;
    push_task(&spant, sizeof(L2_print_all_nodes_task), 0, -1);
    ASSERT(!exec());
    print_log(0, true);
    cout << endl << "\n*** End L3 Print All Nodes ***" << endl;
}