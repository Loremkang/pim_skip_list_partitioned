#pragma once

#include <iostream>
#include <cstdio>
#include <set>
#include "operations.hpp"
#include "timer.hpp"
using namespace std;

// const int VALUE_LIMIT = ;

int64_t randint64() {
    uint64_t v = rand();
    v = (v << 31) + rand();
    v = (v << 3) + (rand() & 3);
    // printf("*%llu %llu %d\n", v, 1ull << 63, v >= (1ull << 63));
    if (v >= (1ull << 63)) {
        return v - (1ull << 63);
    } else {
        return v - (1ull << 63);
    }
}

extern bool print_debug;

static set<int64_t> golden_L3;

void init_test_framework() {
    epoch_number = 0;
    golden_L3.clear();
    golden_L3.insert(LLONG_MIN);
    golden_L3.insert(LLONG_MAX);
}

timer get_timer("get");

bool get_test(int length, bool check_result) {
    for (int i = 0; i < length; i++) {
        if (rand() & 1) {
            auto it = golden_L3.begin();
            int rnd = (randint64() % (golden_L3.size() - 2)) +
                      1;  // don't ask -INF or INF
            advance(it, rnd);
            op_keys[i] = *it;
        } else {
            op_keys[i] = randint64();
        }
    }

    // sort(op_keys, op_keys + length);
    cout << "\n*** Start Get Test ***" << endl;
    get_timer.start();
    get(length);
    get_timer.end();

    // if (print_debug) {
    //     for_each(golden_L3.begin(), golden_L3.end(),
    //              [&](int64_t v) { cout << "*" << v << endl; });
    //     cout << endl;
    // }

    if (check_result) {
        for (int i = 0; i < length; i++) {
            int64_t golden_result =
                (golden_L3.find(op_keys[i]) != golden_L3.end()) ? 1 : 0;
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
                         [&](size_t i) { op_keys[i] = randint64(); });
    // sort(op_keys, op_keys + length);
    cout << "\n*** Start Predecessor Test ***" << endl;
    predecessor_timer.start();
    predecessor(length);
    predecessor_timer.end();

    // if (print_debug) {
    //     for_each(golden_L3.begin(), golden_L3.end(),
    //              [&](int64_t v) { cout << "*" << v << endl; });
    // }

    cout << endl;
    if (check_result) {
        for (int i = 0; i < length; i++) {
            set<int64_t>::iterator it = golden_L3.upper_bound(op_keys[i]);
            it--;
            if (op_results[i] != *it) {
                cout << op_keys[i] << ' ' << op_results[i] << ' ' << *it
                     << endl;
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
            op_keys[i] = randint64();
            golden_L3.insert(op_keys[i]);
        }
    } else {
        for (int i = 0; i < length; i++) {
            op_keys[i] = randint64();
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
        assert(golden_L3.size() > 2);
    }
    cout << "\n*** Start Remove Test ***" << endl;
    for (int i = 0; i < length; i++) {
        if (rand() & 1) {
            auto it = golden_L3.begin();
            int rnd = (randint64() % (golden_L3.size() - 2)) +
                      1;  // don't ask -INF or INF
            advance(it, rnd);
            op_keys[i] = *it;
        } else {
            op_keys[i] = randint64();
        }
    }

    remove_timer.start();
    remove(length);
    remove_timer.end();

    if (check_result) {
        deduplication(op_keys, length);
        for (int i = 0; i < length; i++) {
            golden_L3.erase(op_keys[i]);
        }
    }

    cout << endl << "\n*** End Remove Test ***" << endl;
}

void L3_sancheck() {
    cout << "\n*** Start L3 Sancheck ***" << endl;
    init_send_buffer();
    L3_sancheck_task tst;
    push_task(-1, L3_SANCHECK, &tst, sizeof(L3_sancheck_task));
    exec();
    print_log();
    cout << endl << "\n*** End L3 Sancheck ***" << endl;
}