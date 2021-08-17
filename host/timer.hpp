#pragma once
#include <ctime>
#include <cstdio>
#include <string>
#include <iostream>
#include <chrono>
using namespace std;
using namespace std::chrono;

class timer;

vector<timer*> all_timers;

class timer {
public:
    string name;
    // clock_t total_time;
    duration<double> total_time;
    int count;
    high_resolution_clock::time_point start_time, end_time;
    timer(string _name) {
        name = _name;
        // total_time = 0;
        count = 0;
        all_timers.push_back(this);
    }
    void print() {
        cout<<"Timer "<<name<<": "<<endl;
        printf("Average Time: %lf\n", total_time.count() / this->count);
        printf("Total Time: %lf\n", total_time.count());
        printf("Occurance: %d\n", count);
        printf("\n");
    }
    void start() {
        start_time = high_resolution_clock::now();
    }
    void end() {
        end_time = high_resolution_clock::now();
        total_time += duration_cast<duration<double>>(end_time - start_time);
        count ++;
    }
    void reset() {
        total_time = duration<double>();
        count = 0;
    }
};

template <class F>
inline void time_f(timer& _timer, F f) {
    // clock_t start = clock();
    high_resolution_clock::time_point t1 = high_resolution_clock::now();
    f();
    high_resolution_clock::time_point t2 = high_resolution_clock::now();
    // clock_t end = clock();
    _timer.total_time += duration_cast<duration<double>>(t2 - t1);
    _timer.count ++;
}

inline void print_all_timers() {
    for (auto& timer : all_timers)  // access by reference to avoid copying
    {
        timer->print();
    }
}

timer send_task_timer("send_task");
timer receive_task_timer("receive_task");
timer execute_timer("execute");
timer exec_timer("exec");