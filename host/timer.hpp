#pragma once
#include <ctime>
#include <cstdio>
#include <string>
#include <iostream>
#include <chrono>
#include <set>
#include <map>
using namespace std;
using namespace std::chrono;

class timer;

map<string, timer*> all_timers;
// vector<timer*> all_timers;
// set<string> all_timer_names;

string curname;

class timer {
public:
    string name;
    // clock_t total_time;
    duration<double> total_time;
    int count;
    bool on;
    high_resolution_clock::time_point start_time, end_time;
    vector<double> details;

    timer(string _name) {
        name = _name;
        // total_time = 0;
        details.clear();
        count = 0;
        if (!all_timers.count(_name)) {
            all_timers[_name] = this;
        }
        // all_timers.push_back(this);
        // all_timer_names.insert(_name);
    }
    ~timer() {
        details.clear();
    }

    void print() {
        if (count == 0) return;
        cout<<"Timer "<<name<<": "<<endl;
        printf("Average Time: %lf\n", total_time.count() / this->count);
        printf("Total Time: %lf\n", total_time.count());
        printf("Details: ");
        for (size_t i = 0; i < details.size(); i ++) {
            printf("%lf ", details[i]);
        }
        printf("\n");
        printf("Occurance: %d\n", count);
        printf("\n");
        fflush(stdout);
    }
    void print_succinct() {
        if (count == 0) return;
        // printf("Average Time: %lf\n", total_time.count() / this->count);
        printf("%lf\n", total_time.count() / this->count);
    }
    void print_name() {
        if (count == 0) return;
        cout<<name<<endl;
    }

    void turnon(bool val) {
        on = val;
    }
    void start() {
        start_time = high_resolution_clock::now();
    }
    void end(bool detail = false) {
        if (on) {
            end_time = high_resolution_clock::now();
            auto d = duration_cast<duration<double>>(end_time - start_time);
            total_time += d;
            count ++;
            if (detail) {
                details.push_back(d.count());
            }
        }
    }
    void reset() {
        total_time = duration<double>();
        count = 0;
        details.clear();
    }
};

inline bool timer_turnon = false;
inline timer* start_timer(string name) {
    if (!all_timers.count(name)) {
        timer* tt = new timer(name);
        tt->reset();
        tt->turnon(timer_turnon);
    }
    timer* t = all_timers[name];
    t->start();
    return t;
}

template <class F>
inline void time_root(string basename, F f, bool detail = true) {
    curname = basename;
    cout << curname << endl;
    if (!all_timers.count(curname)) {
        timer* tt = new timer(curname);
        tt->reset();
        tt->turnon(timer_turnon);
    }
    timer* t = all_timers[curname];
    // t->turnon(true);
    t->start();
    f();
    t->end(detail);
}

#define INNER_TIMER (true)

template <class F>
inline void time_nested(string deltaname, F f, bool detail = true) {
    if (INNER_TIMER) {
        string prename = curname;
        curname = curname + string(" -> ") + deltaname;
        cout << curname << endl;
        if (!all_timers.count(curname)) {
            timer* tt = new timer(curname);
            tt->reset();
            tt->turnon(timer_turnon);
        }
        timer* t = all_timers[curname];
        // t->turnon(true);
        t->start();
        f();
        t->end(detail);
        curname = prename;
    } else {
        f();
    }
}

inline string time_nested_start(string deltaname) {
    string prename = curname;
    curname = curname + string(" -> ") + deltaname;
    cout << curname << endl;
    if (!all_timers.count(curname)) {
        timer* tt = new timer(curname);
        tt->reset();
        tt->turnon(timer_turnon);
    }
    timer* t = all_timers[curname];
    t->start();
    return prename;
}

inline void time_nested_end(string prename, bool detail = true) {
    timer* t = all_timers[curname];
    t->end(detail);
    curname = prename;
}

template <class F>
inline void time_f(string name, F f, bool detail = true) {
    cout << name << endl;
    if (!all_timers.count(name)) {
        timer* tt = new timer(name);
        tt->reset();
        tt->turnon(timer_turnon);
    }
    timer* t = all_timers[name];
    t->turnon(true);
    t->start();
    f();
    t->end(detail);
}

enum pt_type {
    pt_full,
    pt_succinct_time,
    pt_name
};

inline void print_all_timers(pt_type t) {
    for (auto& timer : all_timers)  // access by reference to avoid copying
    {
        if (t == pt_full) {
            timer.second->print();
        } else if (t == pt_succinct_time) {
            timer.second->print_succinct();
        } else if (t == pt_name) {
            timer.second->print_name();
        }
    }
}

inline void turnon_all_timers(bool val) {
    timer_turnon = val;
    for (auto& timer : all_timers)  // access by reference to avoid copying
    {
        timer.second->turnon(val);
    }
}

inline void reset_all_timers() {
    for (auto& timer : all_timers)  // access by reference to avoid copying
    {
        timer.second->reset();
    }
}

inline void delete_all_timers() {
    for (auto& timer : all_timers)  // access by reference to avoid copying
    {
        delete timer.second;
    }
    all_timers.clear();
}
// timer send_task_timer("send_task");
// timer receive_task_timer("receive_task");
// timer execute_timer("execute");
// timer exec_timer("exec");