#pragma once

#include <parlay/primitives.h>
#include <parlay/range.h>
#include <parlay/sequence.h>
#include <cstring>
#include <vector>
using namespace std;

typedef parlay::sequence<int64_t> i64_seq;

inline int64_t i64_seq_find(const i64_seq& s, const int64_t& key) {
    int l = 0, r = s.size();
    while (r - l > 1) {
        int mid = (l + r) >> 1;
        if (s[mid] <= key) {
            l = mid;
        } else {
            r = mid;
        }
    }
    if (s[l] <= key) {
        return s[l];
    } else {
        return INT64_MIN;
    }
}

class Oracle {
   public:
    i64_seq inserted;
    // vector<i64_seq> buffers;
    Oracle() {
        inserted = parlay::sequence(1, INT64_MIN);
    }
    void insert_batch(const i64_seq& buffer) {
        auto buf = parlay::sort(buffer);
        inserted = parlay::merge(inserted, buf);
        // buffers.push_back(move(buf));
    }

    int64_t get_predecessor(const int64_t& v) {
        return i64_seq_find(inserted, v);
    }

    void predecessor_batch(const i64_seq& buf, i64_seq& results) {
        int length = buf.size();
        // int bufferssiz = buffers.size();
        // for (int i = 0; i < bufsiz; i ++) {
        
        //     int64_t ret = INT64_MIN;
        //     for (int j = 0; j < bufferssiz; j ++) {
        //         ret = max(ret, i64_seq_find(buffers[j], buf[i]));
        //     }
        //     // for_each(buffers.begin(), buffers.end(), [&](const i64_seq& s) {
        //     //     ret = max(ret, find(s, buf[i]));
        //     // });
        //     printf("%d\t%lx\n", i, ret);
        // }
        // exit(-1);
        parlay::parallel_for(0, length, [&](size_t i) {
            int64_t ret = INT64_MIN;
            ret = i64_seq_find(inserted, buf[i]);
            // for (int j = 0; j < bufferssiz; j ++) {
            //     ret = max(ret, i64_seq_find(buffers[j], buf[i]));
            // }
            results[i] = ret;
        });
        // auto tmp = parlay::map(buf, [&](int64_t x) {
            
        // });
        // return tmp;
    }

    int64_t get() {
        int tid = parlay::worker_id();
        // int i = randint64(tid) % buffers.size();
        // i64_seq a = buffers[i];
        int j = randint64(tid) % inserted.size();
        return inserted[j];
    }

    ~Oracle() {
        // buffers.clear();
    }

    // void print_all() {
    //     // for_each(buffers.begin(), buffers.end(), [&](i64_seq s) {
    //     //     // int len = s.size();
    //     //     // for (int i = 0; i < len; i++) {
    //     //     //     printf("%lx ", s[i]);
    //     //     // }
    //     //     // printf("\n");
    //     //     int64_t v = randint64_rand();
    //     //     printf("v=%lx pred=%lx\n", v, find(s, v));
    //     // });
    // }
};
