#pragma once

#include <iostream>
#include <cstdio>
#include <set>
#include <sys/mman.h>
#include <sys/stat.h>
#include "fcntl.h"
#include "tasks.h"
#include "operations.hpp"
#include "timer.hpp"
// #include "util.hpp"
#include "oracle.hpp"
#include "parlay/internal/sequence_ops.h"
#include "parlay/monoid.h"
// IRAM friendly
using namespace std;

extern bool print_debug;

Oracle oracle; 

void init_test_framework() {
    epoch_number = 0;
}

inline void read_task_file(string name, task *&tasks, int64_t &length) {
    const char *filepath = name.c_str();

    int fd = open(filepath, O_RDONLY, (mode_t)0600);

    if (fd == -1) {
        perror("Error opening file for writing");
        exit(EXIT_FAILURE);
    }

    struct stat fileInfo;

    if (fstat(fd, &fileInfo) == -1) {
        perror("Error getting the file size");
        exit(EXIT_FAILURE);
    }

    if (fileInfo.st_size == 0) {
        fprintf(stderr, "Error: File is empty, nothing to do\n");
        exit(EXIT_FAILURE);
    }

    printf("File size is %ji\n", (intmax_t)fileInfo.st_size);

    void *map = mmap(0, fileInfo.st_size, PROT_READ, MAP_SHARED, fd, 0);

    if (map == MAP_FAILED) {
        close(fd);
        perror("Error mmapping the file");
        exit(EXIT_FAILURE);
    }

    cout<<fileInfo.st_size<<' '<<sizeof(task)<<endl;

    assert(fileInfo.st_size % sizeof(task) == 0);

    length = fileInfo.st_size / sizeof(task);
    // task* mapped_tasks = (task *)map;
    // tasks = new task[length];
    tasks = (task*) map;

    parlay::parallel_for(0, length, [&](size_t i) {
        // tasks[i] = mapped_tasks[i];
        assert(tasks[i].type != empty_t);
        // printf("%ld %d\n", i, tasks[i].type);
    });

    // Don't forget to free the mmapped memory
    // if (munmap(map, fileInfo.st_size) == -1) {
    //     close(fd);
    //     perror("Error un-mmapping the file");
    //     exit(EXIT_FAILURE);
    // }

    // Un-mmaping doesn't close the file, so we still need to do that.
    // close(fd);
}

#define FILE_UPMEM

#ifdef FILE_UPMEM
static inline int getcpu(uint32_t* cpu, uint32_t* node) {
    #ifdef SYS_getcpu
    int status;
    status = syscall(SYS_getcpu, cpu, node, NULL);
    return (status == -1) ? status : 0;
    #else
    return -1; // unavailable
    #endif
}
const string base_dir = "/scratch/";
const string init_file_name = base_dir + "tree_init.buffer";
#else
const string base_dir = "/mnt/nvme2/khb2019/";
const string init_file_name = base_dir + "tree_init.buffer";
#endif

inline size_t num_blocks(size_t n, size_t block_size) {
  if (n == 0)
    return 0;
  else
    return (1 + ((n)-1) / (block_size));
}

int batch_round = 0;

extern int64_t op_keys[];
extern int64_t op_results[];

void execute(task* tasks, int actual_batch_size, int rounds) {
    turnon_all_timers(true);
    time_root("exec", [&]() {
        int _block_size = 1000;

        size_t l = num_blocks(actual_batch_size, _block_size);

        parlay::sequence<size_t>* sums = new parlay::sequence<size_t>[TASK_TYPE];
        int cnts[TASK_TYPE];
        for (int i = 0; i < TASK_TYPE; i ++) {
            sums[i] = parlay::sequence<size_t>(l);
            cnts[i] = 0;
        }

        for (int T = 0; T < rounds; T ++) {
            int i = T * actual_batch_size;
            auto mixed_task_batch = parlay::make_slice(tasks + i, tasks + i + actual_batch_size);

            parlay::internal::sliced_for(actual_batch_size, _block_size,
                       [&](size_t i, size_t s, size_t e) {
                           size_t c[TASK_TYPE] = {0};
                           for (size_t j = s; j < e; j++) {
                               int t = mixed_task_batch[j].type;
                               assert(j >= 0 && j < actual_batch_size);
                               assert(t >= 0 && t < TASK_TYPE);
                               c[t]++;
                           }
                           for (int j = 0; j < TASK_TYPE; j++) {
                               sums[j][i] = c[j];
                           }
                       });
            for (int j = 0; j < TASK_TYPE; j ++) {
                cnts[j] = parlay::scan_inplace(parlay::make_slice(sums[j]), parlay::addm<size_t>());
            }
            parlay::internal::sliced_for(
                actual_batch_size, _block_size,
                [&](size_t i, size_t s, size_t e) {
                    size_t c[TASK_TYPE];
                    for (int j = 0; j < TASK_TYPE; j++) {
                        c[j] = sums[j][i] + tasks_count[j];
                    }
                    for (size_t j = s; j < e; j++) {
                        task_t& task_type = mixed_task_batch[j].type;
                        int x = (int)task_type;
                        task& t = mixed_task_batch[j];
                        switch (task_type) {
                            case task_t::get_t: {
                                get_keys[c[x]++] = t.tsk.g.key;
                                break;
                            }
                            case task_t::update_t: {
                                update_tasks[c[x]++] = t.tsk.u;
                                break;
                            }
                            case task_t::predecessor_t: {
                                predecessor_keys[c[x]++] = t.tsk.p.key;
                                break;
                            }
                            case task_t::scan_t: {
                                assert(false);
                                break;
                            }
                            case task_t::insert_t: {
                                insert_tasks[c[x]++] = t.tsk.i;
                                break;
                            }
                            case task_t::remove_t: {
                                remove_keys[c[x]++] = t.tsk.r.key;
                                break;
                            }
                            default: {
                                assert(false);
                            }
                        }
                    }
                });

            for (int j = 0; j < TASK_TYPE; j++) {
                tasks_count[j] += cnts[j];
            }
            for (int j = 0; j < TASK_TYPE; j++) {
                if (tasks_count[j] >= actual_batch_size) {
                    batch_round ++;
                    switch(j) {
                        case (int)task_t::get_t: {
                            time_root("get", [&]() {
                                assert(false);
                            });
                            break;
                        }
                        case (int)task_t::update_t: {
                            assert(false);
                            break;
                        }
                        case (int)task_t::predecessor_t: {
                            time_root("predecessor", [&]() {
                                int len = tasks_count[j];
                                // auto id = parlay::tabulate(
                                //     length, [&](int i) { return i; });
                                // parlay::sort_inplace(id, [&](int i, int j) {
                                //     return predecessor_keys[i] < predecessor_keys[j];
                                // });

                                // parlay::parallel_for(0, len, [&](int i) {
                                //     op_keys[i] = predecessor_keys[id[i]];
                                // });

                                predecessor(predecessor_only, len,
                                            op_keys);
                                
                                // parlay::sort_inplace(op_results, [&](int i, int j) {
                                //     return id[i] < id[j];
                                // })
                            });
                            break;
                        }
                        case (int)task_t::scan_t: {
                            assert(false);
                            break;
                        }
                        case (int)task_t::insert_t: {
                            time_root("insert", [&]() {
                                int len = tasks_count[j];
                                parlay::sort_inplace(parlay::make_slice(insert_tasks, insert_tasks + len), [&](const insert_task& a, const insert_task& b) {
                                    return a.key < b.key;
                                });
                                auto dup = parlay::delayed_tabulate(
                                    len, [&](int i) -> bool { return i == 0 || insert_tasks[i].key != insert_tasks[i - 1].key; });

                                auto tasks = parlay::pack(insert_tasks, dup);
                                insert(tasks_count[j], tasks);
                            });
                            break;
                        }
                        case (int)task_t::remove_t: {
                            time_root("remove", [&]() {
                                int len = tasks_count[j];
                                auto keys = parlay::make_slice(remove_keys, remove_keys + len);
                                parlay::sort_inplace(keys);
                                auto dup = parlay::delayed_tabulate(
                                    len, [&](int i) -> bool { return i == 0 || keys[i] != keys[i - 1]; });
                                auto tasks = parlay::pack(keys, dup);
                                remove(tasks_count[j], remove_keys);
                            });
                            break;
                        }
                    }
                    tasks_count[j] = 0;
                }
            }
        }
    });
}