# PIM-tree-baseline-jumppush-pushpull

This repository comprise the range-partitioning baselines of the PIM-tree.

## Note

1. This baseline use the same input file format as the PIM-tree. Please use the workload file generated by it.
2. These two baselines have a small DPU program that fits into DPU IRAM, therefore they don't divide it.
3. Please don't use the sorted initialization file as the init file for this baseline, because they'll cause severe load imbalance and the program will crash.
    Instead, use the uniform random initialization file. You can use skewed file for test, but severe skewness can still cause program crash (as reported in the paper).

## Requirements

This implementation was created to run the experiments in the paper. Current implementation of PIM-tree can only run on [UPMEM](https://www.upmem.com/) machines. This codeset is built on [UPMEM SDK](https://sdk.upmem.com/).

## Building

To build everything, enter the root of the cloned repository. You will need to change `NR_DPUS` in `Makefile` to the number of DPU modules on your machine before you start building. Then run your desired command listed below.

| Command | Description |
|---------|-------------|
|make | Compiles and links|
|make test | Compiles and start a test case|
|make clean | Cleans the previous compiled files|

The build produces files in `build` directory, which can be classfied into two types:
- Host program(`range_partitioning_skip_list_host`): The host application used to drive the system.
- DPU program(`range_partitioning_skip_list_dpu`): Application run on the DPUs.

## Running

The basic structure for a running command is:

```
./build/range_partitioning_skip_list_host [--arguments]
```

Please refer to the following list to set the arguments:

| Argument | Abbreviation | Used Scenario* | Usage Description |
|---------|-------------|-------------|-------------|
| `--file` | `-f` | F |Use `--file [init_file] [test_file]` to include the path of dataset files|
| `--length` | `-l` | T |Use `-l [init_length] [test_length]` to set the number of initializing and testing queries|
| `--get` | `-g` | TG |Use `-g [get_ratio]` to set the ratio of Get queries|
| `--predecessor` | `-p` | T |Use `-p [predecessor_ratio]` to set the ratio of Predecessor queries|
| `--insert` | `-i` | T |Use `-i [insert_ratio]` to set the ratio of Insert queries|
| `--remove` | `-r` | TG |Use `-r [remove_ratio]` to set the ratio of Delete queries|
| `--scan` | `-s` | TG |Use `-s [scan_ratio]` to set the ratio of Scan queries|
| `--nocheck` | `-c` | A |Stop checking the correctness of the tested data structure|
| `--noprint` | `-t` | A |Do not print timer name when timing|
| `--nodetail` | `-d` | A |Do not print detail|
| `--init_state` | NA | F |Enables efficient initialization (by always doing Push-Pull when initializing)|

Please refer to the following list for used scenarios*:

| Used Scenario Abbreviation | Scenario Description |
|---------|-------------|
| F | Testing with existing dataset files |
| T | Directly testing with self-set Zipfian workloads |
| A | Any time |


## Code Structure

This section will introduce the contents in each code file and how the codes are organized.

### ```/common``` (similar to the PIM-tree)

This directory contains the configurations of the hardware used and the data structure.

```common.h```: Numeric configurations of the data structure.

```settings.h```: Configuration for program modes. (whether to turn on (1) debugging (2) tracking of various metrics)

```task_base.h```: Macros defining the interface of DPU function calls (tasks sent between CPU and DPUs).

### ```/dpu``` (different from the PIM-tree)

This directory contains source codes used to build the DPU applications.

```dpu.c```: Main function for the DPU applications. Handling function calls (tasks) from the CPU host application.

```gc.h```: DPU side garbage collection.

```l3.h```: Implementation of range-partitioning skip list.

```data_block.h```: Definitions and functions related to "data_block", a variable length vector on MRAM and WRAM, used for scan.

```hashtable_l3size.h```: Definition and implementation of the local linear-probing hash table on each DPU, used in GET, INSERT, and DELETE operations.

```node_dpu.h```: Defining node classes used.

```storage.h```: Maintaining consistency of WRAM heap.

### ```/host``` (similar to the PIM-tree)

This directory contains source codes used to build the CPU host application.

```host.cpp```: Main function for the CPU host application.

```operation.hpp```: Functions for database queries on the CPU host side.

```host_util.hpp```: Functions for scan.

### ```/pim_base/include```

These two baselines use a trimmed version of `pim_base` without support of CPU-DPU pipelining and workload generation.
Remaining parts are similar.