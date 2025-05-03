# Aquila

Aquila is a high-concurrency incremental graph query system which aims to solve concurrent mutilple graph queries in dynamic graph with performance isolation guarantee.

## Building Aquila
Aquila is developed based on [hiactor](https://github.com/alibaba/hiactor), a high-performance framework for building concurrent event-driven systems using C++. The following requirements should meet.

- C++ 17 support (gcc $\geq$ 9.0).
- Linux system.
- CMake $\geq$ 3.13.1.

Build Aquila with following commands:
```
$ git clone https://github.com/CrankyLover/Aquila.git
$ git submodule update --init --recursive
$ sudo ./seastar/seastar/install-dependencies.sh
$ mkdir build
$ cd build
$ cmake -DCMAKE_INSTALL_PREFIX=/usr/local -DHiactor_CXX_DIALECT=gnu++17 \
    -DHiactor_TESTING=OFF -DSeastar_CXX_FLAGS="-DSEASTAR_DEFAULT_ALLOCATOR" ..
$ make
$ make install
$ cd demos/Aquila
```

## Run Aquila
Assume that the file path **"Aquila/build/demos/Aquila"** is located, the following command can be executed to run Aquila.
```
$ ./Aquila -v vertex_file_path -e edge_file_path -u update_file_path -q query_folder_path \
    -core used_CPU_number -batch batch_size -iters max_iteration_times -show if_collect_result_number
```

- `vertex_file_path` represents the data graph which only contains vertices, each line represents a vertex with `v vertex_id vertex_label`, e.g.
```
v 0 1
v 1 3
v 2 5
```
- `edge_file_path` represents the data graph which only contains edges, each line represents an edge with `e source_vertex_id target_vertex_id edge_label`, e.g.
```
e 0 1 2
e 1 2 9
e 0 2 1
```

- `update_file_path` represents the update stream, each line represents an updated vertex with `v vertex_id vertex_label` or an updated edge with `e source_vertex_id target_vertex_id edge_label`, e.g.
```
v 3 4
e 3 2 11
```
- `query_folder_path` represents the query folder path, which contains a set of query graph files, each consists of multiple rows and each row represents a vertex or edge, just like shown in vertex file and edge file.

- `used_CPU_number` is the number of CPU that used in Aquila, which cannot surpass the max number of available CPU.
- `batch_size` is the batch size used in Aquila.
- `max_iteration_times` is the maximum batch number processed in Aquila.
- `if_collect_result_number` is **true** if Aquila collects the number of query results of each query graph and shows them, **false** otherwise.

An example to run Aquila is shown below:
```
$ ./Aquila -v vertex.txt -e edge.txt -u updatestream.txt -q querygraphs -used_CPU_number 1 -batch 1000 -iters 1 -show true
```


