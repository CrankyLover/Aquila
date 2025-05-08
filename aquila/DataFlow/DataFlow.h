#pragma once

#include "actor/map.act.h"
#include "actor/shuffle.act.h"
#include "actor/barrier.act.h"
#include "actor/map_partition.act.h"
#include "actor/flat_map.act.h"
#include "actor/file_sink.act.h"
#include "actor/file_source.act.h"
#include "actor/data_sink.act.h"
#include "actor/data_source.act.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>
#include <fstream>

// 定义操作类型
enum : uint8_t {
    _map_operator = 0,
    _shuffle_operator = 1,
    _barrier_operator = 2,
    _flat_map_operator = 3,
    _map_partition_operator = 4,
    _scope_ingress_operator = 5,
    _scope_egress_operator = 6
};

// 定义操作的基�??
struct Operation {
    uint8_t type;
    hiactor::MapFunc _map_func;
    hiactor::HashFunc _shuffle_func;
    int loop_num;
};

// 定义执行计划�??
// 实现一个withedmap类，接收两个数据流，将其中一个缓存在本地，当所有数据缓存完成后，再开始执行另一个数据流
// 需要的前提条件：bool is_end。在一个数据流结束之后，需要将一个is_end的判断返回到dataflow api层面
// 前提条件与data_sink存在的问题类似，需要研究一下DataType中的load_from函数，实现其应有的功能，看看能否跑�?
class DataFlow {
private:
    hiactor::DataType _file_name;
    hiactor::DataType _input_data;
    std::vector<hiactor::OperatorBase*> operators;
    std::vector<Operation> operations;
    bool _from_file = false;
    int job_id;
    hiactor::Source* first_ptr;

public:
    DataFlow() {}

    void setID(int id) {
        job_id = id;
        std::cout << "DataFlow_id: " << job_id << std::endl;
    }

    void reDefineFunction(hiactor::MapFunc func) {
        operators[0]->setFunc(std::move(func));
    }

    int getOperationSize() {
        return operations.size();
    }

    DataFlow& fromFile(std::string filename) {
        _file_name = hiactor::DataType(filename.c_str());
        _from_file = true;
        return *this;
    }

    // 惰性的map操作
    DataFlow& map(hiactor::MapFunc mapFunc) {
        operations.push_back({_map_operator, mapFunc, hiactor::HashFunc(), 0});
        std::cout << "map pushed\n";
        return *this;
    }

    // 惰性的shuffle操作
    DataFlow& shuffle(hiactor::HashFunc shuffleFunc) {
        operations.push_back({_shuffle_operator, hiactor::MapFunc(), shuffleFunc, 0});
        std::cout << "shuffle pushed\n";
        return *this;
    }

    // 惰性的barrier操作
    DataFlow& barrier() {
        operations.push_back({_barrier_operator, hiactor::MapFunc(), hiactor::HashFunc(), 0});
        std::cout << "barrier pushed\n";
        return *this;
    }

    DataFlow& flatmap(hiactor::MapFunc mapFunc) {
        operations.push_back({_flat_map_operator, mapFunc, hiactor::HashFunc(), 0});
        std::cout << "flatmap pushed\n";
        return *this;
    }

    DataFlow& map_partition(hiactor::MapFunc mapFunc) {
        operations.push_back({_map_partition_operator, mapFunc, hiactor::HashFunc(), 0});
        std::cout << "mapparrtition pushed\n";
        return *this;
    }

    DataFlow& _scope_ingress(int _loop_times){
        operations.push_back({_scope_ingress_operator, hiactor::MapFunc(), hiactor::HashFunc(), _loop_times});
        return *this;
    }

    DataFlow& _scope_egress(){
        operations.push_back({_scope_egress_operator, hiactor::MapFunc(), hiactor::HashFunc(), 0});
        return *this;
    }


    void execute(bool i, uint iteration_time, uint batch_size, uint end_index);
};