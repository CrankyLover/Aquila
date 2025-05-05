#pragma once

#include "DataFlow/DataFlow.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>
#include <fstream>

#include "Graph_source/Graph_source.h"
#include "Graph_source/Graph_source_data.h"


class Executor{
public:
    Executor() = default;
    ~Executor() = default;

    virtual void process(bool i, uint iteration_time, uint batch_size, uint end_index) = 0;
    virtual void setDf(DataFlow&& _df) = 0;
    virtual std::string get_type() = 0;
    virtual void setNext(Executor* next_exe){
        std::cout << "didn't override" << std::endl;
    }
    virtual void ini() = 0;
    virtual void redefine(uint& start_index, uint& end_index) = 0;

};

class ExecutorHandler {
private:

    std::vector<Executor*> executors; 
    Executor* _executor;
    DataFlow _df;
    int ptr = 0;
    int job_id;

public:
    ExecutorHandler(int id) {
        std::string filename = "/home/zouzq/hiactor/demos/LDBC-IC9/actor/node.txt";
        _df.fromFile(filename);
        job_id = id;
        _df.setID(job_id);
    }

    ExecutorHandler& fileSinkExe();

    ExecutorHandler& deltaGenericJoin(ll edge_number, uint core_number, bool is_collect_number);

    ExecutorHandler& edgeScan(uint& start_index, uint& end_index, uint& core_number);

    // 触发执行计划 process
    void execute(bool i, uint iteration_time, uint batch_size, uint end_index);

    // trigger the initialization phrase
    ExecutorHandler& ini();

    ExecutorHandler& redefine(uint& start_index, uint& end_index);
};