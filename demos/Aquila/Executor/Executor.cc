#include "Executor.h"
#include "file_sink_exe.h"
#include "GenericJoin.h"
#include "DeltaGenericJoin.h"
#include "Initialization.h"
#include "EdgeScan.h"

//ExecutorHandler& ExecutorHandler::subgraphMatch(int loop_time) {
//    _executor = new SubgraphMatch(loop_time);
//
//    if(executors.size() == 0){
//        _executor->setDf(std::move(_df));
//    }
//    else{
//        executors.back()->setNext(_executor);
//    }
//
//    executors.push_back(_executor);
//    return *this;
//}

ExecutorHandler& ExecutorHandler::genericJoin(ll vertex_number) {
    for (uint i = 0; i < vertex_number; i++) {
        _executor = new GenericJoin();

        if(executors.size() == 0){
            _executor->setDf(std::move(_df));
        }
        else{
            executors.back()->setNext(_executor);
        }

        executors.push_back(_executor);
    }
    return *this;
}

ExecutorHandler& ExecutorHandler::deltaGenericJoin(ll vertex_number, uint core_number, bool is_collect_number) {
    for (uint i = 0; i < vertex_number; i++) {
        _executor = new DeltaGenericJoin(core_number, is_collect_number);

        if(executors.size() == 0){
            _executor->setDf(std::move(_df));
        }
        else{
            executors.back()->setNext(_executor);
        }

        executors.push_back(_executor);
    }
    return *this;
}

ExecutorHandler& ExecutorHandler::edgeScan(uint& start_index, uint& end_index, uint& core_number) {

    _executor = new EdgeScan(start_index, end_index, core_number);

    if(executors.size() == 0){
        _executor->setDf(std::move(_df));
    }
    else{
        executors.back()->setNext(_executor);
    }

    executors.push_back(_executor);
    return *this;
}

ExecutorHandler& ExecutorHandler::initialization() {
    _executor = new Initialization();
    if(executors.size() == 0){
        _executor->setDf(std::move(_df));
    }
    else{
        executors.back()->setNext(_executor);
    }
    executors.push_back(_executor);
    return *this;
}

ExecutorHandler& ExecutorHandler::fileSinkExe(){
    _executor = new FileSinkExe();

    if(executors.size() == 0){
        _executor->setDf(std::move(_df));
    }
    else{
        executors.back()->setNext(_executor);
    }

    executors.push_back(_executor);
    return *this;
}

ExecutorHandler& ExecutorHandler::ini() {
    if(executors.size() > 0){
        executors[0]->ini();
    }
    else
        std::cout << "ERROR EXECUTORAPI::ini()" << std::endl;
    return *this;
}

void ExecutorHandler::execute(bool i, uint iteration_time, uint batch_size, uint end_index) {
    if(executors.size() > 0){
        executors[0]->process(i, iteration_time, batch_size, end_index);
    }
    else
        std::cout << "ERROR EXECUTORAPI::execute()" << std::endl;
}

ExecutorHandler& ExecutorHandler::redefine(uint& start_index, uint& end_index) {
    std::cout << "222\n";
    std::cout << executors.size() << "\n";
    executors[executors.size() - 1]->redefine(start_index, end_index);  // file_sink need to be redefined.

    return *this;
}
