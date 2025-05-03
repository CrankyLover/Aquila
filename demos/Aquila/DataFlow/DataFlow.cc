#include "DataFlow.h"

// operation[startIndex - 1] = scope_ingress
// operation[endIndex] = scope_egress
unsigned buildOperators(hiactor::scope_builder& sc_builder, const std::vector<Operation>& operations, std::vector<hiactor::OperatorBase*>& operators, unsigned startIndex) {
    unsigned endIndex = startIndex;
    for (unsigned i = startIndex; i < operations.size() + startIndex; i = i + 1) {
        endIndex = i;
        hiactor::OperatorBase* _operator;
        auto& op = operations[i - startIndex];
        switch (op.type) {
            case _map_operator: {
                std::cout << "Map: " << i << "\n";
                _operator = new Map(sc_builder, i);
                hiactor::MapFunc mapFuncCopy = op._map_func;
                _operator->setFunc(std::move(mapFuncCopy));
                operators.push_back(_operator);

                break;
            }
            case _shuffle_operator: {
                std::cout << "Shuffle: " << i << "\n";
                _operator = new Shuffle(sc_builder, i);
                hiactor::HashFunc hashFuncCopy = op._shuffle_func;
                _operator->setFunc(std::move(hashFuncCopy));
                operators.push_back(_operator);

                break;
            }
            case _barrier_operator: {
                std::cout << "Barrier: " << i << "\n";
                _operator = new Barrier(sc_builder, i);
                operators.push_back(_operator);

                break;
            }
            case _flat_map_operator: {
                std::cout << "FlatMap: " << i << "\n";
                _operator = new FlatMap(sc_builder, i);
                hiactor::MapFunc mapFuncCopy = op._map_func;
                _operator->setFunc(std::move(mapFuncCopy));
                operators.push_back(_operator);

                break;
            }
            case _map_partition_operator: {
                std::cout << "Mappartition: " << i << "\n";
                _operator = new MapPartition(sc_builder, i);
                hiactor::MapFunc mapFuncCopy = op._map_func;
                _operator->setFunc(std::move(mapFuncCopy));
                operators.push_back(_operator);

                break;
            }
            case _scope_ingress_operator: {
                for(int _cur_loop_times = 1 ; _cur_loop_times <= op.loop_num ; _cur_loop_times++ ) {
                    sc_builder.enter_sub_scope(hiactor::scope<hiactor::actor_group>(_cur_loop_times));
                    endIndex = buildOperators(sc_builder, operations, operators, i+1);
                }
                i = endIndex;
                break;
            }
            case _scope_egress_operator: {
                sc_builder.back_to_parent_scope();

                return endIndex;
            }
            default: {
                std::cout << "Error. Invalid operator type." << '\n';
                hiactor::actor_engine().exit();
            }
        }
        if(operators.size() > 1) operators[operators.size() - 2]->setNext(std::move(*operators[operators.size() - 1]));
    }
    return endIndex;
}


void DataFlow::execute(bool i, uint iteration_time, uint batch_size, uint end_index) {
    // 这个地方也不是i==1，也应该是第一次执行初始化的时候对应的i
    if (i) {
        hiactor::scope_builder sc_builder;
        sc_builder
                .set_shard(0)
                .enter_sub_scope(hiactor::scope<hiactor::actor_group>(1)); // if meet with scope_ingress and egress, then we need to
        // retain the true size of operations, including all the actors
        // inside the inner scope by the loop time, which can be implemented
        // by a new function.
        std::cout << operations.size() << "\n";

        if(job_id == 0) {
            buildOperators(sc_builder, operations, operators, 1); // 1, 7, 13, 19...
        } else {
            buildOperators(sc_builder, operations, operators, (job_id - 1) * (operations.size() + 2) + 22); // 1, 7, 13, 19...
        }

        hiactor::Source* _source_operator;
        if(_from_file) {
            if(job_id == 0) {
                _source_operator = new FileSource(sc_builder, 0);  // 0
            } else {
                _source_operator = new FileSource(sc_builder, (job_id - 1) * (operations.size() + 2) + 21);  //
            }
        } else {
            _source_operator = new DataSource(sc_builder, job_id * (operations.size() + 2));
        }

        hiactor::Sink* _sink_operator;
        if(job_id == 0) {
            _sink_operator = new FileSink(sc_builder, (operations.size() + 2) - 1);  // 5, 11, 17, 23...
        } else {
            _sink_operator = new FileSink(sc_builder, (job_id - 1) * (operations.size() + 2) + 22 + operations.size());  // 5, 11, 17, 23...
        }

        // source -> operators -> sink
        _source_operator -> setNext(std::move(*operators[0]));
        operators[operators.size() - 1] -> setNext(std::move(*_sink_operator));
        first_ptr = _source_operator;
    }

    // push iteration_time and batch_size information into a vector and send to the first operator.
    hiactor::DataType data;
    std::vector<hiactor::InternalValue> vec;
    hiactor::InternalValue v1{}, v2{}, v3{};
    v1.intValue = iteration_time;
    v2.intValue = batch_size;
    v3.intValue = end_index;
    vec.push_back(v1);
    vec.push_back(v2);
    vec.push_back(v3);
    data._data.vectorValue = new std::vector<hiactor::InternalValue>(vec);
    // std::cout << iteration_time << " " << batch_size << "\n";

    first_ptr -> process(0, std::move(data));
    first_ptr -> receive_eos();
}