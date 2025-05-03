#include "DataFlow/DataFlow.h"
#include "file_sink_exe.h"
#include "Timing.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>
#include <fstream>

#include <cstdlib>

unsigned original_shuffle(const hiactor::InternalValue& input, uint core_number) {
    return (*input.vectorValue).back().intValue % core_number;
}

hiactor::DataType DoNothing(const hiactor::DataType& input) {
    hiactor::DataType data;
    data.type = hiactor::DataType::VECTOR;
    data._data = input._data;
    return data;
}


// mini-batch的操作使得这个edgescan全部在一个shard上面操作了，有没有可能可以平均分配一
hiactor::DataType edgeScan(const hiactor::DataType& input, uint si, uint ei) {
    Graph* G = Graph::GetInstance();
    std::vector<hiactor::InternalValue> result;
    uint id_in_batch = (*input._data.vectorValue)[0].intValue;

    if ((G -> dataGraph.updates[id_in_batch]).type == 'v') {

    } else {
        for (uint u = 0; u < G -> all_vector_unique_edges.size(); u++) {
            // 找到这条边所对应的unique edge
            if ((std::get<0>(G -> all_vector_unique_edges[u]) == G -> dataGraph.GetVertexLabel((G -> dataGraph.updates[id_in_batch]).id1)) &&
                (std::get<1>(G -> all_vector_unique_edges[u]) == G -> dataGraph.GetVertexLabel((G -> dataGraph.updates[id_in_batch]).id2)) &&
                (std::get<2>(G -> all_vector_unique_edges[u]) == (G -> dataGraph.updates[id_in_batch]).label)) {

                std::vector<hiactor::InternalValue> vec;
                hiactor::InternalValue time{id_in_batch}, edge_index_value{u}, position{0}, value_1{}, value_2{};
                std::vector<uint> queries = G -> matching_tree_set[u].tree[0][0]->survive_queries;
                std::vector<hiactor::InternalValue> V;
                for (const auto& ii: queries) {
                    hiactor::InternalValue val{ii};
                    V.push_back(val);
                }
                hiactor::InternalValue query_vec;

                query_vec.vectorValue = new std::vector<hiactor::InternalValue>(V);

                if (G -> matching_tree_set[u].is_reverse) { // 这里用is reverse来判�???
                    value_1.intValue = (G -> dataGraph.updates[id_in_batch]).id2;
                    value_2.intValue = (G -> dataGraph.updates[id_in_batch]).id1;
                } else {
                    value_1.intValue = (G -> dataGraph.updates[id_in_batch]).id1;
                    value_2.intValue = (G -> dataGraph.updates[id_in_batch]).id2;
                }

                vec.push_back(time);
                vec.push_back(edge_index_value);
                vec.push_back(position);
                vec.push_back(query_vec);
                vec.push_back(value_1);
                vec.push_back(value_2);

                hiactor::InternalValue row{};
                row.vectorValue = new std::vector<hiactor::InternalValue>(vec);
                result.push_back(row);

                break;
            }
        }
    //    G->time_mutex.lock();
    //    G->output.emplace_back(hiactor::InternalValue{id_in_batch}, hiactor::InternalValue{}, 1, stopTimingAndGive(), 10000);
    //    G->time_mutex.unlock();
    }


    hiactor::DataType data;
    data.type = hiactor::DataType::VECTOR;
    data._data.vectorValue = new std::vector<hiactor::InternalValue>(result);

    return data;

}