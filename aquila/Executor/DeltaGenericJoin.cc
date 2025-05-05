#include "DataFlow/DataFlow.h"
#include "file_sink_exe.h"
#include "Timing.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>


const long long M_SIZE = 99999999999;

unsigned delta_shuffle_by_last_element(const hiactor::InternalValue& input, uint core_number) { // shuffle by the last element to keep load balance
    return (*input.vectorValue).back().intValue % core_number;
}

void CandidatesProposal(std::unordered_map<uint, uint>& map_neighbor, uint label, const std::vector<hiactor::InternalValue>& prefix_array, const uint& time_stamp, const std::vector<std::tuple<uint, uint, uint>>& TopologyList) {
    Graph *G = Graph::GetInstance();
    uint size, index;
    auto result = M_SIZE;
    std::vector<Edge> min_neighbors;
    // 先找到size最小的一个neighbors
    for (const auto &i: TopologyList) {
        if (std::get<1>(i) == 1) {
            size = G->dataGraph.GetOutNeighbors(prefix_array[std::get<0>(i) + 4].intValue, std::get<2>(i)).size();
            if (size < result) {
                result = size;
                index = std::get<0>(i);
                min_neighbors = G->dataGraph.GetOutNeighbors(prefix_array[std::get<0>(i) + 4].intValue, std::get<2>(i));
            }
        } else if (std::get<1>(i) == 2 || std::get<1>(i) == 4) {
            size = G->dataGraph.GetInNeighbors(prefix_array[std::get<0>(i) + 4].intValue, std::get<2>(i)).size();
            if (size < result) {
                result = size;
                index = std::get<0>(i);
                min_neighbors = G->dataGraph.GetInNeighbors(prefix_array[std::get<0>(i) + 4].intValue, std::get<2>(i));
            }
        }
    }

    for (uint i = 0; i < min_neighbors.size(); i++) {
            uint flag = 0;
            // 两次filter，第一次是点的label，第二次是点是否在partial result中出现过
            if (G->dataGraph.GetVertexLabel(min_neighbors[i].id) == label) {
                for (auto it = prefix_array.begin() + 4; it != prefix_array.end(); ++it) {
                    if ((*it).intValue == min_neighbors[i].id) {
                        flag = 1;
                        break;
                    }
                }
            }
            if (flag == 0 && (min_neighbors[i].timestamp == std::nullopt || min_neighbors[i].timestamp < time_stamp)) {
                map_neighbor.insert({min_neighbors[i].id, 0});
            }
        }

    for (const auto &i: TopologyList) {
        if (std::get<0>(i) == index) continue;
        uint id = prefix_array[std::get<0>(i) + 4].intValue;

        if (std::get<1>(i) == 1) {
            uint _index = 0;
            for (const auto &_vertex: G->dataGraph.GetOutNeighbors(id, std::get<2>(i))) {
                auto val = map_neighbor.find(_vertex.id);
                if (val != map_neighbor.end() && (G->dataGraph.GetOutNeighbors(id, std::get<2>(i))[_index].timestamp == std::nullopt || G->dataGraph.GetOutNeighbors(id, std::get<2>(i))[_index].timestamp < time_stamp)) {
                    map_neighbor[_vertex.id] = val->second + 1;
                }
                _index++;
            }
        } else if (std::get<1>(i) == 2 || std::get<1>(i) == 4) {
            uint _index = 0;
            for (const auto &_vertex: G->dataGraph.GetInNeighbors(id, std::get<2>(i))) {
                auto val = map_neighbor.find(_vertex.id);
                if (val != map_neighbor.end() && (G->dataGraph.GetInNeighbors(id, std::get<2>(i))[_index].timestamp == std::nullopt || G->dataGraph.GetInNeighbors(id, std::get<2>(i))[_index].timestamp < time_stamp)) {
                    map_neighbor[_vertex.id] = val->second + 1;
                }
                _index++;
            }
        }
    }
}

void CandidatesFilter(std::unordered_map<uint, uint>& map_neighbor, const std::vector<hiactor::InternalValue>& prefix_array, const uint& time_stamp, const std::vector<std::tuple<uint, uint, uint>>& TopologyList) {
    Graph *G = Graph::GetInstance();
    for (const auto &i: TopologyList) {
        uint id = prefix_array[std::get<0>(i) + 4].intValue;
        if (std::get<1>(i) == 1) {
            uint _index = 0;
            for (const auto &_vertex: G->dataGraph.GetOutNeighbors(id, std::get<2>(i))) {
                auto val = map_neighbor.find(_vertex.id);
                if (val != map_neighbor.end() && (G->dataGraph.GetOutNeighbors(id, std::get<2>(i))[_index].timestamp == std::nullopt || G->dataGraph.GetOutNeighbors(id, std::get<2>(i))[_index].timestamp < time_stamp)) {
                    map_neighbor[_vertex.id] = val->second + 1;
                }
                _index++;
            }
        } else if (std::get<1>(i) == 2 || std::get<1>(i) == 4) {
            uint _index = 0;
            for (const auto &_vertex: G->dataGraph.GetInNeighbors(id, std::get<2>(i))) {
                auto val = map_neighbor.find(_vertex.id);
                if (val != map_neighbor.end() && (G->dataGraph.GetInNeighbors(id, std::get<2>(i))[_index].timestamp == std::nullopt || G->dataGraph.GetInNeighbors(id, std::get<2>(i))[_index].timestamp < time_stamp)) {
                    map_neighbor[_vertex.id] = val->second + 1;
                }
                _index++;
            }
        }
    }
}


hiactor::DataType DGJ(const hiactor::DataType& input, bool is_collect_number) {
    Graph *G = Graph::GetInstance();
    std::vector<hiactor::InternalValue> prefix_array = *input._data.vectorValue;
    uint time_stamp = prefix_array[0].intValue;
    uint edge_index = prefix_array[1].intValue;
    uint position = prefix_array[2].intValue;
    std::vector<hiactor::InternalValue> con;
    uint join_time = prefix_array.size() - 4;
    // G->time_mutex.lock();
    // G->check.emplace_back(hiactor::global_shard_id(), prefix_array.size() - 4, stopTimingAndGive());
    // G->time_mutex.unlock();
    // G->time_mutex.lock();
    // if (time_stamp == 900) {
    //     for (const auto& ii: *prefix_array[3].vectorValue) {
    //         std::cout << G->global_hash[edge_index][ii.intValue] << " ";
    //     }
    //     std::cout << "Values: ";
    //     for (uint i = 4; i < prefix_array.size(); i++) {
    //         std::cout << prefix_array[i].intValue << " ";
    //     }
    //     std::cout << stopTimingAndGive() << "\n";
    // }
    // G->time_mutex.unlock();

    if (G->matching_tree_set[edge_index].tree.size() == join_time) {
        if (is_collect_number) {
            G->time_mutex.lock();
            for (const auto& q: *prefix_array[3].vectorValue) {
                    G->result[G->global_hash[edge_index][q.intValue]]++;
            }
            // G->output.emplace_back(prefix_array[0], prefix_array[3], 0,  stopTimingAndGive(), edge_index);
            G->time_mutex.unlock();
        }
    } else {

        if (is_collect_number) {
            for (const auto &q: *(prefix_array[3].vectorValue)) {
                if (G->queryGraphs[G->global_hash[edge_index][q.intValue]].NumVertices() == join_time) {
                    G->time_mutex.lock();
                    G->result[G->global_hash[edge_index][q.intValue]]++;
                    G->time_mutex.unlock();
                }
            }
        }
        
        for (const auto &item: G->matching_tree_set[edge_index].children[join_time - 1][position]) {
            if (item == 4294967295) {
            //     G->time_mutex.lock();
            // //     for (const auto& q: *prefix_array[4].vectorValue) {
            // //         G->result[q.intValue]++;
            // //    }
            //     // if (prefix_array[0].intValue == 900) {
            //     //     for (const auto& ii: *prefix_array[3].vectorValue) {
            //     //         std::cout << G->global_hash[edge_index][ii.intValue] << " ";
            //     //     }
            //     //     std::cout << "           " << edge_index << "\n";
            //     // }
            //     G->output.emplace_back(prefix_array[0], prefix_array[3], 0,  stopTimingAndGive(), edge_index);
            //     G->time_mutex.unlock();
                continue;
            } else {
                std::vector<hiactor::InternalValue> query_ids;
                Node* node = G->matching_tree_set[edge_index].tree[join_time][item];
                for (const auto &q: *(prefix_array[3].vectorValue)) {
                    if (G->queryGraphs[G->global_hash[edge_index][q.intValue]].NumVertices() == join_time) {
                        // G->time_mutex.lock();
                        // // G->result[q.intValue]++;
                        // // if (prefix_array[0].intValue == 900) {
                        // //     std::cout << G->global_hash[edge_index][q.intValue] << " ";
                        // //     std::cout << "           " << edge_index << "\n";
                        // // }
                        // G->output.emplace_back(prefix_array[0], q, 1,  stopTimingAndGive(), edge_index);
                        // G->time_mutex.unlock();
                        continue;
                    }
                    if (node->queries.find(q.intValue) != node->queries.end()) {
                        query_ids.push_back(q);
                    }
                }
                if (query_ids.empty()) continue;
                        // 这里不可能出现remain query里面全部都不属于这个node的query的情�????????
                        std::vector<hiactor::InternalValue> ori_prefix;
                        if (!node->HasDAG) {
                            //query的位置保持现有的内容即可，直接join到根结点
                            //由于它们的topology都是一样的，所以node.topology直接取下标为0的就�????????
                            std::unordered_map<uint, uint> map_neighbor;
                            uint total_vector = node->topology[0].size();
                            CandidatesProposal(map_neighbor, node->label, prefix_array, time_stamp, node->topology[0]);
                            // 这个地方不修改prefix下标�????????4的vector的值了
                            for (auto &it: map_neighbor) {
                                if (it.second == total_vector - 1) {
                                    ori_prefix = prefix_array;
                                    ori_prefix[2].intValue = item;
                                    ori_prefix[3].vectorValue = new std::vector<hiactor::InternalValue>(query_ids);
                                    hiactor::InternalValue new_ext{it.first};
                                    ori_prefix.push_back(new_ext);
                                    hiactor::InternalValue Vec{};
                                    Vec.vectorValue = new std::vector<hiactor::InternalValue>(ori_prefix);
                                    con.push_back(Vec);
                                }
                            }
                            // if (con.empty()) {
                            //     G->time_mutex.lock();
                            //     hiactor::InternalValue va;
                            //     va.vectorValue = new std::vector<hiactor::InternalValue>(query_ids);
                            //     G->output.emplace_back(prefix_array[0], va, 0,  stopTimingAndGive(), edge_index);
                            //     G->time_mutex.unlock();
                            // }
                        } else {
                            // 对于每个remain——query对应的DAG节点，计算出他们的根，对于每个根：先join到根节点，然后后续依次join
                            std::unordered_set<uint> DAGRoots;
                            std::unordered_map<uint, std::unordered_set<uint>> RootToNodes;
                            std::unordered_map<uint, std::vector<uint>> NodeToRemainQueries;
                            TinyDAG* DAG = node->TopologyDAG;

                            for (const auto& i: query_ids) {
                                uint n1 = DAG->QueryToNode[i.intValue];
                                uint n2 = DAG->RootOfNode[n1];
                                DAGRoots.insert(n2);
                                RootToNodes[n2].insert(n1);
                                NodeToRemainQueries[n1].push_back(i.intValue);
                            }

                            std::unordered_map<uint, std::vector<uint>> NewVertexToQueries;
                            for (const auto& root: DAGRoots) {
                            
                                std::unordered_map<uint, uint> map_neighbor;
                                uint total_vector = DAG->TopologyOfNode[root].size();
                                CandidatesProposal(map_neighbor, node->label, prefix_array, time_stamp, DAG->TopologyOfNode[root]);
                                // 这里把这个hashmap进行filter，然后把满足条件的value清零
                                for (auto it = map_neighbor.begin(); it != map_neighbor.end();) {
                                    if (it->second != total_vector - 1) {
                                        it = map_neighbor.erase(it);
                                    } else {
                                        it->second = 0;
                                        ++it;
                                    }
                                }
                                std::unordered_map<uint, std::vector<uint>> neighbor_query;
                                for (const auto& DNode: RootToNodes[root]) {
                                    if (DNode == root) {
                                        for (const auto& row: map_neighbor) {
                                            uint v = row.first;
                                            for (const auto& q: NodeToRemainQueries[DNode]) {
                                                neighbor_query[v].push_back(q);
                                            }
                                        }
                                    } else {
                                        CandidatesFilter(map_neighbor, prefix_array, time_stamp, DAG->DifferentialTopologyOfNode[DNode]);
                                        uint number_vector = DAG->DifferentialTopologyOfNode[DNode].size();
                                        for (const auto& row: map_neighbor) {
                                            uint v = row.first;
                                            if (row.second == number_vector) {
                                                for (const auto& q: NodeToRemainQueries[DNode]) {
                                                    neighbor_query[v].push_back(q);
                                                }
                                            }
                                        }
                                    }
                                    for (auto& row: map_neighbor) {
                                        row.second = 0;
                                    }
                                }

                                for (auto &it: map_neighbor) {
                                    // 这里的判断目标是去掉不属于任何一个query的candidate
                                    if (neighbor_query.find(it.first) != neighbor_query.end()) {
                                        for (const auto& q: neighbor_query[it.first]) {
                                            NewVertexToQueries[it.first].push_back(q);
                                        }
                                    }
                                }
                            }
                            // std::bitset<100> judge;
                            for (const auto& row: NewVertexToQueries) {
                                ori_prefix = prefix_array;
                                ori_prefix[2].intValue = item;
                                std::vector<hiactor::InternalValue> V;
                                for (const auto& q: row.second) {
                                    hiactor::InternalValue val{q};
                                    V.push_back(val);
                                    // judge[q] = true;
                                }
                                ori_prefix[3].vectorValue = new std::vector<hiactor::InternalValue>(V);
                                hiactor::InternalValue new_ext{row.first};
                                ori_prefix.push_back(new_ext);
                                hiactor::InternalValue Vec{};
                                Vec.vectorValue = new std::vector<hiactor::InternalValue>(ori_prefix);
                                con.push_back(Vec);
                            }
                            // for (const auto& pp: query_ids) {
                            //     if (judge[pp.intValue] == false) {
                            //         G->time_mutex.lock();
                            //         // if (prefix_array[0].intValue == 900) {
                            //         //     std::cout << G->global_hash[edge_index][pp.intValue] << " ";
                            //         //     std::cout << "           " << edge_index << "\n";
                            //         // }
                            //         G->output.emplace_back(prefix_array[0], pp, 1,  stopTimingAndGive(), edge_index);
                            //         G->time_mutex.unlock();
                            //     }
                            // }
                        }
                    }
                }
            } 
    // G->time_mutex.lock();
    // G->result[hiactor::global_shard_id()] += con.size();
    // G->time_mutex.unlock();

    hiactor::DataType Data;
    Data.type = hiactor::DataType::VECTOR;
    Data._data.vectorValue = new std::vector<hiactor::InternalValue>(con);

    return Data;
}
