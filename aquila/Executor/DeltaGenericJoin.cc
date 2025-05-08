#include "DataFlow/DataFlow.h"
#include "file_sink_exe.h"
#include "Timing.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>


const long long M_SIZE = 1e9;

enum : uint {
	out_neighbor = 1,
    in_neighbor = 2,
    no_neighbor = 3,
    bi_neighbor = 4,
    nei_index = 0,
    nei_edge_type = 1,
    nei_edge_label = 2,
    time_index = 0,
    edgeindex_index = 1,
    pos_index = 2,
    queries_index = 3,
	adjusted_index = 4,
};

unsigned delta_shuffle_by_last_element(const hiactor::InternalValue& input, uint core_number) { // shuffle by the last element to keep load balance
    return (*input.vectorValue).back().intValue % core_number;
}

void CandidatesProposal(std::unordered_map<uint, uint>& map_neighbor, uint label, const std::vector<hiactor::InternalValue>& prefix_array, const uint& time_stamp, const std::vector<std::tuple<uint, uint, uint>>& TopologyList) {
    Graph *G = Graph::GetInstance();
    uint size, index;
    auto result = M_SIZE;
    std::vector<Edge> min_neighbors;
    // find the minimal neighbor size
    for (const auto &i: TopologyList) {
        if (std::get<nei_edge_type>(i) == out_neighbor) {
            size = G->dataGraph.GetOutNeighbors(prefix_array[std::get<nei_index>(i) + adjusted_index].intValue, std::get<nei_edge_label>(i)).size();
            if (size < result) {
                result = size;
                index = std::get<nei_index>(i);
                min_neighbors = G->dataGraph.GetOutNeighbors(prefix_array[std::get<nei_index>(i) + adjusted_index].intValue, std::get<nei_edge_label>(i));
            }
        } else if (std::get<nei_edge_type>(i) == in_neighbor || std::get<nei_edge_type>(i) == bi_neighbor) {
            size = G->dataGraph.GetInNeighbors(prefix_array[std::get<nei_index>(i) + adjusted_index].intValue, std::get<nei_edge_label>(i)).size();
            if (size < result) {
                result = size;
                index = std::get<nei_index>(i);
                min_neighbors = G->dataGraph.GetInNeighbors(prefix_array[std::get<nei_index>(i) + adjusted_index].intValue, std::get<nei_edge_label>(i));
            }
        }
    }

    for (uint i = 0; i < min_neighbors.size(); i++) {
            uint flag = 0;
            // filter by vertex visited
            if (G->dataGraph.GetVertexLabel(min_neighbors[i].id) == label) {
                for (auto it = prefix_array.begin() + adjusted_index; it != prefix_array.end(); ++it) {
                    if ((*it).intValue == min_neighbors[i].id) {
                        flag = 1;
                        break;
                    }
                }
            }
            // only expand valid vertex
            if (flag == 0 && (min_neighbors[i].timestamp == std::nullopt || min_neighbors[i].timestamp < time_stamp)) {
                map_neighbor.insert({min_neighbors[i].id, 0});
            }
        }

    for (const auto &i: TopologyList) {
        if (std::get<nei_index>(i) == index) continue;
        uint id = prefix_array[std::get<nei_index>(i) + adjusted_index].intValue;

        if (std::get<nei_edge_type>(i) == out_neighbor) {
            uint _index = 0;
            for (const auto &_vertex: G->dataGraph.GetOutNeighbors(id, std::get<nei_edge_label>(i))) {
                auto val = map_neighbor.find(_vertex.id);
                if (val != map_neighbor.end() && (G->dataGraph.GetOutNeighbors(id, std::get<nei_edge_label>(i))[_index].timestamp == std::nullopt || G->dataGraph.GetOutNeighbors(id, std::get<nei_edge_label>(i))[_index].timestamp < time_stamp)) {
                    map_neighbor[_vertex.id] = val->second + 1;
                }
                _index++;
            }
        } else if (std::get<nei_edge_type>(i) == in_neighbor || std::get<nei_edge_type>(i) == bi_neighbor) {
            uint _index = 0;
            for (const auto &_vertex: G->dataGraph.GetInNeighbors(id, std::get<2>(i))) {
                auto val = map_neighbor.find(_vertex.id);
                if (val != map_neighbor.end() && (G->dataGraph.GetInNeighbors(id, std::get<nei_edge_label>(i))[_index].timestamp == std::nullopt || G->dataGraph.GetInNeighbors(id, std::get<nei_edge_label>(i))[_index].timestamp < time_stamp)) {
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
        uint id = prefix_array[std::get<nei_index>(i) + adjusted_index].intValue;
        if (std::get<nei_edge_type>(i) == out_neighbor) {
            uint _index = 0;
            for (const auto &_vertex: G->dataGraph.GetOutNeighbors(id, std::get<nei_edge_label>(i))) {
                auto val = map_neighbor.find(_vertex.id);
                if (val != map_neighbor.end() && (G->dataGraph.GetOutNeighbors(id, std::get<2>(i))[_index].timestamp == std::nullopt || G->dataGraph.GetOutNeighbors(id, std::get<nei_edge_label>(i))[_index].timestamp < time_stamp)) {
                    map_neighbor[_vertex.id] = val->second + 1;
                }
                _index++;
            }
        } else if (std::get<nei_edge_type>(i) == in_neighbor || std::get<nei_edge_type>(i) == bi_neighbor) {
            uint _index = 0;
            for (const auto &_vertex: G->dataGraph.GetInNeighbors(id, std::get<2>(i))) {
                auto val = map_neighbor.find(_vertex.id);
                if (val != map_neighbor.end() && (G->dataGraph.GetInNeighbors(id, std::get<nei_edge_label>(i))[_index].timestamp == std::nullopt || G->dataGraph.GetInNeighbors(id, std::get<nei_edge_label>(i))[_index].timestamp < time_stamp)) {
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
    uint time_stamp = prefix_array[time_index].intValue;
    uint edge_index = prefix_array[edgeindex_index].intValue;
    uint position = prefix_array[pos_index].intValue;
    std::vector<hiactor::InternalValue> con;
    uint join_time = prefix_array.size() - adjusted_index;

    if (G->matching_tree_set[edge_index].tree.size() == join_time) {
        if (is_collect_number) {
            G->time_mutex.lock();
            for (const auto& q: *prefix_array[queries_index].vectorValue) {
                G->result[G->global_hash[edge_index][q.intValue]]++;
            }
            G->time_mutex.unlock();
        }
    } else {

        if (is_collect_number) {
            for (const auto &q: *(prefix_array[queries_index].vectorValue)) {
                if (G->queryGraphs[G->global_hash[edge_index][q.intValue]].NumVertices() == join_time) {
                    G->time_mutex.lock();
                    G->result[G->global_hash[edge_index][q.intValue]]++;
                    G->time_mutex.unlock();
                }
            }
        }
        
        for (const auto &item: G->matching_tree_set[edge_index].children[join_time - 1][position]) {
            // it is a leaf node
            if (item == 4294967295) {
                continue;
            } else {
                std::vector<hiactor::InternalValue> query_ids;
                Node* node = G->matching_tree_set[edge_index].tree[join_time][item];
                for (const auto &q: *(prefix_array[queries_index].vectorValue)) {
                    if (G->queryGraphs[G->global_hash[edge_index][q.intValue]].NumVertices() == join_time) {
                        // fully matched queries are passed
                        continue;
                    }
                    if (node->queries.find(q.intValue) != node->queries.end()) {
                        query_ids.push_back(q);
                    }
                }
                if (query_ids.empty()) continue;

                std::vector<hiactor::InternalValue> ori_prefix;
                if (!node->HasDAG) {
                    // node that does not have a SRG
                    std::unordered_map<uint, uint> map_neighbor;
                    uint total_vector = node->topology[0].size();
                    CandidatesProposal(map_neighbor, node->label, prefix_array, time_stamp, node->topology[0]);
                        
                    for (auto &it: map_neighbor) {
                        if (it.second == total_vector - 1) {
                            ori_prefix = prefix_array;
                            ori_prefix[pos_index].intValue = item;
                            ori_prefix[queries_index].vectorValue = new std::vector<hiactor::InternalValue>(query_ids);
                            hiactor::InternalValue new_ext{it.first};
                            ori_prefix.push_back(new_ext);
                            hiactor::InternalValue Vec{};
                            Vec.vectorValue = new std::vector<hiactor::InternalValue>(ori_prefix);
                            con.push_back(Vec);
                        }
                    }

                } else {

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
                        // Do the expand for each root SRG node
                        std::unordered_map<uint, uint> map_neighbor;
                        uint total_vector = DAG->TopologyOfNode[root].size();
                        CandidatesProposal(map_neighbor, node->label, prefix_array, time_stamp, DAG->TopologyOfNode[root]);
                        // Do filter operation and reset the hashmap to 0
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
                                // Do filter for each right SRG node
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
                            if (neighbor_query.find(it.first) != neighbor_query.end()) {
                                for (const auto& q: neighbor_query[it.first]) {
                                    NewVertexToQueries[it.first].push_back(q);
                                }
                            }
                        }
                    }
                    // make one row for each new vaild vertex
                    for (const auto& row: NewVertexToQueries) {
                        ori_prefix = prefix_array;
                        ori_prefix[pos_index].intValue = item;
                        std::vector<hiactor::InternalValue> V;
                        for (const auto& q: row.second) {
                            hiactor::InternalValue val{q};
                            V.push_back(val);
                        }
                        ori_prefix[queries_index].vectorValue = new std::vector<hiactor::InternalValue>(V);
                        hiactor::InternalValue new_ext{row.first};
                        ori_prefix.push_back(new_ext);
                        hiactor::InternalValue Vec{};
                        Vec.vectorValue = new std::vector<hiactor::InternalValue>(ori_prefix);
                        con.push_back(Vec);
                    }

                }
            }
        }
    } 

    hiactor::DataType Data;
    Data.type = hiactor::DataType::VECTOR;
    Data._data.vectorValue = new std::vector<hiactor::InternalValue>(con);

    return Data;
}
