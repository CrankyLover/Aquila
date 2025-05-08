
#include<iostream>
#include<fstream>
#include<sstream>
#include<vector>
#include<algorithm>

#include "Graph_source/Graph_source.h"

Graph * Graph::G_Instance = Graph::GetInstance();

void DataGraph::AllocateADJList(uint max_num_vertex) {
    // pre-allocate some space for adjacent lists, in LDBC-SNB, the label_number for edge is 28, and this number can be modify to adjust to other datasets.
    uint label_number = 28;
    for (uint i = 0; i < label_number; i++) {
        out_neighbors[i].resize(max_num_vertex);
        in_neighbors[i].resize(max_num_vertex);
    }
}


void DataGraph::AddVertex(uint id, uint label) {
    if (id >= v_labels.size()) {
        v_labels.resize(id + 1, NOT_EXIST);
        v_labels[id] = label;
        
    } else if (v_labels[id] == NOT_EXIST) {
        v_labels[id] = label;
    }

    v_label_count = std::max(v_label_count, label + 1);
}


void DataGraph::RemoveVertex(uint id) {   
    v_labels[id] = NOT_EXIST;
}

void DataGraph::AddEdge(uint v1, uint v2, uint label) {

    out_neighbors[label][v1].emplace_back(v2, std::nullopt);
    in_neighbors[label][v2].emplace_back(v1, std::nullopt);
    
    // edge_label = 12 means peron-[knows]->person, which is a bidirectional edge
    if (label == 12) {

        out_neighbors[label][v2].emplace_back(v1, std::nullopt);
        in_neighbors[label][v1].emplace_back(v2, std::nullopt);
    }


    edge_count ++;
    e_label_count = std::max(e_label_count, label + 1);
}

void DataGraph::AddEdge(uint v1, uint v2, uint label, uint time_stamp) {

    out_neighbors[label][v1].emplace_back(v2, time_stamp);
    in_neighbors[label][v2].emplace_back(v1, time_stamp);

    if (label == 12) {

        out_neighbors[label][v2].emplace_back(v1, time_stamp);
        in_neighbors[label][v1].emplace_back(v2, time_stamp);
    }

    edge_count ++;
    e_label_count = std::max(e_label_count, label + 1);
    number_of_out_edge[label]++;
    number_of_in_edge[label]++;
}

void DataGraph::RemoveEdge(uint v1, uint v2, uint edge_label) { 
    uint v1_label = v_labels[v1];
    uint v2_label = v_labels[v2];
    uint index;
    for (uint i = 0; i < out_neighbors[edge_label][v1].size(); i++) {
        if (out_neighbors[edge_label][v1][i].id == v2) {
            index = i;
        }
    }
    out_neighbors[edge_label][v1].erase(out_neighbors[edge_label][v1].begin() + index);

    for (uint i = 0; i < in_neighbors[edge_label][v2].size(); i++) {
        if (in_neighbors[edge_label][v2][i].id == v1) {
            index = i;
        }
    }
    in_neighbors[edge_label][v2].erase(in_neighbors[edge_label][v2].begin() + index);

    if (edge_label == 12) {
        for (uint i = 0; i < out_neighbors[edge_label][v2].size(); i++) {
            if (out_neighbors[edge_label][v2][i].id == v1) {
                index = i;
            }
        }
        out_neighbors[edge_label][v2].erase(out_neighbors[edge_label][v2].begin() + index);
    
        for (uint i = 0; i < in_neighbors[edge_label][v1].size(); i++) {
            if (in_neighbors[edge_label][v1][i].id == v2) {
                index = i;
            }
        }
        in_neighbors[edge_label][v1].erase(in_neighbors[edge_label][v1].begin() + index);
    }

    number_of_out_edge[edge_label]--;
    number_of_in_edge[edge_label]--;
    edge_count--;
}

uint DataGraph::GetVertexLabel(uint id) const {
    return v_labels[id];
}


const std::vector<Edge>& DataGraph::GetOutNeighbors(uint v, uint label) const {
    return out_neighbors[label][v];
}

const std::vector<Edge>& DataGraph::GetInNeighbors(uint v, uint label) const {
    return in_neighbors[label][v];
}


void DataGraph::LoadFromFile(const std::string &path)
{
    std::ifstream ifs(path);
    uint count = 0;
    char type;
    while (ifs >> type) {
        if (type == 'v') {
            uint vertex_id, label;
            ifs >> vertex_id >> label;
            AddVertex(vertex_id, label);
        } else {
            uint from_id, to_id, label;
            ifs >> from_id >> to_id >> label;
            AddEdge(from_id, to_id, label);
        }
        count++;
        if (count % 1000000 == 0) std::cout << count << " number has loaded\n";
    }
    ifs.close();
}

void DataGraph::LoadUpdateStream(const std::string &path)
{
    std::ifstream ifs(path);

    std::string type;

    while (ifs >> type) {
        if (type == "v" || type == "-v") {
            uint vertex_id, label;
            ifs >> vertex_id >> label;
            updates.emplace_back('v', type == "v", vertex_id, 0, label);
        } else {
            uint from_id, to_id, label;
            ifs >> from_id >> to_id >> label;
            updates.emplace_back('e', type == "e", from_id, to_id, label);
        }
    }
    ifs.close();
}

void DataGraph::PrintMetaData() const  {
    std::cout << "# vertices = " << NumVertices() << "\n# edges = " << NumEdges() << std::endl;
}

void QueryGraph::AddVertex(ll id, uint label) {
    if (id >= static_cast<ll>(v_labels.size())) {
        v_labels.resize(id + 1, NOT_EXIST);
        v_labels[id] = label;
        out_neighbors.resize(id + 1);
        out_e_labels.resize(id + 1);
        in_neighbors.resize(id + 1);
        in_e_labels.resize(id + 1);
    } else if (v_labels[id] == NOT_EXIST) {
        v_labels[id] = label;
    }

    v_label_count = std::max(v_label_count, label + 1);
}

void QueryGraph::AddEdge(ll v1, ll v2, uint label) {
    auto lower = std::lower_bound(out_neighbors[v1].begin(), out_neighbors[v1].end(), v2);
    if (lower != out_neighbors[v1].end() && *lower == v2) return;

    size_t dis = std::distance(out_neighbors[v1].begin(), lower);
    out_neighbors[v1].insert(lower, v2);
    out_e_labels[v1].insert(out_e_labels[v1].begin() + dis, label);

    lower = std::lower_bound(in_neighbors[v2].begin(), in_neighbors[v2].end(), v1);
    dis = std::distance(in_neighbors[v2].begin(), lower);
    in_neighbors[v2].insert(lower, v1);
    in_e_labels[v2].insert(in_e_labels[v2].begin() + dis, label);

    if(label == 12) {  
        // handle the case where label == 12(person knows) that we need to handle bidirectional edge
        lower = std::lower_bound(in_neighbors[v1].begin(), in_neighbors[v1].end(), v2);
        dis = std::distance(in_neighbors[v1].begin(), lower);
        in_neighbors[v1].insert(lower, v2);
        in_e_labels[v1].insert(in_e_labels[v1].begin() + dis, label);

        lower = std::lower_bound(out_neighbors[v2].begin(), out_neighbors[v2].end(), v1);
        dis = std::distance(out_neighbors[v2].begin(), lower);
        out_neighbors[v2].insert(lower, v1);
        out_e_labels[v2].insert(out_e_labels[v2].begin() + dis, label);
    }

    edge_count ++;
    e_label_count = std::max(e_label_count, label + 1);
}

uint QueryGraph::GetVertexLabel(ll id) const {
    return v_labels[id];
}

const std::vector<ll>& QueryGraph::GetOutNeighbors(ll v) const {
    return out_neighbors[v];
}

const std::vector<uint>& QueryGraph::GetOutNeighborLabels(ll v) const {
    return out_e_labels[v];
}

const std::vector<ll>& QueryGraph::GetInNeighbors(ll v) const {
    return in_neighbors[v];
}

const std::vector<uint>& QueryGraph::GetInNeighborLabels(ll v) const {
    return in_e_labels[v];
}

uint QueryGraph::GetDegree(ll v) const {
    return out_neighbors[v].size() + in_neighbors[v].size();
}

uint QueryGraph::GetInDegree(ll v) const {
    return in_neighbors[v].size();
}

uint QueryGraph::GetOutDegree(ll v) const {
    return out_neighbors[v].size();
}

std::tuple<uint, uint, uint> QueryGraph::GetEdgeLabel(ll v1, ll v2) const {
    uint v1_label, v2_label, e_label;
    v1_label = GetVertexLabel(v1);
    v2_label = GetVertexLabel(v2);

    const std::vector<ll> *nbrs;
    const std::vector<uint> *elabel;
    uint other;
    if (GetOutDegree(v1) < GetInDegree(v2)) {
        nbrs = &GetOutNeighbors(v1);
        elabel = &out_e_labels[v1];
        other = v2;
    } else {
        nbrs = &GetInNeighbors(v2);
        elabel = &in_e_labels[v2];
        other = v1;
    }

    long start = 0, end = nbrs->size() - 1, mid;
    while (start <= end)
    {
        mid = (start + end) / 2;
        if (nbrs->at(mid) < other)
        {
            start = mid + 1;
        }
        else if (nbrs->at(mid) > other)
        {
            end = mid - 1;
        }
        else
        {
            e_label = elabel->at(mid);
            return {v1_label, v2_label, e_label};
        }
    }
    return {v1_label, v2_label, -1};
}

std::vector<std::tuple<uint, uint, uint>> QueryGraph::LoadFromFile(const std::string &path)
{
    std::ifstream ifs(path);
    std::vector<std::tuple<uint, uint, uint>> edge;
    char type;
    while (ifs >> type) {
        if (type == 'v') {
            uint vertex_id, label;
            ifs >> vertex_id >> label;
            AddVertex(vertex_id, label);
        } else {
            uint from_id, to_id, label;
            ifs >> from_id >> to_id >> label;
            AddEdge(from_id, to_id, label);
            this->edges.emplace_back(GetVertexLabel(from_id), GetVertexLabel(to_id), label, from_id, to_id);
            if (label == 12) {
                this->edges.emplace_back(GetVertexLabel(to_id), GetVertexLabel(from_id), label, to_id, from_id);
            }
            edge.emplace_back(GetVertexLabel(from_id), GetVertexLabel(to_id), label);
        }
    }
    ifs.close();
    return edge;
}

void QueryGraph::PrintMetaData() const  {
    std::cout << "# vertices = " << NumVertices() << "\n# edges = " << NumEdges() << std::endl;
}

void Graph::AdjustMatchingTrees() {
    for (uint i = 0; i <= 27; i++) {
        dataGraph.cardinality_of_out_edge[i] = dataGraph.number_of_out_edge[i]/ (float)(dataGraph.out_neighbors[i].size());
        dataGraph.cardinality_of_in_edge[i] = dataGraph.number_of_in_edge[i]/ (float)(dataGraph.in_neighbors[i].size());
    }
}

void Graph::CalculateEdgeMapping() {

    for (uint i = 0; i < all_vector_unique_edges.size(); i++) {

        uint from_label = std::get<0>(all_vector_unique_edges[i]);
        uint to_label = std::get<1>(all_vector_unique_edges[i]);
        uint edge_label = std::get<2>(all_vector_unique_edges[i]);

        for (uint j = 0; j < queryGraphs.size(); j++) {
            for (const auto &k: queryGraphs[j].edges) {
                // find matched edge instances
                if ((from_label == std::get<0>(k)) && (to_label == std::get<1>(k)) && (edge_label == std::get<2>(k))) {
                    // a match occurs, from query graph [j]
                    if (edge_in_graphs[i].find(j) != edge_in_graphs[i].end()) {
                        // query graph j already exist, just emplace back
                        edge_in_graphs[i][j].emplace_back(std::get<3>(k), std::get<4>(k));
                    } else {
                        std::vector<std::pair<uint, uint>> tmp = {{std::get<3>(k), std::get<4>(k)}};
                        edge_in_graphs[i][j] = tmp;
                    }
                }
            }
        }
    }

}

void Graph::CartesianProductWithLowCard(uint current_index, uint size, std::vector<std::pair<uint, uint>>& maps, std::vector<uint>& ids, uint index) {

}

uint EvaluateOverlap(const std::pair<std::vector<uint>, std::vector<uint>>& v1, const std::pair<std::vector<uint>, std::vector<uint>>& v2) {
    uint overlap1 = 0;
    for (const auto& e1: v1.first) {
        for (const auto& e2: v2.first) {
            if (e1 == e2) overlap1++;
        }
    }
    uint overlap2 = 0;
    for (const auto& e1 : v1.second) {
        for (const auto& e2: v2.second) {
            if (e1 == e2) overlap2++;
        }
    }
    return overlap1 + overlap2;
}

void Graph::CartesianProductWithHighCard(uint size, std::vector<uint>& ids, uint index) {

}

void Graph::ConstructMatchingTrees() {
    const uint threshold = 1e8;
    global_hash.resize(edge_in_graphs.size() + 1);

    for (uint i = 0; i < edge_in_graphs.size(); i++) {

        std::vector<std::pair<uint, uint>> edge_instances;
        std::vector<uint> q_ids;
        uint query_number = 0;
        for (const auto& pair: edge_in_graphs[i]) {
            uint query_id = pair.first;
            for (const auto& edges: pair.second) {
                edge_instances.push_back(edges);
                global_hash[i][query_number] = query_id;
                q_ids.push_back(query_number);
                query_number++;
            }
        }
        matching_tree_set.push_back(ConstructOneTree(edge_instances, q_ids, i, query_number));
    }
}

std::vector<std::tuple<uint, uint, uint>> DropNoneEdge(const std::vector<std::pair<uint, uint>>& hash_vector) {
    std::vector<std::tuple<uint, uint, uint>> result;
    for (uint i = 0; i < hash_vector.size(); i++) {
        if (std::get<0>(hash_vector[i]) == 0) {
            break;
        } else if (std::get<0>(hash_vector[i]) == 3) {
            continue;
        } else {
            result.emplace_back(i, std::get<0>(hash_vector[i]), std::get<1>(hash_vector[i]));
        }
    }
    return result;
}

uint isPartialEquivalence(const std::vector<std::tuple<uint, uint, uint>>& v1, const std::vector<std::tuple<uint, uint, uint>>& v2) {
    if (v1.size() >= v2.size()) {
        std::unordered_map<std::tuple<uint, uint, uint>, bool, tuple_hash> countMap1;
        for (const auto& p : v1) {
            countMap1[p] = true;
        }
        for (const auto& p : v2) {
            if (countMap1.find(p) == countMap1.end() || !countMap1[p]) {
                return 0;
            }
        }
        return 1;
    } else {
        std::unordered_map<std::tuple<uint, uint, uint>, bool, tuple_hash> countMap1;
        for (const auto& p : v2) {
            countMap1[p] = true;
        }
        for (const auto& p : v1) {
            if (countMap1.find(p) == countMap1.end() || !countMap1[p]) {
                return 0;
            }
        }
        return 2;
    }
}

std::vector<std::vector<uint>> mergePairs(const std::vector<std::pair<uint, uint>>& pairs) {
    UnionFind uf;

    for (const auto& p : pairs) {
        uf.unionSets(p.first, p.second);
    }

    // group these pairs
    std::unordered_map<uint, std::vector<uint>> groups;
    for (const auto& p : pairs) {
        uint root = uf.find(p.first); 
        groups[root].push_back(p.first); 
        groups[root].push_back(p.second); 
    }

    // transfer result into the format of vector<vector<int>>
    std::vector<std::vector<uint>> result;
    for (auto& group : groups) {
        std::unordered_set<uint> unique_group(group.second.begin(), group.second.end());
        result.emplace_back(unique_group.begin(), unique_group.end());
    }

    // sort the results
    for (auto& res : result) {
        sort(res.begin(), res.end());
    }

    return result;
}

MatchingTree Graph::ConstructOneTree(std::vector<std::pair<uint, uint>> &maps, std::vector<uint> &ids, uint index, uint size) {

    MatchingTree matchingTree;

    // this bitset to denote whether the vertex has been visited
    std::unordered_map<uint, std::bitset<15>> visit;
    // we assume that the vertex number of each query graph will not exceed 15.
    for (const auto it : ids) {
        for (uint i = 0; i < 15; i++) {
            // pay attention to that we create one visit array for each virtual q_id
            visit[it][i] = false;
        }
    }

    uint degree1 = 0, degree2 = 0;
    std::vector<uint> list1, list2;
    for (uint i = 0; i < maps.size(); i++) {
        degree1 += queryGraphs[global_hash[index][ids[i]]].GetDegree(std::get<0>(maps[i]));
        degree2 += queryGraphs[global_hash[index][ids[i]]].GetDegree(std::get<1>(maps[i]));
        list1.push_back(std::get<0>(maps[i]));
        list2.push_back(std::get<1>(maps[i]));
        visit[ids[i]][std::get<0>(maps[i])] = true;  // when meet with visit array, we must use ids[i] instead of gloabl hash[i]
        visit[ids[i]][std::get<1>(maps[i])] = true;
    }
    float d1 = degree1 / maps.size();
    float d2 = degree2 / maps.size();

    // construct root node and its only children node
    Node node1, node2;

    node1.label = std::get<0>(all_vector_unique_edges[index]);
    node1.survive_queries = ids;
    node1.vertex_id = list1;

    node2.label = std::get<1>(all_vector_unique_edges[index]);
    node2.survive_queries = ids;
    node2.vertex_id = list2;

    if (d1 > d2) {
        matchingTree.tree.push_back({new Node(node2)});
        matchingTree.tree.push_back({new Node(node1)});
        matchingTree.is_reverse = true;
    } else {
        matchingTree.tree.push_back({new Node(node1)});
        matchingTree.tree.push_back({new Node(node2)});
        matchingTree.is_reverse = false;
    }
    matchingTree.children.push_back({{0}});
    for (const auto& i : ids) {
        matchingTree.paths[i] = {};
        matchingTree.paths[i].push_back(0);
        matchingTree.paths[i].push_back(0);
    }

    // get max vertex number in query graphs
    uint max_vertex = 0;
    for (const auto& i : ids) {
        max_vertex = (queryGraphs[global_hash[index][i]].NumVertices() > max_vertex) ? queryGraphs[global_hash[index][i]].NumVertices() : max_vertex;
    }

    for (uint i = 2; i < max_vertex; i++) {
        uint parent_number = matchingTree.tree[i - 1].size();
        uint counter = 0;
        matchingTree.tree.emplace_back();
        // for each parent, we make the next vertex label to match, and decide whether split it into multiple children or not
        std::vector<std::vector<uint>> layer_children_vector = {};
        for (uint j = 0; j < parent_number; j++) {
            std::vector<uint> children_vector = {};
            Node node = *(matchingTree.tree[i - 1][j]);
            std::vector<Node*> layer_nodes;

            // first round to select which vertex to expand
            // choose random one, such as 0, because they are all the same for path
            std::vector<uint> path = matchingTree.paths[node.survive_queries[0]];
            uint last_label = 99999;
            std::vector<uint> classified_queries_remain;

            // delete queries that all verices have been visited
            std::vector<uint> delete_query;
            for (uint k = 0; k < node.survive_queries.size(); k++) {
                uint cc = 0;
                for (uint v = 0; v < queryGraphs[global_hash[index][node.survive_queries[k]]].NumVertices(); v++) {
                    if (visit[node.survive_queries[k]][v]) cc++;
                }
                if (cc == queryGraphs[global_hash[index][node.survive_queries[k]]].NumVertices()) {
                    delete_query.push_back(k);
                }
            }

            uint pp = 0;
            if (delete_query.empty()) classified_queries_remain = node.survive_queries;
            else {
                for (uint k = 0; k < node.survive_queries.size(); k++) {
                    if (pp < delete_query.size() && k == delete_query[pp]) {
                        pp++;
                        continue;
                    } else {
                        classified_queries_remain.push_back(node.survive_queries[k]);
                    }
                }
            }
            // continue if there is no left queries
            if (classified_queries_remain.empty())  {
                children_vector.push_back(-1);
                layer_children_vector.push_back(children_vector);
                continue;
            }

            // cardinality-based vertex selection
            bool is_one_query = false;
            if (classified_queries_remain.size() == 1) {
                Node _node = CalculateOneVertex(visit, classified_queries_remain[0], index, matchingTree);
                layer_nodes.push_back(new Node(_node));
                is_one_query = true;
            }

            // below are the strategy to select next matching vertex(label),
            while (!is_one_query) {
                // use it to get max label occurrence
                std::vector<uint> occurs;
                occurs.resize(30);  // this 30 represents different vertex label

                std::bitset<30> flag;

                std::unordered_map<uint, std::vector<uint>> temp_remain;
                std::unordered_map<uint, std::vector<uint>> Classified_queries_same;
                std::unordered_map<uint, std::vector<uint>> Classified_vertex_same;

                // fill the "occur" vertor
                for (const auto& query_id : classified_queries_remain) {
                    for (uint u = 0; u < path.size(); u++) {
                        auto it = std::find(matchingTree.tree[u][path[u]]->survive_queries.begin(), matchingTree.tree[u][path[u]]->survive_queries.end(), query_id);
                        if (it == matchingTree.tree[u][path[u]]->survive_queries.end()) {
                            std::cout << "ERROR in occur!" << std::endl;
                        }
                        uint pos = std::distance(matchingTree.tree[u][path[u]]->survive_queries.begin(), it);
                        uint this_vertex = matchingTree.tree[u][path[u]]->vertex_id[pos];
                        for (const auto& ver: queryGraphs[global_hash[index][query_id]].GetOutNeighbors(this_vertex)) {
                            if (visit[query_id][ver]) continue;
                            uint _label = queryGraphs[global_hash[index][query_id]].GetVertexLabel(ver);
                            if (!flag[_label]) {
                                occurs[_label] += 1;
                                flag[_label] = true;
                            }
                        }
                        for (const auto& ver: queryGraphs[global_hash[index][query_id]].GetInNeighbors(this_vertex)) {
                            if (visit[query_id][ver]) continue;
                            uint _label = queryGraphs[global_hash[index][query_id]].GetVertexLabel(ver);
                            if (!flag[_label]) {
                                occurs[_label] += 1;
                                flag[_label] = true;
                            }
                        }
                    }
                    flag.reset();
                }

                // get the maximum occurrence label
                uint maxi = 0;
                std::vector<uint> max_labels;
                uint max_label;
                for (uint pos = 0; pos < 30; pos++) {
                    if (pos != last_label && occurs[pos] > maxi) {
                        maxi = occurs[pos];
                    }
                }

                for (uint pos = 0; pos < 30; pos++) {
                    if (occurs[pos] == maxi) {
                        max_labels.push_back(pos);
                    }
                }

                std::unordered_map<uint, std::unordered_map<std::vector<std::pair<uint, uint>>, std::vector<uint>, VectorHash>> Hash_set;
                // iterate all max_labels，find a label with maximum occur freqency
                for (unsigned int current_label : max_labels) {
                    // (query_id, {hash_vector}) The first value in hash vector represents 1,2,3,4 topology relation，the second value represents edge label
                    std::unordered_map<uint, std::unordered_map<uint, std::vector<std::pair<uint ,uint>>>> hash_value;
                    std::unordered_map<uint, std::vector<std::pair<uint, uint>>> hash_vector;

                    for (const auto& query_id: classified_queries_remain) {
                        uint note = 0;
                        // 这个地方找所有邻居，它们的label都是最大共现的label
                        for (uint u = 0; u < path.size(); u++) {
                            auto it = std::find(matchingTree.tree[u][path[u]]->survive_queries.begin(), matchingTree.tree[u][path[u]]->survive_queries.end(), query_id);
                            if (it == matchingTree.tree[u][path[u]]->survive_queries.end()) {
                                std::cout << "ERROR! in selecting neis" << std::endl;
                            }
                            uint pos = std::distance(matchingTree.tree[u][path[u]]->survive_queries.begin(), it);
                            uint this_vertex = matchingTree.tree[u][path[u]]->vertex_id[pos];

                            for (const auto& ver: queryGraphs[global_hash[index][query_id]].GetOutNeighbors(this_vertex)) {
                                if (visit[query_id][ver]) continue;
                                if (queryGraphs[global_hash[index][query_id]].GetVertexLabel(ver) == current_label) {
                                    hash_value[query_id][ver].resize(15);
                                    note = 1;
                                }
                            }
                            for (const auto& ver: queryGraphs[global_hash[index][query_id]].GetInNeighbors(this_vertex)) {
                                if (visit[query_id][ver]) continue;
                                if (queryGraphs[global_hash[index][query_id]].GetVertexLabel(ver) == current_label) {
                                    hash_value[query_id][ver].resize(15);
                                    note = 1;
                                }
                            }
                            // queries that don't have the max_label
                            if ((u == path.size() - 1) && !note) {
                                temp_remain[current_label].push_back(query_id);
                                break;
                            }
                        }
                    }

                    // compute hash vector
                    for (const auto& item: hash_value) {
                        uint query_id = item.first;
                        for (const auto& pair: item.second) {
                            uint vertex_id = pair.first;

                            for (uint u = 0; u < path.size(); u++) {
                                auto it = std::find(matchingTree.tree[u][path[u]]->survive_queries.begin(), matchingTree.tree[u][path[u]]->survive_queries.end(), query_id);
                                if (it == matchingTree.tree[u][path[u]]->survive_queries.end()) {
                                    std::cout << "ERROR in calculating hash vector!" << std::endl;
                                }
                                uint pos = std::distance(matchingTree.tree[u][path[u]]->survive_queries.begin(), it);
                                uint this_vertex = matchingTree.tree[u][path[u]]->vertex_id[pos];
                                bool is_out = false, is_in = false;
                                // oue neighbors := 1，in neighbor := 2，not connect := 3，bi-edge := 4
                                for (const auto& ver: queryGraphs[global_hash[index][query_id]].GetOutNeighbors(this_vertex)) {
                                    if (visit[query_id][ver]) continue;
                                    if (ver == vertex_id) {
                                        hash_value[query_id][vertex_id][u] = {1, std::get<2>(queryGraphs[global_hash[index][query_id]].GetEdgeLabel(this_vertex, ver))};
                                        is_out = true;
                                        break;
                                    }
                                }
                                for (const auto& ver: queryGraphs[global_hash[index][query_id]].GetInNeighbors(this_vertex)) {
                                    if (visit[query_id][ver]) continue;
                                    if ((ver == vertex_id) && is_out) {
                                        hash_value[query_id][vertex_id][u].first = 4; 
                                        is_in = true;
                                        break;
                                    } else if ((ver == vertex_id) && !is_out) {
                                        hash_value[query_id][vertex_id][u] = {2, std::get<2>(queryGraphs[global_hash[index][query_id]].GetEdgeLabel(ver, this_vertex))};
                                        is_in = true;
                                        break;
                                    }
                                }
                                if (!is_out && !is_in) {
                                    // 99999 represents no edge connected
                                    hash_value[query_id][vertex_id][u] = {3, 99999}; 
                                }
                            }
                        }
                    }

                    std::vector<bool> visit_query;
                    visit_query.resize(100, false);
                    while (true) {
                        // pair<id1, id2>, query id and vertex id
                        std::unordered_map<std::vector<std::pair<uint, uint>>, std::vector<std::pair<uint, uint>>, VectorHash> temp_hash_set;
                        for (const auto& item: hash_value) {
                            uint query_id = item.first;
                            if (visit_query[query_id]) {
                                continue;
                            }
                            std::unordered_map<std::vector<std::pair<uint, uint>>, uint, VectorHash> query_hash;
                            for (const auto& pair: item.second) {
                                query_hash[pair.second] = pair.first;
                            }
                            for (const auto& pair: query_hash) {
                                temp_hash_set[pair.first].emplace_back(query_id, pair.second);
                            }
                        }

                        if (temp_hash_set.empty()) {
                            break;
                        }

                        uint max_number = 0;
                        for (const auto& item: temp_hash_set) {
                            if (item.second.size() > max_number) {
                                max_number = item.second.size();
                            }
                        }
                        for (const auto& item: temp_hash_set) {
                            if (item.second.size() == max_number) {
                                for (const auto& pair: item.second) {
                                    Classified_queries_same[current_label].push_back(std::get<0>(pair));
                                    Classified_vertex_same[current_label].push_back(std::get<1>(pair));
                                    visit_query[std::get<0>(pair)] = true;
                                    hash_vector[std::get<0>(pair)] = item.first;
                                }
                                break;
                            }
                        }
                    }

                    // push it into hash set
                    for (const auto& item : hash_vector) {
                        Hash_set[current_label][item.second].push_back(item.first);
                    }
                }

                //  Two classified vectors record query and vertex are full
                std::unordered_map<std::vector<std::tuple<uint, uint, uint>>, std::vector<uint>, VectorTupleHash> hash_set;
                std::vector<uint> classified_queries_same;
                std::vector<uint> classified_vertex_same;
                std::unordered_map<uint, float> score;
                for (const auto& item: Hash_set) {
                    uint this_label = item.first;
                    uint total = 0, reuse_total = 0;
                    for (const auto& it: item.second) {
                        total += it.second.size();
                        if (it.second.size() >= 2) {
                            reuse_total += it.second.size();
                        }
                    }
                    score[this_label] = reuse_total / total;
                }

                float max_score = -1;
                for (const auto& item: score) {
                    if (item.second > max_score) {
                        max_score = item.second;
                        max_label = item.first;
                    }
                }

                classified_queries_remain = temp_remain[max_label];
                classified_queries_same = Classified_queries_same[max_label];
                classified_vertex_same = Classified_vertex_same[max_label];
                for (const auto& p : Hash_set[max_label]) {
                    hash_set[DropNoneEdge(p.first)] = p.second;
                }
                

                std::vector<std::vector<std::tuple<uint, uint, uint>>> hash_set_key;
                std::vector<std::vector<uint>> hash_set_value;
                // use vector to store
                for (const auto& p : hash_set) {
                    hash_set_key.push_back(p.first);
                    hash_set_value.push_back(p.second);
                }

                // iterate all pairs to find whether they are in the same cluster
                std::vector<std::pair<uint, uint>> index_pair;
                for (uint x = 0; x < hash_set_key.size(); x++) {
                    for (uint y = x + 1; y < hash_set_key.size(); y++) {
                        if (isPartialEquivalence(hash_set_key[x], hash_set_key[y])) {
                            index_pair.emplace_back(x, y);
                        }
                    }
                }

                std::vector<std::vector<uint>> result_vector = mergePairs(index_pair);
                std::vector<bool> result_visit(100);

                // calculate the number of the same substructures
                uint combined_count = 0;
                uint all_count = hash_set_key.size();
                // merge child nodes
                for (const auto& each_node : result_vector) {
                    std::vector<uint> node_query;
                    std::vector<uint> node_vertex;
                    std::vector<std::vector<std::tuple<uint, uint, uint>>> node_topology;
                    for (const auto& each_number : each_node) {
                        result_visit[each_number] = true;
                        node_query.insert(node_query.end(), hash_set_value[each_number].begin(), hash_set_value[each_number].end());
                        for (uint tt = 0; tt < hash_set_value[each_number].size(); tt++) {
                            node_topology.push_back(hash_set_key[each_number]);
                        }
                        combined_count++;
                    }
                    for (const auto& p : node_query) {
                        auto it = std::find(classified_queries_same.begin(), classified_queries_same.end(), p);
                        if (it == classified_queries_same.end()) std::cout << "ERROR IN CLASSIFIED\n";
                        uint pos = std::distance(classified_queries_same.begin(), it);
                        node_vertex.push_back(classified_vertex_same[pos]);
                        visit[p][classified_vertex_same[pos]] = true;
                    }
                    Node new_node;
                    new_node.label = max_label;
                    new_node.survive_queries = node_query;
                    for (const auto& ii: node_query) new_node.queries.insert(ii);
                    new_node.vertex_id = node_vertex;
                    new_node.topology = node_topology;
                    layer_nodes.push_back(new Node(new_node));
                }

                for (uint x = 0; x < hash_set_key.size(); x++) {
                    if (!result_visit[x] && hash_set_value[x].size() >= 2) {
                        result_visit[x] = true;
                        std::vector<uint> node_query = hash_set_value[x];
                        std::vector<uint> node_vertex;
                        std::vector<std::vector<std::tuple<uint, uint, uint>>> node_topology;
                        for (const auto& p : node_query) {
                            auto it = std::find(classified_queries_same.begin(), classified_queries_same.end(), p);
                            if (it == classified_queries_same.end()) std::cout << "ERROR IN CLASSIFIED\n";
                            uint pos = std::distance(classified_queries_same.begin(), it);
                            node_vertex.push_back(classified_vertex_same[pos]);
                            node_topology.push_back(hash_set_key[x]);
                            visit[p][classified_vertex_same[pos]] = true;
                        }
                        Node new_node;
                        new_node.label = max_label;
                        new_node.survive_queries = node_query;
                        for (const auto& ii: node_query) new_node.queries.insert(ii);
                        new_node.vertex_id = node_vertex;
                        new_node.topology = node_topology;
                        layer_nodes.push_back(new Node(new_node));
                        combined_count++;
                    }
                }

                if (combined_count != 0 && all_count > combined_count) {
                    if (classified_queries_remain.empty()) { 
                        for (uint x = 0; x < hash_set_key.size(); x++) {
                            if (!result_visit[x] && hash_set_value[x].size() == 1) {
                                result_visit[x] = true;
                                auto it = std::find(classified_queries_same.begin(), classified_queries_same.end(), hash_set_value[x][0]);
                                if (it == classified_queries_same.end()) std::cout << "ERROR IN COMPACT\n";
                                uint pos = std::distance(classified_queries_same.begin(), it);
                                visit[hash_set_value[x][0]][classified_vertex_same[pos]] = true;
                                Node new_node;
                                new_node.label = max_label;
                                new_node.survive_queries = hash_set_value[x];
                                for (const auto& ii: new_node.survive_queries) new_node.queries.insert(ii);
                                new_node.vertex_id = {classified_vertex_same[pos]};
                                new_node.topology = {hash_set_key[x]};
                                layer_nodes.push_back(new Node(new_node));
                            }
                        }
                        break;
                    } else { 
                        last_label = -1;
                        for (uint x = 0; x < hash_set_key.size(); x++) {
                            if (!result_visit[x] && hash_set_value[x].size() == 1) {
                                classified_queries_remain.push_back(hash_set_value[x][0]);
                            }
                        }
                        continue;
                    }
                } 
                else if (all_count != 1 && combined_count == 0) {
                    for (uint x = 0; x < hash_set_key.size(); x++) {
                        if (!result_visit[x] && hash_set_value[x].size() == 1) {
                            result_visit[x] = true;
                            auto it = std::find(classified_queries_same.begin(), classified_queries_same.end(), hash_set_value[x][0]);
                            if (it == classified_queries_same.end()) std::cout << "ERROR IN COMPACT\n";
                            uint pos = std::distance(classified_queries_same.begin(), it);
                            visit[hash_set_value[x][0]][classified_vertex_same[pos]] = true;
                            Node new_node;
                            new_node.label = max_label;
                            new_node.survive_queries = hash_set_value[x];
                            for (const auto& ii: new_node.survive_queries) new_node.queries.insert(ii);
                            new_node.vertex_id = {classified_vertex_same[pos]};
                            new_node.topology = {hash_set_key[x]};
                            layer_nodes.push_back(new Node(new_node));
                        }
                    }
                    if (classified_queries_remain.empty()) break;
                    else continue;
                }
                else if (all_count == combined_count) {
                    if (classified_queries_remain.empty()) {
                        break;
                    } else {
                        last_label = -1;
                        continue;
                    }
                }
                else if (all_count == 1 && combined_count == 0) {
                    Node new_node;
                    new_node.label = max_label;
                    new_node.survive_queries = classified_queries_same;
                    for (const auto& ii: new_node.survive_queries) new_node.queries.insert(ii);
                    new_node.vertex_id = classified_vertex_same;
                    new_node.topology = {hash_set.begin()->first};
                    visit[classified_queries_same[0]][classified_vertex_same[0]] = true;
                    layer_nodes.push_back(new Node(new_node));
                    // create one child node individually
                    for (const auto& id : classified_queries_remain) {
                        Node _node = CalculateOneVertex(visit, id, index, matchingTree);
                        layer_nodes.push_back(new Node(_node));
                    }
                    break;
                }

            }

            // maintain tree information
            if (layer_nodes.empty()) {
                children_vector.push_back(-1);
            }
            for (const auto& item: layer_nodes) {
                for (const auto& each_query : (*item).survive_queries) {
                    matchingTree.paths[each_query].push_back(counter);
                }
                matchingTree.tree[i].push_back(item);
                children_vector.push_back(counter);
                counter++;
            }
            layer_children_vector.push_back(children_vector);

        }
        matchingTree.children.push_back(layer_children_vector);
    }
    return matchingTree;
}

Node Graph::CalculateOneVertex(std::unordered_map<uint, std::bitset<15>>& visit, uint query_id, uint index, MatchingTree& matchingTree) {
    std::vector<uint> path = matchingTree.paths[query_id];
    std::vector<uint> occurs_vector;
    std::vector<float> min_card;
    // fina the vertex with least cardinality
    const uint iniSize = 100000;
    occurs_vector.resize(15);
    min_card.resize(15, iniSize);

    for (uint u = 0; u < path.size(); u++) {
        auto it = std::find(matchingTree.tree[u][path[u]]->survive_queries.begin(), matchingTree.tree[u][path[u]]->survive_queries.end(), query_id);
        if (it == matchingTree.tree[u][path[u]]->survive_queries.end()) {
            std::cout << "ERROR IN Calculating one vertex function!" << std::endl;
        }
        uint pos = std::distance(matchingTree.tree[u][path[u]]->survive_queries.begin(), it);
        uint this_vertex = matchingTree.tree[u][path[u]]->vertex_id[pos];
        uint _index = 0;
        for (const auto& ver: queryGraphs[global_hash[index][query_id]].GetOutNeighbors(this_vertex)) {
            if (visit[query_id][ver]) continue;
            occurs_vector[ver] += 1;
            if (dataGraph.cardinality_of_out_edge[queryGraphs[global_hash[index][query_id]].GetOutNeighborLabels(this_vertex)[_index]] < min_card[ver]) {
                min_card[ver] = dataGraph.cardinality_of_out_edge[queryGraphs[global_hash[index][query_id]].GetOutNeighborLabels(this_vertex)[_index]];
            }
            _index++;
        }
        _index = 0;
        for (const auto& ver: queryGraphs[global_hash[index][query_id]].GetInNeighbors(this_vertex)) {
            if (visit[query_id][ver]) continue;
            occurs_vector[ver] += 1;
            if (dataGraph.cardinality_of_in_edge[queryGraphs[global_hash[index][query_id]].GetInNeighborLabels(this_vertex)[_index]] < min_card[ver]) {
                min_card[ver] = dataGraph.cardinality_of_in_edge[queryGraphs[global_hash[index][query_id]].GetInNeighborLabels(this_vertex)[_index]];
            }
            _index++;
        }
   }
    float min_number = iniSize;
    uint max_label, max_id;
    for (uint i = 0; i < 15; i++) {
        if (min_card[i] != iniSize && min_card[i] < min_number) {
            min_number = min_card[i];
            max_id = i;
        }
    }
    
    max_label = queryGraphs[global_hash[index][query_id]].GetVertexLabel(max_id);
    Node node;
    node.label = max_label;
    node.survive_queries = {query_id};
    for (const auto& ii: node.survive_queries) node.queries.insert(ii);
    node.vertex_id = {max_id};
    visit[query_id][max_id] = true;

    std::vector<std::pair<uint, uint>> vec;
    vec.resize(15);

    for (uint u = 0; u < path.size(); u++) {
        auto it = std::find(matchingTree.tree[u][path[u]]->survive_queries.begin(), matchingTree.tree[u][path[u]]->survive_queries.end(), query_id);
        if (it == matchingTree.tree[u][path[u]]->survive_queries.end()) {
            std::cout << "ERROR in calculating topology!" << std::endl;
        }
        uint pos = std::distance(matchingTree.tree[u][path[u]]->survive_queries.begin(), it);
        uint this_vertex = matchingTree.tree[u][path[u]]->vertex_id[pos];
        bool is_out = false, is_in = false;
        // oue neighbors := 1，in neighbor := 2，not connect := 3，bi-edge := 4
        for (const auto& ver: queryGraphs[global_hash[index][query_id]].GetOutNeighbors(this_vertex)) {
            if (ver == max_id) {
                vec[u] = {1, std::get<2>(queryGraphs[global_hash[index][query_id]].GetEdgeLabel(this_vertex, ver))};
                is_out = true;
                break;
            }
        }
        for (const auto& ver: queryGraphs[global_hash[index][query_id]].GetInNeighbors(this_vertex)) {
            if ((ver == max_id) && is_out) {
                vec[u].first = 4; 
                is_in = true;
                break;
            } else if ((ver == max_id) && !is_out) {
                vec[u] = {2, std::get<2>(queryGraphs[global_hash[index][query_id]].GetEdgeLabel(ver, this_vertex))};
                is_in = true;
                break;
            }
        }
        if (!is_out && !is_in) {
            vec[u] = {3, 99999}; 
        }
    }
    node.topology = {DropNoneEdge(vec)};
    return node;
}


void Graph::CalculateCardOfEdges() {
    dataGraph.cardinality_of_in_edge.resize(28);
    dataGraph.cardinality_of_out_edge.resize(28);
    dataGraph.number_of_in_edge.resize(28);
    dataGraph.number_of_out_edge.resize(28);

    // calculate the number of edges with each edge label, which is designed for LDBC-SNB datasets, thus the numbers are fixed now.
    // this code should be modify if other datasets are adopted.
    std::vector<std::pair<uint, uint>> vertex_start_end = {{0, 9891}, {9892, 2062060}, {2062061, 3065665}, {3065666, 6121439},
                                                           {6121440, 6211931}, {6211932, 6219886}, {6219887, 6221346},
                                                           {6221347, 6237426}, {6237427, 6237497}};
    std::vector<std::pair<uint, uint>> label_start_end = {{1, 0}, {1, 7}, {1, 6}, {1, 1}, {1, 2}, {1, 3}, {4, 2}, {4, 0},
                                                          {4, 0}, {4, 7}, {0, 7}, {0, 6}, {0, 0}, {0, 1}, {0, 2}, {0, 3},
                                                          {0, 5}, {0, 5}, {2, 0}, {2, 7}, {2, 6}, {3, 0}, {3, 6}, {5, 6},
                                                          {6, 6}, {7, 8}, {8, 8}, {3, 7}};
    for (uint i = 0; i <= 27; i++) {
        float card1 = 0, card2 = 0;
        for (uint j = vertex_start_end[label_start_end[i].first].first; j <= vertex_start_end[label_start_end[i].first].second; j++) {
            // card1 += dataGraph.GetOutNeighbors(j, i).first.size();
            card1 += dataGraph.GetOutNeighbors(j, i).size();
        }
        for (uint j = vertex_start_end[label_start_end[i].second].first; j <= vertex_start_end[label_start_end[i].second].second; j++) {
            // card2 += dataGraph.GetInNeighbors(j, i).first.size();
            card2 += dataGraph.GetInNeighbors(j, i).size();
        }
        dataGraph.cardinality_of_out_edge[i] = card1 / (float)(vertex_start_end[label_start_end[i].first].second + 1 - vertex_start_end[label_start_end[i].first].first);
        dataGraph.cardinality_of_in_edge[i] = card2 / (float)(vertex_start_end[label_start_end[i].second].second + 1 - vertex_start_end[label_start_end[i].second].first);
        dataGraph.number_of_out_edge[i] = card1;
        dataGraph.number_of_in_edge[i] = card2;
        
    }

}



void Graph::PruneTreeSet(uint index) {

}

void Graph::CreateMoreTrees(uint current_index, uint size, std::vector<std::pair<uint, uint>>& maps, std::vector<uint>& ids, uint index, std::unordered_map<uint, std::vector<bool>>& visit) {
    if (current_index == size) {
        temp_store.push_back(ConstructOneTree(maps, ids, index, size));
        return;
    } else {
        for (uint i = 0; i < edge_in_graphs[index][ids[current_index]].size(); i++) {
            if (!visit[ids[current_index]][i]) {
                maps.push_back(edge_in_graphs[index][ids[current_index]][i]);
                CreateMoreTrees(current_index + 1, size, maps, ids, index, visit);
                maps.pop_back();
            }
        }
    }
}

uint Graph::GetMaxTreeSize(uint index) {
    return matching_tree_set[index].tree.size();
}

uint Graph::GetMaxTreeSize() {
    uint ans = 0;
    for (const auto& Tree: matching_tree_set) {
        ans = (Tree.tree.size() > ans) ? Tree.tree.size() : ans;
    }
    return ans;
}

std::vector<std::tuple<uint, uint, uint>> DifferentialTopology(const std::vector<std::tuple<uint, uint, uint>>& t1, const std::vector<std::tuple<uint, uint, uint>>& t2);
void IdentifyRoots(std::unordered_map<uint, std::unordered_set<uint>>& RootsOfNode, std::vector<std::vector<uint>>& AdjList, std::bitset<20>& visited, uint root, uint u);
void ConstructCDT(std::unordered_map<uint, std::unordered_map<uint, float>>& CDT, std::unordered_map<uint, std::unordered_set<uint>>& rootsOfNode, std::unordered_map<uint, std::vector<std::tuple<uint, uint, uint>>>& topo, uint v_max);
void SelectRoots(std::unordered_map<uint, std::unordered_map<uint, float>>& CDT, uint ptr, std::vector<uint>& RightNodes, std::unordered_map<uint, std::unordered_set<uint>>& RootsOfNode, std::unordered_map<uint, uint>& NodeToRootMap, double& result, std::unordered_map<uint, uint>& answer);

void Graph::GetTopologyOrder() {
    for (auto& each_tree: matching_tree_set) {
        for (auto& each_layer: each_tree.tree) {
            for (auto& each_node: each_layer) {
                each_node->TopologyDAG = new TinyDAG();
                int ans = each_node->TopologyDAG->Init(each_node->survive_queries, each_node->topology);
                if (!ans) {
                    each_node->HasDAG = false;
                    continue;
                }

                // identify all root nodes
                std::bitset<20> VisitNode;
                for (const auto& node: each_node->TopologyDAG->AdjList) {
                    for (const auto& nei: node) {
                        VisitNode[nei] = true;
                    }
                }
                for (uint i = 0; i < each_node->TopologyDAG->V; i++) {
                    if (!VisitNode[i]) {
                        each_node->TopologyDAG->RootNodes.push_back(i);
                    }
                }
                VisitNode.reset();

                // Fill RootsOfNode
                std::unordered_map<uint, std::unordered_set<uint>> RootsOfNode;
                for (const auto& root: each_node->TopologyDAG->RootNodes) {
                    RootsOfNode[root].insert(root);
                    IdentifyRoots(RootsOfNode, each_node->TopologyDAG->AdjList, VisitNode, root, root);
                    VisitNode.reset();
                }

                // identify all right nodes
                std::unordered_set<uint> rn(each_node->TopologyDAG->RootNodes.begin(), each_node->TopologyDAG->RootNodes.end());
                std::vector<uint> RightNodes;
                for (uint i = 0; i < each_node->TopologyDAG->V; i++) {
                    if (rn.find(i) == rn.end()) {
                        RightNodes.push_back(i);
                    }
                }

                // Construct SRG and select the query plan within it
                std::unordered_map<uint, std::unordered_map<uint, float>> CDTOfNode;
                ConstructCDT(CDTOfNode, RootsOfNode, each_node->TopologyDAG->TopologyOfNode, each_node->TopologyDAG->V);
                std::unordered_map<uint, uint> NodeToRootMap, answerMap;
                double maxValue = 1e5;
                SelectRoots(CDTOfNode, 0, RightNodes, RootsOfNode, NodeToRootMap, maxValue, answerMap);

                each_node->TopologyDAG->RootOfNode = answerMap;
                for (const auto& root: each_node->TopologyDAG->RootNodes) {
                    each_node->TopologyDAG->RootOfNode[root] = root;
                }
                for (const auto& pr: each_node->TopologyDAG->RootOfNode) {
                    each_node->TopologyDAG->DifferentialTopologyOfNode[pr.first] = DifferentialTopology(each_node->TopologyDAG->TopologyOfNode[pr.second], each_node->TopologyDAG->TopologyOfNode[pr.first]);
                }

            }
        }
    }
}

int TinyDAG::Init(std::vector<uint>& QueryList, std::vector<std::vector<std::tuple<uint, uint, uint>>>& TopologyList) {

    std::unordered_map<std::vector<std::tuple<uint, uint, uint>>, std::vector<uint>, VectorTupleHash> VectorHash;
    // Merge all completely same queries
    for (uint i = 0; i < TopologyList.size(); i++) {
        VectorHash[TopologyList[i]].push_back(QueryList[i]);
    }
    AdjList.resize(VectorHash.size());
    V = VectorHash.size();
    uint id = 0;

    // Do not construct SRG with single subgraph
    if (V == 1) {
        return 0;
    }

    for (const auto& item: VectorHash) {
        for (const auto& q: item.second) {
            this->NodeToQuery[id].push_back(q);
            this->QueryToNode[q] = id;
            this->TopologyOfNode[id] = item.first;
        }
        id++;
    }

    std::vector<std::vector<std::tuple<uint, uint, uint>>> TempTopology;
    std::vector<std::vector<uint>> TempQuery;
    // store in vector format
    for (const auto& item: VectorHash) {
        TempTopology.push_back(item.first);
        TempQuery.push_back(item.second);
    }
    for (uint x = 0; x < TempTopology.size(); x++) {
        for (uint y = x + 1; y < TempTopology.size(); y++) {
            if (x == y) {
                continue;
            }
            uint ans = isPartialEquivalence(TempTopology[x], TempTopology[y]);
            if (ans == 1) {
                AdjList[QueryToNode[TempQuery[y][0]]].push_back(QueryToNode[TempQuery[x][0]]);
            } else if (ans == 2) {
                AdjList[QueryToNode[TempQuery[x][0]]].push_back(QueryToNode[TempQuery[y][0]]);
            } else {
                continue;
            }
        }
    }

    return 1;
}

double CalculateSingleCard(std::vector<std::tuple<uint, uint, uint>>& topo) {
    Graph* G = Graph::GetInstance();
    double result = 1e5;
    for (const auto& tp: topo) {
        if (std::get<1>(tp) == 1) {
            result = (G->dataGraph.cardinality_of_out_edge[std::get<2>(tp)] < result) ? G->dataGraph.cardinality_of_out_edge[std::get<2>(tp)] : result;
        } else if (std::get<1>(tp) == 2 || std::get<1>(tp) == 4) {
            result = (G->dataGraph.cardinality_of_in_edge[std::get<2>(tp)] < result) ? G->dataGraph.cardinality_of_in_edge[std::get<2>(tp)] : result;
        }
    }
    return result;
}

double CalculateDTCard(std::vector<std::tuple<uint, uint, uint>>& topo1, std::vector<std::tuple<uint, uint, uint>>& topo2) {
    Graph* G = Graph::GetInstance();
    double result = 1e5;
    std::vector<std::tuple<uint, uint, uint>> tpo = DifferentialTopology(topo1, topo2);
    for (const auto& tp: tpo) {
        if (std::get<1>(tp) == 1) {
            result = (G->dataGraph.cardinality_of_out_edge[std::get<2>(tp)] < result) ? G->dataGraph.cardinality_of_out_edge[std::get<2>(tp)] : result;
        } else if (std::get<1>(tp) == 2 || std::get<1>(tp) == 4) {
            result = (G->dataGraph.cardinality_of_in_edge[std::get<2>(tp)] < result) ? G->dataGraph.cardinality_of_in_edge[std::get<2>(tp)] : result;
        }
    }
    return result;
}

void IdentifyRoots(std::unordered_map<uint, std::unordered_set<uint>>& RootsOfNode, std::vector<std::vector<uint>>& AdjList, std::bitset<20>& visited, uint root, uint u) {
    visited[u] = true;
    for (const auto& v: AdjList[u]) {
        if (!visited[v]) {
            RootsOfNode[v].insert(root);
            IdentifyRoots(RootsOfNode, AdjList, visited, root, v);
        }
    }
}

void ConstructCDT(std::unordered_map<uint, std::unordered_map<uint, float>>& CDT, std::unordered_map<uint, std::unordered_set<uint>>& rootsOfNode, std::unordered_map<uint, std::vector<std::tuple<uint, uint, uint>>>& topo, uint v_max) {
    for (uint u = 0; u < v_max; u++) {
        for (const auto& v: rootsOfNode[u]) {
            if (v == u) {
                CDT[v][v] = CalculateSingleCard(topo[v]);  
                // CDT[v][v] = 1;
            } else {
                CDT[u][v] = CalculateDTCard(topo[v], topo[u]);
                // CDT[u][v] = 1;
            }
        }
    }
}


void SelectRoots(std::unordered_map<uint, std::unordered_map<uint, float>>& CDT, uint ptr, std::vector<uint>& RightNodes, std::unordered_map<uint, std::unordered_set<uint>>& RootsOfNode, std::unordered_map<uint, uint>& NodeToRootMap, double& result, std::unordered_map<uint, uint>& answer) {
    if (ptr == RightNodes.size()) {
        double re = 0;
        std::unordered_set<uint> R;
        for (const auto& n: NodeToRootMap) {
            re += CDT[n.first][n.second];
            R.insert(n.second);
        }
        for (const auto& r: R) {
            re += CDT[r][r];
        }
        if (re < result) {
            result = re;
            answer = NodeToRootMap;
        }
        return;
    }
    for (const auto& root: RootsOfNode[RightNodes[ptr]]) {
        NodeToRootMap[RightNodes[ptr]] = root;
        SelectRoots(CDT, ptr + 1, RightNodes, RootsOfNode, NodeToRootMap, result, answer);
    }
}


void DFSFound(const std::vector<std::vector<uint>>& adj, std::bitset<20>& visited, std::unordered_map<uint, std::vector<uint>>& tree, uint u) {

    visited[u] = true;

    for (const auto& v : adj[u]) {
        if (!visited[v]) {
            tree[u].push_back(v);
            DFSFound(adj, visited, tree, v);
        }
    }
}

std::vector<std::tuple<uint, uint, uint>> DifferentialTopology(const std::vector<std::tuple<uint, uint, uint>>& t1, const std::vector<std::tuple<uint, uint, uint>>& t2) {
    std::vector<std::tuple<uint, uint, uint>> result;

    std::unordered_set<std::tuple<uint, uint, uint>, tuple_hash> set_t1(t1.begin(), t1.end());

    for (const auto& element : t2) {
        if (set_t1.find(element) == set_t1.end()) {
            result.push_back(element);
        }
    }

    return result;
}
