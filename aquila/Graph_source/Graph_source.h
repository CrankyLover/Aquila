#include <map>
#include <unordered_map>
#include <queue>
#include <optional>
#include <array>
#include <hiactor/util/data_type.hh>

typedef long long ll;
typedef unsigned int uint;

#define NOT_EXIST 4294967290

struct update_unit {
    char type;
    bool is_insert;
    ll id1;
    ll id2;
    uint label;
    update_unit(char type_arg, bool is_add_arg, ll id1_arg, ll id2_arg, uint label_arg)
            : type(type_arg), is_insert(is_add_arg), id1(id1_arg), id2(id2_arg), label(label_arg) {}
};

struct Edge {
    uint id;
    std::optional<uint> timestamp;
    Edge(uint i, std::optional<uint> ts)
        : id(i), timestamp(ts) {}
};

class DataGraph {
protected:
    ll edge_count;
    uint v_label_count;
    uint e_label_count;
    uint cc = 0;

public:

    DataGraph()
            : edge_count(0)
            , v_label_count(0)
            , e_label_count(0)
            , out_neighbors{}
            , in_neighbors{}
            , updates{}
            , v_labels{}{
    }

    // 28 is the edge label number in LDBC_SNB, change it to another number if other datasets are adopted
    std::array<std::vector<std::vector<Edge>>, 28> out_neighbors;
    std::array<std::vector<std::vector<Edge>>, 28> in_neighbors;

    std::vector<float> cardinality_of_in_edge;
    std::vector<float> cardinality_of_out_edge;
    std::vector<uint> number_of_in_edge;
    std::vector<uint> number_of_out_edge;

public:

    std::vector<update_unit> updates;
    std::vector<uint> v_labels;

    ll NumVertices() const {
        return v_labels.size();
    }

    ll NumEdges() const {
        return edge_count;
    }

    uint NumVLabels() const {
        return v_label_count;
    }

    uint NumELabels() const {
        return e_label_count;
    }

    void AddVertex(uint id, uint label);
    void RemoveVertex(uint id);
    void AddEdge(uint v1, uint v2, uint label);
    void AddEdge(uint v1, uint v2, uint label, uint time_stamp);
    void RemoveEdge(uint v1, uint v2, uint edge_label);

    void AllocateADJList(uint max_num_vertex);

    uint GetVertexLabel(uint u) const;

    const std::vector<Edge>& GetOutNeighbors(uint v, uint label) const;
    const std::vector<Edge>& GetInNeighbors(uint v, uint label) const;

    void LoadFromFile(const std::string& path);
    void LoadUpdateStream(const std::string& path);
    void PrintMetaData() const;
};

class QueryGraph {
public:
    ll edge_count;
    uint v_label_count;
    uint e_label_count;
    std::vector<std::vector<ll>> out_neighbors;
    std::vector<std::vector<uint>> out_e_labels;
    std::vector<std::vector<ll>> in_neighbors;
    std::vector<std::vector<uint>> in_e_labels;

public:

    QueryGraph()
            : edge_count(0), v_label_count(0), e_label_count(0), out_neighbors{}, out_e_labels{}, in_neighbors{},
              in_e_labels{}, v_labels{} {
    }

    std::vector<std::tuple<uint, uint, uint, uint, uint>> edges;
    // store all its unique edges to construct matching tree (from_label, to_label, edge_label, from_id, to_id).

public:

    std::vector<uint> v_labels;
    // the id of vertex must be continuous.

    uint NumVertices() const {
        return v_labels.size();
    }

    ll NumEdges() const {
        return edge_count;
    }

    uint NumVLabels() const {
        return v_label_count;
    }

    uint NumELabels() const {
        return e_label_count;
    }

    uint GetVertexLabel(ll u) const;

    void AddVertex(ll id, uint label);

    void AddEdge(ll v1, ll v2, uint label);

    const std::vector<ll> &GetOutNeighbors(ll v) const;

    const std::vector<uint> &GetOutNeighborLabels(ll v) const;

    const std::vector<ll> &GetInNeighbors(ll v) const;

    const std::vector<uint> &GetInNeighborLabels(ll v) const;

    uint GetDegree(ll v) const;

    uint GetInDegree(ll v) const;

    uint GetOutDegree(ll v) const;

    std::tuple<uint, uint, uint> GetEdgeLabel(ll v1, ll v2) const;

    std::vector<std::tuple<uint, uint, uint>> LoadFromFile(const std::string &path);

    void LoadUpdateStream(const std::string &path);

    void PrintMetaData() const;
};

struct pair_hash {
    template <typename T1, typename T2>
    size_t operator () (const std::pair<T1, T2>& p) const {
        auto h1 = std::hash<T1>{}(p.first);  
        auto h2 = std::hash<T2>{}(p.second); 
        return h1 ^ (h2 << 1);  
    }
};

struct VectorHash {
    size_t operator () (const std::vector<std::pair<uint, uint>>& v) const {
        size_t seed = 0;
        for (const auto& p : v) {
            seed ^= pair_hash{}(p) + 0x9e3779b9 + (seed << 6) + (seed >> 2);  
        }
        return seed;
    }
};

struct tuple_hash {
    template <typename T1, typename T2, typename T3>
    size_t operator () (const std::tuple<T1, T2, T3>& p) const {
        size_t seed = 0;
        auto h1 = std::hash<T1>{}(std::get<0>(p));  
        auto h2 = std::hash<T2>{}(std::get<1>(p)); 
        auto h3 = std::hash<T3>{}(std::get<2>(p));
        seed ^= (h1 + 0x9e3779b9 + (seed << 6) + (seed >> 2));
        seed ^= (h2 + 0x9e3779b9 + (seed << 6) + (seed >> 2));
        seed ^= (h3 + 0x9e3779b9 + (seed << 6) + (seed >> 2));
        return seed;  
    }
};

struct VectorTupleHash {
    size_t operator () (const std::vector<std::tuple<uint, uint, uint>>& v) const {
        size_t seed = 0;
        for (const auto& p : v) {
            seed ^= tuple_hash{}(p) + 0x9e3779b9 + (seed << 6) + (seed >> 2); 
        }
        return seed;
    }
};

class TinyDAG {
public:
    uint V;
    std::vector<std::vector<uint>> AdjList;
    std::unordered_map<uint, std::vector<uint>> NodeToQuery;
    std::unordered_map<uint, uint> QueryToNode;

    std::vector<uint> RootNodes;
    std::vector<std::unordered_map<uint, std::vector<uint>>> SpanningTrees;

    std::unordered_map<uint, uint> RootOfNode;
    std::unordered_map<uint, std::vector<std::tuple<uint ,uint, uint>>> TopologyOfNode;
    std::unordered_map<uint, std::vector<std::tuple<uint ,uint, uint>>> DifferentialTopologyOfNode;
    std::vector<uint> TempPath;

    TinyDAG() {}

    int Init(std::vector<uint>& QueryList, std::vector<std::vector<std::tuple<uint, uint, uint>>>& TopologyList);

    void DFSSearch(uint index, std::bitset<20>& visit, uint u);
};

struct Node {
    uint label;
    // below two are matched in the same position
    std::vector<uint> survive_queries;
    std::vector<uint> vertex_id;
    std::unordered_set<uint> queries;

    std::vector<std::vector<std::tuple<uint, uint, uint>>> topology;

    bool HasDAG = true;
    TinyDAG* TopologyDAG;
};

struct MatchingTree {

   std::vector<std::vector<Node*>> tree;
   std::vector<std::vector<std::vector<uint>>> children;
   // each matching tree is a vector<vector> e.g. [[2,3,4], [5], [], [], []], which represents a tree in vector
   std::unordered_map<uint, std::vector<uint>> paths;
   // store each path by the index of children for each query graph

   std::unordered_map<uint, uint> QueryToDepth;

   bool is_reverse;
};

class UnionFind {
public:

    UnionFind() {}

    uint find(uint x) {
        if (parent.find(x) == parent.end()) {
            parent[x] = x; 
        }
        if (parent[x] != x) {
            parent[x] = find(parent[x]); 
        }
        return parent[x];
    }

    void unionSets(uint x, uint y) {
        uint rootX = find(x);
        uint rootY = find(y);
        if (rootX != rootY) {
            parent[rootY] = rootX; 
        }
    }

private:
    std::unordered_map<uint, uint> parent; 
};

struct CompareTree {
    bool operator()(const MatchingTree& mtr1, const MatchingTree& mtr2) {
        uint count1 = 0, count2 = 0;
        for (const auto& layer: mtr1.tree) {
            count1 += layer.size();
        }
        for (const auto& layer: mtr2.tree) {
            count2 += layer.size();
        }
        return count1 > count2;
    }
};

class Graph {

public:
    DataGraph dataGraph;
    std::vector<QueryGraph> queryGraphs;

    std::set<std::tuple<uint, uint, uint>> all_unique_edges;
    std::vector<std::tuple<uint, uint, uint>> all_vector_unique_edges;
    // (from_label, to_label, edge_label) to store all unique edges in query graph set

    std::vector<std::unordered_map<uint, std::vector<std::pair<uint, uint>>>> edge_in_graphs;
    std::vector<MatchingTree> matching_tree_set;
    std::vector<std::unordered_map<uint, uint>> global_hash;

    std::vector<MatchingTree> temp_store;

    uint result[67] = {0};
    std::vector<std::vector<uint>> query_end_time;
    std::vector<hiactor::InternalValue> tmp_store;
    std::mutex time_mutex = {};
    std::mutex F_mutex = {};
    seastar::condition_variable cv;
    bool F[65] = {true};
    std::vector<double> elapsed_time;

    std::vector<std::tuple<hiactor::InternalValue, hiactor::InternalValue, bool, double, uint>> output;
    std::vector<std::tuple<uint, uint, double>> check;

public:
    Graph() : dataGraph()
            , queryGraphs(){
    }

    static Graph *G_Instance;

    static Graph * GetInstance() {
        if (G_Instance == NULL)
            G_Instance = new Graph;
        return G_Instance;
    }

    void LoadDataGraph(const std::string& path) {
        dataGraph.LoadFromFile(path);
    }

    void LoadQueryGraph(const std::vector<std::string>& paths) {

        for (const auto& path : paths) {
            QueryGraph queryGraph;
            std::vector<std::tuple<uint, uint, uint>> _edge = queryGraph.LoadFromFile(path);
            queryGraphs.push_back(queryGraph);
            for (const auto& item: _edge) {
                all_unique_edges.insert(item);
            }
        }


        all_vector_unique_edges.assign(all_unique_edges.begin(), all_unique_edges.end());
        // use a vector to store instead of set, which supports better access
        edge_in_graphs.resize(all_vector_unique_edges.size());

    }

    void CartesianProductWithLowCard(uint current_index, uint size, std::vector<std::pair<uint, uint>>& maps, std::vector<uint>& ids, uint index);
    void CartesianProductWithHighCard(uint size, std::vector<uint>& ids, uint index);

    void CalculateEdgeMapping();

    void ConstructMatchingTrees();

    void CalculateCardOfEdges();

    MatchingTree ConstructOneTree(std::vector<std::pair<uint, uint>>& maps, std::vector<uint>& ids, uint index, uint size);

    Node CalculateOneVertex(std::unordered_map<uint, std::bitset<15>>& visit, uint query_id, uint index, MatchingTree& matchingTree);

    void GetTopologyOrder();

    void PruneTreeSet(uint index);

    void CreateMoreTrees(uint current_index, uint size, std::vector<std::pair<uint, uint>>& maps, std::vector<uint>& ids, uint index, std::unordered_map<uint, std::vector<bool>>& visit);

    uint GetMaxTreeSize(uint index);

    uint GetMaxTreeSize();

    void AllocateADJ(uint number) {
        dataGraph.AllocateADJList(number);
    }

    void LoadUpdateStream(const std::string& path) {
        dataGraph.LoadUpdateStream(path);
    }

    void AdjustMatchingTrees();

};

class VectorPool {
public:

    static std::vector<hiactor::InternalValue>* acquire() {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        if (pool_.empty()) {
            return new std::vector<hiactor::InternalValue>();
        }
        auto* vec = pool_.back();
        pool_.pop_back();
        vec->clear(); 
        return vec;
    }

    static void release(std::vector<hiactor::InternalValue>* vec) {
        if (vec == nullptr) return;
        
        std::lock_guard<std::mutex> lock(pool_mutex_);

        if (pool_.size() < max_pool_size_) {
            pool_.push_back(vec);
        } else {

        }
    }

    static void preallocate(size_t num_objects) {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        for (size_t i = 0; i < num_objects; ++i) {
            pool_.push_back(new std::vector<hiactor::InternalValue>());
        }
    }

    static void destroy() {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        for (auto* vec : pool_) {
            delete vec;
        }
        pool_.clear();
    }

    static void set_max_pool_size(size_t size) {
        max_pool_size_ = size;
    }

public:
    inline static std::mutex pool_mutex_{};
    inline static std::vector<std::vector<hiactor::InternalValue>*> pool_{};
    inline static size_t max_pool_size_ = 10000;
};

