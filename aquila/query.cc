#include "Executor/file_sink_exe.h"
#include "Executor/Timing.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/util/data_type.hh>

#include <dirent.h>
#include <sys/stat.h>

void RunQueryInBatch(int i, ll vertex_number, uint start_index, uint end_index, uint core_number, uint batch_size, bool is_collect_number) {

    static ExecutorHandler exe_hd(0);

    static bool firstRun = true;

    if (firstRun) {
        exe_hd.edgeScan(start_index, end_index, core_number)
              .deltaGenericJoin(vertex_number - 2 + 1, core_number, is_collect_number)
              .fileSinkExe()
              .ini()
              .execute(firstRun, i, batch_size, end_index);
        firstRun = false;
    } else {
        exe_hd.execute(firstRun, i, batch_size, end_index);
    }

}

bool isFile(const std::string& path) {
    struct stat info;
    if (stat(path.c_str(), &info) != 0) {
        return false;
    }
    return S_ISREG(info.st_mode);
}


int main(int ac, char* av[]) {

    hiactor::actor_app app;

    std::string vertex_path;
    std::string edge_path;
    std::string query_path;
    std::string update_path;
    uint core_number = 1;
    uint batch_size = 1000;
    uint iteration_times = 10;
    bool is_collect_number = false;

    for (int i = 1; i < ac; i++) {
        if (std::string(av[i]) == "-v") {
            vertex_path = av[i + 1];
        } else if (std::string(av[i]) == "-e") {
            edge_path = av[i + 1];
        } else if (std::string(av[i]) == "-q") {
            query_path = av[i + 1];
        } else if (std::string(av[i]) == "-u") {
            update_path = av[i + 1];
        } else if (std::string(av[i]) == "-core") {
            core_number = std::atoi(av[i + 1]);
        } else if (std::string(av[i]) == "-batch") {
            batch_size = std::atoi(av[i + 1]);
        } else if (std::string(av[i]) == "-iters") {
            iteration_times = std::atoi(av[i + 1]);
        } else if (std::string(av[i]) == "-show") {
            if (std::string(av[i + 1]) == "true") {
                is_collect_number = true;
            } else {
                is_collect_number = false;
            }
        }
    }

    std::vector<std::string> query_path_list;
    DIR* dir = opendir(query_path.c_str());
    if (dir == nullptr) {
        std::cerr << "Could not open directorty: " << query_path << "\n";
    }
    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        std::string file_name = entry->d_name;
        if (file_name == "." || file_name == "..") {
            continue;
        }
        std::string file_path = query_path + "/" + file_name;
        if (isFile(file_path)) {
            query_path_list.push_back(file_path);
        }
    }
    closedir(dir);

    std::cout << vertex_path << "\n";
    std::cout << edge_path << "\n";
    std::cout << query_path << "\n";
    std::cout << update_path << "\n";
    std::cout << core_number << "\n";
    std::cout << batch_size << "\n";
    std::cout << iteration_times << "\n";

    for (uint i = 0; i < query_path_list.size(); i++) {
        std::cout <<"Query Number " << i << ",  corresponds to query file: ";
        std::cout << query_path_list[i] << "\n";
    }

    Graph *G = Graph::GetInstance();
    uint max_number = 6400000;
    G->AllocateADJ(max_number);
    std::cout << "Allocate successful!\n";
    // G->LoadDataGraph("/home/zouziqi/Dataset/LDBC-SF1/vertex.txt");
    // std::cout << "vertex loaded!\n";
    // G->LoadDataGraph("/home/zouziqi/Dataset/LDBC-SF1/edge.txt");
    // std::cout << "edge loaded!\n";
    // G->LoadUpdateStream("/home/zouziqi/Dataset/LDBC-SF1/updatestream.txt");
    // std::cout << "update loaded!\n";
    // G->LoadDataGraph("/home/zouziqi/Dataset/LDBC-SF10/vertex_SF10.txt");
    // std::cout << "vertex loaded!\n";
    // G->LoadDataGraph("/home/zouziqi/Dataset/LDBC-SF10/edge_0.txt");
    // G->LoadDataGraph("/home/zouziqi/Dataset/LDBC-SF10/edge_1.txt");
    // G->LoadDataGraph("/home/zouziqi/Dataset/LDBC-SF10/edge_2.txt");
    // G->LoadDataGraph("/home/zouziqi/Dataset/LDBC-SF10/edge_3.txt");
    // G->LoadDataGraph("/home/zouziqi/Dataset/LDBC-SF10/edge_4.txt");
    // std::cout << "edge loaded!\n";
    // G->LoadUpdateStream("/home/zouziqi/Dataset/LDBC-SF10/updatestream_SF10.txt");
    // std::cout << "update loaded!\n";
    G->LoadDataGraph(vertex_path);
    std::cout << "vertex loaded!\n";
    G->LoadDataGraph(edge_path);
    std::cout << "edge loaded!\n";
    G->LoadUpdateStream(update_path);
    std::cout << "update loaded!\n";
    G->CalculateCardOfEdges();
    G->LoadQueryGraph(query_path_list);
    G->CalculateEdgeMapping();
    G->ConstructMatchingTrees();
    G->GetTopologyOrder();

    // print matching tree information
    for (uint i = 0; i < G->matching_tree_set.size(); i++) {
        std::cout <<  "EDGE: " << i << " NEXT::::::::::::::::::::::::::::::::::::::::::::::\n";
        for (auto& iii : G->matching_tree_set[i].tree) {
            std::cout << "LLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLAYER: " << "\n";
            for (auto & jjj : iii) {
                std::cout << "LABEL: " << jjj->label << "\nSURVIVE: ";
                for (unsigned int survive_querie : jjj->survive_queries) {
                    std::cout << survive_querie << "(" << G->global_hash[i][survive_querie] << ")" << " ";
                }
                std::cout << "\nVERTEX: ";
                for (unsigned int k : jjj->vertex_id) {
                    std::cout << k << " ";
                }
                std::cout << "\n";
                for (const auto& pp : jjj->topology) {
                    for (const auto& qq : pp) {
                        std::cout << "(" << std::get<0>(qq) << ", " << std::get<1>(qq) << ", " << std::get<2>(qq) << ")" << " ";
                    }
                    std::cout << "-----------";
                }
                std::cout << "\n";
                std::cout << "Subgraph Relation Graph: \n";
                for (const auto& qq:  jjj->TopologyDAG->RootOfNode) {
                    std::cout << "SRG Node: " << qq.first << " its root: " << qq.second << "\n";
                }
            }
        }
    }

    // modify the ac and av arguments to avoid errors
    ac = 1;
    const char* program_name = "./aquila";
    av[0] = const_cast<char*>(program_name);

    // start hiactor::app to run IGQ in mini-batch
    app.run(ac, av, [core_number, batch_size, iteration_times, is_collect_number] {

        std::cout<<"app start"<<std::endl;
        Graph* G = Graph::GetInstance();
        std::cout << G -> dataGraph.updates.size() << "\n";
        uint current_interation = 1;  
        ll vertex_number = G->GetMaxTreeSize();
        uint max_core_number = hiactor::global_shard_count();

        seastar::repeat([G, current_interation, vertex_number, max_core_number, core_number, batch_size, iteration_times, is_collect_number]() mutable {

            // Meets an end.
            if (current_interation == iteration_times + 1 || 
                (current_interation - 1) * batch_size >= G->dataGraph.updates.size()) {
                std::cout << "Termination condition meets\n";
                return G->cv.wait().then([G, &current_interation, vertex_number]() mutable {
                    return seastar::make_ready_future<seastar::stop_iteration>(
                        seastar::stop_iteration::yes
                    );
                });  
            }

            if (current_interation == 1) {
                // the first interation which does not need coroutine synchronization.
                uint end_index;
                startTiming();
                for (uint tot = (current_interation - 1) * batch_size; tot < static_cast<uint>(current_interation * batch_size) && tot < G -> dataGraph.updates.size(); tot++) {
                    update_unit unit = G -> dataGraph.updates[tot];
                    if (unit.type == 'v' && unit.is_insert) {
                       G->dataGraph.AddVertex(unit.id1, unit.label);
                   } else if (unit.type == 'v' && !unit.is_insert) {
                       G->dataGraph.RemoveVertex(unit.id1);
                   } else if (unit.type == 'e' && unit.is_insert) {
                       G->dataGraph.AddEdge(unit.id1, unit.id2, unit.label, tot + 1);
                   } else if  (unit.type == 'e' && !unit.is_insert) {
                       G->dataGraph.RemoveEdge(unit.id1, unit.id2, unit.label);
                   }
                   if (tot == G -> dataGraph.updates.size() - 1 || tot == static_cast<uint>(current_interation * batch_size - 1)) end_index = tot;
                }

                G->elapsed_time.push_back(stopTimingAndGive());
                G->AdjustMatchingTrees();
                std::cout << current_interation << " "  << vertex_number <<  " " << (current_interation - 1) * batch_size << " " << end_index << "\n";

                RunQueryInBatch(current_interation, vertex_number, (current_interation - 1) * batch_size, end_index, core_number, batch_size, is_collect_number);
                current_interation++;
                return seastar::make_ready_future<seastar::stop_iteration>(
                    seastar::stop_iteration::no
                );
            } else {
                // Needs coroutine synchronization.
                return G->cv.wait().then([G, &current_interation, vertex_number, max_core_number, core_number, batch_size, iteration_times, is_collect_number]() mutable {

                    for (uint core = 0; core < max_core_number; core++) {
                        G->F[core] = false;
                    }

                    std::cout << "Processing batch ID: " << current_interation << std::endl;
                    uint end_index;
                    startTiming();
                    for (uint tot = (current_interation - 1) * batch_size; tot < static_cast<uint>(current_interation * batch_size) && tot < G -> dataGraph.updates.size(); tot++) {
                        update_unit unit = G -> dataGraph.updates[tot];
                        if (unit.type == 'v' && unit.is_insert) {
                            G->dataGraph.AddVertex(unit.id1, unit.label);
                        } else if (unit.type == 'v' && !unit.is_insert) {
                            G->dataGraph.RemoveVertex(unit.id1);
                        } else if (unit.type == 'e' && unit.is_insert) {
                            G->dataGraph.AddEdge(unit.id1, unit.id2, unit.label, tot + 1);
                        } else if  (unit.type == 'e' && !unit.is_insert) {
                            G->dataGraph.RemoveEdge(unit.id1, unit.id2, unit.label);
                        }
                        if (tot == G -> dataGraph.updates.size() - 1 || tot == static_cast<uint>(current_interation * batch_size - 1)) end_index = tot;
                    }

                    G->elapsed_time.push_back(stopTimingAndGive());
                    G->AdjustMatchingTrees();
                    std::cout << current_interation << " "  << vertex_number <<  " " << (current_interation - 1) * batch_size << " " << end_index << "\n";

                    RunQueryInBatch(current_interation, vertex_number, (current_interation - 1) * batch_size, end_index, core_number, batch_size, is_collect_number);
                    current_interation++;
                    return seastar::make_ready_future<seastar::stop_iteration>(
                        seastar::stop_iteration::no
                    );
                });
            } 
       }).then([iteration_times, is_collect_number] {
            std::cout << "Finished repeating" << std::endl;
            Graph* G = Graph::GetInstance();

            if (is_collect_number) {
                for (uint i = 0; i < G->queryGraphs.size(); i++) {
                    std::cout << "Result Number of Query " << i << " : " << G->result[i] << "\n";
                }
            }
            std::cout << "-------------------------------------------------------------------------------------------\n";
            std::cout << "Overall Execution Time: \n";
            double total_time = 0;
            for (uint i = 0; i < iteration_times * 2; i += 2) {
                std::cout << "Batch " << (i / 2 + 1) <<": Graph Update: " << G->elapsed_time[i] << " Graph Query: " << G->elapsed_time[i + 1] << "\n";
                total_time += G->elapsed_time[i];
                total_time += G->elapsed_time[i + 1];
            }
            std::cout << "Elapsed Time: " << total_time << "\n";
            std::cout << "Average Elapsed Time for Each Batch: " << double(total_time / iteration_times) << "\n";

            hiactor::actor_engine().exit();
        });
    });
}
