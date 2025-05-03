# Copyright 2021 Alibaba Group Holding Limited. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

hiactor_add_demo (Aquila
  SOURCES
    query.cc
    actor/map.act.cc
    actor/map.act.h  
    actor/shuffle.act.cc
    actor/shuffle.act.h  
    actor/barrier.act.cc
    actor/barrier.act.h  
    actor/file_sink.act.h 
    actor/file_sink.act.cc
    actor/file_source.act.h 
    actor/file_source.act.cc
    actor/end_data_sink.act.cc
    actor/end_data_sink.act.h
    actor/end_file_sink.act.cc
    actor/end_file_sink.act.h
    actor/data_sink.act.h 
    actor/data_sink.act.cc
    actor/data_source.act.h 
    actor/data_source.act.cc
    actor/flat_map.act.cc
    actor/flat_map.act.h
    actor/map_partition.act.cc
    actor/map_partition.act.h
    DataFlow/DataFlow.h
    DataFlow/DataFlow.cc
    Executor/Executor.h
    Executor/Executor.cc
    Executor/Timing.cc
    Executor/Timing.h
    Executor/GenericJoin.cc
    Executor/GenericJoin.h
    Executor/DeltaGenericJoin.cc
    Executor/DeltaGenericJoin.h
    Executor/EdgeScan.cc
    Executor/EdgeScan.h
    Executor/Initialization.cc
    Executor/Initialization.h
    Executor/file_sink_exe.h
    Executor/file_sink_exe.cc
    Graph_source/Graph_source.h
    Graph_source/Graph_source.cc
    Graph_source/Graph_source_data.h
    )
