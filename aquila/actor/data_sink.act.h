#pragma once

#include <hiactor/core/reference_base.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>
#include <functional>

#include <hiactor/core/actor_client.hh>
#include <seastar/core/print.hh>
#include <iostream>
#include <fstream>
#include <iomanip>

#include "actor/end_data_sink.act.h"


// // DataSink后面默认连接一个EndDS，将所有shard上的数据汇总到同一个shard，然后返回到最外层
// class EndDataSinkActor : public hiactor::actor {
// public:
//     //最后一个actor，不会有next，不需要_ref{}
//     EndDataSinkActor(hiactor::actor_base* exec_ctx, const hiactor::byte_t* addr) : hiactor::actor(exec_ctx, addr, false) {
//         // _refs.reserve(hiactor::global_shard_count());
//     }
//     ~EndDataSinkActor() override = default;
    
//     void process(hiactor::DataType&& input);
//     void receive_eos(hiactor::Eos&& eos);
//     // seastar::future<hiactor::DataType> send_back();

//     seastar::future<hiactor::stop_reaction> do_work(hiactor::actor_message* msg);

// private:
//     // std::vector<hiactor::Actor_Ref*> _refs{};
//     // seastar::promise<hiactor::DataType> _send_back;
//     std::vector<hiactor::InternalValue> _word_count_vec;
//     unsigned _eos_rcv_num = 0;
//     const unsigned _expect_eos_num = hiactor::global_shard_count();
// };


// class EndDataSinkActor_Ref : public ::hiactor::reference_base, public hiactor::Actor_Ref {
// public:
//     EndDataSinkActor_Ref();
//     ~EndDataSinkActor_Ref() override = default;
//     /// actor methods
//     void process(hiactor::DataType &&input);
//     void receive_eos(hiactor::Eos &&eos);
//     // seastar::future<hiactor::DataType> send_back();
// };




// 默认连接一个EndDS，统计所有shard数据，不需要_refs{}数组，所有shard连接一个相同的actor
class DataSinkActor : public hiactor::actor {
public:
    // 没有setNext函数，下一个actor_ref在构造函数中初始化
    DataSinkActor(hiactor::actor_base* exec_ctx, const hiactor::byte_t* addr) : hiactor::actor(exec_ctx, addr, false) {
        // _refs.reserve(hiactor::global_shard_count());
        auto builder = get_scope();
        builder.set_shard(0);
        _ref = builder.new_ref<EndDataSinkActor_Ref>(0);
    }
    ~DataSinkActor() override = default;
    
//boost::any or internal block
    void process(hiactor::DataType&& input);
    void receive_eos(hiactor::Eos&& eos);
    // seastar::future<hiactor::DataType> send_back();

    seastar::future<hiactor::stop_reaction> do_work(hiactor::actor_message* msg);

private:
    // std::vector<hiactor::Actor_Ref*> _refs{};
    EndDataSinkActor_Ref* _ref;
    hiactor::DataType _output;
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = hiactor::global_shard_count();
    bool _have_send_data;
};


class DataSinkActor_Ref : public ::hiactor::reference_base, public hiactor::Actor_Ref {
public:
    DataSinkActor_Ref();
    ~DataSinkActor_Ref() override = default;
    /// actor methods
    void process(hiactor::DataType &&input);
    void receive_eos(hiactor::Eos &&eos);
    // seastar::future<hiactor::DataType> send_back();
};


class DataSink : public hiactor::Sink {
public:
    DataSink(hiactor::scope_builder& builder, unsigned ds_op_id, const unsigned batch_sz = batch_size)
            : Sink(builder, ds_op_id, batch_sz, false) {
        _refs.reserve(hiactor::global_shard_count());
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            builder.set_shard(i);
            _refs.emplace_back(builder.new_ref<DataSinkActor_Ref>(ds_op_id));
        }
    }
    ~DataSink() {
        for (auto ref : _refs) { delete ref; }
    }

    void process(unsigned dst_shard, hiactor::DataType&& data) override {
        (_refs[dst_shard]->process)(std::move(data));
        if (!_have_send_data) { _have_send_data = true; }
    }


    void receive_eos() override  {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos(hiactor::Eos{_have_send_data});
        }
    }

    void setNext(hiactor::OperatorBase&& next) {}
    void setFunc(hiactor::MapFunc&& func) {}
    void setFunc(hiactor::HashFunc&& func) {}

    // seastar::future<hiactor::DataType> send_back() {
    //     // 默认在shard0上统计所有数据并返回，后续根据需要修改
    //     return _refs[0]->send_back();
    // }

    std::string getActorType() override {
        return "data_sink";
    }

    hiactor::scope_builder getBuilder() override {
        return _builder;
    }

    unsigned getActorId() override {
        return _ds_op_id;
    }

private:
    std::vector<DataSinkActor_Ref*> _refs{};
};




