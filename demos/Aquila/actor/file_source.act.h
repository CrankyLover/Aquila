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

#include "actor/shuffle.act.h"
#include "actor/barrier.act.h"
#include "actor/map.act.h"
#include "actor/flat_map.act.h"
#include "actor/map_partition.act.h"

class FileSource;

class FileSourceActor : public hiactor::actor {
public:
    FileSourceActor(hiactor::actor_base* exec_ctx, const hiactor::byte_t* addr) : hiactor::actor(exec_ctx, addr, false) {
        _refs.reserve(hiactor::global_shard_count());
    }
    ~FileSourceActor() override = default;
    
//boost::any or internal block
    void setNext(hiactor::actor_config_Batch&& next);
    void process(hiactor::DataType&& input);
    void receive_eos(hiactor::Eos&& eos);
    void output();
    // seastar::future<hiactor::DataType> send_back();

    seastar::future<hiactor::stop_reaction> do_work(hiactor::actor_message* msg);

private:
    std::vector<hiactor::Actor_Ref*> _refs{};
    std::string _next_actor_type;
    hiactor::DataType _output;
    bool _have_send_data = false;
    bool _have_next = false;
    unsigned _batch_sz = batch_size;
    bool _use_buffer = true;
};


class FileSourceActor_Ref : public ::hiactor::reference_base, public hiactor::Actor_Ref {
public:
    FileSourceActor_Ref();
    ~FileSourceActor_Ref() override = default;
    /// actor methods
    void setNext(hiactor::actor_config_Batch&& next);
    void process(hiactor::DataType &&input);
    void receive_eos(hiactor::Eos &&eos);
    // seastar::future<hiactor::DataType> send_back();
};


class FileSource : public hiactor::Source {
public:
    FileSource(hiactor::scope_builder& builder, unsigned ds_op_id, const unsigned batch_sz = batch_size)
            : Source(builder, ds_op_id, batch_sz, false) {
        _refs.reserve(hiactor::global_shard_count());
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            builder.set_shard(i);
            _refs.emplace_back(builder.new_ref<FileSourceActor_Ref>(ds_op_id));
        }
    }
    ~FileSource() {
        for (auto ref : _refs) { delete ref; }
    }

    //DataType改成filename
    void process(unsigned dst_shard, hiactor::DataType &&data) override {
        (_refs[dst_shard]->process)(std::move(data));
        if (!_have_send_data) { _have_send_data = true; }
    }


    void receive_eos() override  {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos(hiactor::Eos{_have_send_data});
        }
    }

    // seastar::future<hiactor::DataType> send_back() override {
    //     return _refs[0]->send_back();
    // }

    void setNext(hiactor::OperatorBase&& next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            hiactor::actor_config_Batch next_config{1};
            next_config.emplace_back(next.getBuilder(), next.getActorId(), next.getActorType());
            _refs[i]->setNext(std::move(next_config));
        }
    }

    void setFunc(hiactor::MapFunc&& func) {}
    void setFunc(hiactor::HashFunc&& func) {}

    std::string getActorType() override {
        return "file_source";
    }

    hiactor::scope_builder getBuilder() override {
        return _builder;
    }

    unsigned getActorId() override {
        return _ds_op_id;
    }

private:
    std::vector<FileSourceActor_Ref*> _refs{};

};


