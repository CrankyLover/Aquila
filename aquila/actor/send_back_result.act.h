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


// EndDataSink后面默认连接一个SendBack，因为EndDS的receive_eos需要接收4个
class SendBackResultActor : public hiactor::actor {
public:
    //最后一个actor，不会有next，不需要_ref{}
    SendBackResultActor(hiactor::actor_base* exec_ctx, const hiactor::byte_t* addr) : hiactor::actor(exec_ctx, addr, false) {
        // _refs.reserve(hiactor::global_shard_count());
    }
    ~SendBackResultActor() override = default;
    
    void process(hiactor::DataType&& input);
    void receive_eos(hiactor::Eos&& eos);
    seastar::future<hiactor::DataType> send_back();

    seastar::future<hiactor::stop_reaction> do_work(hiactor::actor_message* msg);

private:
    // std::vector<hiactor::Actor_Ref*> _refs{};
    // seastar::promise<hiactor::DataType> _send_back;
    // std::vector<hiactor::InternalValue> _word_count_vec;
    seastar::promise<hiactor::DataType> _send_back;
    unsigned _eos_rcv_num = 0;
    const unsigned _expect_eos_num = hiactor::global_shard_count();
};


class SendBackResultActor_Ref : public ::hiactor::reference_base {
public:
    SendBackResultActor_Ref();
    ~SendBackResultActor_Ref() override = default;
    /// actor methods
    void process(hiactor::DataType &&input);
    void receive_eos(hiactor::Eos &&eos);
    seastar::future<hiactor::DataType> send_back();
};

