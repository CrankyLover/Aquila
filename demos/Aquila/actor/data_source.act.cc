/** Copyright 2021 Alibaba Group Holding Limited. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "actor/data_source.act.h"
#include <hiactor/core/actor_client.hh>
#include <hiactor/core/actor_factory.hh>
#include <hiactor/util/data_type.hh>
#include <seastar/core/print.hh>
#include <iostream>
#include <fstream>
#include <iomanip>

enum : uint8_t {
	k_data_source_process = 0,
	k_data_source_receive_eos = 1,
    k_data_source_setNext = 2,
    // k_data_source_send_back = 3,
};

DataSourceActor_Ref::DataSourceActor_Ref(): ::hiactor::reference_base() { actor_type = hiactor::TYPE_DATASOURCE; }

void DataSourceActor_Ref::process(hiactor::DataType &&input) {
	addr.set_method_actor_tid(hiactor::TYPE_DATASOURCE);
	hiactor::actor_client::send(addr, k_data_source_process, std::forward<hiactor::DataType>(input));
}

void DataSourceActor_Ref::receive_eos(hiactor::Eos &&eos) {
	addr.set_method_actor_tid(hiactor::TYPE_DATASOURCE);
	hiactor::actor_client::send(addr, k_data_source_receive_eos, std::forward<hiactor::Eos>(eos));
}

void DataSourceActor_Ref::setNext(hiactor::actor_config_Batch&& next) {
	addr.set_method_actor_tid(hiactor::TYPE_DATASOURCE);
	hiactor::actor_client::send(addr, k_data_source_setNext, std::forward<hiactor::actor_config_Batch>(next));
}

// seastar::future<hiactor::DataType> DataSourceActor_Ref::send_back() {
// 	addr.set_method_actor_tid(hiactor::TYPE_DATASOURCE);
// 	return hiactor::actor_client::request<hiactor::DataType>(addr, k_data_source_send_back);
// }




void DataSourceActor::setNext(hiactor::actor_config_Batch&& next) {
    auto builder = next[0]._builder;
    auto ds_op_id = next[0]._ds_op_id;
    _next_actor_type = next[0]._actor_type;
    _have_next = true;

    if (_next_actor_type == "shuffle") {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            builder.set_shard(i);
            _refs.emplace_back(builder.new_ref<ShuffleActor_Ref>(ds_op_id));
        }
    } else if (_next_actor_type == "map") {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            builder.set_shard(i);
            _refs.emplace_back(builder.new_ref<MapActor_Ref>(ds_op_id));
        }
    } else if (_next_actor_type == "map_partition") {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            builder.set_shard(i);
            _refs.emplace_back(builder.new_ref<MapPartitionActor_Ref>(ds_op_id));
        }
    } else if (_next_actor_type == "flat_map") {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            builder.set_shard(i);
            _refs.emplace_back(builder.new_ref<FlatMapActor_Ref>(ds_op_id));
        }
    } else if (_next_actor_type == "barrier") {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            builder.set_shard(i);
            _refs.emplace_back(builder.new_ref<BarrierActor_Ref>(ds_op_id));
        }
    } else {
        std::cout << "Invalid actor type" << '\n';
        hiactor::actor_engine().exit();
    }
}


//直接将input输出，不需要对数据进行处理
void DataSourceActor::process(hiactor::DataType&& input) {
    // std::cout << "DataSourceActor::process" << '\n';
    // std::cout << "next actor type is: " << _next_actor_type << '\n';

    _output = input;
    
    if (_have_next) {
        (_refs[hiactor::global_shard_id()]->process)(std::move(_output));
        if (!_have_send_data) { _have_send_data = true; }
    } else {
        std::cout << "Error! No operators to process data!" << '\n';
        hiactor::actor_engine().exit();
    }
}

void DataSourceActor::receive_eos(hiactor::Eos&& eos) {
    for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
        _refs[i]->receive_eos(hiactor::Eos{_have_send_data});
    }
}

// seastar::future<hiactor::DataType> DataSourceActor::send_back() {
// 	// std::cout << "EndDataSink::send_back" << '\n';
//     return _refs[0]->send_back();
// }

void DataSourceActor::output() {
    // std::cout << "This shard is: " << hiactor::global_shard_id() << '\n';
    // for (unsigned i = 0; i < _output.size(); i++) {
    //     std::cout << "Word: " << std::setw(6) << _output.words[i] << ", Count: " << _output.counts[i] << '\n';
    // }
    
    // std::cout << "DataSource Successful." << std::endl;
    // std::cout << "default global shards number is:" << hiactor::global_shard_count() << std::endl;
}

seastar::future<hiactor::stop_reaction> DataSourceActor::do_work(hiactor::actor_message* msg) {
	// std::cout << "DataSourceActor::do_work" << '\n';
    switch (msg->hdr.behavior_tid) {
		case k_data_source_process: {
            // if (!msg->hdr.from_network) {
                process(std::move(reinterpret_cast<
                    hiactor::actor_message_with_payload<hiactor::DataType>*>(msg)->data));
            // } else {
            //     auto* ori_msg = reinterpret_cast<hiactor::actor_message_with_payload<
            //         hiactor::serializable_queue>*>(msg);
            //     process(InDataT::load_from(ori_msg->data));
            // }
			return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
		}
		case k_data_source_receive_eos: {
            // if (!msg->hdr.from_network) {
                receive_eos(std::move(reinterpret_cast<
                    hiactor::actor_message_with_payload<hiactor::Eos>*>(msg)->data));
            // } else {
            //     auto* ori_msg = reinterpret_cast<hiactor::actor_message_with_payload<
            //         hiactor::serializable_queue>*>(msg);
            //     receive_eos(hiactor::Eos::load_from(ori_msg->data));
            // }
			return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
		}
        case k_data_source_setNext: {
            // if (!msg->hdr.from_network) {
                setNext(std::move(reinterpret_cast<
                    hiactor::actor_message_with_payload<hiactor::actor_config_Batch>*>(msg)->data));
            // } else {
            //     auto* ori_msg = reinterpret_cast<hiactor::actor_message_with_payload<
            //         hiactor::serializable_queue>*>(msg);
            //     setFunc(FuncT::load_from(ori_msg->data));
            // }
			return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
		}
        // case k_data_source_send_back: {
		// 	return send_back().then_wrapped([msg] (seastar::future<hiactor::DataType> fut) {
		// 		if (__builtin_expect(fut.failed(), false)) {
		// 			auto* ex_msg = hiactor::make_response_message(
		// 				msg->hdr.src_shard_id,
		// 				hiactor::simple_string::from_exception(fut.get_exception()),
		// 				msg->hdr.pr_id,
		// 				hiactor::message_type::EXCEPTION_RESPONSE);
		// 			return hiactor::actor_engine().send(ex_msg);
		// 		}
		// 		auto* response_msg = hiactor::make_response_message(
		// 			msg->hdr.src_shard_id, fut.get0(), msg->hdr.pr_id, hiactor::message_type::RESPONSE);
		// 		return hiactor::actor_engine().send(response_msg);
		// 	}).then([] {
		// 		return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
		// 	});
		// }
		default: {
			return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::yes);
		}
	}
}

namespace auto_registration {

hiactor::registration::actor_registration<DataSourceActor> _data_source_auto_registration(hiactor::TYPE_DATASOURCE);

} // namespace auto_registration