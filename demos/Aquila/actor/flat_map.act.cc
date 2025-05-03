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

#include "actor/flat_map.act.h"
#include "Executor/Timing.h"
#include <hiactor/core/actor_client.hh>
#include <hiactor/core/actor_factory.hh>
#include <hiactor/util/data_type.hh>
#include <seastar/core/print.hh>
#include <iostream>
#include <fstream>
#include <iomanip>

enum : uint8_t {
	k_flat_map_process = 0,
	k_flat_map_receive_eos = 1,
	k_flat_map_setFunc = 2,
    k_flat_map_setNext = 3,
    k_flat_map_useBuffer = 4,
};

FlatMapActor_Ref::FlatMapActor_Ref(): ::hiactor::reference_base() { actor_type = hiactor::TYPE_FLATMAP; }

void FlatMapActor_Ref::process(hiactor::DataType &&input) {
	addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
	hiactor::actor_client::send(addr, k_flat_map_process, std::forward<hiactor::DataType>(input));
}

void FlatMapActor_Ref::receive_eos(hiactor::Eos &&eos) {
	addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
	hiactor::actor_client::send(addr, k_flat_map_receive_eos, std::forward<hiactor::Eos>(eos));
}

void FlatMapActor_Ref::setFunc(hiactor::MapFunc &&func) {
	addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
	hiactor::actor_client::send(addr, k_flat_map_setFunc, std::forward<hiactor::MapFunc>(func));
}

void FlatMapActor_Ref::setNext(hiactor::actor_config_Batch&& next) {
	addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
	hiactor::actor_client::send(addr, k_flat_map_setNext, std::forward<hiactor::actor_config_Batch>(next));
}

void FlatMapActor_Ref::useBuffer(hiactor::Eos &&use_buf) {
	addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
	hiactor::actor_client::send(addr, k_flat_map_useBuffer, std::forward<hiactor::Eos>(use_buf));
}

// seastar::future<hiactor::DataType> FlatMapActor_Ref::send_back() {
// 	addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
// 	return hiactor::actor_client::request<hiactor::DataType>(addr, k_flat_map_send_back);
// }




void FlatMapActor::setNext(hiactor::actor_config_Batch&& next) {
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
    } else if (_next_actor_type == "file_sink") {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            builder.set_shard(i);
            _refs.emplace_back(builder.new_ref<FileSinkActor_Ref>(ds_op_id));
        }
    } else if (_next_actor_type == "data_sink") {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            builder.set_shard(i);
            _refs.emplace_back(builder.new_ref<DataSinkActor_Ref>(ds_op_id));
        }
    } else {
        std::cout << "Invalid actor type" << '\n';
        hiactor::actor_engine().exit();
    }
}

void FlatMapActor::setFunc(hiactor::MapFunc&& func) {
    // std::cout << "setFunc" << '\n';
    _func = func;
}

//可能接收两种类型，具体内容用户自己编�?
void FlatMapActor::process(hiactor::DataType&& input) {
    // std::cout << "FlatMapActor::process" << '\n';
    // std::cout << "next actor type is: " << _next_actor_type << '\n';
    std::vector<hiactor::InternalValue> _buf;
    // std::cout << "flatmap process\n";

    std::vector<hiactor::InternalValue> _input_data_buf = *input._data.vectorValue;// data_buffer
    // VectorPool::release(input._data.vectorValue);
    // std::cout << "flatmap process2 " << _input_data_buf.size() << "\n";
    for (unsigned j = 0; j < _input_data_buf.size(); j++) {
        hiactor::DataType _intermediate_result;
        // std::cout << "flatmap process3\n";
        _intermediate_result = _func.process(hiactor::DataType(_input_data_buf[j]));
        // VectorPool::release((*_input_data_buf[j].vectorValue)[4].vectorValue);
        // VectorPool::release(_input_data_buf[j].vectorValue);
        // if(_func.get_is_visited_expand() == true) {
        //     _intermediate_result = _func.process(hiactor::DataType(_input_data_buf[j]), visit);
        // } else {
        //     _intermediate_result = _func.process(hiactor::DataType(_input_data_buf[j]));
        // }
//        std::cout << (*_intermediate_result._data.vectorValue).size() << std::endl;
        for(unsigned i = 0; i < (*_intermediate_result._data.vectorValue).size(); i++) {
            _buf.push_back((*_intermediate_result._data.vectorValue)[i]);
        }
        // VectorPool::release(_intermediate_result._data.vectorValue);
        _output.type = _intermediate_result.type;
    }
    if (_have_next) {
        // std::cout << "pp!\n";
        _output._data.vectorValue = new std::vector<hiactor::InternalValue>(_buf);
        // auto* vec = VectorPool::acquire();
        // *vec = _buf;
        // _output._data.vectorValue = vec;
        // std::cout << "wowo\n";
        (_refs[hiactor::global_shard_id()]->process)(std::move(_output));
    }
}

void FlatMapActor::receive_eos(hiactor::Eos&& eos) {
    if (++_eos_rcv_num % _expect_eos_num == 0 && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos(hiactor::Eos{_have_send_data});
        }
    }
}

void FlatMapActor::emplace_data(unsigned dst_shard, hiactor::DataType&& data) {
    std::vector<hiactor::InternalValue> _vec = *_output_data_buf[dst_shard]._data.vectorValue;
    _vec.push_back(data._data);
    _output_data_buf[dst_shard].type = data.type;
    _output_data_buf[dst_shard]._data.vectorValue = new std::vector<hiactor::InternalValue>(_vec);
    if(_vec.size() == _batch_sz) {
        (_refs[dst_shard]->process)(std::move(_output_data_buf[dst_shard]));
        _vec.clear();
    }
}


void FlatMapActor::flush() {
    for(unsigned i = 0; i < hiactor::global_shard_count(); i++) {
        if((*_output_data_buf[i]._data.vectorValue).size() > 0) {
            hiactor::DataType data;
            data._data.vectorValue = new std::vector<hiactor::InternalValue>(*_output_data_buf[i]._data.vectorValue);
            (_refs[i]->process)(std::move(_output_data_buf[i]));
            (*_output_data_buf[i]._data.vectorValue).clear();
        }
    }
}

// seastar::future<hiactor::DataType> FlatMapActor::send_back() {
// 	// std::cout << "EndDataSink::send_back" << '\n';
//     return _refs[0]->send_back();
// }

void FlatMapActor::output() {
    // std::cout << "This shard is: " << hiactor::global_shard_id() << '\n';
    // for (unsigned i = 0; i < _output.size(); i++) {
    //     std::cout << "Word: " << std::setw(6) << _output.words[i] << ", Count: " << _output.counts[i] << '\n';
    // }
    
    // std::cout << "Map Successful." << std::endl;
    // std::cout << "default global shards number is:" << hiactor::global_shard_count() << std::endl;
}

seastar::future<hiactor::stop_reaction> FlatMapActor::do_work(hiactor::actor_message* msg) {
	// std::cout << "FlatMapActor::do_work" << '\n';
    switch (msg->hdr.behavior_tid) {
		case k_flat_map_process: {
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
		case k_flat_map_receive_eos: {
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
		case k_flat_map_setFunc: {
            // if (!msg->hdr.from_network) {
                setFunc(std::move(reinterpret_cast<
                    hiactor::actor_message_with_payload<hiactor::MapFunc>*>(msg)->data));
            // } else {
            //     auto* ori_msg = reinterpret_cast<hiactor::actor_message_with_payload<
            //         hiactor::serializable_queue>*>(msg);
            //     setFunc(FuncT::load_from(ori_msg->data));
            // }
			return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
		}
        case k_flat_map_setNext: {
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
        // case k_flat_map_send_back: {
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

hiactor::registration::actor_registration<FlatMapActor> _flat_map_auto_registration(hiactor::TYPE_FLATMAP);

} // namespace auto_registration