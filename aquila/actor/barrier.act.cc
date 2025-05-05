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

#include "actor/barrier.act.h"
#include "Executor/Timing.h"
#include <hiactor/core/actor_client.hh>
#include <hiactor/core/actor_factory.hh>
#include <hiactor/util/data_type.hh>
#include <seastar/core/print.hh>
#include <iostream>
#include <fstream>
#include <iomanip>

enum : uint8_t {
	k_barrier_process = 0,
	k_barrier_receive_eos = 1,
    k_barrier_setNext = 2,
    k_barrier_useBuffer = 3,
};

BarrierActor_Ref::BarrierActor_Ref(): ::hiactor::reference_base() { actor_type = hiactor::TYPE_BARRIER; }

void BarrierActor_Ref::process(hiactor::DataType &&input) {
	addr.set_method_actor_tid(hiactor::TYPE_BARRIER);
	hiactor::actor_client::send(addr, k_barrier_process, std::forward<hiactor::DataType>(input));
}

void BarrierActor_Ref::receive_eos(hiactor::Eos &&eos) {
	addr.set_method_actor_tid(hiactor::TYPE_BARRIER);
	hiactor::actor_client::send(addr, k_barrier_receive_eos, std::forward<hiactor::Eos>(eos));
}

void BarrierActor_Ref::setNext(hiactor::actor_config_Batch&& next) {
	addr.set_method_actor_tid(hiactor::TYPE_BARRIER);
	hiactor::actor_client::send(addr, k_barrier_setNext, std::forward<hiactor::actor_config_Batch>(next));
}

void BarrierActor_Ref::useBuffer(hiactor::Eos &&use_buf) {
	addr.set_method_actor_tid(hiactor::TYPE_BARRIER);
	hiactor::actor_client::send(addr, k_barrier_useBuffer, std::forward<hiactor::Eos>(use_buf));
}

// seastar::future<hiactor::DataType> BarrierActor_Ref::send_back() {
// 	addr.set_method_actor_tid(hiactor::TYPE_BARRIER);
// 	return hiactor::actor_client::request<hiactor::DataType>(addr, k_barrier_send_back);
// }




void BarrierActor::setNext(hiactor::actor_config_Batch&& next) {
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

//后续修改，将输入数据解包成InternalValue后存入vector，最后包装为DataType
//输入tuple，输出vector<tuple>
//这里只负责收集所有map的数据并打包，不能进行join
void BarrierActor::process(hiactor::DataType&& input) {
    // std::cout << "BarrierActor::process" << '\n';
    // std::cout << "next actor type is: " << _next_actor_type << '\n';
    // std::cout << "eos received number is: " << _eos_rcv_num << '\n';
    std::vector<hiactor::InternalValue> _input_data_buf = *input._data.vectorValue;// data_buffer
    Graph* G = Graph::GetInstance();
    // G->time_mutex.lock();
    // G->result[hiactor::global_shard_id()] = stopTimingAndGive();
    // G->time_mutex.unlock();
    for (unsigned j = 0; j < _input_data_buf.size(); j++) {
        
        hiactor::InternalValue _tuple = _input_data_buf[j];
//        std::vector<hiactor::InternalValue> vec = *((*_tuple.vectorValue)[0].vectorValue);
//        std::cout << "Barrier result is: ";
//        for (const auto& i : vec) {
//            std::cout << i.intValue << " ";
//        }
//        std::cout << "\n";
        _word_count_vec.push_back(_tuple);
    }
}

void BarrierActor::receive_eos(hiactor::Eos&& eos) {
    // std::cout << "BarrierActor::receive_eos" << '\n';
    if (++_eos_rcv_num % _expect_eos_num == 0) {
        // std::cout << "BarrierActor::receive_eos" << '\n';
        hiactor::DataType _output;
        _output.type = hiactor::DataType::VECTOR;
        _output._data.vectorValue = new std::vector<hiactor::InternalValue>(_word_count_vec);

        if (_have_next) {
            //如果不使用data_buf，也在数据外面套一层vector，便于所有actor统一操作
            std::vector<hiactor::InternalValue> _buf;
            _buf.push_back(_output._data);
            _output._data.vectorValue = new std::vector<hiactor::InternalValue>(_buf);

            _word_count_vec.clear();    // just to output correct result

            (_refs[hiactor::global_shard_id()]->process)(std::move(_output));
            if (!_have_send_data) { _have_send_data = true; }
            
            for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
                _refs[i]->receive_eos(hiactor::Eos{_have_send_data});
            }
        } else {
            output();
            // hiactor::actor_engine().exit();
        }
    }
}

void BarrierActor::useBuffer(hiactor::Eos&& use_buf) {
    // std::cout << "ShuffleActor::eos" << '\n';
    _use_buffer = use_buf.val;
}

void BarrierActor::emplace_data(unsigned dst_shard, hiactor::DataType&& data) {
    std::vector<hiactor::InternalValue> _vec = *_output_data_buf[dst_shard]._data.vectorValue;
    _vec.push_back(data._data);
    _output_data_buf[dst_shard].type = data.type;
    _output_data_buf[dst_shard]._data.vectorValue = new std::vector<hiactor::InternalValue>(_vec);
    if(_vec.size() == _batch_sz) {
        (_refs[dst_shard]->process)(std::move(_output_data_buf[dst_shard]));
        _vec.clear();
    }
}


void BarrierActor::flush() {
    for(unsigned i = 0; i < hiactor::global_shard_count(); i++) {
        if((*_output_data_buf[i]._data.vectorValue).size() > 0) {
            hiactor::DataType data;
            data._data.vectorValue = new std::vector<hiactor::InternalValue>(*_output_data_buf[i]._data.vectorValue);
            (_refs[i]->process)(std::move(_output_data_buf[i]));
            (*_output_data_buf[i]._data.vectorValue).clear();
        }
    }
}

// seastar::future<hiactor::DataType> BarrierActor::send_back() {
// 	// std::cout << "EndDataSink::send_back" << '\n';
//     return _refs[0]->send_back();
// }

void BarrierActor::output() {
    std::cout << "This shard is: " << hiactor::global_shard_id() << '\n';
    // for (unsigned i = 0; i < _output.size(); i++) {
    //     for (unsigned j = 0; j < _output[i].size(); j++) {
    //         std::cout << "Word: " << std::setw(6) << _output[i].words[j] << ", Count: " << _output[i].counts[j] << '\n';
    //     }
    // }
    
    if (++_output_count == hiactor::global_shard_count()) {
        std::cout << "Barrier Successful." << std::endl;
        hiactor::actor_engine().exit();
    }
    // std::cout << "default global shards number is:" << hiactor::global_shard_count() << std::endl;
}

seastar::future<hiactor::stop_reaction> BarrierActor::do_work(hiactor::actor_message* msg) {
	// std::cout << "BarrierActor::do_work" << '\n';
    switch (msg->hdr.behavior_tid) {
		case k_barrier_process: {
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
		case k_barrier_receive_eos: {
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
        case k_barrier_useBuffer: {
            // if (!msg->hdr.from_network) {
                useBuffer(std::move(reinterpret_cast<
                    hiactor::actor_message_with_payload<hiactor::Eos>*>(msg)->data));
            // } else {
            //     auto* ori_msg = reinterpret_cast<hiactor::actor_message_with_payload<
            //         hiactor::serializable_queue>*>(msg);
            //     receive_eos(hiactor::Eos::load_from(ori_msg->data));
            // }
			return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
		}
        case k_barrier_setNext: {
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
        // case k_barrier_send_back: {
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

hiactor::registration::actor_registration<BarrierActor> _barrier_auto_registration(hiactor::TYPE_BARRIER);

} // namespace auto_registration