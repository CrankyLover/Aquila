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

#include "actor/end_data_sink.act.h"
#include <hiactor/core/actor_client.hh>
#include <hiactor/core/actor_factory.hh>
#include <hiactor/util/data_type.hh>
#include <seastar/core/print.hh>
#include <iostream>
#include <fstream>
#include <iomanip>

enum : uint8_t {
	k_end_data_sink_process = 0,
	k_end_data_sink_receive_eos = 1,
	k_end_data_sink_is_end = 2,
	k_end_data_sink_get_result = 3,
};

EndDataSinkActor_Ref::EndDataSinkActor_Ref(): ::hiactor::reference_base() { actor_type = hiactor::TYPE_ENDDATASINK; }

void EndDataSinkActor_Ref::process(hiactor::DataType &&input) {
	addr.set_method_actor_tid(hiactor::TYPE_ENDDATASINK);
	hiactor::actor_client::send(addr, k_end_data_sink_process, std::forward<hiactor::DataType>(input));
}

void EndDataSinkActor_Ref::receive_eos(hiactor::Eos &&eos) {
	addr.set_method_actor_tid(hiactor::TYPE_ENDDATASINK);
	hiactor::actor_client::send(addr, k_end_data_sink_receive_eos, std::forward<hiactor::Eos>(eos));
}

seastar::future<hiactor::Eos> EndDataSinkActor_Ref::is_end() {
	addr.set_method_actor_tid(hiactor::TYPE_ENDDATASINK);
	return hiactor::actor_client::request<hiactor::Eos>(addr, k_end_data_sink_is_end);
}

seastar::future<hiactor::DataType> EndDataSinkActor_Ref::get_result() {
	addr.set_method_actor_tid(hiactor::TYPE_ENDDATASINK);
	return hiactor::actor_client::request<hiactor::DataType>(addr, k_end_data_sink_get_result);
}





//直接将收集到的数据写入对应shard的文件
void EndDataSinkActor::process(hiactor::DataType&& input) {
    // std::cout << "EndDataSink::process" << '\n';
    // std::cout << "next actor type is: " << _next_actor_type << '\n';

    hiactor::InternalValue _vec_tuple = input._data; // input: vector<tuple>
    _word_count_vec.push_back(_vec_tuple);
    
}

void EndDataSinkActor::receive_eos(hiactor::Eos&& eos) {
    // std::cout << "End Data Sink" << '\n';
    if (++_eos_rcv_num == _expect_eos_num) {
        std::cout << "EndDataSink::receive_eos" << '\n';
        // hiactor::DataType _output; // output: vector<vector<tuple>>
        // _output.type = hiactor::DataType::VECTOR_VECTOR_TUPLE;
        // _output._data.vectorValue = new std::vector<hiactor::InternalValue>(_word_count_vec);

        // _send_back.set_value(_output);
		// _ref->process(std::move(_output));
		_result.type = hiactor::DataType::VECTOR_VECTOR_TUPLE;
        _result._data.vectorValue = new std::vector<hiactor::InternalValue>(_word_count_vec);
		_is_end = true;
		// std::cout << _is_end << '\n';
    }
}

seastar::future<hiactor::Eos> EndDataSinkActor::is_end() {
	// std::cout << "EndDataSink::get_result" << '\n';
    return seastar::make_ready_future<hiactor::Eos>(_is_end);
}

seastar::future<hiactor::DataType> EndDataSinkActor::get_result() {
	std::cout << "EndDataSink::get_result" << '\n';
    return seastar::make_ready_future<hiactor::DataType>(_result);
}

seastar::future<hiactor::stop_reaction> EndDataSinkActor::do_work(hiactor::actor_message* msg) {
	// std::cout << "EndDataSinkActor::do_work" << '\n';
    switch (msg->hdr.behavior_tid) {
		case k_end_data_sink_process: {
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
		case k_end_data_sink_receive_eos: {
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
		case k_end_data_sink_is_end: {
			return is_end().then_wrapped([msg] (seastar::future<hiactor::Eos> fut) {
				if (__builtin_expect(fut.failed(), false)) {
					auto* ex_msg = hiactor::make_response_message(
						msg->hdr.src_shard_id,
						hiactor::simple_string::from_exception(fut.get_exception()),
						msg->hdr.pr_id,
						hiactor::message_type::EXCEPTION_RESPONSE);
					return hiactor::actor_engine().send(ex_msg);
				}
				auto* response_msg = hiactor::make_response_message(
					msg->hdr.src_shard_id, fut.get0(), msg->hdr.pr_id, hiactor::message_type::RESPONSE);
				return hiactor::actor_engine().send(response_msg);
			}).then([] {
				return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
			});
		}
        case k_end_data_sink_get_result: {
			return get_result().then_wrapped([msg] (seastar::future<hiactor::DataType> fut) {
				if (__builtin_expect(fut.failed(), false)) {
					auto* ex_msg = hiactor::make_response_message(
						msg->hdr.src_shard_id,
						hiactor::simple_string::from_exception(fut.get_exception()),
						msg->hdr.pr_id,
						hiactor::message_type::EXCEPTION_RESPONSE);
					return hiactor::actor_engine().send(ex_msg);
				}
				auto* response_msg = hiactor::make_response_message(
					msg->hdr.src_shard_id, fut.get0(), msg->hdr.pr_id, hiactor::message_type::RESPONSE);
				return hiactor::actor_engine().send(response_msg);
			}).then([] {
				return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
			});
		}
		default: {
			return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::yes);
		}
	}
}

namespace auto_registration {

hiactor::registration::actor_registration<EndDataSinkActor> _end_data_sink_auto_registration(hiactor::TYPE_ENDDATASINK);

} // namespace auto_registration