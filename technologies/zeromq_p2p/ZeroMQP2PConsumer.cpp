#include "ZeroMQP2PConsumer.hpp"

#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/types.h>
#include <thread>
#include <unordered_set>
#include <utility>
#include <zmq.h>
#include <zmq.hpp>

#include "Payload.hpp"
#include "Utils.hpp"

bool ZeroMQP2PConsumer::deserialize(const void *raw_message, size_t len,
                                    std::string &topic, Payload &out) {
	const char *data = static_cast<const char *>(raw_message);
	size_t offset = 0;

	// Topic length & Topic
	uint8_t topic_len;
	std::memcpy(&topic_len, data + offset, sizeof(uint8_t));
	offset += sizeof(uint8_t);
	topic.assign(data + offset, topic_len);
	offset += topic_len;

	if (Payload::deserialize(data + offset, len - offset, out) == false) {
		logger->log_error("[ZeroMQP2P Consumer] Deserialization failed.");
		return false;
	}

	return true;
}

bool ZeroMQP2PConsumer::deserialize_id(const void *raw_message, size_t len,
                                       std::string &topic, Payload &out) {
	const char *data = static_cast<const char *>(raw_message);
	size_t offset = 0;

	// Topic length & Topic
	uint8_t topic_len;
	std::memcpy(&topic_len, data + offset, sizeof(uint8_t));
	offset += sizeof(uint8_t);
	topic.assign(data + offset, topic_len);
	offset += topic_len;

	if (Payload::deserialize_id(data + offset, len - offset, out) == false) {
		logger->log_error("[ZeroMQP2P Consumer] Deserialization failed.");
		return false;
	}

	return true;
}

ZeroMQP2PConsumer::ZeroMQP2PConsumer(std::shared_ptr<Logger> logger) try
    : IConsumer(logger), context(1), subscriber(context, ZMQ_SUB) {
	subscriber.set(zmq::sockopt::rcvtimeo, 10000);
	logger->log_debug("[ZeroMQP2P Consumer] Constructor finished");
} catch (const zmq::error_t &e) {
	std::string err_msg =
	    "[ZeroMQP2P Consumer] Constructor failed: " + std::string(e.what());
	logger->log_error(err_msg);
	throw std::runtime_error(err_msg);
}

ZeroMQP2PConsumer::~ZeroMQP2PConsumer() {
	stop_receiving_ = true;
	stop_deserialize_thread_();

	subscriber.close();
	context.close();
}

void ZeroMQP2PConsumer::initialize() {
	// assume **all** default ports 5555 if not specified
	const std::string port =
	    utils::get_env_var_or_default("CONSUMER_PORT", "5555");

	const std::optional<std::string> vendpoints =
	    utils::get_env_var("CONSUMER_ENDPOINT");
	const std::optional<std::string> consumer_id =
	    utils::get_env_var("CONTAINER_ID");
	const std::optional<std::string> vtopics = utils::get_env_var("TOPICS");
	std::string err_msg;
	if (!vendpoints || vendpoints.value().empty()) {
		err_msg = "[ZeroMQP2P Consumer] Missing required environment variable "
		          "CONSUMER_ENDPOINT.";
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	}
	if (!consumer_id || consumer_id.value().empty()) {
		err_msg = "[ZeroMQP2P Consumer] Missing required environment variable "
		          "CONTAINER_ID.";
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	}
	if (!vtopics || vtopics.value().empty()) {
		err_msg = "[ZeroMQP2P Consumer] Missing required environment variable "
		          "TOPICS.";
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	}

	std::string topic;
	std::string publisher;
	std::istringstream topics(vtopics.value_or(""));
	std::istringstream publishers(vendpoints.value_or(""));
	std::unordered_set<std::string> unique_pub_topics;
	while (std::getline(topics, topic, ',')) {
		std::getline(publishers, publisher, ',');
		if (publisher.empty()) {
			logger->log_info(
			    "[ZeroMQP2P Consumer] Empty publisher found for topic " + topic
			    + ", skipping subscription.");
			continue;
		}
		if (topic.empty()) {
			logger->log_info(
			    "[ZeroMQP2P Consumer] Empty topic found for publisher "
			    + publisher + ", skipping subscription.");
			continue;
		}

		logger->log_debug("[ZeroMQP2P Consumer] Handling subscription to topic "
		                  + topic + " from publisher " + publisher);

		if (unique_pub_topics.insert(publisher + ":" + topic).second) {
			logger->log_info("[ZeroMQP2P Consumer] Subscribing to topic: "
			                 + topic + " from publisher: " + publisher);
			subscribe(topic);
		}
	}

	logger->log_debug("[ZeroMQP2P Consumer] Subscription list will have size "
	                  + std::to_string(subscribed_streams.get()));
	try {
		for (const auto &pub_topic : unique_pub_topics) {
			auto delimiter_pos = pub_topic.find(':');
			std::string publisher = pub_topic.substr(0, delimiter_pos);
			subscriber.connect("tcp://" + publisher + ":" + port);
			logger->log_info("[ZeroMQP2P Consumer] Connected to publisher: "
			                 + publisher + " on port: " + port);
		}
	} catch (const zmq::error_t &e) {
		err_msg = "[ZeroMQP2P Consumer] Initialization failed: "
		    + std::string(e.what());
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	}

	logger->log_info(
	    "[ZeroMQP2P Consumer] Consumer initialized and connected.");
	log_configuration();
}

void ZeroMQP2PConsumer::subscribe(const std::string &topic) {
	logger->log_debug("[ZeroMQP2P Consumer] Subscribing to topic: " + topic);
	subscriber.set(zmq::sockopt::subscribe,
	               std::string(1, static_cast<char>(topic.size())) + topic);
	// subscribed_streams counter + 1
	subscribed_streams.inc();
}

void ZeroMQP2PConsumer::start_loop() {
	start_deserialize_thread_();

	while (!stop_receiving_) {
		try {
			zmq::message_t msg;
			auto result = subscriber.recv(msg, zmq::recv_flags::none);
			if (!result) {
				logger->log_info(
				    "[ZeroMQP2P Consumer] Receive timed out, retrying...");
				continue;
			}

			const void *data_ptr = msg.data();
			const size_t data_size = msg.size();

			Payload payload;
			std::string topic;
			if (!deserialize_id(data_ptr, data_size, topic, payload)) {
				logger->log_error(
				    "[ZeroMQP2P Consumer] Deserialization failed");
				continue;
			}

			logger->log_study("Reception," + payload.message_id + "," + topic);

			deserialize_queue_.enqueue(std::move(msg));
		} catch (const zmq::error_t &e) {
			logger->log_error("[ZeroMQP2P Consumer] Receive failed: "
			                  + std::string(e.what()));
			continue;
		}
	}

	stop_deserialize_thread_();
}

void ZeroMQP2PConsumer::log_configuration() {
	logger->log_config("[ZeroMQP2P Consumer] [CONFIG_BEGIN]");

	logger->log_config("[CONFIG] socket_type=ZMQ_SUB"); // Adjust if needed
	logger->log_config("[CONFIG] socket_id="
	                   + std::to_string(subscriber.get(zmq::sockopt::fd)));
	logger->log_config("[CONFIG] endpoint="
	                   + std::string(std::getenv("CONSUMER_ENDPOINT")));
	logger->log_config("[CONFIG] topics=" + std::string(std::getenv("TOPICS")));
	int hwm, linger, rcv_buffer;
	size_t sz = sizeof(int);

	zmq_getsockopt(subscriber, ZMQ_RCVHWM, &hwm, &sz);
	zmq_getsockopt(subscriber, ZMQ_LINGER, &linger, &sz);
	zmq_getsockopt(subscriber, ZMQ_RCVTIMEO, &rcv_buffer, &sz);

	logger->log_config("[CONFIG] ZMQ_RCVHWM=" + std::to_string(hwm));
	logger->log_config("[CONFIG] ZMQ_LINGER=" + std::to_string(linger));
	logger->log_config("[CONFIG] ZMQ_RCVTIMEO=" + std::to_string(rcv_buffer));

	int major, minor, patch;
	zmq_version(&major, &minor, &patch);
	logger->log_config("[CONFIG] zmq_version=" + std::to_string(major) + "."
	                   + std::to_string(minor) + "." + std::to_string(patch));

	logger->log_config("[ZeroMQP2P Consumer] [CONFIG_END]");
}

void ZeroMQP2PConsumer::start_deserialize_thread_() {
	stop_deserialization_ = false;
	stop_receiving_ = false;

	deserialize_thread_ = std::thread([this]() {
		while (!stop_deserialization_) {
			zmq::message_t msg;
			if (!deserialize_queue_.try_dequeue(msg)) {
				std::this_thread::yield();
				continue;
			}

			const void *data_ptr = msg.data();
			const size_t data_len = msg.size();

			Payload payload;
			std::string topic;
			bool ok = deserialize(data_ptr, data_len, topic, payload);

			if (!ok) {
				logger->log_error(
				    "[ZeroMQP2P Consumer] Deserialization failed.");
				continue;
			}

			logger->log_study("Deserialized," + payload.message_id + "," + topic
			                  + "," + std::to_string(payload.data_size) + ","
			                  + std::to_string(data_len));

			if (payload.kind == PayloadKind::TERMINATION) {
				subscribed_streams.dec();
				logger->log_info(
				    "[ZeroMQP2P Consumer] Termination signal received "
				    "for message ID: "
				    + payload.message_id);
				if (subscribed_streams.get() == 0) {
					logger->log_info("[ZeroMQP2P Consumer] All streams "
					                 "terminated. Exiting.");
					stop_receiving_ = true;
				}
				logger->log_info(
				    "[ZeroMQP2P Consumer] Remaining subscribed streams: "
				    + std::to_string(subscribed_streams.get()));
			}
		}
	});
}

void ZeroMQP2PConsumer::stop_deserialize_thread_() {
	stop_deserialization_ = true;
	if (deserialize_thread_.joinable()) {
		deserialize_thread_.join();
	}
}