#include "ZeroMQP2PConsumer.hpp"

#include <cstdlib>
#include <iostream>
#include <sstream>
#include <thread>
#include <vector>

#include "Payload.hpp"

bool ZeroMQP2PConsumer::deserialize(const void *raw_message, size_t len,
                                    Payload &out) {
	const char *data = static_cast<const char *>(raw_message);
	size_t offset = 0;

	// Topic length and content (skip over it)
	uint8_t topic_len = static_cast<uint8_t>(data[offset]);
	offset += sizeof(uint8_t);
	std::string topic(data + offset, topic_len);
	offset += topic_len;

	// Message ID length
	uint16_t id_len;
	std::memcpy(&id_len, data + offset, sizeof(uint16_t));
	offset += sizeof(uint16_t);

	// Message ID
	std::string message_id(data + offset, id_len);
	offset += id_len;

	// Kind
	uint8_t kind_byte;
	std::memcpy(&kind_byte, data + offset, sizeof(uint8_t));
	offset += sizeof(uint8_t);
	PayloadKind kind_payload = static_cast<PayloadKind>(kind_byte);

	// Data size
	size_t data_size;
	std::memcpy(&data_size, data + offset, sizeof(size_t));
	offset += sizeof(size_t);

	if (len != offset + data_size) {
		logger->log_error(
		    "[ZeroMQP2P Consumer] Invalid message: size mismatch");
		return false;
	}

	// Data
	std::vector<uint8_t> payload_data(data + offset, data + offset + data_size);

	out.message_id = message_id;
	out.kind = kind_payload;
	out.data_size = data_size;
	out.data = std::move(payload_data);

	return true;
}

ZeroMQP2PConsumer::ZeroMQP2PConsumer(std::shared_ptr<Logger> logger) try
    : IConsumer(logger), context(1), subscriber(context, ZMQ_SUB) {
	logger->log_debug("[ZeroMQP2P Consumer] Constructor finished");
} catch (const zmq::error_t &e) {
	std::cerr << "[ZeroMQP2P Consumer] Constructor failed: " << e.what()
	          << std::endl;
}

ZeroMQP2PConsumer::~ZeroMQP2PConsumer() {
	subscriber.close();
	context.close();
}

void ZeroMQP2PConsumer::initialize() {
	logger->log_study("Initializing");
	const char *vendpoint = std::getenv("CONSUMER_ENDPOINT");
	const char *vtopics = std::getenv("TOPICS");
	std::string consumer_id = std::getenv("CONTAINER_ID");
	if (!vendpoint) {
		unique_publishers.insert("zeromq_p2p-P" + consumer_id.substr(1));
		logger->log_debug(
		    "[ZeroMQP2P Consumer] CONSUMER_ENDPOINT not set, default to "
		    "publisher with same numerical id: zeromq_p2p-P"
		    + consumer_id.substr(1));
	} else {
		if (!vtopics) {
			subscribed_streams.insert({"zeromq_p2p-P" + consumer_id.substr(1),
			                           consumer_id.substr(1)});
			subscribe(consumer_id.substr(1));
			logger->log_debug("[ZeroMQP2P Consumer] TOPICS not set, default to "
			                  "publisher with same numerical id: "
			                  + consumer_id.substr(1));
		} else {
			std::istringstream topics(vtopics);
			std::string topic;
			std::istringstream publishers(vendpoint);
			std::string publisher;
			// logger->log_debug("[ZeroMQP2P Consumer] Subscribing to topic
			// list: " + std::to_string(vtopics));
			while (std::getline(topics, topic, ',')) {
				std::getline(publishers, publisher, ',');
				if (!publisher.empty()) {
					unique_publishers.insert(publisher);
				}
				logger->log_debug(
				    "[ZeroMQP2P Consumer] Handling subscription to topic "
				    + topic);
				if (!topic.empty()) {
					logger->log_info(
					    "[ZeroMQP2P Consumer] Connecting to stream ("
					    + publisher + "," + topic + ")");
					subscribe(topic);
					subscribed_streams.insert({publisher, topic});
				}
			}
		}
	}

	try {
		for (const auto &publisher : unique_publishers) {
			logger->log_debug("[ZeroMQP2P Consumer] Connecting to publisher "
			                  + publisher);
			subscriber.connect("tcp://" + publisher + ":5555");
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(100));

		logger->log_debug("[ZeroMQP2P Consumer] Connected and subscribed.");
	} catch (const zmq::error_t &e) {
		logger->log_error("[ZeroMQP2P Consumer] Initialization failed: "
		                  + std::string(e.what()));
	}
	logger->log_study("Initialized");
	log_configuration();
}

void ZeroMQP2PConsumer::subscribe(const std::string &topic) {
	logger->log_debug("[ZeroMQP2P Consumer] Subscribing to topic: " + topic);
	subscriber.set(zmq::sockopt::subscribe,
	               std::string(1, static_cast<char>(topic.size())) + topic);
	// Set a timeout for receiving messages (10s)
	subscriber.set(zmq::sockopt::rcvtimeo, 10000);
}

Payload ZeroMQP2PConsumer::receive_message() {
	zmq::message_t zmq_message;
	Payload payload;

	try {
		auto result = subscriber.recv(zmq_message, zmq::recv_flags::none);
		if (!result) {
			logger->log_error(
			    "[ZeroMQP2P Consumer] Failed to receive message!");
			return payload;
		}

		std::string raw(static_cast<const char *>(zmq_message.data()),
		                zmq_message.size());

		if (deserialize(raw.data(), raw.size(), payload) == false) {
			logger->log_error("[ZeroMQP2P Consumer] Deserialization failed for "
			                  "received message");
			return payload;
		}

		// Recover topic from the raw message
		const char *data = raw.data();
		size_t offset = 0;
		// 1. Topic length and content (skip over it)
		uint8_t topic_len = static_cast<uint8_t>(data[offset]);
		offset += 1;
		if (raw.size() < offset + topic_len) {
			throw std::runtime_error("Invalid message: incomplete topic");
		}
		std::string topic(data + offset, topic_len);
		logger->log_info(
		    "[ZeroMQP2P Consumer] Received message ID: " + payload.message_id
		    + ", Size: " + std::to_string(payload.data_size) + " bytes");
		logger->log_study("Reception," + payload.message_id + ","
		                  + std::to_string(payload.data_size) + "," + topic
		                  + "," + std::to_string(zmq_message.size()));

		// Poison pill handling based on ID
		if (payload.message_id.find(TERMINATION_SIGNAL) != std::string::npos) {
			std::string source =
			    payload.message_id.substr(0, payload.message_id.find(":"));

			terminated_streams.insert({source, topic});

			logger->log_info(
			    "[ZeroMQP2P Consumer] Termination signal from source: " + source
			    + " on topic: " + topic);
			logger->log_debug("[ZeroMQP2P Consumer] Streams closed: "
			                  + std::to_string(terminated_streams.size()) + "/"
			                  + std::to_string(subscribed_streams.size()));

			return Payload::make(
			    payload.message_id.substr(0, payload.message_id.find(':')) + "-"
			        + topic,
			    0, 0, PayloadKind::TERMINATION);
		}

		return payload;

	} catch (const zmq::error_t &e) {
		logger->log_error("[ZeroMQP2P Consumer] Receive failed: "
		                  + std::string(e.what()));
		return payload;
	}
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
	logger->log_config("[CONFIG] ZMQ_RCVBUF=" + std::to_string(rcv_buffer));

	int major, minor, patch;
	zmq_version(&major, &minor, &patch);
	logger->log_config("[CONFIG] zmq_version=" + std::to_string(major) + "."
	                   + std::to_string(minor) + "." + std::to_string(patch));

	logger->log_config("[ZeroMQP2P Consumer] [CONFIG_END]");
}
