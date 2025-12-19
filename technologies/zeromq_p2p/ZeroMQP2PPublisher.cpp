#include "ZeroMQP2PPublisher.hpp"

#include <cstddef>
#include <cstdlib>
#include <string>

#include "Payload.hpp"

bool ZeroMQP2PPublisher::serialize(const Payload &messages, void *out) {
	logger->log_error("[ZeroMQP2P Publisher] serialize called without topic. "
	                  "This should not happen.");
	return false;
}

bool ZeroMQP2PPublisher::serialize(const Payload &message, std::string topic,
                                   void *out) {
	char *ptr = static_cast<char *>(out);

	// Topic Length
	const uint8_t topic_len = static_cast<uint8_t>(topic.size());
	std::memcpy(ptr, &topic_len, sizeof(topic_len));
	ptr += sizeof(topic_len);

	// Topic
	std::memcpy(ptr, topic.data(), topic_len);
	ptr += topic_len;

	// Message ID length
	const uint16_t id_len = static_cast<uint16_t>(message.message_id.size());
	std::memcpy(ptr, &id_len, sizeof(id_len));
	ptr += sizeof(id_len);

	// Message ID
	std::memcpy(ptr, message.message_id.data(), id_len);
	ptr += id_len;

	// Kind
	const uint8_t kind = static_cast<uint8_t>(message.kind);
	std::memcpy(ptr, &kind, sizeof(kind));
	ptr += sizeof(kind);

	// Data size
	const size_t data_size = static_cast<size_t>(message.data_size);
	std::memcpy(ptr, &data_size, sizeof(data_size));
	ptr += sizeof(data_size);

	// Data
	std::memcpy(ptr, message.data.data(), data_size);

	return true;
}

ZeroMQP2PPublisher::ZeroMQP2PPublisher(std::shared_ptr<Logger> logger) try
    : IPublisher(logger), context(1), publisher(context, ZMQ_PUB) {
	logger->log_debug("[ZeroMQP2P Publisher] Constructor finished");
} catch (const zmq::error_t &e) {
	logger->log_error("[ZeroMQP2P Publisher] Constructor failed: "
	                  + std::string(e.what()));
}

ZeroMQP2PPublisher::~ZeroMQP2PPublisher() {
	publisher.close();
	context.close();
}

void ZeroMQP2PPublisher::initialize() {
	logger->log_study("[ZeroMQP2P Publisher] Initializing");
	const char *vendpoint = std::getenv("PUBLISHER_ENDPOINT");
	if (!vendpoint) {
		logger->log_debug("[ZeroMQP2P Publisher] PUBLISHER_ENDPOINT not set, "
		                  "default to 0.0.0.0");
		// throw std::runtime_error("PUBLISHER_ENDPOINT environment variable not
		// set.");
		endpoint = "tcp://0.0.0.0:5555";
	} else {
		endpoint =
		    "tcp://" + std::string(std::getenv("PUBLISHER_ENDPOINT")) + ":5555";
	}

	logger->log_debug("[ZeroMQP2P Publisher] Binding to " + endpoint);
	try {
		publisher.bind(endpoint);
		logger->log_debug("[ZeroMQP2P Publisher] Bound to " + endpoint);
	} catch (const zmq::error_t &e) {
		logger->log_error("[ZeroMQP2P Publisher] Initialization failed: "
		                  + std::string(e.what()));
	}
	logger->log_study("Initialized");
	log_configuration();
}

void ZeroMQP2PPublisher::send_message(const Payload &message,
                                      std::string &topic) {
	logger->log_study("Intention," + message.message_id + ","
	                  + std::to_string(message.data_size) + "," + topic);
	try {
		size_t total_size = sizeof(uint8_t) // Topic Length
		    + topic.size()                  // Topic
		    + sizeof(uint16_t)              // Message ID Length
		    + message.message_id.size()     // Message ID
		    + sizeof(uint8_t)               // Kind
		    + sizeof(size_t)                // Data size
		    + message.data_size;            // Data
		std::string serialized(total_size, '\0');
		serialize(message, topic, serialized.data());

		logger->log_debug(
		    "[ZeroMQP2P Publisher] Serialized payload ID: " + message.message_id
		    + " and size: " + std::to_string(message.data_size) + " bytes");

		zmq::message_t zmq_message(serialized.begin(), serialized.end());
		publisher.send(zmq_message, zmq::send_flags::none);
		logger->log_study("Publication," + message.message_id + ","
		                  + std::to_string(message.data_size) + "," + topic
		                  + "," + std::to_string(zmq_message.size()));
		logger->log_debug(
		    "[ZeroMQP2P Publisher] Socket connected clients: "
		    + std::to_string(publisher.get(zmq::sockopt::events)));

	} catch (const zmq::error_t &e) {
		logger->log_study("DeliveryError" + message.message_id + ","
		                  + std::to_string(message.data_size) + "," + topic);
	}
}

void ZeroMQP2PPublisher::log_configuration() {
	logger->log_config("[ZeroMQP2P Publisher] [CONFIG_BEGIN]");

	logger->log_config("[CONFIG] socket_type=ZMQ_PUB");
	logger->log_config("[CONFIG] endpoint=" + endpoint);

	// Common socket options
	int hwm, linger, snd_buffer;
	size_t sz = sizeof(int);

	zmq_getsockopt(publisher, ZMQ_SNDHWM, &hwm, &sz);
	zmq_getsockopt(publisher, ZMQ_LINGER, &linger, &sz);
	zmq_getsockopt(publisher, ZMQ_SNDTIMEO, &snd_buffer, &sz);

	logger->log_config("[CONFIG] ZMQ_SNDHWM=" + std::to_string(hwm));
	logger->log_config("[CONFIG] ZMQ_LINGER=" + std::to_string(linger));
	logger->log_config("[CONFIG] ZMQ_SNDBUF=" + std::to_string(snd_buffer));

	int major, minor, patch;
	zmq_version(&major, &minor, &patch);
	logger->log_config("[CONFIG] zmq_version=" + std::to_string(major) + "."
	                   + std::to_string(minor) + "." + std::to_string(patch));

	logger->log_config("[ZeroMQP2P Publisher] [CONFIG_END]");
}