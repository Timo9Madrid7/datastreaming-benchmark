#include "NatsConsumer.hpp"

#include <cstdint>
#include <cstring>
#include <nats/status.h>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "Logger.hpp"
#include "Payload.hpp"
#include "Utils.hpp"

namespace {
std::string build_nats_url(const std::string &endpoint,
                           const std::string &port) {
	if (endpoint.rfind("nats://", 0) == 0 || endpoint.rfind("tls://", 0) == 0) {
		return endpoint;
	}
	return "nats://" + endpoint + ":" + port;
}
} // namespace

NatsConsumer::NatsConsumer(std::shared_ptr<Logger> logger)
    : IConsumer(logger), connection_(nullptr, &natsConnection_Destroy),
      subscription_(nullptr, &natsSubscription_Destroy) {
	logger->log_info("[NATS Consumer] NatsConsumer created.");
}

NatsConsumer::~NatsConsumer() {
	logger->log_debug("[NATS Consumer] Cleaning up NATS consumer...");
	subscription_.reset();
	connection_.reset();
	logger->log_debug("[NATS Consumer] Destructor finished.");
}

void NatsConsumer::initialize() {
	const std::optional<std::string> vtopics = utils::get_env_var("TOPICS");
	if (!vtopics || vtopics->empty()) {
		throw std::runtime_error(
		    "[NATS Consumer] Missing required environment variable TOPICS.");
	}

	const std::string port =
	    utils::get_env_var_or_default("CONSUMER_PORT", "4222");
	const std::string vendpoints = utils::get_env_var_or_default(
	    "PUBLISHER_ENDPOINTS",
	    utils::get_env_var_or_default("CONSUMER_ENDPOINT", "localhost"));
	std::string endpoint;
	std::istringstream endpoints_stream(vendpoints);
	// FIXME: Only use the first endpoint from the list
	std::getline(endpoints_stream, endpoint, ',');
	if (endpoint.empty()) {
		throw std::runtime_error("[NATS Consumer] CONSUMER_ENDPOINT is empty.");
	}
	nats_url_ = build_nats_url(endpoint, port);

	std::unordered_set<std::string> unique_topics;
	std::string topic;
	std::istringstream topics_stream(vtopics.value());
	while (std::getline(topics_stream, topic, ',')) {
		if (topic.empty()) {
			continue;
		}
		if (unique_topics.insert(topic).second) {
			subscribe(topic);
		}
	}

	if (unique_topics.empty()) {
		throw std::runtime_error("[NATS Consumer] No valid topics provided.");
	}

	natsConnection *conn = nullptr;
	natsStatus status = natsConnection_ConnectTo(&conn, nats_url_.c_str());
	if (status != NATS_OK) {
		throw std::runtime_error("[NATS Consumer] Failed to connect: "
		                         + std::string(natsStatus_GetText(status)));
	}
	connection_.reset(conn);

	natsSubscription *sub = nullptr;
	// TODO: Subscribe to each topic separately
	status = natsConnection_SubscribeSync(&sub, conn, ">");
	if (status != NATS_OK) {
		throw std::runtime_error("[NATS Consumer] Failed to subscribe: "
		                         + std::string(natsStatus_GetText(status)));
	}
	subscription_.reset(sub);

	logger->log_info("[NATS Consumer] Consumer initialized.");
	log_configuration();
}

void NatsConsumer::subscribe(const std::string &subject) {
	logger->log_info("[NATS Consumer] Registering topic: " + subject);
	subscribed_streams.inc();
}

bool NatsConsumer::deserialize(const void *raw_message, size_t len,
                               Payload &out) {
	const char *data = static_cast<const char *>(raw_message);
	size_t offset = 0;

	if (len < sizeof(uint16_t) + sizeof(uint8_t) + sizeof(size_t)) {
		logger->log_error("[NATS Consumer] Message too short to deserialize.");
		return false;
	}

	uint16_t id_len = 0;
	std::memcpy(&id_len, data + offset, sizeof(id_len));
	offset += sizeof(id_len);

	if (len < offset + id_len + sizeof(uint8_t) + sizeof(size_t)) {
		logger->log_error(
		    "[NATS Consumer] Invalid message: id length out of bounds.");
		return false;
	}

	std::string message_id(data + offset, id_len);
	offset += id_len;

	uint8_t kind_byte = 0;
	std::memcpy(&kind_byte, data + offset, sizeof(kind_byte));
	offset += sizeof(kind_byte);
	PayloadKind kind_payload = static_cast<PayloadKind>(kind_byte);

	size_t data_size = 0;
	std::memcpy(&data_size, data + offset, sizeof(data_size));
	offset += sizeof(data_size);

	if (len != offset + data_size) {
		logger->log_error(
		    "[NATS Consumer] Mismatch in expected data size. Expected total "
		    "size: "
		    + std::to_string(offset + data_size)
		    + ", actual size: " + std::to_string(len));
		return false;
	}

	std::vector<uint8_t> payload_data(data + offset, data + offset + data_size);

	out.message_id = std::move(message_id);
	out.kind = kind_payload;
	out.data_size = data_size;
	out.data = std::move(payload_data);

	return true;
}

void NatsConsumer::start_loop() {
	while (true) {
		natsMsg *msg = nullptr;
		natsStatus status =
		    natsSubscription_NextMsg(&msg, subscription_.get(), 1000);
		if (status == NATS_TIMEOUT) {
			continue;
		}
		if (status != NATS_OK) {
			logger->log_error("[NATS Consumer] Receive failed: "
			                  + std::string(natsStatus_GetText(status)));
			continue;
		}
		if (!msg) {
			continue;
		}

		const char *subject = natsMsg_GetSubject(msg);
		const std::string subject_str = subject ? subject : "";

		Payload payload;
		const void *data_ptr = natsMsg_GetData(msg);
		const size_t data_len = static_cast<size_t>(natsMsg_GetDataLength(msg));

		if (!deserialize(data_ptr, data_len, payload)) {
			logger->log_error("[NATS Consumer] Deserialization failed.");
			natsMsg_Destroy(msg);
			continue;
		}

		const size_t total_size = sizeof(uint16_t) + payload.message_id.size()
		    + sizeof(uint8_t) + sizeof(size_t) + payload.data_size;

		logger->log_study("Reception," + payload.message_id + ","
		                  + std::to_string(payload.data_size) + ","
		                  + subject_str + "," + std::to_string(total_size));

		if (payload.message_id.find(TERMINATION_SIGNAL) != std::string::npos) {
			subscribed_streams.dec();
			logger->log_info(
			    "[NATS Consumer] Termination signal received for topic: "
			    + subject_str);
			if (subscribed_streams.get() == 0) {
				logger->log_info(
				    "[NATS Consumer] All streams terminated. Exiting.");
				natsMsg_Destroy(msg);
				break;
			}
			logger->log_info("[NATS Consumer] Remaining subscribed streams: "
			                 + std::to_string(subscribed_streams.get()));
		}

		natsMsg_Destroy(msg);
	}
}

void NatsConsumer::log_configuration() {
	logger->log_config("[NATS Consumer] [CONFIG_BEGIN]");
	logger->log_config("[CONFIG] NATS_URL=" + nats_url_);
	logger->log_config("[CONFIG] TOPICS="
	                   + utils::get_env_var_or_default("TOPICS", ""));
	logger->log_config("[NATS Consumer] [CONFIG_END]");
}
