#include "NatsPublisher.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <string>

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

void destroy_connection(std::unique_ptr<natsConnection> &connection) {
	if (!connection)
		return;
	natsConnection_Destroy(connection.get());
	connection.release();
}
} // namespace

NatsPublisher::NatsPublisher(std::shared_ptr<Logger> logger)
    : IPublisher(logger) {
	logger->log_info("[NATS Publisher] NatsPublisher created.");
}

NatsPublisher::~NatsPublisher() {
	logger->log_debug("[NATS Publisher] Cleaning up NATS publisher...");
	destroy_connection(connection_);
	logger->log_debug("[NATS Publisher] Destructor finished.");
}

void NatsPublisher::initialize() {
	const std::optional<std::string> vurl = utils::get_env_var("NATS_URL");
	if (vurl && !vurl->empty()) {
		nats_url_ = vurl.value();
	} else {
		std::string endpoint = utils::get_env_var_or_default(
		    "PUBLISHER_ENDPOINT", "127.0.0.1");
		if (endpoint == "0.0.0.0") {
			endpoint = "127.0.0.1";
		}
		const std::string port = utils::get_env_var_or_default(
		    "NATS_PORT",
		    utils::get_env_var_or_default("PUBLISHER_PORT", "4222"));
		nats_url_ = build_nats_url(endpoint, port);
	}

	natsConnection *conn = nullptr;
	natsStatus status = natsConnection_ConnectTo(&conn, nats_url_.c_str());
	if (status != NATS_OK) {
		throw std::runtime_error(
		    "[NATS Publisher] Failed to connect: "
		    + std::string(natsStatus_GetText(status)));
	}
	connection_.reset(conn);

	logger->log_info("[NATS Publisher] Publisher initialized.");
	log_configuration();
}

bool NatsPublisher::serialize(const Payload &message, void *out) {
	char *ptr = static_cast<char *>(out);

	const uint16_t id_len = static_cast<uint16_t>(message.message_id.size());
	std::memcpy(ptr, &id_len, sizeof(id_len));
	ptr += sizeof(id_len);

	std::memcpy(ptr, message.message_id.data(), id_len);
	ptr += id_len;

	const uint8_t kind = static_cast<uint8_t>(message.kind);
	std::memcpy(ptr, &kind, sizeof(kind));
	ptr += sizeof(kind);

	const size_t size = static_cast<size_t>(message.data_size);
	std::memcpy(ptr, &size, sizeof(size));
	ptr += sizeof(size);

	std::memcpy(ptr, message.data.data(), size);

	return true;
}

void NatsPublisher::send_message(const Payload &message, std::string &subject) {
	const size_t serialized_size = sizeof(uint16_t)
	    + message.message_id.size() + sizeof(uint8_t) + sizeof(size_t)
	    + message.data_size;
	std::string serialized(serialized_size, '\0');
	if (!serialize(message, serialized.data())) {
		logger->log_error(
		    "[NATS Publisher] Serialization failed for message ID: "
		    + message.message_id);
		return;
	}

	natsStatus status = natsConnection_Publish(
	    connection_.get(), subject.c_str(), serialized.data(),
	    static_cast<int>(serialized.size()));

	if (status != NATS_OK) {
		logger->log_error("[NATS Publisher] Publish failed: "
		                  + std::string(natsStatus_GetText(status)));
		return;
	}

	logger->log_study("Publication," + message.message_id + ","
	                  + std::to_string(message.data_size) + "," + subject + ","
	                  + std::to_string(serialized_size));
}

void NatsPublisher::log_configuration() {
	logger->log_config("[NATS Publisher] [CONFIG_BEGIN]");
	logger->log_config("[CONFIG] NATS_URL=" + nats_url_);
	logger->log_config("[CONFIG] TOPICS="
	                   + utils::get_env_var_or_default("TOPICS", ""));
	logger->log_config("[NATS Publisher] [CONFIG_END]");
}
