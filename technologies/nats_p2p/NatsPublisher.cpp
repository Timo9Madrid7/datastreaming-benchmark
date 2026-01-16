#include "NatsPublisher.hpp"

#include <cstring>
#include <nats/status.h>
#include <stdexcept>
#include <string>

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

NatsPublisher::NatsPublisher(std::shared_ptr<Logger> logger)
    : IPublisher(logger), connection_(nullptr, &natsConnection_Destroy) {
	logger->log_info("[NATS Publisher] NatsPublisher created.");
}

NatsPublisher::~NatsPublisher() {
	logger->log_debug("[NATS Publisher] Cleaning up NATS publisher...");
	connection_.reset();
	logger->log_debug("[NATS Publisher] Destructor finished.");
}

void NatsPublisher::initialize() {
	std::string endpoint =
	    utils::get_env_var_or_default("PUBLISHER_ENDPOINT", "127.0.0.1");
	if (endpoint == "0.0.0.0") {
		endpoint = "127.0.0.1"; // NATS server and producer on same machine
	}
	const std::string port =
	    utils::get_env_var_or_default("PUBLISHER_PORT", "4222");
	nats_url_ = build_nats_url(endpoint, port);

	natsConnection *conn = nullptr;
	natsStatus status = natsConnection_ConnectTo(&conn, nats_url_.c_str());
	if (status != NATS_OK) {
		throw std::runtime_error("[NATS Publisher] Failed to connect: "
		                         + std::string(natsStatus_GetText(status)));
	}
	connection_.reset(conn);

	logger->log_info("[NATS Publisher] Publisher initialized.");
	log_configuration();
}

void NatsPublisher::send_message(const Payload &message, std::string &subject) {
	logger->log_study("Serializing," + message.message_id + "," + subject);

	const size_t serialized_size = message.serialized_bytes;
	std::string serialized(serialized_size, '\0');
	if (!Payload::serialize(message, serialized.data())) {
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

	logger->log_study("Publication," + message.message_id + "," + subject + ","
	                  + std::to_string(message.data_size) + ","
	                  + std::to_string(serialized_size));
}

void NatsPublisher::log_configuration() {
	logger->log_config("[NATS Publisher] [CONFIG_BEGIN]");
	logger->log_config("[CONFIG] NATS_URL=" + nats_url_);
	logger->log_config("[CONFIG] TOPICS="
	                   + utils::get_env_var_or_default("TOPICS", ""));
	logger->log_config("[NATS Publisher] [CONFIG_END]");
}
