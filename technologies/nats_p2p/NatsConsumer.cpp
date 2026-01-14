#include "NatsConsumer.hpp"

#include <chrono>        // for milliseconds, nanoseconds
#include <climits>       // for INT_MAX
#include <cstddef>       // for size_t
#include <nats/nats.h>   // for natsStatus_GetText, natsMsg_Destroy, natsMs...
#include <nats/status.h> // for NATS_OK, natsStatus, NATS_TIMEOUT
#include <optional>      // for optional
#include <sstream>       // for istringstream, basic_istream
#include <stdexcept>     // for runtime_error
#include <string>        // for operator+, string, char_traits, to_string
#include <thread>        // for sleep_for, thread
#include <unordered_set> // for unordered_set
#include <utility>       // for pair

#include "Logger.hpp"  // for Logger
#include "Payload.hpp" // for Payload, PayloadKind, PayloadKind::TERMINATION
#include "Utils.hpp"   // for get_env_var_or_default, get_env_var

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

	stop_receiving_ = true;
	stop_deserialize_thread_();

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
	natsStatus status = NATS_OK;

	constexpr int kMaxAttempts = 60; // 60 * 500ms = 30s
	for (int attempt = 1; attempt <= kMaxAttempts; ++attempt) {
		status = natsConnection_ConnectTo(&conn, nats_url_.c_str());
		if (status == NATS_OK) {
			break;
		}

		logger->log_info(
		    "[NATS Consumer] Connect attempt " + std::to_string(attempt) + "/"
		    + std::to_string(kMaxAttempts) + " failed to " + nats_url_ + ": "
		    + std::string(natsStatus_GetText(status)));

		std::this_thread::sleep_for(std::chrono::milliseconds(500));
	}

	if (status != NATS_OK) {
		throw std::runtime_error("[NATS Consumer] Failed to connect to "
		                         + nats_url_ + ": "
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

	// maximize pending limits to avoid message drops
	status = natsSubscription_SetPendingLimits(sub, 500 * 1000, INT_MAX);
	if (status != NATS_OK) {
		throw std::runtime_error(
		    "[NATS Consumer] Failed to set pending limits: "
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

void NatsConsumer::start_loop() {
	start_deserialize_thread_();

	while (!stop_receiving_) {
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

		Payload payload;
		const char *subject = natsMsg_GetSubject(msg);
		const std::string subject_str = subject ? subject : "";
		const void *data_ptr = natsMsg_GetData(msg);
		const size_t data_len = static_cast<size_t>(natsMsg_GetDataLength(msg));

		if (!Payload::deserialize_id(data_ptr, data_len, payload)) {
			logger->log_error("[NATS Consumer] Deserialization failed.");
			natsMsg_Destroy(msg);
			continue;
		}

		logger->log_study("Reception," + payload.message_id + ","
		                  + subject_str);

		deserialize_queue_.enqueue(msg);
	}

	stop_deserialize_thread_();
}

void NatsConsumer::log_configuration() {
	logger->log_config("[NATS Consumer] [CONFIG_BEGIN]");
	logger->log_config("[CONFIG] NATS_URL=" + nats_url_);
	logger->log_config("[CONFIG] TOPICS="
	                   + utils::get_env_var_or_default("TOPICS", ""));
	logger->log_config("[NATS Consumer] [CONFIG_END]");
}

void NatsConsumer::start_deserialize_thread_() {
	stop_deserialization_ = false;
	stop_receiving_ = false;

	deserialize_thread_ = std::thread([this]() {
		while (!stop_deserialization_) {
			natsMsg *msg;
			if (!deserialize_queue_.try_dequeue(msg)) {
				std::this_thread::sleep_for(std::chrono::nanoseconds(100));
				continue;
			}

			const char *subject = natsMsg_GetSubject(msg);
			const std::string subject_str = subject ? subject : "";
			const void *data_ptr = natsMsg_GetData(msg);
			const size_t data_len =
			    static_cast<size_t>(natsMsg_GetDataLength(msg));

			Payload payload;
			bool ok = Payload::deserialize(data_ptr, data_len, payload);
			natsMsg_Destroy(msg);

			if (!ok) {
				logger->log_error("[NATS Consumer] Deserialization failed.");
				continue;
			}

			logger->log_study("Deserialized," + payload.message_id + ","
			                  + std::to_string(payload.data_size) + ","
			                  + subject_str + "," + std::to_string(data_len));

			if (payload.kind == PayloadKind::TERMINATION) {
				subscribed_streams.dec();
				logger->log_info("[NATS Consumer] Termination signal received "
				                 "for message ID: "
				                 + payload.message_id);
				if (subscribed_streams.get() == 0) {
					logger->log_info(
					    "[NATS Consumer] All streams terminated. Exiting.");
					stop_receiving_ = true;
				}
				logger->log_info(
				    "[NATS Consumer] Remaining subscribed streams: "
				    + std::to_string(subscribed_streams.get()));
			}
		}
	});
}

void NatsConsumer::stop_deserialize_thread_() {
	stop_deserialization_ = true;
	if (deserialize_thread_.joinable()) {
		deserialize_thread_.join();
	}

	natsMsg *msg;
	while (deserialize_queue_.try_dequeue(msg)) {
		if (msg) {
			natsMsg_Destroy(msg);
		}
	}
}