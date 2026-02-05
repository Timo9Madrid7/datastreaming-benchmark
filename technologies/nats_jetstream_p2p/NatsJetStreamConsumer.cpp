#include "NatsJetStreamConsumer.hpp"

#include <chrono>
#include <climits>
#include <cstddef>
#include <nats/status.h>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
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

std::vector<std::string> split_csv(const std::string &value) {
	std::vector<std::string> items;
	std::string item;
	std::istringstream stream(value);
	while (std::getline(stream, item, ',')) {
		if (!item.empty()) {
			items.push_back(item);
		}
	}
	return items;
}
} // namespace

NatsJetStreamConsumer::NatsJetStreamConsumer(std::shared_ptr<Logger> logger)
    : IConsumer(std::move(logger)), connection_(nullptr, &natsConnection_Destroy),
      subscription_(nullptr, &natsSubscription_Destroy), js_(nullptr, &jsCtx_Destroy),
      deserializer_(this->logger) {
	this->logger->log_info("[NATS JetStream Consumer] Created.");
}

NatsJetStreamConsumer::~NatsJetStreamConsumer() {
	logger->log_debug("[NATS JetStream Consumer] Cleaning up...");
	stop_receiving_ = true;
	deserializer_.stop();

	subscription_.reset();
	js_.reset();
	connection_.reset();
	logger->log_debug("[NATS JetStream Consumer] Destructor finished.");
}

void NatsJetStreamConsumer::initialize() {
	const std::optional<std::string> vtopics = utils::get_env_var("TOPICS");
	if (!vtopics || vtopics->empty()) {
		throw std::runtime_error(
		    "[NATS JetStream Consumer] Missing required environment variable TOPICS.");
	}

	const std::string port =
	    utils::get_env_var_or_default("CONSUMER_PORT", "4222");
	const std::string vendpoints = utils::get_env_var_or_default(
	    "PUBLISHER_ENDPOINTS",
	    utils::get_env_var_or_default("CONSUMER_ENDPOINT", "localhost"));
	std::string endpoint;
	std::istringstream endpoints_stream(vendpoints);
	std::getline(endpoints_stream, endpoint, ',');
	if (endpoint.empty()) {
		throw std::runtime_error(
		    "[NATS JetStream Consumer] CONSUMER_ENDPOINT is empty.");
	}
	nats_url_ = build_nats_url(endpoint, port);

	std::unordered_set<std::string> unique_topics;
	for (const auto &topic : split_csv(vtopics.value())) {
		if (unique_topics.insert(topic).second) {
			subscribe(topic);
		}
	}
	if (unique_topics.empty()) {
		throw std::runtime_error("[NATS JetStream Consumer] No valid topics provided.");
	}

	natsConnection *conn = nullptr;
	natsStatus status = NATS_OK;

	constexpr int kMaxAttempts = 60;
	for (int attempt = 1; attempt <= kMaxAttempts; ++attempt) {
		status = natsConnection_ConnectTo(&conn, nats_url_.c_str());
		if (status == NATS_OK) {
			break;
		}

		logger->log_info("[NATS JetStream Consumer] Connect attempt "
		                 + std::to_string(attempt) + "/"
		                 + std::to_string(kMaxAttempts) + " failed to " + nats_url_ + ": "
		                 + std::string(natsStatus_GetText(status)));
		std::this_thread::sleep_for(std::chrono::milliseconds(500));
	}

	if (status != NATS_OK) {
		throw std::runtime_error("[NATS JetStream Consumer] Failed to connect to "
		                         + nats_url_ + ": "
		                         + std::string(natsStatus_GetText(status)));
	}

	connection_.reset(conn);

	jsOptions js_opts;
	jsOptions_Init(&js_opts);
	jsCtx *js = nullptr;
	status = natsConnection_JetStream(&js, connection_.get(), &js_opts);
	if (status != NATS_OK) {
		throw std::runtime_error("[NATS JetStream Consumer] JetStream init failed: "
		                         + std::string(natsStatus_GetText(status)));
	}
	js_.reset(js);

	stream_name_ = utils::get_env_var_or_default("NATS_STREAM", "benchmark");
	jsErrCode jerr = static_cast<jsErrCode>(0);
	jsStreamInfo *stream_info = nullptr;
	constexpr int kStreamWaitAttempts = 60;
	for (int attempt = 1; attempt <= kStreamWaitAttempts; ++attempt) {
		status =
		    js_GetStreamInfo(&stream_info, js_.get(), stream_name_.c_str(),
		                     nullptr, &jerr);
		if (status == NATS_OK) {
			jsStreamInfo_Destroy(stream_info);
			stream_info = nullptr;
			break;
		}
		logger->log_info("[NATS JetStream Consumer] Waiting for stream '"
		                 + stream_name_ + "' ("
		                 + std::to_string(attempt) + "/"
		                 + std::to_string(kStreamWaitAttempts) + "): "
		                 + std::string(natsStatus_GetText(status)) + ", jsErr="
		                 + std::to_string(static_cast<int>(jerr)));
		std::this_thread::sleep_for(std::chrono::milliseconds(500));
	}
	if (status != NATS_OK) {
		throw std::runtime_error(
		    "[NATS JetStream Consumer] Stream not available: "
		    + std::string(natsStatus_GetText(status)) + ", jsErr="
		    + std::to_string(static_cast<int>(jerr)));
	}

	jsSubOptions sub_opts;
	jsSubOptions_Init(&sub_opts);
	sub_opts.Stream = stream_name_.c_str();

	natsSubscription *sub = nullptr;
	status =
	    js_SubscribeSync(&sub, js_.get(), ">", nullptr, &sub_opts, &jerr);
	if (status != NATS_OK) {
		throw std::runtime_error("[NATS JetStream Consumer] Subscribe failed: "
		                         + std::string(natsStatus_GetText(status)));
	}

	status = natsSubscription_SetPendingLimits(sub, 500 * 1000, INT_MAX);
	if (status != NATS_OK) {
		throw std::runtime_error(
		    "[NATS JetStream Consumer] Failed to set pending limits: "
		    + std::string(natsStatus_GetText(status)));
	}

	subscription_.reset(sub);

	logger->log_info("[NATS JetStream Consumer] Initialized.");
	log_configuration();
}

void NatsJetStreamConsumer::subscribe(const std::string &subject) {
	logger->log_info("[NATS JetStream Consumer] Registering topic: " + subject);
	subscribed_streams.inc();
}

void NatsJetStreamConsumer::start_loop() {
	deserializer_.start(
	    [](const void *data, size_t len, std::string & /*topic*/, Payload &out) {
		    return Payload::deserialize(data, len, out);
	    },
	    [this](const Payload &payload, const std::string & /*topic*/, size_t /*raw_len*/) {
		    if (payload.kind == PayloadKind::TERMINATION) {
			    subscribed_streams.dec();
			    logger->log_info(
			        "[NATS JetStream Consumer] Termination signal received for message ID: "
			        + payload.message_id);
			    if (subscribed_streams.get() == 0) {
				    logger->log_info(
				        "[NATS JetStream Consumer] All streams terminated. Exiting.");
				    stop_receiving_ = true;
			    }
			    logger->log_info(
			        "[NATS JetStream Consumer] Remaining subscribed streams: "
			        + std::to_string(subscribed_streams.get()));
		    }
	    });

	while (!stop_receiving_) {
		natsMsg *msg = nullptr;
		natsStatus status =
		    natsSubscription_NextMsg(&msg, subscription_.get(), 1000);
		if (status == NATS_TIMEOUT) {
			continue;
		}
		if (status != NATS_OK) {
			logger->log_error("[NATS JetStream Consumer] Receive failed: "
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
			logger->log_error("[NATS JetStream Consumer] Deserialization failed.");
			natsMsg_Destroy(msg);
			continue;
		}

		logger->log_study("Reception," + payload.message_id + "," + subject_str);

		utils::Deserializer::Item item;
		item.holder = std::shared_ptr<void>(
		    msg, [](void *p) { natsMsg_Destroy(static_cast<natsMsg *>(p)); });
		item.data = data_ptr;
		item.len = data_len;
		item.topic = subject_str;
		item.message_id = payload.message_id;
		if (!deserializer_.enqueue(std::move(item))) {
			logger->log_error(
			    "[NATS JetStream Consumer] Deserializer queue full; dropping message.");
			continue;
		}

		status = natsMsg_Ack(msg, nullptr);
		if (status != NATS_OK) {
			logger->log_error("[NATS JetStream Consumer] Ack failed: "
			                  + std::string(natsStatus_GetText(status)));
		}
	}

	deserializer_.stop();
}

void NatsJetStreamConsumer::log_configuration() {
	logger->log_config("[NATS JetStream Consumer] [CONFIG_BEGIN]");
	logger->log_config("[CONFIG] NATS_URL=" + nats_url_);
	logger->log_config("[CONFIG] NATS_STREAM=" + stream_name_);
	logger->log_config(
	    "[CONFIG] TOPICS=" + utils::get_env_var_or_default("TOPICS", ""));
	logger->log_config("[NATS JetStream Consumer] [CONFIG_END]");
}
