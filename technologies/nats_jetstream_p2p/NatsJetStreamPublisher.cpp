#include "NatsJetStreamPublisher.hpp"

#include <cstdint>
#include <nats/status.h>
#include <sstream>
#include <stdexcept>
#include <string>
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

NatsJetStreamPublisher::NatsJetStreamPublisher(std::shared_ptr<Logger> logger)
    : IPublisher(std::move(logger)),
      connection_(nullptr, &natsConnection_Destroy),
      js_(nullptr, &jsCtx_Destroy) {
	this->logger->log_info("[NATS JetStream Publisher] Created.");
}

NatsJetStreamPublisher::~NatsJetStreamPublisher() {
	logger->log_debug("[NATS JetStream Publisher] Cleaning up...");
	js_.reset();
	connection_.reset();
	logger->log_debug("[NATS JetStream Publisher] Destructor finished.");
}

void NatsJetStreamPublisher::initialize() {
	std::string endpoint =
	    utils::get_env_var_or_default("PUBLISHER_ENDPOINT", "127.0.0.1");
	if (endpoint == "0.0.0.0") {
		endpoint = "127.0.0.1";
	}
	const std::string port =
	    utils::get_env_var_or_default("PUBLISHER_PORT", "4222");
	nats_url_ = build_nats_url(endpoint, port);

	natsConnection *conn = nullptr;
	natsStatus status = natsConnection_ConnectTo(&conn, nats_url_.c_str());
	if (status != NATS_OK) {
		throw std::runtime_error(
		    "[NATS JetStream Publisher] Failed to connect: "
		    + std::string(natsStatus_GetText(status)));
	}
	connection_.reset(conn);

	jsOptions js_opts;
	jsOptions_Init(&js_opts);
	jsCtx *js = nullptr;
	status = natsConnection_JetStream(&js, connection_.get(), &js_opts);
	if (status != NATS_OK) {
		throw std::runtime_error(
		    "[NATS JetStream Publisher] JetStream init failed: "
		    + std::string(natsStatus_GetText(status)));
	}
	js_.reset(js);

	stream_name_ = utils::get_env_var_or_default("NATS_STREAM", "benchmark");
	const std::string topics = utils::get_env_var_or_default("TOPICS", "");
	const std::vector<std::string> topic_list = split_csv(topics);
	if (topic_list.empty()) {
		throw std::runtime_error("[NATS JetStream Publisher] TOPICS is "
		                         "required to configure the stream.");
	}
	jsStreamConfig stream_cfg;
	jsStreamConfig_Init(&stream_cfg);
	stream_cfg.Name = stream_name_.c_str();
	stream_cfg.Storage = js_MemoryStorage;
	// consistent with nats.config
	stream_cfg.MaxBytes = static_cast<int64_t>(4) * 1024 * 1024 * 1024;
	std::vector<const char *> subjects;
	subjects.reserve(topic_list.size());
	for (const auto &topic : topic_list) {
		subjects.push_back(topic.c_str());
	}
	stream_cfg.Subjects = subjects.data();
	stream_cfg.SubjectsLen = static_cast<int>(subjects.size());

	jsStreamInfo *stream_info = nullptr;
	jsErrCode jerr = static_cast<jsErrCode>(0);
	status = js_AddStream(&stream_info, js_.get(), &stream_cfg, nullptr, &jerr);
	if (status == NATS_OK) {
		jsStreamInfo_Destroy(stream_info);
	} else if (jerr != JSStreamNameExistErr) {
		throw std::runtime_error(
		    "[NATS JetStream Publisher] Stream setup failed: "
		    + std::string(natsStatus_GetText(status))
		    + ", jsErr=" + std::to_string(static_cast<int>(jerr)));
	}

	logger->log_info("[NATS JetStream Publisher] Initialized.");
	log_configuration();
}

void NatsJetStreamPublisher::send_message(const Payload &message,
                                          std::string &subject) {
	logger->log_study("Serializing," + message.message_id + "," + subject);

	const size_t serialized_size = message.serialized_bytes;
	std::string serialized(serialized_size, '\0');
	if (!Payload::serialize(message, serialized.data())) {
		logger->log_error(
		    "[NATS JetStream Publisher] Serialization failed for message ID: "
		    + message.message_id);
		return;
	}

	// // Note: The commented-out codes are synchronous publish with ack
	// jsPubAck *ack = nullptr;
	// jsErrCode jerr = static_cast<jsErrCode>(0);
	// natsStatus status = js_Publish(
	//     &ack, js_.get(), subject.c_str(), serialized.data(),
	//     static_cast<int>(serialized.size()), nullptr, &jerr);
	natsStatus status =
	    js_PublishAsync(js_.get(), subject.c_str(), serialized.data(),
	                    static_cast<int>(serialized.size()), nullptr);

	if (status != NATS_OK) {
		logger->log_error("[NATS JetStream Publisher] Publish failed: "
		                  + std::string(natsStatus_GetText(status)));
		return;
	}
	// if (ack != nullptr) {
	// 	jsPubAck_Destroy(ack);
	// }

	logger->log_study("Publication," + message.message_id + "," + subject + ","
	                  + std::to_string(message.data_size) + ","
	                  + std::to_string(serialized_size));
}

void NatsJetStreamPublisher::log_configuration() {
	logger->log_config("[NATS JetStream Publisher] [CONFIG_BEGIN]");
	logger->log_config("[CONFIG] NATS_URL=" + nats_url_);
	logger->log_config("[CONFIG] NATS_STREAM=" + stream_name_);
	logger->log_config("[CONFIG] TOPICS="
	                   + utils::get_env_var_or_default("TOPICS", ""));
	logger->log_config("[NATS JetStream Publisher] [CONFIG_END]");
}
