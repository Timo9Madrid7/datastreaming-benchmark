#include "KafkaCppPublisher.hpp"

#include <cstddef>
#include <cstring>
#include <librdkafka/rdkafkacpp.h>
#include <list>
#include <memory>
#include <string>

#include "KafkaCallbacks.hpp"
#include "Payload.hpp"
#include "Utils.hpp"

KafkaCppPublisher::KafkaCppPublisher(std::shared_ptr<Logger> logger) try
    : IPublisher(logger), producer_(nullptr), conf_(nullptr) {
	logger->log_info("[Kafka Publisher] KafkaPublisher created.");
} catch (const std::exception &e) {
	logger->log_error("[Kafka Publisher] Constructor failed: "
	                  + std::string(e.what()));
}

KafkaCppPublisher::~KafkaCppPublisher() {
	logger->log_debug("[Kafka Publisher] Cleaning up Kafka producer...");
	if (producer_) {
		logger->log_debug("[Kafka Publisher] Polling before flush...");
		int served = 0;
		do {
			served = producer_->poll(0);
		} while (served > 0);
		while (producer_->outq_len() > 0) {
			producer_->poll(100);
		}
		logger->log_debug("[Kafka Publisher] Flushing");
		producer_->flush(10 * 1000);
		logger->log_debug("[Kafka Publisher] Destroying");
	}
	logger->log_debug("[Kafka Publisher] Kafka destructor finished.");
}

void KafkaCppPublisher::initialize() {
	std::string vendpoint =
	    utils::get_env_var_or_default("PUBLISHER_ENDPOINT", "localhost");
	const std::string port =
	    utils::get_env_var_or_default("PUBLISHER_PORT", "9092");
	// 0.0.0.0 is valid for bind(), not for connect(). Treat it as a common
	// misconfiguration and default to localhost (broker runs in-container for
	// kafka_p2p).
	if (vendpoint == "0.0.0.0") {
		logger->log_error("[Kafka Publisher] PUBLISHER_ENDPOINT=0.0.0.0 is not "
		                  "a valid broker address; using localhost instead.");
		vendpoint = "localhost";
	}
	broker_ = vendpoint + ":" + port;
	logger->log_info("[Kafka Publisher] Using broker: " + broker_);

	event_cb_ = std::make_unique<KafkaEventCb>(logger, "Kafka Publisher");
	dr_cb_ = std::make_unique<KafkaDeliveryReportCb>(logger);

	conf_.reset(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

	std::string errstr;

	if (conf_->set("event_cb", event_cb_.get(), errstr)
	    != RdKafka::Conf::CONF_OK) {
		logger->log_error("[Kafka Publisher] Failed to set event_cb: "
		                  + errstr);
		throw std::runtime_error("Failed to set event_cb: " + errstr);
	}

	if (conf_->set("dr_cb", dr_cb_.get(), errstr) != RdKafka::Conf::CONF_OK) {
		logger->log_error("[Kafka Publisher] Failed to set dr_cb: " + errstr);
		throw std::runtime_error("Failed to set dr_cb: " + errstr);
	}

	if (conf_->set("bootstrap.servers", broker_, errstr)
	    != RdKafka::Conf::CONF_OK) {
		logger->log_error("[Kafka Publisher] Failed to set bootstrap.servers: "
		                  + errstr);
		throw std::runtime_error("Failed to set bootstrap.servers: " + errstr);
	}

	// Producer performance configuration (latency/throughput trade-offs)
	conf_->set("acks", "0", errstr);
	conf_->set("linger.ms", "5", errstr);
	conf_->set("batch.size", "1048576", errstr);
	conf_->set("batch.num.messages", "10000", errstr);
	conf_->set("compression.type", "none", errstr);
	conf_->set("queue.buffering.max.kbytes", "1048576", errstr);
	conf_->set("queue.buffering.max.messages", "1000000", errstr);
	// Force to send large messages if needed
	conf_->set("message.max.bytes", "16777216", errstr);

	producer_.reset(RdKafka::Producer::create(conf_.get(), errstr));

	if (!producer_) {
		logger->log_error("[Kafka Publisher] Failed to create producer: "
		                  + errstr);
		throw std::runtime_error("Failed to create producer: " + errstr);
	}
	log_configuration();
}

void KafkaCppPublisher::send_message(const Payload &message,
                                     std::string &topic) {
	logger->log_study("Serializing," + message.message_id + "," + topic);

	size_t serialized_size = message.serialized_bytes;
	std::string serialized(serialized_size, '\0');
	if (!Payload::serialize(message, serialized.data())) {
		logger->log_error(
		    "[Kafka Publisher] Serialization failed for message ID: "
		    + message.message_id);
		return;
	}

	RdKafka::ErrorCode err;
	do {
		err = producer_->produce(
		    topic, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY,
		    const_cast<char *>(serialized.data()), serialized.size(),
		    message.message_id.c_str(), message.message_id.size(), 0, nullptr);

		if (err == RdKafka::ERR__QUEUE_FULL) {
			producer_->poll(100);
		}

	} while (err == RdKafka::ERR__QUEUE_FULL);

	if (err != RdKafka::ERR_NO_ERROR) {
		logger->log_error("[Kafka Publisher] Failed to produce message ID: "
		                  + message.message_id
		                  + " Error: " + RdKafka::err2str(err));
	} else {
		logger->log_info("[Kafka Publisher] Produced message ID: "
		                 + message.message_id + " to topic: " + topic);
	}

	producer_->poll(0);
}

void KafkaCppPublisher::log_configuration() {
	std::unique_ptr<std::list<std::string>> dump(conf_->dump());

	logger->log_config("[Kafka Publisher] [CONFIG_BEGIN]");
	for (auto it = dump->begin(); it != dump->end();) {
		logger->log_config("[CONFIG] " + *it++ + "=" + *it++);
	}
	logger->log_config("[Kafka Publisher] [CONFIG_END]");
}