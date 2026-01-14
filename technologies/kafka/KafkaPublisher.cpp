#include "KafkaPublisher.hpp"

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <string>

#include "Payload.hpp"

static void kafka_log_callback(const rd_kafka_t *rk, int level, const char *fac,
                               const char *buf) {
	Logger *logger = static_cast<Logger *>(rd_kafka_opaque(rk));
	logger->log_error("[librdkafka][" + std::string(fac) + "] "
	                  + std::string(buf));
}

std::string extract_message_id(const rd_kafka_message_t *msg) {
	return std::string(static_cast<const char *>(msg->key), msg->key_len);
}

void dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage,
               void * /*opaque*/) {
	Logger *logger = static_cast<Logger *>(rd_kafka_opaque(rk));

	if (rkmessage->err) {
		logger->log_study("DeliveryError," + extract_message_id(rkmessage) + ","
		                  + std::to_string(rkmessage->len) + ",-1,"
		                  + rd_kafka_topic_name(rkmessage->rkt));
	} else {
		logger->log_study("Publication," + extract_message_id(rkmessage) + ","
		                  + std::to_string(rkmessage->len) + ","
		                  + rd_kafka_topic_name(rkmessage->rkt) + ","
		                  + std::to_string(rkmessage->len));
	}
}

KafkaPublisher::KafkaPublisher(std::shared_ptr<Logger> logger) try
    : IPublisher(logger), producer_(nullptr), conf_(nullptr) {
	logger->log_info("[Kafka Publisher] KafkaPublisher created.");
} catch (const std::exception &e) {
	logger->log_error("[Kafka Publisher] Constructor failed: "
	                  + std::string(e.what()));
}

KafkaPublisher::~KafkaPublisher() {
	logger->log_debug("[Kafka Publisher] Cleaning up Kafka topic handles...");

	for (auto &[topic, handle] : topic_handles_) {
		logger->log_debug("[Kafka Publisher] Destroying topic handle for: "
		                  + topic);
		destroy_topic_handle(topic);
		logger->log_debug("[Kafka Publisher] Topic handle for '" + topic
		                  + "' has been destroyed");
	}
	logger->log_debug("[Kafka Publisher] Destroyed topic handles");
	topic_handles_.clear();

	if (producer_) {
		logger->log_debug("[Kafka Publisher] Polling before flush...");
		while (rd_kafka_outq_len(producer_) > 0) {
			rd_kafka_poll(producer_, 100); // wait up to 100ms
		}
		logger->log_debug("[Kafka Publisher] Flushing");
		rd_kafka_flush(producer_, 10 * 1000); // Wait for delivery
		logger->log_debug("[Kafka Publisher] Destroying");

		// destroy producer to free resources
		rd_kafka_destroy(producer_);
		producer_ = nullptr;
		int remaining = rd_kafka_wait_destroyed(5000);
		if (remaining != 0) {
			logger->log_error("[Kafka Publisher] Kafka still has "
			                  + std::to_string(remaining)
			                  + " references after destroy.");
		} else {
			logger->log_debug("[Kafka Publisher] Kafka destroyed cleanly.");
		}
	}
	if (conf_) {
		rd_kafka_conf_destroy(conf_);
		conf_ = nullptr;
	}
	logger->log_debug("[Kafka Publisher] Kafka destructor finished.");
}

void KafkaPublisher::initialize() {
	const char *vendpoint = std::getenv("PUBLISHER_ENDPOINT");
	broker_ = vendpoint ? std::string(vendpoint) + ":9092" : "localhost:9092";
	logger->log_info("[Kafka Publisher] Using broker: " + broker_);

	char errstr[512];
	conf_ = rd_kafka_conf_new();

	rd_kafka_conf_set_log_cb(conf_, kafka_log_callback);
	rd_kafka_conf_set_dr_msg_cb(conf_, dr_msg_cb);
	rd_kafka_conf_set_opaque(conf_, static_cast<void *>(logger.get()));

	if (rd_kafka_conf_set(conf_, "bootstrap.servers", broker_.c_str(), errstr,
	                      sizeof(errstr))
	    != RD_KAFKA_CONF_OK) {
		throw std::runtime_error("Failed to set bootstrap.servers: "
		                         + std::string(errstr));
	}

	rd_kafka_conf_t *snapshot_conf = rd_kafka_conf_dup(conf_);

	if (!(producer_ = rd_kafka_new(RD_KAFKA_PRODUCER, snapshot_conf, errstr,
	                               sizeof(errstr)))) {
		throw std::runtime_error("Failed to create producer: "
		                         + std::string(errstr));
	}
	snapshot_conf = nullptr;

	log_configuration();
}

void KafkaPublisher::send_message(const Payload &message, std::string &topic) {
	const size_t serialized_size = sizeof(uint16_t) // Message ID Length
	    + message.message_id.size()                 // Message ID
	    + sizeof(uint8_t)                           // Kind
	    + sizeof(size_t)                            // Data size
	    + message.data_size;                        // Data
	std::string serialized(serialized_size, '\0');
	if (!Payload::serialize(message, serialized.data())) {
		logger->log_error(
		    "[Kafka Publisher] Serialization failed for message ID: "
		    + message.message_id);
		return;
	}

	rd_kafka_topic_t *topic_handle = get_or_create_topic_handle(topic);
	if (!topic_handle) {
		logger->log_error("[Kafka Publisher] Failed to obtain topic handle for "
		                  + topic);
		return;
	}

	// todo termination signal -> rd_kafka_topic_destroy?
	int err = rd_kafka_produce(
	    topic_handle,                                          // topic
	    RD_KAFKA_PARTITION_UA,                                 // partition
	    RD_KAFKA_MSG_F_COPY,                                   // copy payload
	    const_cast<char *>(serialized.data()),                 // payload ptr
	    serialized.size(),                                     // payload len
	    message.message_id.c_str(), message.message_id.size(), // key
	    nullptr                                                // msg_opaque
	);
	rd_kafka_poll(producer_, 0);

	if (err != 0) {
		logger->log_error(
		    "[Kafka Publisher] Produce failed: "
		    + std::string(rd_kafka_err2str(rd_kafka_last_error())));
	} else {
		logger->log_debug("[Kafka Publisher] Message queued for topic: "
		                  + topic);
	}
	// If message is end-of-stream, destroy the topic handle now
	if (message.message_id.find(TERMINATION_SIGNAL) != std::string::npos) {
		logger->log_info("[Kafka Publisher] Received termination signal — "
		                 "destroying topic handle for: "
		                 + topic);
		destroy_topic_handle(topic);
	}
}

inline rd_kafka_topic_t *
KafkaPublisher::get_or_create_topic_handle(const std::string &topic) {
	auto it = topic_handles_.find(topic);
	if (it != topic_handles_.end()) {
		return it->second;
	}

	rd_kafka_topic_t *handle =
	    rd_kafka_topic_new(producer_, topic.c_str(), nullptr);
	if (!handle) {
		logger->log_error(
		    "[Kafka Publisher] Failed to create topic handle for: " + topic);
		return nullptr;
	}

	topic_handles_[topic] = handle;
	logger->log_debug("[Kafka Publisher] Created new topic handle for: "
	                  + topic);
	return handle;
}

inline void KafkaPublisher::destroy_topic_handle(const std::string &topic) {
	auto it = topic_handles_.find(topic);
	if (it != topic_handles_.end()) {
		rd_kafka_topic_destroy(it->second);
		topic_handles_.erase(it);
		logger->log_debug("[Kafka Publisher] Destroyed topic handle for: "
		                  + topic);
	}
}

void KafkaPublisher::log_configuration() {
	size_t cnt;
	const char **conf = rd_kafka_conf_dump(conf_, &cnt);

	logger->log_config("[Kafka Publisher] [CONFIG_BEGIN]");
	for (size_t i = 0; i < cnt; i += 2) {
		logger->log_config("[CONFIG] " + std::string(conf[i]) + "="
		                   + std::string(conf[i + 1]));
	}
	logger->log_config("[Kafka Publisher] [CONFIG_END]");

	rd_kafka_conf_dump_free(conf, cnt);
}
