#include "KafkaConsumer.hpp"

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <sstream>
#include <utility>

KafkaConsumer::KafkaConsumer(std::shared_ptr<Logger> logger)
    : IConsumer(logger), consumer_(nullptr), conf_(nullptr),
      subscription_list_(nullptr), initialized_(false) {
	logger->log_info("[Kafka Consumer] KafkaConsumer created.");
}

KafkaConsumer::~KafkaConsumer() {
	if (consumer_) {
		rd_kafka_consumer_close(consumer_);
		rd_kafka_destroy_flags(consumer_, RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);
		// rd_kafka_destroy(consumer_);
		consumer_ = nullptr;
		// Wait up to 5 seconds for all Kafka threads to stop
		int remaining = rd_kafka_wait_destroyed(5000);
		if (remaining != 0) {
			logger->log_error("[Kafka Consumer] Kafka still has "
			                  + std::to_string(remaining)
			                  + " references after destroy.");
		} else {
			logger->log_debug("[Kafka Consumer] Kafka destroyed cleanly.");
		}
	}

	if (conf_) {
		rd_kafka_conf_destroy(conf_);
		conf_ = nullptr;
	}

	if (subscription_list_) {
		rd_kafka_topic_partition_list_destroy(subscription_list_);
	}
	logger->log_debug("[Kafka Consumer] Destructor finished");
}

void KafkaConsumer::subscribe(const std::string &topic) {
	if (initialized_) {
		logger->log_error("[Kafka Consumer] Cannot subscribe to new topics "
		                  "after initialization.");
		return;
	}

	if (subscribed_streams.emplace(topic, "default").second) {
		logger->log_info("[Kafka Consumer] Queued subscription for topic: "
		                 + topic);
		topic_names_.insert(topic);
	}
}

void KafkaConsumer::initialize() {
	logger->log_study("Initializing");
	if (initialized_) {
		logger->log_error(
		    "[Kafka Consumer] Kafka Consumer already initialized.");
		return;
	}
	std::string consumer_id = std::getenv("CONTAINER_ID");
	const char *vendpoint = std::getenv("CONSUMER_ENDPOINT");
	broker_ = vendpoint ? std::string(vendpoint) + ":9092" : "localhost:9092";

	logger->log_info("[Kafka Consumer] Using broker: " + broker_);

	char errstr[512];
	conf_ = rd_kafka_conf_new();

	if (rd_kafka_conf_set(conf_, "bootstrap.servers", broker_.c_str(), errstr,
	                      sizeof(errstr))
	    != RD_KAFKA_CONF_OK) {
		throw std::runtime_error(
		    "[Kafka Consumer] Failed to set bootstrap.servers: "
		    + std::string(errstr));
	}
	if (rd_kafka_conf_set(conf_, "group.id", "benchmark_group", errstr,
	                      sizeof(errstr))
	    != RD_KAFKA_CONF_OK) {
		throw std::runtime_error("[Kafka Consumer] Failed to set group.id: "
		                         + std::string(errstr));
	}
	if (rd_kafka_conf_set(conf_, "enable.auto.commit", "false", errstr,
	                      sizeof(errstr))
	    != RD_KAFKA_CONF_OK) {
		throw std::runtime_error(
		    "[Kafka Consumer] Failed to set enable.auto.commit: "
		    + std::string(errstr));
	}
	if (rd_kafka_conf_set(conf_, "auto.offset.reset", "earliest", errstr,
	                      sizeof(errstr))
	    != RD_KAFKA_CONF_OK) {
		throw std::runtime_error(
		    "[Kafka Consumer] Failed to set auto.offset.reset: "
		    + std::string(errstr));
	}
	rd_kafka_conf_t *snapshot_conf = rd_kafka_conf_dup(conf_);

	if (!(consumer_ = rd_kafka_new(RD_KAFKA_CONSUMER, snapshot_conf, errstr,
	                               sizeof(errstr)))) {
		throw std::runtime_error("[Kafka Consumer] Failed to create consumer: "
		                         + std::string(errstr));
	}
	// rd_kafka_conf_destroy(conf_);
	snapshot_conf = nullptr;

	rd_kafka_poll_set_consumer(consumer_);

	const char *vtopics = std::getenv("TOPICS");
	if (!vtopics) {
		subscribed_streams.insert({broker_, consumer_id.substr(1)});
		subscribe(consumer_id.substr(1));
		logger->log_debug("[Kafka Consumer] TOPICS not set, default to topic "
		                  "with same numerical id: "
		                  + consumer_id.substr(1));
	} else {
		std::istringstream topics(vtopics);
		std::string topic;
		while (std::getline(topics, topic, ',')) {
			logger->log_debug("[Kafka Consumer] Handling subscription to topic "
			                  + topic);
			if (!topic.empty()) {
				logger->log_info("[Kafka Consumer] Connecting to stream ("
				                 + broker_ + "," + topic + ")");
				subscribe(topic);
			}
		}
	}

	logger->log_debug("[KafkaConsumer] Subscription list will have size "
	                  + std::to_string(static_cast<int>(topic_names_.size())));
	subscription_list_ = rd_kafka_topic_partition_list_new(
	    static_cast<int>(topic_names_.size()));
	for (const auto &topic : topic_names_) {
		rd_kafka_topic_partition_list_add(subscription_list_, topic.c_str(),
		                                  -1);
		logger->log_debug("[Kafka Consumer] Prepared subscription to topic: "
		                  + topic);
	}

	if (rd_kafka_subscribe(consumer_, subscription_list_)
	    != RD_KAFKA_RESP_ERR_NO_ERROR) {
		throw std::runtime_error(
		    "[Kafka Consumer] Failed to subscribe to Kafka topics.");
	}

	rd_kafka_topic_partition_list_destroy(subscription_list_);
	subscription_list_ = nullptr;

	initialized_ = true;

	logger->log_info("[Kafka Consumer] Consumer initialized and subscribed.");
	logger->log_study("Initialized");
	log_configuration();
}

bool KafkaConsumer::deserialize(const void *raw_message, size_t len,
                                Payload &out) {
	const char *data = static_cast<const char *>(raw_message);
	size_t offset = 0;

	// Message ID Length
	uint16_t id_len;
	std::memcpy(&id_len, data + offset, sizeof(id_len));
	offset += sizeof(id_len);

	// Message ID
	std::string message_id(data + offset, id_len);
	offset += id_len;

	// Kind
	uint8_t kind_byte;
	std::memcpy(&kind_byte, data + offset, sizeof(uint8_t));
	PayloadKind kind_payload = static_cast<PayloadKind>(kind_byte);
	offset += sizeof(uint8_t);

	// Data size
	size_t data_size;
	std::memcpy(&data_size, data + offset, sizeof(size_t));
	offset += sizeof(size_t);

	if (len != offset + data_size) {
		logger->log_error(
		    "[Kafka Consumer] Deserialization error: message length ("
		    + std::to_string(len) + ") does not match expected size ("
		    + std::to_string(offset + data_size) + ")");
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

Payload KafkaConsumer::receive_message() {
	logger->log_debug("[Kafka Consumer] Polling for messages...");
	rd_kafka_message_t *msg = rd_kafka_consumer_poll(consumer_, 2000);
	if (!msg) {
		logger->log_debug("[Kafka Consumer] Poll returned null message");
		return {};
	}
	std::string topic = msg->rkt ? rd_kafka_topic_name(msg->rkt) : "unknown";

	Payload payload = {};
	if (msg->err) {
		logger->log_error("[Kafka Consumer] Kafka error: "
		                  + std::string(rd_kafka_message_errstr(msg)));
	} else if (msg->len > 0) {
		logger->log_info("[Kafka Consumer] Received message on topic '" + topic
		                 + "' with " + std::to_string(msg->len) + " bytes");
		try {
			// COPY from message buffer BEFORE destroying
			if (deserialize(msg->payload, msg->len, payload) == false) {
				logger->log_error("[Kafka Consumer] Deserialization failed for "
				                  "message on topic: "
				                  + topic);
				rd_kafka_message_destroy(msg);
				return {};
			}
		} catch (const std::exception &e) {
			logger->log_error("[Kafka Consumer] Failed to deserialize payload: "
			                  + std::string(e.what()));
		}

		if (payload.message_id.find(TERMINATION_SIGNAL) != std::string::npos) {
			terminated_streams.insert({broker_, topic});
			logger->log_info("[Kafka Consumer] Received termination for topic: "
			                 + topic);
			logger->log_debug("[Kafka Consumer] Streams closed: "
			                  + std::to_string(terminated_streams.size()) + "/"
			                  + std::to_string(subscribed_streams.size()));
			// rd_kafka_message_destroy(msg);
			payload = Payload::make(
			    payload.message_id.substr(0, payload.message_id.find(':')) + "-"
			        + topic,
			    0, 0, PayloadKind::TERMINATION);
		}
		logger->log_study("Reception," + payload.message_id + ","
		                  + std::to_string(payload.data_size) + "," + topic
		                  + "," + std::to_string(msg->len));
	} else {
		logger->log_error("[Kafka Consumer] Unknown msg handling condition");
	}

	rd_kafka_message_destroy(msg);
	return payload;
}

void KafkaConsumer::log_configuration() {
	size_t cnt;
	const char **conf = rd_kafka_conf_dump(conf_, &cnt);
	logger->log_config("[Kafka Consumer] [CONFIG_BEGIN]");
	for (size_t i = 0; i < cnt; i += 2) {
		logger->log_config("[CONFIG] " + std::string(conf[i]) + " = "
		                   + std::string(conf[i + 1]));
	}
	logger->log_config("[Kafka Consumer] [CONFIG_END]");

	rd_kafka_conf_dump_free(conf, cnt);
}
