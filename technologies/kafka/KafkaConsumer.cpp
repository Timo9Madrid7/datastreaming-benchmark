#include "KafkaConsumer.hpp"

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <sstream>
#include <utility>

#include "Utils.hpp"

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
	if (topic_names_.insert(topic).second) {
		logger->log_info("[Kafka Consumer] Subscribing to topic: " + topic);
		subscribed_streams.inc();
	} else {
		logger->log_debug("[Kafka Consumer] Already subscribed to topic: "
		                  + topic);
	}
}

void KafkaConsumer::initialize() {
	if (initialized_) {
		logger->log_error(
		    "[Kafka Consumer] Kafka Consumer already initialized.");
		return;
	}

	const std::string vendpoint =
	    utils::get_env_var_or_default("CONSUMER_ENDPOINT", "localhost");
	const std::string port =
	    utils::get_env_var_or_default("CONSUMER_PORT", "9092");
	broker_ = vendpoint + ":" + port;
	logger->log_info("[Kafka Consumer] Using broker: " + broker_);

	const std::optional<std::string> consumer_id =
	    utils::get_env_var("CONTAINER_ID");
	const std::optional<std::string> vtopics = utils::get_env_var("TOPICS");
	std::string err_msg;
	if (!consumer_id) {
		err_msg = "[Kafka Consumer] Missing required environment variable "
		          "CONTAINER_ID.";
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	}
	if (!vtopics || vtopics.value().empty()) {
		err_msg =
		    "[Kafka Consumer] Missing required environment variable TOPICS.";
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	}

	const std::string group_id = "benchmark_group_" + consumer_id.value();

	char errstr[512];
	conf_ = rd_kafka_conf_new();

	if (rd_kafka_conf_set(conf_, "bootstrap.servers", broker_.c_str(), errstr,
	                      sizeof(errstr))
	    != RD_KAFKA_CONF_OK) {
		throw std::runtime_error(
		    "[Kafka Consumer] Failed to set bootstrap.servers: "
		    + std::string(errstr));
	}
	if (rd_kafka_conf_set(conf_, "group.id", group_id.c_str(), errstr,
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

	std::istringstream topics(vtopics.value_or(""));
	std::string topic;
	while (std::getline(topics, topic, ',')) {
		logger->log_debug("[Kafka Consumer] Handling subscription to topic "
		                  + topic);
		if (!topic.empty()) {
			logger->log_info("[Kafka Consumer] Connecting to stream (" + broker_
			                 + "," + topic + ")");
			subscribe(topic);
		}
	}

	logger->log_debug("[KafkaConsumer] Subscription list will have size "
	                  + std::to_string(subscribed_streams.get()));
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

bool KafkaConsumer::deserialize_id(const void *raw_message, size_t len,
                                   Payload &out) {
	const char *data = static_cast<const char *>(raw_message);
	size_t offset = 0;

	// Message ID Length
	uint16_t id_len;
	std::memcpy(&id_len, data + offset, sizeof(id_len));
	offset += sizeof(id_len);

	// Message ID
	std::string message_id(data + offset, id_len);

	out.message_id = message_id;
	out.data_size = 0;

	return true;
}

void KafkaConsumer::start_loop() {
	while (true) {
		logger->log_debug("[Kafka Consumer] Polling for messages...");
		rd_kafka_message_t *msg = rd_kafka_consumer_poll(consumer_, 2000);

		if (!msg) {
			logger->log_debug("[Kafka Consumer] Poll returned null message");
			continue;
		}

		if (msg->err) {
			logger->log_debug("[Kafka Consumer] Kafka error: "
			                  + std::string(rd_kafka_message_errstr(msg)));
			rd_kafka_message_destroy(msg);
			continue;
		}

		std::string topic =
		    msg->rkt ? rd_kafka_topic_name(msg->rkt) : "unknown";

		if (msg->len == 0) {
			logger->log_debug(
			    "[Kafka Consumer] Received empty message on topic: " + topic);
			rd_kafka_message_destroy(msg);
			continue;
		}

		Payload payload = {};
		logger->log_info("[Kafka Consumer] Received message on topic '" + topic
		                 + "' with " + std::to_string(msg->len) + " bytes");

		if (
		    // !deserialize(msg->payload, msg->len, payload)
		    !deserialize_id(msg->payload, msg->len, payload)) {
			logger->log_error("[Kafka Consumer] Deserialization failed for "
			                  "message on topic: "
			                  + topic);
			rd_kafka_message_destroy(msg);
			continue;
		}

		logger->log_study("Reception," + payload.message_id + ",-1," + topic
		                  + "," + std::to_string(msg->len));
		rd_kafka_message_destroy(msg);

		if (payload.message_id.find(TERMINATION_SIGNAL) != std::string::npos) {
			subscribed_streams.dec();

			logger->log_info(
			    "[Kafka Consumer] Received termination signal for topic: "
			    + topic);

			if (subscribed_streams.get() == 0) {
				logger->log_info("[Kafka Consumer] All streams "
				                 "terminated. Exiting.");
				break;
			}

			logger->log_info("[Kafka Consumer] Remaining subscribed streams: "
			                 + std::to_string(subscribed_streams.get()));
		}
	}
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
