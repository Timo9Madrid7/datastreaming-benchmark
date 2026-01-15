#include "KafkaCppConsumer.hpp"

#include <librdkafka/rdkafkacpp.h>
#include <memory>
#include <optional>
#include <sstream>
#include <unordered_set>
#include <utility>

#include "Logger.hpp"
#include "Payload.hpp"
#include "Utils.hpp"

KafkaCppConsumer::KafkaCppConsumer(std::shared_ptr<Logger> logger)
    : IConsumer(logger), consumer_(nullptr), conf_(nullptr),
      deserializer_(logger) {
	logger->log_info("[Kafka Consumer] KafkaConsumer created.");
}

KafkaCppConsumer::~KafkaCppConsumer() {
	if (consumer_) {
		RdKafka::ErrorCode err = consumer_->unsubscribe();
		if (err != RdKafka::ERR_NO_ERROR) {
			logger->log_error("[Kafka Consumer] Failed to unsubscribe: "
			                  + RdKafka::err2str(err));
		}

		stop_receiving_ = true;
		deserializer_.stop();

		consumer_->close();
		consumer_.reset();
		logger->log_debug("[Kafka Consumer] Kafka consumer closed.");
	}
	logger->log_debug("[Kafka Consumer] Destructor finished");
}

void KafkaCppConsumer::initialize() {
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

	conf_.reset(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
	if (!conf_) {
		err_msg = "[Kafka Consumer] Failed to create global config.";
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	}

	if (conf_->set("bootstrap.servers", broker_, err_msg)
	    != RdKafka::Conf::CONF_OK) {
		logger->log_error("[Kafka Consumer] Failed to set bootstrap.servers: "
		                  + err_msg);
		throw std::runtime_error("Failed to set bootstrap.servers: " + err_msg);
	}

	if (conf_->set("group.id", group_id, err_msg) != RdKafka::Conf::CONF_OK) {
		logger->log_error("[Kafka Consumer] Failed to set group.id: "
		                  + err_msg);
		throw std::runtime_error("Failed to set group.id: " + err_msg);
	}

	if (conf_->set("enable.auto.commit", "false", err_msg)
	    != RdKafka::Conf::CONF_OK) {
		logger->log_error("[Kafka Consumer] Failed to set enable.auto.commit: "
		                  + err_msg);
		throw std::runtime_error("Failed to set enable.auto.commit: "
		                         + err_msg);
	}

	if (conf_->set("auto.offset.reset", "earliest", err_msg)
	    != RdKafka::Conf::CONF_OK) {
		logger->log_error("[Kafka Consumer] Failed to set auto.offset.reset: "
		                  + err_msg);
		throw std::runtime_error("Failed to set auto.offset.reset: " + err_msg);
	}

	// Low Latency Configuration
	conf_->set("fetch.min.bytes", "1048576", err_msg);
	conf_->set("fetch.wait.max.ms", "20", err_msg);
	conf_->set("max.partition.fetch.bytes", "8388608", err_msg);
	conf_->set("fetch.message.max.bytes", "8388608", err_msg);
	conf_->set("queued.max.messages.kbytes", "4194304", err_msg);

	consumer_.reset(RdKafka::KafkaConsumer::create(conf_.get(), err_msg));

	if (!consumer_) {
		logger->log_error("[Kafka Consumer] Failed to create consumer: "
		                  + err_msg);
		throw std::runtime_error("Failed to create consumer: " + err_msg);
	}

	std::istringstream topics(vtopics.value_or(""));
	std::string topic;
	std::unordered_set<std::string> unique_topics;
	while (std::getline(topics, topic, ',')) {
		logger->log_debug("[Kafka Consumer] Handling subscription to topic "
		                  + topic);
		if (!topic.empty() && unique_topics.insert(topic).second) {
			logger->log_info("[Kafka Consumer] Connecting to stream (" + broker_
			                 + "," + topic + ")");
			subscribe(topic);
		} else {
			logger->log_debug(
			    "[Kafka Consumer] Skipping empty or duplicate topic.");
		}
	}

	logger->log_debug("[Kafka Consumer] Subscription list will have size "
	                  + std::to_string(topic_names_.size()));
	RdKafka::ErrorCode err = consumer_->subscribe(topic_names_);
	if (err != RdKafka::ERR_NO_ERROR) {
		logger->log_error(
		    "[Kafka Consumer] Failed to subscribe to Kafka topics: "
		    + RdKafka::err2str(err));
		throw std::runtime_error("Failed to subscribe to Kafka topics: "
		                         + RdKafka::err2str(err));
	}

	logger->log_info("[Kafka Consumer] Consumer initialized and subscribed.");
	log_configuration();
}

void KafkaCppConsumer::subscribe(const std::string &topic) {
	logger->log_info("[Kafka Consumer] Queued subscription for topic: "
	                 + topic);
	topic_names_.push_back(topic);
	subscribed_streams.inc();
}

void KafkaCppConsumer::start_loop() {
	deserializer_.start(
	    [](const void *data, size_t len, std::string & /*topic*/,
	       Payload &out) { return Payload::deserialize(data, len, out); },
	    [this](const Payload &payload, const std::string & /*topic*/,
	           size_t /*raw_len*/) {
		    if (payload.kind == PayloadKind::TERMINATION) {
			    subscribed_streams.dec();
			    logger->log_info("[Kafka Consumer] Termination signal received "
			                     "for message ID: "
			                     + payload.message_id);
			    if (subscribed_streams.get() == 0) {
				    logger->log_info(
				        "[Kafka Consumer] All streams terminated. Exiting.");
				    stop_receiving_ = true;
			    }
			    logger->log_info(
			        "[Kafka Consumer] Remaining subscribed streams: "
			        + std::to_string(subscribed_streams.get()));
		    }
	    });

	while (!stop_receiving_) {
		logger->log_debug("[Kafka Consumer] Polling for messages...");
		std::unique_ptr<RdKafka::Message> msg(consumer_->consume(2000));

		if (!msg) {
			logger->log_debug("[Kafka Consumer] Poll returned null message");
			continue;
		}

		if (msg->err() == RdKafka::ERR__TIMED_OUT) {
			logger->log_debug("[Kafka Consumer] Poll timed out with no "
			                  "messages available.");
			continue;
		}

		if (msg->err() != RdKafka::ERR_NO_ERROR) {
			logger->log_error("[Kafka Consumer] Error while consuming message: "
			                  + msg->errstr());
			continue;
		}

		std::string topic =
		    !msg->topic_name().empty() ? msg->topic_name() : "unknown";

		if (msg->len() == 0) {
			logger->log_debug(
			    "[Kafka Consumer] Received empty message on topic: " + topic);
			continue;
		}

		Payload payload;
		logger->log_info("[Kafka Consumer] Received message on topic '" + topic
		                 + "' with " + std::to_string(msg->len()) + " bytes");

		if (!Payload::deserialize_id(msg->payload(), msg->len(), payload)) {
			logger->log_error("[Kafka Consumer] Deserialization failed for "
			                  "message on topic: "
			                  + topic);
			continue;
		}

		logger->log_study("Reception," + payload.message_id + "," + topic);

		RdKafka::Message *raw = msg.release();
		utils::Deserializer::Item item;
		item.holder = std::shared_ptr<void>(
		    raw, [](void *p) { delete static_cast<RdKafka::Message *>(p); });
		item.data = raw->payload();
		item.len = static_cast<size_t>(raw->len());
		item.topic = topic;
		item.message_id = payload.message_id;
		if (!deserializer_.enqueue(std::move(item))) {
			logger->log_error(
			    "[Kafka Consumer] Deserializer queue full; dropping message.");
		}
	}

	deserializer_.stop();
}

void KafkaCppConsumer::log_configuration() {
	std::unique_ptr<std::list<std::string>> dump(conf_->dump());

	logger->log_config("[Kafka Consumer] [CONFIG_BEGIN]");
	for (auto it = dump->begin(); it != dump->end();) {
		logger->log_config("[CONFIG] " + *it++ + "=" + *it++);
	}
	logger->log_config("[Kafka Consumer] [CONFIG_END]");
}

// Deserialization worker is implemented by utils::Deserializer