#include "KafkaCppConsumer.hpp"

#include <chrono>
#include <librdkafka/rdkafkacpp.h>
#include <memory>
#include <optional>
#include <sstream>
#include <thread>
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
	// Support both a single host and a comma-separated host list.
	// librdkafka expects: host:port,host:port,...
	auto build_bootstrap_servers = [](const std::string &endpoints,
	                                  const std::string &default_port) {
		std::string out;
		std::string token;
		std::istringstream iss(endpoints);
		bool first = true;
		while (std::getline(iss, token, ',')) {
			auto start = token.find_first_not_of(" \t");
			if (start == std::string::npos) {
				continue;
			}
			auto end = token.find_last_not_of(" \t");
			std::string host = token.substr(start, end - start + 1);
			if (host.empty()) {
				continue;
			}
			if (!first) {
				out += ",";
			}
			first = false;
			// If already has a port, keep it.
			if (host.find(':') != std::string::npos) {
				out += host;
			} else {
				out += host + ":" + default_port;
			}
		}
		return out;
	};

	broker_ = build_bootstrap_servers(vendpoint, port);
	if (broker_.empty()) {
		broker_ = std::string("localhost:") + port;
	}
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

	// If multiple consumer containers should split the stream, they must share
	// the same consumer group. Default keeps backwards behavior (one group per
	// container => each consumer receives the full stream).
	const std::optional<std::string> env_group_id =
	    utils::get_env_var("KAFKA_GROUP_ID");
	const std::string group_id = (env_group_id && !env_group_id->empty())
	    ? env_group_id.value()
	    : ("benchmark_group_" + consumer_id.value());
	logger->log_info("[Kafka Consumer] Using group.id: " + group_id);

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

	// Startup robustness: in this benchmark the broker and consumers are
	// unpaused simultaneously. If the consumer attempts to connect before the
	// broker is ready, librdkafka's reconnect backoff can grow to seconds,
	// inflating Publication->Reception latency. Keep backoff tight.
	conf_->set("reconnect.backoff.ms", "50", err_msg);
	conf_->set("reconnect.backoff.max.ms", "500", err_msg);

	// Latency-focused fetch settings for small payloads.
	// The previous fetch.min.bytes=1MB can add significant buffering delay when
	// payloads are ~1KB and rates are modest.
	conf_->set("fetch.min.bytes", "1", err_msg);
	conf_->set("fetch.wait.max.ms", "5", err_msg);

	// Support larger messages if needed (up to 16MB here).
	conf_->set("max.partition.fetch.bytes", "16777216", err_msg);
	conf_->set("fetch.message.max.bytes", "16777216", err_msg);
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

	// Ensure the server is running and all topics exist before subscribing.
	{
		using namespace std::chrono_literals;
		auto deadline = std::chrono::steady_clock::now() + 30s;
		bool all_present = false;
		while (std::chrono::steady_clock::now() < deadline) {
			RdKafka::Metadata *metadata = nullptr;
			RdKafka::ErrorCode ec =
			    consumer_->metadata(true, nullptr, &metadata, 2000);
			all_present = true;
			if (ec != RdKafka::ERR_NO_ERROR || metadata == nullptr) {
				all_present = false;
			} else {
				// Build a set of topics present in metadata.
				std::unordered_set<std::string> present;
				present.reserve(metadata->topics()->size());
				for (const auto *t : *(metadata->topics())) {
					if (t == nullptr) {
						continue;
					}
					// Some topics might exist but still have errors during
					// creation; treat those as not ready yet.
					if (t->err() == RdKafka::ERR_NO_ERROR
					    && !t->topic().empty()) {
						present.insert(t->topic());
					}
				}
				for (const auto &name : topic_names_) {
					if (present.find(name) == present.end()) {
						all_present = false;
						break;
					}
				}
			}
			if (metadata != nullptr) {
				delete metadata;
				metadata = nullptr;
			}
			if (all_present) {
				break;
			}

			auto now = std::chrono::steady_clock::now();
			std::this_thread::sleep_for(200ms);
		}
		if (!all_present) {
			std::string err_msg = "[Kafka Consumer] Timed out waiting for "
			                      "topics to exist on broker "
			                      "(broker='"
			    + broker_
			    + "'). "
			      "This usually means the broker didn't create the topics yet, "
			      "or the consumer was pointed at a different broker.";
			logger->log_error(err_msg);
			throw std::runtime_error(err_msg);
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
		std::unique_ptr<RdKafka::Message> msg(consumer_->consume(50));

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

		std::string message_id;
		if (const std::string *key_ptr = msg->key(); key_ptr != nullptr) {
			message_id = *key_ptr;
		}
		if (message_id.empty()) {
			Payload id_payload;
			if (!Payload::deserialize_id(msg->payload(), msg->len(),
			                             id_payload)) {
				logger->log_error("[Kafka Consumer] Deserialization failed for "
				                  "message on topic: "
				                  + topic);
				continue;
			}
			message_id = id_payload.message_id;
		}

		logger->log_debug("[Kafka Consumer] Received message on topic '" + topic
		                  + "' with " + std::to_string(msg->len()) + " bytes");
		logger->log_study("Reception," + message_id + "," + topic);

		RdKafka::Message *raw = msg.release();
		utils::Deserializer::Item item;
		item.holder = std::shared_ptr<void>(
		    raw, [](void *p) { delete static_cast<RdKafka::Message *>(p); });
		item.data = raw->payload();
		item.len = static_cast<size_t>(raw->len());
		item.topic = topic;
		item.message_id = message_id;
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