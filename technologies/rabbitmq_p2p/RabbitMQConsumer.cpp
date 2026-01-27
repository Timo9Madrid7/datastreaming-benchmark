#include "RabbitMQConsumer.hpp"

#include <chrono>
#include <event2/event.h>
#include <event2/thread.h>
#include <sstream>
#include <stdexcept>
#include <unordered_set>

#include "Logger.hpp"
#include "Payload.hpp"
#include "Utils.hpp"

RabbitMQConsumer::RabbitMQConsumer(std::shared_ptr<Logger> logger)
    : IConsumer(logger), deserializer_(logger) {
	logger->log_info("[RabbitMQ Consumer] RabbitMQConsumer created.");
}

RabbitMQConsumer::~RabbitMQConsumer() {
	logger->log_debug("[RabbitMQ Consumer] Cleaning up RabbitMQ consumer...");
	stop_receiving_ = true;
	deserializer_.stop();
	stop_event_loop_();
	channel_.reset();
	connection_.reset();
	handler_.reset();
	logger->log_debug("[RabbitMQ Consumer] Destructor finished.");
}

void RabbitMQConsumer::initialize() {
	static std::once_flag libevent_threading_once;
	std::call_once(libevent_threading_once, []() {
		if (evthread_use_pthreads() != 0) {
			throw std::runtime_error("[RabbitMQ Consumer] Failed to enable "
			                         "libevent pthread support.");
		}
	});

	const auto vtopics = utils::get_env_var("TOPICS");
	if (!vtopics || vtopics->empty()) {
		throw std::runtime_error("[RabbitMQ Consumer] Missing required "
		                         "environment variable TOPICS.");
	}

	const std::string port =
	    utils::get_env_var_or_default("CONSUMER_PORT", "5672");
	const std::string vendpoints = utils::get_env_var_or_default(
	    "PUBLISHER_ENDPOINTS",
	    utils::get_env_var_or_default("CONSUMER_ENDPOINT", "localhost"));
	std::string endpoint;
	std::istringstream endpoints_stream(vendpoints);
	std::getline(endpoints_stream, endpoint, ',');
	if (endpoint.empty()) {
		throw std::runtime_error(
		    "[RabbitMQ Consumer] CONSUMER_ENDPOINT is empty.");
	}

	const auto vurl = utils::get_env_var("RABBITMQ_URL");
	if (vurl && !vurl->empty()) {
		amqp_url_ = vurl.value();
	} else {
		amqp_url_ = build_amqp_url_(endpoint, port);
	}

	exchange_ = utils::get_env_var_or_default("RABBITMQ_EXCHANGE",
	                                          "benchmark_exchange");

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
		throw std::runtime_error(
		    "[RabbitMQ Consumer] No valid topics provided.");
	}

	event_base_ = event_base_new();
	if (!event_base_) {
		throw std::runtime_error(
		    "[RabbitMQ Consumer] Failed to create libevent base.");
	}

	handler_ = std::make_unique<AMQP::LibEventHandler>(event_base_);
	connection_ = std::make_unique<AMQP::TcpConnection>(
	    handler_.get(), AMQP::Address(amqp_url_));

	channel_ = std::make_unique<AMQP::TcpChannel>(connection_.get());
	channel_->onError([this](const char *msg) {
		logger->log_error(std::string("[RabbitMQ Consumer] Channel error: ")
		                  + msg);
	});

	start_event_loop_();

	constexpr int kMaxAttempts = 60; // 60 * 500ms = 30s
	bool channel_ready = false;
	for (int attempt = 1; attempt <= kMaxAttempts; ++attempt) {
		channel_ready = channel_->ready();
		if (channel_ready) {
			break;
		}
		logger->log_info(
		    "[RabbitMQ Consumer] Connect attempt " + std::to_string(attempt)
		    + "/" + std::to_string(kMaxAttempts) + " failed to " + amqp_url_);
		std::this_thread::sleep_for(std::chrono::milliseconds(500));
	}
	if (!channel_ready) {
		throw std::runtime_error("[RabbitMQ Consumer] Failed to connect to "
		                         + amqp_url_);
	}

	channel_->declareExchange(exchange_, AMQP::direct)
	    .onSuccess([this]() {
		    {
			    std::lock_guard<std::mutex> lock(ready_mu_);
			    ready_.store(true, std::memory_order_release);
		    }
		    ready_cv_.notify_all();
		    logger->log_info("[RabbitMQ Consumer] Exchange declared: "
		                     + exchange_);
	    })
	    .onError([this](const char *msg) {
		    logger->log_error(
		        std::string("[RabbitMQ Consumer] Exchange declare error: ")
		        + msg);
	    });

	if (!wait_ready_(30000)) {
		throw std::runtime_error(
		    "[RabbitMQ Consumer] Timed out waiting for channel ready.");
	}

	for (const auto &t : topics_) {
		setup_topic_queue_(t);
	}

	logger->log_info("[RabbitMQ Consumer] Consumer initialized.");
	log_configuration();
}

void RabbitMQConsumer::subscribe(const std::string &topic) {
	logger->log_info("[RabbitMQ Consumer] Registering topic: " + topic);
	topics_.push_back(topic);
	subscribed_streams.inc();
}

void RabbitMQConsumer::start_loop() {
	deserializer_.start(
	    [](const void *data, size_t len, std::string & /*topic*/,
	       Payload &out) { return Payload::deserialize(data, len, out); },
	    [this](const Payload &payload, const std::string & /*topic*/,
	           size_t /*raw_len*/) {
		    if (payload.kind == PayloadKind::TERMINATION) {
			    subscribed_streams.dec();
			    logger->log_info(
			        "[RabbitMQ Consumer] Termination signal received "
			        "for message ID: "
			        + payload.message_id);
			    if (subscribed_streams.get() == 0) {
				    logger->log_info(
				        "[RabbitMQ Consumer] All streams terminated. Exiting.");
				    stop_receiving_ = true;
			    }
			    logger->log_info(
			        "[RabbitMQ Consumer] Remaining subscribed streams: "
			        + std::to_string(subscribed_streams.get()));
		    }
	    });

	while (!stop_receiving_) {
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
	}

	deserializer_.stop();
	stop_event_loop_();
}

void RabbitMQConsumer::log_configuration() {
	logger->log_config("[RabbitMQ Consumer] [CONFIG_BEGIN]");
	logger->log_config("[CONFIG] AMQP_URL=" + amqp_url_);
	logger->log_config("[CONFIG] EXCHANGE=" + exchange_);
	logger->log_config("[CONFIG] TOPICS="
	                   + utils::get_env_var_or_default("TOPICS", ""));
	for (const auto &kv : topic_queues_) {
		logger->log_config("[CONFIG] QUEUE[" + kv.first + "]=" + kv.second);
	}
	logger->log_config("[RabbitMQ Consumer] [CONFIG_END]");
}

void RabbitMQConsumer::start_event_loop_() {
	if (!event_base_) {
		return;
	}
	io_thread_ = std::thread([this]() { event_base_dispatch(event_base_); });
}

void RabbitMQConsumer::stop_event_loop_() {
	logger->log_debug("[RabbitMQ Consumer] Stopping event loop...");
	if (event_base_) {
		event_base_loopbreak(event_base_);
	}
	if (io_thread_.joinable()) {
		io_thread_.join();
	}
	if (event_base_) {
		event_base_free(event_base_);
		event_base_ = nullptr;
	}
	logger->log_debug("[RabbitMQ Consumer] Event loop stopped.");
}

bool RabbitMQConsumer::wait_ready_(int timeout_ms) {
	std::unique_lock<std::mutex> lock(ready_mu_);
	if (ready_.load(std::memory_order_acquire)) {
		return true;
	}
	return ready_cv_.wait_for(
	    lock, std::chrono::milliseconds(timeout_ms),
	    [this]() { return ready_.load(std::memory_order_acquire); });
}

std::string RabbitMQConsumer::build_amqp_url_(const std::string &endpoint,
                                              const std::string &port) const {
	const std::string user =
	    utils::get_env_var_or_default("RABBITMQ_USER", "guest");
	const std::string password =
	    utils::get_env_var_or_default("RABBITMQ_PASSWORD", "guest");
	std::string vhost = utils::get_env_var_or_default("RABBITMQ_VHOST", "/");
	if (vhost.empty() || vhost[0] != '/') {
		vhost = "/" + vhost;
	}
	return "amqp://" + user + ":" + password + "@" + endpoint + ":" + port
	    + vhost;
}

void RabbitMQConsumer::setup_topic_queue_(const std::string &topic) {
	if (!channel_) {
		return;
	}

	channel_->declareQueue("", AMQP::exclusive)
	    .onSuccess([this, topic](const std::string &name, uint32_t, uint32_t) {
		    topic_queues_[topic] = name;
		    channel_->bindQueue(exchange_, name, topic)
		        .onSuccess([this, topic, name]() {
			        channel_->consume(name, AMQP::noack)
			            .onReceived([this, topic](const AMQP::Message &message,
			                                      uint64_t, bool) {
				            const size_t len = message.bodySize();
				            auto holder = std::make_shared<std::string>(
				                message.body(), message.body() + len);
				            Payload payload;
				            if (!Payload::deserialize_id(
				                    holder->data(), holder->size(), payload)) {
					            logger->log_error("[RabbitMQ Consumer] "
					                              "Deserialization failed.");
					            return;
				            }

				            logger->log_study("Reception," + payload.message_id
				                              + "," + topic);

				            utils::Deserializer::Item item;
				            item.holder = holder;
				            item.data = holder->data();
				            item.len = holder->size();
				            item.topic = topic;
				            item.message_id = payload.message_id;
				            if (!deserializer_.enqueue(std::move(item))) {
					            logger->log_error("[RabbitMQ Consumer] "
					                              "Deserializer queue full; "
					                              "dropping message.");
				            }
			            })
			            .onSuccess([this, topic](const std::string &tag) {
				            logger->log_info(
				                "[RabbitMQ Consumer] Consuming topic=" + topic
				                + " tag=" + tag);
			            })
			            .onError([this, topic](const char *msg) {
				            logger->log_error(
				                std::string(
				                    "[RabbitMQ Consumer] Consume error (topic="
				                    + topic + "): ")
				                + msg);
			            });
		        })
		        .onError([this, topic](const char *msg) {
			        logger->log_error(
			            std::string("[RabbitMQ Consumer] Bind error (topic="
			                        + topic + "): ")
			            + msg);
		        });
	    })
	    .onError([this, topic](const char *msg) {
		    logger->log_error(
		        std::string("[RabbitMQ Consumer] Queue declare error (topic="
		                    + topic + "): ")
		        + msg);
	    });
}
