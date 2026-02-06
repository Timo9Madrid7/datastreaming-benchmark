#include "RabbitMQPublisher.hpp"

#include <amqpcpp/linux_tcp/tcpconnection.h>
#include <chrono>
#include <event2/event.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

#include "Logger.hpp"
#include "Payload.hpp"
#include "Utils.hpp"

RabbitMQPublisher::RabbitMQPublisher(std::shared_ptr<Logger> logger)
    : IPublisher(logger) {
	logger->log_info("[RabbitMQ Publisher] RabbitMQPublisher created.");
}

RabbitMQPublisher::~RabbitMQPublisher() {
	logger->log_debug("[RabbitMQ Publisher] Cleaning up RabbitMQ publisher...");
	while (connection_->queued() > 0
	       || !terminated_.load(std::memory_order_acquire)) {
		pump_event_loop_(4);
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}
	std::this_thread::sleep_for(
	    std::chrono::milliseconds(1500)); // wait for deliveries
	if (channel_) {
		channel_->close();
	}
	if (connection_) {
		connection_->close();
	}
	channel_.reset();
	connection_.reset();
	handler_.reset();
	stop_event_loop_();
	logger->log_debug("[RabbitMQ Publisher] Destructor finished.");
}

void RabbitMQPublisher::initialize() {
	std::string endpoint =
	    utils::get_env_var_or_default("PUBLISHER_ENDPOINT", "127.0.0.1");
	if (endpoint == "0.0.0.0") {
		endpoint = "127.0.0.1";
	}
	const std::string port =
	    utils::get_env_var_or_default("PUBLISHER_PORT", "5672");
	amqp_url_ = build_amqp_url_(endpoint, port);

	exchange_ = utils::get_env_var_or_default("RABBITMQ_EXCHANGE",
	                                          "benchmark_exchange");

	max_out_queue_bytes_ = static_cast<size_t>(std::stoul(
	    utils::get_env_var_or_default("RABBITMQ_MAX_OUT_BUFFER_BYTES",
	                                  "33554432"))); // default 32MB

	event_base_ = event_base_new();
	if (!event_base_) {
		throw std::runtime_error(
		    "[RabbitMQ Publisher] Failed to create libevent base.");
	}

	handler_ = std::make_unique<AMQP::LibEventHandler>(event_base_);
	connection_ = std::make_unique<AMQP::TcpConnection>(
	    handler_.get(), AMQP::Address(amqp_url_));

	channel_ = std::make_unique<AMQP::TcpChannel>(connection_.get());
	channel_->onError([this](const char *msg) {
		logger->log_error(std::string("[RabbitMQ Publisher] Channel error: ")
		                  + msg);
	});

	channel_->declareExchange(exchange_, AMQP::direct)
	    .onSuccess([this]() {
		    ready_.store(true, std::memory_order_release);
		    logger->log_info("[RabbitMQ Publisher] Exchange declared: "
		                     + exchange_);
	    })
	    .onError([this](const char *msg) {
		    logger->log_error(
		        std::string("[RabbitMQ Publisher] Exchange declare error: ")
		        + msg);
	    });

	if (!wait_ready_(30000)) {
		throw std::runtime_error(
		    "[RabbitMQ Publisher] Timed out waiting for channel ready.");
	}

	logger->log_info("[RabbitMQ Publisher] Publisher initialized.");
	log_configuration();
}

void RabbitMQPublisher::send_message(const Payload &message,
                                     std::string &topic) {
	logger->log_study("Serializing," + message.message_id + "," + topic);

	std::string serialized(message.serialized_bytes, '\0');
	if (!Payload::serialize(message, serialized.data())) {
		logger->log_error(
		    "[RabbitMQ Publisher] Serialization failed for message ID: "
		    + message.message_id);
		return;
	}

	while (connection_->queued() > max_out_queue_bytes_) {
		pump_event_loop_(2);
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}

	bool ok = channel_->publish(exchange_, topic, serialized.data(),
	                            message.serialized_bytes);

	if (ok) {
		logger->log_study("Publication," + message.message_id + "," + topic
		                  + "," + std::to_string(message.data_size) + ","
		                  + std::to_string(message.serialized_bytes));
	} else {
		logger->log_error("[RabbitMQ Publisher] Publish failed for message ID: "
		                  + message.message_id);
	}

	if (message.kind == PayloadKind::TERMINATION) {
		terminated_.store(true, std::memory_order_release);
	}

	pump_event_loop_(2);
}

void RabbitMQPublisher::log_configuration() {
	logger->log_config("[RabbitMQ Publisher] [CONFIG_BEGIN]");
	logger->log_config("[CONFIG] AMQP_URL=" + amqp_url_);
	logger->log_config("[CONFIG] EXCHANGE=" + exchange_);
	logger->log_config("[CONFIG] TOPICS="
	                   + utils::get_env_var_or_default("TOPICS", ""));
	logger->log_config("[CONFIG] RABBITMQ_MAX_OUT_BUFFER_BYTES="
	                   + std::to_string(max_out_queue_bytes_));
	logger->log_config("[RabbitMQ Publisher] [CONFIG_END]");
}

void RabbitMQPublisher::stop_event_loop_() {
	if (event_base_) {
		event_base_free(event_base_);
		event_base_ = nullptr;
	}
}

bool RabbitMQPublisher::wait_ready_(int timeout_ms) {
	auto deadline = std::chrono::steady_clock::now()
	    + std::chrono::milliseconds(timeout_ms);
	while (std::chrono::steady_clock::now() < deadline) {
		if (ready_.load(std::memory_order_acquire)) {
			return true;
		}
		pump_event_loop_(4);
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	return ready_.load(std::memory_order_acquire);
}

void RabbitMQPublisher::pump_event_loop_(int max_iterations) {
	if (!event_base_) {
		return;
	}
	for (int i = 0; i < max_iterations; ++i) {
		const int rc = event_base_loop(event_base_, EVLOOP_NONBLOCK);
		if (rc != 0) {
			break;
		}
	}
}

std::string RabbitMQPublisher::build_amqp_url_(const std::string &endpoint,
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
