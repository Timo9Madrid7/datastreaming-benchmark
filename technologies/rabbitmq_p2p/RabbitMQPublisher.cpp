#include "RabbitMQPublisher.hpp"

#include <amqpcpp/linux_tcp/tcpconnection.h>
#include <chrono>
#include <cstddef>
#include <event2/event.h>
#include <event2/thread.h>
#include <stdexcept>
#include <string>
#include <utility>

#include "Logger.hpp"
#include "Payload.hpp"
#include "Utils.hpp"

RabbitMQPublisher::RabbitMQPublisher(std::shared_ptr<Logger> logger)
    : IPublisher(logger) {
	logger->log_info("[RabbitMQ Publisher] RabbitMQPublisher created.");
}

RabbitMQPublisher::~RabbitMQPublisher() {
	logger->log_debug("[RabbitMQ Publisher] Cleaning up RabbitMQ publisher...");
	if (channel_) {
		channel_->close();
	}
	if (connection_) {
		connection_->close();
	}
	stop_event_loop_();
	channel_.reset();
	connection_.reset();
	handler_.reset();
	logger->log_debug("[RabbitMQ Publisher] Destructor finished.");
}

void RabbitMQPublisher::initialize() {
	static std::once_flag libevent_threading_once;
	std::call_once(libevent_threading_once, []() {
		if (evthread_use_pthreads() != 0) {
			throw std::runtime_error("[RabbitMQ Publisher] Failed to enable "
			                         "libevent pthread support.");
		}
	});

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
	                                  "1073741824"))); // 1GB default

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

	start_event_loop_();

	channel_->declareExchange(exchange_, AMQP::direct)
	    .onSuccess([this]() {
		    {
			    std::lock_guard<std::mutex> lock(ready_mu_);
			    ready_.store(true, std::memory_order_release);
		    }
		    ready_cv_.notify_all();
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

	auto *arg_data =
	    new std::tuple<RabbitMQPublisher *, std::string, std::string>(
	        this, std::move(serialized), topic);

	while (connection_->queued() > max_out_queue_bytes_) {
		logger->log_debug("[RabbitMQ Publisher] Throttling publish, out "
		                  "queue size: "
		                  + std::to_string(connection_->queued()) + " bytes.");
		std::this_thread::sleep_for(std::chrono::milliseconds(200));
	}

	int ret = event_base_once(
	    event_base_, -1, EV_TIMEOUT,
	    [](evutil_socket_t, short, void *arg) {
		    auto *data = static_cast<
		        std::tuple<RabbitMQPublisher *, std::string, std::string> *>(
		        arg);
		    RabbitMQPublisher *publisher = std::get<0>(*data);
		    const std::string &serialized = std::get<1>(*data);
		    const std::string &topic = std::get<2>(*data);

		    bool ok = publisher->channel_->publish(publisher->exchange_, topic,
		                                           serialized.data(),
		                                           serialized.size());

		    if (!ok) {
			    publisher->logger->log_error(
			        "[RabbitMQ Publisher] Publish failed.");
		    }

		    delete data;
	    },
	    arg_data, nullptr);

	logger->log_study("Publication," + message.message_id + "," + topic + ","
	                  + std::to_string(message.data_size) + ","
	                  + std::to_string(message.serialized_bytes));

	if (ret != 0) {
		logger->log_error(
		    "[RabbitMQ Publisher] event_base_once failed to schedule publish.");
		delete arg_data;
	}
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

void RabbitMQPublisher::start_event_loop_() {
	if (!event_base_) {
		return;
	}
	io_thread_ = std::thread([this]() { event_base_dispatch(event_base_); });
}

void RabbitMQPublisher::stop_event_loop_() {
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
}

bool RabbitMQPublisher::wait_ready_(int timeout_ms) {
	std::unique_lock<std::mutex> lock(ready_mu_);
	if (ready_.load(std::memory_order_acquire)) {
		return true;
	}
	return ready_cv_.wait_for(
	    lock, std::chrono::milliseconds(timeout_ms),
	    [this]() { return ready_.load(std::memory_order_acquire); });
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
