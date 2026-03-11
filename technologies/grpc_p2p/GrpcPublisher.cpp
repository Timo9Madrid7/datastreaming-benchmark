#include "GrpcPublisher.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <stdexcept>
#include <thread>
#include <vector>

#include "Logger.hpp"
#include "Payload.hpp"
#include "Utils.hpp"

GrpcPublisher::GrpcPublisher(std::shared_ptr<Logger> logger)
    : IPublisher(logger) {
	logger->log_info("[gRPC Publisher] GrpcPublisher created.");
}

GrpcPublisher::~GrpcPublisher() {
	logger->log_debug("[gRPC Publisher] Cleaning up gRPC publisher...");
	shutdown_server_();
	logger->log_debug("[gRPC Publisher] Destructor finished.");
}

void GrpcPublisher::initialize() {
	std::string endpoint =
	    utils::get_env_var_or_default("PUBLISHER_ENDPOINT", "0.0.0.0");
	if (endpoint.empty()) {
		endpoint = "0.0.0.0";
	}
	const std::string port =
	    utils::get_env_var_or_default("PUBLISHER_PORT", "50051");
	endpoint_ = endpoint + ":" + port;

	const std::string fanout_threads_env =
	    utils::get_env_var_or_default("GRPC_FANOUT_THREADS", "5");
	const std::string queue_size_env =
	    utils::get_env_var_or_default("GRPC_MAX_QUEUE_PER_CONSUMER", "1024");

	try {
		const int fanout_threads = std::max(1, std::stoi(fanout_threads_env));
		if (static_cast<size_t>(fanout_threads)
		    != fanout_pool_.get_thread_count()) {
			fanout_pool_.reset(static_cast<size_t>(fanout_threads));
		}
	} catch (...) {
		throw std::runtime_error(
		    "[gRPC Publisher] Invalid GRPC_FANOUT_THREADS: "
		    + fanout_threads_env);
	}

	try {
		max_queue_per_consumer_ =
		    std::max(static_cast<size_t>(std::stoul(queue_size_env)),
		             static_cast<size_t>(1));
	} catch (...) {
		throw std::runtime_error(
		    "[gRPC Publisher] Invalid GRPC_MAX_QUEUE_PER_CONSUMER: "
		    + queue_size_env);
	}

	grpc::ServerBuilder builder;
	builder.AddListeningPort(endpoint_, grpc::InsecureServerCredentials());
	builder.RegisterService(this);
	builder.SetMaxSendMessageSize(16 * 1024 * 1024);

	server_ = builder.BuildAndStart();
	if (!server_) {
		throw std::runtime_error(
		    "[gRPC Publisher] Failed to start gRPC server.");
	}

	server_started_ = true;
	server_thread_ = std::thread([this]() { server_->Wait(); });

	logger->log_info("[gRPC Publisher] Publisher initialized.");
	log_configuration();
}

void GrpcPublisher::send_message(const Payload &message, std::string &topic) {
	if (!server_started_) {
		logger->log_error("[gRPC Publisher] Server is not started.");
		return;
	}

	logger->log_study("Serializing," + message.message_id + "," + topic);

	streaming::WireMessage wire;
	wire.set_topic(topic);
	wire.set_message_id(message.message_id);
	wire.set_kind(static_cast<streaming::PayloadKind>(message.kind));
	if (!message.bytes.empty()) {
		wire.mutable_nested()->set_data(
		    reinterpret_cast<const char *>(message.bytes.data()),
		    message.bytes.size());
	}
	if (message.kind == PayloadKind::COMPLEX) {
		wire.mutable_nested()->mutable_doubles()->Add(
		    message.nested_payload.doubles.begin(),
		    message.nested_payload.doubles.end());
		wire.mutable_nested()->mutable_strings()->Add(
		    message.nested_payload.strings.begin(),
		    message.nested_payload.strings.end());
	}

	const size_t wire_size = wire.ByteSizeLong();
	auto shared_wire =
	    std::make_shared<const streaming::WireMessage>(std::move(wire));

	std::vector<std::shared_ptr<Subscriber>> subscribers;
	{
		std::lock_guard<std::mutex> lock(subscribers_mu_);
		subscribers.reserve(subscribers_.size());
		for (const auto &entry : subscribers_) {
			subscribers.push_back(entry.second);
		}
	}

	for (const auto &subscriber : subscribers) {
		fanout_pool_.detach_task([this, subscriber, shared_wire]() {
			enqueue_to_subscriber_(subscriber, shared_wire);
		});
	}
	// Ensure the message is enqueued to all current subscribers before logging Publication.
	fanout_pool_.wait();

	logger->log_study("Publication," + message.message_id + "," + topic + ","
	                  + std::to_string(message.data_size) + ","
	                  + std::to_string(wire_size));
}

void GrpcPublisher::log_configuration() {
	logger->log_config("[gRPC Publisher] [CONFIG_BEGIN]");
	logger->log_config("[CONFIG] ENDPOINT=" + endpoint_);
	logger->log_config("[CONFIG] GRPC_FANOUT_THREADS="
	                   + std::to_string(fanout_pool_.get_thread_count()));
	logger->log_config("[CONFIG] GRPC_MAX_QUEUE_PER_CONSUMER="
	                   + std::to_string(max_queue_per_consumer_));
	logger->log_config("[CONFIG] TOPICS="
	                   + utils::get_env_var_or_default("TOPICS", ""));
	logger->log_config("[gRPC Publisher] [CONFIG_END]");
}

grpc::Status
GrpcPublisher::DoGet(grpc::ServerContext *context,
                     const google::protobuf::Empty * /*request*/,
                     grpc::ServerWriter<streaming::WireMessage> *writer) {
	std::shared_ptr<Subscriber> subscriber;
	{
		std::lock_guard<std::mutex> lock(subscribers_mu_);
		subscriber = std::make_shared<Subscriber>(next_subscriber_id_++,
		                                          max_queue_per_consumer_);
		subscribers_[subscriber->id] = subscriber;
	}
	logger->log_info("[gRPC Publisher] Consumer connected. subscriber_id="
	                 + std::to_string(subscriber->id));

	while (!context->IsCancelled()) {
		std::shared_ptr<const streaming::WireMessage> next;
		{
			std::unique_lock<std::mutex> qlock(subscriber->queue_mu);
			subscriber->queue_cv.wait_for(
			    qlock, std::chrono::milliseconds(50), [&subscriber, context]() {
				    return !subscriber->queue.empty()
				        || subscriber->closed.load(std::memory_order_acquire)
				        || context->IsCancelled();
			    });

			if (subscriber->queue.empty()) {
				if (subscriber->closed.load(std::memory_order_acquire)
				    || context->IsCancelled()) {
					break;
				}
				continue;
			}

			next = subscriber->queue.front();
			subscriber->queue.pop_front();
		}

		if (!next) {
			continue;
		}

		if (!writer->Write(*next)) {
			break;
		}
	}

	{
		std::lock_guard<std::mutex> lock(subscribers_mu_);
		subscribers_.erase(subscriber->id);
	}
	subscriber->closed.store(true, std::memory_order_release);
	subscriber->queue_cv.notify_all();

	logger->log_info("[gRPC Publisher] Consumer disconnected. subscriber_id="
	                 + std::to_string(subscriber->id));
	return grpc::Status::OK;
}

void GrpcPublisher::enqueue_to_subscriber_(
    const std::shared_ptr<Subscriber> &subscriber,
    const std::shared_ptr<const streaming::WireMessage> &msg) {
	if (subscriber->closed.load(std::memory_order_acquire)) {
		return;
	}

	{
		std::lock_guard<std::mutex> qlock(subscriber->queue_mu);
		if (subscriber->closed.load(std::memory_order_acquire)) {
			return;
		}

		if (subscriber->queue.size() >= subscriber->capacity
		    && !subscriber->queue.empty()) {
			subscriber->queue.pop_front();
		}
		subscriber->queue.push_back(msg);
	}
	subscriber->queue_cv.notify_one();
}

void GrpcPublisher::shutdown_server_() {
	if (!server_started_) {
		return;
	}

	// Ensure all pending enqueue tasks are completed before attempting a drain.
	fanout_pool_.wait();
	while (true) {
		bool all_queues_empty = true;
		{
			std::lock_guard<std::mutex> lock(subscribers_mu_);
			for (const auto &entry : subscribers_) {
				std::lock_guard<std::mutex> qlock(entry.second->queue_mu);
				if (!entry.second->queue.empty()) {
					all_queues_empty = false;
					break;
				}
			}
		}

		if (all_queues_empty) {
			break;
		}

		std::this_thread::sleep_for(std::chrono::seconds(1));
	}

	{
		std::lock_guard<std::mutex> lock(subscribers_mu_);
		for (auto &entry : subscribers_) {
			entry.second->closed.store(true, std::memory_order_release);
			entry.second->queue_cv.notify_all();
		}
	}

	if (server_) {
		server_->Shutdown();
	}
	if (server_thread_.joinable()) {
		server_thread_.join();
	}
	server_started_ = false;
}