#include "GrpcPublisher.hpp"

#include <algorithm>
#include <cstdlib>
#include <future>
#include <stdexcept>
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
		if (static_cast<size_t>(fanout_threads) != fanout_pool_.get_thread_count()) {
			fanout_pool_.reset(static_cast<size_t>(fanout_threads));
		}
	} catch (...) {
		throw std::runtime_error("[gRPC Publisher] Invalid GRPC_FANOUT_THREADS: "
		                         + fanout_threads_env);
	}

	try {
		max_queue_per_consumer_ = static_cast<size_t>(std::stoul(queue_size_env));
		if (max_queue_per_consumer_ == 0) {
			max_queue_per_consumer_ = 1;
		}
	} catch (...) {
		throw std::runtime_error(
		    "[gRPC Publisher] Invalid GRPC_MAX_QUEUE_PER_CONSUMER: "
		    + queue_size_env);
	}

	grpc::ServerBuilder builder;
	builder.AddListeningPort(endpoint_, grpc::InsecureServerCredentials());
	builder.RegisterService(this);
	builder.SetMaxReceiveMessageSize(16 * 1024 * 1024);
	builder.SetMaxSendMessageSize(16 * 1024 * 1024);

	server_ = builder.BuildAndStart();
	if (!server_) {
		throw std::runtime_error("[gRPC Publisher] Failed to start gRPC server.");
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

	const size_t serialized_size = message.serialized_bytes;
	std::string body(serialized_size, '\0');
	if (!Payload::serialize(message, body.data())) {
		logger->log_error(
		    "[gRPC Publisher] Serialization failed for message ID: "
		    + message.message_id);
		return;
	}

	streaming::WireMessage wire;
	wire.set_topic(topic);
	wire.set_message_id(message.message_id);
	wire.set_kind(static_cast<streaming::PayloadKind>(message.kind));
	wire.set_body(body.data(), body.size());
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

	std::vector<std::future<void>> fanout_tasks;
	fanout_tasks.reserve(subscribers.size());
	for (const auto &subscriber : subscribers) {
		fanout_tasks.push_back(fanout_pool_.submit_task(
		    [this, subscriber, shared_wire]() {
			    enqueue_to_subscriber_(subscriber, shared_wire);
		    }));
	}
	for (auto &task : fanout_tasks) {
		task.wait();
	}

	logger->log_study("Publication," + message.message_id + "," + topic + ","
	                  + std::to_string(message.data_size) + ","
	                  + std::to_string(serialized_size));
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

grpc::Status GrpcPublisher::DoGet(
    grpc::ServerContext *context,
    const google::protobuf::Empty * /*request*/,
    grpc::ServerWriter<streaming::WireMessage> *writer) {
	std::shared_ptr<Subscriber> subscriber;
	{
		std::lock_guard<std::mutex> lock(subscribers_mu_);
		subscriber = std::make_shared<Subscriber>(next_subscriber_id_++);
		subscribers_[subscriber->id] = subscriber;
	}
	logger->log_info("[gRPC Publisher] Consumer connected. subscriber_id="
	                 + std::to_string(subscriber->id));

	while (!context->IsCancelled()) {
		std::shared_ptr<const streaming::WireMessage> next;
		{
			std::unique_lock<std::mutex> lock(subscriber->mu);
			subscriber->cv.wait(lock, [&]() {
				return subscriber->closed || !subscriber->queue.empty()
				    || context->IsCancelled();
			});

			if (subscriber->closed || context->IsCancelled()) {
				break;
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

	{
		std::lock_guard<std::mutex> lock(subscriber->mu);
		subscriber->closed = true;
	}
	subscriber->cv.notify_all();

	logger->log_info("[gRPC Publisher] Consumer disconnected. subscriber_id="
	                 + std::to_string(subscriber->id));
	return grpc::Status::OK;
}

void GrpcPublisher::enqueue_to_subscriber_(
    const std::shared_ptr<Subscriber> &subscriber,
    const std::shared_ptr<const streaming::WireMessage> &msg) {
	std::lock_guard<std::mutex> lock(subscriber->mu);
	if (subscriber->closed) {
		return;
	}

	while (subscriber->queue.size() >= max_queue_per_consumer_) {
		subscriber->queue.pop_front();
	}
	subscriber->queue.push_back(msg);
	subscriber->cv.notify_one();
}

void GrpcPublisher::shutdown_server_() {
	if (!server_started_) {
		return;
	}

	{
		std::lock_guard<std::mutex> lock(subscribers_mu_);
		for (auto &entry : subscribers_) {
			std::lock_guard<std::mutex> subscriber_lock(entry.second->mu);
			entry.second->closed = true;
			entry.second->cv.notify_all();
		}
	}

	if (server_) {
		server_->Shutdown();
	}
	if (server_thread_.joinable()) {
		server_thread_.join();
	}
	server_started_ = false;

	fanout_pool_.wait();
}