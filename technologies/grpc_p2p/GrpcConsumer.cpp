#include "GrpcConsumer.hpp"

#include <chrono>
#include <google/protobuf/empty.pb.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <thread>

#include "Logger.hpp"
#include "Payload.hpp"
#include "Utils.hpp"

GrpcConsumer::GrpcConsumer(std::shared_ptr<Logger> logger) : IConsumer(logger) {
	logger->log_info("[gRPC Consumer] GrpcConsumer created.");
}

GrpcConsumer::~GrpcConsumer() {
	logger->log_debug("[gRPC Consumer] Cleaning up gRPC consumer...");
	stop_receiving_.store(true, std::memory_order_release);
	stop_worker_();
	close_stream_();
	logger->log_debug("[gRPC Consumer] Destructor finished.");
}

void GrpcConsumer::initialize() {
	const std::optional<std::string> env_topics = utils::get_env_var("TOPICS");
	if (!env_topics || env_topics->empty()) {
		throw std::runtime_error(
		    "[gRPC Consumer] Missing required environment variable TOPICS.");
	}

	const std::string vendpoints = utils::get_env_var_or_default(
	    "PUBLISHER_ENDPOINTS",
	    utils::get_env_var_or_default(
	        "PUBLISHER_ENDPOINT",
	        utils::get_env_var_or_default("CONSUMER_ENDPOINT", "127.0.0.1")));

	std::string endpoint;
	std::istringstream endpoints_stream(vendpoints);
	std::getline(endpoints_stream, endpoint, ',');
	if (endpoint.empty()) {
		throw std::runtime_error(
		    "[gRPC Consumer] No valid publisher endpoint provided.");
	}

	const std::string port = utils::get_env_var_or_default(
	    "PUBLISHER_PORT",
	    utils::get_env_var_or_default("CONSUMER_PORT", "50051"));
	connect_endpoint_ = endpoint + ":" + port;

	std::string topic;
	std::istringstream topics_stream(env_topics.value());
	while (std::getline(topics_stream, topic, ',')) {
		if (topic.empty()) {
			continue;
		}
		subscribe(topic);
	}

	if (topics_.empty()) {
		throw std::runtime_error("[gRPC Consumer] No valid topics provided.");
	}

	const std::string queue_capacity_env =
	    utils::get_env_var_or_default("GRPC_CONSUMER_QUEUE_SIZE", "1024");
	try {
		queue_capacity_ =
		    std::max(static_cast<size_t>(std::stoul(queue_capacity_env)),
		             static_cast<size_t>(1));
	} catch (...) {
		throw std::runtime_error(
		    "[gRPC Consumer] Invalid GRPC_CONSUMER_QUEUE_SIZE: "
		    + queue_capacity_env);
	}

	grpc::ChannelArguments channel_args;
	channel_args.SetMaxReceiveMessageSize(32 * 1024 * 1024);
	channel_ = grpc::CreateCustomChannel(
	    connect_endpoint_, grpc::InsecureChannelCredentials(), channel_args);

	constexpr int kMaxConnectAttempts = 30;
	bool connected = false;
	for (int attempt = 1; attempt <= kMaxConnectAttempts; ++attempt) {
		const auto deadline =
		    std::chrono::system_clock::now() + std::chrono::seconds(1);
		if (channel_->WaitForConnected(deadline)) {
			connected = true;
			break;
		}
		logger->log_info("[gRPC Consumer] Channel connect attempt "
		                 + std::to_string(attempt) + "/"
		                 + std::to_string(kMaxConnectAttempts) + " failed to "
		                 + connect_endpoint_);
	}
	if (!connected) {
		throw std::runtime_error("[gRPC Consumer] Failed to connect to "
		                         + connect_endpoint_ + " within timeout.");
	}

	stub_ = streaming::Streamer::NewStub(channel_);

	logger->log_info("[gRPC Consumer] Consumer initialized.");
	log_configuration();
}

void GrpcConsumer::subscribe(const std::string &topic) {
	if (!topics_.insert(topic).second) {
		return;
	}
	logger->log_info("[gRPC Consumer] Registering topic: " + topic);
	subscribed_streams.inc();
}

void GrpcConsumer::start_loop() {
	start_worker_();

	while (!stop_receiving_.load(std::memory_order_acquire)) {
		{
			std::lock_guard<std::mutex> lock(stream_mu_);
			context_ = std::make_unique<grpc::ClientContext>();
		}
		google::protobuf::Empty request;
		{
			std::lock_guard<std::mutex> lock(stream_mu_);
			reader_ = stub_->DoGet(context_.get(), request);
		}
		if (!reader_) {
			logger->log_error("[gRPC Consumer] Failed to open DoGet stream.");
			std::this_thread::sleep_for(std::chrono::milliseconds(500));
			continue;
		}

		streaming::WireBatch batch;
		while (!stop_receiving_.load(std::memory_order_acquire)
		       && reader_->Read(&batch)) {
			for (const auto &msg : batch.messages()) {
				if (stop_receiving_.load(std::memory_order_acquire)) {
					break;
				}

				const std::string &topic = msg.topic();
				if (topics_.find(topic) == topics_.end()) {
					continue;
				}

				logger->log_study("Reception," + msg.message_id() + ","
				                  + topic);

				auto holder = std::make_shared<streaming::WireMessage>(msg);
				if (!enqueue_for_worker_(std::move(holder))) {
					break;
				}
			}
			batch.Clear();
		}

		const grpc::Status status = reader_->Finish();
		if (!status.ok() && !stop_receiving_.load(std::memory_order_acquire)) {
			logger->log_error("[gRPC Consumer] Stream closed with error: "
			                  + status.error_message());
		}

		{
			std::lock_guard<std::mutex> lock(stream_mu_);
			reader_.reset();
			context_.reset();
		}
		if (!stop_receiving_.load(std::memory_order_acquire)) {
			std::this_thread::sleep_for(std::chrono::milliseconds(200));
		}
	}

	stop_worker_();
	close_stream_();
}

void GrpcConsumer::log_configuration() {
	logger->log_config("[gRPC Consumer] [CONFIG_BEGIN]");
	logger->log_config("[CONFIG] CONNECT_ENDPOINT=" + connect_endpoint_);
	logger->log_config(
	    "[CONFIG] PUBLISHER_ENDPOINTS="
	    + utils::get_env_var_or_default("PUBLISHER_ENDPOINTS", ""));
	logger->log_config("[CONFIG] TOPICS="
	                   + utils::get_env_var_or_default("TOPICS", ""));
	logger->log_config("[CONFIG] GRPC_CONSUMER_QUEUE_SIZE="
	                   + std::to_string(queue_capacity_));
	logger->log_config(
	    "[CONFIG] MESSAGE_HANDLING=batch_receive_async_deserialize");
	logger->log_config("[gRPC Consumer] [CONFIG_END]");
}

void GrpcConsumer::close_stream_() {
	std::lock_guard<std::mutex> lock(stream_mu_);
	if (context_) {
		context_->TryCancel();
	}
}

void GrpcConsumer::start_worker_() {
	if (worker_running_.exchange(true, std::memory_order_acq_rel)) {
		return;
	}
	worker_thread_ = std::thread([this]() { worker_loop_(); });
}

void GrpcConsumer::stop_worker_() {
	if (!worker_running_.exchange(false, std::memory_order_acq_rel)) {
		return;
	}
	queue_cv_.notify_all();
	if (worker_thread_.joinable()) {
		worker_thread_.join();
	}
}

bool GrpcConsumer::enqueue_for_worker_(
    std::shared_ptr<streaming::WireMessage> msg) {
	std::unique_lock<std::mutex> lock(queue_mu_);
	queue_cv_.wait(lock, [this]() {
		return stop_receiving_.load(std::memory_order_acquire)
		    || queue_.size() < queue_capacity_;
	});

	if (stop_receiving_.load(std::memory_order_acquire)) {
		return false;
	}

	queue_.push_back(std::move(msg));
	queue_cv_.notify_all();
	return true;
}

void GrpcConsumer::worker_loop_() {
	while (true) {
		std::shared_ptr<streaming::WireMessage> msg;
		{
			std::unique_lock<std::mutex> lock(queue_mu_);
			queue_cv_.wait(lock, [this]() {
				return !worker_running_.load(std::memory_order_acquire)
				    || !queue_.empty();
			});

			if (queue_.empty()) {
				if (!worker_running_.load(std::memory_order_acquire)) {
					break;
				}
				continue;
			}

			msg = std::move(queue_.front());
			queue_.pop_front();
			queue_cv_.notify_all();
		}

		if (msg) {
			process_message_(*msg);
		}
	}
}

void GrpcConsumer::process_message_(const streaming::WireMessage &msg) {
	const std::string &topic = msg.topic();
	const std::string &message_id = msg.message_id();
	const PayloadKind kind = static_cast<PayloadKind>(msg.kind());

	const streaming::ActualData &nested = msg.nested();
	size_t payload_size = nested.data().size();
	payload_size += static_cast<size_t>(nested.doubles_size()) * sizeof(double);
	for (const auto &s : nested.strings()) {
		payload_size += s.size();
	}
	const size_t serialized_size =
	    payload_size + topic.size() + message_id.size() + sizeof(uint8_t);

	logger->log_study("Deserialized," + message_id + "," + topic + ","
	                  + std::to_string(payload_size) + ","
	                  + std::to_string(serialized_size));

	if (kind == PayloadKind::TERMINATION) {
		subscribed_streams.dec();
		logger->log_info("[gRPC Consumer] Termination signal received for "
		                 "message ID: "
		                 + message_id);
		if (subscribed_streams.get() == 0) {
			logger->log_info(
			    "[gRPC Consumer] All streams terminated. Exiting.");
			stop_receiving_.store(true, std::memory_order_release);
			queue_cv_.notify_all();
			close_stream_();
		}
		logger->log_info("[gRPC Consumer] Remaining subscribed streams: "
		                 + std::to_string(subscribed_streams.get()));
	}
}
