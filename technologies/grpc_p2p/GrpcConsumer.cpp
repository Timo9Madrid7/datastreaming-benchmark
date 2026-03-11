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
	queue_ = std::make_unique<moodycamel::BlockingReaderWriterQueue<
	    std::shared_ptr<streaming::WireMessage>>>(queue_capacity_);

	grpc::ChannelArguments channel_args;
	channel_args.SetMaxReceiveMessageSize(16 * 1024 * 1024);
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
		context_ = std::make_unique<grpc::ClientContext>();
		google::protobuf::Empty request;
		reader_ = stub_->DoGet(context_.get(), request);
		if (!reader_) {
			logger->log_error("[gRPC Consumer] Failed to open DoGet stream.");
			std::this_thread::sleep_for(std::chrono::milliseconds(500));
			continue;
		}

		streaming::WireMessage msg;
		while (!stop_receiving_.load(std::memory_order_acquire)
		       && reader_->Read(&msg)) {
			const std::string topic = msg.topic();
			if (topics_.find(topic) == topics_.end()) {
				msg.Clear();
				continue;
			}

			auto holder =
			    std::make_shared<streaming::WireMessage>(std::move(msg));
			logger->log_study("Reception," + holder->message_id() + ","
			                  + topic);
			enqueue_latest_(std::move(holder));
			msg.Clear();
		}

		const grpc::Status status = reader_->Finish();
		if (!status.ok() && !stop_receiving_.load(std::memory_order_acquire)) {
			logger->log_error("[gRPC Consumer] Stream closed with error: "
			                  + status.error_message());
		}

		reader_.reset();
		context_.reset();
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
	logger->log_config("[gRPC Consumer] [CONFIG_END]");
}

void GrpcConsumer::close_stream_() {
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
	if (worker_thread_.joinable()) {
		worker_thread_.join();
	}
	if (queue_) {
		std::shared_ptr<streaming::WireMessage> dropped;
		while (queue_->try_dequeue(dropped)) {
		}
	}
}

void GrpcConsumer::enqueue_latest_(
    std::shared_ptr<streaming::WireMessage> msg) {
	if (!queue_) {
		return;
	}

	if (queue_->try_enqueue(std::move(msg))) {
		return;
	}

	// Queue full: drop the earliest pending item and enqueue the latest one.
	std::shared_ptr<streaming::WireMessage> dropped;
	(void)queue_->try_dequeue(dropped);
	if (!queue_->try_enqueue(std::move(msg))) {
		logger->log_error(
		    "[gRPC Consumer] Failed to enqueue latest message after "
		    "dropping oldest.");
	}
}

void GrpcConsumer::worker_loop_() {
	if (!queue_) {
		return;
	}

	while (worker_running_.load(std::memory_order_acquire)
	       || queue_->size_approx() > 0) {
		std::shared_ptr<streaming::WireMessage> msg;
		if (!queue_->wait_dequeue_timed(msg, std::chrono::milliseconds(50))) {
			if (!worker_running_.load(std::memory_order_acquire)
			    && queue_->size_approx() == 0) {
				break;
			}
			continue;
		}

		const std::string &topic = msg->topic();
		const std::string &message_id = msg->message_id();
		const PayloadKind kind = static_cast<PayloadKind>(msg->kind());

		const streaming::ActualData &nested = msg->nested();
		size_t payload_size = nested.data().size();
		payload_size +=
		    static_cast<size_t>(nested.doubles_size()) * sizeof(double);
		for (const auto &s : nested.strings()) {
			payload_size += s.size();
		}
		const size_t wire_size = msg->ByteSizeLong();

		logger->log_study("Deserialized," + message_id + "," + topic + ","
		                  + std::to_string(payload_size) + ","
		                  + std::to_string(wire_size));

		if (kind == PayloadKind::TERMINATION) {
			subscribed_streams.dec();
			logger->log_info("[gRPC Consumer] Termination signal received for "
			                 "message ID: "
			                 + message_id);
			if (subscribed_streams.get() == 0) {
				logger->log_info(
				    "[gRPC Consumer] All streams terminated. Exiting.");
				stop_receiving_.store(true, std::memory_order_release);
				close_stream_();
			}
			logger->log_info("[gRPC Consumer] Remaining subscribed streams: "
			                 + std::to_string(subscribed_streams.get()));
		}
	}
}