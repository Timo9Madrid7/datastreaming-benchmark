#include "GrpcConsumer.hpp"

#include <chrono>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <thread>

#include <google/protobuf/empty.pb.h>

#include "Logger.hpp"
#include "Payload.hpp"
#include "Utils.hpp"

GrpcConsumer::GrpcConsumer(std::shared_ptr<Logger> logger)
    : IConsumer(logger), deserializer_(logger) {
	logger->log_info("[gRPC Consumer] GrpcConsumer created.");
}

GrpcConsumer::~GrpcConsumer() {
	logger->log_debug("[gRPC Consumer] Cleaning up gRPC consumer...");
	stop_receiving_.store(true, std::memory_order_release);
	deserializer_.stop();
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

	const std::string port =
	    utils::get_env_var_or_default("PUBLISHER_PORT",
	                                  utils::get_env_var_or_default(
	                                      "CONSUMER_PORT", "50051"));
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

	grpc::ChannelArguments channel_args;
	channel_args.SetMaxReceiveMessageSize(16 * 1024 * 1024);
	channel_ = grpc::CreateCustomChannel(connect_endpoint_,
	                                     grpc::InsecureChannelCredentials(),
	                                     channel_args);

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
	deserializer_.start(
	    [](const void *data, size_t len, std::string & /*topic*/, Payload &out) {
		    return Payload::deserialize(data, len, out);
	    },
	    [this](const Payload &payload, const std::string & /*topic*/,
	           size_t /*raw_len*/) {
		    if (payload.kind == PayloadKind::TERMINATION) {
			    subscribed_streams.dec();
			    logger->log_info(
			        "[gRPC Consumer] Termination signal received for message ID: "
			        + payload.message_id);
			    if (subscribed_streams.get() == 0) {
				    logger->log_info(
				        "[gRPC Consumer] All streams terminated. Exiting.");
				    stop_receiving_.store(true, std::memory_order_release);
				    close_stream_();
			    }
			    logger->log_info(
			        "[gRPC Consumer] Remaining subscribed streams: "
			        + std::to_string(subscribed_streams.get()));
		    }
	    });

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
		while (!stop_receiving_.load(std::memory_order_acquire) && reader_->Read(&msg)) {
			const std::string topic = msg.topic();
			if (topics_.find(topic) == topics_.end()) {
				msg.Clear();
				continue;
			}

			std::string message_id = msg.message_id();
			if (message_id.empty()) {
				Payload id_payload;
				if (!Payload::deserialize_id(msg.body().data(), msg.body().size(),
				                             id_payload)) {
					logger->log_error("[gRPC Consumer] Failed to extract message id "
					                  "from payload.");
					msg.Clear();
					continue;
				}
				message_id = id_payload.message_id;
			}

			logger->log_study("Reception," + message_id + "," + topic);

			auto holder =
			    std::make_shared<streaming::WireMessage>(std::move(msg));
			utils::Deserializer::Item item;
			item.holder = holder;
			item.data = holder->body().data();
			item.len = holder->body().size();
			item.topic = topic;
			item.message_id = message_id;
			if (!deserializer_.enqueue(std::move(item))) {
				logger->log_error(
				    "[gRPC Consumer] Deserializer queue full; dropping message.");
			}

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

	deserializer_.stop();
	close_stream_();
}

void GrpcConsumer::log_configuration() {
	logger->log_config("[gRPC Consumer] [CONFIG_BEGIN]");
	logger->log_config("[CONFIG] CONNECT_ENDPOINT=" + connect_endpoint_);
	logger->log_config("[CONFIG] PUBLISHER_ENDPOINTS="
	                   + utils::get_env_var_or_default("PUBLISHER_ENDPOINTS", ""));
	logger->log_config("[CONFIG] TOPICS="
	                   + utils::get_env_var_or_default("TOPICS", ""));
	logger->log_config("[gRPC Consumer] [CONFIG_END]");
}

void GrpcConsumer::close_stream_() {
	if (context_) {
		context_->TryCancel();
	}
}