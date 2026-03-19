#include "GrpcPublisher.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <thread>

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

	const std::string shared_queue_env =
	    utils::get_env_var_or_default("GRPC_MAX_SHARED_QUEUE_BATCHES", "1024");
	const std::optional<std::string> payload_size_str =
	    utils::get_env_var("PAYLOAD_SIZE");
	try {
		max_shared_queue_batches_ =
		    std::max(static_cast<size_t>(std::stoul(shared_queue_env)),
		             static_cast<size_t>(1));
	} catch (...) {
		throw std::runtime_error(
		    "[gRPC Publisher] Invalid GRPC_MAX_SHARED_QUEUE_BATCHES: "
		    + shared_queue_env);
	}

	if (!payload_size_str) {
		throw std::runtime_error(
		    "[gRPC Publisher] PAYLOAD_SIZE environment variable is not set.");
	}

	try {
		const uint64_t payload_size = std::stoull(payload_size_str.value());
		max_batch_bytes_ = (payload_size >= 4ULL * 1024ULL * 1024ULL)
		    ? (16ULL * 1024ULL * 1024ULL)
		    : (8ULL * 1024ULL * 1024ULL);
	} catch (...) {
		throw std::runtime_error(
		    "[gRPC Publisher] Invalid PAYLOAD_SIZE: " + payload_size_str.value());
	}

	grpc::ServerBuilder builder;
	builder.AddListeningPort(endpoint_, grpc::InsecureServerCredentials());
	builder.RegisterService(this);
	builder.SetMaxSendMessageSize(32 * 1024 * 1024);

	server_ = builder.BuildAndStart();
	if (!server_) {
		throw std::runtime_error(
		    "[gRPC Publisher] Failed to start gRPC server.");
	}

	server_started_ = true;
	shutting_down_.store(false, std::memory_order_release);
	server_thread_ = std::thread([this]() { server_->Wait(); });

	logger->log_info("[gRPC Publisher] Publisher initialized.");
	log_configuration();
}

void GrpcPublisher::add_message_to_pending_batch_(const Payload &message,
                                                  const std::string &topic,
                                                  size_t row_size_bytes) {
	streaming::WireMessage *wire = pending_batch_.add_messages();
	wire->set_topic(topic);
	wire->set_message_id(message.message_id);
	wire->set_kind(static_cast<streaming::PayloadKind>(message.kind));
	if (!message.bytes.empty()) {
		wire->mutable_nested()->set_data(
		    reinterpret_cast<const char *>(message.bytes.data()),
		    message.bytes.size());
	}
	if (message.kind == PayloadKind::COMPLEX) {
		wire->mutable_nested()->mutable_doubles()->Add(
		    message.nested_payload.doubles.begin(),
		    message.nested_payload.doubles.end());
		wire->mutable_nested()->mutable_strings()->Add(
		    message.nested_payload.strings.begin(),
		    message.nested_payload.strings.end());
	}

	pending_batch_bytes_ += row_size_bytes;
	pending_publication_logs_.push_back("Publication," + message.message_id
	                                    + "," + topic + ","
	                                    + std::to_string(message.data_size)
	                                    + "," + std::to_string(row_size_bytes));
}

void GrpcPublisher::flush_pending_batch_locked_(bool /*is_termination_batch*/) {
	if (pending_batch_.messages_size() == 0) {
		return;
	}

	auto shared_batch =
	    std::make_shared<const streaming::WireBatch>(pending_batch_);

	if (log_.empty()) {
		base_seq_ = next_seq_;
	}
	log_.push_back(LogEntry{next_seq_, shared_batch});
	++next_seq_;

	for (const std::string &entry : pending_publication_logs_) {
		logger->log_study(entry);
	}
	pending_publication_logs_.clear();
	pending_batch_.clear_messages();
	pending_batch_bytes_ = 0;
	gc_locked_();
}

void GrpcPublisher::send_message(const Payload &message, std::string &topic) {
	if (!server_started_) {
		logger->log_error("[gRPC Publisher] Server is not started.");
		return;
	}

	logger->log_study("Serializing," + message.message_id + "," + topic);

	const size_t row_size_bytes = topic.size() + message.message_id.size()
	    + sizeof(uint8_t) + message.byte_size
	    + (message.kind == PayloadKind::COMPLEX
	           ? message.nested_payload.double_size
	               + message.nested_payload.string_size
	           : 0);

	bool did_flush = false;
	{
		std::unique_lock<std::mutex> lock(log_mu_);
		while (!shutting_down_.load(std::memory_order_acquire)
		       && log_.size() >= max_shared_queue_batches_) {
			log_cv_.wait(lock, [this]() {
				return shutting_down_.load(std::memory_order_acquire)
				    || log_.size() < max_shared_queue_batches_;
			});
		}

		if (shutting_down_.load(std::memory_order_acquire)) {
			return;
		}

		add_message_to_pending_batch_(message, topic, row_size_bytes);


		if (pending_batch_bytes_ >= max_batch_bytes_
		    || message.kind == PayloadKind::TERMINATION) {
			flush_pending_batch_locked_(message.kind
			                            == PayloadKind::TERMINATION);
			did_flush = true;
		}
	}

	if (did_flush) {
		log_cv_.notify_all();
	}
}

void GrpcPublisher::log_configuration() {
	logger->log_config("[gRPC Publisher] [CONFIG_BEGIN]");
	logger->log_config("[CONFIG] ENDPOINT=" + endpoint_);
	logger->log_config("[CONFIG] FANOUT_MODE=shared_batch_log_cursor");
	logger->log_config("[CONFIG] GRPC_MAX_SHARED_QUEUE_BATCHES="
	                   + std::to_string(max_shared_queue_batches_));
	logger->log_config("[CONFIG] MAX_BATCH_BYTES="
	                   + std::to_string(max_batch_bytes_));
	logger->log_config("[CONFIG] BATCH_SIZE_POLICY=static_from_payload_size");
	logger->log_config("[CONFIG] TOPICS="
	                   + utils::get_env_var_or_default("TOPICS", ""));
	logger->log_config("[gRPC Publisher] [CONFIG_END]");
}

grpc::Status
GrpcPublisher::DoGet(grpc::ServerContext *context,
                     const google::protobuf::Empty * /*request*/,
                     grpc::ServerWriter<streaming::WireBatch> *writer) {
	std::shared_ptr<ConsumerState> consumer;
	uint64_t consumer_id = 0;
	{
		std::lock_guard<std::mutex> lock(log_mu_);
		consumer = std::make_shared<ConsumerState>();
		consumer->next_seq = base_seq_;
		consumer_id = next_consumer_id_++;
		consumers_.emplace(consumer_id, consumer);
	}
	logger->log_info("[gRPC Publisher] Consumer connected. subscriber_id="
	                 + std::to_string(consumer_id));

	while (!context->IsCancelled()) {
		std::shared_ptr<const streaming::WireBatch> next;
		uint64_t next_seq_candidate = 0;
		{
			std::unique_lock<std::mutex> lock(log_mu_);
			log_cv_.wait_for(lock, std::chrono::milliseconds(50),
			                 [&consumer, this, context]() {
				                 return context->IsCancelled()
				                     || shutting_down_.load(
				                         std::memory_order_acquire)
				                     || consumer->next_seq < next_seq_;
			                 });

			if (context->IsCancelled()) {
				break;
			}

			if (consumer->next_seq >= next_seq_) {
				if (shutting_down_.load(std::memory_order_acquire)) {
					break;
				}
				continue;
			}

			if (consumer->next_seq < base_seq_) {
				logger->log_error(
				    "[gRPC Publisher] Consumer cursor behind buffer. "
				    "subscriber_id="
				    + std::to_string(consumer_id));
				break;
			}

			const uint64_t index = consumer->next_seq - base_seq_;
			if (index >= log_.size()) {
				logger->log_error(
				    "[gRPC Publisher] Log index out of range. subscriber_id="
				    + std::to_string(consumer_id));
				break;
			}

			next = log_[static_cast<size_t>(index)].batch;
			next_seq_candidate = consumer->next_seq;
		}

		if (!next) {
			continue;
		}

		if (!writer->Write(*next)) {
			break;
		}

		{
			std::lock_guard<std::mutex> lock(log_mu_);
			if (consumer->next_seq == next_seq_candidate) {
				++consumer->next_seq;
				gc_locked_();
			}
		}
		log_cv_.notify_all();
	}

	{
		std::lock_guard<std::mutex> lock(log_mu_);
		consumers_.erase(consumer_id);
		gc_locked_();
	}
	log_cv_.notify_all();

	logger->log_info("[gRPC Publisher] Consumer disconnected. subscriber_id="
	                 + std::to_string(consumer_id));
	return grpc::Status::OK;
}

void GrpcPublisher::gc_locked_() {
	if (consumers_.empty()) {
		log_.clear();
		base_seq_ = next_seq_;
		return;
	}

	uint64_t min_next = std::numeric_limits<uint64_t>::max();
	for (const auto &entry : consumers_) {
		min_next = std::min(min_next, entry.second->next_seq);
	}
	if (min_next <= base_seq_) {
		return;
	}

	const uint64_t drop = min_next - base_seq_;
	for (uint64_t i = 0; i < drop && !log_.empty(); ++i) {
		log_.pop_front();
	}
	base_seq_ = min_next;
}

void GrpcPublisher::shutdown_server_() {
	if (!server_started_) {
		return;
	}
	shutting_down_.store(true, std::memory_order_release);

	{
		std::lock_guard<std::mutex> lock(log_mu_);
		flush_pending_batch_locked_(false);
	}
	log_cv_.notify_all();

	{
		std::unique_lock<std::mutex> lock(log_mu_);
		log_cv_.wait(lock,
		             [this]() { return log_.empty() || consumers_.empty(); });

		if (!log_.empty() && consumers_.empty()) {
			logger->log_error(
			    "[gRPC Publisher] Shutdown reached with pending batches but "
			    "no active consumers; batches cannot be drained.");
		}
	}

	if (server_) {
		server_->Shutdown();
	}
	if (server_thread_.joinable()) {
		server_thread_.join();
	}
	{
		std::lock_guard<std::mutex> lock(log_mu_);
		consumers_.clear();
		gc_locked_();
	}
	server_started_ = false;
}
