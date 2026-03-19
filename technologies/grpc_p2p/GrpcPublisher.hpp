#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "IPublisher.hpp"
#include "streaming.grpc.pb.h"

class Logger;
struct Payload;

class GrpcPublisher : public IPublisher, public streaming::Streamer::Service {
  public:
	GrpcPublisher(std::shared_ptr<Logger> logger);
	~GrpcPublisher() override;

	void initialize() override;
	void send_message(const Payload &message, std::string &topic) override;
	void log_configuration() override;

	grpc::Status
	DoGet(grpc::ServerContext *context, const google::protobuf::Empty *request,
	      grpc::ServerWriter<streaming::WireBatch> *writer) override;

  private:
	struct ConsumerState {
		uint64_t next_seq{0};
	};

	struct LogEntry {
		uint64_t seq;
		std::shared_ptr<const streaming::WireBatch> batch;
	};

	void add_message_to_pending_batch_(const Payload &message,
	                                   const std::string &topic,
	                                   size_t row_size_bytes);
	void flush_pending_batch_locked_(bool is_termination_batch);
	void gc_locked_();
	void shutdown_server_();

	std::string endpoint_;
	size_t max_shared_queue_batches_{1024};
	size_t max_batch_bytes_{8 * 1024 * 1024};

	streaming::WireBatch pending_batch_;
	size_t pending_batch_bytes_{0};
	std::deque<std::string> pending_publication_logs_;

	std::unique_ptr<grpc::Server> server_;
	std::thread server_thread_;

	std::mutex log_mu_;
	std::condition_variable log_cv_;
	std::deque<LogEntry> log_;
	std::unordered_map<uint64_t, std::shared_ptr<ConsumerState>> consumers_;
	uint64_t base_seq_{0};
	uint64_t next_seq_{0};
	uint64_t next_consumer_id_{1};
	std::atomic<bool> shutting_down_{false};

	bool server_started_{false};
};
