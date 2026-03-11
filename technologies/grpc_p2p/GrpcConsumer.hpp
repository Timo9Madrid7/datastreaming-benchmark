#pragma once

#include <atomic>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>

#include "IConsumer.hpp"
#include "readerwriterqueue.h"
#include "streaming.grpc.pb.h"

class Logger;

class GrpcConsumer : public IConsumer {
  public:
	GrpcConsumer(std::shared_ptr<Logger> logger);
	~GrpcConsumer() override;

	void initialize() override;
	void subscribe(const std::string &topic) override;
	void start_loop() override;
	void log_configuration() override;

  private:
	void close_stream_();
	void start_worker_();
	void stop_worker_();
	void enqueue_latest_(std::shared_ptr<streaming::WireMessage> msg);
	void worker_loop_();

	std::string connect_endpoint_;
	std::unordered_set<std::string> topics_;

	std::shared_ptr<grpc::Channel> channel_;
	std::unique_ptr<streaming::Streamer::Stub> stub_;

	std::unique_ptr<grpc::ClientContext> context_;
	std::unique_ptr<grpc::ClientReader<streaming::WireMessage>> reader_;

	std::unique_ptr<moodycamel::BlockingReaderWriterQueue<
	    std::shared_ptr<streaming::WireMessage>>>
	    queue_;
	size_t queue_capacity_{1024};
	std::thread worker_thread_;
	std::atomic<bool> worker_running_{false};

	std::atomic<bool> stop_receiving_{false};
};
