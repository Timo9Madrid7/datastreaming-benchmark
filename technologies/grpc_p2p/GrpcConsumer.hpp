#pragma once

#include <atomic>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>
#include <unordered_set>

#include "IConsumer.hpp"
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

	std::string connect_endpoint_;
	std::unordered_set<std::string> topics_;

	std::shared_ptr<grpc::Channel> channel_;
	std::unique_ptr<streaming::Streamer::Stub> stub_;

	std::unique_ptr<grpc::ClientContext> context_;
	std::unique_ptr<grpc::ClientReader<streaming::WireMessage>> reader_;

	std::atomic<bool> stop_receiving_{false};
};
