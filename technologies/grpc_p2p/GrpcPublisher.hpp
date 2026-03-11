#pragma once

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "BS_thread_pool.hpp"
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
	      grpc::ServerWriter<streaming::WireMessage> *writer) override;

  private:
	struct Subscriber {
		explicit Subscriber(uint64_t sid) : id(sid) {
		}

		uint64_t id;
		std::mutex mu;
		std::condition_variable cv;
		std::deque<std::shared_ptr<const streaming::WireMessage>> queue;
		bool closed{false};
	};

	void enqueue_to_subscriber_(
	    const std::shared_ptr<Subscriber> &subscriber,
	    const std::shared_ptr<const streaming::WireMessage> &msg);
	void shutdown_server_();

	std::string endpoint_;
	// size_t max_queue_per_consumer_{1024};

	std::unique_ptr<grpc::Server> server_;
	std::thread server_thread_;

	std::mutex subscribers_mu_;
	std::unordered_map<uint64_t, std::shared_ptr<Subscriber>> subscribers_;
	uint64_t next_subscriber_id_{1};

	BS::pause_thread_pool fanout_pool_{5};
	bool server_started_{false};
};
