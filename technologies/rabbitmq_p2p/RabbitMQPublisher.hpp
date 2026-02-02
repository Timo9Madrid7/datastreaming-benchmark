#pragma once

#include <amqpcpp.h>
#include <amqpcpp/libevent.h>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "IPublisher.hpp"

class Logger;
struct Payload;

class RabbitMQPublisher : public IPublisher {
  public:
	RabbitMQPublisher(std::shared_ptr<Logger> logger);
	~RabbitMQPublisher() override;

	void initialize() override;
	void send_message(const Payload &message, std::string &topic) override;
	void log_configuration() override;

  private:
	void start_event_loop_();
	void stop_event_loop_();
	bool wait_ready_(int timeout_ms);
	std::string build_amqp_url_(const std::string &endpoint,
	                            const std::string &port) const;

	std::string amqp_url_;
	std::string exchange_;

	struct event_base *event_base_{nullptr};
	std::unique_ptr<AMQP::LibEventHandler> handler_;
	std::unique_ptr<AMQP::TcpConnection> connection_;
	std::unique_ptr<AMQP::TcpChannel> channel_;
	size_t max_out_queue_bytes_;

	std::thread io_thread_;
	std::mutex ready_mu_;
	std::condition_variable ready_cv_;
	std::atomic<bool> ready_{false};
	std::atomic<bool> terminated_{false};
};