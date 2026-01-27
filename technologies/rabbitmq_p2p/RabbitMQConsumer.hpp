#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <amqpcpp.h>
#include <amqpcpp/libevent.h>

#include "Deserializer.hpp"
#include "IConsumer.hpp"

class Logger;
struct Payload;

class RabbitMQConsumer : public IConsumer {
  public:
	RabbitMQConsumer(std::shared_ptr<Logger> logger);
	~RabbitMQConsumer() override;

	void initialize() override;
	void subscribe(const std::string &topic) override;
	void start_loop() override;
	void log_configuration() override;

  private:
	void start_event_loop_();
	void stop_event_loop_();
	bool wait_ready_(int timeout_ms);
	std::string build_amqp_url_(const std::string &endpoint,
	                            const std::string &port) const;
	void setup_topic_queue_(const std::string &topic);

	std::string amqp_url_;
	std::string exchange_;
	std::vector<std::string> topics_;
	std::unordered_map<std::string, std::string> topic_queues_;

	struct event_base *event_base_{nullptr};
	std::unique_ptr<AMQP::LibEventHandler> handler_;
	std::unique_ptr<AMQP::TcpConnection> connection_;
	std::unique_ptr<AMQP::TcpChannel> channel_;

	std::thread io_thread_;
	std::mutex ready_mu_;
	std::condition_variable ready_cv_;
	std::atomic<bool> ready_{false};

	utils::Deserializer deserializer_;
	std::atomic<bool> stop_receiving_{false};
};