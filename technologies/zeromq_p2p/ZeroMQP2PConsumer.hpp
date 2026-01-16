#ifndef ZEROMQP2P_CONSUMER_HPP
#define ZEROMQP2P_CONSUMER_HPP

#include <memory>
#include <string>
#include <zmq.hpp>

#include "Deserializer.hpp"
#include "IConsumer.hpp"
#include "Logger.hpp"


struct Payload;

class ZeroMQP2PConsumer : public IConsumer {
  private:
	zmq::context_t context;
	zmq::socket_t subscriber;

	utils::Deserializer deserializer_;
	std::atomic<bool> stop_receiving_{false};

  public:
	ZeroMQP2PConsumer(std::shared_ptr<Logger> logger);
	~ZeroMQP2PConsumer();

	void initialize() override;
	void subscribe(const std::string &topic) override;
	void start_loop() override;
	bool deserialize(const void *raw_message, size_t len, std::string &topic,
	                 Payload &out);
	bool deserialize_id(const void *raw_message, size_t len, std::string &topic,
	                    Payload &out);
	void log_configuration() override;
};

#endif // ZEROMQP2P_CONSUMER_HPP
