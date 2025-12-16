#ifndef ZEROMQP2P_CONSUMER_HPP
#define ZEROMQP2P_CONSUMER_HPP

#include <set>
#include <string>
#include <zmq.hpp>

#include "IConsumer.hpp"
#include "Logger.hpp"

class ZeroMQP2PConsumer : public IConsumer {
  private:
	zmq::context_t context;
	zmq::socket_t subscriber;
	std::set<std::string> unique_publishers;

  public:
	ZeroMQP2PConsumer(std::shared_ptr<Logger> logger);
	~ZeroMQP2PConsumer();

	void initialize() override;
	void subscribe(const std::string &topic) override;
	Payload receive_message() override;
	bool deserialize(const void *raw_message, size_t len,
	                 Payload &out) override;

	void log_configuration() override;
};

#endif // ZEROMQP2P_CONSUMER_HPP
