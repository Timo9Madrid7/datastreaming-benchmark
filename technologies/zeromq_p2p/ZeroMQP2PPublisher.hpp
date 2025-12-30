#ifndef ZEROMQP2P_PUBLISHER_HPP
#define ZEROMQP2P_PUBLISHER_HPP

#include <string>
#include <zmq.hpp>

#include "IPublisher.hpp"
#include "Logger.hpp"
#include "Payload.hpp"

class ZeroMQP2PPublisher : public IPublisher {
  private:
	zmq::context_t context;
	zmq::socket_t publisher;

	std::string endpoint;

  private:
	bool serialize(const Payload &message, void *out) override;
	bool serialize(const Payload &message, std::string topic, void *out);

	void log_configuration() override;

  public:
	ZeroMQP2PPublisher(std::shared_ptr<Logger> logger);
	~ZeroMQP2PPublisher();

	void initialize() override;
	void send_message(const Payload &message, std::string &topic) override;
};

#endif // ZEROMQ_PUBLISHER_HPP
