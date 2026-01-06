#pragma once

#include <memory>
#include <nats/nats.h>
#include <string>

#include "IPublisher.hpp"

class Logger;
struct Payload;

class NatsPublisher : public IPublisher {
  public:
	NatsPublisher(std::shared_ptr<Logger> logger);
	~NatsPublisher() override;

	void initialize() override;
	bool serialize(const Payload &message, void *out) override;
	void send_message(const Payload &message, std::string &subject) override;
	void log_configuration() override;

  private:
	using NatsConnectionPtr =
	    std::unique_ptr<natsConnection, decltype(&natsConnection_Destroy)>;
	NatsConnectionPtr connection_;
	std::string nats_url_;
};