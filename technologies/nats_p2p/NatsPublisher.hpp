#pragma once

#include <memory>
#include <nats.h>

#include "IPublisher.hpp"
#include "Logger.hpp"

class NatsPublisher : public IPublisher {
  public:
	NatsPublisher(std::shared_ptr<Logger> logger);
	~NatsPublisher() override;

	void initialize() override;
	bool serialize(const Payload &message, void *out) override;
	void send_message(const Payload &message, std::string &subject) override;
	void log_configuration() override;

  private:
	std::unique_ptr<natsConnection> connection_;
	std::string nats_url_;
};