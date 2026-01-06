#pragma once

#include <memory>
#include <nats/nats.h>
#include <string>

#include "IConsumer.hpp"

class Logger;
struct Payload;

class NatsConsumer : public IConsumer {
  public:
	NatsConsumer(std::shared_ptr<Logger> logger);
	~NatsConsumer() override;

	void initialize() override;
	void subscribe(const std::string &subject) override;
	void start_loop() override;
	bool deserialize(const void *raw_message, size_t len,
	                 Payload &out) override;
	void log_configuration() override;

  private:
	using NatsConnectionPtr =
	    std::unique_ptr<natsConnection, decltype(&natsConnection_Destroy)>;
	using NatsSubscriptionPtr =
	    std::unique_ptr<natsSubscription, decltype(&natsSubscription_Destroy)>;
	NatsConnectionPtr connection_;
	NatsSubscriptionPtr subscription_;

	std::string nats_url_;
};