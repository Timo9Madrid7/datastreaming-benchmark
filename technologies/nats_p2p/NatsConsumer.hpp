#pragma once

#include <atomic>      // for atomic
#include <memory>      // for shared_ptr, unique_ptr
#include <nats/nats.h> // for natsSubscription_Destroy, natsConnect...
#include <string>      // for string

#include "Deserializer.hpp"
#include "IConsumer.hpp" // for IConsumer


class Logger;

class NatsConsumer : public IConsumer {
  public:
	NatsConsumer(std::shared_ptr<Logger> logger);
	~NatsConsumer() override;

	void initialize() override;
	void subscribe(const std::string &subject) override;
	void start_loop() override;
	void log_configuration() override;

  private:
	using NatsConnectionPtr =
	    std::unique_ptr<natsConnection, decltype(&natsConnection_Destroy)>;
	using NatsSubscriptionPtr =
	    std::unique_ptr<natsSubscription, decltype(&natsSubscription_Destroy)>;
	NatsConnectionPtr connection_;
	NatsSubscriptionPtr subscription_;

	std::string nats_url_;

	utils::Deserializer deserializer_;
	std::atomic<bool> stop_receiving_{false};
};