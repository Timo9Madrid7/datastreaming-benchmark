#pragma once

#include <atomic>
#include <memory>
#include <nats/nats.h>
#include <string>

#include "Deserializer.hpp"
#include "IConsumer.hpp"

class Logger;

class NatsJetStreamConsumer : public IConsumer {
  public:
	NatsJetStreamConsumer(std::shared_ptr<Logger> logger);
	~NatsJetStreamConsumer() override;

	void initialize() override;
	void subscribe(const std::string &subject) override;
	void start_loop() override;
	void log_configuration() override;

  private:
	using NatsConnectionPtr =
	    std::unique_ptr<natsConnection, decltype(&natsConnection_Destroy)>;
	using NatsSubscriptionPtr =
	    std::unique_ptr<natsSubscription, decltype(&natsSubscription_Destroy)>;
	using JsContextPtr = std::unique_ptr<jsCtx, decltype(&jsCtx_Destroy)>;

	NatsConnectionPtr connection_;
	NatsSubscriptionPtr subscription_;
	JsContextPtr js_;

	std::string nats_url_;
	std::string stream_name_;

	utils::Deserializer deserializer_;
	std::atomic<bool> stop_receiving_{false};
};
