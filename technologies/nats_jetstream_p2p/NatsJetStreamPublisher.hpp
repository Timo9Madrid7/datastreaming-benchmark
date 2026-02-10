#pragma once

#include <memory>
#include <nats/nats.h>
#include <string>

#include "IPublisher.hpp"

class Logger;
struct Payload;

class NatsJetStreamPublisher : public IPublisher {
  public:
	NatsJetStreamPublisher(std::shared_ptr<Logger> logger);
	~NatsJetStreamPublisher() override;

	void initialize() override;
	void send_message(const Payload &message, std::string &subject) override;
	void log_configuration() override;

  private:
	using NatsConnectionPtr =
	    std::unique_ptr<natsConnection, decltype(&natsConnection_Destroy)>;
	using JsContextPtr = std::unique_ptr<jsCtx, decltype(&jsCtx_Destroy)>;

	NatsConnectionPtr connection_;
	JsContextPtr js_;
	std::string nats_url_;
	std::string stream_name_;
};
