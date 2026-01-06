#pragma once

#include <memory>
#include <nats.h>
#include <string.h>

#include "IConsumer.hpp"
#include "Logger.hpp"

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
	std::unique_ptr<natsConnection> connection_;
	std::unique_ptr<natsSubscription> subscription_;
	std::string nats_url_;
};