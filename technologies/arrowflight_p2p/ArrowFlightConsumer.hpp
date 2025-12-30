#pragma once

#include <arrow/flight/client.h>
#include <memory>
#include <string>
#include <vector>

#include "BS_thread_pool.hpp"
#include "IConsumer.hpp"
#include "Logger.hpp"

struct Payload;

class ArrowFlightConsumer : public IConsumer {
  public:
	ArrowFlightConsumer(std::shared_ptr<Logger> logger);
	~ArrowFlightConsumer() override;

	void initialize() override;
	void subscribe(const std::string &ticket) override;
	void start_loop() override;
	bool deserialize(const void *raw_message, size_t len,
	                 Payload &out) override;
	void log_configuration() override;

  private:
	std::vector<std::pair<std::string, std::string>> ticket_publisher_pairs_;
	int publisher_port_;

	//FIXME: hardcoded number of threads, make it configurable
	BS::pause_thread_pool thread_pool_{4};

	void consume_from_publisher_(const std::string &endpoint,
	                             const std::string &ticket);
};