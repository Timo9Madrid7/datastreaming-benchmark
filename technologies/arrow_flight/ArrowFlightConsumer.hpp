#pragma once

#include <arrow/flight/api.h>
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/record_batch.h>
#include <memory>
#include <string>
#include <vector>

#include "BS_thread_pool.hpp"
#include "IConsumer.hpp"

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
	arrow::flight::Location location_;      // similar to broker
	std::vector<std::string> ticket_names_; // similar to topic names
	arrow::flight::FlightCallOptions call_options_;

	std::unique_ptr<arrow::flight::FlightClient> consumer_;

	BS::thread_pool<BS::tp::pause> thread_pool_{8}; // default to 8 threads

	void _do_get_(std::string ticket);
};