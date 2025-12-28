#pragma once

#include <arrow/flight/api.h>
#include <arrow/flight/client.h>
#include <arrow/flight/server.h>
#include <arrow/flight/types.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "IConsumer.hpp"
#include "Logger.hpp"

struct Payload;

class ArrowFlightConsumer
    : public IConsumer,
      public std::enable_shared_from_this<ArrowFlightConsumer> {
	class FlightServerLight : public arrow::flight::FlightServerBase {
		arrow::Status
		DoPut(const arrow::flight::ServerCallContext &context,
		      std::unique_ptr<arrow::flight::FlightMessageReader> reader,
		      std::unique_ptr<arrow::flight::FlightMetadataWriter> writer)
		    override;

		ArrowFlightConsumer *consumer_ = nullptr;

	  public:
		explicit FlightServerLight(ArrowFlightConsumer *consumer);
		~FlightServerLight() override = default;
	};

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

	std::unique_ptr<FlightServerLight> server_;

	std::atomic_bool shutdown_requested_{false};
};