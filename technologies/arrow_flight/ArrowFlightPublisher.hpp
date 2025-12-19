#pragma once

#include <arrow/api.h>
#include <arrow/array/builder_binary.h>
#include <arrow/flight/api.h>
#include <arrow/flight/client.h>
#include <arrow/flight/server.h>
#include <arrow/flight/types.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/util/byte_size.h>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "IPublisher.hpp"

class ArrowFlightPublisher : public IPublisher {
  public:
	ArrowFlightPublisher(std::shared_ptr<Logger> logger);
	~ArrowFlightPublisher() override;

	void initialize() override;
	bool serialize(const std::vector<Payload> &messages, void *out) override;
	void send_message(const Payload &message, std::string ticket) override;
	void log_configuration() override;

  private:
	const static std::shared_ptr<arrow::Schema> schema_;

	struct BatchBuilder {
		arrow::StringBuilder message_id_builder;
		arrow::UInt8Builder kind_builder;
		arrow::BinaryBuilder data_builder;

		std::deque<std::string> publication_logs;

		uint64_t rows;
		uint64_t byte_size;

		BatchBuilder() : rows(0), byte_size(0) {
		}

		void reset() {
			message_id_builder.Reset();
			kind_builder.Reset();
			data_builder.Reset();
			publication_logs.clear();
			rows = 0;
			byte_size = 0;
		}

		bool make_batch(std::shared_ptr<arrow::RecordBatch> &batch) {
			std::shared_ptr<arrow::Array> message_id_array;
			std::shared_ptr<arrow::Array> kind_array;
			std::shared_ptr<arrow::Array> data_array;
			arrow::Status status;

			status = message_id_builder.Finish(&message_id_array);
			if (!status.ok()) {
				return false;
			}

			status = kind_builder.Finish(&kind_array);
			if (!status.ok()) {
				return false;
			}

			status = data_builder.Finish(&data_array);
			if (!status.ok()) {
				return false;
			}

			batch = arrow::RecordBatch::Make(
			    schema_, rows, {message_id_array, kind_array, data_array});

			return true;
		}
	};

	std::unordered_map<std::string, BatchBuilder> ticket_batch_builders_;
	uint64_t MAX_BATCH_BYTES;

	arrow::flight::Location location_;
	std::unique_ptr<arrow::flight::FlightClient> publisher_;

	bool _do_put_(const std::string& ticket, BatchBuilder &batch_builder);
};