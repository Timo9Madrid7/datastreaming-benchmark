#pragma once

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/flight/server.h>
#include <arrow/flight/types.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "IPublisher.hpp"
#include "Payload.hpp"

class ArrowFlightPublisher : public IPublisher {
  public:
	ArrowFlightPublisher(std::shared_ptr<Logger> logger);
	~ArrowFlightPublisher() override;

	void initialize() override;
	bool serialize(const Payload &message, void *out);
	void send_message(const Payload &message, std::string &ticket) override;
	void log_configuration() override;

	struct TicketState {
		struct Entry {
			uint64_t seq;
			std::shared_ptr<arrow::RecordBatch> batch;
		};
		struct ConsumerState {
			uint64_t next_seq = 0;
		};

		std::mutex m;
		std::condition_variable cv;
		std::deque<Entry> q;
		uint64_t base_seq = 0; // seq of q.front() (if q not empty)
		uint64_t next_seq = 0; // seq to assign to next pushed entry
		bool finished = false; // true after termination enqueued

		uint64_t next_consumer_id = 1;
		std::unordered_map<uint64_t, std::shared_ptr<ConsumerState>> consumers;
	};

  private:
	const static std::shared_ptr<arrow::Schema> schema_;

	struct BatchBuilder {
		arrow::StringBuilder message_id_builder;
		arrow::UInt8Builder kind_builder;
		arrow::BinaryBuilder data_builder;
		std::unique_ptr<arrow::ListBuilder> doubles_builder;
		std::unique_ptr<arrow::ListBuilder> strings_builder;

		std::deque<std::string> publication_logs;

		uint64_t rows;
		uint64_t byte_size;

		BatchBuilder() : rows(0), byte_size(0) {
			auto *pool = arrow::default_memory_pool();
			// schema_: [message_id, kind, bytes, doubles, strings]
			doubles_builder = std::make_unique<arrow::ListBuilder>(
			    pool, std::make_shared<arrow::DoubleBuilder>(pool));
			strings_builder = std::make_unique<arrow::ListBuilder>(
			    pool, std::make_shared<arrow::StringBuilder>(pool));
		}

		void reset() {
			message_id_builder.Reset();
			kind_builder.Reset();
			data_builder.Reset();
			if (doubles_builder)
				doubles_builder->Reset();
			if (strings_builder)
				strings_builder->Reset();
			publication_logs.clear();
			rows = 0;
			byte_size = 0;
		}

		bool make_batch(std::shared_ptr<arrow::RecordBatch> &batch) {
			std::shared_ptr<arrow::Array> message_id_array;
			std::shared_ptr<arrow::Array> kind_array;
			std::shared_ptr<arrow::Array> data_array;
			std::shared_ptr<arrow::Array> doubles_array;
			std::shared_ptr<arrow::Array> strings_array;
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
			if (!doubles_builder || !strings_builder)
				return false;
			status = doubles_builder->Finish(&doubles_array);
			if (!status.ok())
				return false;
			status = strings_builder->Finish(&strings_array);
			if (!status.ok())
				return false;

			batch = arrow::RecordBatch::Make(
			    schema_, rows,
			    {message_id_array, kind_array, data_array, doubles_array,
			     strings_array});

			return true;
		}
	};

	class FlightServerLight : public arrow::flight::FlightServerBase {
	  public:
		explicit FlightServerLight(ArrowFlightPublisher *publisher)
		    : publisher_(publisher) {
		}
		~FlightServerLight() override = default;

		arrow::Status DoGet(
		    const arrow::flight::ServerCallContext &context,
		    const arrow::flight::Ticket &request,
		    std::unique_ptr<arrow::flight::FlightDataStream> *stream) override;

	  private:
		ArrowFlightPublisher *publisher_;
	};

	// batching
	std::unordered_map<std::string, BatchBuilder> ticket_batch_builders_;
	uint64_t MAX_BATCH_BYTES;

	// server
	arrow::flight::Location location_;
	std::unique_ptr<FlightServerLight> server_;
	std::thread server_thread_;
	std::atomic_bool server_started_{false};

	// ticket -> queue
	std::mutex streams_mu_;
	std::unordered_map<std::string, std::shared_ptr<TicketState>> streams_;

	std::shared_ptr<TicketState> get_or_create_stream_(const std::string &ticket);
	void enqueue_batch_(const std::string &ticket,
	                    const std::shared_ptr<arrow::RecordBatch> &batch,
	                    bool is_termination);
};