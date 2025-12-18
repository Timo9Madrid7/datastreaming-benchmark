#pragma once

#include <arrow/flight/api.h>
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/record_batch.h>
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <utility>
#include <vector>

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

	std::queue<std::string> task_queue_; // tickets to process
	std::queue<std::pair<std::string, std::shared_ptr<arrow::RecordBatch>>>
	    batch_queue_; // batch per ticket

	std::mutex task_mutex_;
	std::mutex batch_mutex_;
	std::condition_variable task_cv_;
	std::condition_variable batch_cv_;
	size_t num_threads_;
	std::vector<std::thread> thread_pool_;
	std::atomic<bool> stop_{false};

	inline size_t default_thread_pool_size();
	void worker_loop_();
	void do_get_(std::string ticket);
};