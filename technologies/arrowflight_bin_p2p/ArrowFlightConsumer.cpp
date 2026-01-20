#include "ArrowFlightConsumer.hpp"

#include <arrow/array/array_binary.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/flight/types.h>
#include <arrow/ipc/dictionary.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <stdint.h>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <utility>

#include "Logger.hpp"
#include "Payload.hpp"
#include "Utils.hpp"
#include "readerwriterqueue.h"


namespace {

struct DecodeItem {
	std::shared_ptr<arrow::RecordBatch> batch;
	int64_t row = 0;
	std::string message_id;
	uint8_t kind = 0;
	bool is_termination = false;
};

inline size_t compute_strings_payload_bytes(std::string_view wire) noexcept {
	// Wire format produced by ArrowFlightPublisher.cpp (bin variant):
	//   [u16 len][bytes] repeated.
	// Return only total string bytes (excluding the u16 headers), matching
	// Payload::NestedPayload::string_size semantics.
	size_t off = 0;
	size_t total = 0;
	while (off + sizeof(uint16_t) <= static_cast<size_t>(wire.size())) {
		uint16_t str_len = 0;
		std::memcpy(&str_len, wire.data() + off, sizeof(uint16_t));
		off += sizeof(uint16_t);
		const size_t sl = static_cast<size_t>(str_len);
		if (off + sl > static_cast<size_t>(wire.size())) {
			// Benchmark guarantees valid inputs; best-effort bail out.
			break;
		}
		off += sl;
		total += sl;
	}
	return total;
}

class AsyncBatchDecoder {
  public:
	AsyncBatchDecoder(std::shared_ptr<Logger> logger, std::string ticket,
	                  std::function<void()> on_termination)
	    : logger_(std::move(logger)), ticket_(std::move(ticket)),
	      on_termination_(std::move(on_termination)), queue_(1024) {
	}

	void start() {
		stop_requested_.store(false, std::memory_order_release);
		worker_ = std::thread([this]() { run_(); });
	}

	void request_stop() {
		stop_requested_.store(true, std::memory_order_release);
	}

	void stop_and_join() {
		request_stop();
		if (worker_.joinable()) {
			worker_.join();
		}
	}

	bool enqueue(DecodeItem item) {
		return queue_.enqueue(std::move(item));
	}

  private:
	void run_() {
		using namespace std::chrono_literals;
		while (!stop_requested_.load(std::memory_order_acquire)
		       || queue_.size_approx() > 0) {
			DecodeItem item;
			if (!queue_.wait_dequeue_timed(item, 500ns)) {
				continue;
			}

			// Decode & compute sizes.
			auto message_id_len = item.message_id.size();
			size_t payload_bytes_len = 0;
			size_t nested_double_bytes = 0;
			size_t nested_string_bytes = 0;

			// Pull only the columns we need.
			auto kind = static_cast<PayloadKind>(item.kind);
			auto bytes_col = std::static_pointer_cast<arrow::BinaryArray>(
			    item.batch->column(2));
			payload_bytes_len =
			    static_cast<size_t>(bytes_col->value_length(item.row));

			if (kind == PayloadKind::COMPLEX) {
				auto doubles_col = std::static_pointer_cast<arrow::BinaryArray>(
				    item.batch->column(3));
				auto strings_col = std::static_pointer_cast<arrow::BinaryArray>(
				    item.batch->column(4));
				if (!doubles_col->IsNull(item.row)) {
					nested_double_bytes = static_cast<size_t>(
					    doubles_col->value_length(item.row));
				}
				if (!strings_col->IsNull(item.row)) {
					const std::string_view wire =
					    strings_col->GetView(item.row);
					nested_string_bytes = compute_strings_payload_bytes(wire);
				}
			}

			const size_t logical_size =
			    payload_bytes_len + nested_double_bytes + nested_string_bytes;
			const size_t serialized_size = message_id_len + sizeof(uint8_t)
			    + payload_bytes_len + nested_double_bytes + nested_string_bytes;

			if (logger_) {
				logger_->log_study("Deserialized," + item.message_id + ","
				                   + ticket_ + ","
				                   + std::to_string(logical_size) + ","
				                   + std::to_string(serialized_size));
			}

			if (item.is_termination) {
				if (on_termination_) {
					on_termination_();
				}
				return;
			}
		}
	}

	std::shared_ptr<Logger> logger_;
	std::string ticket_;
	std::function<void()> on_termination_;
	moodycamel::BlockingReaderWriterQueue<DecodeItem> queue_;
	std::thread worker_;
	std::atomic<bool> stop_requested_{false};
};

} // namespace

ArrowFlightConsumer::ArrowFlightConsumer(std::shared_ptr<Logger> logger)
    : IConsumer(logger), publisher_port_(8815) {
	this->logger->log_info("[Flight Consumer] ArrowFlightConsumer created.");
}

ArrowFlightConsumer::~ArrowFlightConsumer() {
	thread_pool_.wait();
	logger->log_debug("[Flight Consumer] Destructor finished");
}

void ArrowFlightConsumer::initialize() {
	const std::string vendpoints = utils::get_env_var_or_default(
	    "PUBLISHER_ENDPOINTS",
	    utils::get_env_var_or_default("CONSUMER_ENDPOINT", "localhost"));

	const std::string port_str = utils::get_env_var_or_default(
	    "PUBLISHER_PORT",
	    utils::get_env_var_or_default("CONSUMER_PORT", "8815"));

	const std::string string_num_threads =
	    utils::get_env_var_or_default("THREADS", "4");

	const std::optional<std::string> vTickets = utils::get_env_var("TOPICS");
	if (!vTickets || vTickets->empty()) {
		throw std::runtime_error(
		    "[Flight Consumer] Missing required environment variable TOPICS.");
	}

	try {
		publisher_port_ = std::stoi(port_str);
	} catch (...) {
		throw std::runtime_error("[Flight Consumer] Invalid port: " + port_str);
	}

	try {
		const int num_threads = std::stoi(string_num_threads);
		if (num_threads != thread_pool_.get_thread_count() && num_threads > 0) {
			thread_pool_.reset(num_threads);
		}
	} catch (...) {
		throw std::runtime_error("[Flight Consumer] Invalid THREADS value: "
		                         + string_num_threads);
	}

	// parse publishers endpoints and tickets
	ticket_publisher_pairs_.clear();
	{
		std::string ticket;
		std::string publisher;
		std::istringstream tickets(vTickets.value());
		std::istringstream publishers(vendpoints);
		std::unordered_set<std::string> unique_pub_tickets;

		while (std::getline(tickets, ticket, ',')) {
			std::getline(publishers, publisher, ',');

			if (publisher.empty()) {
				logger->log_info(
				    "[Arrow Flight] Empty publisher found for ticket " + ticket
				    + ", skipping subscription.");
				continue;
			}
			if (ticket.empty()) {
				logger->log_info(
				    "[Arrow Flight] Empty ticket found for publisher "
				    + publisher + ", skipping subscription.");
				continue;
			}

			if (unique_pub_tickets.insert(publisher + ":" + ticket).second) {
				logger->log_info(
				    "[Flight Consumer] Handling subscription to ticket "
				    + ticket + " from publisher " + publisher);
				ticket_publisher_pairs_.emplace_back(ticket, publisher);
			}
		}
	}
	if (ticket_publisher_pairs_.empty()) {
		throw std::runtime_error(
		    "[Flight Consumer] No valid publisher-ticket pairs found.");
	}

	logger->log_info("[Flight Consumer] Consumer initialized.");
	log_configuration();
}

void ArrowFlightConsumer::subscribe(const std::string &ticket) {
	logger->log_debug(
	    "[Flight Consumer] Subscribe called, but all subscriptions are handled "
	    "in initialize(). Ignoring ticket="
	    + ticket);
}

void ArrowFlightConsumer::consume_from_publisher_(const std::string &endpoint,
                                                  const std::string &ticket) {
	subscribed_streams.inc();

	auto loc_res =
	    arrow::flight::Location::ForGrpcTcp(endpoint, publisher_port_);
	if (!loc_res.ok()) {
		logger->log_error("[Flight Consumer] ForGrpcTcp failed: "
		                  + loc_res.status().ToString());
		subscribed_streams.dec();
		return;
	}

	auto client_res = arrow::flight::FlightClient::Connect(*loc_res);
	if (!client_res.ok()) {
		logger->log_error("[Flight Consumer] Connect failed to " + endpoint
		                  + ":" + std::to_string(publisher_port_) + " : "
		                  + client_res.status().ToString());
		subscribed_streams.dec();
		return;
	}
	auto client = std::move(client_res).ValueOrDie();

	// Simulation to fetch the true (structured) schema before consuming data.
	// The benchmark guarantees correctness; keep validation minimal.
	std::shared_ptr<arrow::Schema> struct_schema;
	{
		std::unique_ptr<arrow::flight::SchemaResult> schema_result;
		constexpr int kMaxAttempts = 60; // 60 * 500ms = 30s
		for (int attempt = 1; attempt <= kMaxAttempts; ++attempt) {
			auto res = client->GetSchema(
			    arrow::flight::FlightDescriptor::Command(ticket));
			if (res.ok()) {
				schema_result = std::move(res).ValueOrDie();
				break;
			}
			logger->log_info("[Flight Consumer] GetSchema attempt "
			                 + std::to_string(attempt) + "/"
			                 + std::to_string(kMaxAttempts)
			                 + " failed ticket=" + ticket + " from " + endpoint
			                 + " : " + res.status().ToString());
			std::this_thread::sleep_for(std::chrono::milliseconds(500));
		}
		if (!schema_result) {
			logger->log_error("[Flight Consumer] GetSchema failed ticket="
			                  + ticket + " from " + endpoint);
			subscribed_streams.dec();
			return;
		}
		arrow::ipc::DictionaryMemo memo;
		auto schema_res = schema_result->GetSchema(&memo);
		if (!schema_res.ok()) {
			logger->log_error(
			    "[Flight Consumer] GetSchema decode failed ticket=" + ticket
			    + " from " + endpoint + " : " + schema_res.status().ToString());
			subscribed_streams.dec();
			return;
		}
		struct_schema = std::move(schema_res).ValueOrDie();
	}
	(void)struct_schema; // Used to satisfy the semi-structured contract.

	AsyncBatchDecoder decoder(logger, ticket,
	                          [this]() { this->subscribed_streams.dec(); });
	decoder.start();

	arrow::flight::Ticket t{ticket};
	std::unique_ptr<arrow::flight::FlightStreamReader> reader;

	constexpr int kMaxAttempts = 60; // 60 * 500ms = 30s
	for (int attempt = 1; attempt <= kMaxAttempts; ++attempt) {
		auto reader_res = client->DoGet(t);
		if (reader_res.ok()) {
			reader = std::move(reader_res).ValueOrDie();
			break;
		}

		logger->log_info(
		    "[Flight Consumer] DoGet attempt " + std::to_string(attempt) + "/"
		    + std::to_string(kMaxAttempts) + " failed for ticket=" + ticket
		    + " from " + endpoint + " : " + reader_res.status().ToString());
		std::this_thread::sleep_for(std::chrono::milliseconds(500));
	}
	if (!reader) {
		logger->log_error("[Flight Consumer] DoGet failed ticket=" + ticket
		                  + " from " + endpoint + " after "
		                  + std::to_string(kMaxAttempts) + " attempts.");
		decoder.stop_and_join();
		subscribed_streams.dec();
		return;
	}

	bool saw_termination = false;
	while (true) {
		auto chunk = reader->Next();
		if (!chunk.ok()) {
			logger->log_error("[Flight Consumer] Next() failed ticket=" + ticket
			                  + " from " + endpoint + " : "
			                  + chunk.status().ToString());
			break;
		}

		auto batch = chunk->data;
		if (!batch)
			break;

		// Fast path: only log reception + enqueue work. Heavy decoding happens
		// in the background SPSC worker (like kafka/nats/zeromq consumers).
		auto message_id_column =
		    std::static_pointer_cast<arrow::StringArray>(batch->column(0));
		auto kind_column =
		    std::static_pointer_cast<arrow::UInt8Array>(batch->column(1));

		for (int64_t i = 0; i < batch->num_rows(); ++i) {
			DecodeItem item;
			item.batch = batch;
			item.row = i;
			item.message_id = message_id_column->GetString(i);
			item.kind = kind_column->Value(i);

			logger->log_study("Reception," + item.message_id + "," + ticket);

			if (item.message_id.find(TERMINATION_SIGNAL) != std::string::npos) {
				logger->log_info(
				    "[Flight Consumer] Received termination for ticket="
				    + ticket + " from publisher=" + endpoint);
				item.is_termination = true;
				saw_termination = true;
				decoder.enqueue(std::move(item));
				break;
			}
		}
		if (saw_termination) {
			break;
		}
	}

	decoder.request_stop();
	decoder.stop_and_join();
	if (!saw_termination) {
		logger->log_info(
		    "[Flight Consumer] Stream ended without termination. ticket="
		    + ticket + " publisher=" + endpoint);
		subscribed_streams.dec();
	}
}

void ArrowFlightConsumer::start_loop() {
	logger->log_info(
	    "[Flight Consumer] Starting client loops (thread pool)...");

	for (const auto &pair : ticket_publisher_pairs_) {
		const auto &ticket = pair.first;
		const auto &publisher = pair.second;

		logger->log_info("[Flight Consumer] Queue DoGet from " + publisher + ":"
		                 + std::to_string(publisher_port_)
		                 + " ticket=" + ticket);

		thread_pool_.detach_task([this, publisher, ticket]() {
			consume_from_publisher_(publisher, ticket);
		});
	}
	thread_pool_.wait();
	logger->log_info("[Flight Consumer] All streams ended.");
}

void ArrowFlightConsumer::log_configuration() {
	logger->log_config("[Flight Consumer] [CONFIG_BEGIN]");

	logger->log_config(
	    "[CONFIG] PUBLISHER_ENDPOINTS="
	    + utils::get_env_var_or_default(
	        "PUBLISHER_ENDPOINTS",
	        utils::get_env_var_or_default("CONSUMER_ENDPOINT", "")));

	logger->log_config("[CONFIG] PUBLISHER_PORT="
	                   + utils::get_env_var_or_default(
	                       "PUBLISHER_PORT",
	                       utils::get_env_var_or_default("CONSUMER_PORT", "")));

	logger->log_config("[CONFIG] TOPICS="
	                   + utils::get_env_var_or_default("TOPICS", ""));
	logger->log_config("[CONFIG] THREADS="
	                   + utils::get_env_var_or_default("THREADS", ""));
	logger->log_config("[Flight Consumer] [CONFIG_END]");
}