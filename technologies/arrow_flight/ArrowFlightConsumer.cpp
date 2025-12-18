#include "ArrowFlightConsumer.hpp"

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type_fwd.h>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <sys/types.h>
#include <utility>

#include "Logger.hpp"
#include "Payload.hpp"
#include "Utils.hpp"

ArrowFlightConsumer::ArrowFlightConsumer(std::shared_ptr<Logger> logger)
    : IConsumer(logger), consumer_(nullptr) {
	// Set a timeout for receiving messages (10s)
	call_options_.timeout = std::chrono::seconds(10);
	num_threads_ = default_thread_pool_size();
	logger->log_info("[Flight Consumer] ArrowFlightConsumer created.");
}

ArrowFlightConsumer::~ArrowFlightConsumer() {
	stop_.store(true, std::memory_order_relaxed);
	task_cv_.notify_all();
	batch_cv_.notify_all();

	for (auto &thread : thread_pool_) {
		if (thread.joinable()) {
			thread.join();
		}
	}
	logger->log_debug("[Flight Consumer] Destructor finished");
}

inline size_t ArrowFlightConsumer::default_thread_pool_size() {
	size_t num_threads = std::thread::hardware_concurrency() * 2;
	return num_threads == 0 ? 2 : num_threads;
}

void ArrowFlightConsumer::initialize() {
	logger->log_info("Initializing");

	const std::string vendpoint =
	    utils::get_env_var_or_default("CONSUMER_ENDPOINT", "localhost");
	const std::string port_str =
	    utils::get_env_var_or_default("CONSUMER_PORT", "8815");
	const std::optional<std::string> vTickets = utils::get_env_var("TOPICS");

	std::string err_msg;
	int port;
	try {
		port = std::stoi(port_str);
	} catch (const std::invalid_argument &e) {
		err_msg = "[Flight Consumer] Invalid port number: " + port_str;
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	} catch (const std::out_of_range &e) {
		err_msg = "[Flight Consumer] Port number out of range: " + port_str;
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	}
	if (!vTickets || vTickets.value().empty()) {
		err_msg = "[Flight Consumer] Missing required environment "
		          "variable TOPICS.";
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	}

	auto loc_res = arrow::flight::Location::ForGrpcTcp(vendpoint, port);
	if (!loc_res.ok()) {
		logger->log_error("[Flight Consumer] ForGrpcTcp failed: "
		                  + loc_res.status().ToString());
		throw std::runtime_error("[Flight Consumer] ForGrpcTcp failed: "
		                         + loc_res.status().ToString());
	}
	location_ = *loc_res;

	auto client_res = arrow::flight::FlightClient::Connect(location_);
	if (!client_res.ok()) {
		logger->log_error("[Flight Consumer] Connect failed: "
		                  + client_res.status().ToString());
		throw std::runtime_error("[Flight Consumer] Connect failed: "
		                         + client_res.status().ToString());
	}
	consumer_ = std::move(*client_res);

	// Setup thread pool
	thread_pool_.reserve(num_threads_);
	for (size_t i = 0; i < num_threads_; ++i) {
		thread_pool_.emplace_back([this]() { worker_loop_(); });
	}
	logger->log_debug("[Flight Consumer] Initialized thread pool with "
	                  + std::to_string(num_threads_) + " threads.");

	std::istringstream tickets(vTickets.value_or(""));
	std::string ticket;
	std::unordered_set<std::string> unique_tickets;
	while (std::getline(tickets, ticket, ',')) {
		logger->log_debug("[Flight Consumer] Handling subscription to ticket "
		                  + ticket);
		if (!ticket.empty() && unique_tickets.insert(ticket).second) {
			logger->log_info("[Flight Consumer] Connecting to stream ("
			                 + vendpoint + ":" + port_str + "," + ticket + ")");
			subscribe(ticket); // add to thread pool
		}
	}

	logger->log_debug("[Flight Consumer] Subscription list will have size "
	                  + std::to_string(ticket_names_.size()));

	logger->log_info("[Flight Consumer] Consumer initialized and connected.");
	logger->log_study("Initialized");
	log_configuration();
}

void ArrowFlightConsumer::subscribe(const std::string &ticket) {
	logger->log_info("[Flight Consumer] Queued subscription for ticket: "
	                 + ticket);
	ticket_names_.push_back(ticket);
	subscribed_streams.inc();
	{
		std::lock_guard<std::mutex> lock_guard(task_mutex_);
		task_queue_.push(ticket);
	}
	task_cv_.notify_one();
}

bool ArrowFlightConsumer::deserialize(const void *raw_message, size_t len,
                                      Payload &out) {
	logger->log_error(
	    "[Flight Consumer] Flight does not need deserialization!");
	return false;
}

void ArrowFlightConsumer::start_loop() {
	while (true) {
		std::pair<std::string, std::shared_ptr<arrow::RecordBatch>> item;
		{
			std::unique_lock<std::mutex> lock(batch_mutex_);
			batch_cv_.wait(lock, [this]() {
				return stop_.load(std::memory_order_relaxed)
				    || !batch_queue_.empty();
			});

			if (stop_.load(std::memory_order_relaxed)) {
				break;
			} else if (batch_queue_.empty()) {
				continue;
			}

			item = std::move(batch_queue_.front());
			batch_queue_.pop();
		}

		auto message_id_column = std::static_pointer_cast<arrow::StringArray>(
		    item.second->column(0));
		auto kind_column =
		    std::static_pointer_cast<arrow::UInt8Array>(item.second->column(1));
		auto data_column = std::static_pointer_cast<arrow::BinaryArray>(
		    item.second->column(2));

		for (int64_t i = 0; i < item.second->num_rows(); ++i) {
			std::string message_id = message_id_column->GetString(i);
			PayloadKind kind = static_cast<PayloadKind>(kind_column->Value(i));
			size_t row_size = message_id_column->value_length(i)
			    + sizeof(uint8_t) + data_column->value_length(i);
			std::string_view data_view = data_column->GetView(i);
			size_t data_size = data_view.size();

			logger->log_study("Reception," + message_id + ","
			                  + std::to_string(data_size) + ',' + item.first
			                  + "," + std::to_string(data_size));

			if (message_id.find(TERMINATION_SIGNAL) != std::string::npos) {
				subscribed_streams.dec();

				logger->log_info(
				    "[Flight Consumer] Received termination signal for ticket: "
				    + item.first);

				if (subscribed_streams.get() == 0) {
					logger->log_info("[Flight Consumer] All streams "
					                 "terminated. Exiting.");
					stop_.store(true, std::memory_order_relaxed);
					task_cv_.notify_all();
					batch_cv_.notify_all();
					break;
				}
			}
		}
	}
}

void ArrowFlightConsumer::worker_loop_() {
	while (stop_.load(std::memory_order_relaxed) == false) {
		std::string ticket;
		{
			std::unique_lock<std::mutex> lock(task_mutex_);
			task_cv_.wait(lock, [this]() {
				return stop_.load(std::memory_order_relaxed)
				    || !task_queue_.empty();
			});

			if (stop_.load(std::memory_order_relaxed) && task_queue_.empty()) {
				return;
			}

			ticket = task_queue_.front();
			task_queue_.pop();
		}
		do_get_(std::move(ticket));
	}
}

void ArrowFlightConsumer::do_get_(std::string ticket) {
	logger->log_info("[Flight Consumer] Starting do_get for ticket: " + ticket);

	arrow::flight::Ticket flight_ticket(ticket);

	auto reader_res = consumer_->DoGet(call_options_, flight_ticket);
	if (!reader_res.ok()) {
		logger->log_error("[Flight Consumer] DoGet failed: "
		                  + reader_res.status().ToString());
		return;
	}
	std::unique_ptr<arrow::flight::FlightStreamReader> reader =
	    std::move(*reader_res);

	while (stop_.load(std::memory_order_relaxed) == false) {
		auto result_chunk = reader->Next();

		if (!result_chunk.ok()) {
			logger->log_error("[Flight Consumer] Error reading batch: "
			                  + result_chunk.status().ToString());
			break;
		}

		auto chunk = std::move(result_chunk.ValueOrDie());
		if (!chunk.data) {
			logger->log_info("[Flight Consumer] No more data for ticket: "
			                 + ticket);
			break; // No more data
		}

		{
			std::lock_guard<std::mutex> lock_guard(batch_mutex_);
			batch_queue_.emplace(ticket, chunk.data);
		}

		batch_cv_.notify_one();
		logger->log_info("[Flight Consumer] Received batch for ticket: "
		                 + ticket);
	}
}

void ArrowFlightConsumer::log_configuration() {
	logger->log_config("[Flight Consumer] [CONFIG_BEGIN]");

	logger->log_config("[CONFIG]  Endpoint=" + location_.ToString());
	logger->log_config("[CONFIG] Number of threads="
	                   + std::to_string(num_threads_));
	logger->log_config("[CONFIG] topics="
	                   + utils::get_env_var_or_default("TOPICS", ""));
	logger->log_config(
	    "[CONFIG] Timeout (s)="
	    + std::to_string(std::chrono::duration_cast<std::chrono::seconds>(
	                         call_options_.timeout)
	                         .count()));

	logger->log_config("[Flight Consumer] [CONFIG_END]");
}