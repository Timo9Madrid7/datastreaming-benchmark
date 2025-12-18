#include "ArrowFlightConsumer.hpp"

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type_fwd.h>
#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <sys/types.h>
#include <utility>

#include "BS_thread_pool.hpp"
#include "Logger.hpp"
#include "Payload.hpp"
#include "Utils.hpp"

ArrowFlightConsumer::ArrowFlightConsumer(std::shared_ptr<Logger> logger)
    : IConsumer(logger), consumer_(nullptr) {
	// Set a timeout for receiving messages (10s)
	call_options_.timeout = std::chrono::seconds(10);
	logger->log_info("[Flight Consumer] ArrowFlightConsumer created.");
}

ArrowFlightConsumer::~ArrowFlightConsumer() {
	logger->log_debug("[Flight Consumer] Destructor finished");
}

void ArrowFlightConsumer::initialize() {
	logger->log_study("Initializing");

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

	thread_pool_
	    .pause(); // Pause the thread pool until all subscriptions are added

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
	thread_pool_.detach_task([ticket, this] { this->_do_get_(ticket); });
}

bool ArrowFlightConsumer::deserialize(const void *raw_message, size_t len,
                                      Payload &out) {
	logger->log_error(
	    "[Flight Consumer] Flight does not need deserialization!");
	return false;
}

void ArrowFlightConsumer::start_loop() {
	thread_pool_.unpause();
	thread_pool_.wait();
	logger->log_info("[Kafka Consumer] Remaining subscribed streams: "
	                 + std::to_string(subscribed_streams.get()));
}

void ArrowFlightConsumer::_do_get_(std::string ticket) {
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

	while (true) {
		auto result_chunk = reader->Next();

		if (!result_chunk.ok()) {
			logger->log_error("[Flight Consumer] Error reading chunk for ticket: "
			                  + ticket + ": "
			                  + result_chunk.status().ToString());
			return;
		}

		auto chunk = std::move(result_chunk.ValueOrDie());
		if (!chunk.data) {
			logger->log_info(
			    "[Flight Consumer] End of stream reached for ticket: " + ticket);
			return;
		}

		auto batch = std::move(chunk.data);

		auto message_id_column =
		    std::static_pointer_cast<arrow::StringArray>(batch->column(0));
		auto kind_column =
		    std::static_pointer_cast<arrow::UInt8Array>(batch->column(1));
		auto data_column =
		    std::static_pointer_cast<arrow::BinaryArray>(batch->column(2));

		for (int64_t i = 0; i < batch->num_rows(); ++i) {
			std::string message_id = message_id_column->GetString(i);
			PayloadKind kind = static_cast<PayloadKind>(kind_column->Value(i));
			size_t row_size = message_id_column->value_length(i)
			    + sizeof(uint8_t) + data_column->value_length(i);
			std::string_view data_view = data_column->GetView(i);
			size_t data_size = data_view.size();

			logger->log_study("Reception," + message_id + ","
			                  + std::to_string(data_size) + ',' + ticket + ","
			                  + std::to_string(row_size));

			if (message_id.find(TERMINATION_SIGNAL) != std::string::npos) {
				logger->log_info(
				    "[Flight Consumer] Received termination signal for ticket: "
				    + ticket);

				return;
			}
		}
	}
}

void ArrowFlightConsumer::log_configuration() {
	logger->log_config("[Flight Consumer] [CONFIG_BEGIN]");

	logger->log_config("[CONFIG] Endpoint=" + location_.ToString());
	logger->log_config("[CONFIG] Number of threads="
	                   + std::to_string(thread_pool_.get_thread_count()));
	logger->log_config("[CONFIG] topics="
	                   + utils::get_env_var_or_default("TOPICS", ""));
	logger->log_config(
	    "[CONFIG] Timeout (s)="
	    + std::to_string(std::chrono::duration_cast<std::chrono::seconds>(
	                         call_options_.timeout)
	                         .count()));

	logger->log_config("[Flight Consumer] [CONFIG_END]");
}