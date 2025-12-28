#include "ArrowFlightConsumer.hpp"

#include <arrow/array/array_binary.h>
#include <arrow/array/array_primitive.h>
#include <arrow/flight/server.h>
#include <arrow/flight/types.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <cstddef>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <stdint.h>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>

#include "Logger.hpp"
#include "Payload.hpp"
#include "Utils.hpp"

ArrowFlightConsumer::ArrowFlightConsumer(std::shared_ptr<Logger> logger)
    : IConsumer(logger) {
	this->logger->log_info("[Flight Consumer] ArrowFlightConsumer created.");
}

ArrowFlightConsumer::~ArrowFlightConsumer() {
	thread_pool_.wait();
	logger->log_debug("[Flight Consumer] Destructor finished");
}

void ArrowFlightConsumer::initialize() {
	logger->log_study("Initializing");

	const std::string vendpoints = utils::get_env_var_or_default(
	    "PUBLISHER_ENDPOINTS",
	    utils::get_env_var_or_default("CONSUMER_ENDPOINT", "localhost"));

	const std::string port_str = utils::get_env_var_or_default(
	    "PUBLISHER_PORT",
	    utils::get_env_var_or_default("CONSUMER_PORT", "8815"));

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

	// parse publishers endpoints
	publisher_endpoints_.clear();
	{
		std::istringstream ss(vendpoints);
		std::string ep;
		while (std::getline(ss, ep, ',')) {
			if (!ep.empty())
				publisher_endpoints_.push_back(ep);
		}
	}
	if (publisher_endpoints_.empty()) {
		throw std::runtime_error(
		    "[Flight Consumer] No publisher endpoints provided.");
	}

	// parse + dedupe tickets
	ticket_names_.clear();
	{
		std::istringstream tickets(vTickets.value());
		std::string ticket;
		std::unordered_set<std::string> uniq;
		while (std::getline(tickets, ticket, ',')) {
			if (!ticket.empty() && uniq.insert(ticket).second) {
				subscribe(ticket);
			}
		}
	}
	if (ticket_names_.empty()) {
		throw std::runtime_error(
		    "[Flight Consumer] No tickets provided in TOPICS.");
	}

	logger->log_info("[Flight Consumer] Consumer initialized.");
	logger->log_study("Initialized");
	log_configuration();
}

void ArrowFlightConsumer::subscribe(const std::string &ticket) {
	ticket_names_.push_back(ticket);
	logger->log_info("[Flight Consumer] Subscribed ticket: " + ticket);
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

	arrow::flight::Ticket t{ticket};
	auto reader_res = client->DoGet(t);
	if (!reader_res.ok()) {
		logger->log_error("[Flight Consumer] DoGet failed ticket=" + ticket
		                  + " from " + endpoint + " : "
		                  + reader_res.status().ToString());
		subscribed_streams.dec();
		return;
	}
	auto reader = std::move(reader_res).ValueOrDie();

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

		// [message_id, kind, data]
		if (batch->num_columns() < 3) {
			logger->log_error("[Flight Consumer] Invalid batch schema: "
			                  "expected >= 3 columns");
			break;
		}

		auto message_id_column =
		    std::static_pointer_cast<arrow::StringArray>(batch->column(0));
		auto kind_column =
		    std::static_pointer_cast<arrow::UInt8Array>(batch->column(1));
		auto data_column =
		    std::static_pointer_cast<arrow::BinaryArray>(batch->column(2));

		for (int64_t i = 0; i < batch->num_rows(); ++i) {
			std::string message_id = message_id_column->GetString(i);
			PayloadKind kind = static_cast<PayloadKind>(kind_column->Value(i));
			(void)kind;

			std::string_view data_view = data_column->GetView(i);
			size_t data_size = data_view.size();

			size_t row_size = message_id_column->value_length(i)
			    + sizeof(uint8_t) + data_column->value_length(i);

			logger->log_study("Reception," + message_id + ","
			                  + std::to_string(data_size) + "," + ticket + ","
			                  + std::to_string(row_size));

			if (message_id.find(TERMINATION_SIGNAL) != std::string::npos) {
				logger->log_info(
				    "[Flight Consumer] Received termination for ticket="
				    + ticket + " from publisher=" + endpoint);
				subscribed_streams.dec();
				return;
			}
		}
	}

	logger->log_info(
	    "[Flight Consumer] Stream ended without termination. ticket=" + ticket
	    + " publisher=" + endpoint);
	subscribed_streams.dec();
}

void ArrowFlightConsumer::start_loop() {
	logger->log_info(
	    "[Flight Consumer] Starting client loops (thread pool)...");

	for (const auto &pub : publisher_endpoints_) {
		for (const auto &ticket : ticket_names_) {
			logger->log_info("[Flight Consumer] Queue DoGet from " + pub + ":"
			                 + std::to_string(publisher_port_)
			                 + " ticket=" + ticket);
			thread_pool_.detach_task(
			    [this, pub, ticket] { consume_from_publisher_(pub, ticket); });
		}
	}
	thread_pool_.wait();
	logger->log_info("[Flight Consumer] All streams ended.");
}

bool ArrowFlightConsumer::deserialize(const void *raw_message, size_t len,
                                      Payload &out) {
	logger->log_error(
	    "[Flight Consumer] Flight does not need deserialization!");
	return false;
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
	logger->log_config("[CONFIG] THREADS=4");
	logger->log_config("[Flight Consumer] [CONFIG_END]");
}