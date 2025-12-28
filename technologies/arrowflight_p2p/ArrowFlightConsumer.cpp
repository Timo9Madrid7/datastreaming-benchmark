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
#include <thread>
#include <unordered_set>
#include <utility>

#include "Logger.hpp"
#include "Payload.hpp"
#include "Utils.hpp"

ArrowFlightConsumer::FlightServerLight::FlightServerLight(
    ArrowFlightConsumer *consumer)
    : consumer_(consumer) {
}

arrow::Status ArrowFlightConsumer::FlightServerLight::DoPut(
    const arrow::flight::ServerCallContext &context,
    std::unique_ptr<arrow::flight::FlightMessageReader> reader,
    std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) {

	consumer_->logger->log_info("[Flight Consumer] DoPut called by client: "
	                            + context.peer_identity());

	if (reader->descriptor().path.empty()) {
		consumer_->logger->log_error(
		    "[Flight Consumer] Received DoPut with empty ticket!");
		return arrow::Status::Invalid("Empty ticket in DoPut");
	}
	std::string ticket = reader->descriptor().path[0];

	while (true) {
		ARROW_ASSIGN_OR_RAISE(auto chunk, reader->Next());
		auto batch = chunk.data;
		if (!batch) {
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
			size_t row_size = message_id_column->value_length(i)
			    + sizeof(uint8_t) + data_column->value_length(i);
			std::string_view data_view = data_column->GetView(i);
			size_t data_size = data_view.size();

			consumer_->logger->log_study(
			    "Reception," + message_id + "," + std::to_string(data_size)
			    + ',' + ticket + "," + std::to_string(row_size));

			if (message_id.find(TERMINATION_SIGNAL) != std::string::npos) {
				consumer_->subscribed_streams.dec();
				consumer_->logger->log_info(
				    "[Flight Consumer] Received termination signal for ticket: "
				    + ticket);

				if (consumer_->subscribed_streams.get() == 0) {
					bool expected = false;
					if (consumer_->shutdown_requested_.compare_exchange_strong(
					        expected, true)) {
						consumer_->logger->log_info(
						    "[Flight Consumer] All streams terminated, "
						    "shutting down server.");

						// Shutdown the server asynchronously to avoid deadlock
						std::thread([srv = consumer_->server_.get(),
						             log = consumer_->logger]() {
							auto st =
							    srv ? srv->Shutdown() : arrow::Status::OK();
							if (!st.ok())
								log->log_error(
								    "[Flight Consumer] Server shutdown failed: "
								    + st.ToString());
						}).detach();
					}
				}

				consumer_->logger->log_info(
				    "[Flight Consumer] Remaining streams: "
				    + std::to_string(consumer_->subscribed_streams.get()));

				return arrow::Status::OK();
			}
		}
	}

	return arrow::Status::OK();
}

ArrowFlightConsumer::ArrowFlightConsumer(std::shared_ptr<Logger> logger)
    : IConsumer(logger), server_(nullptr) {
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

	std::istringstream endpoints(vendpoint);
	std::string endpoint;
	std::getline(endpoints, endpoint, ',');
	if (endpoint.empty()) {
		err_msg = "[Flight Consumer] Empty endpoint provided.";
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	}

	auto loc_res = arrow::flight::Location::ForGrpcTcp(endpoint, port);
	if (!loc_res.ok()) {
		logger->log_error("[Flight Consumer] ForGrpcTcp failed: "
		                  + loc_res.status().ToString());
		throw std::runtime_error("[Flight Consumer] ForGrpcTcp failed: "
		                         + loc_res.status().ToString());
	}
	location_ = *loc_res;

	arrow::flight::FlightServerOptions options(location_);
	server_ = std::make_unique<FlightServerLight>(this);
	auto status = server_->Init(options);
	if (!status.ok()) {
		logger->log_error(
		    "[Flight Consumer] Flight server initialization failed: "
		    + status.ToString());
		throw std::runtime_error(
		    "[Flight Consumer] Flight server initialization failed: "
		    + status.ToString());
	}

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
	subscribed_streams.inc();
}

bool ArrowFlightConsumer::deserialize(const void *raw_message, size_t len,
                                      Payload &out) {
	logger->log_error(
	    "[Flight Consumer] Flight does not need deserialization!");
	return false;
}

void ArrowFlightConsumer::start_loop() {
	logger->log_info("[Flight Consumer] Starting server loop...");
	auto status = server_->Serve();
	if (!status.ok()) {
		logger->log_error("[Flight Consumer] Server failed: "
		                  + status.ToString());
		throw std::runtime_error("[Flight Consumer] Server failed: "
		                         + status.ToString());
	}
	logger->log_info("[Flight Consumer] Server loop has ended.");
}

void ArrowFlightConsumer::log_configuration() {
	logger->log_config("[Flight Consumer] [CONFIG_BEGIN]");

	logger->log_config("[CONFIG] Endpoint=" + location_.ToString());

	logger->log_config("[CONFIG] topics="
	                   + utils::get_env_var_or_default("TOPICS", ""));
	logger->log_config("[Flight Consumer] [CONFIG_END]");
}