#include "ArrowFlightPublisher.hpp"

#include <arrow/array/array_base.h>
#include <arrow/flight/client.h>
#include <arrow/type_fwd.h>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "Logger.hpp"
#include "Utils.hpp"

const std::shared_ptr<arrow::Schema> ArrowFlightPublisher::schema_ =
    arrow::schema({
        arrow::field("message_id", arrow::utf8()),
        arrow::field("kind", arrow::uint8()),
        arrow::field("data", arrow::binary()),
    });

ArrowFlightPublisher::ArrowFlightPublisher(std::shared_ptr<Logger> logger) try
    : IPublisher(logger), publisher_(), MAX_BATCH_BYTES(8 * 1024 * 1024) {
	logger->log_info("[Flight Publisher] ArrowFlightPublisher created.");
} catch (const std::exception &e) {
	logger->log_error("[Flight Publisher] Constructor failed: "
	                  + std::string(e.what()));
}

ArrowFlightPublisher::~ArrowFlightPublisher() {
	logger->log_debug("[Flight Publisher] Cleaning up Flight client...");
	publisher_.reset();
	logger->log_debug("[Flight Publisher] Flight destructor finished.");
}

void ArrowFlightPublisher::initialize() {
	logger->log_study("Initializing");

	const std::string vendpoint =
	    utils::get_env_var_or_default("PUBLISHER_ENDPOINT", "localhost");
	const std::string port_str =
	    utils::get_env_var_or_default("PUBLISHER_PORT", "8815");
	const std::string max_batch_bytes =
	    utils::get_env_var_or_default("MAX_BATCH_BYTES", "8388608");
	logger->log_info("[Flight Publisher] Using location: " + vendpoint + ":"
	                 + port_str);
	const std::optional<std::string> vTickets = utils::get_env_var("TOPICS");

	std::string err_msg;
	int port;
	try {
		port = std::stoi(port_str);
	} catch (const std::invalid_argument &e) {
		err_msg = "[Flight Publisher] Invalid port number: " + port_str;
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	} catch (const std::out_of_range &e) {
		err_msg = "[Flight Publisher] Port number out of range: " + port_str;
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	}
	if (!vTickets || vTickets.value().empty()) {
		err_msg = "[Flight Publisher] Missing required "
		          "environment variable TOPICS.";
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	}
	try {
		MAX_BATCH_BYTES = std::stoull(max_batch_bytes);
	} catch (const std::invalid_argument &e) {
		err_msg =
		    "[Flight Publisher] Invalid MAX_BATCH_BYTES: " + max_batch_bytes;
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	} catch (const std::out_of_range &e) {
		err_msg = "[Flight Publisher] MAX_BATCH_BYTES out of range: "
		    + max_batch_bytes;
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	}

	auto loc_res = arrow::flight::Location::ForGrpcTcp(vendpoint, port);
	if (!loc_res.ok()) {
		err_msg = "[Flight Publisher] Failed to create location: "
		    + loc_res.status().ToString();
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	}
	location_ = std::move(loc_res.ValueOrDie());

	auto client_res = arrow::flight::FlightClient::Connect(location_);
	if (!client_res.ok()) {
		err_msg = "[Flight Publisher] Failed to create client: "
		    + client_res.status().ToString();
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	}
	publisher_ = std::move(client_res.ValueOrDie());

	logger->log_info("[Flight Publisher] Publisher initialized and connected.");
	logger->log_study("Initialized");
	log_configuration();
}

bool ArrowFlightPublisher::serialize(const std::vector<Payload> &messages,
                                     void *out) {
	BatchBuilder *builder = static_cast<BatchBuilder *>(out);

	for (const Payload &message : messages) {
		arrow::Status status;
		status = builder->message_id_builder.Append(message.message_id);
		if (!status.ok()) {
			logger->log_error("[Flight Publisher] Failed to append message ID: "
			                  + status.ToString());
			return false;
		}
		status =
		    builder->kind_builder.Append(static_cast<uint8_t>(message.kind));
		if (!status.ok()) {
			logger->log_error("[Flight Publisher] Failed to append kind: "
			                  + status.ToString());
			return false;
		}
		status = builder->data_builder.Append(
		    message.data.data(), static_cast<size_t>(message.data_size));
		if (!status.ok()) {
			logger->log_error("[Flight Publisher] Failed to append data: "
			                  + status.ToString());
			return false;
		}

		builder->rows += 1;
		builder->byte_size +=
		    message.message_id.size() + sizeof(uint8_t) + message.data_size;
	}

	return true;
}

void ArrowFlightPublisher::send_message(const Payload &message,
                                        std::string& ticket) {
	logger->log_study("Intention," + message.message_id + ","
	                  + std::to_string(message.data_size) + "," + ticket);

	BatchBuilder &batch_builder = ticket_batch_builders_[ticket];

	if (!serialize({message}, &batch_builder)) {
		logger->log_error(
		    "[Flight Publisher] Serialization failed for message ID: "
		    + message.message_id);
		return;
	}

	batch_builder.publication_logs.push_back(
	    "Publication," + message.message_id + ","
	    + std::to_string(message.data_size) + "," + ticket + ","
	    + std::to_string(message.message_id.size() + sizeof(uint8_t)
	                     + message.data_size));

	if (batch_builder.byte_size >= MAX_BATCH_BYTES
	    || message.kind == PayloadKind::TERMINATION) {
		if (_do_put_(ticket, batch_builder)) {
			for (const auto &log_entry : batch_builder.publication_logs) {
				logger->log_study(log_entry);
			}
		}
		batch_builder.reset();
	}
}

bool ArrowFlightPublisher::_do_put_(const std::string &ticket,
                                    BatchBuilder &batch_builder) {
	std::shared_ptr<arrow::RecordBatch> batch;
	if (!batch_builder.make_batch(batch)) {
		logger->log_error(
		    "[Flight Publisher] Failed to create RecordBatch for ticket: "
		    + ticket);
		return false;
	}

	if (!batch) {
		logger->log_error("[Flight Publisher] Null RecordBatch for ticket: "
		                  + ticket);
		return false;
	}

	// Diagnose schema mismatch early (local)
    if (!batch->schema()->Equals(*schema_, /*check_metadata=*/true)) {
        logger->log_error("[Flight Publisher] Schema mismatch BEFORE DoPut for ticket: " + ticket);
        logger->log_error("[Flight Publisher] schema_      = " + schema_->ToString());
        logger->log_error("[Flight Publisher] batch schema = " + batch->schema()->ToString());
        return false;
    }

	arrow::flight::FlightDescriptor descriptor =
	    arrow::flight::FlightDescriptor::Path({ticket});

	std::unique_ptr<arrow::flight::FlightStreamWriter> writer;
	auto do_put_res = publisher_->DoPut(descriptor, schema_);
	if (!do_put_res.ok()) {
		logger->log_error("[Flight Publisher] DoPut failed for ticket: "
		                  + ticket
		                  + " Error: " + do_put_res.status().ToString());
		return false;
	}
	auto do_put = std::move(do_put_res.ValueOrDie());
	writer = std::move(do_put.writer);

	auto status = writer->WriteRecordBatch(*batch);
	if (!status.ok()) {
		logger->log_error(
		    "[Flight Publisher] WriteRecordBatch failed for ticket: " + ticket
		    + " Error: " + status.ToString());
		return false;
	}

	status = writer->DoneWriting();
	if (!status.ok()) {
		logger->log_error("[Flight Publisher] DoneWriting failed for ticket: "
		                  + ticket + " Error: " + status.ToString());
		return false;
	}

	return true;
}

void ArrowFlightPublisher::log_configuration() {
	logger->log_info("[Flight Publisher] Configuration:");
	logger->log_info(
	    "[CONFIG] PUBLISHER_ENDPOINT: "
	    + utils::get_env_var_or_default("PUBLISHER_ENDPOINT", "localhost"));
	logger->log_info("[CONFIG] PUBLISHER_PORT: "
	                 + utils::get_env_var_or_default("PUBLISHER_PORT", "8815"));
	logger->log_info(
	    "[CONFIG] MAX_BATCH_BYTES: "
	    + utils::get_env_var_or_default("MAX_BATCH_BYTES", "8388608"));
	logger->log_info("[CONFIG] TOPICS: "
	                 + utils::get_env_var_or_default("TOPICS", ""));
	logger->log_config("[Flight Publisher] [CONFIG_END]");
}