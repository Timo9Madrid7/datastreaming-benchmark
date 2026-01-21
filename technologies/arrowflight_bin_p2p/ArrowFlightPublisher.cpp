#include "ArrowFlightPublisher.hpp"

#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/ipc/options.h>
#include <arrow/ipc/writer.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "Logger.hpp"
#include "Payload.hpp"
#include "Utils.hpp"

const std::shared_ptr<arrow::Schema> ArrowFlightPublisher::struct_schema_ =
    arrow::schema({
        arrow::field("message_id", arrow::utf8()),
        arrow::field("kind", arrow::uint8()),
        arrow::field("bytes", arrow::binary()),
        arrow::field("doubles", arrow::list(arrow::float64())),
        arrow::field("strings", arrow::list(arrow::utf8())),
    });

const std::shared_ptr<arrow::Schema> ArrowFlightPublisher::data_schema_ =
    arrow::schema({
        arrow::field("message_id", arrow::utf8()),
        arrow::field("kind", arrow::uint8()),
        arrow::field("bytes", arrow::binary()),
        arrow::field("doubles", arrow::binary()),
        arrow::field("strings", arrow::binary()),
    });

ArrowFlightPublisher::ArrowFlightPublisher(std::shared_ptr<Logger> logger) try
    : IPublisher(logger), MAX_BATCH_BYTES(8 * 1024 * 1024) {
	logger->log_info("[Flight Publisher] ArrowFlightPublisher created.");
} catch (const std::exception &e) {
	logger->log_error("[Flight Publisher] Constructor failed: "
	                  + std::string(e.what()));
}

ArrowFlightPublisher::~ArrowFlightPublisher() {
	logger->log_debug("[Flight Publisher] Cleaning up Flight server...");
	if (server_) {
		(void)server_->Shutdown();
	}
	if (server_thread_.joinable()) {
		server_thread_.join();
	}
	logger->log_debug("[Flight Publisher] Publisher destructor finished.");
}

namespace {
static void gc_locked(ArrowFlightPublisher::TicketState &state) {
	if (state.consumers.empty()) {
		state.q.clear();
		state.base_seq = state.next_seq;
		return;
	}

	uint64_t min_next = std::numeric_limits<uint64_t>::max();
	for (const auto &kv : state.consumers) {
		min_next = std::min(min_next, kv.second->next_seq);
	}
	if (min_next <= state.base_seq) {
		return;
	}

	const uint64_t drop = min_next - state.base_seq;
	for (uint64_t i = 0; i < drop && !state.q.empty(); ++i) {
		state.q.pop_front();
	}
	state.base_seq = min_next;
}
} // namespace

std::shared_ptr<ArrowFlightPublisher::TicketState>
ArrowFlightPublisher::get_or_create_stream_(const std::string &ticket) {
	std::lock_guard<std::mutex> lk(streams_mu_);
	auto it = streams_.find(ticket);
	if (it != streams_.end())
		return it->second;
	auto s = std::make_shared<TicketState>();
	streams_[ticket] = s;
	return s;
}

void ArrowFlightPublisher::enqueue_batch_(
    const std::string &ticket, const std::shared_ptr<arrow::RecordBatch> &batch,
    bool is_termination) {
	auto s = get_or_create_stream_(ticket);
	{
		std::lock_guard<std::mutex> lk(s->m);
		if (s->q.empty()) {
			s->base_seq = s->next_seq;
		}
		s->q.push_back(TicketState::Entry{s->next_seq, batch});
		++s->next_seq;
		if (is_termination)
			s->finished = true;
		gc_locked(*s);
	}
	s->cv.notify_all();
}

namespace {
class FanoutRecordBatchReader : public arrow::RecordBatchReader {
  public:
	FanoutRecordBatchReader(
	    std::shared_ptr<arrow::Schema> schema,
	    std::shared_ptr<ArrowFlightPublisher::TicketState> state,
	    uint64_t consumer_id,
	    std::shared_ptr<ArrowFlightPublisher::TicketState::ConsumerState>
	        consumer_state)
	    : schema_(std::move(schema)), state_(std::move(state)),
	      consumer_id_(consumer_id), consumer_state_(std::move(consumer_state)) {
	}

	~FanoutRecordBatchReader() override {
		auto state = state_;
		if (!state)
			return;
		std::lock_guard<std::mutex> lk(state->m);
		state->consumers.erase(consumer_id_);
		gc_locked(*state);
		state->cv.notify_all();
	}

	std::shared_ptr<arrow::Schema> schema() const override {
		return schema_;
	}

	arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch> *out) override {
		if (!state_ || !consumer_state_) {
			*out = nullptr;
			return arrow::Status::OK();
		}

		std::unique_lock<std::mutex> lk(state_->m);
		state_->cv.wait(lk, [&] {
			return (consumer_state_->next_seq < state_->next_seq)
			    || (state_->finished && consumer_state_->next_seq >= state_->next_seq);
		});

		if (consumer_state_->next_seq < state_->next_seq) {
			if (consumer_state_->next_seq < state_->base_seq) {
				return arrow::Status::Invalid(
				    "consumer cursor behind buffer (unexpected)");
			}
			const uint64_t idx = consumer_state_->next_seq - state_->base_seq;
			if (idx >= state_->q.size()) {
				return arrow::Status::Invalid(
				    "buffer index out of range (unexpected)");
			}

			*out = state_->q[static_cast<size_t>(idx)].batch;
			++consumer_state_->next_seq;
			gc_locked(*state_);
			return arrow::Status::OK();
		}

		*out = nullptr;
		return arrow::Status::OK();
	}

  private:
	std::shared_ptr<arrow::Schema> schema_;
	std::shared_ptr<ArrowFlightPublisher::TicketState> state_;
	uint64_t consumer_id_;
	std::shared_ptr<ArrowFlightPublisher::TicketState::ConsumerState>
	    consumer_state_;
};
} // namespace

arrow::Status ArrowFlightPublisher::FlightServerLight::DoGet(
    const arrow::flight::ServerCallContext &context,
    const arrow::flight::Ticket &request,
    std::unique_ptr<arrow::flight::FlightDataStream> *stream) {

	if (!publisher_)
		return arrow::Status::Invalid("publisher_ is null");

	const std::string ticket = request.ticket;
	publisher_->logger->log_info("[Flight Publisher] DoGet ticket=" + ticket);

	auto state = publisher_->get_or_create_stream_(ticket);
	std::shared_ptr<ArrowFlightPublisher::TicketState::ConsumerState> consumer;
	uint64_t consumer_id = 0;
	{
		std::lock_guard<std::mutex> lk(state->m);
		consumer = std::make_shared<ArrowFlightPublisher::TicketState::ConsumerState>();
		// start from current head (replay backlog)
		consumer->next_seq = state->base_seq;
		consumer_id = state->next_consumer_id++;
		state->consumers.emplace(consumer_id, consumer);
	}

	auto reader = std::make_shared<FanoutRecordBatchReader>(
	    ArrowFlightPublisher::data_schema_, state, consumer_id, consumer);

	*stream = std::make_unique<arrow::flight::RecordBatchStream>(reader);
	return arrow::Status::OK();
}

arrow::Status ArrowFlightPublisher::FlightServerLight::GetSchema(
    const arrow::flight::ServerCallContext &context,
    const arrow::flight::FlightDescriptor &descriptor,
    std::unique_ptr<arrow::flight::SchemaResult> *schema) {

	if (!publisher_)
		return arrow::Status::Invalid("publisher_ is null");

	publisher_->logger->log_info("[Flight Publisher] GetSchema for descriptor: "
	                             + descriptor.ToString());

	auto res = arrow::flight::SchemaResult::Make(*struct_schema_);
	if (!res.ok()) {
		return res.status();
	}
	*schema = std::move(res).ValueOrDie();

	return arrow::Status::OK();
}

void ArrowFlightPublisher::initialize() {
	const std::string vendpoint =
	    utils::get_env_var_or_default("PUBLISHER_ENDPOINT", "0.0.0.0");
	const std::string port_str =
	    utils::get_env_var_or_default("PUBLISHER_PORT", "8815");
	const std::optional<std::string> payload_size_str =
	    utils::get_env_var("PAYLOAD_SIZE");

	std::string err_msg;

	if (!payload_size_str) {
		err_msg =
		    "[Flight Publisher] PAYLOAD_SIZE environment variable is not set.";
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	}

	int port = 0;
	try {
		port = std::stoi(port_str);
		uint64_t payload_size = std::stoull(payload_size_str.value());
		if (payload_size >= 4 * 1024 * 1024) {  // 4 MB
			MAX_BATCH_BYTES = 16 * 1024 * 1024; // 16 MB
		} else {
			// For payloads smaller than 4 MB, keep the default MAX_BATCH_BYTES
			// value (8 MB) set in the constructor.
		}
	} catch (...) {
		err_msg = "[Flight Publisher] Invalid port or PAYLOAD_SIZE value.";
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	}

	auto loc_res = arrow::flight::Location::ForGrpcTcp(vendpoint, port);
	if (!loc_res.ok()) {
		throw std::runtime_error("[Flight Publisher] ForGrpcTcp failed: "
		                         + loc_res.status().ToString());
	}
	location_ = *loc_res;

	server_ = std::make_unique<FlightServerLight>(this);
	arrow::flight::FlightServerOptions options(location_);

	auto st = server_->Init(options);
	if (!st.ok()) {
		throw std::runtime_error("[Flight Publisher] Server Init failed: "
		                         + st.ToString());
	}

	server_started_.store(true, std::memory_order_release);
	server_thread_ = std::thread([this] {
		logger->log_info("[Flight Publisher] Starting server loop...");
		auto s = server_->Serve();
		if (!s.ok()) {
			logger->log_error("[Flight Publisher] Server failed: "
			                  + s.ToString());
		}
		logger->log_info("[Flight Publisher] Server loop ended.");
	});

	logger->log_info("[Flight Publisher] Publisher server initialized.");
	log_configuration();
}

bool ArrowFlightPublisher::serialize(const Payload &message, void *out) {
	BatchBuilder *builder = static_cast<BatchBuilder *>(out);

	auto st = builder->message_id_builder.Append(message.message_id);
	if (!st.ok())
		return false;

	st = builder->kind_builder.Append(static_cast<uint8_t>(message.kind));
	if (!st.ok())
		return false;

	st = builder->data_builder.Append(message.bytes.data(),
	                                  static_cast<size_t>(message.byte_size));
	if (!st.ok())
		return false;

	// nested payload (only for COMPLEX)
	if (message.kind == PayloadKind::COMPLEX) {
		st = builder->doubles_bin_builder.Append(
		    reinterpret_cast<const uint8_t *>(
		        message.nested_payload.doubles.data()),
		    static_cast<int64_t>(message.nested_payload.double_size));
		if (!st.ok())
			return false;

		const auto &strings = message.nested_payload.strings;
		// Serialize as: [uint16_t len][bytes]...[uint16_t len][bytes]
		// Reuse a scratch buffer to avoid per-message allocations.
		const size_t total_string_bytes = message.nested_payload.string_size;
		const size_t header_bytes = strings.size() * sizeof(uint16_t);
		builder->strings_wire_scratch.clear();
		builder->strings_wire_scratch.resize(header_bytes + total_string_bytes);
		char *dst = builder->strings_wire_scratch.data();
		for (const auto &str : strings) {
			const uint16_t str_len = static_cast<uint16_t>(str.size());
			std::memcpy(dst, &str_len, sizeof(str_len));
			dst += sizeof(str_len);
			if (str_len > 0) {
				std::memcpy(dst, str.data(), str_len);
				dst += str_len;
			}
		}

		st = builder->strings_bin_builder.Append(
		    reinterpret_cast<const uint8_t *>(builder->strings_wire_scratch.data()),
		    static_cast<int64_t>(builder->strings_wire_scratch.size()));
		if (!st.ok())
			return false;
	} else {
		st = builder->doubles_bin_builder.AppendNull();
		if (!st.ok())
			return false;

		st = builder->strings_bin_builder.AppendNull();
		if (!st.ok())
			return false;
	}

	builder->rows += 1;
	builder->byte_size +=
	    message.message_id.size() + sizeof(uint8_t) + message.byte_size;
	if (message.kind == PayloadKind::COMPLEX) {
		builder->byte_size += message.nested_payload.double_size;
		builder->byte_size += message.nested_payload.string_size;
	}
	return true;
}

void ArrowFlightPublisher::send_message(const Payload &message,
                                        std::string &ticket) {
	logger->log_study("Serializing," + message.message_id + "," + ticket);

	BatchBuilder &bb = ticket_batch_builders_[ticket];

	if (!serialize(message, &bb)) {
		logger->log_error(
		    "[Flight Publisher] Serialization failed for message ID: "
		    + message.message_id);
		return;
	}

	const size_t row_size = message.message_id.size() + sizeof(uint8_t)
	    + message.byte_size
	    + (message.kind == PayloadKind::COMPLEX
	           ? message.nested_payload.double_size
	               + message.nested_payload.string_size
	           : 0);

	bb.publication_logs.push_back({message.message_id, message.data_size, row_size});

	const bool flush = (bb.byte_size >= MAX_BATCH_BYTES)
	    || (message.kind == PayloadKind::TERMINATION);
	if (!flush)
		return;

	std::shared_ptr<arrow::RecordBatch> batch;
	if (!bb.make_batch(batch) || !batch) {
		logger->log_error(
		    "[Flight Publisher] Failed to create batch for ticket: " + ticket);
		bb.reset();
		return;
	}

	const bool is_term = (message.kind == PayloadKind::TERMINATION);
	enqueue_batch_(ticket, batch, is_term);
	for (const auto &entry : bb.publication_logs) {
		logger->log_study("Publication," + entry.message_id + "," + ticket + ","
		                  + std::to_string(entry.data_size) + ","
		                  + std::to_string(entry.row_size));
	}

	bb.reset();
}

void ArrowFlightPublisher::log_configuration() {
	logger->log_info("[Flight Publisher] Configuration:");
	logger->log_info(
	    "[CONFIG] PUBLISHER_ENDPOINT: "
	    + utils::get_env_var_or_default("PUBLISHER_ENDPOINT", "0.0.0.0"));
	logger->log_info("[CONFIG] PUBLISHER_PORT: "
	                 + utils::get_env_var_or_default("PUBLISHER_PORT", "8815"));
	logger->log_info("[CONFIG] MAX_BATCH_BYTES: "
	                 + std::to_string(MAX_BATCH_BYTES));
	logger->log_info("[CONFIG] TOPICS: "
	                 + utils::get_env_var_or_default("TOPICS", ""));
	logger->log_config("[Flight Publisher] [CONFIG_END]");
}