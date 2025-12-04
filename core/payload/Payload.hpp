#pragma once

#include <cstdint>
#include <cstdlib>
#include <string>
#include <vector>

enum class PayloadKind { TERMINATION = 0, BOOLEAN, FLAT, COMPLEX };
const std::string TERMINATION_SIGNAL = "__END__";

struct Payload {
	std::string message_id;
	PayloadKind kind;
	size_t data_size;
	std::vector<uint8_t> data;

	/**
	@brief Create a payload with specified parameters
	@param publisher_id ID of the publisher
	@param sequence_number Sequence number for the message
	@param data_size Size of the data payload
	@param kind Kind of the payload (default is FLAT)
	@return Constructed Payload object
	*/
	static Payload make(const std::string &publisher_id, int sequence_number,
	                    size_t data_size, PayloadKind kind = PayloadKind::FLAT);

	/**
	@brief Reuse an existing payload with a new message ID
	@param publisher_id ID of the publisher
	@param sequence_number Sequence number for the new message
	@param message Existing Payload to reuse data from
	@return New Payload object with updated message ID
	*/
	static Payload reuse_with_new_id(const std::string &publisher_id,
	                                 int sequence_number, Payload message);

	/**
	@brief Convert PayloadKind enum to string representation
	@param kind PayloadKind enum value
	@return String representation of the PayloadKind
	*/
	static PayloadKind string_to_payloadkind(const std::string &kind_str);

	/**
	@brief Convert string representation to PayloadKind enum
	@param kind_str String representation of the PayloadKind
	@return PayloadKind enum value
	*/
	static std::string payloadkind_to_string(PayloadKind kind);
};
