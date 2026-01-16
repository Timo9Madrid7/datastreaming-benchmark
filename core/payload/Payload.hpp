#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <vector>

enum class PayloadKind { TERMINATION = 0, FLAT, COMPLEX };
const std::string TERMINATION_SIGNAL = "__END__";

struct Payload {
	struct NestedPayload {
		size_t double_size = 0;
		size_t string_size = 0;
		std::vector<double> doubles;
		std::vector<std::string> strings;
	};

	std::string message_id;
	PayloadKind kind;

	size_t byte_size = 0;
	std::vector<uint8_t> bytes;

	NestedPayload nested_payload;

	size_t data_size = 0;
	size_t serialized_bytes = 0;

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
	                                 int sequence_number,
	                                 const Payload &message);

	/**
	@brief Compute serialized byte size for a payload
	@param message Payload to measure
	@return Total bytes required to serialize
	*/
	static size_t serialized_size(const Payload &message) noexcept;

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

	/**
	@brief Serialize a Payload object into a raw byte buffer
	@param message Payload object to serialize
	@param out Pointer to the output buffer
	@return True if serialization is successful, false otherwise
	*/
	static bool serialize(const Payload &message, void *out) noexcept;

	/**
	@brief Deserialize a raw byte buffer into a Payload object
	@param raw_message Pointer to the raw byte buffer
	@param len Length of the raw byte buffer
	@param out Reference to the output Payload object
	@return True if deserialization is successful, false otherwise
	*/
	static bool deserialize(const void *raw_message, size_t len,
	                        Payload &out) noexcept;

	/**
	@brief Deserialize only the message ID from a raw byte buffer into a Payload
	object
	@param raw_message Pointer to the raw byte buffer
	@param len Length of the raw byte buffer
	@param out Reference to the output Payload object
	@return True if deserialization is successful, false otherwise
	*/
	static bool deserialize_id(const void *raw_message, size_t len,
	                           Payload &out) noexcept;
};
