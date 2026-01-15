#include "Payload.hpp"

#include <cstring>
#include <iostream>

Payload Payload::make(const std::string &publisher_id, int sequence_number,
                      size_t data_size, PayloadKind kind) {
	Payload p;
	p.message_id = publisher_id + "-" + std::to_string(sequence_number);
	p.kind = kind;

	switch (kind) {
	case PayloadKind::TERMINATION:
		p.message_id = publisher_id + ":" + TERMINATION_SIGNAL;
		// Termination signal
		p.data = {0xFF};
		p.data_size = 1;
		return p;

	case PayloadKind::BOOLEAN:
		// Only 1 byte: 0 or 1
		p.data = {static_cast<uint8_t>(sequence_number % 2)};
		break;

	case PayloadKind::FLAT:
		// Fill with repeated pattern
		p.data.resize(data_size, static_cast<uint8_t>(sequence_number % 256));
		break;

	case PayloadKind::COMPLEX:
		// Arbitrary non-uniform data generation
		p.data.reserve(data_size);
		for (size_t i = 0; i < data_size; ++i) {
			p.data.push_back(static_cast<uint8_t>((i * sequence_number) % 251));
		}
		break;
	}

	p.data_size = p.data.size();
	return p;
}

Payload Payload::reuse_with_new_id(const std::string &publisher_id,
                                   int sequence_number, Payload message) {
	Payload p;
	p.message_id = publisher_id + "-" + std::to_string(sequence_number);
	p.kind = message.kind;
	p.data = message.data;
	p.data_size = message.data.size();
	return p;
}

std::string Payload::payloadkind_to_string(PayloadKind kind) {
	switch (kind) {
	case PayloadKind::BOOLEAN:
		return "BOOLEAN";
	case PayloadKind::FLAT:
		return "FLAT";
	case PayloadKind::COMPLEX:
		return "COMPLEX";
	case PayloadKind::TERMINATION:
		return "TERMINATION";
	default:
		return "UNKNOWN";
	}
}

PayloadKind Payload::string_to_payloadkind(const std::string &kind) {
	if (kind.empty()) {
		std::cerr << "Missing PayloadKind: " << kind << ", defaulting to FLAT"
		          << std::endl;
		return PayloadKind::FLAT;
	}

	if (kind == "BOOLEAN") {
		return PayloadKind::BOOLEAN;
	} else if (kind == "FLAT") {
		return PayloadKind::FLAT;
	} else if (kind == "COMPLEX") {
		return PayloadKind::COMPLEX;
	} else if (kind == "TERMINATION") {
		return PayloadKind::TERMINATION;
	} else {
		std::cerr << "Invalid PayloadKind: " << kind << ", defaulting to FLAT"
		          << std::endl;
		return PayloadKind::FLAT;
	}
}

bool Payload::serialize(const Payload &message, void *out) noexcept {
	char *ptr = static_cast<char *>(out);

	// Message ID Length
	const uint16_t id_len = static_cast<uint16_t>(message.message_id.size());
	std::memcpy(ptr, &id_len, sizeof(id_len));
	ptr += sizeof(id_len);

	// Message ID
	std::memcpy(ptr, message.message_id.data(), id_len);
	ptr += id_len;

	// Kind
	const uint8_t kind = static_cast<uint8_t>(message.kind);
	std::memcpy(ptr, &kind, sizeof(kind));
	ptr += sizeof(kind);

	// Data size
	const size_t size = static_cast<size_t>(message.data_size);
	std::memcpy(ptr, &size, sizeof(size));
	ptr += sizeof(size);

	// Data
	std::memcpy(ptr, message.data.data(), size);

	return true;
}

bool Payload::deserialize(const void *raw_message, size_t len,
                          Payload &out) noexcept {
	if (len < sizeof(uint16_t) + sizeof(uint8_t) + sizeof(size_t)) {
		return false;
	}

	const char *data = static_cast<const char *>(raw_message);
	size_t offset = 0;

	uint16_t id_len = 0;
	std::memcpy(&id_len, data + offset, sizeof(id_len));
	offset += sizeof(id_len);

	out.message_id.assign(data + offset, static_cast<size_t>(id_len));
	offset += static_cast<size_t>(id_len);

	uint8_t kind_byte = 0;
	std::memcpy(&kind_byte, data + offset, sizeof(kind_byte));
	offset += sizeof(kind_byte);
	PayloadKind kind_payload = static_cast<PayloadKind>(kind_byte);

	size_t data_size = 0;
	std::memcpy(&data_size, data + offset, sizeof(data_size));
	offset += sizeof(data_size);

	if (len != offset + data_size) {
		return false;
	}

	out.kind = kind_payload;
	out.data_size = data_size;
	out.data.resize(data_size);
	if (data_size > 0) {
		std::memcpy(out.data.data(), data + offset, data_size);
	}

	return true;
}

bool Payload::deserialize_id(const void *raw_message, size_t len,
                             Payload &out) noexcept {
	if (len < sizeof(uint16_t)) {
		return false;
	}

	const char *data = static_cast<const char *>(raw_message);
	size_t offset = 0;

	uint16_t id_len = 0;
	std::memcpy(&id_len, data + offset, sizeof(id_len));
	offset += sizeof(id_len);
	if (len < offset + static_cast<size_t>(id_len)) {
		return false;
	}

	out.message_id.assign(data + offset, static_cast<size_t>(id_len));

	return true;
}