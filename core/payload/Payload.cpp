#include "Payload.hpp"

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