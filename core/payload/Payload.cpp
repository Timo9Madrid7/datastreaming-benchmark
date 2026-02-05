#include "Payload.hpp"

#include <cstddef>
#include <cstring>
#include <iostream>

namespace {

size_t compute_serialized_size(const Payload &message) noexcept {
	const size_t id_len = message.message_id.size();
	size_t total = sizeof(uint16_t) + id_len + sizeof(uint8_t) + sizeof(size_t)
	    + message.bytes.size();

	if (message.kind == PayloadKind::COMPLEX) {
		const size_t double_count = message.nested_payload.doubles.size();
		total += sizeof(size_t) + double_count * sizeof(double);

		const size_t string_count = message.nested_payload.strings.size();
		total += sizeof(size_t); // total_string_bytes
		total += sizeof(size_t); // string_count
		total += string_count * sizeof(uint16_t);

		size_t string_bytes = 0;
		for (const auto &str : message.nested_payload.strings) {
			string_bytes += str.size();
		}
		total += string_bytes;
	}

	return total;
}

} // namespace

Payload Payload::make(const std::string &publisher_id, int sequence_number,
                      size_t data_size, PayloadKind kind) {
	Payload p;
	p.message_id = publisher_id + "-" + std::to_string(sequence_number);
	p.kind = kind;

	switch (kind) {
	case PayloadKind::TERMINATION:
		p.message_id = publisher_id + ":" + TERMINATION_SIGNAL;
		// Termination signal
		p.bytes = {0xFF};
		p.byte_size = 1;
		p.data_size = p.byte_size;
		p.serialized_bytes = compute_serialized_size(p);
		return p;

	case PayloadKind::FLAT:
		// Fill with random pattern
		p.bytes.reserve(data_size);
		for (size_t i = 0; i < data_size; ++i) {
			p.bytes.push_back(
			    static_cast<uint8_t>((i * sequence_number) % 251));
		}
		p.byte_size = p.bytes.size();
		break;

	case PayloadKind::COMPLEX:
		// Payload has a nested pattern
		const size_t double_target_bytes = static_cast<size_t>(data_size * 0.6);
		const size_t string_target_bytes = static_cast<size_t>(data_size * 0.2);

		// Fill doubles
		p.nested_payload.doubles.reserve(double_target_bytes / sizeof(double));
		for (size_t i = 0; i < double_target_bytes / sizeof(double); ++i) {
			p.nested_payload.doubles.push_back(static_cast<double>(i) * 1.111);
		}
		const size_t actual_double_bytes =
		    p.nested_payload.doubles.size() * sizeof(double);

		// Fill strings (different lengths)
		p.nested_payload.strings.reserve(string_target_bytes / 10);
		size_t cur_string_size = 0;
		while (cur_string_size < string_target_bytes) {
			std::string str = "str_" + std::to_string(cur_string_size);
			p.nested_payload.strings.push_back(str);
			cur_string_size += str.size();
		}
		if (cur_string_size > string_target_bytes
		    && !p.nested_payload.strings.empty()) {
			const size_t excess = cur_string_size - string_target_bytes;
			p.nested_payload.strings.back().resize(
			    p.nested_payload.strings.back().size() - excess);
			cur_string_size -= excess;
		}
		const size_t actual_string_bytes = cur_string_size;

		const size_t byte_size =
		    data_size - actual_double_bytes - actual_string_bytes;

		// Fill raw data
		p.bytes.reserve(byte_size);
		for (size_t i = 0; i < byte_size; ++i) {
			p.bytes.push_back(
			    static_cast<uint8_t>((i + sequence_number) % 199));
		}
		p.byte_size = p.bytes.size();

		p.nested_payload.double_size = actual_double_bytes;
		p.nested_payload.string_size = actual_string_bytes;
		break;
	}

	p.data_size = p.nested_payload.double_size + p.nested_payload.string_size
	              + p.byte_size;
	p.serialized_bytes = compute_serialized_size(p);
	return p;
}

Payload Payload::reuse_with_new_id(const std::string &publisher_id,
                                   int sequence_number,
                                   const Payload &message) {
	Payload p(message);
	const size_t old_id_len = message.message_id.size();
	p.message_id = publisher_id + "-" + std::to_string(sequence_number);
	const size_t new_id_len = p.message_id.size();

	if (new_id_len != old_id_len) {
		const long long len_diff = static_cast<long long>(new_id_len)
		    - static_cast<long long>(old_id_len);
		p.serialized_bytes = static_cast<size_t>(
		    static_cast<long long>(p.serialized_bytes) + len_diff);
	}

	return p;
}

size_t Payload::serialized_size(const Payload &message) noexcept {
	return message.serialized_bytes;
}

std::string Payload::payloadkind_to_string(PayloadKind kind) {
	switch (kind) {
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

	if (kind == "FLAT") {
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
	const size_t size = static_cast<size_t>(message.byte_size);
	std::memcpy(ptr, &size, sizeof(size));
	ptr += sizeof(size);

	// Bytes
	std::memcpy(ptr, message.bytes.data(), size);

	// nested payload for COMPLEX kind
	if (message.kind == PayloadKind::COMPLEX) {
		ptr += size;

		// Doubles count
		const size_t double_count = message.nested_payload.doubles.size();
		std::memcpy(ptr, &double_count, sizeof(double_count));
		ptr += sizeof(double_count);

		// Doubles
		std::memcpy(ptr, message.nested_payload.doubles.data(),
		            double_count * sizeof(double));
		ptr += double_count * sizeof(double);

		// Strings size
		const size_t total_string_bytes = message.nested_payload.string_size;
		std::memcpy(ptr, &total_string_bytes, sizeof(total_string_bytes));
		ptr += sizeof(total_string_bytes);

		// Strings count
		const size_t string_count = message.nested_payload.strings.size();
		std::memcpy(ptr, &string_count, sizeof(string_count));
		ptr += sizeof(string_count);

		// Wire format: [len_0..len_n][bytes_0..bytes_n].
		char *len_ptr = ptr;
		char *data_ptr = ptr + string_count * sizeof(uint16_t);
		for (const auto &str : message.nested_payload.strings) {
			const uint16_t str_len = static_cast<uint16_t>(str.size());
			std::memcpy(len_ptr, &str_len, sizeof(str_len));
			len_ptr += sizeof(str_len);

			std::memcpy(data_ptr, str.data(), str.size());
			data_ptr += str.size();
		}
		ptr = data_ptr;
	}

	return true;
}

bool Payload::deserialize(const void *raw_message, size_t len,
                          Payload &out) noexcept {
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

	size_t byte_size = 0;
	std::memcpy(&byte_size, data + offset, sizeof(byte_size));
	offset += sizeof(byte_size);

	out.kind = kind_payload;
	out.byte_size = byte_size;

	if (out.bytes.capacity() < byte_size) {
		out.bytes.reserve(byte_size);
	}
	out.bytes.resize(byte_size);
	if (byte_size > 0) {
		std::memcpy(out.bytes.data(), data + offset, byte_size);
	}

	if (out.kind == PayloadKind::COMPLEX) {
		offset += byte_size;

		size_t double_count = 0;
		std::memcpy(&double_count, data + offset, sizeof(double_count));
		offset += sizeof(double_count);

		auto &doubles = out.nested_payload.doubles;
		if (doubles.capacity() < double_count) {
			doubles.reserve(double_count);
		}
		doubles.resize(double_count);
		std::memcpy(doubles.data(), data + offset,
		            double_count * sizeof(double));
		offset += double_count * sizeof(double);

		size_t total_string_bytes = 0;
		std::memcpy(&total_string_bytes, data + offset,
		            sizeof(total_string_bytes));
		offset += sizeof(total_string_bytes);

		size_t string_count = 0;
		std::memcpy(&string_count, data + offset, sizeof(string_count));
		offset += sizeof(string_count);

		auto &strings = out.nested_payload.strings;
		if (strings.capacity() < string_count) {
			strings.reserve(string_count);
		}
		strings.resize(string_count);

		// Wire format: [len_0..len_n][bytes_0..bytes_n].
		const char *len_ptr = data + offset;
		const char *data_ptr = len_ptr + string_count * sizeof(uint16_t);
		for (size_t i = 0; i < string_count; ++i) {
			uint16_t str_len = 0;
			std::memcpy(&str_len, len_ptr, sizeof(str_len));
			len_ptr += sizeof(str_len);

			strings[i].assign(data_ptr, static_cast<size_t>(str_len));
			data_ptr += static_cast<size_t>(str_len);
		}
		offset = static_cast<size_t>(data_ptr - data);

		out.nested_payload.double_size = double_count * sizeof(double);
		out.nested_payload.string_size = total_string_bytes;
	}

	out.data_size = out.nested_payload.double_size + out.nested_payload.string_size
	              + out.byte_size;

	out.serialized_bytes = compute_serialized_size(out);

	return true;
}

bool Payload::deserialize_id(const void *raw_message, size_t len,
                             Payload &out) noexcept {
	const char *data = static_cast<const char *>(raw_message);
	size_t offset = 0;

	uint16_t id_len = 0;
	std::memcpy(&id_len, data + offset, sizeof(id_len));
	offset += sizeof(id_len);

	out.message_id.assign(data + offset, static_cast<size_t>(id_len));

	return true;
}