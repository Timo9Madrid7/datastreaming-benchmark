#include "../payload/Payload.hpp"

#include <cassert>
#include <cstdint>
#include <iostream>
#include <vector>

static void roundtrip(const Payload &input) {
	const size_t size = Payload::serialized_size(input);
	std::vector<uint8_t> buffer(size);

	const bool serialized = Payload::serialize(input, buffer.data());
	assert(serialized);

	Payload output;
	const bool deserialized =
	    Payload::deserialize(buffer.data(), buffer.size(), output);
	assert(deserialized);

	assert(output.message_id == input.message_id);
	assert(output.kind == input.kind);
	assert(output.data_size == input.data_size);
	assert(output.byte_size == input.byte_size);
	assert(output.bytes == input.bytes);

	if (input.kind == PayloadKind::COMPLEX) {
		assert(output.inner_payload.doubles == input.inner_payload.doubles);
		assert(output.inner_payload.strings == input.inner_payload.strings);
		assert(output.inner_payload.double_size
		       == input.inner_payload.double_size);
		assert(output.inner_payload.string_size
		       == input.inner_payload.string_size);
	}
}

int main() {
	// FLAT payload
	{
		auto p = Payload::make("pub", 1, 32, PayloadKind::FLAT);
		roundtrip(p);
	}

	// COMPLEX payload
	{
		auto p = Payload::make("pub", 2, 128, PayloadKind::COMPLEX);
		roundtrip(p);
	}

	// TERMINATION payload
	{
		auto p = Payload::make("pub", 3, 0, PayloadKind::TERMINATION);
		roundtrip(p);
		assert(p.data_size == 1);
		assert(p.byte_size == 1);
		assert(p.bytes.size() == 1);
		assert(p.bytes[0] == 0xFF);
	}

	std::cout << "PayloadTest passed\n";
	return 0;
}
