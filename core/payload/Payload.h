#pragma once
#ifndef _PAYLOAD_H_
#define _PAYLOAD_H_

#include <string>
#include <vector>
#include <cstdint>
#include <cstdlib>
#include <iostream>

enum class PayloadKind {
    TERMINATION = 0,
    BOOLEAN,
    FLAT,
    COMPLEX
};
const std::string TERMINATION_SIGNAL = "__END__";

struct Payload {
    std::string message_id;
    PayloadKind kind;
    size_t data_size;
    std::vector<uint8_t> data;

    // Create a fully generated payload with specified kind and size
    static Payload make(const std::string& publisher_id, int sequence_number, size_t data_size, PayloadKind kind = PayloadKind::FLAT);

    // Create a payload by reusing existing data with a new message_id
    static Payload reuse_with_new_id(const std::string& publisher_id, int sequence_number, Payload message);

    static PayloadKind string_to_payloadkind(const std::string& kind_str);
    static std::string payloadkind_to_string(PayloadKind kind);
};

#endif 