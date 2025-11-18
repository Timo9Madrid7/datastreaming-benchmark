#ifndef ICONSUMER_HPP
#define ICONSUMER_HPP

#include <string>
#include <set>
#include <memory>
#include "Logger.hpp"
#include "Payload.h"

class IConsumer {
protected:
    std::shared_ptr<Logger> logger;
    std::set<std::pair<std::string, std::string>> terminated_streams;
    std::set<std::pair<std::string, std::string>> subscribed_streams;

private:
    // Log consumer configuration during runtime
    virtual void log_configuration() = 0;

    // Deserializes a message from a string format to a Payload object
    virtual Payload deserialize(const std::string& raw_message) = 0;

public:
    IConsumer(std::shared_ptr<Logger> loggerp) {
        logger = loggerp;
    }
    virtual ~IConsumer() = default;

    // Initializes the consumer (e.g., connects to a broker, subscribes to a topic)
    virtual void initialize() = 0;

    // Subscribes to a topic (if applicable)
    virtual void subscribe(const std::string &topic) = 0;

    // Receives a message (blocking or non-blocking depending on implementation)
    virtual Payload receive_message() = 0;


    int get_subscribed_streams_size() const {
        return static_cast<int>(subscribed_streams.size());
    }
    int get_terminated_streams_size() const {
        return static_cast<int>(terminated_streams.size());
    }
};

#endif // ICONSUMER_HPP
