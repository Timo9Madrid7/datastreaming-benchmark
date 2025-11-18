#ifndef IPUBLISHER_HPP
#define IPUBLISHER_HPP

#include <string>
#include <memory>
#include "Logger.hpp"
#include "Payload.h"

class IPublisher {
protected:
    std::shared_ptr<Logger> logger;
    
private:
    // Serializes a Payload object to a string format
    virtual std::string serialize(const Payload& message) = 0;

    // Log publisher configuration during runtime
    virtual void log_configuration() = 0;

public:
    IPublisher(std::shared_ptr<Logger> loggerp) {
        logger = loggerp;
    }
    virtual ~IPublisher() = default;

    // Initializes the publisher (e.g., connects to a broker)
    virtual void initialize() = 0;

    // Sends a message
    virtual void send_message(const Payload &message, std::string topic) = 0;
};

#endif // IPUBLISHER_HPP
