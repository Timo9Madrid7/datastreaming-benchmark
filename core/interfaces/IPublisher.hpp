#pragma once

#include <memory>
#include <string>

#include "Logger.hpp"
#include "Payload.hpp"

struct Payload;

class IPublisher {
  protected:
	std::shared_ptr<Logger> logger;

  private:
	/**
	@brief Logs the configuration of the publisher.
	*/
	virtual void log_configuration() = 0;

  public:
	IPublisher(std::shared_ptr<Logger> loggerp) {
		logger = loggerp;
	}
	virtual ~IPublisher() = default;

	/**
	@brief Initializes the publisher by connecting to the message broker.
	*/
	virtual void initialize() = 0;

	/**
	@brief Sends a message to a specific topic.
	@param message The Payload object to send.
	*/
	virtual void send_message(const Payload &message, std::string &topic) = 0;
};
