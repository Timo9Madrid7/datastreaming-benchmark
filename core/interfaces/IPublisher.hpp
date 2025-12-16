#pragma once

#include <memory>
#include <string>
#include <vector>

#include "Logger.hpp"
#include "Payload.hpp"

class IPublisher {
  protected:
	std::shared_ptr<Logger> logger;

  private:
	/**
	@brief Serializes a vector of Payload objects into a raw message.
	@param messages The vector of Payload objects to serialize.
	@param out The output buffer to store the serialized message.
	@return True if serialization was successful, false otherwise.
	*/
	virtual bool serialize(const std::vector<Payload> &messages, void *out) = 0;

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
	virtual void send_message(const Payload &message, std::string topic) = 0;
};
