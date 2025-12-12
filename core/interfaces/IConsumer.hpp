#pragma once

#include <memory>
#include <set>
#include <string>

#include "Logger.hpp"
#include "Payload.hpp"

class IConsumer {
  protected:
	std::shared_ptr<Logger> logger;
	std::set<std::pair<std::string, std::string>> terminated_streams;
	std::set<std::pair<std::string, std::string>> subscribed_streams;

  private:
	/**
	@brief Logs the configuration of the consumer.
	*/
	virtual void log_configuration() = 0;

	/**
	@brief Deserializes a raw message string into a Payload object.
	@param raw_message The raw message string to deserialize.
	@return The deserialized Payload object.
	*/
	virtual Payload deserialize(const std::string &raw_message) = 0;

  public:
	IConsumer(std::shared_ptr<Logger> loggerp) {
		logger = loggerp;
	}
	virtual ~IConsumer() = default;

	/**
	@brief Initializes the consumer by connecting to the message broker and
	setting up subscriptions.
	*/
	virtual void initialize() = 0;

	/**
	@brief Subscribes to a specific topic.
	@param topic The topic to subscribe to.
	*/
	virtual void subscribe(const std::string &topic) = 0;

	/**
	@brief Receives a message from the subscribed topics.
	@return The received Payload object.
	*/
	virtual Payload receive_message() = 0;

	/**
	@brief Gets the number of subscribed streams.
	@return The number of subscribed streams.
	*/
	int get_subscribed_streams_size() const {
		return static_cast<int>(subscribed_streams.size());
	}

	/**
	@brief Gets the number of terminated streams.
	@return The number of terminated streams.
	*/
	int get_terminated_streams_size() const {
		return static_cast<int>(terminated_streams.size());
	}
};
