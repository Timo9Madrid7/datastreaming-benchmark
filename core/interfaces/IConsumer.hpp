#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <string>
#include <sys/types.h>

#include "Logger.hpp"
#include "Payload.hpp"

class IConsumer {
  private:
	/**
	@brief Logs the configuration of the consumer.
	*/
	virtual void log_configuration() = 0;

	/**
	@brief Deserializes a raw message into a vector of Payload objects.
	@param raw_message The raw message to deserialize.
	@param len The length of the raw message.
	@param out The Payload object to store the deserialized message.
	@return True if deserialization was successful, false otherwise.
	*/
	virtual bool deserialize(const void *raw_message, size_t len,
	                         Payload &out) = 0;

	/**
	@brief A thread-safe counter for tracking the number of subscribed streams.
	*/
	class StreamCounter {
	  public:
		void inc() {
			value.fetch_add(1, std::memory_order_relaxed);
		}

		void dec() {
			if (value.load(std::memory_order_relaxed) == 0) {
				return;
			}
			value.fetch_sub(1, std::memory_order_relaxed);
		}

		size_t get() const {
			return value.load(std::memory_order_relaxed);
		}

	  private:
		std::atomic<size_t> value{0};
	};

  protected:
	std::shared_ptr<Logger> logger;
	StreamCounter subscribed_streams;

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
	@brief Starts receiving messages from the subscribed topics until all
	streams have been terminated.
	*/
	virtual void start_loop() = 0;
};
