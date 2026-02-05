#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <string>
#include <sys/types.h>

#include "Logger.hpp"

class IConsumer {
  private:
	/**
	@brief Logs the configuration of the consumer.
	*/
	virtual void log_configuration() = 0;

	/**
	@brief A thread-safe counter for tracking the number of subscribed streams.
	*/
	class StreamCounter {
	  public:
		void inc() {
			value.fetch_add(1, std::memory_order_relaxed);
		}

		void dec() {
			size_t cur = value.load(std::memory_order_relaxed);
			while (cur > 0) {
				if (value.compare_exchange_weak(cur, cur - 1,
				                                std::memory_order_relaxed)) {
					return;
				}
			}
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
