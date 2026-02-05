#pragma once

#include <memory>

#include "IConsumer.hpp"
#include "Logger.hpp"

class ConsumerApp {
  protected:
	std::shared_ptr<Logger> logger;
	std::unique_ptr<IConsumer> consumer;

  public:
	ConsumerApp(Logger::LogLevel log_level = Logger::LogLevel::INFO);
	~ConsumerApp() = default;

	/**
	@brief Creates a consumer based on the TECHNOLOGY environment variable.
	@throws std::runtime_error if the TECHNOLOGY environment variable is not set
	        or if the consumer creation fails.
	*/
	void create_consumer();

	/**
	@brief Runs the consumer application, initializing the consumer and
	       continuously receiving messages.
	@throws std::runtime_error if initialization fails or if message
	        reception encounters an error.
	*/
	void run();
};
