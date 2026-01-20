#pragma once

#include <memory>
#include <stddef.h>
#include <string>
#include <vector>

#include "IPublisher.hpp"
#include "Logger.hpp"
#include "Payload.hpp"
#include "RateLimiter.hpp"

class PublisherApp {
  protected:
	std::string id;
	std::string topics;
	int message_count;
	int duration;
	int update_every; // time interval between messages in duration mode (us)
	size_t payload_size;
	size_t payload_samples;
	PayloadKind payload_kind;
	std::vector<Payload> payloads;

	std::shared_ptr<Logger> logger;
	bool enable_rate_limiter;
	utils::RateLimiter rate_limiter;

	std::unique_ptr<IPublisher> publisher;

  public:
	PublisherApp(Logger::LogLevel log_level = Logger::LogLevel::INFO);
	~PublisherApp() = default;

	/**
	@brief Loads configuration values from environment variables into the class
	attributes.
	*/
	void load_from_env();

	/**
	@brief Creates a publisher instance using the factory method pattern.
	*/
	void create_publisher();

	/**
	@brief Main run loop of the PublisherApp.
	*/
	void run();

  private:
	/**
	@brief Publishes a message on a specific topic.
	@param topic The topic to publish the message on.
	@param message The message payload to be published.
	*/
	void publish_on_topic(std::string topic, Payload message);

	/**
	@brief Terminates a specific topic by sending a termination message.
	@param topic The topic to be terminated.
	*/
	void terminate_topic(std::string topic);

	/**
	@brief Internally calls publish_on_topic for all topics.
	@param message The message payload to be published on all topics.
	*/
	void publish_on_all_topics(Payload message);

	/**
	@brief Internally calls terminate_topic for all topics.
	*/
	void terminate_all_topics();

	/**
	@brief Execute the message-count based publishing loop.
	This method blocks until all messages have been sent.
	*/
	void run_messages();

	/**
	@brief Execute the duration-based publishing loop.
	This method blocks until the specified duration has elapsed.
	*/
	void run_duration();

	/**
	@brief Generates a message payload with a unique message ID.
	@param i An integer used to create a unique message ID.
	*/
	Payload generate_message(int i);

	/**
	@brief Generates a termination message payload.
	*/
	Payload generate_termination_message();

	/**
	@brief Generates a payload of specified size in memory.
	@param target_bytes The size of the payload to generate in bytes.
	*/
	Payload generate_payload_in_memory(size_t target_bytes);

	/**
	@brief Pre-generates a set of payloads to be used during publishing.
	@param max_size The size of each payload in bytes.
	@param num_samples The number of payload samples to generate.
	*/
	void generate_payloads(size_t max_size, size_t num_samples);

	/**
	@brief Picks a random payload from the pre-generated set.
	*/
	const Payload &pick_random_payload();
};
