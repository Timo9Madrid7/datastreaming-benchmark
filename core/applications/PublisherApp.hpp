#ifndef IPUBLISHER_APP_HPP
#define IPUBLISHER_APP_HPP

#include <string>
#include <memory>
#include <thread>
#include <iostream>
#include <fstream>
#include <sstream>
#include <chrono>
#include <random>

#include "Logger.hpp"
#include "PublisherFactory.hpp"
#include "TechnologyLoader.hpp"
#include "IPublisher.hpp"


class PublisherApp {
protected:
    std::string id;
    std::string topics;
    int message_count;
    int duration;
    int update_every;
    size_t payload_size;
    size_t payload_samples;
    PayloadKind payload_kind;
    std::vector<Payload> payloads;

    std::shared_ptr<Logger> logger;

    std::unique_ptr<IPublisher> publisher;

public:
    PublisherApp(Logger::LogLevel log_level = Logger::LogLevel::INFO);
    ~PublisherApp() = default;


    // Loads values from environment variables into the attributes
    void load_from_env();

    // Factory call to Create Publisher
    void create_publisher();

    // Runs the publisher logic (can now be fully generalized)
    void run();

private:
    // Send update on topic
    void publish_on_topic(std::string topic, Payload message);

    // Terminate topic
    void terminate_topic(std::string topic);

    // Send update on all topics
    void publish_on_all_topics(Payload message);

    // Terminate all topics
    void terminate_all_topics();

    // Runs to send a number of messages
    void run_messages();

    // Runs for a set duration
    void run_duration();

    Payload generate_message(int i);

    Payload generate_termination_message();

    Payload generate_payload_in_memory(size_t target_bytes);

    void generate_payloads(size_t max_size, size_t num_samples);

    const Payload& pick_random_payload();
};

#endif // IPUBLISHER_APP_HPP
