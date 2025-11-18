#pragma once

#include "IPublisher.hpp"
#include <librdkafka/rdkafka.h>
#include <string>
#include <unordered_map>

class KafkaPublisher : public IPublisher {
public:
    KafkaPublisher(std::shared_ptr<Logger> logger);
    ~KafkaPublisher() override;

    void initialize() override;
    void send_message(const Payload& message, std::string topic) override;

private:
    std::string serialize(const Payload& message) override;
    void log_configuration() override;
    
    rd_kafka_t* producer_;
    rd_kafka_conf_t* conf_;
    std::string broker_;

    std::unordered_map<std::string, rd_kafka_topic_t*> topic_handles_;

    rd_kafka_topic_t* get_or_create_topic_handle(const std::string& topic);
    void destroy_topic_handle(const std::string& topic);
};
