#pragma once

#include "IConsumer.hpp"
#include "Payload.hpp"

#include <librdkafka/rdkafkacpp.h>
#include <memory>
#include <vector>

class KafkaCppConsumer : public IConsumer {
    public:
    KafkaCppConsumer(std::shared_ptr<Logger> logger);
    ~KafkaCppConsumer() override;

    void initialize() override;
    void subscribe(const std::string& topic) override;
    Payload receive_message() override;
    Payload deserialize(const std::string& raw) override;
    Payload deserialize(const void* data_ptr, size_t len);
    void log_configuration() override;

    private:
    std::string broker_;
    std::vector<std::string> topic_names_;

    std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
    std::unique_ptr<RdKafka::Conf> conf_;
    std::unique_ptr<RdKafka::TopicPartition> subscription_list_;
};