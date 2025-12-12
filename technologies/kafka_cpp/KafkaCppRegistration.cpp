#include "PublisherFactory.hpp"
#include "./KafkaCppPublisher.hpp"
#include "ConsumerFactory.hpp"
#include "./KafkaCppConsumer.hpp"

extern "C" void register_technology(std::shared_ptr<Logger> logger) {
    PublisherFactory::registerPublisher("kafka_cpp", [](std::shared_ptr<Logger> logger) -> std::unique_ptr<IPublisher> {
        return std::make_unique<KafkaCppPublisher>(logger);
    });
    ConsumerFactory::registerConsumer("kafka_cpp", [](std::shared_ptr<Logger> logger) -> std::unique_ptr<IConsumer> {
        return std::make_unique<KafkaCppConsumer>(logger);
    });
    logger->log_debug("[Kafka Registration] Registered creators in factories");
    PublisherFactory::debug_print_registry(logger);
    ConsumerFactory::debug_print_registry(logger);
}