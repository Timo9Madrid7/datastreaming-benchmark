#include "Factory.hpp"
#include "IConsumer.hpp"
#include "./KafkaPublisher.hpp"
#include "./KafkaConsumer.hpp"
#include "IPublisher.hpp"

extern "C" void register_technology(std::shared_ptr<Logger> logger) {
    Factory<IPublisher>::registerClient("kafka", [](std::shared_ptr<Logger> logger) -> std::unique_ptr<IPublisher> {
        return std::make_unique<KafkaPublisher>(logger);
    });
    Factory<IConsumer>::registerClient("kafka", [](std::shared_ptr<Logger> logger) -> std::unique_ptr<IConsumer> {
        return std::make_unique<KafkaConsumer>(logger);
    });
    logger->log_debug("[Kafka Registration] Registered creators in factories");
    Factory<IPublisher>::debug_print_registry(logger);
    Factory<IConsumer>::debug_print_registry(logger);
}
