#include "Factory.hpp"
#include "IConsumer.hpp"
#include "./KafkaCppPublisher.hpp"
#include "./KafkaCppConsumer.hpp"
#include "IPublisher.hpp"

extern "C" void register_technology(std::shared_ptr<Logger> logger) {
    Factory<IPublisher>::registerClient("kafka_p2p", [](std::shared_ptr<Logger> logger) -> std::unique_ptr<IPublisher> {
        return std::make_unique<KafkaCppPublisher>(logger);
    });
    Factory<IConsumer>::registerClient("kafka_p2p", [](std::shared_ptr<Logger> logger) -> std::unique_ptr<IConsumer> {
        return std::make_unique<KafkaCppConsumer>(logger);
    });
    logger->log_debug("[Kafka Registration] Registered creators in factories");
    Factory<IPublisher>::debug_print_registry(logger);
    Factory<IConsumer>::debug_print_registry(logger);
}