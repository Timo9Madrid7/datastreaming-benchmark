#include "PublisherFactory.hpp"
#include "./ZeroMQP2PPublisher.hpp"
#include "ConsumerFactory.hpp"
#include "./ZeroMQP2PConsumer.hpp"

extern "C" void register_technology(std::shared_ptr<Logger> logger) {
    PublisherFactory::registerPublisher("zeromq_p2p", [](std::shared_ptr<Logger> logger) -> std::unique_ptr<IPublisher> {
        return std::make_unique<ZeroMQP2PPublisher>(logger);
    });
    ConsumerFactory::registerConsumer("zeromq_p2p", [](std::shared_ptr<Logger> logger) -> std::unique_ptr<IConsumer> {
        return std::make_unique<ZeroMQP2PConsumer>(logger);
    });
    logger->log_debug("[ZeroMQP2P Registration] Registered creators in factories");
    PublisherFactory::debug_print_registry(logger);
    ConsumerFactory::debug_print_registry(logger);
}
