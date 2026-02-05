#include "Factory.hpp"
#include "IConsumer.hpp"
#include "./ZeroMQP2PPublisher.hpp"
#include "./ZeroMQP2PConsumer.hpp"
#include "IPublisher.hpp"

extern "C" void register_technology(std::shared_ptr<Logger> logger) {
    Factory<IPublisher>::registerClient("zeromq_p2p", [](std::shared_ptr<Logger> logger) -> std::unique_ptr<IPublisher> {
        return std::make_unique<ZeroMQP2PPublisher>(logger);
    });
    Factory<IConsumer>::registerClient("zeromq_p2p", [](std::shared_ptr<Logger> logger) -> std::unique_ptr<IConsumer> {
        return std::make_unique<ZeroMQP2PConsumer>(logger);
    });
    logger->log_debug("[ZeroMQP2P Registration] Registered creators in factories");
    Factory<IPublisher>::debug_print_registry(logger);
    Factory<IConsumer>::debug_print_registry(logger);
}
