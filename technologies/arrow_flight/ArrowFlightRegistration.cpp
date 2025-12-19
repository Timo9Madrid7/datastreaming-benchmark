#include "Factory.hpp"
#include "IConsumer.hpp"
#include "./ArrowFlightConsumer.hpp"
#include "./ArrowFlightPublisher.hpp"
#include "IPublisher.hpp"

extern "C" void register_technology(std::shared_ptr<Logger> logger) {
    Factory<IPublisher>::registerClient("arrow_flight", [](std::shared_ptr<Logger> logger) -> std::unique_ptr<IPublisher> {
        return std::make_unique<ArrowFlightPublisher>(logger);
    });
    Factory<IConsumer>::registerClient("arrow_flight", [](std::shared_ptr<Logger> logger) -> std::unique_ptr<IConsumer> {
        return std::make_unique<ArrowFlightConsumer>(logger);
    });
    logger->log_debug("[Arrow Flight Registration] Registered creators in factories");
    Factory<IPublisher>::debug_print_registry(logger);
    Factory<IConsumer>::debug_print_registry(logger);
}