#include "./ArrowFlightConsumer.hpp"
#include "./ArrowFlightPublisher.hpp"
#include "Factory.hpp"
#include "IConsumer.hpp"
#include "IPublisher.hpp"

extern "C" void register_technology(std::shared_ptr<Logger> logger) {
	Factory<IPublisher>::registerClient(
	    "arrowflight_p2p",
	    [](std::shared_ptr<Logger> logger) -> std::unique_ptr<IPublisher> {
		    return std::make_unique<ArrowFlightPublisher>(logger);
	    });
	Factory<IConsumer>::registerClient(
	    "arrowflight_p2p",
	    [](std::shared_ptr<Logger> logger) -> std::unique_ptr<IConsumer> {
		    return std::make_unique<ArrowFlightConsumer>(logger);
	    });
	logger->log_debug(
	    "[Arrow Flight Registration] Registered creators in factories");
	Factory<IPublisher>::debug_print_registry(logger);
	Factory<IConsumer>::debug_print_registry(logger);
}