#include "./NatsConsumer.hpp"
#include "./NatsPublisher.hpp"
#include "Factory.hpp"
#include "IConsumer.hpp"
#include "IPublisher.hpp"

extern "C" void register_technology(std::shared_ptr<Logger> logger) {
	Factory<IPublisher>::registerClient(
	    "nats_p2p",
	    [](std::shared_ptr<Logger> logger) -> std::unique_ptr<IPublisher> {
		    return std::make_unique<NatsPublisher>(logger);
	    });
	Factory<IConsumer>::registerClient(
	    "nats_p2p",
	    [](std::shared_ptr<Logger> logger) -> std::unique_ptr<IConsumer> {
		    return std::make_unique<NatsConsumer>(logger);
	    });
	logger->log_debug(
	    "[Arrow Flight Registration] Registered creators in factories");
	Factory<IPublisher>::debug_print_registry(logger);
	Factory<IConsumer>::debug_print_registry(logger);
}