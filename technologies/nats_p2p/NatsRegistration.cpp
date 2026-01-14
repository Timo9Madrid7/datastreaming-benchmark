#include <memory> // for shared_ptr, unique_ptr, make_unique

#include "./NatsConsumer.hpp"  // for NatsConsumer
#include "./NatsPublisher.hpp" // for NatsPublisher
#include "Factory.hpp"         // for Factory
#include "Logger.hpp"          // for Logger

class IConsumer;
class IPublisher;

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
	    "[NATS P2P Registration] Registered creators in factories");
	Factory<IPublisher>::debug_print_registry(logger);
	Factory<IConsumer>::debug_print_registry(logger);
}