#include <memory>

#include "./NatsJetStreamConsumer.hpp"
#include "./NatsJetStreamPublisher.hpp"
#include "Factory.hpp"
#include "Logger.hpp"

class IConsumer;
class IPublisher;

extern "C" void register_technology(std::shared_ptr<Logger> logger) {
	Factory<IPublisher>::registerClient(
	    "nats_jetstream_p2p",
	    [](std::shared_ptr<Logger> logger) -> std::unique_ptr<IPublisher> {
		    return std::make_unique<NatsJetStreamPublisher>(logger);
	    });
	Factory<IConsumer>::registerClient(
	    "nats_jetstream_p2p",
	    [](std::shared_ptr<Logger> logger) -> std::unique_ptr<IConsumer> {
		    return std::make_unique<NatsJetStreamConsumer>(logger);
	    });
	logger->log_debug(
	    "[NATS JetStream P2P Registration] Registered creators in factories");
	Factory<IPublisher>::debug_print_registry(logger);
	Factory<IConsumer>::debug_print_registry(logger);
}
