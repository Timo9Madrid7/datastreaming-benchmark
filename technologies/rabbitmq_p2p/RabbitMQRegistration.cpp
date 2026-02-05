#include <memory>

#include "./RabbitMQConsumer.hpp"
#include "./RabbitMQPublisher.hpp"
#include "Factory.hpp"
#include "IConsumer.hpp"
#include "IPublisher.hpp"

extern "C" void register_technology(std::shared_ptr<Logger> logger) {
	Factory<IPublisher>::registerClient(
	    "rabbitmq_p2p",
	    [](std::shared_ptr<Logger> logger) -> std::unique_ptr<IPublisher> {
		    return std::make_unique<RabbitMQPublisher>(logger);
	    });
	Factory<IConsumer>::registerClient(
	    "rabbitmq_p2p",
	    [](std::shared_ptr<Logger> logger) -> std::unique_ptr<IConsumer> {
		    return std::make_unique<RabbitMQConsumer>(logger);
	    });
	logger->log_debug(
	    "[RabbitMQ Registration] Registered creators in factories");
	Factory<IPublisher>::debug_print_registry(logger);
	Factory<IConsumer>::debug_print_registry(logger);
}