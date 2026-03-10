#include <memory>

#include "./GrpcConsumer.hpp"
#include "./GrpcPublisher.hpp"
#include "Factory.hpp"
#include "IConsumer.hpp"
#include "IPublisher.hpp"
#include "Logger.hpp"

extern "C" void register_technology(std::shared_ptr<Logger> logger) {
	Factory<IPublisher>::registerClient(
	    "grpc_p2p",
	    [](std::shared_ptr<Logger> logger) -> std::unique_ptr<IPublisher> {
		    return std::make_unique<GrpcPublisher>(logger);
	    });
	Factory<IConsumer>::registerClient(
	    "grpc_p2p",
	    [](std::shared_ptr<Logger> logger) -> std::unique_ptr<IConsumer> {
		    return std::make_unique<GrpcConsumer>(logger);
	    });
	logger->log_debug("[gRPC Registration] Registered creators in factories");
	Factory<IPublisher>::debug_print_registry(logger);
	Factory<IConsumer>::debug_print_registry(logger);
}