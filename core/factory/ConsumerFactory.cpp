#include "ConsumerFactory.hpp"

std::unordered_map<std::string, ConsumerFactory::CreateFunc>& ConsumerFactory::getRegistry() {
    static std::unordered_map<std::string, CreateFunc> registry;
    return registry;
}

void ConsumerFactory::registerConsumer(const std::string& name, CreateFunc func) {
    getRegistry()[name] = func;
}

std::unique_ptr<IConsumer> ConsumerFactory::create(const std::string& name, std::shared_ptr<Logger> logger) {
    auto it = getRegistry().find(name);
    if (it != getRegistry().end()) {
        return it->second(logger);
    }
    throw std::runtime_error("Consumer type not registered");
}

void ConsumerFactory::debug_print_registry(std::shared_ptr<Logger> logger) {
    logger->log_debug("[ConsumerFactory] Registered consumer types:");
    for (const auto& entry : getRegistry()) {
        logger->log_debug(" - " + entry.first);
    }
}