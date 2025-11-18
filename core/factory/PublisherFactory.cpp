#include "PublisherFactory.hpp"

std::unordered_map<std::string, PublisherFactory::CreateFunc>& PublisherFactory::getRegistry() {
    static std::unordered_map<std::string, CreateFunc> registry;
    return registry;
}

void PublisherFactory::registerPublisher(const std::string& name, CreateFunc func) {
    getRegistry()[name] = func;
}

std::unique_ptr<IPublisher> PublisherFactory::create(const std::string& name, std::shared_ptr<Logger> logger) {
    auto it = getRegistry().find(name);
    if (it != getRegistry().end()) {
        return it->second(logger);
    }
    throw std::runtime_error("Publisher type not registered");
}

void PublisherFactory::debug_print_registry(std::shared_ptr<Logger> logger) {
    logger->log_debug("[PublisherFactory] Registered publisher types:");
    for (const auto& entry : getRegistry()) {
        logger->log_debug(" - " + entry.first);
    }
}