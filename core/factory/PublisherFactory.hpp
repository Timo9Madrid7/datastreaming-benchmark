#pragma once
#include <memory>
#include <string>
#include <unordered_map>
#include "IPublisher.hpp"

class PublisherFactory {
public:
    using CreateFunc = std::unique_ptr<IPublisher>(*)(std::shared_ptr<Logger> logger);

    static void registerPublisher(const std::string& name, CreateFunc func);

    static std::unique_ptr<IPublisher> create(const std::string& name, std::shared_ptr<Logger> logger);

    static void debug_print_registry(std::shared_ptr<Logger> logger);
private:
    static std::unordered_map<std::string, CreateFunc>& getRegistry();
};
