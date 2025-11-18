#ifndef ICONSUMER_APP_HPP
#define ICONSUMER_APP_HPP

#include <string>
#include <iostream>
#include <thread>
#include <fstream>
#include <chrono>

#include "Logger.hpp"
#include "ConsumerFactory.hpp"
#include "TechnologyLoader.hpp"
#include "IConsumer.hpp"

class ConsumerApp {
protected:
    std::string id;
    std::string topics;
    
    std::shared_ptr<Logger> logger;

    std::unique_ptr<IConsumer> consumer;

public:
    ConsumerApp(Logger::LogLevel log_level = Logger::LogLevel::INFO);
    ~ConsumerApp() = default;

    // Factory call to Create Consumer
    void create_consumer();

    // Initializes and runs the consumer logic
    void run();
};

#endif // ICONSUMER_APP_HPP
