#ifndef LOGGER_HPP
#define LOGGER_HPP

#include <iostream>
#include <string>
#include <spdlog/spdlog.h>
#include <spdlog/async.h>

class Logger {
public:
    // Define log levels
    enum class LogLevel {
        DEBUG,
        INFO,
        STUDY,
        CONFIG,
        ERROR
    };

    // Constructor & Destructor
    Logger(Logger::LogLevel log_level = Logger::LogLevel::INFO);
    ~Logger();

    // Set the log level
    void set_level(Logger::LogLevel level);

    // Get the log level
    Logger::LogLevel get_level();

    // Log functions
    void log_debug(const std::string& message);
    void log_info(const std::string& message);
    void log_study(const std::string& message);
    void log_config(const std::string& message);
    void log_error(const std::string& message);

    // Helper function to convert LogLevel to string
    static std::string level_to_string(LogLevel level);

    // Static function to convert string to LogLevel
    static LogLevel string_to_level(const std::string& level);

private:
    LogLevel current_log_level; // Current log level
    std::shared_ptr<spdlog::async_logger> async_logger;

    // Helper to map internal LogLevel to spdlog::level
    spdlog::level::level_enum to_spdlog_level(LogLevel level);
    
};

#endif // LOGGER_H
