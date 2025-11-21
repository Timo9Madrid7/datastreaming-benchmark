#include "Logger.hpp"

#include <spdlog/sinks/stdout_color_sinks.h>

#include <iostream>

// Constructor
Logger::Logger(Logger::LogLevel log_level) : current_log_level(log_level) {
    try {
        // Queue size: 8192, backing threads: 1
        spdlog::init_thread_pool(8192, 1);

        // Create a color sink for stdout
        auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();

        // Create the async logger
        async_logger =
            std::make_shared<spdlog::async_logger>("async_logger", console_sink, spdlog::thread_pool(),
                                                   spdlog::async_overflow_policy::block  // Block if queue is full, or use overrun_oldest
            );

        // Register the logger globally (optional, but good for finding it later if needed)
        spdlog::register_logger(async_logger);

        // Set the pattern: [Time] [Level] Message
        async_logger->set_pattern("[%H:%M:%S.%e] [%^%l%$] %v");

        // Set the initial level
        set_level(log_level);
    } catch (const spdlog::spdlog_ex& ex) {
        std::cerr << "Log initialization failed: " << ex.what() << std::endl;
    }
}

// Destructor
Logger::~Logger() {
    if (async_logger) {
        async_logger->flush();
    }
    spdlog::drop_all();
}

// Set the log level
void Logger::set_level(Logger::LogLevel level) {
    current_log_level = level;
    if (async_logger) {
        async_logger->set_level(to_spdlog_level(level));
    }
}

// Helper function to convert LogLevel to string
std::string Logger::level_to_string(Logger::LogLevel level) {
    switch (level) {
        case Logger::LogLevel::DEBUG:
            return "DEBUG";
        case Logger::LogLevel::INFO:
            return "INFO";
        case Logger::LogLevel::STUDY:
            return "STUDY";
        case Logger::LogLevel::CONFIG:
            return "CONFIG";
        case Logger::LogLevel::ERROR:
            return "ERROR";
        default:
            return "UNKNOWN";
    }
}

// Helper function to convert string to LogLevel
Logger::LogLevel Logger::string_to_level(const std::string& level) {
    if (level == "DEBUG") return LogLevel::DEBUG;
    if (level == "INFO") return LogLevel::INFO;
    if (level == "STUDY") return LogLevel::STUDY;
    if (level == "CONFIG") return LogLevel::CONFIG;
    if (level == "ERROR") return LogLevel::ERROR;
    return LogLevel::INFO;  // Default
}

// Map internal LogLevel to spdlog::level
spdlog::level::level_enum Logger::to_spdlog_level(LogLevel level) {
    switch (level) {
        case LogLevel::DEBUG:
            return spdlog::level::debug;
        case LogLevel::INFO:
            return spdlog::level::info;
        // Mapping custom levels to standard spdlog levels
        case LogLevel::STUDY:
            return spdlog::level::warn;
        case LogLevel::CONFIG:
            return spdlog::level::info;
        case LogLevel::ERROR:
            return spdlog::level::err;
        default:
            return spdlog::level::info;
    }
}

// Get log level
Logger::LogLevel Logger::get_level() { return current_log_level; }

// Log debug messages
void Logger::log_debug(const std::string& message) {
    if (async_logger) async_logger->debug(message);
}

// Log info messages
void Logger::log_info(const std::string& message) {
    if (async_logger) async_logger->info(message);
}

// Log study messages
void Logger::log_study(const std::string& message) {
    // Mapping STUDY to warn or info depending on preference,
    // or just logging as info with a prefix if spdlog doesn't support custom levels easily.
    // Here we map it to WARN for visibility.
    if (async_logger) async_logger->warn("[STUDY]" + message);
}

// Log config messages
void Logger::log_config(const std::string& message) {
    if (async_logger) async_logger->info("[CONFIG]" + message);
}

// Log error messages
void Logger::log_error(const std::string& message) {
    if (async_logger) async_logger->error(message);
}
