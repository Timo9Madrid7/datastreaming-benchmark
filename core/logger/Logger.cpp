#include "Logger.hpp"
#include <chrono>
#include <iomanip>
#include <sstream>

std::string get_current_timestamp() {
    using namespace std::chrono;

    auto now = system_clock::now();
    auto time_t_now = system_clock::to_time_t(now);
    auto local_time = *std::localtime(&time_t_now);

    // Calculate milliseconds part
    auto ms_part = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;

    std::ostringstream oss;
    oss << std::put_time(&local_time, "%Y-%m-%d %H:%M:%S");
    oss << "." << std::setfill('0') << std::setw(3) << ms_part.count();

    return oss.str();
}

std::string make_message(std::string message){
    return get_current_timestamp() + "," + message;
}

// Constructor
Logger::Logger(Logger::LogLevel log_level) : log_level(log_level) {}

// Destructor
Logger::~Logger() {}

// Set the log level
void Logger::set_level(Logger::LogLevel level) {
    log_level = level;
}

// Helper function to convert LogLevel to string
std::string Logger::level_to_string(Logger::LogLevel level) {
    switch (level) {
        case Logger::LogLevel::DEBUG: return "DEBUG";
        case Logger::LogLevel::INFO: return "INFO";
        case Logger::LogLevel::STUDY: return "STUDY";
        case Logger::LogLevel::CONFIG: return "CONFIG";
        case Logger::LogLevel::ERROR: return "ERROR";
        default: return "UNKNOWN";
    }
}

// Helper function to convert string to LogLevel
Logger::LogLevel Logger::string_to_level(const std::string& level) {
    if (level.empty()) {
        std::cerr <<"Missing log level: " << level << ", defaulting to INFO" << std::endl;
        return Logger::LogLevel::INFO;
    }

    if (level == "DEBUG") {
        return Logger::LogLevel::DEBUG;
    } else if (level == "INFO") {
        return Logger::LogLevel::INFO;
    } else if (level == "STUDY") {
        return Logger::LogLevel::STUDY;
    } else if (level == "CONFIG") {
        return Logger::LogLevel::CONFIG;
    } else if (level == "ERROR") {
        return Logger::LogLevel::ERROR;
    } else {
        std::cerr <<"Invalid log level: " << level << ", defaulting to INFO" << std::endl;
        return Logger::LogLevel::INFO;
    }
}


// Get log level
Logger::LogLevel Logger::get_level(){
    return log_level;
}

// Log debug messages
void Logger::log_debug(const std::string& message) {
    if (log_level <= Logger::LogLevel::DEBUG) {
        std::cout << "[" << level_to_string(Logger::LogLevel::DEBUG) << "] " << make_message(message) << std::endl;
    }
}

// Log info messages
void Logger::log_info(const std::string& message) {
    if (log_level <= Logger::LogLevel::INFO) {
        std::cout << "[" << level_to_string(Logger::LogLevel::INFO) << "] " << make_message(message) << std::endl;
    }
}

// Log study messages
void Logger::log_study(const std::string& message) {
    if (log_level <= Logger::LogLevel::STUDY) {
        std::cout << "[" << level_to_string(Logger::LogLevel::STUDY) << "] " << make_message(message) << std::endl;
    }
}

// Log config messages
void Logger::log_config(const std::string& message) {
    if (log_level <= Logger::LogLevel::CONFIG) {
        std::cout << "[" << level_to_string(Logger::LogLevel::CONFIG) << "] " << make_message(message) << std::endl;
    }
}

// Log error messages
void Logger::log_error(const std::string& message) {
    if (log_level <= Logger::LogLevel::ERROR) {
        std::cout << "[" << level_to_string(Logger::LogLevel::ERROR) << "] " << make_message(message) << std::endl;
    }
}
