#include "Logger.hpp"

int main() {
    Logger myLogger(Logger::LogLevel::DEBUG);

    myLogger.log_debug("This is a debug message.");
    myLogger.log_info("This is an info message.");
    myLogger.log_study("This is a study message.");
    myLogger.log_config("This is a config message.");
    myLogger.log_error("This is an error message.");

    return 0;
}