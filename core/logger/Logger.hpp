#pragma once

#include <ctime>
#include <memory>
#include <spdlog/async.h>
#include <spdlog/common.h>
#include <spdlog/pattern_formatter.h>
#include <spdlog/spdlog.h>
#include <string>

class Logger {
  public:
	// Define log levels
	enum class LogLevel { DEBUG, INFO, STUDY, CONFIG, ERROR };

	Logger(Logger::LogLevel log_level = Logger::LogLevel::INFO);
	~Logger();

	/**
	@brief Set the log level
	@param level The desired log level
	*/
	void set_level(Logger::LogLevel level);

	/**
	@brief Get the current log level
	@return The current log level
	*/
	Logger::LogLevel get_level();

	void log_debug(const std::string &message);
	void log_info(const std::string &message);
	void log_study(const std::string &message);
	void log_config(const std::string &message);
	void log_error(const std::string &message);

	/**
	@brief Convert LogLevel to string
	@param level The log level to convert
	@return The string representation of the log level
	*/
	static std::string level_to_string(LogLevel level);

	/**
	@brief Convert string to LogLevel
	@param level The string representation of the log level
	@return The corresponding LogLevel
	*/
	static LogLevel string_to_level(const std::string &level);

  private:
	LogLevel current_log_level;
	std::shared_ptr<spdlog::async_logger> async_logger;

	/**
	@brief Map internal LogLevel to spdlog::level
	@param level The internal log level
	@return The corresponding spdlog::level
	*/
	spdlog::level::level_enum to_spdlog_level(LogLevel level);
};

class CustomLevelFlag : public spdlog::custom_flag_formatter {
  public:
	/**
	@brief Format the log message to include custom level strings
	@param msg The log message
	@param tm The time structure
	@param dest The destination buffer to write the formatted string
	*/
	void format(const spdlog::details::log_msg &msg, const std::tm &,
	            spdlog::memory_buf_t &dest) override;

	/**
	@brief Clone the formatter
	@return A unique pointer to the cloned formatter
	*/
	std::unique_ptr<spdlog::custom_flag_formatter> clone() const override;
};