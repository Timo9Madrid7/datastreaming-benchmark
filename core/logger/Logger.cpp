#include "Logger.hpp"

#include <fmt/format.h>
#include <iostream>
#include <spdlog/async.h>
#include <spdlog/async_logger.h>
#include <spdlog/details/log_msg.h>
#include <spdlog/formatter.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <utility>

void CustomLevelFlag::format(const spdlog::details::log_msg &msg,
                             const std::tm &, spdlog::memory_buf_t &dest) {
	std::string level_name;
	switch (msg.level) {
	case spdlog::level::critical:
		level_name = "STUDY";
		break;
	case spdlog::level::warn:
		level_name = "CONFIG";
		break;
	case spdlog::level::debug:
		level_name = "DEBUG";
		break;
	case spdlog::level::info:
		level_name = "INFO";
		break;
	case spdlog::level::err:
		level_name = "ERROR";
		break;
	default:
		level_name = spdlog::level::to_string_view(msg.level).data();
		break;
	}
	dest.append(level_name.data(), level_name.data() + level_name.size());
}

std::unique_ptr<spdlog::custom_flag_formatter> CustomLevelFlag::clone() const {
	return spdlog::details::make_unique<CustomLevelFlag>();
}

Logger::Logger(Logger::LogLevel log_level) : current_log_level(log_level) {
	try {
		// Queue size: 8192, backing threads: 1
		if (!spdlog::thread_pool()) {
			spdlog::init_thread_pool(16384, 1);
		}

		// Create a color sink for stdout
		auto console_sink =
		    std::make_shared<spdlog::sinks::stdout_color_sink_mt>();

		// Create the async logger
		async_logger = std::make_shared<spdlog::async_logger>(
		    "async_logger", console_sink, spdlog::thread_pool(),
		    spdlog::async_overflow_policy::block // Block if queue is full, or
		                                         // use overrun_oldest
		);

		// Register the logger globally (optional, but good for finding it later
		// if needed)
		spdlog::register_logger(async_logger);

		// Create and set pattern formatter with custom level flag
		auto formatter = std::make_unique<spdlog::pattern_formatter>();
		formatter->add_flag<CustomLevelFlag>('u');
		formatter->set_pattern("[%^%u%$] %Y-%m-%d %H:%M:%S.%f,%v");
		async_logger->set_formatter(std::move(formatter));

		// Set the initial level
		set_level(log_level);

	} catch (const spdlog::spdlog_ex &ex) {
		std::cerr << "Log initialization failed: " << ex.what() << std::endl;
	}
}

Logger::~Logger() {
	if (async_logger) {
		async_logger->flush();
	}
	spdlog::drop(async_logger->name());
}

void Logger::set_level(Logger::LogLevel level) {
	current_log_level = level;
	if (async_logger) {
		async_logger->set_level(to_spdlog_level(level));
	}
}

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

Logger::LogLevel Logger::string_to_level(const std::string &level) {
	if (level == "DEBUG")
		return LogLevel::DEBUG;
	if (level == "INFO")
		return LogLevel::INFO;
	if (level == "STUDY")
		return LogLevel::STUDY;
	if (level == "CONFIG")
		return LogLevel::CONFIG;
	if (level == "ERROR")
		return LogLevel::ERROR;
	return LogLevel::INFO; // Default
}

spdlog::level::level_enum Logger::to_spdlog_level(LogLevel level) {
	switch (level) {
	case LogLevel::DEBUG:
		return spdlog::level::debug;
	case LogLevel::INFO:
		return spdlog::level::info;
	// Mapping custom levels to standard spdlog levels
	case LogLevel::STUDY:
		return spdlog::level::critical; // Using critical for STUDY
	case LogLevel::CONFIG:
		return spdlog::level::warn; // Using warn for CONFIG
	case LogLevel::ERROR:
		return spdlog::level::err;
	default:
		return spdlog::level::info;
	}
}

Logger::LogLevel Logger::get_level() {
	return current_log_level;
}

void Logger::log_debug(const std::string &message) {
	if (async_logger)
		async_logger->debug(message);
}

void Logger::log_info(const std::string &message) {
	if (async_logger)
		async_logger->info(message);
}

void Logger::log_study(const std::string &message) {
	if (async_logger)
		async_logger->critical(message);
}

void Logger::log_config(const std::string &message) {
	if (async_logger)
		async_logger->warn(message);
}

void Logger::log_error(const std::string &message) {
	if (async_logger)
		async_logger->error(message);
}
