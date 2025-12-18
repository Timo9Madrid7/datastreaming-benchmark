#include "ConsumerApp.hpp"

#include <chrono>
#include <exception>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>

#include "Factory.hpp"
#include "IConsumer.hpp"
#include "TechnologyLoader.hpp"
#include "Utils.hpp"

ConsumerApp::ConsumerApp(Logger::LogLevel log_level) {
	logger = std::make_shared<Logger>(log_level);
}

void ConsumerApp::create_consumer() {
	std::optional<std::string> technology = utils::get_env_var("TECHNOLOGY");
	if (!technology) {
		std::string err_msg =
		    "[ConsumerApp] Missing required environment variable TECHNOLOGY.";
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	}
	logger->log_debug("[ConsumerApp] Creating consumer for technology "
	                  + technology.value() + ", log_level: "
	                  + Logger::level_to_string(logger->get_level()));

	std::string tech_lib;
#ifdef _WIN32
	tech_lib = technology.value() + "_technology.dll"; // or with full path
#else
	std::string tech_lib_dir =
	    utils::get_env_var_or_default("TECHNOLOGY_DIR", "/app/lib/lib");
	tech_lib = tech_lib_dir + technology.value() + "_technology.so";
	logger->log_debug("[ConsumerApp] Using technology lib: " + tech_lib);
#endif

	TechnologyLoader::load_technology(tech_lib, logger);
	consumer = Factory<IConsumer>::create(technology.value(), logger);
	logger->log_debug("[ConsumerApp] Created " + technology.value()
	                  + " consumer");
}

void ConsumerApp::run() {
	logger->log_info("[ConsumerApp] Initializing");
	consumer->initialize();
	int sleep_time = 4000; // milliseconds

	std::optional<std::string> technology = utils::get_env_var("TECHNOLOGY");
	if (!technology) {
		std::string err_msg =
		    "[ConsumerApp] Missing required environment variable TECHNOLOGY.";
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	}

	if (technology.value().find("p2p") == std::string::npos) {
		// Give some time for the broker&producer to initialize
		logger->log_info("[ConsumerApp] Wait" + std::to_string(sleep_time)
		                 + "ms for initialization");
		std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
	}
	logger->log_info("[ConsumerApp] Initialized");

	logger->log_info("[ConsumerApp] Start receiving loop");
	consumer->start_loop(); // technology-specific start receiving loop
}

int main(int argc, char *argv[]) {
	// Disable synchronization between C and C++ standard streams
	std::ios::sync_with_stdio(false);
	std::cout << "[ConsumerApp] Start" << std::endl << std::flush;
	try {
		Logger::LogLevel log_level = Logger::LogLevel::INFO;
		if (argc >= 2 && argv[1] != nullptr) {
			log_level = Logger::string_to_level(argv[1]);
		}
		ConsumerApp app = ConsumerApp(log_level);
		app.create_consumer();
		app.run();
	} catch (const std::exception &e) {
		std::cerr << "[ConsumerApp] Exception caught: " << e.what()
		          << std::endl;
	} catch (...) {
		std::cerr << "[ConsumerApp] Unknown exception caught!" << std::endl;
	}
	return 0;
}