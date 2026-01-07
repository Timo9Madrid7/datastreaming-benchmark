#include "PublisherApp.hpp"

#include <chrono>
#include <exception>
#include <iostream>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <stdlib.h>
#include <string>
#include <thread>

#include "Factory.hpp"
#include "IPublisher.hpp"
#include "TechnologyLoader.hpp"
#include "Utils.hpp"
#include "cstdlib"


Payload PublisherApp::generate_message(int i) {
	Payload message = pick_random_payload();
	return Payload::reuse_with_new_id(id, i, message);
}

Payload PublisherApp::generate_termination_message() {
	return Payload::make(id, 0, 0, PayloadKind::TERMINATION);
}

void PublisherApp::generate_payloads(size_t target_size, size_t num_samples) {
	for (size_t i = 0; i < num_samples; ++i) {
		payloads.push_back(Payload::make(id, i, target_size, payload_kind));
	}
}

const Payload &PublisherApp::pick_random_payload() {
	int index = utils::Random::random_int<int>(0, payloads.size() - 1);
	return payloads[index];
}

PublisherApp::PublisherApp(Logger::LogLevel log_level) {
	logger = std::make_shared<Logger>(log_level);
	load_from_env();
	generate_payloads(payload_size, payload_samples);
}

void PublisherApp::load_from_env() {
	const std::optional<std::string> env_id =
	    utils::get_env_var("CONTAINER_ID");
	const std::optional<std::string> env_topics = utils::get_env_var("TOPICS");
	const std::optional<std::string> env_psize =
	    utils::get_env_var("PAYLOAD_SIZE");
	const std::optional<std::string> env_psamp =
	    utils::get_env_var("PAYLOAD_SAMPLES");
	const std::optional<std::string> env_pkind =
	    utils::get_env_var("PAYLOAD_KIND");
	const std::string env_update =
	    utils::get_env_var_or_default("UPDATE_EVERY", "0");
	const std::string env_messages =
	    utils::get_env_var_or_default("MESSAGES", "0");
	const std::string env_duration =
	    utils::get_env_var_or_default("DURATION", "0");

	std::string err_msg;
	if (!env_id) {
		err_msg =
		    "[PublisherApp] Missing required environment variable CONTAINER_ID";
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	}
	if (!env_topics) {
		err_msg = "[PublisherApp] Missing required environment variable TOPICS";
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	}
	if (!env_psize) {
		err_msg =
		    "[PublisherApp] Missing required environment variable PAYLOAD_SIZE";
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	}
	if (!env_psamp) {
		err_msg = "[PublisherApp] Missing required environment variable "
		          "PAYLOAD_SAMPLES";
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	}
	if (!env_pkind) {
		err_msg =
		    "[PublisherApp] Missing required environment variable PAYLOAD_KIND";
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	}

	id = env_id.value();
	topics = env_topics.value();
	payload_size = std::atoi(env_psize.value().c_str());
	payload_samples = std::atoi(env_psamp.value().c_str());
	payload_kind = Payload::string_to_payloadkind(env_pkind.value());
	message_count = std::atoi(env_messages.c_str());
	duration = std::atoi(env_duration.c_str());
	update_every = std::atoi(env_update.c_str());

	logger->log_debug(
	    "[PublisherApp] Loaded from environment: ID=" + id
	    + ", TOPICS=" + topics + ", MESSAGES=" + std::to_string(message_count)
	    + ", DURATION=" + std::to_string(duration)
	    + ", UPDATE_EVERY=" + std::to_string(update_every) + " us"
	    + ", PAYLOAD_SIZE=" + std::to_string(payload_size)
	    + ", PAYLOAD_SAMPLES=" + std::to_string(payload_samples)
	    + ", PAYLOAD_KIND=" + Payload::payloadkind_to_string(payload_kind));
}

void PublisherApp::create_publisher() {
	std::optional<std::string> technology = utils::get_env_var("TECHNOLOGY");
	if (!technology) {
		std::string err_msg =
		    "[PublisherApp] Missing required environment variable TECHNOLOGY";
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	}

	logger->log_debug("[PublisherApp] Creating publisher for technology "
	                  + technology.value() + ", log_level: "
	                  + Logger::level_to_string(logger->get_level()));

	std::string tech_lib;
#ifdef _WIN32
	tech_lib = technology + "_technology.dll"; // or with full path
#else
	std::string tech_lib_dir =
	    utils::get_env_var_or_default("TECHNOLOGY_DIR", "/app/lib/lib");
	tech_lib = tech_lib_dir + technology.value() + "_technology.so";
	logger->log_debug("[PublisherApp] Using technology lib: " + tech_lib);
#endif

	TechnologyLoader::load_technology(tech_lib, logger);
	logger->log_debug("[PublisherApp] Factory state before calling 'create'");
	Factory<IPublisher>::debug_print_registry(logger);

	publisher = Factory<IPublisher>::create(technology.value(), logger);
	logger->log_debug("[PublisherApp] Created " + technology.value()
	                  + " publisher");
}

void PublisherApp::publish_on_topic(std::string topic, Payload message) {
	const Payload &base = pick_random_payload();
	logger->log_info("[PublisherApp] Publishing," + message.message_id + ","
	                 + std::to_string(base.data_size) + "," + topic);
	publisher->send_message(message, topic); // technology-specific send
	logger->log_info("[PublisherApp] Published," + message.message_id + ","
	                 + std::to_string(base.data_size) + "," + topic);
}

void PublisherApp::publish_on_all_topics(Payload message) {
	try {
		std::istringstream ss(topics);
		std::string topic;
		while (std::getline(ss, topic, ',')) {
			publish_on_topic(topic, message);
		}
	} catch (const std::exception &e) {
		logger->log_error("[Publisher App] Exception during publish: "
		                  + std::string(e.what()));
	}
}

void PublisherApp::terminate_topic(std::string topic) {
	logger->log_info("[PublisherApp] Closing," + topic);
	publisher->send_message(generate_termination_message(), topic);
	logger->log_info("[PublisherApp] Closed," + topic);
}

void PublisherApp::terminate_all_topics() {
	std::istringstream ss(topics);
	std::string topic;
	while (std::getline(ss, topic, ',')) {
		terminate_topic(topic);
	}
}

void PublisherApp::run() {
	logger->log_info("[PublisherApp] Initializing");
	publisher->initialize();
	int sleep_time = 4000; // milliseconds

	std::optional<std::string> technology = utils::get_env_var("TECHNOLOGY");
	if (!technology) {
		std::string err_msg =
		    "[PublisherApp] Missing required environment variable TECHNOLOGY.";
		logger->log_error(err_msg);
		throw std::runtime_error(err_msg);
	}

	if (technology.value().find("p2p") != std::string::npos) {
		// Give some time for P2P connections to establish
		logger->log_info("[PublisherApp] Wait " + std::to_string(sleep_time)
		                 + "ms for initialization");
		std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
	}
	logger->log_info("[PublisherApp] Initialized");

	if (message_count > 0) {
		logger->log_info("[PublisherApp] Goal: " + std::to_string(message_count)
		                 + " messages,");
		run_messages();
	} else if (duration > 0) {
		logger->log_info("[PublisherApp] Goal: " + std::to_string(duration)
		                 + " seconds," + std::to_string(update_every) + " us");
		run_duration();
	} else {
		logger->log_error("[PublisherApp] Neither MESSAGES nor DURATION are "
		                  "positive integer values. No messages are sent.");
		return;
	}

	logger->log_info("[PublisherApp] Terminating");
	terminate_all_topics();
	// Give some time for messages to be sent before exiting
	std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
	logger->log_info("[PublisherApp] Terminated");
}

void PublisherApp::run_messages() {
	int i = 1;
	Payload message;
	while (i <= message_count) {
		message = generate_message(i);
		logger->log_info("[PublisherApp] Sending message " + std::to_string(i));
		publish_on_all_topics(message);
		logger->log_info("[PublisherApp] Sent message " + std::to_string(i));
		i++;
	}
}

void PublisherApp::run_duration() {
	using namespace std::chrono;
	auto start_time = steady_clock::now();
	auto end_time = start_time + seconds(duration);

	int i = 0;
	Payload message;
	while (steady_clock::now() < end_time) {
		message = generate_message(i);
		logger->log_info("[PublisherApp] Sending message "
		                 + std::to_string(i + 1));
		publish_on_all_topics(message);
		logger->log_info("[PublisherApp] Sent message "
		                 + std::to_string(i + 1));
		logger->log_debug("[PublisherApp] Now sleeping for "
		                  + std::to_string(update_every) + "us");
		std::this_thread::sleep_for(microseconds(update_every));
		++i;
	}
}

int main(int argc, char *argv[]) {
	// Disable synchronization between C and C++ standard streams
	std::ios::sync_with_stdio(false);
	std::cout << "[PublisherApp] Start" << std::endl << std::flush;
	try {
		Logger::LogLevel log_level = Logger::LogLevel::INFO;
		if (argc >= 2 && argv[1] != nullptr) {
			log_level = Logger::string_to_level(argv[1]);
		}
		PublisherApp app = PublisherApp(log_level);
		app.create_publisher();
		app.run();
	} catch (const std::exception &e) {
		std::cerr << "[PublisherApp] Exception caught: " << e.what()
		          << std::endl;
	} catch (...) {
		std::cerr << "[PublisherApp] Unknown exception caught!" << std::endl;
	}
	return 0;
}