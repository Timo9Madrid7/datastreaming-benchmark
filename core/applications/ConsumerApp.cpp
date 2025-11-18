#include "ConsumerApp.hpp"
#include "cstdlib"


ConsumerApp::ConsumerApp(Logger::LogLevel log_level){
    logger = std::make_shared<Logger>(log_level);
}

void ConsumerApp::create_consumer() {
    std::string technology = std::getenv("TECHNOLOGY");
    logger->log_debug("[ConsumerApp] Creating consumer for technology " + technology + ", log_level: " + Logger::level_to_string(logger->get_level()));
    std::string tech_lib;
#ifdef _WIN32
    tech_lib = technology + "_technology.dll";  // or with full path
#else
    tech_lib = "/app/lib/lib"+ technology + "_technology.so";
#endif

    TechnologyLoader::load_technology(tech_lib, logger);

    consumer = ConsumerFactory::create(technology, logger);
    logger->log_debug("[ConsumerApp] Created " + technology + " consumer");
}

// Initializes and runs the consumer logic
void ConsumerApp::run() {
    logger->log_info("[ConsumerApp] Initializing");
    consumer->initialize();
    int sleep_time = 4000; // milliseconds

    // Determine if technology follows p2p or brokered pattern to decide how to synchronize start-up of publisher vs consumer
    std::string technology = std::getenv("TECHNOLOGY");
    if (technology.find("p2p") == std::string::npos) {
        // wait for publisher to send the first message and the broker to create the topic
        logger->log_info("[ConsumerApp] Initialized," + std::to_string(sleep_time));
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
    } else {
        // p2p technologies do not need this, as they are not brokered
        logger->log_info("[ConsumerApp] Initialized,0"); 
    }
    while (true) {
        logger->log_debug("[ConsumerApp] Waiting for message...");
        Payload message = consumer->receive_message(); // technology-specific receive
        if (message.message_id == ""){
            logger->log_info("[ConsumerApp] Received message with no label -> Retrying.");
            continue; //todo: add maximum retries?
        }
        if (message.message_id.find(TERMINATION_SIGNAL) != std::string::npos) {
            logger->log_info("[ConsumerApp] Termination," + std::to_string(consumer->get_terminated_streams_size()) + "/" + std::to_string(consumer->get_subscribed_streams_size()));
            if (consumer->get_terminated_streams_size() >= consumer->get_subscribed_streams_size()) {
                break; // All streams terminated
            }
            continue;
        }
        logger->log_info("[ConsumerApp] Update," + message.message_id + "," + std::to_string(message.data_size));
    }
}


int main(int argc, char * argv[]) {
    std::ios::sync_with_stdio(false); // Disable stream buffering
    std::cout << "[ConsumerApp] Start" << std::endl << std::flush;
    try {
        Logger::LogLevel log_level = Logger::LogLevel::INFO;
        if (argc >= 2 && argv[1] != nullptr){
            log_level = Logger::string_to_level(argv[1]);
        }
        // std::cout << "[ConsumerApp] Log level: " << Logger::level_to_string(log_level) << std::endl << std::flush;
        ConsumerApp app = ConsumerApp(log_level);
        app.create_consumer();
        app.run();
    } catch (const std::exception &e) {
        std::cerr << "[ConsumerApp] Exception caught: " << e.what() << std::endl;
    } catch (...) {
        std::cerr << "[ConsumerApp] Unknown exception caught!" << std::endl;
    }
    return 0;
}