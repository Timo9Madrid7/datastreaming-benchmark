#include "PublisherApp.hpp"
#include "cstdlib"

template<typename T>
T from_string(const std::string& str, T default_value) {
    std::istringstream iss(str);
    T result;
    if (!(iss >> result)) {
        return default_value;
    }
    return result;
}

Payload PublisherApp::generate_message(int i){
    Payload message = pick_random_payload();
    return Payload::reuse_with_new_id(id, i, message);
}

// Generate termination message
Payload PublisherApp::generate_termination_message(){
    return Payload::make(id, 0, 0, PayloadKind::TERMINATION);
}

// Batch generation of payloads across size range
void PublisherApp::generate_payloads(size_t target_size, size_t num_samples) {
    for (size_t i = 0; i < num_samples; ++i) {
        payloads.push_back(Payload::make(id, i, target_size, payload_kind));
    }
}

// Pick a random payload from the pool
const Payload& PublisherApp::pick_random_payload() {
    static std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<> dist(0, payloads.size() - 1);
    return payloads[dist(rng)];
}

PublisherApp::PublisherApp(Logger::LogLevel log_level){
    logger = std::make_shared<Logger>(log_level);
    load_from_env();
    generate_payloads(payload_size, payload_samples);
}

void PublisherApp::load_from_env() {
    const char* env_id = std::getenv("CONTAINER_ID");
    const char* env_topics = std::getenv("TOPICS");
    const char* env_update = std::getenv("UPDATE_EVERY");
    const char* env_psize = std::getenv("PAYLOAD_SIZE");
    const char* env_psamp = std::getenv("PAYLOAD_SAMPLES");
    const char* env_pkind = std::getenv("PAYLOAD_KIND");


    if (!env_id || !env_topics || !env_update || !env_psize || !env_psamp) {
        std::string err_msg;
        if (std::string(env_id).empty()){
            err_msg = "[PublisherApp] Missing required environment variable CONTAINER_ID";
            logger->log_error(err_msg);
            throw std::runtime_error(err_msg);
        }
        else if(std::string(env_topics).empty()){
            err_msg = "[PublisherApp] Missing required environment variable TOPICS";
            logger->log_error(err_msg);
            throw std::runtime_error(err_msg);
        }
        else if(std::string(env_update).empty()){
            err_msg = "[PublisherApp] Missing required environment variable UPDATE_EVERY";
            logger->log_error(err_msg);
            throw std::runtime_error(err_msg);
        }
        else if(std::string(env_psize).empty()){
            err_msg = "[PublisherApp] Missing required environment variable PAYLOAD_SIZE";
            logger->log_error(err_msg);
            throw std::runtime_error(err_msg);
        }
        else if(std::string(env_psamp).empty()){
            err_msg = "[PublisherApp] Missing required environment variable PAYLOAD_SAMPLES";
            logger->log_error(err_msg);
            throw std::runtime_error(err_msg);
        }
        else if(std::string(env_pkind).empty()){
            err_msg = "[PublisherApp] Missing required environment variable PAYLOAD_KIND";
            logger->log_error(err_msg);
            throw std::runtime_error(err_msg);
        }
        else{
            err_msg = "[PublisherApp] Unknown error related to environment variables";
            logger->log_error(err_msg);
            throw std::runtime_error(err_msg);
        }
    }

    id = std::string(env_id);
    topics = std::string(env_topics);
    message_count = std::getenv("MESSAGES")? from_string<int>(std::getenv("MESSAGES"), 0) : 0;
    duration = std::getenv("DURATION")? from_string<int>(std::getenv("DURATION"), 0) : 0;
    update_every = from_string(env_update,5000000);
    payload_size = std::atoi(std::getenv("PAYLOAD_SIZE")); 
    payload_samples = std::atoi(std::getenv("PAYLOAD_SAMPLES"));
    payload_kind = Payload::string_to_payloadkind(std::getenv("PAYLOAD_KIND"));

    logger->log_debug("[PublisherApp] Loaded from environment: ID=" + id 
        + ", TOPICS=" + topics 
        + ", MESSAGES=" + std::to_string(message_count)
        + ", DURATION=" + std::to_string(duration)
        + ", UPDATE_EVERY=" + std::to_string(update_every) + " us"
        + ", PAYLOAD_SIZE=" + std::to_string(payload_size)
        + ", PAYLOAD_SAMPLES=" + std::to_string(payload_samples)
        + ", PAYLOAD_KIND=" + Payload::payloadkind_to_string(payload_kind)
    );
}


// Factory Method to Create Publisher
void PublisherApp::create_publisher() {
    std::string technology = std::getenv("TECHNOLOGY");
    logger->log_debug("[PublisherApp] Creating publisher for technology " + technology + ", log_level: " + Logger::level_to_string(logger->get_level()));
    std::string tech_lib;
#ifdef _WIN32
    tech_lib = technology + "_technology.dll";  // or with full path
#else
    tech_lib = "/app/lib/lib"+ technology + "_technology.so";
#endif

    TechnologyLoader::load_technology(tech_lib, logger);
    logger->log_debug("[PublisherApp] Factory state before calling 'create'");
    PublisherFactory::debug_print_registry(logger);
    
    publisher = PublisherFactory::create(technology, logger);
    logger->log_debug("[PublisherApp] Created " + technology + " publisher");
}

void PublisherApp::publish_on_topic(std::string topic, Payload message){
    const Payload& base = pick_random_payload();
    logger->log_info("[PublisherApp] Publishing," + message.message_id + 
                      "," + std::to_string(base.data_size) + 
                      "," + topic);
    publisher->send_message(message, topic); // technology-specific send
    logger->log_info("[PublisherApp] Published," + message.message_id + 
                      "," + std::to_string(base.data_size) + 
                      "," + topic);
}

void PublisherApp::publish_on_all_topics(Payload message){
    try {
        std::istringstream ss(topics);
        std::string topic;
        while (std::getline(ss, topic, ',')) {
            publish_on_topic(topic, message);
        }
    } catch (const std::exception& e){
        logger->log_error("[Publisher App] Exception during publish: " + std::string(e.what()));
    }
}

void PublisherApp::terminate_topic(std::string topic){
    logger->log_info("[PublisherApp] Closing," + topic);
    publisher->send_message(generate_termination_message(), topic);
    logger->log_info("[PublisherApp] Closed," + topic);
}

void PublisherApp::terminate_all_topics(){
    std::istringstream ss(topics);
    std::string topic;
    while (std::getline(ss, topic, ',')) {
        terminate_topic(topic);
    }
}

// Runs the publisher logic (can now be fully generalized)
void PublisherApp::run() {
    logger->log_info("[PublisherApp] Initializing");
    publisher->initialize(); // Technology-specific initialization
    int sleep_time = 4000; // milliseconds

    // Determine if technology follows p2p or brokered pattern to decide how to synchronize start-up of publisher vs consumer
    std::string technology = std::getenv("TECHNOLOGY");
    if (technology.find("p2p") != std::string::npos) {
        // wait for consumer to connect before starting to send messages
        logger->log_info("[PublisherApp] Initialized," + std::to_string(sleep_time));
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
    } else {
        // brokered technologies do not need this
        logger->log_info("[PublisherApp] Initialized,0");
    }

    // A run's goal is either to send a number of messages or to run for a set duration
    if (message_count > 0) {
        logger->log_info("[PublisherApp] Goal: " + std::to_string(message_count) 
            + " messages," + std::to_string(update_every) + " us"
        );
        run_messages();
    }
    else if (duration > 0) {
        logger->log_info("[PublisherApp] Goal: " + std::to_string(duration) 
            + " seconds," + std::to_string(update_every) + " us"
        );
        run_duration();
    }
    else{
        logger->log_error("[PublisherApp] Neither MESSAGES nor DURATION are positive integer values. No messages are sent.");
        return;
    }
    // Send termination signal (poison pill)
    logger->log_info("[PublisherApp] Terminating");	
    terminate_all_topics();
    logger->log_info("[PublisherApp] Terminated");
}

void PublisherApp::run_messages(){
    int i = 1;
    Payload message;
    while (i <= message_count) {
        message = generate_message(i);
        logger->log_info("[PublisherApp] Sending message " + std::to_string(i));
        // std::string message = "Message " + std::to_string(i + 1) + " [END] to topics: " + topics;
        publish_on_all_topics(message);
        logger->log_info("[PublisherApp] Sent message " + std::to_string(i));
        i++;
        // If you want to simulate a data generating system, uncomment this section
        // if (i < message_count){
        //     console.log_debug("[PublisherApp] Now sleeping for " + std::to_string(update_every) + "us");
        //     std::this_thread::sleep_for(std::chrono::microseconds(update_every));
        // }
    }
}

void PublisherApp::run_duration(){
    using namespace std::chrono;
    auto start_time = steady_clock::now();
    auto end_time = start_time + seconds(duration);

    int i = 0;
    Payload message;
    while (steady_clock::now() < end_time) {
        message = generate_message(i);
        logger->log_info("[PublisherApp] Sending message " + std::to_string(i + 1));
        // std::string message = "Message " + std::to_string(i + 1) + " [END] to topics: " + topics;
        publish_on_all_topics(message);
        logger->log_info("[PublisherApp] Sent message " + std::to_string(i + 1));
        logger->log_debug("[PublisherApp] Now sleeping for " + std::to_string(update_every) + "us");
        std::this_thread::sleep_for(microseconds(update_every));

        ++i;
    }
}

int main(int argc, char * argv[]) {
    std::ios::sync_with_stdio(false); // Disable stream buffering
    std::cout << "[PublisherApp] Start" << std::endl << std::flush;
    try {
        Logger::LogLevel log_level = Logger::LogLevel::INFO;
        if (argc >= 2 && argv[1] != nullptr){
            log_level = Logger::string_to_level(argv[1]);
        }
        // std::cout << "[PublisherApp] Log level: " << Logger::level_to_string(log_level) << std::endl << std::flush;
        PublisherApp app = PublisherApp(log_level);
        app.create_publisher();
        app.run();
    } catch (const std::exception &e) {
        std::cerr << "[PublisherApp] Exception caught: " << e.what() << std::endl;
    } catch (...) {
        std::cerr << "[PublisherApp] Unknown exception caught!" << std::endl;
    }
    return 0;
}