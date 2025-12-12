#include "KafkaCallbacks.hpp"

KafkaEventCb::KafkaEventCb(std::shared_ptr<Logger> logger, std::string name)
    : logger_(std::move(logger)), name_(std::move(name)) {
    logger_->log_info("KafkaEventCb initialized.");
}

void KafkaEventCb::event_cb(RdKafka::Event& event) {
    switch (event.type()) {
        case RdKafka::Event::EVENT_ERROR:
            logger_->log_error("[" + name_ + "] " + RdKafka::err2str(event.err()) + " - " + event.str());
            break;
        case RdKafka::Event::EVENT_STATS:
            logger_->log_info("[" + name_ + "] " + event.str());
            break;
        case RdKafka::Event::EVENT_LOG:
            logger_->log_debug("[" + name_ + "] " + std::to_string(event.severity()) + "-" + event.fac() + ": " + event.str());
            break;
        default:
            logger_->log_info("[" + name_ + "] " + std::to_string(event.type()) + " (" + RdKafka::err2str(event.err()) + "): " + event.str());
            break;
    }
}

KafkaDeliveryReportCb::KafkaDeliveryReportCb(std::shared_ptr<Logger> logger)
    : logger_(std::move(logger)) {
    logger_->log_info("KafkaDeliveryReportCb initialized.");
}

void KafkaDeliveryReportCb::dr_cb(RdKafka::Message& message) {
    const std::string* key_ptr = message.key();
    std::string key_str = (key_ptr != nullptr) ? *key_ptr : "null";

    if (message.err()) {
        logger_->log_study("DeliveryError," + key_str + 
                            "," + std::to_string(message.len()) + 
                            ",-1," +
                            message.topic_name()
                        );
    } else {
        logger_->log_study("Publication," + key_str + 
                          "," + std::to_string(message.len()) +  
                          "," + message.topic_name() + 
                          "," + std::to_string(message.len())
                        );
    }
}