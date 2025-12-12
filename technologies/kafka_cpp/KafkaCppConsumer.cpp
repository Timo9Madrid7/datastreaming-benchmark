#include "KafkaCppConsumer.hpp"
#include "Env.hpp"
#include "Logger.hpp"
#include "Payload.h"
#include <librdkafka/rdkafkacpp.h>
#include <memory>
#include <optional>
#include <sstream>

KafkaCppConsumer::KafkaCppConsumer(std::shared_ptr<Logger> logger)
    : IConsumer(logger),
      consumer_(nullptr),
      conf_(nullptr) {
    logger->log_info("[Kafka Consumer] KafkaConsumer created.");
}

KafkaCppConsumer::~KafkaCppConsumer() {
    logger->log_debug("[Kafka Consumer] Destructor finished");
}

void KafkaCppConsumer::initialize() {
    logger->log_study("Initializing");
    
    const std::string vendpoint = utils::get_env_var_or_default("CONSUMER_ENDPOINT", "localhost");
    const std::string port = utils::get_env_var_or_default("CONSUMER_PORT", "9092");
    broker_ = vendpoint + ":" + port;
    logger->log_info("[Kafka Consumer] Using broker: " + broker_);
    
    const std::optional<std::string> consumer_id = utils::get_env_var("CONTAINER_ID");
    const std::optional<std::string> vtopics = utils::get_env_var("TOPICS");
    if (!consumer_id || !vtopics) {
        logger->log_error("[Kafka Consumer] CONTAINER_ID or TOPICS environment variable not set.");
        throw std::runtime_error("[Kafka Consumer] CONTAINER_ID or TOPICS environment variable not set.");
    }
    
    std::string errstr;
    conf_.reset(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

    if (conf_->set("bootstrap.servers", broker_, errstr) != RdKafka::Conf::CONF_OK) {
        logger->log_error("[Kafka Consumer] Failed to set bootstrap.servers: " + errstr);
        throw std::runtime_error("Failed to set bootstrap.servers: " + errstr);
    }

    if (conf_->set("group.id", "benchmark_group", errstr) != RdKafka::Conf::CONF_OK) {
        logger->log_error("[Kafka Consumer] Failed to set group.id: " + errstr);
        throw std::runtime_error("Failed to set group.id: " + errstr);
    }

    if (conf_->set("enable.auto.commit", "false", errstr) != RdKafka::Conf::CONF_OK) {
        logger->log_error("[Kafka Consumer] Failed to set enable.auto.commit: " + errstr);
        throw std::runtime_error("Failed to set enable.auto.commit: " + errstr);
    }

    if (conf_->set("auto.offset.reset", "earliest", errstr) != RdKafka::Conf::CONF_OK) {
        logger->log_error("[Kafka Consumer] Failed to set auto.offset.reset: " + errstr);
        throw std::runtime_error("Failed to set auto.offset.reset: " + errstr);
    }

    consumer_.reset(RdKafka::KafkaConsumer::create(conf_.get(), errstr));

    if (!consumer_) {
        logger->log_error("[Kafka Consumer] Failed to create consumer: " + errstr);
        throw std::runtime_error("Failed to create consumer: " + errstr);
    }

    std::istringstream topics(vtopics.value_or(""));
    std::string topic;
    while(std::getline(topics, topic, ',')){
        logger->log_debug("[Kafka Consumer] Handling subscription to topic " + topic);
        if(!topic.empty()){
            logger->log_info("[Kafka Consumer] Connecting to stream ("+ broker_ + "," + topic + ")");
            subscribe(topic);
        }
    }

    logger->log_debug("[Kafka Consumer] Subscription list will have size " + std::to_string(topic_names_.size()));
    RdKafka::ErrorCode err = consumer_->subscribe(topic_names_);
    if (err != RdKafka::ERR_NO_ERROR) {
        logger->log_error("[Kafka Consumer] Failed to subscribe to Kafka topics: " + RdKafka::err2str(err));
        throw std::runtime_error("Failed to subscribe to Kafka topics: " + RdKafka::err2str(err));
    }

    logger->log_info("[Kafka Consumer] Consumer initialized and subscribed.");
    logger->log_study("Initialized");
    log_configuration();
}

void KafkaCppConsumer::subscribe(const std::string& topic) {
    if (subscribed_streams.emplace(topic, "default").second) {
        logger->log_info("[Kafka Consumer] Queued subscription for topic: " + topic);
        topic_names_.push_back(topic);
    }
}

Payload KafkaCppConsumer::deserialize(const std::string& raw) {
    return deserialize(raw.data(), raw.size());
}

Payload KafkaCppConsumer::deserialize(const void* data_ptr, size_t len) {
    const char* data = static_cast<const char*>(data_ptr);
    size_t offset = 0;

    // Message ID Length
    uint16_t id_len;
    std::memcpy(&id_len, data + offset, sizeof(id_len));
    offset += sizeof(id_len);

    // Message ID
    std::string message_id(data + offset, id_len);
    offset += id_len;

    // Kind
    uint8_t kind_byte;
    std::memcpy(&kind_byte, data + offset, sizeof(kind_byte));
    offset += sizeof(kind_byte);
    PayloadKind kind_payload = static_cast<PayloadKind>(kind_byte);

    // Data size
    size_t data_size;
    std::memcpy(&data_size, data + offset, sizeof(data_size));
    offset += sizeof(data_size);

    // Data
    std::vector<uint8_t> payload_data(data + offset, data + offset + data_size);
   
    Payload p;
    p.message_id = message_id;
    p.kind = kind_payload;
    p.data_size = data_size;
    p.data = std::move(payload_data);
   
    return p;
}

Payload KafkaCppConsumer::receive_message() {
    logger->log_debug("[Kafka Consumer] Polling for messages...");
    std::unique_ptr<RdKafka::Message> msg(consumer_->consume(2000));
    
    if (!msg) {
        logger->log_debug("[Kafka Consumer] Poll returned null message");
        return {};
    }
    
    std::string topic = !msg->topic_name().empty() ? msg->topic_name() : "unknown";

    Payload payload = {};
    if (msg->err()) {
        logger->log_error("[Kafka Consumer] Kafka error: " + RdKafka::err2str(msg->err()));
    } 
    else if (msg->len() > 0) {
        logger->log_info("[Kafka Consumer] Received message on topic '" + topic + "' with " + std::to_string(msg->len()) + " bytes");
        try {
            payload = deserialize(msg->payload(), msg->len());
        } catch (const std::exception& e) {
            logger->log_error("[Kafka Consumer] Failed to deserialize payload: " + std::string(e.what()));
        }

        if (payload.message_id.find(TERMINATION_SIGNAL)  != std::string::npos) {
            terminated_streams.insert({broker_, topic});
            logger->log_info("[Kafka Consumer] Received termination for topic: " + topic);
            logger->log_debug("[Kafka Consumer] Streams closed: " + std::to_string(terminated_streams.size()) + "/" + std::to_string(subscribed_streams.size()));
            payload = Payload::make(payload.message_id.substr(0, payload.message_id.find(':')) + "-" + topic, 
                                    0, 0, PayloadKind::TERMINATION);
        }
        logger->log_study("Reception," + payload.message_id + 
            "," + std::to_string(payload.data_size) + 
            "," + topic +
            "," + std::to_string(msg->len()));
    }
    else{
        logger->log_error("[Kafka Consumer] Unknown msg handling condition");
    }

    return payload;
}

void KafkaCppConsumer::log_configuration() {
    std::unique_ptr<std::list<std::string>> dump(conf_->dump());
    
    logger->log_config("[Kafka Consumer] [CONFIG_BEGIN]");
    for (auto it = dump->begin(); it != dump->end();) {
        logger->log_config("[CONFIG] " + *it++ + "=" + *it++);
    }
    logger->log_config("[Kafka Consumer] [CONFIG_END]");
}