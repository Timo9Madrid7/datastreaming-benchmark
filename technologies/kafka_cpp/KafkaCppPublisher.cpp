#include "KafkaCppPublisher.hpp"
#include "KafkaCallbacks.hpp"

#include "Utils.hpp"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <librdkafka/rdkafkacpp.h>
#include <list>
#include <memory>
#include <string>

KafkaCppPublisher::KafkaCppPublisher(std::shared_ptr<Logger> logger) try : IPublisher(logger), producer_(nullptr), conf_(nullptr) {
    logger->log_info("[Kafka Publisher] KafkaPublisher created.");
} catch (const std::exception& e) {
    logger->log_error("[Kafka Publisher] Constructor failed: " + std::string(e.what()));
}

KafkaCppPublisher::~KafkaCppPublisher() {
    logger->log_debug("[Kafka Publisher] Cleaning up Kafka producer...");
    if (producer_) {
        logger->log_debug("[Kafka Publisher] Polling before flush...");
        while (producer_->outq_len() > 0) {
            producer_->poll(100);
        }
        logger->log_debug("[Kafka Publisher] Flushing");
        producer_->flush(10 * 1000);
        logger->log_debug("[Kafka Publisher] Destroying");
    }
    logger->log_debug("[Kafka Publisher] Kafka destructor finished.");
}

void KafkaCppPublisher::initialize() {
    logger->log_study("Initializing");
    
    const std::string vendpoint = utils::get_env_var_or_default("PUBLISHER_ENDPOINT", "localhost");
    const std::string port = utils::get_env_var_or_default("PUBLISHER_PORT", "9092");
    broker_ = vendpoint + ":" + port;
    logger->log_info("[Kafka Publisher] Using broker: " + broker_);

    event_cb_ = std::make_unique<KafkaEventCb>(logger, "Kafka Publisher");
    dr_cb_ = std::make_unique<KafkaDeliveryReportCb>(logger);

    conf_.reset(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    
    std::string errstr;

    if (conf_->set("event_cb", event_cb_.get(), errstr) != RdKafka::Conf::CONF_OK) {
        logger->log_error("[Kafka Publisher] Failed to set event_cb: " + errstr);
        throw std::runtime_error("Failed to set event_cb: " + errstr);
    }

    if (conf_->set("dr_cb", dr_cb_.get(), errstr) != RdKafka::Conf::CONF_OK) {
        logger->log_error("[Kafka Publisher] Failed to set dr_cb: " + errstr);
        throw std::runtime_error("Failed to set dr_cb: " + errstr);
    }

    if (conf_->set("bootstrap.servers", broker_, errstr) != RdKafka::Conf::CONF_OK) {
        logger->log_error("[Kafka Publisher] Failed to set bootstrap.servers: " + errstr);
        throw std::runtime_error("Failed to set bootstrap.servers: " + errstr);
    }

    //TODO [optimization] Enable message batching
    conf_->set("linger.ms", "5", errstr);
    conf_->set("batch.num.messages", "65536", errstr);
    conf_->set("compression.type", "lz4", errstr);
    //TODO [Optimization] Increase buffer memory
    conf_->set("queue.buffering.max.kbytes", "1048576", errstr);
    conf_->set("queue.buffering.max.messages", "1000000", errstr);

    producer_.reset(RdKafka::Producer::create(conf_.get(), errstr));

    if (!producer_) {
        logger->log_error("[Kafka Publisher] Failed to create producer: " + errstr);
        throw std::runtime_error("Failed to create producer: " + errstr);
    }

    logger->log_study("Initialized");
    log_configuration();
}

std::string KafkaCppPublisher::serialize(const Payload& payload) {
    size_t total_size = sizeof(uint16_t) + payload.message_id.size() +
                        sizeof(uint8_t) +
                        sizeof(size_t) + payload.data.size();
    std::string buffer;
    buffer.resize(total_size);

    char* ptr = &buffer[0];

    // Message ID Length
    uint16_t id_len = static_cast<uint16_t>(payload.message_id.size());
    std::memcpy(ptr, &id_len, sizeof(id_len));
    ptr += sizeof(id_len);

    // Message ID
    std::memcpy(ptr, payload.message_id.data(), id_len);
    ptr += id_len;

    // Kind
    uint8_t kind = static_cast<uint8_t>(payload.kind);
    std::memcpy(ptr, &kind, sizeof(kind));
    ptr += sizeof(kind);

    // Data size
    size_t size = static_cast<size_t>(payload.data_size);
    std::memcpy(ptr, &size, sizeof(size));
    ptr += sizeof(size);

    // Data
    std::memcpy(ptr, payload.data.data(), size);

    return buffer;
}

void KafkaCppPublisher::send_message(const Payload& message, std::string topic) {
    logger->log_study("Intention," + message.message_id + "," + std::to_string(message.data_size) + "," + topic);
    std::string serialized = serialize(message);

    RdKafka::ErrorCode err = producer_->produce(
        topic,                                          
        RdKafka::Topic::PARTITION_UA,
        RdKafka::Producer::RK_MSG_COPY,
        const_cast<char*>(serialized.data()),
        serialized.size(),
        message.message_id.c_str(),
        message.message_id.size(),
        0,
        nullptr
    );

    producer_->poll(0);

    if (err != RdKafka::ERR_NO_ERROR) {
        logger->log_error("[Kafka Publisher] Produce failed: " + RdKafka::err2str(err));
    } else {
        logger->log_debug("[Kafka Publisher] Message queued for topic: " + topic);
    }

}

void KafkaCppPublisher::log_configuration() {
    std::unique_ptr<std::list<std::string>> dump(conf_->dump());
    
    logger->log_config("[Kafka Pubublihser] [CONFIG_BEGIN]");
    for (auto it = dump->begin(); it != dump->end();) {
        logger->log_config("[CONFIG] " + *it++ + "=" + *it++);
    }
    logger->log_config("[Kafka Publisher] [CONFIG_ENG]");
}