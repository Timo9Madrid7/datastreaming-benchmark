#pragma once

#include <memory>
#include "librdkafka/rdkafkacpp.h"
#include "Logger.hpp"

/**
* @class KafkaEventCb
* @brief Callback class for handling Kafka events.
*/
class KafkaEventCb : public RdKafka::EventCb {
    public:
        explicit KafkaEventCb(std::shared_ptr<Logger> logger, std::string name);
        void event_cb(RdKafka::Event& event) override;

    private:
        std::shared_ptr<Logger> logger_;
        std::string name_;
};


/**
 * @class KafkaDeliveryReportCb
 * @brief Callback class for handling Kafka delivery reports.
 */
class KafkaDeliveryReportCb : public RdKafka::DeliveryReportCb {
    public:
        explicit KafkaDeliveryReportCb(std::shared_ptr<Logger> logger);
        void dr_cb(RdKafka::Message& message) override;

    private:
        std::shared_ptr<Logger> logger_;
};