#pragma once

#include <librdkafka/rdkafka.h>
#include <string>
#include <unordered_map>

#include "IPublisher.hpp"
#include "Payload.hpp"

class KafkaPublisher : public IPublisher {
  public:
	KafkaPublisher(std::shared_ptr<Logger> logger);
	~KafkaPublisher() override;

	void initialize() override;
	void send_message(const Payload &message, std::string &topic) override;

  private:
	void log_configuration() override;

	rd_kafka_t *producer_;
	rd_kafka_conf_t *conf_;
	std::string broker_;

	std::unordered_map<std::string, rd_kafka_topic_t *> topic_handles_;

	rd_kafka_topic_t *get_or_create_topic_handle(const std::string &topic);
	void destroy_topic_handle(const std::string &topic);
};
