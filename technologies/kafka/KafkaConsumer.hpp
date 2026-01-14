#pragma once

#include <librdkafka/rdkafka.h>
#include <string>
#include <unordered_set>

#include "IConsumer.hpp"

class KafkaConsumer : public IConsumer {
  public:
	KafkaConsumer(std::shared_ptr<Logger> logger);
	~KafkaConsumer() override;

	void initialize() override;
	void subscribe(const std::string &topic) override;
	void start_loop() override;
	void log_configuration() override;

  private:
	std::string broker_;
	std::unordered_set<std::string> topic_names_;

	rd_kafka_t *consumer_;
	rd_kafka_conf_t *conf_;
	rd_kafka_topic_partition_list_t *subscription_list_;
	bool initialized_;
};
