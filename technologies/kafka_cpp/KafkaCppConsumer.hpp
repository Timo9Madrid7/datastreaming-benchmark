#pragma once

#include <librdkafka/rdkafkacpp.h>
#include <memory>
#include <vector>

#include "IConsumer.hpp"
#include "Payload.hpp"

class KafkaCppConsumer : public IConsumer {
  public:
	KafkaCppConsumer(std::shared_ptr<Logger> logger);
	~KafkaCppConsumer() override;

	void initialize() override;
	void subscribe(const std::string &topic) override;
	Payload receive_message() override;
	bool deserialize(const void *raw_message, size_t len,
	                 Payload &out) override;
	void log_configuration() override;

  private:
	std::string broker_;
	std::vector<std::string> topic_names_;

	std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
	std::unique_ptr<RdKafka::Conf> conf_;
	std::unique_ptr<RdKafka::TopicPartition> subscription_list_;
};