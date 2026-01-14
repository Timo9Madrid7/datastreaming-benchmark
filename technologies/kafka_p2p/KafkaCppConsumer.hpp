#pragma once

#include <librdkafka/rdkafkacpp.h>
#include <memory>
#include <vector>

#include "IConsumer.hpp"
#include "readerwriterqueue.h"

class KafkaCppConsumer : public IConsumer {
  public:
	KafkaCppConsumer(std::shared_ptr<Logger> logger);
	~KafkaCppConsumer() override;

	void initialize() override;
	void subscribe(const std::string &topic) override;
	void start_loop() override;
	void log_configuration() override;

  private:
	std::string broker_;
	std::vector<std::string> topic_names_;

	std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
	std::unique_ptr<RdKafka::Conf> conf_;

	std::thread deserialize_thread_;
	std::atomic<bool> stop_deserialization_{false};
	std::atomic<bool> stop_receiving_{false};
	moodycamel::ReaderWriterQueue<std::unique_ptr<RdKafka::Message>>
	    deserialize_queue_{1024};

	void start_deserialize_thread_();
	void stop_deserialize_thread_();
};