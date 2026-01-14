#pragma once

#include <memory>
#include <string>

#include "IPublisher.hpp"
#include "librdkafka/rdkafkacpp.h"

class KafkaCppPublisher : public IPublisher {
  public:
	KafkaCppPublisher(std::shared_ptr<Logger> logger);
	~KafkaCppPublisher() override;

	void initialize() override;
	void send_message(const Payload &message, std::string &topic) override;

  private:
	void log_configuration() override;

	std::string broker_;

	std::unique_ptr<RdKafka::Producer> producer_;
	std::unique_ptr<RdKafka::Conf> conf_;

	std::unique_ptr<RdKafka::EventCb> event_cb_;
	std::unique_ptr<RdKafka::DeliveryReportCb> dr_cb_;
};