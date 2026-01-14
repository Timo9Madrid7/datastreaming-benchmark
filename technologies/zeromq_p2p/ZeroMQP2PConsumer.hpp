#ifndef ZEROMQP2P_CONSUMER_HPP
#define ZEROMQP2P_CONSUMER_HPP

#include <memory>
#include <string>
#include <zmq.hpp>

#include "IConsumer.hpp"
#include "Logger.hpp"
#include "readerwriterqueue.h"

struct Payload;

class ZeroMQP2PConsumer : public IConsumer {
  private:
	zmq::context_t context;
	zmq::socket_t subscriber;

	std::thread deserialize_thread_;
	std::atomic<bool> stop_deserialization_{false};
	std::atomic<bool> stop_receiving_{false};
	moodycamel::ReaderWriterQueue<zmq::message_t> deserialize_queue_{1024};

	void start_deserialize_thread_();
	void stop_deserialize_thread_();

  public:
	ZeroMQP2PConsumer(std::shared_ptr<Logger> logger);
	~ZeroMQP2PConsumer();

	void initialize() override;
	void subscribe(const std::string &topic) override;
	void start_loop() override;
	bool deserialize(const void *raw_message, size_t len, std::string& topic, Payload &out);
	bool deserialize_id(const void *raw_message, size_t len, std::string& topic, Payload &out);
	void log_configuration() override;
};

#endif // ZEROMQP2P_CONSUMER_HPP
