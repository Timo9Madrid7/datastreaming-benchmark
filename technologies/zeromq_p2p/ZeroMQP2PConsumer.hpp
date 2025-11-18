#ifndef ZEROMQP2P_CONSUMER_HPP
#define ZEROMQP2P_CONSUMER_HPP

#include "IConsumer.hpp"
#include <zmq.hpp>
#include <string>
#include <sstream>
#include <set>
#include <iostream>
#include "Logger.hpp"

class ZeroMQP2PConsumer : public IConsumer {
private:
    zmq::context_t context;
    zmq::socket_t subscriber;
    std::set<std::string> unique_publishers;

public:
    ZeroMQP2PConsumer(std::shared_ptr<Logger> logger);
    ~ZeroMQP2PConsumer();

    void initialize() override;
    void subscribe(const std::string &topic) override;
    Payload receive_message() override;
    Payload deserialize(const std::string& raw_message) override;

    void log_configuration() override;
};

#endif // ZEROMQP2P_CONSUMER_HPP
