#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "IConsumer.hpp"
#include "Logger.hpp"

class ConsumerFactory {
  public:
	// Function pointer type for creating IConsumer instances
	using CreateFunc = std::unique_ptr<IConsumer> (*)(std::shared_ptr<Logger>);

	static void registerConsumer(const std::string &name, CreateFunc func);

	static std::unique_ptr<IConsumer> create(const std::string &name,
	                                         std::shared_ptr<Logger> logger);

	static void debug_print_registry(std::shared_ptr<Logger> logger);

  private:
	static std::unordered_map<std::string, CreateFunc> &getRegistry();
};
