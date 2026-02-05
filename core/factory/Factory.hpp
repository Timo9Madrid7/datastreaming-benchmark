#pragma once

#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>

#include "IConsumer.hpp"
#include "IPublisher.hpp"
#include "Logger.hpp"

template <typename IClient> class Factory {
	static_assert(
	    std::is_same_v<IClient,
	                   IConsumer> || std::is_same_v<IClient, IPublisher>,
	    "Factory can only be instantiated with IConsumer or IPublisher types.");

  public:
	// Function pointer type for creating IClient instances
	using CreateFunc = std::unique_ptr<IClient> (*)(std::shared_ptr<Logger>);

	/**
	@brief Registers a client type with the factory.
	@param name The name of the client type.
	@param func The function pointer to create instances of the client type.
	*/
	static void registerClient(const std::string &name, CreateFunc func) {
		getRegistry()[name] = func;
	}

	/**
	@brief Creates an instance of the specified client type.
	@param name The name of the client type to create.
	@param logger A shared pointer to a Logger instance.
	@return A unique pointer to the created IClient instance.
	@throws std::runtime_error if the client type is not registered.
	*/
	static std::unique_ptr<IClient> create(const std::string &name,
	                                       std::shared_ptr<Logger> logger) {
		auto it = getRegistry().find(name);
		if (it != getRegistry().end()) {
			return it->second(logger);
		}
		throw std::runtime_error("Client type not registered");
	}

	/**
	@brief Prints the registered client types for debugging purposes.
	@param logger A shared pointer to a Logger instance.
	*/
	static void debug_print_registry(std::shared_ptr<Logger> logger) {
		std::string factoryType = std::is_same_v<IClient, IConsumer>
		    ? "ConsumerFactory"
		    : "PublisherFactory";
		logger->log_debug("[" + factoryType + "] Registered client types:");
		for (const auto &entry : getRegistry()) {
			logger->log_debug(" - " + entry.first);
		}
	}

  private:
	/**
	@brief Retrieves the registry of client types.
	@return A reference to the unordered map of registered client types.
	*/
	static std::unordered_map<std::string, CreateFunc> &getRegistry() {
		static std::unordered_map<std::string, CreateFunc> registry;
		return registry;
	}
};

extern template class Factory<IConsumer>;
extern template class Factory<IPublisher>;