#pragma once

#include <memory>
#include <string>

#include "Logger.hpp"

class TechnologyLoader {
  public:
	/**
	@brief Dynamically loads a technology library and registers it.
	@param lib_name The name of the library to load.
	@param logger A shared pointer to a Logger instance.
	@return true if the library was loaded and registered successfully, false
	otherwise.
	*/
	static bool load_technology(const std::string &lib_name,
	                            std::shared_ptr<Logger> logger);
};
