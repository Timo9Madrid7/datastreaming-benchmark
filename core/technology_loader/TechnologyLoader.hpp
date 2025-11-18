#ifndef TECHNOLOGY_LOADER_HPP
#define TECHNOLOGY_LOADER_HPP

#include <string>
#include <memory>
#include "Logger.hpp"

class TechnologyLoader {
public:
    static bool load_technology(const std::string& lib_name, std::shared_ptr<Logger> logger);
};

#endif // TECHNOLOGY_LOADER_HPP
