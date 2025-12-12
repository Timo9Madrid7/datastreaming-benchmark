#pragma once

#include <string>
#include <optional>

namespace utils {
    /**
     * @brief Retrieves the value of an environment variable.
     * @param key The name of the environment variable.
     * @return The value of the environment variable if it exists, otherwise an empty optional.
     */
    const std::optional<std::string> get_env_var(const std::string& key);

    /**
     * @brief Retrieves the value of an environment variable or returns a default value if not set.
     * @param key The name of the environment variable.
     * @param default_value The default value to return if the environment variable is not set.
     * @return The value of the environment variable or the default value.
     */
    const std::string get_env_var_or_default(const std::string& key, const std::string& default_value);
}