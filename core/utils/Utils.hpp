#pragma once

#include <optional>
#include <random>
#include <string>

namespace utils {
/**
 * @brief Retrieves the value of an environment variable.
 * @param key The name of the environment variable.
 * @return The value of the environment variable if it exists, otherwise an
 * empty optional.
 */
const std::optional<std::string> get_env_var(const std::string &key);

/**
 * @brief Retrieves the value of an environment variable or returns a default
 * value if not set.
 * @param key The name of the environment variable.
 * @param default_value The default value to return if the environment variable
 * is not set.
 * @return The value of the environment variable or the default value.
 */
const std::string get_env_var_or_default(const std::string &key,
                                         const std::string &default_value);

class Random {
  public:
	/**
	 * @brief Retrieves a reference to the static random number generator.
	 * @return A reference to the static std::mt19937 instance.
	 */
	static std::mt19937 &get_rng();

	/**
	 * @brief Generates a random integer between min and max (inclusive).
	 * @param min The minimum value.
	 * @param max The maximum value.
	 * @return A random integer between min and max.
	 */
	template <typename IntType = int>
	static IntType random_int(int min, int max) {
		static_assert(std::is_integral_v<IntType>,
		              "IntType must be an integral type");
		std::uniform_int_distribution<IntType> dist(min, max);
		return dist(get_rng());
	}

	/**
	 * @brief Seeds the random number generator with a specific seed.
	 * @param seed The seed value.
	 */
	static void seed_rng(unsigned int seed);
};
} // namespace utils