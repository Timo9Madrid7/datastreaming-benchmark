#include "Utils.hpp"

#include <cstdlib>
#include <random>

namespace utils {

const std::optional<std::string> get_env_var(const std::string &key) {
	const char *value = std::getenv(key.c_str());
	if (value) {
		return std::string(value);
	} else {
		return std::nullopt;
	}
}

const std::string get_env_var_or_default(const std::string &key,
                                         const std::string &default_value) {
	const char *value = std::getenv(key.c_str());
	if (value) {
		return std::string(value);
	} else {
		return default_value;
	}
}

std::mt19937 &Random::get_rng() {
	thread_local std::mt19937 rng(std::random_device{}());
	return rng;
}

void Random::seed_rng(unsigned int seed) {
	get_rng().seed(seed);
}

} // namespace utils