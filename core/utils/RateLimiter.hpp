#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <thread>

namespace utils {

class RateLimiter {
  public:
	// Default: 200 MiB/s
	static constexpr uint64_t kDefaultMaxBytesPerSec = 200ULL * 1024ULL * 1024ULL;

	explicit RateLimiter(uint64_t max_bytes_per_sec = kDefaultMaxBytesPerSec,
	                     uint64_t burst_bytes = kDefaultMaxBytesPerSec)
	    : max_bytes_per_sec_(max_bytes_per_sec),
	      burst_bytes_(std::max<uint64_t>(1, burst_bytes)),
	      tokens_(static_cast<double>(burst_bytes_)),
	      last_(Clock::now()) {
	}

	void set_rate(uint64_t max_bytes_per_sec) {
		std::scoped_lock lock(mu_);
		max_bytes_per_sec_ = std::max<uint64_t>(1, max_bytes_per_sec);
		// Keep burst aligned (1s worth) unless user configured differently.
		burst_bytes_ = std::max<uint64_t>(burst_bytes_, max_bytes_per_sec_);
		tokens_ = std::min(tokens_, static_cast<double>(burst_bytes_));
		last_ = Clock::now();
	}

	uint64_t rate() const {
		std::scoped_lock lock(mu_);
		return max_bytes_per_sec_;
	}

	// Blocks until `bytes` can be sent under the configured rate.
	void acquire(uint64_t bytes) {
		if (bytes == 0) {
			return;
		}

		for (;;) {
			std::chrono::nanoseconds sleep_for_ns{0};
			{
				std::unique_lock lock(mu_);
				refill_(Clock::now());

				const double need = static_cast<double>(bytes);
				if (tokens_ >= need) {
					tokens_ -= need;
					return;
				}

				const double missing = need - tokens_;
				// missing bytes / (bytes per second) => seconds
				const double seconds = missing / static_cast<double>(max_bytes_per_sec_);
				sleep_for_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
				    std::chrono::duration<double>(seconds));
			}

			// Sleep outside lock.
			if (sleep_for_ns.count() > 0) {
				std::this_thread::sleep_for(sleep_for_ns);
			} else {
				std::this_thread::yield();
			}
		}
	}

  private:
	using Clock = std::chrono::steady_clock;

	void refill_(Clock::time_point now) {
		const auto elapsed = now - last_;
		if (elapsed <= Clock::duration::zero()) {
			return;
		}
		last_ = now;

	const double add =
	    std::chrono::duration<double>(elapsed).count()
	    * static_cast<double>(max_bytes_per_sec_);
		tokens_ = std::min(static_cast<double>(burst_bytes_), tokens_ + add);
	}

	mutable std::mutex mu_;
	uint64_t max_bytes_per_sec_{kDefaultMaxBytesPerSec};
	uint64_t burst_bytes_{kDefaultMaxBytesPerSec};
	double tokens_{0.0};
	Clock::time_point last_;
};

} // namespace utils
