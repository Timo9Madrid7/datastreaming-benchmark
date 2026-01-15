#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <thread>

#include "Logger.hpp"
#include "Payload.hpp"
#include "readerwriterqueue.h"

namespace utils {

/**
 * @brief A generic deserialization worker.
 *
 * It decouples message transport (NATS/Kafka/ZeroMQ/...) from the common
 * pattern of:
 *   - enqueue received raw bytes (+ metadata)
 *   - deserialize on a background thread
 *   - log study lines and notify a callback
 */
class Deserializer {
  public:
	struct Item {
		// Keeps the underlying message/buffer alive until processed.
		std::shared_ptr<void> holder;

		// Raw buffer (must remain valid as long as holder is alive).
		const void *data{nullptr};
		size_t len{0};

		// Metadata extracted cheaply on the receive thread.
		std::string message_id;
		std::string topic;
	};

	// Decode raw bytes to payload. Topic is provided (and may be refined).
	using DecodeFn = std::function<bool(const void *data, size_t len,
	                                    std::string &topic, Payload &out)>;

	// Called after successful decode (e.g., termination bookkeeping).
	using OnPayloadFn = std::function<void(
	    const Payload &payload, const std::string &topic, size_t raw_len)>;

	explicit Deserializer(std::shared_ptr<Logger> logger)
	    : logger_(std::move(logger)), queue_(1024) {
	}

	~Deserializer() {
		stop();
	}

	Deserializer(const Deserializer &) = delete;
	Deserializer &operator=(const Deserializer &) = delete;
	Deserializer(Deserializer &&) = delete;
	Deserializer &operator=(Deserializer &&) = delete;

	void start(DecodeFn decode, OnPayloadFn on_payload) {
		stop();
		decode_ = std::move(decode);
		on_payload_ = std::move(on_payload);
		stop_requested_.store(false, std::memory_order_release);
		worker_ = std::thread([this]() { this->run_(); });
	}

	void stop() {
		stop_requested_.store(true, std::memory_order_release);
		if (worker_.joinable()) {
			worker_.join();
		}
		decode_ = nullptr;
		on_payload_ = nullptr;
		// Best-effort drain to release holders promptly.
		Item item;
		while (queue_.try_dequeue(item)) {
			// drop
		}
	}

	bool enqueue(Item item) {
		return queue_.try_enqueue(std::move(item));
	}

	bool is_running() const {
		return worker_.joinable()
		    && !stop_requested_.load(std::memory_order_acquire);
	}

  private:
	void run_() {
		using namespace std::chrono_literals;
		while (!stop_requested_.load(std::memory_order_acquire)
		       || queue_.size_approx() > 0) {
			Item item;
			// Timed wait avoids busy-spinning but still responds quickly to
			// stop.
			if (!queue_.wait_dequeue_timed(item, 25ms)) {
				continue;
			}

			if (!decode_) {
				continue;
			}

			std::string topic = item.topic;
			Payload payload;
			const bool ok = decode_(item.data, item.len, topic, payload);
			if (!ok) {
				this->log_error_("[Deserializer] Deserialization failed.");
				continue;
			}

			this->log_study_("Deserialized," + payload.message_id + "," + topic
			                 + "," + std::to_string(payload.data_size) + ","
			                 + std::to_string(item.len));

			if (on_payload_) {
				on_payload_(payload, topic, item.len);
			}
		}
	}

	void log_error_(const std::string &msg) {
		if (logger_) {
			logger_->log_error(msg);
		}
	}

	void log_study_(const std::string &msg) {
		if (logger_) {
			logger_->log_study(msg);
		}
	}

	std::shared_ptr<Logger> logger_;
	moodycamel::BlockingReaderWriterQueue<Item> queue_;
	std::thread worker_;
	std::atomic<bool> stop_requested_{false};
	DecodeFn decode_;
	OnPayloadFn on_payload_;
};

} // namespace utils
