// Copyright (c) 2025-present Polymath Robotics, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

namespace livekit_ros2_bridge
{

// Bounds the number of concurrent operations per string key (for example, one
// client identity), mirroring RosServiceCaller's per-requester in-flight cap.
//
// The count lives behind a shared_ptr, so a Reservation may be carried onto a
// detached worker thread and still release its slot safely even after the
// limiter's owner has been destroyed. tryReserve/release are internally locked,
// so a reservation taken on one thread may be released on another.
class PerKeyConcurrencyLimiter
{
  struct State
  {
    explicit State(int max_per_key)
    : max_per_key(max_per_key)
    {}

    bool tryAcquire(const std::string & key)
    {
      std::lock_guard<std::mutex> lock(mutex);
      const auto existing = counts.find(key);
      const int in_flight = existing == counts.end() ? 0 : existing->second;
      if (in_flight >= max_per_key) {
        return false;
      }
      counts[key] = in_flight + 1;
      return true;
    }

    void release(const std::string & key)
    {
      std::lock_guard<std::mutex> lock(mutex);
      const auto existing = counts.find(key);
      if (existing == counts.end()) {
        return;
      }
      if (existing->second <= 1) {
        counts.erase(existing);
        return;
      }
      existing->second -= 1;
    }

    std::mutex mutex;
    std::unordered_map<std::string, int> counts;
    const int max_per_key;
  };

public:
  // RAII handle for one reserved slot; releases on destruction. Move-only so it
  // can be handed to whichever worker owns the operation's lifetime. A
  // default-constructed (or moved-from) reservation owns nothing and releases
  // nothing.
  class Reservation final
  {
  public:
    Reservation() = default;

    Reservation(Reservation && other) noexcept
    : state_(std::move(other.state_))
    , key_(std::move(other.key_))
    {
      other.state_.reset();
    }

    Reservation & operator=(Reservation && other) noexcept
    {
      if (this != &other) {
        release();
        state_ = std::move(other.state_);
        key_ = std::move(other.key_);
        other.state_.reset();
      }
      return *this;
    }

    Reservation(const Reservation &) = delete;
    Reservation & operator=(const Reservation &) = delete;

    ~Reservation()
    {
      release();
    }

  private:
    friend class PerKeyConcurrencyLimiter;

    Reservation(std::shared_ptr<State> state, std::string key)
    : state_(std::move(state))
    , key_(std::move(key))
    {}

    void release()
    {
      if (state_ == nullptr) {
        return;
      }
      state_->release(key_);
      state_.reset();
    }

    std::shared_ptr<State> state_;
    std::string key_;
  };

  explicit PerKeyConcurrencyLimiter(int max_per_key)
  : state_(std::make_shared<State>(max_per_key))
  {}

  // Reserves one slot for `key`. Returns the RAII reservation on success, or
  // nullopt when `key` already holds the maximum number of concurrent slots.
  std::optional<Reservation> tryReserve(const std::string & key)
  {
    if (!state_->tryAcquire(key)) {
      return std::nullopt;
    }
    return Reservation(state_, key);
  }

private:
  std::shared_ptr<State> state_;
};

}  // namespace livekit_ros2_bridge
