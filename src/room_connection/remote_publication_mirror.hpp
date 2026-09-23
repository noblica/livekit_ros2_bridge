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

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "livekit/remote_track_publication.h"
#include "room_connection.hpp"

namespace livekit_ros2_bridge
{

// Bridge-owned copy of the remote participants' publication map. The LiveKit SDK owns that map and
// mutates it on its FFI event thread; reading it from ROS executor threads is unsynchronized. This
// mirror is mutated only from the FFI delegate thread (plus one connect-time seed) and read from
// every other thread, so callers serialize it themselves with SdkRoomConnection::mutex_.
//
// No internal locking: the caller holds mutex_ across every call.
class RemotePublicationMirror
{
public:
  struct Record
  {
    RoomConnection::RemoteTrackSnapshotEntry entry;
    std::shared_ptr<livekit::RemoteTrackPublication> handle;
  };

  // Inserts any candidate whose sid is not already known and returns just those newly-added
  // snapshots; existing records are updated in place (name/kind/handle) without being returned.
  std::vector<RoomConnection::RemoteTrackSnapshotEntry> merge(const std::vector<Record> & candidates)
  {
    std::vector<RoomConnection::RemoteTrackSnapshotEntry> added;
    for (const auto & candidate : candidates) {
      if (mergeRecord(candidate)) {
        added.push_back(candidate.entry);
      }
    }
    return added;
  }

  std::vector<RoomConnection::RemoteTrackSnapshotEntry> merge(
    const std::vector<RoomConnection::RemoteTrackSnapshotEntry> & candidates,
    const std::shared_ptr<livekit::RemoteTrackPublication> & handle = nullptr)
  {
    std::vector<Record> records;
    records.reserve(candidates.size());
    for (const auto & candidate : candidates) {
      records.push_back(Record{candidate, handle});
    }
    return merge(records);
  }

  std::vector<RoomConnection::RemoteTrackSnapshotEntry> merge(
    const RoomConnection::RemoteTrackSnapshotEntry & candidate,
    const std::shared_ptr<livekit::RemoteTrackPublication> & handle = nullptr)
  {
    return merge(std::vector<Record>{Record{candidate, handle}});
  }

  std::optional<Record> find(const std::string & track_sid) const
  {
    const auto record = records_.find(track_sid);
    if (record == records_.end()) {
      return std::nullopt;
    }
    return record->second;
  }

  bool contains(const std::string & track_sid) const
  {
    return records_.find(track_sid) != records_.end();
  }

  void erase(const std::string & track_sid)
  {
    records_.erase(track_sid);
  }

  void eraseIdentity(const std::string & participant_identity)
  {
    for (auto record = records_.begin(); record != records_.end();) {
      if (record->second.entry.participant_identity == participant_identity) {
        record = records_.erase(record);
        continue;
      }
      ++record;
    }
  }

  void setSubscribed(const std::string & track_sid, bool subscribed)
  {
    const auto record = records_.find(track_sid);
    if (record == records_.end()) {
      return;
    }
    record->second.entry.subscribed = subscribed;
  }

  void clear()
  {
    records_.clear();
  }

  std::vector<RoomConnection::RemoteTrackSnapshotEntry> snapshot() const
  {
    std::vector<RoomConnection::RemoteTrackSnapshotEntry> entries;
    entries.reserve(records_.size());
    for (const auto & [track_sid, record] : records_) {
      (void)track_sid;
      entries.push_back(record.entry);
    }
    return entries;
  }

  std::size_t size() const
  {
    return records_.size();
  }

private:
  // Returns true when the candidate was newly added, false when it updated an existing record.
  bool mergeRecord(const Record & candidate)
  {
    if (candidate.entry.track_sid.empty()) {
      return false;
    }

    const auto existing = records_.find(candidate.entry.track_sid);
    if (existing == records_.end()) {
      records_.emplace(candidate.entry.track_sid, candidate);
      return true;
    }

    existing->second.entry = candidate.entry;
    // Keep a good handle when an update arrives without one (never clobber it with null).
    if (candidate.handle != nullptr) {
      existing->second.handle = candidate.handle;
    }
    return false;
  }

  std::unordered_map<std::string, Record> records_;
};

}  // namespace livekit_ros2_bridge
