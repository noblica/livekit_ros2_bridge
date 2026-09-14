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

// TALKBACK POC (Layer A) — THROWAWAY CODE, DO NOT MERGE.
// Proves the bridge can subscribe to a remote client audio track and read
// decoded PCM from it. Feeds the results section of operator-talkback-design.md
// §7; delete this file (and its wiring) once the POC matrix is recorded.

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "livekit/room_event_types.h"
#include "poc/talkback_sink.hpp"
#include "rclcpp/logger.hpp"

namespace livekit
{
class AudioStream;
class Track;
}  // namespace livekit

namespace livekit_ros2_bridge
{

// Subscribes to every remote audio track, dumps PCM to per-track WAV files, and
// logs the frame cadence/RMS evidence the talkback design doc needs. Enabled
// only when LIVEKIT_TALKBACK_POC=1 is set at node construction.
class TalkbackPoc
{
public:
  TalkbackPoc(rclcpp::Logger logger, std::string wav_dir);
  ~TalkbackPoc();

  TalkbackPoc(const TalkbackPoc &) = delete;
  TalkbackPoc & operator=(const TalkbackPoc &) = delete;
  TalkbackPoc(TalkbackPoc &&) = delete;
  TalkbackPoc & operator=(TalkbackPoc &&) = delete;

  // RoomEventCallbacks adapters. These run on SDK delegate threads.
  void onTrackSubscribed(const livekit::TrackSubscribedEvent & event);
  void onTrackUnsubscribed(const livekit::TrackUnsubscribedEvent & event);
  void onTrackUnpublished(const livekit::TrackUnpublishedEvent & event);
  void onParticipantDisconnected(const livekit::ParticipantDisconnectedEvent & event);

private:
  struct ReaderState;

  void stopReader(const std::string & track_sid, const char * reason);

  // Members below are read only by the constructor / owner thread; reader
  // threads capture copies or shared_ptrs and never dereference `this`.
  const rclcpp::Logger logger_;
  const std::string wav_dir_;
  std::shared_ptr<TalkbackSink> sink_;

  // Guards readers_. Reader threads own their ReaderState exclusively; the map
  // only locates the shared stop flag and the stream for close().
  std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<ReaderState>> readers_;

  std::atomic<bool> shutdown_{false};
  std::atomic<std::uint64_t> reader_seq_{0};
};

}  // namespace livekit_ros2_bridge
