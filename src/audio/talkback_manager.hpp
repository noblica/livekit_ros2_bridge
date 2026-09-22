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

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "audio/talkback_sink.hpp"
#include "livekit/audio_stream.h"
#include "livekit/track.h"
#include "room_connection.hpp"

namespace livekit_ros2_bridge::audio
{

// The bridge-side Talkback receive path. Consumes the one remote media track
// the bridge names — the Talkback Track (`lkros.audio.operator`) — by exact
// name, reads decoded PCM on a dedicated reader thread, and feeds the
// bridge-owned playback sink. The publisher's identity reaches logs via track
// events; the bridge performs no identity checks on it.
//
// Lifecycle: the manager is created only when `audio.sink` is configured, so
// an unconfigured deployment never touches track events. Every cleanup path
// (unsubscribe, unpublish, participant disconnect, room generation change,
// shutdown) tears down its readers; no orphaned reader threads remain.
class TalkbackManager
{
public:
  TalkbackManager(RoomConnection & room_connection, std::string sink_fragment);
  ~TalkbackManager();

  TalkbackManager(const TalkbackManager &) = delete;
  TalkbackManager & operator=(const TalkbackManager &) = delete;
  TalkbackManager(TalkbackManager &&) = delete;
  TalkbackManager & operator=(TalkbackManager &&) = delete;

  // True while this manager may process track events. The room callback path
  // consults the gate before invoking any handler; the Runtime additionally
  // holds the manager only while configured, so this guard is belt-and-braces.
  bool isActive() const;

  // Room event handlers.
  void onRemoteTrackPublished(const RemoteTrackEvent & event);
  void onRemoteTrackUnpublished(const RemoteTrackEvent & event);
  void onRemoteTrackSubscribed(const RemoteTrackEvent & event);
  void onRemoteTrackUnsubscribed(const RemoteTrackEvent & event);
  void onParticipantDisconnected(const livekit::ParticipantDisconnectedEvent & event);
  void onRoomGenerationChanged();

private:
  struct Reader
  {
    std::string participant_identity;
    std::string track_sid;
    std::atomic<bool> stop{false};
    std::shared_ptr<livekit::AudioStream> stream;
  };

  void subscribeOperatorTrack(const RemoteTrackEvent & event);
  void stopReader(const std::string & track_sid, const char * reason);
  void snapshotSubscribe();

  RoomConnection & room_connection_;
  std::shared_ptr<TalkbackSink> sink_;

  std::mutex mutex_;
  std::map<std::string, std::shared_ptr<Reader>> readers_;
  std::atomic<std::uint64_t> reader_seq_{0};
  std::atomic<bool> is_shutdown_{false};
};

}  // namespace livekit_ros2_bridge::audio
