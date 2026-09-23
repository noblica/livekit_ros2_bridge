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
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
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

// Abstract pull-based decoded-PCM stream, mirroring the one livekit::AudioStream
// surface the Talkback reader uses, so reader-handover and shutdown can be
// tested without a real LiveKit track.
class TalkbackAudioStream
{
public:
  virtual ~TalkbackAudioStream() = default;
  virtual bool read(livekit::AudioFrameEvent & out_event) = 0;
  virtual void close() = 0;
};

using TalkbackStreamFactory = std::function<std::shared_ptr<TalkbackAudioStream>(
  const std::shared_ptr<livekit::Track> & track, std::size_t capacity)>;

// The bridge-side Talkback receive path. Consumes the one remote media track
// the bridge names — the Talkback Track (`lkros.audio.operator`) — by exact
// name, reads decoded PCM on a dedicated reader thread, and feeds the
// bridge-owned playback sink. The publisher's identity reaches logs via track
// events; the bridge performs no identity checks on it.
//
// Lifecycle: the manager is created only when `audio.sink` is configured, so
// an unconfigured deployment never touches track events. Every cleanup path
// (unsubscribe, unpublish, subscription failure, participant disconnect,
// shutdown) tears down its readers; no orphaned reader threads remain. A
// reconnect is not a cleanup path: an SDK resume keeps remote tracks and their
// media alive, and a full restart unpublishes every remote track before it
// reports Reconnecting, which ends those readers through the paths above.
class TalkbackManager
{
public:
  TalkbackManager(RoomConnection & room_connection, std::string sink_fragment);
  TalkbackManager(
    RoomConnection & room_connection,
    std::shared_ptr<TalkbackSinkInterface> sink,
    TalkbackStreamFactory stream_factory);
  ~TalkbackManager();

  TalkbackManager(const TalkbackManager &) = delete;
  TalkbackManager & operator=(const TalkbackManager &) = delete;
  TalkbackManager(TalkbackManager &&) = delete;
  TalkbackManager & operator=(TalkbackManager &&) = delete;

  // Room event handlers. Every public handler serializes on event_mutex_ so the
  // reader map's check-then-act in subscribeOperatorTrack cannot interleave.
  void onRemoteTrackPublished(const RemoteTrackEvent & event);
  void onRemoteTrackUnpublished(const RemoteTrackEvent & event);
  void onRemoteTrackSubscribed(const RemoteTrackEvent & event);
  void onRemoteTrackUnsubscribed(const RemoteTrackEvent & event);
  void onRemoteTrackSubscriptionFailed(const RemoteTrackSubscriptionFailedEvent & event);
  void onParticipantDisconnected(const livekit::ParticipantDisconnectedEvent & event);

  // Connected (the first connect or the end of a reconnect) subscribes every
  // operator-named publication in the snapshot that is not yet subscribed:
  // tracks already present at connect, and tracks a full-restart reconnect
  // re-announced while Reconnecting, when publish events are not forwarded.
  // Running readers are left alone, so a resume keeps playing. Idempotent.
  void onConnected();

private:
  struct Reader
  {
    std::string participant_identity;
    std::string track_sid;
    std::atomic<bool> stop{false};
    std::shared_ptr<TalkbackAudioStream> stream;
  };

  void subscribeOperatorTrack(const RemoteTrackEvent & event);
  void stopReader(const std::string & track_sid, const char * reason);
  void snapshotSubscribe();

  RoomConnection & room_connection_;
  std::shared_ptr<TalkbackSinkInterface> sink_;
  TalkbackStreamFactory stream_factory_;

  // Serializes public handlers; taken once at the top of each handler and never
  // recursively. stopReader() and the reader map use mutex_, which may be taken
  // while event_mutex_ is held.
  std::mutex event_mutex_;
  std::mutex mutex_;
  std::map<std::string, std::shared_ptr<Reader>> readers_;
  std::atomic<std::uint64_t> reader_seq_{0};
  std::atomic<bool> is_shutdown_{false};

  // Reader lifetime bookkeeping: the destructor waits for every detached reader
  // to finish before returning, so no thread can still be inside LiveKit FFI
  // when the SDK shuts down. Each reader first drops its stream, reader, and
  // sink references, then decrements live_readers_ and notifies wait_cv_ while
  // holding wait_mutex_, and touches no member after unlocking; the destructor
  // therefore cannot see zero and free wait_cv_ while a notify is in flight.
  std::atomic<std::size_t> live_readers_{0};
  std::mutex wait_mutex_;
  std::condition_variable wait_cv_;
};

}  // namespace livekit_ros2_bridge::audio
