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

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

#include "audio/stream_spec.hpp"
#include "livekit/audio_frame.h"
#include "livekit/audio_source.h"
#include "room_connection.hpp"

namespace livekit_ros2_bridge::audio
{

class GStreamerStream;
struct PipelineCallbacks;

class TrackPublisher final
{
public:
  static std::shared_ptr<TrackPublisher> create(RoomConnection & connection, StreamSpec spec);

  // Does not start a GStreamer input stream.
  TrackPublisher(RoomConnection & connection, StreamSpec spec);

  ~TrackPublisher();

  TrackPublisher(const TrackPublisher &) = delete;
  TrackPublisher & operator=(const TrackPublisher &) = delete;
  TrackPublisher(TrackPublisher &&) = delete;
  TrackPublisher & operator=(TrackPublisher &&) = delete;

  const StreamSpec & spec() const
  {
    return spec_;
  }

  // First frame publishes; the AudioSource is created lazily from the first
  // sample's caps. The bridge tail forces mono 48 kHz, so no republish path
  // is needed on rate or channel change.
  void capture(const livekit::AudioFrame & frame);

private:
  friend class GStreamerStream;

  // May be invoked from GStreamer or LiveKit worker threads after close() starts.
  void onSampleUnpackFailed(const std::string & error);
  void onCaptureFailed(const std::string & error);
  void onRestartFailed(const std::string & error);

  PipelineCallbacks makePipelineCallbacks(
    std::function<bool()> is_shutdown, std::function<void(const std::string & reason)> on_failed);

  void close();

  RoomConnection & connection_;
  StreamSpec spec_;

  // Guards stream handles, publication state, and late callbacks racing with close().
  std::mutex mutex_;
  bool closed_ = false;
  bool published_once_ = false;
  bool captured_frame_logged_ = false;
  std::unique_ptr<GStreamerStream> gstreamer_stream_;
  std::shared_ptr<livekit::AudioSource> source_;
  std::shared_ptr<livekit::LocalAudioTrack> track_;
};

}  // namespace livekit_ros2_bridge::audio
