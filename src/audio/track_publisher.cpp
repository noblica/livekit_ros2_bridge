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

#include "audio/track_publisher.hpp"

#include <cstdint>
#include <memory>
#include <utility>

#include "audio/gstreamer_pipeline.hpp"
#include "audio/gstreamer_stream.hpp"
#include "livekit/local_audio_track.h"
#include "rclcpp/logging.hpp"
#include "utils/log_event.hpp"

namespace livekit_ros2_bridge::audio
{

namespace
{

const auto kLogger = rclcpp::get_logger("audio_track_publisher");
constexpr int kAudioSourceQueueSizeMs = 100;

void tryUnpublish(RoomConnection & connection, const std::shared_ptr<livekit::LocalAudioTrack> & track) noexcept
{
  if (track == nullptr) {
    return;
  }

  try {
    connection.unpublishAudioTrack(track);
  } catch (...) {}
}

}  // namespace

std::shared_ptr<TrackPublisher> TrackPublisher::create(RoomConnection & connection, StreamSpec spec)
{
  auto publisher = std::make_shared<TrackPublisher>(connection, std::move(spec));
  auto stream = std::make_unique<GStreamerStream>(publisher->spec_, *publisher);
  stream->start();
  publisher->gstreamer_stream_ = std::move(stream);
  return publisher;
}

TrackPublisher::TrackPublisher(RoomConnection & connection, StreamSpec spec)
: connection_(connection)
, spec_(std::move(spec))
{}

TrackPublisher::~TrackPublisher()
{
  close();
}

PipelineCallbacks TrackPublisher::makePipelineCallbacks(
  std::function<bool()> is_shutdown, std::function<void(const std::string & reason)> on_failed)
{
  return PipelineCallbacks{
    std::move(is_shutdown),
    [this](const livekit::AudioFrame & frame) { capture(frame); },
    [this](const std::string & error) { onSampleUnpackFailed(error); },
    [this](const std::string & error) { onCaptureFailed(error); },
    std::move(on_failed),
  };
}

void TrackPublisher::capture(const livekit::AudioFrame & frame)
{
  std::lock_guard<std::mutex> lock(mutex_);
  if (closed_) {
    return;
  }

  if (source_ == nullptr) {
    // The bridge tail forces mono 48 kHz, so the first sample's caps define the
    // AudioSource for the life of the stream.
    auto source =
      std::make_shared<livekit::AudioSource>(frame.sampleRate(), frame.numChannels(), kAudioSourceQueueSizeMs);
    auto track = connection_.publishAudioTrack(spec_.track_name, source, spec_.publish_options);
    source_ = std::move(source);
    track_ = std::move(track);
    published_once_ = true;
    captured_frame_logged_ = false;
  }

  source_->captureFrame(frame);
  if (!captured_frame_logged_) {
    LogEvent(kLogger, "audio_stream_frame_captured")
      .fieldOr("stream_key", spec_.stream_key)
      .fieldOr("track_name", spec_.track_name)
      .field("sample_rate", frame.sampleRate())
      .field("channels", frame.numChannels())
      .info();
    captured_frame_logged_ = true;
  }
}

void TrackPublisher::close()
{
  // Stop streams outside mutex_; their callbacks can re-enter this publisher.
  std::unique_ptr<GStreamerStream> gstreamer_stream;
  std::shared_ptr<livekit::AudioSource> source;
  std::shared_ptr<livekit::LocalAudioTrack> track;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_) {
      return;
    }

    published_once_ = false;
    captured_frame_logged_ = false;
    closed_ = true;
    gstreamer_stream = std::move(gstreamer_stream_);
    source = std::move(source_);
    track = std::move(track_);
  }

  if (gstreamer_stream != nullptr) {
    gstreamer_stream->close();
  }
  tryUnpublish(connection_, track);
  track.reset();
  source.reset();
}

void TrackPublisher::onSampleUnpackFailed(const std::string & error)
{
  std::lock_guard<std::mutex> lock(mutex_);
  if (closed_) {
    return;
  }

  LogEvent(kLogger, "audio_stream_sample_unpack_failed")
    .fieldOr("stream_key", spec_.stream_key)
    .fieldOr("error", error)
    .warn();
}

void TrackPublisher::onCaptureFailed(const std::string & error)
{
  std::lock_guard<std::mutex> lock(mutex_);
  if (closed_) {
    return;
  }

  LogEvent(kLogger, "audio_stream_capture_failed")
    .fieldOr("stream_key", spec_.stream_key)
    .fieldOr("error", error)
    .warn();
}

void TrackPublisher::onRestartFailed(const std::string & error)
{
  std::lock_guard<std::mutex> lock(mutex_);
  if (closed_) {
    return;
  }

  LogEvent(kLogger, "audio_stream_restart_failed")
    .fieldOr("stream_key", spec_.stream_key)
    .fieldOr("error", error)
    .warn();
}

}  // namespace livekit_ros2_bridge::audio
