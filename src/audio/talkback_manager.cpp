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

#include "audio/talkback_manager.hpp"

#include <condition_variable>
#include <exception>
#include <thread>
#include <utility>
#include <vector>

#include "livekit/participant.h"
#include "livekit/remote_participant.h"
#include "protocol/constants.hpp"
#include "rclcpp/logging.hpp"
#include "utils/log_event.hpp"
#include "utils/scope_exit.hpp"

namespace livekit_ros2_bridge::audio
{

namespace
{

const auto kLogger = rclcpp::get_logger("livekit_ros2_bridge.talkback");
// Ring-buffer capacity for AudioStream — newest-wins so a stalled reader drains
// stale audio instead of lagging unboundedly on a lossy operator link.
constexpr std::size_t kStreamCapacity = 50;

// Thin adapter from the test-facing TalkbackAudioStream seam onto the SDK's
// livekit::AudioStream, the production stream the reader consumes.
class LiveKitTalkbackAudioStream final : public TalkbackAudioStream
{
public:
  explicit LiveKitTalkbackAudioStream(std::shared_ptr<livekit::AudioStream> stream)
  : stream_(std::move(stream))
  {}

  bool read(livekit::AudioFrameEvent & out_event) override
  {
    return stream_->read(out_event);
  }

  void close() override
  {
    stream_->close();
  }

private:
  std::shared_ptr<livekit::AudioStream> stream_;
};

std::shared_ptr<TalkbackAudioStream> makeLiveKitTalkbackStream(
  const std::shared_ptr<livekit::Track> & track, std::size_t capacity)
{
  livekit::AudioStream::Options options;
  options.capacity = capacity;
  return std::make_shared<LiveKitTalkbackAudioStream>(livekit::AudioStream::fromTrack(track, options));
}

}  // namespace

TalkbackManager::TalkbackManager(RoomConnection & room_connection, std::string sink_fragment)
: TalkbackManager(room_connection, std::make_shared<TalkbackSink>(std::move(sink_fragment)), makeLiveKitTalkbackStream)
{}

TalkbackManager::TalkbackManager(
  RoomConnection & room_connection, std::shared_ptr<TalkbackSinkInterface> sink, TalkbackStreamFactory stream_factory)
: room_connection_(room_connection)
, sink_(std::move(sink))
, stream_factory_(std::move(stream_factory))
{}

TalkbackManager::~TalkbackManager()
{
  std::map<std::string, std::shared_ptr<Reader>> readers;
  {
    // Take event_mutex_ so no in-flight handler can add a reader after this
    // snapshot; then release it before blocking on any reader.
    std::lock_guard<std::mutex> event_lock(event_mutex_);
    is_shutdown_.store(true, std::memory_order_release);
    std::lock_guard<std::mutex> lock(mutex_);
    readers = std::move(readers_);
    readers_.clear();
  }

  // Signal every reader and close its stream so a read() blocked between
  // frames wakes and exits.
  for (auto & [track_sid, reader] : readers) {
    (void)track_sid;
    reader->stop.store(true, std::memory_order_release);
    if (reader->stream != nullptr) {
      reader->stream->close();
    }
  }

  sink_->stop();

  // Wait for every detached reader to finish its ScopeExit. live_readers_ is
  // incremented before each spawn and decremented on every exit path, so once
  // it reaches zero no thread can still be inside LiveKit FFI.
  std::unique_lock<std::mutex> wait_lock(wait_mutex_);
  wait_cv_.wait(wait_lock, [this]() { return live_readers_.load(std::memory_order_acquire) == 0; });
}

bool TalkbackManager::isActive() const
{
  return !is_shutdown_.load(std::memory_order_acquire);
}

void TalkbackManager::onRemoteTrackPublished(const RemoteTrackEvent & event)
{
  std::lock_guard<std::mutex> event_lock(event_mutex_);
  if (is_shutdown_.load(std::memory_order_acquire)) {
    return;
  }

  // The publisher's identity reaches logs here; the bridge performs no
  // identity checks on the Talkback Track.
  LogEvent(kLogger, "remote_track_published")
    .fieldOr("participant_identity", event.participant_identity)
    .fieldOr("track_sid", event.track_sid)
    .fieldQuoted("track_name", event.track_name)
    .info();

  if (event.track_name != protocol::kTalkbackTrackName) {
    return;
  }
  // A second live operator track still gets subscribed (it is the named
  // track); its frames are then logged once and dropped by the single active
  // sink, so it can never steal the speaker from the active operator.
  if (!room_connection_.subscribeRemoteTrack(event.participant_identity, event.track_sid)) {
    LogEvent(kLogger, "talkback_subscribe_failed")
      .fieldOr("participant_identity", event.participant_identity)
      .fieldOr("track_sid", event.track_sid)
      .warn();
  }
}

void TalkbackManager::onRemoteTrackUnpublished(const RemoteTrackEvent & event)
{
  std::lock_guard<std::mutex> event_lock(event_mutex_);
  if (is_shutdown_.load(std::memory_order_acquire)) {
    return;
  }

  LogEvent(kLogger, "remote_track_unpublished")
    .fieldOr("participant_identity", event.participant_identity)
    .fieldOr("track_sid", event.track_sid)
    .fieldQuoted("track_name", event.track_name)
    .info();

  if (event.track_name == protocol::kTalkbackTrackName) {
    stopReader(event.track_sid, "track_unpublished");
  }
}

void TalkbackManager::onRemoteTrackSubscribed(const RemoteTrackEvent & event)
{
  std::lock_guard<std::mutex> event_lock(event_mutex_);
  if (is_shutdown_.load(std::memory_order_acquire)) {
    return;
  }
  if (
    event.track == nullptr || event.track_kind != livekit::TrackKind::KIND_AUDIO ||
    event.track_name != protocol::kTalkbackTrackName)
  {
    LogEvent(kLogger, "talkback_track_ignored")
      .field("reason", "not_operator_audio")
      .fieldOr("track_sid", event.track_sid)
      .fieldQuoted("track_name", event.track_name)
      .debug();
    return;
  }

  subscribeOperatorTrack(event);
}

void TalkbackManager::onRemoteTrackUnsubscribed(const RemoteTrackEvent & event)
{
  std::lock_guard<std::mutex> event_lock(event_mutex_);
  if (is_shutdown_.load(std::memory_order_acquire)) {
    return;
  }
  if (event.track_name == protocol::kTalkbackTrackName) {
    stopReader(event.track_sid, "track_unsubscribed");
  }
}

void TalkbackManager::onRemoteTrackSubscriptionFailed(const RemoteTrackSubscriptionFailedEvent & event)
{
  std::lock_guard<std::mutex> event_lock(event_mutex_);
  if (is_shutdown_.load(std::memory_order_acquire)) {
    return;
  }

  LogEvent(kLogger, "talkback_subscription_failed")
    .fieldOr("participant_identity", event.participant_identity)
    .fieldOr("track_sid", event.track_sid)
    .fieldOr("error", event.error)
    .warn();

  stopReader(event.track_sid, "subscription_failed");
}

void TalkbackManager::onParticipantDisconnected(const livekit::ParticipantDisconnectedEvent & event)
{
  std::lock_guard<std::mutex> event_lock(event_mutex_);
  if (is_shutdown_.load(std::memory_order_acquire)) {
    return;
  }
  const auto * participant = event.participant;
  if (participant == nullptr) {
    return;
  }

  // Snapshot the reader keys owned by this identity, then stop them outside
  // mutex_ (stopReader() also takes it).
  std::vector<std::string> track_sids_to_stop;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto & [track_sid, reader] : readers_) {
      if (reader->participant_identity == participant->identity()) {
        track_sids_to_stop.push_back(track_sid);
      }
    }
  }

  for (const std::string & track_sid : track_sids_to_stop) {
    stopReader(track_sid, "participant_disconnected");
  }
}

void TalkbackManager::onReconnecting()
{
  std::lock_guard<std::mutex> event_lock(event_mutex_);
  if (is_shutdown_.load(std::memory_order_acquire)) {
    return;
  }
  // A reconnect replaces the SDK media session: every reader is stale. Tear
  // them down and release the device, but do not subscribe until Connected.
  stopAllReaders("room_reconnecting");
}

void TalkbackManager::onConnected()
{
  std::lock_guard<std::mutex> event_lock(event_mutex_);
  if (is_shutdown_.load(std::memory_order_acquire)) {
    return;
  }
  stopAllReaders("room_connected");
  snapshotSubscribe();
}

void TalkbackManager::subscribeOperatorTrack(const RemoteTrackEvent & event)
{
  if (event.track == nullptr || event.track_sid.empty()) {
    return;
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (is_shutdown_.load(std::memory_order_acquire) || readers_.find(event.track_sid) != readers_.end()) {
      return;
    }
  }

  std::shared_ptr<TalkbackAudioStream> stream;
  try {
    stream = stream_factory_(event.track, kStreamCapacity);
  } catch (...) {
    LogEvent(kLogger, "talkback_stream_create_failed")
      .fieldOr("track_sid", event.track_sid)
      .fieldException("error", std::current_exception())
      .warn();
    return;
  }

  auto reader = std::make_shared<Reader>();
  reader->participant_identity = event.participant_identity;
  reader->track_sid = event.track_sid;
  reader->stream = stream;

  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (is_shutdown_.load(std::memory_order_acquire)) {
      return;
    }
    readers_[event.track_sid] = reader;
  }

  const std::uint64_t reader_id = reader_seq_.fetch_add(1, std::memory_order_relaxed) + 1;

  LogEvent(kLogger, "talkback_reader_started")
    .field("reader_id", reader_id)
    .fieldOr("track_sid", event.track_sid)
    .fieldQuoted("track_name", event.track_name)
    .fieldOr("participant_identity", event.participant_identity)
    .info();

  live_readers_.fetch_add(1, std::memory_order_acq_rel);

  // Dedicated reader thread: reads decoded PCM and feeds the playback sink.
  // It captures `this` for the live-reader counter, which is safe because the
  // destructor waits for live_readers_ to reach zero before returning. The
  // reader owns the sink claim once it binds and releases it in the ScopeExit,
  // so a handover rebinds the sink with zero bridge-side identity knowledge.
  try {
    std::thread([this, reader, stream, reader_id, sink = sink_]() {
      const std::string track_sid = reader->track_sid;

      std::uint64_t total_frames = 0;
      bool first_frame_logged = false;
      bool sink_owned = false;

      // Runs on every exit path, in order: release the sink claim, log the
      // stop, then drop the live-reader count and wake the destructor.
      ScopeExit on_reader_exit([this, &sink, &track_sid, reader_id, &total_frames]() {
        sink->unbind(reader_id);
        LogEvent(kLogger, "talkback_reader_stopped")
          .field("reader_id", reader_id)
          .fieldOr("track_sid", track_sid)
          .field("frames_received", total_frames)
          .info();
        live_readers_.fetch_sub(1, std::memory_order_acq_rel);
        wait_cv_.notify_all();
      });

      // One backstop for the whole body: read()/bind()/push() are SDK/GStreamer
      // calls that may throw across the FFI boundary. An exception escaping a
      // detached thread calls std::terminate(), so nothing is allowed past this
      // boundary; every failure is logged, then the ScopeExit finalizes.
      try {
        livekit::AudioFrameEvent frame_event;
        while (!reader->stop.load(std::memory_order_acquire)) {
          bool got_frame = false;
          try {
            got_frame = stream->read(frame_event);
          } catch (...) {
            LogEvent(kLogger, "talkback_read_failed")
              .field("reader_id", reader_id)
              .fieldOr("track_sid", track_sid)
              .fieldException("error", std::current_exception())
              .warn();
            break;
          }

          if (!got_frame) {
            // EOS / close(): track gone, SDK disconnect, or stream closed.
            break;
          }

          const auto & frame = frame_event.frame;
          const auto & samples = frame.data();
          if (samples.empty()) {
            continue;
          }

          // Retry the claim on every frame until it succeeds. A handover can
          // find the previous owner not yet finalized, so a one-shot bind on
          // the first frame would permanently silence this reader; once owned,
          // stop retrying. Caps are taken from the claiming frame's actual
          // rate/channels. Mute is silence-through: silent frames keep flowing
          // so the sink's claim never blocks the next holder.
          if (!sink_owned) {
            sink_owned = sink->bind(reader_id, frame.sampleRate(), frame.numChannels());
          }

          if (!first_frame_logged) {
            first_frame_logged = true;
            LogEvent(kLogger, "talkback_first_frame")
              .field("reader_id", reader_id)
              .fieldOr("track_sid", track_sid)
              .field("sample_rate", frame.sampleRate())
              .field("channels", frame.numChannels())
              .field("samples_per_channel", frame.samplesPerChannel())
              .field("sink_bound", sink_owned)
              .info();
          }

          // While unbound, push() logs once and drops; live frames still re-arm
          // the sink's rate-bounded restart loop, so a restored device self-heals.
          sink->push(reader_id, samples.data(), samples.size());
          ++total_frames;
        }
      } catch (...) {
        LogEvent(kLogger, "talkback_reader_failed")
          .field("reader_id", reader_id)
          .fieldOr("track_sid", track_sid)
          .fieldException("error", std::current_exception())
          .warn();
      }
    }).detach();
  } catch (...) {
    live_readers_.fetch_sub(1, std::memory_order_acq_rel);
    wait_cv_.notify_all();
    {
      std::lock_guard<std::mutex> lock(mutex_);
      readers_.erase(event.track_sid);
    }
    LogEvent(kLogger, "talkback_reader_start_failed")
      .fieldOr("track_sid", event.track_sid)
      .fieldException("error", std::current_exception())
      .warn();
    return;
  }
}

void TalkbackManager::stopReader(const std::string & track_sid, const char * reason)
{
  std::shared_ptr<Reader> reader;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = readers_.find(track_sid);
    if (it == readers_.end()) {
      return;
    }
    reader = it->second;
    readers_.erase(it);
  }

  reader->stop.store(true, std::memory_order_release);
  if (reader->stream != nullptr) {
    reader->stream->close();
  }
  LogEvent(kLogger, "talkback_reader_stopping").fieldOr("track_sid", track_sid).field("reason", reason).info();
}

void TalkbackManager::stopAllReaders(const char * reason)
{
  std::vector<std::string> track_sids_to_stop;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    track_sids_to_stop.reserve(readers_.size());
    for (const auto & [track_sid, reader] : readers_) {
      (void)reader;
      track_sids_to_stop.push_back(track_sid);
    }
  }
  for (const std::string & track_sid : track_sids_to_stop) {
    stopReader(track_sid, reason);
  }
}

void TalkbackManager::snapshotSubscribe()
{
  const auto snapshot = room_connection_.remoteTrackSnapshot();
  for (const auto & entry : snapshot) {
    if (entry.track_name == protocol::kTalkbackTrackName && !entry.subscribed) {
      (void)room_connection_.subscribeRemoteTrack(entry.participant_identity, entry.track_sid);
    }
  }
}

}  // namespace livekit_ros2_bridge::audio
