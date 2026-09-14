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
// See talkback_poc.hpp. Design doc: operator-talkback-design.md §5/§7.
// PRD: talkback-poc-layer-a-prd.md.

#include "poc/talkback_poc.hpp"

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "livekit/audio_frame.h"
#include "livekit/audio_stream.h"
#include "livekit/participant.h"
#include "livekit/remote_participant.h"
#include "livekit/remote_track_publication.h"
#include "livekit/track.h"
#include "livekit/track_publication.h"
#include "utils/log_event.hpp"

namespace livekit_ros2_bridge
{

namespace
{

const auto kLogger = rclcpp::get_logger("livekit_ros2_bridge.talkback_poc");

// Ring-buffer capacity for AudioStream::fromTrack — newest-wins so a stalled
// reader drains stale audio instead of lagging unboundedly (PRD Implementation
// Decisions: "bounded queue capacity in stream options").
constexpr std::size_t kStreamCapacity = 50;

std::string sanitizeSid(const std::string & sid)
{
  std::string clean;
  clean.reserve(sid.size());
  for (const char character : sid) {
    const bool safe = (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
                      (character >= '0' && character <= '9') || character == '_' || character == '-';
    clean.push_back(safe ? character : '_');
  }
  return clean.empty() ? std::string("tracksid") : clean;
}

// Minimal 44-byte RIFF/WAVE header for PCM16. Hand-rolled because the POC adds
// no new dependencies (no sndfile). All fields are written as explicit
// little-endian bytes manually, which is endian-safe on any platform.
void writeWavHeader(std::ofstream & file, std::uint32_t sample_rate, std::uint16_t num_channels)
{
  const std::uint16_t bits_per_sample = 16;
  const std::uint16_t block_align = num_channels * (bits_per_sample / 8);
  const std::uint32_t byte_rate = sample_rate * block_align;

  auto writeU16 = [&file](std::uint16_t value) {
    char bytes[2] = {static_cast<char>(value & 0xFF), static_cast<char>((value >> 8) & 0xFF)};
    file.write(bytes, 2);
  };
  auto writeU32 = [&file](std::uint32_t value) {
    char bytes[4] = {
      static_cast<char>(value & 0xFF),
      static_cast<char>((value >> 8) & 0xFF),
      static_cast<char>((value >> 16) & 0xFF),
      static_cast<char>((value >> 24) & 0xFF)};
    file.write(bytes, 4);
  };

  file.write("RIFF", 4);
  writeU32(36);  // placeholder; patched on close (36 + data size)
  file.write("WAVE", 4);
  file.write("fmt ", 4);
  writeU32(16);  // fmt chunk size
  writeU16(1);  // PCM
  writeU16(num_channels);
  writeU32(sample_rate);
  writeU32(byte_rate);
  writeU16(block_align);
  writeU16(bits_per_sample);
  file.write("data", 4);
  writeU32(0);  // placeholder; patched on close
}

// Rewrites the two placeholder sizes after data is flushed. Requires an
// openable-for-update stream; failure is logged by the caller, not fatal.
void patchWavHeader(const std::string & path, std::uint32_t data_bytes)
{
  std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
  if (!file.is_open()) {
    return;
  }
  const std::uint32_t riff_size = 36 + data_bytes;

  auto writeU32At = [&file](std::streamoff offset, std::uint32_t value) {
    char bytes[4] = {
      static_cast<char>(value & 0xFF),
      static_cast<char>((value >> 8) & 0xFF),
      static_cast<char>((value >> 16) & 0xFF),
      static_cast<char>((value >> 24) & 0xFF)};
    file.seekp(offset);
    file.write(bytes, 4);
  };

  writeU32At(4, riff_size);  // RIFF chunk size
  writeU32At(40, data_bytes);  // data chunk size
  file.flush();
}

}  // namespace

struct TalkbackPoc::ReaderState
{
  // Written once by onTrackSubscribed before the reader thread starts, then
  // read-only from the reader thread.
  std::string track_sid;
  std::string track_name;
  std::string participant_identity;

  // Set from other threads (unsubscribed/unpublished/disconnect/destructor);
  // the reader thread checks it between blocking read() calls.
  std::atomic<bool> stop{false};

  // Shared with the reader thread so the signalling side can close() the
  // stream, which unblocks a read() that would otherwise block indefinitely.
  std::shared_ptr<livekit::AudioStream> stream;
};

TalkbackPoc::TalkbackPoc(rclcpp::Logger logger, std::string wav_dir)
: logger_(std::move(logger))
, wav_dir_(std::move(wav_dir))
, sink_(std::make_shared<TalkbackSink>())
{
  std::error_code fs_error;
  std::filesystem::create_directories(wav_dir_, fs_error);
  if (fs_error) {
    LogEvent(kLogger, "talkback_poc_wav_dir_failed")
      .fieldOr("wav_dir", wav_dir_)
      .field("error", fs_error.message())
      .warn();
  }
  LogEvent(logger_, "talkback_poc_enabled").fieldOr("wav_dir", wav_dir_).info();
}

TalkbackPoc::~TalkbackPoc()
{
  // Signal every reader to exit; each detached thread checks shutdown_ before
  // using its ReaderState, so a reader mid-read() finishes its iteration and
  // exits without touching other TalkbackPoc members. The sink survives via
  // reader-thread shared_ptr copies until the last reader releases it, but
  // playback is stopped here deterministically.
  shutdown_.store(true);
  sink_->stop();
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto & [track_sid, reader] : readers_) {
    (void)track_sid;
    reader->stop.store(true);
    if (reader->stream != nullptr) {
      reader->stream->close();
    }
  }
}

void TalkbackPoc::onTrackSubscribed(const livekit::TrackSubscribedEvent & event)
{
  if (shutdown_.load()) {
    return;
  }
  if (event.track == nullptr || event.publication == nullptr) {
    LogEvent(kLogger, "talkback_poc_subscribed_ignored").field("reason", "null_track_or_publication").warn();
    return;
  }
  if (event.track->kind() != livekit::TrackKind::KIND_AUDIO) {
    LogEvent(kLogger, "talkback_poc_track_ignored")
      .field("reason", "non_audio")
      .fieldEnum("kind", event.track->kind())
      .fieldOr("track_sid", event.track->sid())
      .debug();
    return;
  }

  const std::string track_sid = event.track->sid();
  const std::string track_name = event.publication->name();
  const std::string identity = event.participant == nullptr ? std::string() : event.participant->identity();

  LogEvent(kLogger, "talkback_poc_track_subscribed")
    .fieldOr("track_sid", track_sid)
    .fieldOr("track_name", track_name)
    .fieldOr("participant_identity", identity)
    .fieldEnumOr("source", std::optional<livekit::TrackSource>(event.publication->source()), kUnknownFieldValue)
    .info();

  livekit::AudioStream::Options options;
  options.capacity = kStreamCapacity;
  std::shared_ptr<livekit::AudioStream> stream;
  try {
    // Throws on the FFI boundary if the track handle is dead (e.g. the track
    // unsubscribed between the event and this call). Isolated below.
    stream = livekit::AudioStream::fromTrack(event.track, options);
  } catch (...) {
    LogEvent(kLogger, "talkback_poc_stream_create_failed")
      .fieldOr("track_sid", track_sid)
      .fieldException("error", std::current_exception())
      .warn();
    return;
  }

  auto reader = std::make_shared<ReaderState>();
  reader->track_sid = track_sid;
  reader->track_name = track_name;
  reader->participant_identity = identity;
  reader->stream = stream;

  const std::uint64_t reader_id = reader_seq_.fetch_add(1) + 1;

  // Same-SID resubscribe (e.g. same-process reconnect re-fires
  // onTrackSubscribed): stop the previous reader for this SID before
  // registering the new one, so its stop flag stays reachable and no two live
  // readers share a track SID. stopReader() takes mutex_ itself, so this call
  // must stay outside the insert lock below.
  stopReader(track_sid, "resubscribed");

  {
    std::lock_guard<std::mutex> lock(mutex_);
    readers_[track_sid] = reader;
  }

  // Detached reader thread (PRD: "one dedicated reader thread per subscribed
  // audio track"). Shutdown-safety contract, kept deliberately simple for the
  // POC:
  // - The thread captures only shared_ptr copies (reader/stream) and copies of
  //   small strings — never `this` or any TalkbackPoc member. After ~TalkbackPoc
  //   starts, a reader still blocks in read() until stream->close() (called in
  //   the destructor) wakes it; it then sees shutdown_/stop and exits without
  //   touching TalkbackPoc.
  // - RISK (documented, accepted for POC): the thread may briefly outlive the
  //   TalkbackPoc object (read() can also block between frames while a track is
  //   muted — PRD story 5 tests whether mute actually stops delivery). Only
  //   refcounted/shared state is touched after that point, so this cannot
  //   use-after-free TalkbackPoc; the worst case is a leaked thread + stream
  //   until the process exits or the SDK closes the stream.
  std::thread([reader, stream, reader_id, wav_dir = wav_dir_, sink = sink_]() {
    const std::string & track_sid = reader->track_sid;

    std::string wav_path;
    std::ofstream wav_file;
    std::uint32_t wav_sample_rate = 0;
    std::uint16_t wav_num_channels = 0;
    std::uint64_t data_bytes = 0;
    bool wav_open = false;

    std::uint64_t total_frames = 0;
    std::uint64_t total_samples = 0;
    long double total_square_sum = 0.0L;
    std::uint64_t window_frames = 0;
    long double window_square_sum = 0.0L;
    auto window_start = std::chrono::steady_clock::now();

    const auto finalize = [&]() {
      if (wav_open) {
        wav_file.close();
        patchWavHeader(wav_path, data_bytes);
        wav_open = false;
      }
      sink->unbind(reader_id);
      const double seconds = wav_sample_rate > 0 && wav_num_channels > 0
                               ? static_cast<double>(total_samples) /
                                   (static_cast<double>(wav_sample_rate) * static_cast<double>(wav_num_channels))
                               : 0.0;
      LogEvent(kLogger, "talkback_poc_capture_summary")
        .field("reader_id", reader_id)
        .fieldOr("track_sid", track_sid)
        .field("total_frames", total_frames)
        .field("seconds_captured", seconds)
        .fieldOr("wav_path", wav_path)
        .info();
    };

    livekit::AudioFrameEvent frame_event;
    while (!reader->stop.load()) {
      bool got_frame = false;
      try {
        got_frame = stream->read(frame_event);
      } catch (...) {
        LogEvent(kLogger, "talkback_poc_read_failed")
          .field("reader_id", reader_id)
          .fieldOr("track_sid", track_sid)
          .fieldException("error", std::current_exception())
          .warn();
        break;
      }

      if (!got_frame) {
        // EOS / close(): track gone, SDK disconnect, or stream closed.
        LogEvent(kLogger, "talkback_poc_stream_ended")
          .field("reader_id", reader_id)
          .fieldOr("track_sid", track_sid)
          .info();
        break;
      }

      const auto & frame = frame_event.frame;
      const auto & samples = frame.data();

      if (total_frames == 0) {
        wav_sample_rate = static_cast<std::uint32_t>(frame.sampleRate());
        wav_num_channels = static_cast<std::uint16_t>(frame.numChannels());
        wav_path = wav_dir + "/talkback_poc_" + sanitizeSid(track_sid) + "_" + std::to_string(reader_id) + ".wav";
        wav_file.open(wav_path, std::ios::binary | std::ios::trunc);
        if (wav_file.is_open()) {
          writeWavHeader(wav_file, wav_sample_rate, wav_num_channels);
          wav_open = true;
        }
        const bool sink_active =
          sink->onFirstFrame(reader_id, track_sid, static_cast<int>(frame.sampleRate()), frame.numChannels());
        LogEvent(kLogger, "talkback_poc_first_frame")
          .field("reader_id", reader_id)
          .fieldOr("track_sid", track_sid)
          .fieldOr("track_name", reader->track_name)
          .fieldOr("participant_identity", reader->participant_identity)
          .field("sample_rate", wav_sample_rate)
          .field("num_channels", wav_num_channels)
          .field("samples_per_channel", frame.samplesPerChannel())
          .fieldOr("wav_path", wav_open ? wav_path : std::string())
          .field("sink_active", sink_active)
          .info();
      }

      total_frames += 1;
      window_frames += 1;
      for (const std::int16_t sample : samples) {
        const long double sample_value = static_cast<long double>(sample);
        total_square_sum += sample_value * sample_value;
        window_square_sum += sample_value * sample_value;
      }
      total_samples += samples.size();

      if (wav_open && !samples.empty()) {
        // data() is interleaved int16 in host byte order — raw WAV payload.
        wav_file.write(
          reinterpret_cast<const char *>(samples.data()),
          static_cast<std::streamsize>(samples.size() * sizeof(std::int16_t)));
        data_bytes += static_cast<std::uint32_t>(samples.size() * sizeof(std::int16_t));
        if (!wav_file) {
          LogEvent(kLogger, "talkback_poc_wav_write_failed")
            .field("reader_id", reader_id)
            .fieldOr("wav_path", wav_path)
            .warn();
          wav_file.close();
          wav_open = false;
        }
      }

      if (!samples.empty()) {
        sink->push(reader_id, samples.data(), samples.size());
      }

      const auto now = std::chrono::steady_clock::now();
      if (now - window_start >= std::chrono::seconds(1)) {
        const double window_seconds = std::chrono::duration<double>(now - window_start).count();
        const std::size_t window_samples = samples.size() * window_frames;
        const double window_rms =
          window_square_sum > 0.0L && window_samples > 0
            ? std::sqrt(static_cast<double>(window_square_sum) / static_cast<double>(window_samples))
            : 0.0;
        LogEvent(kLogger, "talkback_poc_frame_rate")
          .field("reader_id", reader_id)
          .fieldOr("track_sid", track_sid)
          .field("frames_last_second", window_frames)
          .field("window_seconds", window_seconds)
          .field("rms_avg", window_rms)
          .info();
        window_frames = 0;
        window_square_sum = 0.0L;
        window_start = now;
      }
    }

    finalize();
  }).detach();
}

void TalkbackPoc::stopReader(const std::string & track_sid, const char * reason)
{
  std::shared_ptr<ReaderState> reader;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = readers_.find(track_sid);
    if (it == readers_.end()) {
      return;
    }
    reader = it->second;
    readers_.erase(it);
  }

  reader->stop.store(true);
  if (reader->stream != nullptr) {
    reader->stream->close();
  }
  LogEvent(kLogger, "talkback_poc_reader_stopped").fieldOr("track_sid", track_sid).field("reason", reason).info();
}

void TalkbackPoc::onTrackUnsubscribed(const livekit::TrackUnsubscribedEvent & event)
{
  if (event.track == nullptr) {
    return;
  }
  LogEvent(kLogger, "talkback_poc_track_unsubscribed").fieldOr("track_sid", event.track->sid()).info();
  stopReader(event.track->sid(), "track_unsubscribed");
}

void TalkbackPoc::onTrackUnpublished(const livekit::TrackUnpublishedEvent & event)
{
  if (event.publication == nullptr) {
    return;
  }
  LogEvent(kLogger, "talkback_poc_track_unpublished").fieldOr("track_sid", event.publication->sid()).info();
  stopReader(event.publication->sid(), "track_unpublished");
}

void TalkbackPoc::onParticipantDisconnected(const livekit::ParticipantDisconnectedEvent & event)
{
  const auto * participant = event.participant;
  if (participant == nullptr) {
    return;
  }
  const std::string identity = participant->identity();

  // Snapshot the reader SIDs owned by this participant, then stop them outside
  // mutex_ (stopReader() also takes it).
  std::vector<std::string> sids_to_stop;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto & [track_sid, reader] : readers_) {
      (void)track_sid;
      if (reader->participant_identity == identity) {
        sids_to_stop.push_back(reader->track_sid);
      }
    }
  }

  LogEvent(kLogger, "talkback_poc_participant_disconnected")
    .fieldOr("participant_identity", identity)
    .field("readers_stopped", sids_to_stop.size())
    .info();
  for (const std::string & track_sid : sids_to_stop) {
    stopReader(track_sid, "participant_disconnected");
  }
}

}  // namespace livekit_ros2_bridge
